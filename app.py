"""FastAPI web application for WSLCB licensing tracker.

Public routes live here.  Admin routes (/admin/*) are in admin_routes.py,
registered as an APIRouter and included at startup.
"""
import csv
import html
import io
import json
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from db import get_db
from schema import init_db
from admin_auth import get_current_user, AdminRedirectException
from entities import backfill_entities, get_entity_by_id
from queries import (
    search_records, export_records_cursor,
    get_filter_options, get_cities_for_state, US_STATES,
    get_stats,
    get_record_by_id, get_related_records, get_entity_records,
    get_record_sources, get_record_link,
    hydrate_records,
)
from parser import extract_tbody_from_snapshot, extract_tbody_from_diff, strip_anchor_tags
from db import DATA_DIR
from endorsements import (
    seed_endorsements, backfill, repair_code_name_endorsements,
    merge_mixed_case_endorsements,
)
from link_records import build_all_links, get_reverse_link_info, get_outcome_status
from display import format_outcome, summarize_provenance
from log_config import setup_logging
import admin_routes

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database and endorsement tables on startup."""
    setup_logging()
    init_db()
    with get_db() as conn:
        n = seed_endorsements(conn)
        if n:
            logger.info("Seeded %d endorsement code mapping(s)", n)
        repair_code_name_endorsements(conn)
        merged = merge_mixed_case_endorsements(conn)
        if merged:
            logger.info("Merged %d mixed-case endorsement duplicate(s)", merged)
        processed = backfill(conn)
        if processed:
            logger.info("Backfilled endorsements for %d record(s)", processed)
        entity_count = backfill_entities(conn)
        if entity_count:
            logger.info("Backfilled entities for %d record(s)", entity_count)
        # Build application→outcome links if table is empty (first run).
        # Subsequent updates are handled incrementally by the scraper.
        existing_links = conn.execute(
            "SELECT COUNT(*) FROM record_links"
        ).fetchone()[0]
        if not existing_links:
            link_stats = build_all_links(conn)
            if link_stats["total"]:
                logger.info(
                    "Record linking: %d links (%d high, %d medium)",
                    link_stats["total"], link_stats["high"], link_stats["medium"],
                )
    yield


app = FastAPI(title="WSLCB Licensing Tracker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(admin_routes.router)
templates = Jinja2Templates(directory="templates")

PER_PAGE = 50

SECTION_LABELS = {
    "new_application": "New Application",
    "approved": "Approved",
    "discontinued": "Discontinued",
}


def section_label(value: str) -> str:
    return SECTION_LABELS.get(value, value)


def phone_format(value: str) -> str:
    if not value or len(value) != 10 or not value.isdigit():
        return value or ""
    return f"({value[:3]}) {value[3:6]}-{value[6:]}"


templates.env.filters["section_label"] = section_label
templates.env.filters["phone_format"] = phone_format


def _filter_build_qs(params: dict) -> str:
    """Jinja2 filter: build a URL query string from a dict, handling list values.

    List values produce multiple ``key=val`` pairs (one per element).  Keys
    with empty-string or None values are omitted.

    Usage in templates::

        {% set pq = {"q": q, "endorsement": endorsement, ...} | build_qs %}
    """
    from urllib.parse import urlencode as _urlencode
    items: list[tuple[str, str]] = []
    for k, v in params.items():
        if isinstance(v, list):
            items.extend((k, vi) for vi in v if vi)
        elif v:
            items.append((k, str(v)))
    return _urlencode(items)


templates.env.filters["build_qs"] = _filter_build_qs


async def _tpl(request: Request, template: str, ctx: dict, status_code: int = 200):
    """Render a template with ``current_user`` injected into the context."""
    ctx.setdefault("current_user", await get_current_user(request))
    return templates.TemplateResponse(request, template, ctx, status_code=status_code)


# Inject shared template renderer into admin router (must precede first request).
admin_routes.init_router(_tpl)


async def _admin_redirect_handler(request: Request, exc: AdminRedirectException):
    return RedirectResponse(url=exc.location, status_code=302)


app.add_exception_handler(AdminRedirectException, _admin_redirect_handler)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Render HTML 404 (and other HTTP errors) instead of raw JSON."""
    if exc.status_code == 404:
        return await _tpl(request, "404.html", {"request": request}, status_code=404)
    # For other HTTP errors, return a simple styled page
    return HTMLResponse(
        f"<html><body style='font-family:sans-serif;padding:2rem'>"
        f"<h1>Error {exc.status_code}</h1><p>{exc.detail}</p>"
        f"<a href='/'>Back to Dashboard</a></body></html>",
        status_code=exc.status_code,
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Render HTML 404 for malformed path parameters (e.g. /record/abc)."""
    return await _tpl(
        request, "404.html", {"request": request, "message": "Invalid URL."},
        status_code=404,
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with get_db() as conn:
        stats = get_stats(conn)
    return await _tpl(request, "index.html", {"request": request, "stats": stats})


@app.get("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: list[str] = Query(default=[]),
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
    page: int = Query(1, ge=1),
):
    # City requires state context (names aren't unique across states).
    if not state:
        city = ""

    with get_db() as conn:
        records, total = search_records(
            conn,
            query=q,
            section_type=section_type,
            application_type=application_type,
            endorsements=endorsement,
            state=state,
            city=city,
            date_from=date_from,
            date_to=date_to,
            outcome_status=outcome_status,
            page=page,
        )
        filters = get_filter_options(conn)
        cities = get_cities_for_state(conn, state) if state else []

    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)

    # Build export URL with multi-value endorsement params.
    base_params = [
        ("q", q), ("section_type", section_type),
        ("application_type", application_type),
        ("state", state), ("city", city),
        ("date_from", date_from), ("date_to", date_to),
        ("outcome_status", outcome_status),
    ]
    export_params = urlencode(
        base_params + [("endorsement", e) for e in endorsement]
    )

    ctx = {
        "request": request,
        "records": records,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "filters": filters,
        "cities": cities,
        "q": q,
        "section_type": section_type,
        "application_type": application_type,
        "endorsement": endorsement,  # now a list
        "state": state,
        "city": city,
        "date_from": date_from,
        "date_to": date_to,
        "outcome_status": outcome_status,
        "export_url": f"/export?{export_params}",
    }

    if request.headers.get("HX-Request"):
        return await _tpl(request, "partials/results.html", ctx)

    return await _tpl(request, "search.html", ctx)


@app.get("/record/{record_id}", response_class=HTMLResponse)
async def record_detail(request: Request, record_id: int):
    with get_db() as conn:
        record = get_record_by_id(conn, record_id)
        if not record:
            return await _tpl(
                request, "404.html", {"request": request, "message": "Record not found."},
                status_code=404,
            )

        related_rows = get_related_records(conn, record["license_number"], record_id)

        # Hydrate record + related in a single batch
        hydrated = hydrate_records(conn, [record] + related_rows)
        record = hydrated[0]
        related = hydrated[1:]

        sources = get_record_sources(conn, record_id)
        provenance = summarize_provenance(sources)

        # Outcome link info for the detail page
        link = get_record_link(conn, record_id)
        outcome = format_outcome(get_outcome_status(record, link))
        reverse_link = get_reverse_link_info(conn, record)

    return await _tpl(request, "detail.html", {
        "request": request, "record": record,
        "related": related, "sources": sources,
        "provenance": provenance,
        "outcome": outcome, "reverse_link": reverse_link,
    })


@app.get("/source/{source_id}/record/{record_id}", response_class=HTMLResponse)
async def source_viewer(
    request: Request,
    source_id: int,
    record_id: int,
):
    """Return an HTMX partial showing the original archived source for a record.

    Extracts the raw ``<tbody>`` HTML for the record from its archival source
    file (full HTML snapshot or unified diff) and renders it inside an iframe
    with the original WSLCB inline styles.  Public endpoint — no auth required.
    """
    with get_db() as conn:
        # Verify both IDs exist and are linked.
        source_row = conn.execute(
            """SELECT s.id, st.slug AS source_type, st.label AS source_label,
                      s.snapshot_path, s.url, s.captured_at, s.metadata
               FROM sources s
               JOIN source_types st ON st.id = s.source_type_id
               WHERE s.id = ?""",
            (source_id,),
        ).fetchone()
        if source_row is None:
            raise HTTPException(status_code=404, detail="Source not found")

        record = get_record_by_id(conn, record_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Record not found")

        link_row = conn.execute(
            "SELECT 1 FROM record_sources WHERE record_id = ? AND source_id = ?",
            (record_id, source_id),
        ).fetchone()
        if link_row is None:
            raise HTTPException(status_code=404, detail="Source not linked to record")

    source = dict(source_row)
    raw_meta = source.get("metadata")
    source["metadata"] = json.loads(raw_meta) if raw_meta else {}

    tbody_html: str | None = None
    snapshot_path = source.get("snapshot_path")
    if snapshot_path:
        path = DATA_DIR / snapshot_path
        if source["source_type"] == "co_diff_archive":
            tbody_html = extract_tbody_from_diff(
                path,
                record["section_type"],
                record["license_number"],
                record["record_date"],
                record["application_type"],
            )
        else:
            tbody_html = extract_tbody_from_snapshot(
                path,
                record["section_type"],
                record["license_number"],
                record["record_date"],
                record["application_type"],
            )

    # Strip anchor tags from the tbody HTML before embedding — WSLCB pages can
    # contain <a href="..."> wrappers around cell values that are broken or
    # irrelevant inside the sandboxed iframe.  Text content is preserved.
    if tbody_html is not None:
        tbody_html = strip_anchor_tags(tbody_html)

    # Build the srcdoc attribute value server-side using html.escape() so that
    # all special characters (&, <, >, ", ') are correctly encoded for an HTML
    # attribute context — avoids the fragile Jinja2 filter chain.
    srcdoc_attr: str | None = None
    if tbody_html is not None:
        page_html = (
            "<!DOCTYPE html><html><head><meta charset='utf-8'><style>"
            "body{margin:0;padding:8px;background:#fff;"
            "font-family:Arial,sans-serif;font-size:.80em;}"
            "table{width:100%;border-collapse:collapse;}"
            "</style></head><body><table>"
            + tbody_html
            + "</table></body></html>"
        )
        srcdoc_attr = html.escape(page_html, quote=True)

    return await _tpl(request, "partials/source_viewer.html", {
        "request": request,
        "source": source,
        "record": record,
        "found": tbody_html is not None,
        "srcdoc_attr": srcdoc_attr,
    })


@app.get("/entity/{entity_id}", response_class=HTMLResponse)
async def entity_detail(request: Request, entity_id: int):
    with get_db() as conn:
        entity = get_entity_by_id(conn, entity_id)
        if not entity:
            return await _tpl(
                request, "404.html", {"request": request, "message": "Entity not found."},
                status_code=404,
            )
        records = get_entity_records(conn, entity_id)
        # Count distinct license numbers
        license_numbers = set(r["license_number"] for r in records)
    return await _tpl(request, "entity.html", {
        "request": request,
        "entity": entity,
        "records": records,
        "unique_licenses": len(license_numbers),
    })


@app.get("/api/cities")
async def api_cities(state: str = ""):
    """Return cities for a given state (for dynamic filter population)."""
    if not state or state not in US_STATES:
        return JSONResponse([], headers={"Cache-Control": "public, max-age=300"})
    with get_db() as conn:
        cities = get_cities_for_state(conn, state)
    return JSONResponse(cities, headers={"Cache-Control": "public, max-age=300"})


@app.get("/export")
async def export_csv(
    q: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: list[str] = Query(default=[]),
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
):
    """Export search results as CSV, streaming rows directly from the cursor."""
    if not state:
        city = ""

    fieldnames = [
        "section_type", "record_date", "business_name", "business_location",
        "address_line_1", "address_line_2", "applicants", "license_type",
        "endorsements", "application_type", "license_number", "contact_phone",
        "city", "state", "zip_code", "std_city", "std_region", "std_postal_code", "std_country",
        "previous_business_name", "previous_applicants",
        "previous_business_location",
        "prev_address_line_1", "prev_address_line_2",
        "prev_std_city", "prev_std_region", "prev_std_postal_code",
        "outcome_status", "outcome_date", "days_to_outcome",
    ]

    def _csv_generator():
        """Yield CSV rows incrementally from the database cursor."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()

        with get_db() as conn:
            for record in export_records_cursor(
                conn, query=q, section_type=section_type,
                application_type=application_type, endorsements=endorsement,
                state=state, city=city, date_from=date_from, date_to=date_to,
                outcome_status=outcome_status,
            ):
                buf.seek(0)
                buf.truncate(0)
                writer.writerow({k: record.get(k, "") or "" for k in fieldnames})
                yield buf.getvalue()

    return StreamingResponse(
        _csv_generator(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=wslcb_records.csv"},
    )


@app.get("/api/stats")
async def api_stats():
    with get_db() as conn:
        stats = get_stats(conn)
    if stats.get("date_range"):
        stats["date_range"] = list(stats["date_range"])
    if stats.get("last_scrape"):
        stats["last_scrape"] = dict(stats["last_scrape"])
    return stats
