"""FastAPI web application for WSLCB licensing tracker.

Public routes live here.  Admin routes (/admin/*) are in admin_routes.py,
registered as an APIRouter and included at startup.
"""

import html
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import admin_routes, api_routes
from .admin_auth import AdminRedirectException, get_current_user
from .db import DATA_DIR, get_db
from .display import format_outcome, summarize_provenance
from .endorsements import (
    backfill,
    merge_mixed_case_endorsements,
    repair_code_name_endorsements,
    seed_endorsements,
)
from .entities import backfill_entities, get_entity_by_id
from .link_records import build_all_links, get_outcome_status, get_reverse_link_info
from .log_config import setup_logging
from .parser import extract_tbody_from_diff, extract_tbody_from_snapshot, strip_anchor_tags
from .queries import (
    get_cities_for_state,
    get_entities,
    get_entity_records,
    get_filter_options,
    get_record_by_id,
    get_record_link,
    get_record_sources,
    get_related_records,
    get_stats,
    hydrate_records,
    search_records,
)
from .schema import init_db

logger = logging.getLogger(__name__)

_PHONE_LENGTH = 10
_HTTP_404 = 404


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
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
        existing_links = conn.execute("SELECT COUNT(*) FROM record_links").fetchone()[0]
        if not existing_links:
            link_stats = build_all_links(conn)
            if link_stats["total"]:
                logger.info(
                    "Record linking: %d links (%d high, %d medium)",
                    link_stats["total"],
                    link_stats["high"],
                    link_stats["medium"],
                )
    yield


app = FastAPI(title="WSLCB Licensing Tracker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(admin_routes.router)
app.include_router(api_routes.router)
templates = Jinja2Templates(directory="templates")

PER_PAGE = 50

SECTION_LABELS = {
    "new_application": "New Application",
    "approved": "Approved",
    "discontinued": "Discontinued",
}


def section_label(value: str) -> str:
    """Return a human-readable label for a section type slug."""
    return SECTION_LABELS.get(value, value)


def phone_format(value: str) -> str:
    """Format a 10-digit phone number as (NXX) NXX-XXXX."""
    if not value or len(value) != _PHONE_LENGTH or not value.isdigit():
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
    items: list[tuple[str, str]] = []
    for k, v in params.items():
        if isinstance(v, list):
            items.extend((k, vi) for vi in v if vi)
        elif v:
            items.append((k, str(v)))
    return urlencode(items)


templates.env.filters["build_qs"] = _filter_build_qs


async def _tpl(
    request: Request,
    template: str,
    ctx: dict,
    status_code: int = 200,
) -> HTMLResponse:
    """Render a template with ``current_user`` injected into the context."""
    ctx.setdefault("current_user", await get_current_user(request))
    return templates.TemplateResponse(request, template, ctx, status_code=status_code)


# Inject shared template renderer into admin router (must precede first request).
admin_routes.init_router(_tpl)


async def _admin_redirect_handler(
    _request: Request,
    exc: AdminRedirectException,
) -> RedirectResponse:
    """Redirect to login page on AdminRedirectException."""
    return RedirectResponse(url=exc.location, status_code=302)


app.add_exception_handler(AdminRedirectException, _admin_redirect_handler)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(
    request: Request,
    exc: StarletteHTTPException,
) -> HTMLResponse:
    """Render HTML 404 (and other HTTP errors) instead of raw JSON."""
    if exc.status_code == _HTTP_404:
        return await _tpl(request, "404.html", {"request": request}, status_code=_HTTP_404)
    # For other HTTP errors, return a simple styled page
    return HTMLResponse(
        f"<html><body style='font-family:sans-serif;padding:2rem'>"
        f"<h1>Error {exc.status_code}</h1><p>{exc.detail}</p>"
        f"<a href='/'>Back to Dashboard</a></body></html>",
        status_code=exc.status_code,
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request,
    _exc: RequestValidationError,
) -> HTMLResponse:
    """Render HTML 404 for malformed path parameters (e.g. /record/abc)."""
    return await _tpl(
        request,
        "404.html",
        {"request": request, "message": "Invalid URL."},
        status_code=_HTTP_404,
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Render the dashboard index page."""
    with get_db() as conn:
        stats = get_stats(conn)
    return await _tpl(request, "index.html", {"request": request, "stats": stats})


@app.get("/search", response_class=HTMLResponse)
async def search(  # noqa: PLR0913
    request: Request,
    q: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: Annotated[list[str], Query()] = [],  # noqa: B006
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
    page: Annotated[int, Query(ge=1)] = 1,
) -> HTMLResponse:
    """Search and filter license records."""
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
        ("q", q),
        ("section_type", section_type),
        ("application_type", application_type),
        ("state", state),
        ("city", city),
        ("date_from", date_from),
        ("date_to", date_to),
        ("outcome_status", outcome_status),
    ]
    export_params = urlencode(base_params + [("endorsement", e) for e in endorsement])

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
        "export_url": f"/api/v1/export?{export_params}",
    }

    if request.headers.get("HX-Request"):
        return await _tpl(request, "partials/results.html", ctx)

    return await _tpl(request, "search.html", ctx)


@app.get("/record/{record_id}", response_class=HTMLResponse)
async def record_detail(request: Request, record_id: int) -> HTMLResponse:
    """Render the detail page for a single license record."""
    with get_db() as conn:
        record = get_record_by_id(conn, record_id)
        if not record:
            return await _tpl(
                request,
                "404.html",
                {"request": request, "message": "Record not found."},
                status_code=_HTTP_404,
            )

        related_rows = get_related_records(conn, record["license_number"], record_id)

        # Hydrate record + related in a single batch
        hydrated = hydrate_records(conn, [record, *related_rows])
        record = hydrated[0]
        related = hydrated[1:]

        sources = get_record_sources(conn, record_id)
        provenance = summarize_provenance(sources)

        # Outcome link info for the detail page
        link = get_record_link(conn, record_id)
        outcome = format_outcome(get_outcome_status(record, link))
        reverse_link = get_reverse_link_info(conn, record)

    return await _tpl(
        request,
        "detail.html",
        {
            "request": request,
            "record": record,
            "related": related,
            "sources": sources,
            "provenance": provenance,
            "outcome": outcome,
            "reverse_link": reverse_link,
        },
    )


@app.get("/source/{source_id}/record/{record_id}", response_class=HTMLResponse)
async def source_viewer(
    request: Request,
    source_id: int,
    record_id: int,
) -> HTMLResponse:
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
            "</style></head><body><table>" + tbody_html + "</table></body></html>"
        )
        srcdoc_attr = html.escape(page_html, quote=True)

    return await _tpl(
        request,
        "partials/source_viewer.html",
        {
            "request": request,
            "source": source,
            "record": record,
            "found": tbody_html is not None,
            "srcdoc_attr": srcdoc_attr,
        },
    )


@app.get("/entities", response_class=HTMLResponse)
async def entities_list(
    request: Request,
    q: str = "",
    entity_type: Annotated[str, Query(alias="type")] = "",
    sort: str = "count",
    page: int = 1,
) -> HTMLResponse:
    """Searchable, paginated list of all applicant entities."""
    with get_db() as conn:
        result = get_entities(
            conn,
            q=q.strip() or None,
            entity_type=entity_type.strip() or None,
            sort=sort,
            page=page,
            per_page=50,
        )

    total_pages = max(1, (result["total"] + 49) // 50)
    ctx = {
        "request": request,
        "entities": result["entities"],
        "total": result["total"],
        "q": q,
        "entity_type": entity_type,
        "sort": sort,
        "page": page,
        "total_pages": total_pages,
    }
    if request.headers.get("HX-Request"):
        return await _tpl(request, "partials/entities_results.html", ctx)
    return await _tpl(request, "entities.html", ctx)


@app.get("/entity/{entity_id}", response_class=HTMLResponse)
async def entity_detail(request: Request, entity_id: int) -> HTMLResponse:
    """Render the detail page for a single entity."""
    with get_db() as conn:
        entity = get_entity_by_id(conn, entity_id)
        if not entity:
            return await _tpl(
                request,
                "404.html",
                {"request": request, "message": "Entity not found."},
                status_code=_HTTP_404,
            )
        records = get_entity_records(conn, entity_id)
        # Count distinct license numbers
        license_numbers = {r["license_number"] for r in records}
    return await _tpl(
        request,
        "entity.html",
        {
            "request": request,
            "entity": entity,
            "records": records,
            "unique_licenses": len(license_numbers),
        },
    )
