"""FastAPI web application for WSLCB licensing tracker."""
import csv
import io
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlencode

from fastapi import FastAPI, Request, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from database import get_db, init_db
from entities import backfill_entities, get_entity_by_id
from queries import (
    search_records, export_records,
    get_filter_options, get_cities_for_state, US_STATES,
    get_stats,
    get_record_by_id, get_related_records, get_entity_records,
    get_record_sources, get_record_link,
    hydrate_records,
)
from endorsements import seed_endorsements, backfill, repair_code_name_endorsements, merge_mixed_case_endorsements
from link_records import build_all_links, get_reverse_link_info, get_outcome_status
from display import format_outcome, summarize_provenance
from log_config import setup_logging

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


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Render HTML 404 (and other HTTP errors) instead of raw JSON."""
    if exc.status_code == 404:
        return templates.TemplateResponse(
            "404.html", {"request": request}, status_code=404,
        )
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
    return templates.TemplateResponse(
        "404.html", {"request": request, "message": "Invalid URL."},
        status_code=404,
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with get_db() as conn:
        stats = get_stats(conn)
    return templates.TemplateResponse(
        "index.html", {"request": request, "stats": stats}
    )


@app.get("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",
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
            endorsement=endorsement,
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

    export_params = urlencode({
        "q": q, "section_type": section_type,
        "application_type": application_type, "endorsement": endorsement,
        "state": state, "city": city,
        "date_from": date_from, "date_to": date_to,
        "outcome_status": outcome_status,
    })

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
        "endorsement": endorsement,
        "state": state,
        "city": city,
        "date_from": date_from,
        "date_to": date_to,
        "outcome_status": outcome_status,
        "export_url": f"/export?{export_params}",
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/results.html", ctx)

    return templates.TemplateResponse("search.html", ctx)


@app.get("/record/{record_id}", response_class=HTMLResponse)
async def record_detail(request: Request, record_id: int):
    with get_db() as conn:
        record = get_record_by_id(conn, record_id)
        if not record:
            return templates.TemplateResponse(
                "404.html", {"request": request, "message": "Record not found."},
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

    return templates.TemplateResponse(
        "detail.html", {
            "request": request, "record": record,
            "related": related, "sources": sources,
            "provenance": provenance,
            "outcome": outcome, "reverse_link": reverse_link,
        }
    )


@app.get("/entity/{entity_id}", response_class=HTMLResponse)
async def entity_detail(request: Request, entity_id: int):
    with get_db() as conn:
        entity = get_entity_by_id(conn, entity_id)
        if not entity:
            return templates.TemplateResponse(
                "404.html", {"request": request, "message": "Entity not found."},
                status_code=404,
            )
        records = get_entity_records(conn, entity_id)
        # Count distinct license numbers
        license_numbers = set(r["license_number"] for r in records)
    return templates.TemplateResponse(
        "entity.html", {
            "request": request,
            "entity": entity,
            "records": records,
            "unique_licenses": len(license_numbers),
        }
    )


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
    endorsement: str = "",
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
):
    """Export search results as CSV."""
    if not state:
        city = ""
    with get_db() as conn:
        records = export_records(
            conn, query=q, section_type=section_type,
            application_type=application_type, endorsement=endorsement,
            state=state, city=city, date_from=date_from, date_to=date_to,
            outcome_status=outcome_status,
        )

    if not records:
        return HTMLResponse(
            "<html><body style='font-family:sans-serif;padding:2rem'>"
            "<h2>No records to export</h2>"
            "<p>Your search returned no results. "
            "<a href='/search'>Go back to search</a> and adjust your filters.</p>"
            "</body></html>",
            status_code=200,
        )

    fieldnames = [
        "section_type", "record_date", "business_name", "business_location",
        "address_line_1", "address_line_2", "applicants", "license_type",
        "endorsements", "application_type", "license_number", "contact_phone",
        "city", "state", "zip_code", "std_city", "std_state", "std_zip",
        "previous_business_name", "previous_applicants",
        "previous_business_location",
        "prev_address_line_1", "prev_address_line_2",
        "prev_std_city", "prev_std_state", "prev_std_zip",
        "outcome_status", "outcome_date", "days_to_outcome",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in records:
        row = {k: r.get(k, "") or "" for k in fieldnames}
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
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
