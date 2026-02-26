"""FastAPI web application for WSLCB licensing tracker."""
import csv
import io
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlencode

from fastapi import FastAPI, Request, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from database import get_db, init_db
from entities import backfill_entities, get_entity_by_id
from queries import (
    search_records, get_filter_options, get_cities_for_state, get_stats,
    get_record_by_id, get_related_records, get_entity_records,
    _hydrate_records,
)
from endorsements import seed_endorsements, backfill, repair_code_name_endorsements
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
        processed = backfill(conn)
        if processed:
            logger.info("Backfilled endorsements for %d record(s)", processed)
        entity_count = backfill_entities(conn)
        if entity_count:
            logger.info("Backfilled entities for %d record(s)", entity_count)
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
    page: int = Query(1, ge=1),
):
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
        hydrated = _hydrate_records(conn, [record] + related_rows)
        record = hydrated[0]
        related = hydrated[1:]

    return templates.TemplateResponse(
        "detail.html", {"request": request, "record": record, "related": related}
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
    if not state:
        return []
    with get_db() as conn:
        return get_cities_for_state(conn, state)


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
):
    """Export search results as CSV."""
    with get_db() as conn:
        records, total = search_records(
            conn, query=q, section_type=section_type,
            application_type=application_type, endorsement=endorsement,
            state=state, city=city, date_from=date_from, date_to=date_to,
            page=1, per_page=100_000,
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
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in records:
        row = {k: r.get(k, "") for k in fieldnames}
        row["endorsements"] = "; ".join(r.get("endorsements", []))
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
