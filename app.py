"""FastAPI web application for WSLCB licensing tracker."""
import csv
import io
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database import (
    get_db, init_db, search_records, get_filter_options, get_stats,
    get_record_by_id, get_related_records, backfill_entities,
    get_entity_by_id, get_entity_records, get_record_entities,
)
from endorsements import (
    seed_endorsements, backfill, get_record_endorsements,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database and endorsement tables on startup."""
    init_db()
    with get_db() as conn:
        n = seed_endorsements(conn)
        if n:
            print(f"Seeded {n} endorsement code mapping(s)")
        processed = backfill(conn)
        if processed:
            print(f"Backfilled endorsements for {processed} record(s)")
        entity_count = backfill_entities(conn)
        if entity_count:
            print(f"Backfilled entities for {entity_count} record(s)")
    yield


app = FastAPI(title="WSLCB Licensing Tracker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

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


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with get_db() as conn:
        stats = get_stats(conn)
        filters = get_filter_options(conn)
    return templates.TemplateResponse(
        "index.html", {"request": request, "stats": stats, "filters": filters}
    )


@app.get("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",
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
            city=city,
            date_from=date_from,
            date_to=date_to,
            page=page,
        )
        filters = get_filter_options(conn)

    per_page = 50
    total_pages = max(1, (total + per_page - 1) // per_page)

    ctx = {
        "request": request,
        "records": records,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "filters": filters,
        "q": q,
        "section_type": section_type,
        "application_type": application_type,
        "endorsement": endorsement,
        "city": city,
        "date_from": date_from,
        "date_to": date_to,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/results.html", ctx)

    return templates.TemplateResponse("search.html", ctx)


@app.get("/record/{record_id}", response_class=HTMLResponse)
async def record_detail(request: Request, record_id: int):
    with get_db() as conn:
        record = get_record_by_id(conn, record_id)
        if not record:
            return HTMLResponse("Record not found", status_code=404)

        related = get_related_records(conn, record["license_number"], record_id)

        # Attach endorsements and entities to record + related
        all_ids = [record["id"]] + [r["id"] for r in related]
        emap = get_record_endorsements(conn, all_ids)
        entmap = get_record_entities(conn, all_ids)
        record["endorsements"] = emap.get(record["id"], [])
        record["entities"] = entmap.get(record["id"], {"applicant": [], "previous_applicant": []})
        for r in related:
            r["endorsements"] = emap.get(r["id"], [])
            r["entities"] = entmap.get(r["id"], {"applicant": [], "previous_applicant": []})

    return templates.TemplateResponse(
        "detail.html", {"request": request, "record": record, "related": related}
    )


@app.get("/entity/{entity_id}", response_class=HTMLResponse)
async def entity_detail(request: Request, entity_id: int):
    with get_db() as conn:
        entity = get_entity_by_id(conn, entity_id)
        if not entity:
            return HTMLResponse("Entity not found", status_code=404)
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


@app.get("/export")
async def export_csv(
    q: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
):
    """Export search results as CSV."""
    with get_db() as conn:
        records, _ = search_records(
            conn, query=q, section_type=section_type,
            application_type=application_type, endorsement=endorsement,
            city=city, date_from=date_from, date_to=date_to,
            page=1, per_page=100000,
        )

    output = io.StringIO()
    if records:
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
