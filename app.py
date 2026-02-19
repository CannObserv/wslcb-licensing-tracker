"""FastAPI web application for WSLCB licensing tracker."""
import csv
import io
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database import get_db, init_db, search_records, get_filter_options, get_stats, resolve_license_type, learn_license_type_mappings

app = FastAPI(title="WSLCB Licensing Tracker")
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


@app.on_event("startup")
def startup():
    init_db()
    # Seed license-type mappings from existing data
    with get_db() as conn:
        learned = learn_license_type_mappings(conn)
        if learned:
            print(f"Seeded {len(learned)} license-type mapping(s)")


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
    license_type: str = "",
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
            license_type=license_type,
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
        "license_type": license_type,
        "city": city,
        "date_from": date_from,
        "date_to": date_to,
    }

    # If HTMX request, return just the results partial
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/results.html", ctx)

    return templates.TemplateResponse("search.html", ctx)


@app.get("/record/{record_id}", response_class=HTMLResponse)
async def record_detail(request: Request, record_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM license_records WHERE id = ?", (record_id,)).fetchone()
        if not row:
            return HTMLResponse("Record not found", status_code=404)
        record = dict(row)
        record["license_type"] = resolve_license_type(conn, record.get("license_type", ""))
        # Get other records with same license number
        related_rows = conn.execute(
            "SELECT * FROM license_records WHERE license_number = ? AND id != ? ORDER BY record_date DESC",
            (row["license_number"], record_id),
        ).fetchall()
        related = []
        for r in related_rows:
            d = dict(r)
            d["license_type"] = resolve_license_type(conn, d.get("license_type", ""))
            related.append(d)
    return templates.TemplateResponse(
        "detail.html", {"request": request, "record": record, "related": related}
    )


@app.get("/export")
async def export_csv(
    q: str = "",
    section_type: str = "",
    application_type: str = "",
    license_type: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
):
    """Export search results as CSV."""
    with get_db() as conn:
        records, _ = search_records(
            conn, query=q, section_type=section_type,
            application_type=application_type, license_type=license_type,
            city=city, date_from=date_from, date_to=date_to,
            page=1, per_page=100000,
        )

    output = io.StringIO()
    if records:
        fieldnames = [
            "section_type", "record_date", "business_name", "business_location",
            "applicants", "license_type", "license_type_raw", "application_type",
            "license_number", "contact_phone", "city", "state", "zip_code",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

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
    # Convert Row objects
    if stats.get("date_range"):
        stats["date_range"] = dict(stats["date_range"]) if hasattr(stats["date_range"], "keys") else list(stats["date_range"])
    if stats.get("last_scrape"):
        stats["last_scrape"] = dict(stats["last_scrape"]) if hasattr(stats["last_scrape"], "keys") else None
    return stats
