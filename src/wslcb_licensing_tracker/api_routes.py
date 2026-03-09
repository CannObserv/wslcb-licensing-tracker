"""Versioned public API routes for the WSLCB licensing tracker.

All endpoints live under the /api/v1 prefix and return a consistent
JSON envelope::

    {"ok": true, "message": "<human-readable>", "data": { ... }}

The CSV export endpoint (/api/v1/export) is exempt from the envelope
— it returns a StreamingResponse with media_type text/csv.
"""
import csv
import io
import logging

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse

from .db import get_db, US_STATES
from .queries import (
    get_cities_for_state,
    get_stats,
    export_records_cursor,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["api"])


def _ok(data, message: str = "OK") -> JSONResponse:
    """Return a 200 envelope response."""
    return JSONResponse({"ok": True, "message": message, "data": data})


# ---------------------------------------------------------------------------
# GET /api/v1/cities
# ---------------------------------------------------------------------------

@router.get("/cities")
async def api_cities(state: str = ""):
    """Return cities for a given US state code.

    Used by the search form to populate the city dropdown dynamically.
    Returns an empty list for unknown or missing state codes.
    """
    if not state or state not in US_STATES:
        return JSONResponse(
            {"ok": True, "message": "No cities for state", "data": []},
            headers={"Cache-Control": "public, max-age=300"},
        )
    with get_db() as conn:
        cities = get_cities_for_state(conn, state)
    return JSONResponse(
        {"ok": True, "message": f"Cities for {state}", "data": cities},
        headers={"Cache-Control": "public, max-age=300"},
    )


# ---------------------------------------------------------------------------
# GET /api/v1/stats
# ---------------------------------------------------------------------------

@router.get("/stats")
async def api_stats():
    """Return aggregate statistics about the licensing record database."""
    with get_db() as conn:
        stats = get_stats(conn)
    if stats.get("date_range"):
        stats["date_range"] = list(stats["date_range"])
    else:
        stats["date_range"] = None
    if stats.get("last_scrape"):
        stats["last_scrape"] = dict(stats["last_scrape"])
    return _ok(stats, "Stats retrieved")


# ---------------------------------------------------------------------------
# GET /api/v1/health
# ---------------------------------------------------------------------------

@router.get("/health")
async def api_health():
    """Lightweight health check: verifies the process is alive and the DB is reachable.

    Returns HTTP 200 when healthy, HTTP 503 when the database cannot be
    reached.  No authentication required — this endpoint must be reachable
    by systemd and external uptime monitors.
    """
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
        return JSONResponse(
            {"ok": True, "message": "Healthy", "data": {"db": "ok"}},
            status_code=200,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Health check failed: %s", exc)
        return JSONResponse(
            {
                "ok": False,
                "message": "Database unreachable",
                "data": {"db": "error", "detail": str(exc)},
            },
            status_code=503,
        )


# ---------------------------------------------------------------------------
# GET /api/v1/export
# ---------------------------------------------------------------------------

_EXPORT_FIELDNAMES = [
    "section_type", "record_date", "business_name", "business_location",
    "address_line_1", "address_line_2", "applicants", "license_type",
    "endorsements", "application_type", "license_number", "contact_phone",
    "city", "state", "zip_code", "std_city", "std_region", "std_postal_code",
    "std_country",
    "previous_business_name", "previous_applicants",
    "previous_business_location",
    "prev_address_line_1", "prev_address_line_2",
    "prev_std_city", "prev_std_region", "prev_std_postal_code",
    "outcome_status", "outcome_date", "days_to_outcome",
]


@router.get("/export")
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
    """Stream search results as a CSV file.

    Accepts the same filter parameters as the search form.  Rows are
    yielded directly from the SQLite cursor to keep memory usage flat
    regardless of result set size.
    """
    if not state:
        city = ""

    def _csv_generator():
        """Yield CSV rows incrementally from the database cursor."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=_EXPORT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()

        with get_db() as conn:
            for record in export_records_cursor(
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
            ):
                buf.seek(0)
                buf.truncate(0)
                writer.writerow({k: record.get(k, "") or "" for k in _EXPORT_FIELDNAMES})
                yield buf.getvalue()

    return StreamingResponse(
        _csv_generator(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=wslcb_records.csv"},
    )
