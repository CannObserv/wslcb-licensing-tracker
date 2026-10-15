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
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .config import get_build_id
from .db import US_STATES
from .engine import get_db
from .queries_export import export_records_cursor
from .queries_filter import get_cities_for_state
from .queries_stats import get_stats
from .scraper import scrape_freshness

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["api"])


async def _get_db(request: Request) -> AsyncGenerator[AsyncConnection, None]:
    """FastAPI dependency yielding an AsyncConnection from the shared engine pool."""
    async with get_db(request.app.state.engine) as conn:
        yield conn


Conn = Annotated[AsyncConnection, Depends(_get_db)]


def _ok(
    data: object,
    message: str = "OK",
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Return a 200 envelope response.

    Runs the payload through ``jsonable_encoder`` first — ``JSONResponse``
    renders with plain ``json.dumps``, which chokes on the tz-aware datetimes
    that reach /stats via the scrape_log row (#170).

    Every envelope-producing route builds its response here or in ``_fail``,
    ``headers`` included, so no endpoint hand-rolls JSONResponse and drops back
    to raw ``json.dumps``.
    """
    return JSONResponse(
        {"ok": True, "message": message, "data": jsonable_encoder(data)},
        headers=headers,
    )


def _fail(data: object, message: str, status_code: int = 503) -> JSONResponse:
    """Return an ``ok: false`` envelope response, encoded as ``_ok`` encodes."""
    return JSONResponse(
        {"ok": False, "message": message, "data": jsonable_encoder(data)},
        status_code=status_code,
    )


# ---------------------------------------------------------------------------
# GET /api/v1/cities
# ---------------------------------------------------------------------------


@router.get("/cities")
async def api_cities(
    state: str = "",
    conn: Conn = ...,  # injected by FastAPI
) -> JSONResponse:
    """Return cities for a given US state code.

    Used by the search form to populate the city dropdown dynamically.
    Returns an empty list for unknown or missing state codes.
    """
    cache = {"Cache-Control": "public, max-age=300"}
    if not state or state not in US_STATES:
        return _ok([], "No cities for state", headers=cache)
    cities = await get_cities_for_state(conn, state)
    return _ok(cities, f"Cities for {state}", headers=cache)


# ---------------------------------------------------------------------------
# GET /api/v1/stats
# ---------------------------------------------------------------------------


@router.get("/stats")
async def api_stats(
    conn: Conn = ...,  # injected by FastAPI
) -> JSONResponse:
    """Return aggregate statistics about the licensing record database."""
    stats = await get_stats(conn)
    # An empty corpus yields (None, None) — a truthy tuple — so test it by
    # content, not truthiness. A populated tuple needs no conversion here:
    # _ok()'s encoder renders it as a JSON list.
    if not any(stats.get("date_range") or ()):
        stats["date_range"] = None
    if stats.get("last_scrape"):
        stats["last_scrape"] = dict(stats["last_scrape"])
    return _ok(stats, "Stats retrieved")


# ---------------------------------------------------------------------------
# GET /api/v1/health
# ---------------------------------------------------------------------------


def _db_unreachable(exc: Exception) -> JSONResponse:
    """The 503 both health routes return when the database can't be reached."""
    logger.warning("Health check failed: %s", exc)
    # The health routes are unauthenticated by design (systemd + uptime monitors),
    # so the body carries a fixed string: driver errors render host/port/user/database
    # into their text. The real cause goes to the log line above.
    return _fail({"db": "error", "detail": "database unreachable"}, "Database unreachable")


async def _read_freshness(conn: AsyncConnection) -> dict | None:
    """scrape_freshness(), or None when scrape_log can't be read (logged)."""
    try:
        return await scrape_freshness(conn)
    except Exception as exc:  # noqa: BLE001 — a degraded report, not a dead process
        logger.warning("Health check could not read scrape freshness: %s", exc)
        return None


@router.get("/health")
async def api_health(request: Request) -> JSONResponse:
    """Liveness check: verifies the process is alive and the DB is reachable.

    Returns HTTP 200 when healthy, HTTP 503 when the database cannot be
    reached.  No authentication required — this endpoint must be reachable
    by systemd and external uptime monitors.

    ``data.scrape`` reports scrape freshness (#197), or null when it can't be
    read. Stale data never fails the check: ``wslcb-healthcheck`` restarts the
    web app on any non-2xx, and a restart can't fix an upstream outage.
    ``/health/ingest`` is the check that fails on stale data.
    """
    try:
        async with get_db(request.app.state.engine) as conn:
            await conn.execute(text("SELECT 1"))
            scrape = await _read_freshness(conn)
    except Exception as exc:  # noqa: BLE001
        return _db_unreachable(exc)
    if scrape is None:
        message = "Healthy; scrape freshness unknown"
    elif scrape["stale"]:
        message = "Healthy; scrape stale"
    else:
        message = "Healthy"
    return _ok({"db": "ok", "build": get_build_id(), "scrape": scrape}, message)


@router.get("/health/ingest")
async def api_health_ingest(request: Request) -> JSONResponse:
    """Readiness of the data: HTTP 503 when the scrape is stale (#197).

    For external uptime checks that should alarm on an ingest outage. Never
    point ``wslcb-healthcheck`` here — it would restart the web app for the
    whole of an outage the restart can't fix. A ``scrape_log`` read failure is
    also a 503, reported apart from an unreachable database.
    """
    try:
        async with get_db(request.app.state.engine) as conn:
            await conn.execute(text("SELECT 1"))
            scrape = await _read_freshness(conn)
    except Exception as exc:  # noqa: BLE001
        return _db_unreachable(exc)
    if scrape is None:
        return _fail(
            {"db": "ok", "detail": "scrape freshness unavailable"},
            "Scrape freshness unavailable",
        )
    if scrape["stale"]:
        return _fail(scrape, "Scrape stale")
    return _ok(scrape, "Scrape fresh")


# ---------------------------------------------------------------------------
# GET /api/v1/export
# ---------------------------------------------------------------------------

_EXPORT_FIELDNAMES = [
    "section_type",
    "record_date",
    "business_name",
    "business_location",
    "std_address_line_1",
    "std_address_line_2",
    "applicants",
    "license_type",
    "endorsements",
    "application_type",
    "license_number",
    "contact_phone",
    "city",
    "state",
    "zip_code",
    "std_city",
    "std_region",
    "std_postal_code",
    "std_country",
    "previous_business_name",
    "previous_applicants",
    "previous_business_location",
    "prev_std_address_line_1",
    "prev_std_address_line_2",
    "prev_std_city",
    "prev_std_region",
    "prev_std_postal_code",
    "outcome_status",
    "outcome_date",
    "days_to_outcome",
]


@router.get("/export")
async def export_csv(  # noqa: PLR0913
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
) -> StreamingResponse:
    """Stream search results as a CSV file.

    Accepts the same filter parameters as the search form.  Rows are
    yielded directly from the PostgreSQL cursor to keep memory usage flat
    regardless of result set size.
    """
    if not state:
        city = ""

    async def _async_csv_generator() -> AsyncGenerator[str, None]:
        """Yield CSV rows incrementally from the database cursor."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=_EXPORT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()

        async with get_db(request.app.state.engine) as conn:
            async for record in export_records_cursor(
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
        _async_csv_generator(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=wslcb_records.csv"},
    )
