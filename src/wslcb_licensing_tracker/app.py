"""FastAPI web application for WSLCB licensing tracker.

Public routes live here.  Admin routes (/admin/*) are in admin_routes.py,
registered as an APIRouter and included at startup.
"""

import html
import json
import logging
import shutil
import subprocess
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from . import admin_routes, api_routes, pg_address_validator
from .admin_auth import AdminRedirectException, get_current_user
from .data_migration import run_pending_migrations
from .database import create_engine_from_env, get_db
from .display import format_outcome, summarize_provenance
from .log_config import setup_logging
from .parser import extract_tbody_from_diff, extract_tbody_from_snapshot, strip_anchor_tags
from .pg_db import DATA_DIR, get_record_sources
from .pg_entities import get_entity_by_id
from .pg_link_records import get_outcome_status, get_reverse_link_info
from .pg_queries_entity import get_entities, get_entity_records
from .pg_queries_filter import get_cities_for_state, get_filter_options
from .pg_queries_hydrate import hydrate_records
from .pg_queries_search import (
    get_record_by_id,
    get_record_link,
    get_record_source_link,
    get_related_records,
    get_source_by_id,
    search_records,
)
from .pg_queries_stats import get_stats

logger = logging.getLogger(__name__)

_PHONE_LENGTH = 10
_HTTP_404 = 404


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize async engine and run pending data migrations on startup."""
    setup_logging()
    engine = create_engine_from_env()
    _app.state.engine = engine
    await run_pending_migrations(engine)
    yield
    await engine.dispose()
    await pg_address_validator.close_shared_client()


app = FastAPI(title="WSLCB Licensing Tracker", lifespan=lifespan)


async def get_db_dep(request: Request) -> AsyncGenerator[AsyncConnection, None]:
    """FastAPI dependency yielding an AsyncConnection from the shared engine pool."""
    async with get_db(request.app.state.engine) as conn:
        yield conn


class _StaticCacheMiddleware(BaseHTTPMiddleware):
    """Add long-lived Cache-Control headers to all /static/ responses."""

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]  # noqa: ANN001
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "public, max-age=31536000"
        return response


app.add_middleware(_StaticCacheMiddleware)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(admin_routes.router)
app.include_router(api_routes.router)
templates = Jinja2Templates(directory="templates")


def _get_css_version() -> str:
    """Return short git SHA for cache-busting static assets. Falls back to 'dev'."""
    git = shutil.which("git")
    if not git:
        return "dev"
    try:
        return subprocess.run(  # noqa: S603
            [git, "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "dev"


_CSS_VERSION = _get_css_version()
templates.env.globals["css_version"] = _CSS_VERSION

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


def _filter_ts(value: datetime | str | None) -> str:
    """Jinja2 filter: convert a datetime (or ISO string) to ISO 8601 string.

    Returns an empty string for None.  Passes strings through unchanged so
    that templates work correctly whether the DB returns datetime objects or
    plain text (e.g. during tests with mocked data).
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


templates.env.filters["ts"] = _filter_ts


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
    async with get_db(request.app.state.engine) as conn:
        stats = await get_stats(conn)
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

    async with get_db(request.app.state.engine) as conn:
        records, total = await search_records(
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
        filters = await get_filter_options(conn)
        cities = await get_cities_for_state(conn, state) if state else []

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
    async with get_db(request.app.state.engine) as conn:
        record = await get_record_by_id(conn, record_id)
        if not record:
            return await _tpl(
                request,
                "404.html",
                {"request": request, "message": "Record not found."},
                status_code=_HTTP_404,
            )

        # API difference: pg_queries.get_related_records takes (conn, record: dict)
        related_rows = await get_related_records(conn, record)

        # Hydrate record + related in a single batch
        hydrated = await hydrate_records(conn, [record, *related_rows])
        record = hydrated[0]
        related = hydrated[1:]

        sources = await get_record_sources(conn, record_id)
        provenance = summarize_provenance(sources)

        link = await get_record_link(conn, record_id)
        outcome = format_outcome(get_outcome_status(record, link))
        reverse_link = await get_reverse_link_info(conn, record)

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
    async with get_db(request.app.state.engine) as conn:
        # 1. Look up source row
        source_row = await get_source_by_id(conn, source_id)
        if source_row is None:
            raise HTTPException(status_code=404, detail="Source not found")

        # 2. Look up record
        record = await get_record_by_id(conn, record_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Record not found")

        # 3. Verify record_sources link
        if not await get_record_source_link(conn, record_id, source_id):
            raise HTTPException(status_code=404, detail="Source not linked to record")

    source = source_row
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
    async with get_db(request.app.state.engine) as conn:
        result = await get_entities(
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
    async with get_db(request.app.state.engine) as conn:
        entity = await get_entity_by_id(conn, entity_id)
        if not entity:
            return await _tpl(
                request,
                "404.html",
                {"request": request, "message": "Entity not found."},
                status_code=_HTTP_404,
            )
        records = await get_entity_records(conn, entity_id)
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
