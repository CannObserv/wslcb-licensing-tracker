"""FastAPI web application for WSLCB licensing tracker."""
import csv
import io
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Form, HTTPException, Request, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from database import get_db, init_db
from admin_auth import get_current_user, require_admin, AdminRedirectException
from admin_audit import get_audit_log, log_action
from entities import backfill_entities, get_entity_by_id
from queries import (
    search_records, export_records,
    get_filter_options, get_cities_for_state, US_STATES,
    get_stats,
    get_record_by_id, get_related_records, get_entity_records,
    get_record_sources, get_record_link,
    hydrate_records,
)
from integrity import (
    check_orphaned_locations,
    check_unenriched_records,
    check_endorsement_anomalies,
)
from endorsements import (
    seed_endorsements, backfill, repair_code_name_endorsements,
    merge_mixed_case_endorsements,
    set_canonical_endorsement, rename_endorsement,
    get_endorsement_list, get_code_mappings,
    suggest_duplicate_endorsements, dismiss_suggestion,
    add_code_mapping, remove_code_mapping, create_code,
    reprocess_endorsements,
)
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


async def _tpl(request: Request, template: str, ctx: dict, status_code: int = 200):
    """Render a template with ``current_user`` injected into the context."""
    ctx.setdefault("current_user", await get_current_user(request))
    return templates.TemplateResponse(template, ctx, status_code=status_code)


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
        "city", "state", "zip_code", "std_city", "std_region", "std_postal_code", "std_country",
        "previous_business_name", "previous_applicants",
        "previous_business_location",
        "prev_address_line_1", "prev_address_line_2",
        "prev_std_city", "prev_std_region", "prev_std_postal_code",
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


# -- Admin routes -------------------------------------------------------

@app.get("/admin/", response_class=HTMLResponse)
async def admin_dashboard(request: Request, admin: dict = Depends(require_admin)):
    """Admin dashboard — record counts, scrape status, data quality metrics."""
    with get_db() as conn:
        # Record counts
        agg = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN section_type = 'new_application' THEN 1 ELSE 0 END) AS new_apps,
                SUM(CASE WHEN section_type = 'approved' THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN section_type = 'discontinued' THEN 1 ELSE 0 END) AS discontinued
            FROM license_records
        """).fetchone()
        recent = conn.execute("""
            SELECT
                SUM(CASE WHEN created_at >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS last_24h,
                SUM(CASE WHEN created_at >= datetime('now', '-7 days') THEN 1 ELSE 0 END) AS last_7d
            FROM license_records
        """).fetchone()
        # Last 5 scrape runs (with computed duration in seconds)
        scrapes_raw = conn.execute("""
            SELECT id, status, records_new, records_approved, records_discontinued,
                   records_skipped, started_at, finished_at,
                   ROUND(
                       (julianday(finished_at) - julianday(started_at)) * 86400
                   ) AS duration_secs
            FROM scrape_log
            ORDER BY id DESC LIMIT 5
        """).fetchall()
        scrapes = [dict(r) for r in scrapes_raw]
        # Data quality
        orphans = check_orphaned_locations(conn)
        unenriched = check_unenriched_records(conn)
        endorsement_issues = check_endorsement_anomalies(conn)
        user_count = conn.execute("SELECT COUNT(*) FROM admin_users").fetchone()[0]

    return await _tpl(request, "admin/dashboard.html", {
        "request": request,
        "admin": admin,
        "active_section": "dashboard",
        "record_counts": {
            "total": agg["total"],
            "new_apps": agg["new_apps"],
            "approved": agg["approved"],
            "discontinued": agg["discontinued"],
            "last_24h": recent["last_24h"] or 0,
            "last_7d": recent["last_7d"] or 0,
        },
        "scrapes": scrapes,
        "data_quality": {
            "orphaned_locations": orphans["count"],
            "no_endorsements": unenriched["no_endorsements"],
            "no_entities": unenriched["no_entities"],
            "unresolved_codes": endorsement_issues["unresolved_codes"],
            "placeholder_endorsements": endorsement_issues["placeholder_endorsements"],
        },
        "user_count": user_count,
    })


@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request, admin: dict = Depends(require_admin)):
    """List all admin users."""
    with get_db() as conn:
        users = conn.execute(
            "SELECT id, email, role, created_at, created_by FROM admin_users ORDER BY created_at"
        ).fetchall()
    return await _tpl(request, "admin/users.html", {
        "request": request,
        "admin": admin,
        "active_section": "users",
        "users": [dict(u) for u in users],
        "error": request.query_params.get("error", ""),
    })


@app.post("/admin/users/add", response_class=HTMLResponse)
async def admin_users_add(
    request: Request,
    admin: dict = Depends(require_admin),
    email: str = Form(...),
):
    """Add a new admin user."""
    email = email.strip().lower()
    if not email:
        return RedirectResponse(
            "/admin/users?" + urlencode({"error": "Email is required"}), status_code=303
        )
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM admin_users WHERE email = ? COLLATE NOCASE", (email,)
        ).fetchone()
        if existing:
            return RedirectResponse(
                "/admin/users?" + urlencode({"error": f"User {email} already exists"}),
                status_code=303,
            )
        conn.execute(
            "INSERT INTO admin_users (email, role, created_by) VALUES (?, 'admin', ?)",
            (email, admin["email"]),
        )
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        log_action(
            conn,
            email=admin["email"],
            action="admin_user.add",
            target_type="admin_user",
            target_id=new_id,
            details={"added_email": email},
        )
        conn.commit()
    return RedirectResponse("/admin/users", status_code=303)


@app.post("/admin/users/remove", response_class=HTMLResponse)
async def admin_users_remove(
    request: Request,
    admin: dict = Depends(require_admin),
    email: str = Form(...),
):
    """Remove an admin user. Cannot remove yourself."""
    email = email.strip().lower()
    if email == admin["email"].lower():
        return RedirectResponse(
            "/admin/users?" + urlencode({"error": "Cannot remove yourself"}), status_code=303
        )
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM admin_users WHERE email = ? COLLATE NOCASE", (email,)
        ).fetchone()
        if not row:
            return RedirectResponse(
                "/admin/users?" + urlencode({"error": f"User {email} not found"}),
                status_code=303,
            )
        conn.execute(
            "DELETE FROM admin_users WHERE email = ? COLLATE NOCASE", (email,)
        )
        log_action(
            conn,
            email=admin["email"],
            action="admin_user.remove",
            target_type="admin_user",
            target_id=row["id"],
            details={"removed_email": email},
        )
        conn.commit()
    return RedirectResponse("/admin/users", status_code=303)


@app.get("/admin/audit-log", response_class=HTMLResponse)
async def admin_audit_log(
    request: Request,
    admin: dict = Depends(require_admin),
    page: int = Query(default=1, ge=1),
    action: str = "",
    target_type: str = "",
    admin_email: str = "",
    date_from: str = "",
    date_to: str = "",
):
    """Paginated audit log viewer with optional filters."""
    per_page = 50
    filters = {
        k: v for k, v in {
            "action": action,
            "target_type": target_type,
            "admin_email": admin_email,
            "date_from": date_from,
            "date_to": date_to,
        }.items() if v
    }
    with get_db() as conn:
        rows, total_count = get_audit_log(conn, page=page, per_page=per_page, filters=filters)
    total_pages = max(1, (total_count + per_page - 1) // per_page)

    def _page_url(p: int) -> str:
        # `filters` already contains only non-empty values; add page only when > 1
        params = {**filters, **({"page": p} if p > 1 else {})}
        qs = urlencode(params)
        return f"/admin/audit-log{'?' + qs if qs else ''}"

    return await _tpl(request, "admin/audit_log.html", {
        "request": request,
        "admin": admin,
        "active_section": "audit-log",
        "rows": rows,
        "total_count": total_count,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "page_url": _page_url,
        # filter values for form repopulation
        "filter_action": action,
        "filter_target_type": target_type,
        "filter_admin_email": admin_email,
        "filter_date_from": date_from,
        "filter_date_to": date_to,
    })


@app.get("/admin/endorsements", response_class=HTMLResponse)
async def admin_endorsements(
    request: Request,
    admin: dict = Depends(require_admin),
    section: str = "",
    q: str = "",
):
    """Revised endorsement admin — Section 1: endorsement list + suggestions.
    Section 2: code mappings.
    """
    active_tab = section or "endorsements"
    with get_db() as conn:
        endorsements = get_endorsement_list(conn)
        # Suggestions are O(n²) across all endorsement pairs — only compute
        # when the suggestions tab is actually displayed.
        suggestions = (
            suggest_duplicate_endorsements(conn)
            if active_tab == "suggestions"
            else []
        )
        code_mappings = get_code_mappings(conn)
        # All endorsements for the autocomplete dropdown
        all_endorsements_for_select = sorted(
            [{"id": e["id"], "name": e["name"]} for e in endorsements],
            key=lambda e: e["name"],
        )
    return await _tpl(request, "admin/endorsements.html", {
        "request": request,
        "admin": admin,
        "active_section": "endorsements",
        "endorsements": endorsements,
        "suggestions": suggestions,
        "code_mappings": code_mappings,
        "all_endorsements_for_select": all_endorsements_for_select,
        "active_tab": active_tab,
        "q": q,
        "flash": request.query_params.get("flash", ""),
    })


@app.post("/admin/endorsements/alias", response_class=HTMLResponse)
async def admin_alias_endorsement(
    request: Request,
    admin: dict = Depends(require_admin),
    canonical_id: int = Form(...),
    variant_ids: list[int] = Form(default=[]),
    return_section: str = Form(default="endorsements"),
):
    """Designate a canonical endorsement and alias the selected variants to it."""
    if not canonical_id:
        raise HTTPException(status_code=422, detail="canonical_id is required")
    # Drop any variant that equals the canonical (belt-and-suspenders; the DB
    # layer also guards this, but we want a clean error rather than a silent no-op).
    variant_ids = [v for v in variant_ids if v != canonical_id]
    if not variant_ids:
        raise HTTPException(status_code=422, detail="At least one variant must differ from the canonical")
    with get_db() as conn:
        canonical_name = conn.execute(
            "SELECT name FROM license_endorsements WHERE id = ?",
            (canonical_id,),
        ).fetchone()
        canonical_name = canonical_name[0] if canonical_name else str(canonical_id)

        variant_names = []
        for vid in variant_ids:
            row = conn.execute(
                "SELECT name FROM license_endorsements WHERE id = ?",
                (vid,),
            ).fetchone()
            if row:
                variant_names.append(row[0])

        written = set_canonical_endorsement(
            conn,
            canonical_id=canonical_id,
            variant_ids=variant_ids,
            created_by=admin["email"],
        )
        log_action(
            conn,
            email=admin["email"],
            action="endorsement.set_canonical",
            target_type="endorsement",
            target_id=canonical_id,
            details={
                "canonical_name": canonical_name,
                "variant_ids": variant_ids,
                "variant_names": variant_names,
                "aliases_written": written,
            },
        )
        conn.commit()

    return RedirectResponse(f"/admin/endorsements?flash=aliased&section={return_section}", status_code=303)


# Keep the old /set-canonical URL working (used by existing tests)
@app.post("/admin/endorsements/set-canonical", response_class=HTMLResponse)
async def admin_set_canonical(
    request: Request,
    admin: dict = Depends(require_admin),
    canonical_id: int = Form(...),
    variant_ids: list[int] = Form(default=[]),
):
    """Legacy alias for /admin/endorsements/alias (backward compat)."""
    return await admin_alias_endorsement(
        request, admin, canonical_id=canonical_id, variant_ids=variant_ids
    )


@app.post("/admin/endorsements/rename", response_class=HTMLResponse)
async def admin_rename_endorsement(
    request: Request,
    admin: dict = Depends(require_admin),
    endorsement_id: int = Form(...),
    new_name: str = Form(...),
):
    """Assign a text name to a bare numeric-code endorsement."""
    new_name = new_name.strip()
    if not new_name:
        raise HTTPException(status_code=422, detail="new_name must not be empty")
    with get_db() as conn:
        old_name = conn.execute(
            "SELECT name FROM license_endorsements WHERE id = ?",
            (endorsement_id,),
        ).fetchone()
        old_name = old_name[0] if old_name else str(endorsement_id)

        canonical_id = rename_endorsement(
            conn,
            endorsement_id=endorsement_id,
            new_name=new_name,
            created_by=admin["email"],
        )
        log_action(
            conn,
            email=admin["email"],
            action="endorsement.rename",
            target_type="endorsement",
            target_id=endorsement_id,
            details={
                "old_name": old_name,
                "new_name": new_name,
                "canonical_id": canonical_id,
            },
        )
        conn.commit()

    return RedirectResponse("/admin/endorsements?flash=renamed&section=endorsements", status_code=303)


@app.post("/admin/endorsements/dismiss-suggestion", response_class=HTMLResponse)
async def admin_dismiss_suggestion(
    request: Request,
    admin: dict = Depends(require_admin),
    id_a: int = Form(...),
    id_b: int = Form(...),
    return_section: str = Form(default="endorsements"),
):
    """Permanently suppress a suggested duplicate pair."""
    with get_db() as conn:
        dismiss_suggestion(conn, id_a, id_b, admin["email"])
        log_action(
            conn,
            email=admin["email"],
            action="endorsement.dismiss_suggestion",
            target_type="endorsement",
            details={"id_a": id_a, "id_b": id_b},
        )
        conn.commit()
    return RedirectResponse(f"/admin/endorsements?flash=dismissed&section={return_section}", status_code=303)


@app.post("/admin/endorsements/code/add", response_class=HTMLResponse)
async def admin_code_add_endorsement(
    request: Request,
    admin: dict = Depends(require_admin),
    code: str = Form(...),
    endorsement_id: int = Form(...),
):
    """Add an endorsement to a code's expansion and retroactively reprocess."""
    code = code.strip()
    if not code:
        raise HTTPException(status_code=422, detail="code must not be empty")
    with get_db() as conn:
        added = add_code_mapping(conn, code, endorsement_id)
        if added:
            reprocessed = reprocess_endorsements(conn, code=code)
            log_action(
                conn,
                email=admin["email"],
                action="endorsement.code_add",
                target_type="endorsement",
                target_id=endorsement_id,
                details={"code": code, "reprocessed_records": reprocessed},
            )
        conn.commit()
    return RedirectResponse("/admin/endorsements?flash=code_updated&section=codes", status_code=303)


@app.post("/admin/endorsements/code/remove", response_class=HTMLResponse)
async def admin_code_remove_endorsement(
    request: Request,
    admin: dict = Depends(require_admin),
    code: str = Form(...),
    endorsement_id: int = Form(...),
):
    """Remove an endorsement from a code's expansion and retroactively reprocess."""
    code = code.strip()
    with get_db() as conn:
        removed = remove_code_mapping(conn, code, endorsement_id)
        if removed:
            reprocessed = reprocess_endorsements(conn, code=code)
            log_action(
                conn,
                email=admin["email"],
                action="endorsement.code_remove",
                target_type="endorsement",
                target_id=endorsement_id,
                details={"code": code, "reprocessed_records": reprocessed},
            )
        conn.commit()
    return RedirectResponse("/admin/endorsements?flash=code_updated&section=codes", status_code=303)


@app.post("/admin/endorsements/code/create", response_class=HTMLResponse)
async def admin_code_create(
    request: Request,
    admin: dict = Depends(require_admin),
    code: str = Form(...),
    endorsement_ids: list[int] = Form(default=[]),
):
    """Create a new code and assign endorsement(s) to it."""
    code = code.strip()
    if not code:
        raise HTTPException(status_code=422, detail="code must not be empty")
    with get_db() as conn:
        inserted = create_code(conn, code, endorsement_ids)
        if inserted:
            reprocessed = reprocess_endorsements(conn, code=code)
            log_action(
                conn,
                email=admin["email"],
                action="endorsement.code_create",
                target_type="endorsement",
                details={"code": code, "endorsement_ids": endorsement_ids, "reprocessed_records": reprocessed},
            )
        conn.commit()
    return RedirectResponse("/admin/endorsements?flash=code_created&section=codes", status_code=303)
