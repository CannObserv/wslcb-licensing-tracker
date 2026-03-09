"""FastAPI APIRouter for all /admin/* routes.

All /admin/* handlers live here and are included into the main app via
``app.include_router(admin_routes.router)`` in app.py.  The shared
``_tpl()`` coroutine is injected at startup via ``init_router()``.
"""
import logging
from collections.abc import Callable
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from .admin_auth import require_admin
from .admin_audit import get_audit_log, log_action
from .db import get_db
from .endorsements import (
    reprocess_endorsements,
    rename_endorsement,
    set_canonical_endorsement,
)
from .endorsements_admin import (
    add_code_mapping,
    create_code,
    dismiss_suggestion,
    get_code_mappings,
    get_endorsement_list,
    remove_code_mapping,
    suggest_duplicate_endorsements,
)
from .integrity import (
    check_endorsement_anomalies,
    check_orphaned_locations,
    check_unenriched_records,
)
from .queries import invalidate_filter_cache
from .substances import (
    add_substance,
    get_regulated_substances,
    remove_substance,
    set_substance_endorsements,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

_VALID_ENDORSEMENT_SECTIONS = frozenset({"substances", "endorsements", "suggestions", "codes"})


# ---------------------------------------------------------------------------
# Shared helpers (injected by app.py at include time)
# ---------------------------------------------------------------------------

_tpl: Callable | None = None  # set by init_router()


def init_router(tpl_fn: Callable) -> None:
    """Bind the shared _tpl() coroutine from app.py into this router.

    Must be called before the first request is served.  Raises
    ``RuntimeError`` on any route invocation if skipped.
    """
    global _tpl
    _tpl = tpl_fn


async def _render(request, template: str, ctx: dict, status_code: int = 200):
    """Thin wrapper around the injected _tpl helper; raises if uninitialised."""
    if _tpl is None:
        raise RuntimeError(
            "admin_routes.init_router() was never called — "
            "cannot render templates"
        )
    return await _tpl(request, template, ctx, status_code)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def admin_dashboard(request: Request, admin: dict = Depends(require_admin)):
    """Admin dashboard — record counts, scrape status, data quality metrics."""
    with get_db() as conn:
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
        orphans = check_orphaned_locations(conn)
        unenriched = check_unenriched_records(conn)
        endorsement_issues = check_endorsement_anomalies(conn)
        user_count = conn.execute("SELECT COUNT(*) FROM admin_users").fetchone()[0]

    return await _render(request, "admin/dashboard.html", {
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


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@router.get("/users", response_class=HTMLResponse)
async def admin_users(request: Request, admin: dict = Depends(require_admin)):
    """List all admin users."""
    with get_db() as conn:
        users = conn.execute(
            "SELECT id, email, role, created_at, created_by FROM admin_users ORDER BY created_at"
        ).fetchall()
    return await _render(request, "admin/users.html", {
        "request": request,
        "admin": admin,
        "active_section": "users",
        "users": [dict(u) for u in users],
        "error": request.query_params.get("error", ""),
    })


@router.post("/users/add", response_class=HTMLResponse)
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


@router.post("/users/remove", response_class=HTMLResponse)
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


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

@router.get("/audit-log", response_class=HTMLResponse)
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
        params = {**filters, **({"page": p} if p > 1 else {})}
        qs = urlencode(params)
        return f"/admin/audit-log{'?' + qs if qs else ''}"

    return await _render(request, "admin/audit_log.html", {
        "request": request,
        "admin": admin,
        "active_section": "audit-log",
        "rows": rows,
        "total_count": total_count,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "page_url": _page_url,
        "filter_action": action,
        "filter_target_type": target_type,
        "filter_admin_email": admin_email,
        "filter_date_from": date_from,
        "filter_date_to": date_to,
    })


# ---------------------------------------------------------------------------
# Endorsements
# ---------------------------------------------------------------------------

@router.get("/endorsements", response_class=HTMLResponse)
async def admin_endorsements(
    request: Request,
    admin: dict = Depends(require_admin),
    section: str = "",
    q: str = "",
):
    """Endorsement admin — tabs: substances, endorsement list, suggestions, codes."""
    active_tab = section if section in _VALID_ENDORSEMENT_SECTIONS else "substances"
    with get_db() as conn:
        substances = get_regulated_substances(conn)
        endorsements = get_endorsement_list(conn)
        suggestions = (
            suggest_duplicate_endorsements(conn)
            if active_tab == "suggestions"
            else []
        )
        code_mappings = get_code_mappings(conn)
        all_endorsements_for_select = sorted(
            [{"id": e["id"], "name": e["name"], "record_count": e.get("record_count", 0)}
             for e in endorsements],
            key=lambda e: e["name"],
        )
    return await _render(request, "admin/endorsements.html", {
        "request": request,
        "admin": admin,
        "active_section": "endorsements",
        "substances": substances,
        "endorsements": endorsements,
        "suggestions": suggestions,
        "code_mappings": code_mappings,
        "all_endorsements_for_select": all_endorsements_for_select,
        "active_tab": active_tab,
        "q": q,
        "flash": request.query_params.get("flash", ""),
    })


@router.post("/endorsements/substances/add", response_class=HTMLResponse)
async def admin_substance_add(
    request: Request,
    admin: dict = Depends(require_admin),
    name: str = Form(...),
):
    """Create a new regulated substance."""
    name = name.strip()
    if not name:
        return RedirectResponse(
            "/admin/endorsements?section=substances&flash=substance_name_required",
            status_code=303,
        )
    with get_db() as conn:
        row = conn.execute("SELECT COALESCE(MAX(display_order), 0) + 1 FROM regulated_substances").fetchone()
        display_order = row[0] if row else 1
        sid = add_substance(conn, name, display_order)
        log_action(conn, admin["email"], "substance.add", "regulated_substance",
                   target_id=sid, details={"name": name})
        conn.commit()
    invalidate_filter_cache()
    return RedirectResponse("/admin/endorsements?section=substances&flash=substance_added", status_code=303)


@router.post("/endorsements/substances/remove", response_class=HTMLResponse)
async def admin_substance_remove(
    request: Request,
    admin: dict = Depends(require_admin),
    substance_id: int = Form(...),
):
    """Delete a regulated substance and its endorsement associations."""
    with get_db() as conn:
        substance_name = remove_substance(conn, substance_id) or str(substance_id)
        log_action(conn, admin["email"], "substance.remove", "regulated_substance",
                   target_id=substance_id, details={"name": substance_name})
        conn.commit()
    invalidate_filter_cache()
    return RedirectResponse("/admin/endorsements?section=substances&flash=substance_removed", status_code=303)


@router.post("/endorsements/substances/set-endorsements", response_class=HTMLResponse)
async def admin_substance_set_endorsements(
    request: Request,
    admin: dict = Depends(require_admin),
    substance_id: int = Form(...),
    endorsement_ids: list[int] = Form(default=[]),
):
    """Replace the endorsement associations for a regulated substance."""
    with get_db() as conn:
        set_substance_endorsements(conn, substance_id, endorsement_ids)
        log_action(conn, admin["email"], "substance.set_endorsements", "regulated_substance",
                   target_id=substance_id, details={"endorsement_count": len(endorsement_ids)})
        conn.commit()
    invalidate_filter_cache()
    return RedirectResponse(
        f"/admin/endorsements?section=substances&selected={substance_id}&flash=substance_updated",
        status_code=303,
    )


@router.post("/endorsements/alias", response_class=HTMLResponse)
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

    safe_section = return_section if return_section in _VALID_ENDORSEMENT_SECTIONS else "endorsements"
    invalidate_filter_cache()
    return RedirectResponse(f"/admin/endorsements?flash=aliased&section={safe_section}", status_code=303)


@router.post("/endorsements/set-canonical", response_class=HTMLResponse)
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


@router.post("/endorsements/rename", response_class=HTMLResponse)
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

    invalidate_filter_cache()
    return RedirectResponse("/admin/endorsements?flash=renamed&section=endorsements", status_code=303)


@router.post("/endorsements/dismiss-suggestion", response_class=HTMLResponse)
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
    safe_section = return_section if return_section in _VALID_ENDORSEMENT_SECTIONS else "endorsements"
    invalidate_filter_cache()
    return RedirectResponse(f"/admin/endorsements?flash=dismissed&section={safe_section}", status_code=303)


@router.post("/endorsements/code/add", response_class=HTMLResponse)
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
    invalidate_filter_cache()
    return RedirectResponse("/admin/endorsements?flash=code_updated&section=codes", status_code=303)


@router.post("/endorsements/code/remove", response_class=HTMLResponse)
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
    invalidate_filter_cache()
    return RedirectResponse("/admin/endorsements?flash=code_updated&section=codes", status_code=303)


@router.post("/endorsements/code/create", response_class=HTMLResponse)
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
    invalidate_filter_cache()
    return RedirectResponse("/admin/endorsements?flash=code_created&section=codes", status_code=303)
