"""FastAPI APIRouter for core /admin/* routes.

Dashboard, admin-user management, and the audit-log viewer.  The
endorsement-curation routes live in admin_endorsement_routes.py (#140).
Both routers are included into the main app in app.py; the shared
``tpl`` coroutine is read from ``request.app.state.tpl`` at render time.
"""

import logging
from typing import Annotated, Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .admin_auth import require_admin
from .database import get_db
from .models import (
    admin_users as admin_users_table,
)
from .pg_admin_audit import get_audit_log, log_action
from .pg_integrity import (
    check_endorsement_anomalies,
    check_orphaned_locations,
    check_unenriched_records,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _render(
    request: Request,
    template: str,
    ctx: dict[str, Any],
    status_code: int = 200,
) -> HTMLResponse:
    """Render a template via the ``tpl`` coroutine stored on ``app.state``.

    Also used by admin_endorsement_routes.
    """
    tpl = getattr(request.app.state, "tpl", None)
    if tpl is None:
        msg = "app.state.tpl not set — template renderer not configured in lifespan"
        raise RuntimeError(msg)
    return await tpl(request, template, ctx, status_code)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def admin_dashboard(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
) -> HTMLResponse:
    """Admin dashboard — record counts, scrape status, data quality metrics."""
    async with get_db(request.app.state.engine) as conn:
        agg = (
            (
                await conn.execute(
                    text("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN section_type = 'new_application' THEN 1 ELSE 0 END) AS new_apps,
                SUM(CASE WHEN section_type = 'approved' THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN section_type = 'discontinued' THEN 1 ELSE 0 END) AS discontinued
            FROM license_records
        """)
                )
            )
            .mappings()
            .one()
        )

        recent = (
            (
                await conn.execute(
                    text("""
            SELECT
                SUM(CASE WHEN created_at >= NOW()
                    - INTERVAL '1 day' THEN 1 ELSE 0 END) AS last_24h,
                SUM(CASE WHEN created_at >= NOW()
                    - INTERVAL '7 days' THEN 1 ELSE 0 END) AS last_7d
            FROM license_records
        """)
                )
            )
            .mappings()
            .one()
        )

        scrapes_raw = (
            (
                await conn.execute(
                    text("""
            SELECT id, status, records_new, records_approved, records_discontinued,
                   records_skipped, started_at, finished_at,
                   ROUND(EXTRACT(EPOCH FROM (
                       finished_at - started_at
                   ))) AS duration_secs
            FROM scrape_log
            ORDER BY id DESC LIMIT 5
        """)
                )
            )
            .mappings()
            .all()
        )
        scrapes = [dict(r) for r in scrapes_raw]

        orphans = await check_orphaned_locations(conn)
        unenriched = await check_unenriched_records(conn)
        endorsement_issues = await check_endorsement_anomalies(conn)

        user_count = (
            await conn.execute(select(func.count()).select_from(admin_users_table))
        ).scalar_one()

    return await _render(
        request,
        "admin/dashboard.html",
        {
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
        },
    )


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


@router.get("/users", response_class=HTMLResponse)
async def admin_users(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
) -> HTMLResponse:
    """List all admin users."""
    async with get_db(request.app.state.engine) as conn:
        users = (
            (
                await conn.execute(
                    select(
                        admin_users_table.c.id,
                        admin_users_table.c.email,
                        admin_users_table.c.role,
                        admin_users_table.c.created_at,
                        admin_users_table.c.created_by,
                    ).order_by(admin_users_table.c.created_at)
                )
            )
            .mappings()
            .all()
        )
    return await _render(
        request,
        "admin/users.html",
        {
            "request": request,
            "admin": admin,
            "active_section": "users",
            "users": [dict(u) for u in users],
            "error": request.query_params.get("error", ""),
        },
    )


@router.post("/users/add", response_class=HTMLResponse)
async def admin_users_add(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    email: Annotated[str, Form()],
) -> HTMLResponse:
    """Add a new admin user."""
    email = email.strip().lower()
    if not email:
        return RedirectResponse(
            "/admin/users?" + urlencode({"error": "Email is required"}), status_code=303
        )
    async with get_db(request.app.state.engine) as conn:
        existing = (
            await conn.execute(
                select(admin_users_table.c.id).where(func.lower(admin_users_table.c.email) == email)
            )
        ).one_or_none()
        if existing:
            return RedirectResponse(
                "/admin/users?" + urlencode({"error": f"User {email} already exists"}),
                status_code=303,
            )
        result = await conn.execute(
            pg_insert(admin_users_table)
            .values(email=email, role="admin", created_by=admin["email"])
            .returning(admin_users_table.c.id)
        )
        new_id = result.scalar_one()
        await log_action(
            conn,
            email=admin["email"],
            action="admin_user.add",
            target_type="admin_user",
            target_id=new_id,
            details={"added_email": email},
        )
        await conn.commit()
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/users/remove", response_class=HTMLResponse)
async def admin_users_remove(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    email: Annotated[str, Form()],
) -> HTMLResponse:
    """Remove an admin user. Cannot remove yourself."""
    email = email.strip().lower()
    if email == admin["email"].lower():
        return RedirectResponse(
            "/admin/users?" + urlencode({"error": "Cannot remove yourself"}), status_code=303
        )
    async with get_db(request.app.state.engine) as conn:
        row = (
            await conn.execute(
                select(admin_users_table.c.id).where(func.lower(admin_users_table.c.email) == email)
            )
        ).one_or_none()
        if not row:
            return RedirectResponse(
                "/admin/users?" + urlencode({"error": f"User {email} not found"}),
                status_code=303,
            )
        await conn.execute(
            admin_users_table.delete().where(func.lower(admin_users_table.c.email) == email)
        )
        await log_action(
            conn,
            email=admin["email"],
            action="admin_user.remove",
            target_type="admin_user",
            target_id=row[0],
            details={"removed_email": email},
        )
        await conn.commit()
    return RedirectResponse("/admin/users", status_code=303)


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------


@router.get("/audit-log", response_class=HTMLResponse)
async def admin_audit_log(  # noqa: PLR0913
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    page: Annotated[int, Query(ge=1)] = 1,
    action: str = "",
    target_type: str = "",
    admin_email: str = "",
    date_from: str = "",
    date_to: str = "",
) -> HTMLResponse:
    """Paginated audit log viewer with optional filters."""
    per_page = 50
    filters = {
        k: v
        for k, v in {
            "action": action,
            "target_type": target_type,
            "admin_email": admin_email,
            "date_from": date_from,
            "date_to": date_to,
        }.items()
        if v
    }
    async with get_db(request.app.state.engine) as conn:
        rows, total_count = await get_audit_log(conn, page=page, per_page=per_page, filters=filters)
    total_pages = max(1, (total_count + per_page - 1) // per_page)

    def _page_url(p: int) -> str:
        params = {**filters, **({"page": p} if p > 1 else {})}
        qs = urlencode(params)
        return f"/admin/audit-log{'?' + qs if qs else ''}"

    return await _render(
        request,
        "admin/audit_log.html",
        {
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
        },
    )
