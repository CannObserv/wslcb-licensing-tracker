"""FastAPI APIRouter for all /admin/* routes.

All /admin/* handlers live here and are included into the main app via
``app.include_router(admin_routes.router)`` in app.py.  The shared
``tpl`` coroutine is read from ``request.app.state.tpl`` at render time.
"""

import logging
from collections.abc import AsyncGenerator
from typing import Annotated, Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .admin_auth import require_admin
from .database import get_db
from .models import (
    admin_users as admin_users_table,
)
from .models import (
    endorsement_aliases,
    license_endorsements,
    license_records,
    record_endorsements,
    regulated_substances,
)
from .pg_admin_audit import get_audit_log, log_action
from .pg_endorsements import (
    process_record,
    remove_alias,
    rename_endorsement,
    reprocess_endorsements,
    set_canonical_endorsement,
)
from .pg_endorsements_admin import (
    add_code_mapping,
    create_code,
    dismiss_suggestion,
    get_code_mappings,
    get_endorsement_list,
    remove_code_mapping,
    suggest_duplicate_endorsements,
)
from .pg_integrity import (
    check_endorsement_anomalies,
    check_orphaned_locations,
    check_unenriched_records,
)
from .pg_queries_filter import invalidate_filter_cache as invalidate_all_filter_caches
from .pg_substances import (
    add_substance,
    get_regulated_substances,
    remove_substance,
    set_substance_endorsements,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

_VALID_ENDORSEMENT_SECTIONS = frozenset({"substances", "endorsements", "suggestions", "codes"})


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _render(
    request: Request,
    template: str,
    ctx: dict[str, Any],
    status_code: int = 200,
) -> HTMLResponse:
    """Render a template via the ``tpl`` coroutine stored on ``app.state``."""
    tpl = getattr(request.app.state, "tpl", None)
    if tpl is None:
        msg = "app.state.tpl not set — template renderer not configured in lifespan"
        raise RuntimeError(msg)
    return await tpl(request, template, ctx, status_code)


async def _get_db(request: Request) -> AsyncGenerator[AsyncConnection, None]:
    """Yield an AsyncConnection for use as a FastAPI dependency."""
    async with get_db(request.app.state.engine) as conn:
        yield conn


Conn = Annotated[AsyncConnection, Depends(_get_db)]


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


# ---------------------------------------------------------------------------
# Endorsements
# ---------------------------------------------------------------------------


@router.get("/endorsements", response_class=HTMLResponse)
async def admin_endorsements(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    section: str = "",
    q: str = "",
) -> HTMLResponse:
    """Endorsement admin — tabs: substances, endorsement list, suggestions, codes."""
    active_tab = section if section in _VALID_ENDORSEMENT_SECTIONS else "substances"
    async with get_db(request.app.state.engine) as conn:
        substances = await get_regulated_substances(conn)
        endorsements = await get_endorsement_list(conn)
        suggestions = (
            await suggest_duplicate_endorsements(conn) if active_tab == "suggestions" else []
        )
        code_mappings = await get_code_mappings(conn)
        all_endorsements_for_select = sorted(
            [
                {"id": e["id"], "name": e["name"], "record_count": e.get("record_count", 0)}
                for e in endorsements
            ],
            key=lambda e: e["name"],
        )
    return await _render(
        request,
        "admin/endorsements.html",
        {
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
        },
    )


@router.post("/endorsements/substances/add", response_class=HTMLResponse)
async def admin_substance_add(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    name: Annotated[str, Form()],
) -> HTMLResponse:
    """Create a new regulated substance."""
    name = name.strip()
    if not name:
        return RedirectResponse(
            "/admin/endorsements?section=substances&flash=substance_name_required",
            status_code=303,
        )
    async with get_db(request.app.state.engine) as conn:
        display_order = (
            await conn.execute(
                select(func.coalesce(func.max(regulated_substances.c.display_order), 0) + 1)
            )
        ).scalar_one()
        sid = await add_substance(conn, name, display_order)
        await log_action(
            conn,
            admin["email"],
            "substance.add",
            "regulated_substance",
            target_id=sid,
            details={"name": name},
        )
        await conn.commit()
    invalidate_all_filter_caches()
    return RedirectResponse(
        "/admin/endorsements?section=substances&flash=substance_added", status_code=303
    )


@router.post("/endorsements/substances/remove", response_class=HTMLResponse)
async def admin_substance_remove(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    substance_id: Annotated[int, Form()],
) -> HTMLResponse:
    """Delete a regulated substance and its endorsement associations."""
    async with get_db(request.app.state.engine) as conn:
        substance_name = await remove_substance(conn, substance_id) or str(substance_id)
        await log_action(
            conn,
            admin["email"],
            "substance.remove",
            "regulated_substance",
            target_id=substance_id,
            details={"name": substance_name},
        )
        await conn.commit()
    invalidate_all_filter_caches()
    return RedirectResponse(
        "/admin/endorsements?section=substances&flash=substance_removed", status_code=303
    )


@router.post("/endorsements/substances/set-endorsements", response_class=HTMLResponse)
async def admin_substance_set_endorsements(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    substance_id: Annotated[int, Form()],
    endorsement_ids: Annotated[list[int], Form()] = [],  # noqa: B006
) -> HTMLResponse:
    """Replace the endorsement associations for a regulated substance."""
    async with get_db(request.app.state.engine) as conn:
        await set_substance_endorsements(conn, substance_id, endorsement_ids)
        await log_action(
            conn,
            admin["email"],
            "substance.set_endorsements",
            "regulated_substance",
            target_id=substance_id,
            details={"endorsement_count": len(endorsement_ids)},
        )
        await conn.commit()
    invalidate_all_filter_caches()
    return RedirectResponse(
        f"/admin/endorsements?section=substances&selected={substance_id}&flash=substance_updated",
        status_code=303,
    )


@router.post("/endorsements/alias", response_class=HTMLResponse)
async def admin_alias_endorsement(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    canonical_id: Annotated[int, Form()],
    variant_ids: Annotated[list[int], Form()] = [],  # noqa: B006
    return_section: Annotated[str, Form()] = "endorsements",
) -> HTMLResponse:
    """Designate a canonical endorsement and alias the selected variants to it."""
    if not canonical_id:
        raise HTTPException(status_code=422, detail="canonical_id is required")
    variant_ids = [v for v in variant_ids if v != canonical_id]
    if not variant_ids:
        raise HTTPException(
            status_code=422, detail="At least one variant must differ from the canonical"
        )
    async with get_db(request.app.state.engine) as conn:
        canonical_name = (
            await conn.execute(
                select(license_endorsements.c.name).where(license_endorsements.c.id == canonical_id)
            )
        ).scalar_one_or_none() or str(canonical_id)

        variant_names = []
        for vid in variant_ids:
            name_val = (
                await conn.execute(
                    select(license_endorsements.c.name).where(license_endorsements.c.id == vid)
                )
            ).scalar_one_or_none()
            if name_val:
                variant_names.append(name_val)

        written = await set_canonical_endorsement(
            conn,
            canonical_id=canonical_id,
            variant_ids=variant_ids,
            created_by=admin["email"],
        )
        await log_action(
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
        await conn.commit()

    safe_section = (
        return_section if return_section in _VALID_ENDORSEMENT_SECTIONS else "endorsements"
    )
    invalidate_all_filter_caches()
    return RedirectResponse(
        f"/admin/endorsements?flash=aliased&section={safe_section}", status_code=303
    )


@router.post("/endorsements/set-canonical", response_class=HTMLResponse)
async def admin_set_canonical(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    canonical_id: Annotated[int, Form()],
    variant_ids: Annotated[list[int], Form()] = [],  # noqa: B006
) -> HTMLResponse:
    """Legacy alias for /admin/endorsements/alias (backward compat)."""
    return await admin_alias_endorsement(
        request, admin, canonical_id=canonical_id, variant_ids=variant_ids
    )


@router.post("/endorsements/rename", response_class=HTMLResponse)
async def admin_rename_endorsement(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    endorsement_id: Annotated[int, Form()],
    new_name: Annotated[str, Form()],
) -> HTMLResponse:
    """Assign a text name to a bare numeric-code endorsement."""
    new_name = new_name.strip()
    if not new_name:
        raise HTTPException(status_code=422, detail="new_name must not be empty")
    async with get_db(request.app.state.engine) as conn:
        old_name = (
            await conn.execute(
                select(license_endorsements.c.name).where(
                    license_endorsements.c.id == endorsement_id
                )
            )
        ).scalar_one_or_none() or str(endorsement_id)

        canonical_id = await rename_endorsement(
            conn,
            endorsement_id=endorsement_id,
            new_name=new_name,
            created_by=admin["email"],
        )
        await log_action(
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
        await conn.commit()

    invalidate_all_filter_caches()
    return RedirectResponse(
        "/admin/endorsements?flash=renamed&section=endorsements", status_code=303
    )


@router.post("/endorsements/unalias", response_class=HTMLResponse)
async def admin_unalias_endorsement(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    endorsement_id: Annotated[int, Form()],
) -> HTMLResponse:
    """Remove an endorsement alias, making the variant standalone."""
    async with get_db(request.app.state.engine) as conn:
        exists = (
            await conn.execute(
                select(license_endorsements.c.id).where(license_endorsements.c.id == endorsement_id)
            )
        ).one_or_none()
        if not exists:
            raise HTTPException(status_code=404, detail="endorsement not found")

        alias_row = (
            await conn.execute(
                select(endorsement_aliases.c.canonical_endorsement_id).where(
                    endorsement_aliases.c.endorsement_id == endorsement_id
                )
            )
        ).one_or_none()
        if not alias_row:
            raise HTTPException(
                status_code=422, detail="endorsement is not a variant — no alias to remove"
            )
        canonical_id = alias_row[0]

        variant_name = (
            await conn.execute(
                select(license_endorsements.c.name).where(
                    license_endorsements.c.id == endorsement_id
                )
            )
        ).scalar_one_or_none() or str(endorsement_id)

        canonical_name = (
            await conn.execute(
                select(license_endorsements.c.name).where(license_endorsements.c.id == canonical_id)
            )
        ).scalar_one_or_none() or str(canonical_id)

        await remove_alias(conn, endorsement_id=endorsement_id, removed_by=admin["email"])

        # Reprocess resolved_endorsements FTS for all affected records.
        affected = (
            await conn.execute(
                select(license_records.c.id, license_records.c.license_type)
                .select_from(
                    license_records.join(
                        record_endorsements,
                        record_endorsements.c.record_id == license_records.c.id,
                    )
                )
                .where(record_endorsements.c.endorsement_id == endorsement_id)
            )
        ).fetchall()
        for record_id, raw_license_type in affected:
            await process_record(conn, record_id, raw_license_type or "")

        await log_action(
            conn,
            email=admin["email"],
            action="endorsement.remove_alias",
            target_type="endorsement",
            target_id=endorsement_id,
            details={
                "variant_name": variant_name,
                "canonical_id": canonical_id,
                "canonical_name": canonical_name,
                "records_reprocessed": len(affected),
            },
        )
        await conn.commit()

    invalidate_all_filter_caches()
    return RedirectResponse(
        "/admin/endorsements?flash=unaliased&section=endorsements", status_code=303
    )


@router.post("/endorsements/dismiss-suggestion", response_class=HTMLResponse)
async def admin_dismiss_suggestion(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    id_a: Annotated[int, Form()],
    id_b: Annotated[int, Form()],
    return_section: Annotated[str, Form()] = "endorsements",
) -> HTMLResponse:
    """Permanently suppress a suggested duplicate pair."""
    async with get_db(request.app.state.engine) as conn:
        await dismiss_suggestion(conn, id_a, id_b, admin["email"])
        await log_action(
            conn,
            email=admin["email"],
            action="endorsement.dismiss_suggestion",
            target_type="endorsement",
            details={"id_a": id_a, "id_b": id_b},
        )
        await conn.commit()
    safe_section = (
        return_section if return_section in _VALID_ENDORSEMENT_SECTIONS else "endorsements"
    )
    invalidate_all_filter_caches()
    return RedirectResponse(
        f"/admin/endorsements?flash=dismissed&section={safe_section}", status_code=303
    )


@router.post("/endorsements/code/add", response_class=HTMLResponse)
async def admin_code_add_endorsement(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    code: Annotated[str, Form()],
    endorsement_id: Annotated[int, Form()],
) -> HTMLResponse:
    """Add an endorsement to a code's expansion and retroactively reprocess."""
    code = code.strip()
    if not code:
        raise HTTPException(status_code=422, detail="code must not be empty")
    async with get_db(request.app.state.engine) as conn:
        added = await add_code_mapping(conn, code, endorsement_id)
        if added:
            reprocessed = await reprocess_endorsements(conn, code=code)
            await log_action(
                conn,
                email=admin["email"],
                action="endorsement.code_add",
                target_type="endorsement",
                target_id=endorsement_id,
                details={"code": code, "reprocessed_records": reprocessed},
            )
        await conn.commit()
    invalidate_all_filter_caches()
    return RedirectResponse("/admin/endorsements?flash=code_updated&section=codes", status_code=303)


@router.post("/endorsements/code/remove", response_class=HTMLResponse)
async def admin_code_remove_endorsement(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    code: Annotated[str, Form()],
    endorsement_id: Annotated[int, Form()],
) -> HTMLResponse:
    """Remove an endorsement from a code's expansion and retroactively reprocess."""
    code = code.strip()
    async with get_db(request.app.state.engine) as conn:
        removed = await remove_code_mapping(conn, code, endorsement_id)
        if removed:
            reprocessed = await reprocess_endorsements(conn, code=code)
            await log_action(
                conn,
                email=admin["email"],
                action="endorsement.code_remove",
                target_type="endorsement",
                target_id=endorsement_id,
                details={"code": code, "reprocessed_records": reprocessed},
            )
        await conn.commit()
    invalidate_all_filter_caches()
    return RedirectResponse("/admin/endorsements?flash=code_updated&section=codes", status_code=303)


@router.post("/endorsements/code/create", response_class=HTMLResponse)
async def admin_code_create(
    request: Request,
    admin: Annotated[dict[str, Any], Depends(require_admin)],
    code: Annotated[str, Form()],
    endorsement_ids: Annotated[list[int], Form()] = [],  # noqa: B006
) -> HTMLResponse:
    """Create a new code and assign endorsement(s) to it."""
    code = code.strip()
    if not code:
        raise HTTPException(status_code=422, detail="code must not be empty")
    async with get_db(request.app.state.engine) as conn:
        inserted = await create_code(conn, code, endorsement_ids)
        if inserted:
            reprocessed = await reprocess_endorsements(conn, code=code)
            await log_action(
                conn,
                email=admin["email"],
                action="endorsement.code_create",
                target_type="endorsement",
                details={
                    "code": code,
                    "endorsement_ids": endorsement_ids,
                    "reprocessed_records": reprocessed,
                },
            )
        await conn.commit()
    invalidate_all_filter_caches()
    return RedirectResponse("/admin/endorsements?flash=code_created&section=codes", status_code=303)
