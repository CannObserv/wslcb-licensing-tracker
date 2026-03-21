"""Async PostgreSQL admin audit log helpers.

Async equivalent of admin_audit.py. Uses SQLAlchemy Core + asyncpg.

Public API
----------
- :func:`log_action` — insert one audit row.
- :func:`get_audit_log` — paginated retrieval with optional filters.
"""

import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import admin_audit_log

logger = logging.getLogger(__name__)


async def log_action(  # noqa: PLR0913
    conn: AsyncConnection,
    email: str,
    action: str,
    target_type: str,
    target_id: int | None = None,
    details: dict[str, Any] | None = None,
) -> int:
    """Insert one audit log row and return its ``id``.

    Parameters
    ----------
    conn:
        Open async database connection — the caller is responsible for committing.
    email:
        Email address of the admin performing the action.
    action:
        ``{domain}.{verb}`` identifier, e.g. ``endorsement.set_canonical``.
    target_type:
        The entity type being mutated, e.g. ``'endorsement'``.
    target_id:
        Optional primary key of the row being mutated.
    details:
        Optional dict with action-specific context (old/new values, counts
        etc.).  Serialized to JSON for storage; pass ``None`` when there is
        nothing extra to record.
    """
    details_json: str | None = json.dumps(details) if details is not None else None
    stmt = (
        pg_insert(admin_audit_log)
        .values(
            admin_email=email,
            action=action,
            target_type=target_type,
            target_id=target_id,
            details=details_json,
        )
        .returning(admin_audit_log.c.id)
    )
    row_id = (await conn.execute(stmt)).scalar_one()
    logger.debug(
        "Audit log: %s by %s on %s#%s",
        action,
        email,
        target_type,
        target_id,
    )
    return row_id


async def get_audit_log(
    conn: AsyncConnection,
    page: int = 1,
    per_page: int = 50,
    filters: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return a paginated slice of the audit log.

    Parameters
    ----------
    conn:
        Open async database connection.
    page:
        1-based page number.
    per_page:
        Rows per page.
    filters:
        Optional dict with any combination of:
        ``action``, ``target_type``, ``admin_email``,
        ``date_from`` (inclusive, ``YYYY-MM-DD``),
        ``date_to`` (inclusive, ``YYYY-MM-DD``).

    Returns:
    -------
    (rows, total_count)
        *rows* is a list of dicts with keys matching the table columns plus a
        ``details_parsed`` key that holds the decoded JSON dict (or ``None``).
        *total_count* is the number of rows matching the filters (ignoring
        pagination).
    """
    filters = filters or {}
    where_parts: list[str] = []
    params: dict[str, Any] = {}

    if filters.get("action"):
        where_parts.append("action = :action")
        params["action"] = filters["action"]

    if filters.get("target_type"):
        where_parts.append("target_type = :target_type")
        params["target_type"] = filters["target_type"]

    if filters.get("admin_email"):
        where_parts.append("lower(admin_email) = lower(:admin_email)")
        params["admin_email"] = filters["admin_email"]

    if filters.get("date_from"):
        where_parts.append("created_at::date >= :date_from")
        params["date_from"] = filters["date_from"]

    if filters.get("date_to"):
        where_parts.append("created_at::date <= :date_to")
        params["date_to"] = filters["date_to"]

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    total_count = int(
        (
            await conn.execute(
                text(f"SELECT COUNT(*) FROM admin_audit_log {where_sql}"),
                params,
            )
        ).scalar_one()
    )

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset
    rows_raw = (
        (
            await conn.execute(
                text(f"""
            SELECT id, admin_email, action, target_type, target_id,
                   details, created_at
            FROM admin_audit_log
            {where_sql}
            ORDER BY id DESC
            LIMIT :limit OFFSET :offset
        """),
                params,
            )
        )
        .mappings()
        .all()
    )

    rows: list[dict[str, Any]] = []
    for r in rows_raw:
        details_raw = r["details"]
        rows.append(
            {
                "id": r["id"],
                "admin_email": r["admin_email"],
                "action": r["action"],
                "target_type": r["target_type"],
                "target_id": r["target_id"],
                "details": details_raw,
                "details_parsed": json.loads(details_raw) if details_raw else None,
                "created_at": r["created_at"],
            }
        )

    return rows, total_count
