"""Admin audit log helpers for the WSLCB Licensing Tracker.

Every mutation through the admin interface should call :func:`log_action` so
there is an immutable trail of who did what and when.

Public API
----------
- :func:`log_action` — insert one audit row.
- :func:`get_audit_log` — paginated retrieval with optional filters.
"""
import json
import logging
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)


def log_action(
    conn: sqlite3.Connection,
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
        Open database connection — the caller is responsible for committing.
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
    cursor = conn.execute(
        """
        INSERT INTO admin_audit_log
            (admin_email, action, target_type, target_id, details)
        VALUES (?, ?, ?, ?, ?)
        """,
        (email, action, target_type, target_id, details_json),
    )
    row_id = cursor.lastrowid
    logger.debug(
        "Audit log: %s by %s on %s#%s",
        action, email, target_type, target_id,
    )
    return row_id  # type: ignore[return-value]


def get_audit_log(
    conn: sqlite3.Connection,
    page: int = 1,
    per_page: int = 50,
    filters: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return a paginated slice of the audit log.

    Parameters
    ----------
    conn:
        Open database connection.
    page:
        1-based page number.
    per_page:
        Rows per page.
    filters:
        Optional dict with any combination of:
        ``action``, ``target_type``, ``admin_email``,
        ``date_from`` (inclusive, ``YYYY-MM-DD``),
        ``date_to`` (inclusive, ``YYYY-MM-DD``).

    Returns
    -------
    (rows, total_count)
        *rows* is a list of dicts with keys matching the table columns plus a
        ``details_parsed`` key that holds the decoded JSON dict (or ``None``).
        *total_count* is the number of rows matching the filters (ignoring
        pagination).
    """
    filters = filters or {}
    where_clauses: list[str] = []
    params: list[Any] = []

    if filters.get("action"):
        where_clauses.append("action = ?")
        params.append(filters["action"])

    if filters.get("target_type"):
        where_clauses.append("target_type = ?")
        params.append(filters["target_type"])

    if filters.get("admin_email"):
        where_clauses.append("admin_email = ? COLLATE NOCASE")
        params.append(filters["admin_email"])

    if filters.get("date_from"):
        where_clauses.append("date(created_at) >= ?")
        params.append(filters["date_from"])

    if filters.get("date_to"):
        where_clauses.append("date(created_at) <= ?")
        params.append(filters["date_to"])

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total_count: int = conn.execute(
        f"SELECT COUNT(*) FROM admin_audit_log {where_sql}", params
    ).fetchone()[0]

    offset = (page - 1) * per_page
    rows_raw = conn.execute(
        f"""
        SELECT id, admin_email, action, target_type, target_id, details, created_at
        FROM admin_audit_log
        {where_sql}
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        """,
        params + [per_page, offset],
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for r in rows_raw:
        details_raw = r[5]
        rows.append({
            "id": r[0],
            "admin_email": r[1],
            "action": r[2],
            "target_type": r[3],
            "target_id": r[4],
            "details": details_raw,
            "details_parsed": json.loads(details_raw) if details_raw else None,
            "created_at": r[6],
        })

    return rows, total_count
