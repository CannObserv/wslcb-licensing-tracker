"""Entity-centric record queries for WSLCB licensing tracker.

Contains:
- get_entity_records() — all hydrated records associated with an entity
- get_entities() — paginated, searchable entity list
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .pg_queries_hydrate import _hydrate_records
from .pg_queries_search import RECORD_COLUMNS, RECORD_JOINS

logger = logging.getLogger(__name__)


async def get_entity_records(conn: AsyncConnection, entity_id: int) -> list[dict]:
    """Fetch all records associated with an entity, with location data."""
    result = await conn.execute(
        text(
            f"SELECT DISTINCT {RECORD_COLUMNS} {RECORD_JOINS}"
            " JOIN record_entities re ON re.record_id = lr.id"
            " WHERE re.entity_id = :entity_id"
            " ORDER BY lr.record_date DESC, lr.id DESC"
        ),
        {"entity_id": entity_id},
    )
    rows = [dict(r) for r in result.mappings().all()]
    return await _hydrate_records(conn, rows)


async def get_entities(  # noqa: PLR0913
    conn: AsyncConnection,
    *,
    q: str | None = None,
    entity_type: str | None = None,
    sort: str = "count",
    page: int = 1,
    per_page: int = 50,
) -> dict:
    """Return a paginated, searchable list of all entities.

    Parameters
    ----------
    q:
        Optional name substring (case-insensitive ILIKE search).
    entity_type:
        Optional filter: 'person', 'organization', or None for all.
    sort:
        'count' (default) — most active first (record_count DESC, name ASC);
        'name' — alphabetical (name ASC).
    page:
        1-based page number.
    per_page:
        Rows per page (default 50).

    Returns a dict with keys ``entities`` (list of row dicts) and ``total`` (int).
    """
    where_parts = []
    params: dict = {}
    if q:
        where_parts.append("e.name ILIKE :q_like")
        params["q_like"] = f"%{q}%"
    if entity_type is not None:
        where_parts.append("e.entity_type = :entity_type")
        params["entity_type"] = entity_type

    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    order_clause = (
        "ORDER BY record_count DESC, e.name ASC" if sort != "name" else "ORDER BY e.name ASC"
    )

    base_sql = f"""
        SELECT e.id, e.name, e.entity_type, COUNT(re.record_id) AS record_count
        FROM entities e
        LEFT JOIN record_entities re ON re.entity_id = e.id
        {where_clause}
        GROUP BY e.id
    """

    total_result = await conn.execute(
        text(f"SELECT COUNT(*) FROM ({base_sql}) sub"),
        params,
    )
    total = total_result.scalar_one()

    page = max(1, page)
    offset = (page - 1) * per_page
    rows_result = await conn.execute(
        text(f"{base_sql} {order_clause} LIMIT :limit OFFSET :offset"),
        {**params, "limit": per_page, "offset": offset},
    )
    return {"entities": [dict(r) for r in rows_result.mappings().all()], "total": total}
