"""Async PostgreSQL database helpers for the WSLCB licensing tracker.

Async equivalents of the location, source, and provenance helpers in db.py.
Uses SQLAlchemy Core expressions against the table objects in models.py.

Pure-string helpers (_normalize_raw_address, clean_entity_name, etc.) are
re-imported from db.py — they have no DB dependency.
"""

import json
import logging

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from .db import SOURCE_ROLE_PRIORITY, _normalize_raw_address
from .models import locations, record_sources, sources

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Location helpers
# ------------------------------------------------------------------


async def get_or_create_location(
    conn: AsyncConnection,
    raw_address: str | None,
    city: str = "",
    state: str = "WA",
    zip_code: str = "",
) -> int | None:
    """Return the location id for *raw_address*, creating if needed.

    Returns None if raw_address is empty/None/whitespace-only.
    Uses INSERT ... ON CONFLICT DO NOTHING + RETURNING for atomicity.
    """
    if not raw_address or not raw_address.strip():
        return None

    normalized = _normalize_raw_address(raw_address)

    # Try insert; ON CONFLICT returns no row
    stmt = (
        pg_insert(locations)
        .values(raw_address=normalized, city=city, state=state, zip_code=zip_code)
        .on_conflict_do_nothing(index_elements=["raw_address"])
        .returning(locations.c.id)
    )
    result = await conn.execute(stmt)
    row = result.first()
    if row:
        return row[0]

    # Conflict — fetch existing
    result = await conn.execute(select(locations.c.id).where(locations.c.raw_address == normalized))
    return result.scalar_one()


# ------------------------------------------------------------------
# Source helpers
# ------------------------------------------------------------------


async def get_or_create_source(  # noqa: PLR0913
    conn: AsyncConnection,
    source_type_id: int,
    snapshot_path: str | None = None,
    url: str | None = None,
    captured_at: str | None = None,
    scrape_log_id: int | None = None,
    metadata: dict | None = None,
) -> int:
    """Return the source id for the given type + snapshot_path, creating if needed.

    When snapshot_path is not None, uses ON CONFLICT DO NOTHING on the
    (source_type_id, snapshot_path) unique constraint.

    When snapshot_path is None, PostgreSQL treats NULLs as distinct in
    UNIQUE constraints, so we use manual lookup-before-insert logic
    (same as the SQLite version in db.py).
    """
    meta_json = json.dumps(metadata) if metadata else "{}"

    if snapshot_path is not None:
        stmt = (
            pg_insert(sources)
            .values(
                source_type_id=source_type_id,
                snapshot_path=snapshot_path,
                url=url,
                captured_at=captured_at,
                scrape_log_id=scrape_log_id,
                metadata=meta_json,
            )
            .on_conflict_do_nothing(constraint="uq_sources_type_path")
            .returning(sources.c.id)
        )
        result = await conn.execute(stmt)
        row = result.first()
        if row:
            return row[0]
        # Conflict — fetch existing
        result = await conn.execute(
            select(sources.c.id).where(
                sources.c.source_type_id == source_type_id,
                sources.c.snapshot_path == snapshot_path,
            )
        )
        row = result.first()
        if row is None:
            msg = f"Source row vanished for type={source_type_id}, path={snapshot_path!r}"
            raise RuntimeError(msg)
        return row[0]

    # NULL snapshot_path — PostgreSQL NULLs are distinct in UNIQUE constraints,
    # so ON CONFLICT DO NOTHING won't fire. Use SAVEPOINT to make the insert
    # atomic: attempt insert; on IntegrityError roll back to the savepoint and
    # fetch the row that won the race.
    async def _select_null_path() -> int:
        if scrape_log_id is not None:
            r = await conn.execute(
                select(sources.c.id).where(
                    sources.c.source_type_id == source_type_id,
                    sources.c.snapshot_path.is_(None),
                    sources.c.scrape_log_id == scrape_log_id,
                )
            )
        else:
            r = await conn.execute(
                select(sources.c.id).where(
                    sources.c.source_type_id == source_type_id,
                    sources.c.snapshot_path.is_(None),
                    sources.c.scrape_log_id.is_(None),
                )
            )
        row = r.scalar_one_or_none()
        if row is None:
            msg = (
                f"Source row vanished for type={source_type_id},"
                f" scrape_log_id={scrape_log_id!r} (NULL snapshot_path)"
            )
            raise RuntimeError(msg)
        return row

    await conn.execute(text("SAVEPOINT get_or_create_source_null"))
    try:
        stmt = (
            pg_insert(sources)
            .values(
                source_type_id=source_type_id,
                snapshot_path=None,
                url=url,
                captured_at=captured_at,
                scrape_log_id=scrape_log_id,
                metadata=meta_json,
            )
            .returning(sources.c.id)
        )
        result = await conn.execute(stmt)
        await conn.execute(text("RELEASE SAVEPOINT get_or_create_source_null"))
        return result.scalar_one()
    except IntegrityError:
        await conn.execute(text("ROLLBACK TO SAVEPOINT get_or_create_source_null"))
        return await _select_null_path()


async def link_record_source(
    conn: AsyncConnection,
    record_id: int,
    source_id: int,
    role: str = "first_seen",
) -> None:
    """Link a license record to a source (idempotent).

    Does nothing if the (record_id, source_id, role) combination already exists.
    """
    stmt = (
        pg_insert(record_sources)
        .values(record_id=record_id, source_id=source_id, role=role)
        .on_conflict_do_nothing()
    )
    await conn.execute(stmt)


# ------------------------------------------------------------------
# Provenance query helpers
# ------------------------------------------------------------------


async def get_primary_source(
    conn: AsyncConnection,
    record_id: int,
) -> dict | None:
    """Return the single most-relevant source for a record, or None.

    Priority order:
    1. Role: first_seen > repaired > confirmed
    2. Within a role: sources with a non-NULL snapshot_path first
    3. Newest captured_at as tiebreaker
    """
    stmt = text("""
        SELECT s.id, st.slug AS source_type, st.label AS source_label,
               s.snapshot_path, s.url, s.captured_at, s.ingested_at,
               s.metadata, rs.role
        FROM record_sources rs
        JOIN sources s ON s.id = rs.source_id
        JOIN source_types st ON st.id = s.source_type_id
        WHERE rs.record_id = :record_id
        ORDER BY s.captured_at DESC
    """)
    result = await conn.execute(stmt, {"record_id": record_id})
    rows = result.mappings().all()

    if not rows:
        return None

    best = None
    best_priority = (999, 999)
    for r in rows:
        d = dict(r)
        role_rank = SOURCE_ROLE_PRIORITY.get(d["role"], 2)
        no_snap = 0 if d["snapshot_path"] else 1
        priority = (role_rank, no_snap)
        if best is None or priority < best_priority:
            best = d
            best_priority = priority

    if best is not None:
        raw = best.get("metadata")
        best["metadata"] = json.loads(raw) if raw else {}
    return best


async def get_record_sources(
    conn: AsyncConnection,
    record_id: int,
) -> list[dict]:
    """Return provenance sources for a record, newest first."""
    stmt = text("""
        SELECT s.id, st.slug AS source_type, st.label AS source_label,
               s.snapshot_path, s.url, s.captured_at, s.ingested_at,
               s.metadata, rs.role
        FROM record_sources rs
        JOIN sources s ON s.id = rs.source_id
        JOIN source_types st ON st.id = s.source_type_id
        WHERE rs.record_id = :record_id
        ORDER BY s.captured_at DESC
    """)
    result = await conn.execute(stmt, {"record_id": record_id})
    results = []
    for r in result.mappings().all():
        d = dict(r)
        raw = d.get("metadata")
        d["metadata"] = json.loads(raw) if raw else {}
        results.append(d)
    return results
