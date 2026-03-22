"""Async PostgreSQL entity normalization for WSLCB licensing tracker.

Async equivalent of entities.py. Ports all public functions to async
SQLAlchemy Core. Uses table objects from models.py.

Manages the ``entities`` and ``record_entities`` tables — extracting
individual people and organizations from semicolon-delimited applicant
strings, deduplicating by name, and linking them to license records.
"""

import logging
import re
from datetime import UTC, datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import entities, license_records, record_enrichments, record_entities
from .text_utils import clean_entity_name, strip_duplicate_marker

logger = logging.getLogger(__name__)

# Enrichment version written by reprocess_entities.
# Bump this integer when entity processing logic changes.
_ENTITY_REPROCESS_VERSION = 2

# Meta-labels that WSLCB embeds in applicant lists as truncation notices.
# These are not real people or organizations and must be excluded from entity creation.
ADDITIONAL_NAMES_MARKERS: frozenset[str] = frozenset(
    {
        "ADDITIONAL NAMES ON FILE",
        "ADDTIONAL NAMES ON FILE",  # typo variant present in WSLCB source
    }
)

# Patterns that indicate an organization rather than a person.
_ORG_PATTERNS = re.compile(
    r"\b(LLC|L\.?L\.?C\.?|INC\.?|CORP\.?|CORPORATION|TRUST|LTD\.?|LIMITED"
    r"|PARTNERS|PARTNERSHIP|HOLDINGS|GROUP|ENTERPRISE|ENTERPRISES"
    r"|ASSOCIATION|FOUNDATION|COMPANY|CO\.|L\.?P\.?)\b"
)


def _classify_entity_type(name: str) -> str:
    """Classify an entity name as 'person' or 'organization'."""
    return "organization" if _ORG_PATTERNS.search(name) else "person"


async def get_or_create_entity(conn: AsyncConnection, name: str, entity_type: str = "") -> int:
    """Return the entity id for *name*, creating if needed.

    Names are cleaned via ``clean_entity_name`` and uppercased.
    Uses ON CONFLICT DO NOTHING + RETURNING + fallback SELECT for idempotency.

    Raises ``ValueError`` if *name* is empty after cleaning.
    """
    normalized = clean_entity_name(name)
    if not normalized:
        msg = "Entity name must not be empty"
        raise ValueError(msg)

    resolved_type = entity_type or _classify_entity_type(normalized)

    stmt = (
        pg_insert(entities)
        .values(name=normalized, entity_type=resolved_type)
        .on_conflict_do_nothing(index_elements=["name"])
        .returning(entities.c.id)
    )
    row_id = (await conn.execute(stmt)).scalar_one_or_none()
    if row_id is None:
        row_id = (
            await conn.execute(
                select(entities.c.id).where(func.lower(entities.c.name) == normalized.lower())
            )
        ).scalar_one()
    return row_id


async def parse_and_link_entities(
    conn: AsyncConnection,
    record_id: int,
    applicants_str: str,
    role: str = "applicant",
    *,
    delete_existing: bool = False,
) -> int:
    """Split applicants string, skip first element (business name), create entities and link.

    Assigns contiguous 0-based positions to successfully linked entities.
    Returns the number of entities linked.

    Parameters
    ----------
    delete_existing:
        If True, delete existing ``record_entities`` rows for this
        ``(record_id, role)`` pair before inserting (idempotent mode).
    """
    if delete_existing:
        await conn.execute(
            delete(record_entities).where(
                (record_entities.c.record_id == record_id) & (record_entities.c.role == role)
            )
        )

    if not applicants_str or ";" not in applicants_str:
        return 0

    parts = [p.strip() for p in applicants_str.split(";")]

    # First element is the business name — skip it.
    seen_names: set[str] = set()
    entity_names: list[str] = []
    for raw in parts[1:]:
        if not raw:
            continue
        clean = clean_entity_name(strip_duplicate_marker(raw))
        if clean and clean not in seen_names:
            seen_names.add(clean)
            entity_names.append(clean)

    linked = 0
    for name in entity_names:
        if name in ADDITIONAL_NAMES_MARKERS:
            logger.debug("Skipping meta-label %r in record %d (role %s)", name, record_id, role)
            continue

        entity_id = await get_or_create_entity(conn, name)
        stmt = (
            pg_insert(record_entities)
            .values(record_id=record_id, entity_id=entity_id, role=role, position=linked)
            .on_conflict_do_nothing()
        )
        result = await conn.execute(stmt)
        linked += result.rowcount

    return linked


async def get_record_entities(
    conn: AsyncConnection, record_ids: list[int]
) -> dict[int, dict[str, list[dict]]]:
    """Batch-fetch entities for a list of record ids.

    Returns ``{record_id: {"applicant": [...], "previous_applicant": [...]}}``.
    Each entity dict has keys ``id``, ``name``, ``entity_type``.
    """
    if not record_ids:
        return {}

    result: dict[int, dict[str, list[dict]]] = {
        rid: {"applicant": [], "previous_applicant": []} for rid in record_ids
    }

    chunk_size = 500
    for i in range(0, len(record_ids), chunk_size):
        batch = record_ids[i : i + chunk_size]
        stmt = (
            select(
                record_entities.c.record_id,
                record_entities.c.role,
                record_entities.c.position,
                entities.c.id.label("entity_id"),
                entities.c.name,
                entities.c.entity_type,
            )
            .select_from(record_entities)
            .join(entities, entities.c.id == record_entities.c.entity_id)
            .where(record_entities.c.record_id.in_(batch))
            .order_by(
                record_entities.c.record_id,
                record_entities.c.role,
                record_entities.c.position,
            )
        )
        rows = (await conn.execute(stmt)).mappings().all()
        for r in rows:
            rid = r["record_id"]
            role = r["role"]
            if rid not in result:
                result[rid] = {"applicant": [], "previous_applicant": []}
            if role not in result[rid]:
                result[rid][role] = []
            result[rid][role].append(
                {
                    "id": r["entity_id"],
                    "name": r["name"],
                    "entity_type": r["entity_type"],
                }
            )

    return result


async def merge_duplicate_entities(conn: AsyncConnection) -> int:
    """Find and merge entities whose names differ only by casing (case-insensitive dupes).

    For each group of entities sharing the same ``lower(name)``, keeps the
    one with the lowest id as canonical and re-links all ``record_entities``
    rows from the others to it, then deletes the duplicates.

    Returns the number of entities merged (deleted).
    """
    # Find case-insensitive duplicates: groups with more than one member
    subq = (
        select(
            func.lower(entities.c.name).label("lower_name"),
            func.min(entities.c.id).label("canonical_id"),
            func.count(entities.c.id).label("cnt"),
        )
        .group_by(func.lower(entities.c.name))
        .having(func.count(entities.c.id) > 1)
        .subquery()
    )

    dup_rows = (
        await conn.execute(
            select(entities.c.id, entities.c.name, subq.c.canonical_id)
            .join(subq, func.lower(entities.c.name) == subq.c.lower_name)
            .where(entities.c.id != subq.c.canonical_id)
        )
    ).fetchall()

    merged = 0
    for dirty_id, dirty_name, canon_id in dup_rows:
        # Fetch links for the duplicate entity
        dirty_links = (
            await conn.execute(
                select(
                    record_entities.c.record_id,
                    record_entities.c.role,
                    record_entities.c.position,
                ).where(record_entities.c.entity_id == dirty_id)
            )
        ).fetchall()

        for link in dirty_links:
            rid, role, pos = link[0], link[1], link[2]
            # Check if canonical already has a link for this (record, role)
            existing = (
                await conn.execute(
                    select(record_entities.c.position).where(
                        (record_entities.c.record_id == rid)
                        & (record_entities.c.entity_id == canon_id)
                        & (record_entities.c.role == role)
                    )
                )
            ).scalar_one_or_none()

            if existing is not None:
                # Conflict — keep lower position on canonical
                if pos < existing:
                    await conn.execute(
                        update(record_entities)
                        .where(
                            (record_entities.c.record_id == rid)
                            & (record_entities.c.entity_id == canon_id)
                            & (record_entities.c.role == role)
                        )
                        .values(position=pos)
                    )
            else:
                await conn.execute(
                    update(record_entities)
                    .where(
                        (record_entities.c.record_id == rid)
                        & (record_entities.c.entity_id == dirty_id)
                        & (record_entities.c.role == role)
                    )
                    .values(entity_id=canon_id)
                )

        # Delete remaining dirty links (conflict cases) and the entity itself
        await conn.execute(delete(record_entities).where(record_entities.c.entity_id == dirty_id))
        await conn.execute(delete(entities).where(entities.c.id == dirty_id))
        logger.info("Merged entity %d %r → %d", dirty_id, dirty_name, canon_id)
        merged += 1

    if merged:
        logger.info("Merged %d duplicate entities", merged)

    return merged


async def reprocess_entities(
    conn: AsyncConnection,
    *,
    record_id: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Regenerate ``record_entities`` for all or a subset of records.

    Treats ``record_entities`` as derived data: existing rows for each
    targeted ``(record_id, role)`` pair are deleted and rebuilt from
    ``license_records.applicants`` / ``previous_applicants`` using the
    current entity-normalization logic.  The ``record_enrichments``
    version stamp is updated to ``_ENTITY_REPROCESS_VERSION`` for every
    processed record.

    Parameters
    ----------
    conn:
        Open database connection. Caller is responsible for committing.
    record_id:
        If given, only reprocess this single record.
    dry_run:
        If True, compute what would be done and return counts without changes.

    Returns:
    -------
    dict
        ``{"records_processed": int, "entities_linked": int}``
    """
    if record_id is not None:
        stmt = select(
            license_records.c.id,
            license_records.c.applicants,
            license_records.c.previous_applicants,
        ).where(license_records.c.id == record_id)
    else:
        stmt = select(
            license_records.c.id,
            license_records.c.applicants,
            license_records.c.previous_applicants,
        )

    rows = (await conn.execute(stmt)).fetchall()

    records_processed = 0
    entities_linked = 0
    now = datetime.now(UTC).isoformat()

    for row in rows:
        rid = row[0]
        applicants = row[1] or ""
        previous_applicants = row[2] or ""

        if dry_run:
            records_processed += 1
            continue

        linked = await parse_and_link_entities(
            conn, rid, applicants, "applicant", delete_existing=True
        )
        linked += await parse_and_link_entities(
            conn, rid, previous_applicants, "previous_applicant", delete_existing=True
        )
        entities_linked += linked
        records_processed += 1

        enrich_stmt = (
            pg_insert(record_enrichments)
            .values(
                record_id=rid,
                step="entities",
                completed_at=now,
                version=str(_ENTITY_REPROCESS_VERSION),
            )
            .on_conflict_do_update(
                index_elements=["record_id", "step"],
                set_={
                    "completed_at": now,
                    "version": str(_ENTITY_REPROCESS_VERSION),
                },
            )
        )
        await conn.execute(enrich_stmt)

    if dry_run:
        logger.info(
            "reprocess_entities (dry-run): would process %d record(s).",
            records_processed,
        )
    else:
        logger.info(
            "reprocess_entities: processed %d record(s), linked %d entity link(s).",
            records_processed,
            entities_linked,
        )

    return {"records_processed": records_processed, "entities_linked": entities_linked}


async def get_entity_by_id(
    conn: AsyncConnection,
    entity_id: int,
) -> dict | None:
    """Fetch a single entity by id."""
    row = (
        (
            await conn.execute(
                select(
                    entities.c.id,
                    entities.c.name,
                    entities.c.entity_type,
                    entities.c.created_at,
                ).where(entities.c.id == entity_id)
            )
        )
        .mappings()
        .one_or_none()
    )
    return dict(row) if row else None


async def backfill_entities(conn: AsyncConnection) -> int:
    """Backfill record_entities for records missing entity rows but having semicolons in applicants.

    Async port of entities.backfill_entities. Returns number of records processed.
    Caller must commit.
    """
    stmt = (
        select(
            license_records.c.id,
            license_records.c.applicants,
            license_records.c.previous_applicants,
        )
        .outerjoin(
            record_entities,
            record_entities.c.record_id == license_records.c.id,
        )
        .where(record_entities.c.record_id.is_(None))
        .where(
            (license_records.c.applicants.contains(";"))
            | (license_records.c.previous_applicants.contains(";"))
        )
    )

    rows = (await conn.execute(stmt)).fetchall()
    processed = 0
    for row in rows:
        r_id, r_applicants, r_prev = row[0], row[1] or "", row[2] or ""
        await parse_and_link_entities(conn, r_id, r_applicants, "applicant")
        if r_prev:
            await parse_and_link_entities(conn, r_id, r_prev, "previous_applicant")
        processed += 1

    if processed:
        await merge_duplicate_entities(conn)

    logger.info("backfill_entities: processed %d record(s)", processed)
    return processed
