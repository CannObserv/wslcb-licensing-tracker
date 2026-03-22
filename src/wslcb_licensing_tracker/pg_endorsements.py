"""Async PostgreSQL endorsement pipeline for WSLCB licensing tracker.

Async equivalent of endorsements.py. Ports all public functions to async
SQLAlchemy Core. Uses table objects from models.py.

Handles three raw license_type formats:
- Numeric code: ``"450,"`` → look up code in endorsement_codes
- Code + name: ``"450, GROCERY STORE - BEER/WINE"`` → extract code + name
- Text names: ``"GROCERY STORE - BEER/WINE; SNACK BAR"`` → split on semicolons

See Also:
- ``endorsements`` — SQLite synchronous original
- ``pg_pipeline`` — async ingestion pipeline (calls process_record)
"""

import logging
import re
from datetime import UTC, datetime

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import (
    endorsement_aliases,
    endorsement_codes,
    license_endorsements,
    license_records,
    record_endorsements,
    record_enrichments,
)

logger = logging.getLogger(__name__)

# Matches the historical "CODE, NAME" format used in approved/discontinued
# sections before ~2025 (e.g. "450, GROCERY STORE - BEER/WINE").
# The first capturing group is the numeric code, the second is the name.
# Handles names that themselves contain commas (e.g. "< 250,000 LITERS").
CODE_NAME_RE = re.compile(r"^(\d+),\s+(.+)$")

# Endorsement enrichment version — bump when processing logic changes.
_ENDORSEMENT_REPROCESS_VERSION = 2


# ---
# Endorsement CRUD helpers
# ---


async def ensure_endorsement(conn: AsyncConnection, name: str) -> int:
    """Return the id for *name*, creating the row if needed.

    Names are upper-cased before lookup/insert for consistency.
    Uses ON CONFLICT DO NOTHING + RETURNING + fallback SELECT for idempotency.
    """
    name = name.upper()
    stmt = (
        pg_insert(license_endorsements)
        .values(name=name)
        .on_conflict_do_nothing(index_elements=["name"])
        .returning(license_endorsements.c.id)
    )
    row_id = (await conn.execute(stmt)).scalar_one_or_none()
    if row_id is None:
        row_id = (
            await conn.execute(
                select(license_endorsements.c.id).where(license_endorsements.c.name == name)
            )
        ).scalar_one()
    return row_id


async def _link_endorsement(conn: AsyncConnection, record_id: int, endorsement_id: int) -> None:
    """Insert a record↔endorsement link, ignoring duplicates."""
    stmt = (
        pg_insert(record_endorsements)
        .values(record_id=record_id, endorsement_id=endorsement_id)
        .on_conflict_do_nothing()
    )
    await conn.execute(stmt)


# ---
# Processing helpers
# ---


async def _process_code(
    conn: AsyncConnection,
    record_id: int,
    code: str,
    fallback_name: str | None = None,
) -> int:
    """Resolve a numeric code to endorsements and link to *record_id*.

    If the code is already mapped in ``endorsement_codes``, use those
    mappings. Otherwise, if *fallback_name* is provided (from a ``CODE, NAME``
    raw value), create the endorsement from the name and register the code
    mapping. As a last resort, create a numeric placeholder endorsement.

    Returns the number of endorsements linked.
    """
    rows = (
        await conn.execute(
            select(endorsement_codes.c.endorsement_id).where(endorsement_codes.c.code == code)
        )
    ).fetchall()

    if rows:
        for r in rows:
            await _link_endorsement(conn, record_id, r[0])
        return len(rows)

    # Unknown code — use fallback name from CODE, NAME if available
    if fallback_name:
        eid = await ensure_endorsement(conn, fallback_name)
        await conn.execute(
            pg_insert(endorsement_codes)
            .values(code=code, endorsement_id=eid)
            .on_conflict_do_nothing()
        )
        await _link_endorsement(conn, record_id, eid)
        return 1

    # No name available — create a numeric placeholder
    logger.info("Unknown code '%s' for record %d; creating placeholder.", code, record_id)
    eid = await ensure_endorsement(conn, code)
    await conn.execute(
        pg_insert(endorsement_codes).values(code=code, endorsement_id=eid).on_conflict_do_nothing()
    )
    await _link_endorsement(conn, record_id, eid)
    return 1


async def _sync_resolved_endorsements(conn: AsyncConnection, record_id: int) -> None:
    """Write the denormalized resolved_endorsements string for *record_id*.

    Joins current ``record_endorsements`` rows to ``license_endorsements``
    and stores the semicolon-joined canonical names in
    ``license_records.resolved_endorsements``. Called by :func:`process_record`
    after all endorsement links are inserted.
    """
    stmt = text("""
        UPDATE license_records
        SET resolved_endorsements = COALESCE((
            SELECT STRING_AGG(
                COALESCE(
                    (SELECT le2.name FROM license_endorsements le2
                     JOIN endorsement_aliases ea
                       ON ea.canonical_endorsement_id = le2.id
                     WHERE ea.endorsement_id = le.id
                     LIMIT 1),
                    le.name
                ), '; ' ORDER BY le.name
            )
            FROM record_endorsements re
            JOIN license_endorsements le ON le.id = re.endorsement_id
            WHERE re.record_id = :record_id
        ), '')
        WHERE id = :record_id
    """)
    await conn.execute(stmt, {"record_id": record_id})


async def process_record(conn: AsyncConnection, record_id: int, raw_license_type: str) -> int:
    """Parse a record's raw license_type and create endorsement links.

    Idempotent: deletes any existing ``record_endorsements`` rows for
    *record_id* before inserting fresh ones.

    Handles three formats:
    - Numeric code: ``"450,"`` → look up code in endorsement_codes
    - Code + name: ``"450, GROCERY STORE - BEER/WINE"`` → extract code,
      use name as the endorsement (and register the code mapping)
    - Text names: ``"GROCERY STORE - BEER/WINE; SNACK BAR"`` → split on
      semicolons, each part is an endorsement name

    Returns the number of endorsements linked.
    """
    if not raw_license_type:
        return 0

    # Delete existing links so re-processing is idempotent.
    await conn.execute(
        delete(record_endorsements).where(record_endorsements.c.record_id == record_id)
    )

    cleaned = raw_license_type.rstrip(",").strip()

    # Pure numeric code (e.g. "450" after stripping trailing comma)
    if cleaned.isdigit():
        linked = await _process_code(conn, record_id, cleaned)
        await _sync_resolved_endorsements(conn, record_id)
        return linked

    # Historical "CODE, NAME" format (e.g. "450, GROCERY STORE - BEER/WINE")
    m = CODE_NAME_RE.match(cleaned)
    if m:
        code, name = m.group(1), m.group(2).strip()
        linked = await _process_code(conn, record_id, code, fallback_name=name)
        await _sync_resolved_endorsements(conn, record_id)
        return linked

    # Text — split on semicolons
    linked = 0
    for part in raw_license_type.split(";"):
        name = part.strip()
        if name:
            eid = await ensure_endorsement(conn, name)
            await _link_endorsement(conn, record_id, eid)
            linked += 1
    await _sync_resolved_endorsements(conn, record_id)
    return linked


async def reprocess_endorsements(
    conn: AsyncConnection,
    *,
    record_id: int | None = None,
    code: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Regenerate record_endorsements for all or a subset of records.

    Treats ``record_endorsements`` as derived data: existing rows are deleted
    and rebuilt from ``license_records.license_type`` using current
    ``endorsement_codes`` mappings. The ``record_enrichments`` version stamp
    is updated to ``_ENDORSEMENT_REPROCESS_VERSION`` for every processed record.

    Parameters
    ----------
    conn:
        Open database connection. Caller is responsible for committing.
    record_id:
        If given, only reprocess this single record.
    code:
        If given, only reprocess records whose ``license_type`` matches this
        numeric code (handles both ``"450,"`` and ``"450, NAME"`` formats).
    dry_run:
        If True, compute what would be done and return counts without changes.

    Returns:
    -------
    dict
        ``{"records_processed": int, "endorsements_linked": int}``
    """
    if record_id is not None:
        rows = (
            await conn.execute(
                select(license_records.c.id, license_records.c.license_type).where(
                    license_records.c.id == record_id
                )
            )
        ).fetchall()
    elif code is not None:
        code_stripped = code.rstrip(",").strip()
        code_prefix = f"{code_stripped}, %"
        rows = (
            await conn.execute(
                select(license_records.c.id, license_records.c.license_type).where(
                    (func.rtrim(license_records.c.license_type, ",") == code_stripped)
                    | license_records.c.license_type.like(code_prefix)
                )
            )
        ).fetchall()
    else:
        rows = (
            await conn.execute(select(license_records.c.id, license_records.c.license_type))
        ).fetchall()

    records_processed = 0
    endorsements_linked = 0
    now = datetime.now(UTC)

    for row in rows:
        rid, license_type = row[0], row[1]
        if not license_type:
            continue

        if dry_run:
            records_processed += 1
            continue

        linked = await process_record(conn, rid, license_type)
        endorsements_linked += linked
        records_processed += 1

        stmt = (
            pg_insert(record_enrichments)
            .values(
                record_id=rid,
                step="endorsements",
                completed_at=now,
                version=str(_ENDORSEMENT_REPROCESS_VERSION),
            )
            .on_conflict_do_update(
                index_elements=["record_id", "step"],
                set_={
                    "completed_at": now,
                    "version": str(_ENDORSEMENT_REPROCESS_VERSION),
                },
            )
        )
        await conn.execute(stmt)

    if dry_run:
        logger.info(
            "reprocess_endorsements (dry-run): would process %d record(s).",
            records_processed,
        )
    else:
        logger.info(
            "reprocess_endorsements: processed %d record(s), linked %d endorsement(s).",
            records_processed,
            endorsements_linked,
        )

    return {"records_processed": records_processed, "endorsements_linked": endorsements_linked}


# ---
# Query helpers
# ---


async def get_endorsement_options(conn: AsyncConnection) -> list[str]:
    """Distinct canonical endorsement names linked to at least one record.

    Aliases are resolved: if an endorsement has an alias row, its canonical
    name is used instead. Deduplicates the filter dropdown so each semantic
    endorsement appears only once.
    """
    le = license_endorsements.alias("le")
    canonical_le = license_endorsements.alias("canonical_le")
    stmt = (
        select(func.coalesce(canonical_le.c.name, le.c.name).label("display_name"))
        .distinct()
        .select_from(le)
        .join(record_endorsements, record_endorsements.c.endorsement_id == le.c.id)
        .outerjoin(endorsement_aliases, endorsement_aliases.c.endorsement_id == le.c.id)
        .outerjoin(
            canonical_le,
            canonical_le.c.id == endorsement_aliases.c.canonical_endorsement_id,
        )
        .order_by(text("display_name"))
    )
    rows = (await conn.execute(stmt)).fetchall()
    return [r[0] for r in rows]


async def get_record_endorsements(
    conn: AsyncConnection, record_ids: list[int]
) -> dict[int, list[str]]:
    """Batch-fetch canonical endorsement names for a list of record ids.

    Alias resolution is applied: if the endorsement linked to a record has an
    alias row, the canonical name is returned instead of the variant name.
    """
    if not record_ids:
        return {}

    le = license_endorsements.alias("le")
    canonical_le = license_endorsements.alias("canonical_le")

    result: dict[int, list[str]] = {rid: [] for rid in record_ids}

    # Process in chunks to avoid huge IN clauses
    chunk_size = 500
    for i in range(0, len(record_ids), chunk_size):
        batch = record_ids[i : i + chunk_size]
        stmt = (
            select(
                record_endorsements.c.record_id,
                func.coalesce(canonical_le.c.name, le.c.name).label("display_name"),
            )
            .select_from(record_endorsements)
            .join(le, le.c.id == record_endorsements.c.endorsement_id)
            .outerjoin(endorsement_aliases, endorsement_aliases.c.endorsement_id == le.c.id)
            .outerjoin(
                canonical_le,
                canonical_le.c.id == endorsement_aliases.c.canonical_endorsement_id,
            )
            .where(record_endorsements.c.record_id.in_(batch))
            .order_by(record_endorsements.c.record_id, text("display_name"))
        )
        rows = (await conn.execute(stmt)).mappings().all()
        for r in rows:
            result[r["record_id"]].append(r["display_name"])

    return result


# ---
# Alias management
# ---


async def resolve_endorsement(conn: AsyncConnection, endorsement_id: int) -> int:
    """Return the canonical endorsement ID for *endorsement_id*.

    If *endorsement_id* has an alias row, the ``canonical_endorsement_id``
    is returned. Follows the alias chain up to depth 5 for safety.
    Otherwise the same ID is returned unchanged.
    """
    current = endorsement_id
    for _ in range(5):
        row = (
            await conn.execute(
                select(endorsement_aliases.c.canonical_endorsement_id).where(
                    endorsement_aliases.c.endorsement_id == current
                )
            )
        ).scalar_one_or_none()
        if row is None:
            return current
        current = row
    return current


async def set_canonical_endorsement(
    conn: AsyncConnection,
    canonical_id: int,
    endorsement_ids: list[int],
    created_by: str = "admin",
) -> int:
    """Create alias rows mapping each id in *endorsement_ids* to *canonical_id*.

    Uses ON CONFLICT DO UPDATE so re-running is idempotent: existing alias rows
    for the same ``endorsement_id`` are updated in place.

    Parameters
    ----------
    conn:
        Open database connection — caller must commit.
    canonical_id:
        Primary key of the endorsement to mark as canonical.
    endorsement_ids:
        Primary keys of endorsements that should alias to *canonical_id*.
    created_by:
        Admin email to record in the audit trail column.

    Returns:
    -------
    int
        Number of alias rows written (created or updated).
    """
    now = datetime.now(UTC)
    written = 0
    for eid in endorsement_ids:
        if eid == canonical_id:
            continue  # never alias an endorsement to itself
        stmt = (
            pg_insert(endorsement_aliases)
            .values(
                endorsement_id=eid,
                canonical_endorsement_id=canonical_id,
                created_at=now,
                created_by=created_by,
            )
            .on_conflict_do_update(
                index_elements=["endorsement_id"],
                set_={
                    "canonical_endorsement_id": canonical_id,
                    "created_at": now,
                },
            )
        )
        await conn.execute(stmt)
        written += 1
    logger.info(
        "set_canonical: %d alias(es) → endorsement#%d by %s",
        written,
        canonical_id,
        created_by,
    )
    return written


async def remove_alias(
    conn: AsyncConnection,
    endorsement_id: int,
    removed_by: str | None = None,
) -> None:
    """Remove the alias row for *endorsement_id*, making it standalone.

    Parameters
    ----------
    conn:
        Open database connection — caller must commit.
    endorsement_id:
        Primary key of the variant endorsement whose alias should be deleted.
    removed_by:
        Admin email for logging.

    Raises:
    ------
    ValueError
        If *endorsement_id* has no alias row.
    """
    result = await conn.execute(
        delete(endorsement_aliases).where(endorsement_aliases.c.endorsement_id == endorsement_id)
    )
    if result.rowcount == 0:
        msg = f"endorsement #{endorsement_id} has no alias to remove"
        raise ValueError(msg)
    logger.info(
        "remove_alias: endorsement#%d unaliased by %s",
        endorsement_id,
        removed_by,
    )


async def rename_endorsement(
    conn: AsyncConnection,
    endorsement_id: int,
    new_name: str,
    created_by: str | None = None,
) -> int:
    """Assign a text name to an endorsement (typically a bare numeric code).

    Creates a new ``license_endorsements`` row with *new_name* (or reuses one
    that already exists), then creates an alias row mapping *endorsement_id*
    to the new (canonical) ID. The original row is left intact for history.

    Parameters
    ----------
    conn:
        Open database connection — caller must commit.
    endorsement_id:
        Primary key of the endorsement to rename (the variant / bare code).
    new_name:
        Human-readable canonical name.
    created_by:
        Admin email for the audit trail.

    Returns:
    -------
    int
        Primary key of the canonical (named) endorsement.
    """
    if not new_name:
        msg = "new_name must be a non-empty string"
        raise ValueError(msg)

    canonical_id = await ensure_endorsement(conn, new_name)

    await set_canonical_endorsement(
        conn,
        canonical_id=canonical_id,
        endorsement_ids=[endorsement_id],
        created_by=created_by or "admin",
    )
    logger.info(
        "rename_endorsement: #%d → '%s' (#%d) by %s",
        endorsement_id,
        new_name,
        canonical_id,
        created_by,
    )
    return canonical_id


async def get_endorsement_groups(conn: AsyncConnection) -> list[dict]:
    """Return all endorsements grouped by numeric code for the admin UI.

    Returns a list of group dicts, each with keys:

    - ``code`` — the WSLCB numeric code string (or ``None`` for ungrouped)
    - ``endorsements`` — list of endorsement dicts with keys:
        - ``id``, ``name``, ``record_count``, ``is_canonical``, ``is_variant``,
          ``canonical_id`` (None if not a variant)

    Groups are ordered by code (numeric), with the ungrouped section last.
    """
    # Fetch all endorsements with record counts
    stmt = (
        select(
            license_endorsements.c.id,
            license_endorsements.c.name,
            func.count(record_endorsements.c.record_id).label("record_count"),
        )
        .select_from(license_endorsements)
        .outerjoin(
            record_endorsements,
            record_endorsements.c.endorsement_id == license_endorsements.c.id,
        )
        .group_by(license_endorsements.c.id)
        .order_by(license_endorsements.c.name)
    )
    rows = (await conn.execute(stmt)).mappings().all()

    # Fetch alias map: variant_id → canonical_id
    alias_rows = (
        await conn.execute(
            select(
                endorsement_aliases.c.endorsement_id,
                endorsement_aliases.c.canonical_endorsement_id,
            )
        )
    ).fetchall()
    alias_map: dict[int, int] = {r[0]: r[1] for r in alias_rows}
    canonical_ids: set[int] = set(alias_map.values())

    # Fetch code memberships: endorsement_id → list[code]
    code_rows = (
        await conn.execute(select(endorsement_codes.c.endorsement_id, endorsement_codes.c.code))
    ).fetchall()
    eid_to_codes: dict[int, list[str]] = {}
    for eid, code in code_rows:
        eid_to_codes.setdefault(eid, []).append(code)

    # Build per-code buckets
    code_buckets: dict[str, list[dict]] = {}
    ungrouped: list[dict] = []

    for row in rows:
        eid = row["id"]
        name = row["name"]
        count = row["record_count"]
        entry = {
            "id": eid,
            "name": name,
            "record_count": count,
            "is_canonical": eid in canonical_ids,
            "is_variant": eid in alias_map,
            "canonical_id": alias_map.get(eid),
        }
        codes = eid_to_codes.get(eid, [])
        if codes:
            for code in codes:
                code_buckets.setdefault(code, []).append(dict(entry))
        else:
            ungrouped.append(dict(entry))

    def _code_sort_key(code: str) -> tuple:
        try:
            return (0, int(code))
        except ValueError:
            return (1, code)

    groups: list[dict] = [
        {"code": code, "endorsements": entries}
        for code, entries in sorted(code_buckets.items(), key=lambda kv: _code_sort_key(kv[0]))
    ]
    if ungrouped:
        groups.append({"code": None, "endorsements": ungrouped})

    return groups
