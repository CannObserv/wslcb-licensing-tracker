"""Async PostgreSQL endorsement seeding, repair, and backfill.

Async equivalent of endorsements_seed.py. Ports all public functions to async
SQLAlchemy Core. Uses table objects from models.py.

- ``seed_endorsements`` — populate endorsement_codes from SEED_CODE_MAP
- ``merge_mixed_case_endorsements`` — repair mixed-case duplicates
- ``repair_code_name_endorsements`` — migrate legacy "CODE, NAME" endorsements
- ``backfill`` — process records with no endorsement links yet
- ``discover_code_mappings`` — cross-reference license numbers to learn new mappings

All functions follow the caller-commits convention: they do not call
``await conn.commit()`` themselves.
"""

import logging

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .endorsements_seed import SEED_CODE_MAP
from .models import (
    endorsement_codes,
    license_endorsements,
    record_endorsements,
)
from .pg_endorsements import CODE_NAME_RE, ensure_endorsement, process_record

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


async def seed_endorsements(conn: AsyncConnection) -> int:
    """Populate license_endorsements and endorsement_codes from SEED_CODE_MAP.

    Safe to call repeatedly — skips existing rows. After seeding, merges
    any placeholder endorsements (where the endorsement name equals the
    numeric code) that now have real mappings.

    Returns the number of new code mappings inserted. Caller must commit.
    """
    inserted = 0
    for code, names in SEED_CODE_MAP.items():
        for name in names:
            eid = await ensure_endorsement(conn, name)
            stmt = (
                pg_insert(endorsement_codes)
                .values(code=code, endorsement_id=eid)
                .on_conflict_do_nothing()
            )
            result = await conn.execute(stmt)
            inserted += result.rowcount

    # Merge any placeholder endorsements now that seed mappings exist.
    await _merge_seeded_placeholders(conn)

    return inserted


async def _merge_seeded_placeholders(conn: AsyncConnection) -> int:
    """Merge placeholder endorsements that now have real seed mappings.

    A placeholder endorsement has ``name == code`` (e.g. endorsement named
    ``"331"`` for code ``"331"``). If ``seed_endorsements`` has since
    registered a real mapping for that code, migrate all record links from
    the placeholder to the real endorsement(s) and delete the placeholder.

    Returns the number of record links migrated.
    """
    stmt = text("""
        SELECT le.id, le.name
        FROM license_endorsements le
        JOIN endorsement_codes ec ON ec.endorsement_id = le.id AND ec.code = le.name
        WHERE le.name ~ :digit_re
          AND EXISTS (
              SELECT 1 FROM endorsement_codes ec2
              JOIN license_endorsements le2 ON le2.id = ec2.endorsement_id
              WHERE ec2.code = le.name AND le2.name != le.name
          )
    """)
    rows = (await conn.execute(stmt, {"digit_re": r"^\d+$"})).fetchall()
    if not rows:
        return 0

    migrated = 0
    for pid, code in rows:
        real_stmt = text("""
            SELECT ec.endorsement_id FROM endorsement_codes ec
            JOIN license_endorsements le ON le.id = ec.endorsement_id
            WHERE ec.code = :code AND le.name != :code
        """)
        real_eids = [r[0] for r in (await conn.execute(real_stmt, {"code": code})).fetchall()]
        if not real_eids:
            continue

        count = await _merge_endorsement(conn, pid, real_eids[0])
        migrated += count
        if len(real_eids) > 1:
            rec_rows = (
                await conn.execute(
                    select(record_endorsements.c.record_id).where(
                        record_endorsements.c.endorsement_id == real_eids[0]
                    )
                )
            ).fetchall()
            for rec in rec_rows:
                for eid in real_eids[1:]:
                    await _link_endorsement(conn, rec[0], eid)

    if migrated:
        logger.info(
            "Merged %d record link(s) from %d placeholder endorsement(s).",
            migrated,
            len(rows),
        )
    return migrated


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _link_endorsement(conn: AsyncConnection, record_id: int, endorsement_id: int) -> None:
    """Insert a record↔endorsement link, ignoring duplicates."""
    stmt = (
        pg_insert(record_endorsements)
        .values(record_id=record_id, endorsement_id=endorsement_id)
        .on_conflict_do_nothing()
    )
    await conn.execute(stmt)


async def _merge_endorsement(conn: AsyncConnection, src_id: int, dst_id: int) -> int:
    """Migrate all record links from src_id to dst_id, then delete src.

    Returns the number of record links migrated.
    """
    # Migrate record_endorsements rows
    rows = (
        await conn.execute(
            select(record_endorsements.c.record_id).where(
                record_endorsements.c.endorsement_id == src_id
            )
        )
    ).fetchall()
    count = 0
    for (record_id,) in rows:
        await _link_endorsement(conn, record_id, dst_id)
        count += 1

    # Delete old links from src
    await conn.execute(
        record_endorsements.delete().where(record_endorsements.c.endorsement_id == src_id)
    )

    # Migrate endorsement_codes rows
    await conn.execute(
        text("""
            UPDATE endorsement_codes SET endorsement_id = :dst
            WHERE endorsement_id = :src
              AND NOT EXISTS (
                  SELECT 1 FROM endorsement_codes
                  WHERE code = endorsement_codes.code AND endorsement_id = :dst
              )
        """),
        {"src": src_id, "dst": dst_id},
    )
    await conn.execute(
        endorsement_codes.delete().where(endorsement_codes.c.endorsement_id == src_id)
    )

    # Delete the old endorsement row
    await conn.execute(license_endorsements.delete().where(license_endorsements.c.id == src_id))

    return count


# ---------------------------------------------------------------------------
# Repair: merge mixed-case endorsement duplicates
# ---------------------------------------------------------------------------


async def merge_mixed_case_endorsements(conn: AsyncConnection) -> int:
    """Merge endorsements whose names differ only by case.

    For each endorsement where ``name != UPPER(name)`` and an UPPER
    counterpart already exists, migrate all record links and code mappings
    to the canonical (upper-case) row via ``_merge_endorsement()``, then
    delete the mixed-case row. If no upper-case counterpart exists, the
    mixed-case row is simply renamed in place.

    Returns the number of endorsements fixed. Caller must commit.
    """
    stmt = text("""
        SELECT id, name
        FROM license_endorsements
        WHERE name != UPPER(name)
    """)
    dupes = (await conn.execute(stmt)).fetchall()

    if not dupes:
        return 0

    for mixed_id, mixed_name in dupes:
        upper_name = mixed_name.upper()

        upper_row = (
            await conn.execute(
                select(license_endorsements.c.id).where(license_endorsements.c.name == upper_name)
            )
        ).fetchone()

        if not upper_row:
            await conn.execute(
                update(license_endorsements)
                .where(license_endorsements.c.id == mixed_id)
                .values(name=upper_name)
            )
            logger.info("Renamed endorsement %r -> %r (id=%d)", mixed_name, upper_name, mixed_id)
            continue

        await _merge_endorsement(conn, mixed_id, upper_row[0])
        logger.info(
            "Merged endorsement %r (id=%d) into %r (id=%d)",
            mixed_name,
            mixed_id,
            upper_name,
            upper_row[0],
        )

    return len(dupes)


# ---------------------------------------------------------------------------
# Repair: migrate "CODE, NAME" endorsements to proper names
# ---------------------------------------------------------------------------


async def repair_code_name_endorsements(conn: AsyncConnection) -> int:
    """Migrate record links from spurious ``CODE, NAME`` endorsements.

    Historical data used license_type values like ``"450, GROCERY STORE -
    BEER/WINE"`` which were stored as endorsement names verbatim. This
    function re-resolves each one: if the embedded code is already mapped
    in ``endorsement_codes``, migrate to those endorsements; otherwise
    use the embedded name (creating the endorsement if needed) and
    register the code mapping.

    Also cleans up bogus ``endorsement_codes`` rows whose code column
    contains spaces.

    Returns the number of record links migrated. Safe to call repeatedly.
    Caller must commit.
    """
    stmt = text("""
        SELECT id, name FROM license_endorsements
        WHERE name ~ :pattern
    """)
    bogus = (await conn.execute(stmt, {"pattern": r"^\d+,\s*.+$"})).fetchall()

    if not bogus:
        deleted = await _cleanup_space_codes(conn)
        if deleted:
            logger.info("Cleaned up %d bogus endorsement_codes row(s).", deleted)
        return 0

    migrated = 0
    for eid_old, full_name in bogus:
        m = CODE_NAME_RE.match(full_name)
        if not m:
            continue
        code, name = m.group(1), m.group(2).strip()

        mapped_stmt = text("""
            SELECT ec.endorsement_id FROM endorsement_codes ec
            JOIN license_endorsements le ON le.id = ec.endorsement_id
            WHERE ec.code = :code AND le.name != :full_name
        """)
        mapped_eids = [
            r[0]
            for r in (
                await conn.execute(mapped_stmt, {"code": code, "full_name": full_name})
            ).fetchall()
        ]

        if mapped_eids:
            target_eids = mapped_eids
        else:
            target_eid = await ensure_endorsement(conn, name)
            await conn.execute(
                pg_insert(endorsement_codes)
                .values(code=code, endorsement_id=target_eid)
                .on_conflict_do_nothing()
            )
            target_eids = [target_eid]

        migrated += await _merge_endorsement(conn, eid_old, target_eids[0])
        if len(target_eids) > 1:
            rec_rows = (
                await conn.execute(
                    select(record_endorsements.c.record_id).where(
                        record_endorsements.c.endorsement_id == target_eids[0]
                    )
                )
            ).fetchall()
            for (record_id,) in rec_rows:
                for tgt in target_eids[1:]:
                    await _link_endorsement(conn, record_id, tgt)

    await _cleanup_space_codes(conn)

    if migrated:
        logger.info(
            "Repaired %d record-endorsement link(s) from %d 'CODE, NAME' endorsement(s).",
            migrated,
            len(bogus),
        )
    return migrated


async def _cleanup_space_codes(conn: AsyncConnection) -> int:
    """Remove ``endorsement_codes`` rows whose code contains spaces.

    These are artifacts of ``discover_code_mappings()`` processing
    ``CODE, NAME`` license_type values.

    Returns the number of rows deleted.
    """
    result = await conn.execute(
        endorsement_codes.delete().where(endorsement_codes.c.code.like("% %"))
    )
    if result.rowcount:
        logger.info(
            "Removed %d bogus endorsement_codes row(s) with spaces in code.",
            result.rowcount,
        )
    return result.rowcount


# ---------------------------------------------------------------------------
# Backfill: process records with no endorsement links
# ---------------------------------------------------------------------------


async def backfill(conn: AsyncConnection) -> int:
    """Process all records that don't yet have endorsement links.

    Returns the number of records processed. Caller must commit.
    """
    stmt = text("""
        SELECT lr.id, lr.license_type
        FROM license_records lr
        LEFT JOIN record_endorsements re ON re.record_id = lr.id
        WHERE re.record_id IS NULL
          AND lr.license_type IS NOT NULL AND lr.license_type != ''
    """)
    rows = (await conn.execute(stmt)).fetchall()

    for record_id, license_type in rows:
        await process_record(conn, record_id, license_type)

    return len(rows)


# ---------------------------------------------------------------------------
# Discovery: cross-reference license numbers to learn new code→name mappings
# ---------------------------------------------------------------------------


async def discover_code_mappings(conn: AsyncConnection) -> dict[str, list[str]]:  # noqa: C901
    """Cross-reference license numbers to learn new code→name mappings.

    For each unmapped numeric code, find new_application records sharing
    the same license_number. When every matched text record contains the
    same endorsement(s), adopt that as the mapping.

    Returns {code: [name, ...]} for newly discovered mappings. Caller must commit.
    """
    mapped_stmt = text("""
        SELECT DISTINCT ec.code
        FROM endorsement_codes ec
        JOIN license_endorsements le ON le.id = ec.endorsement_id
        WHERE le.name != ec.code
    """)
    mapped = {r[0] for r in (await conn.execute(mapped_stmt)).fetchall()}

    all_codes_stmt = text("""
        SELECT DISTINCT license_type
        FROM license_records
        WHERE section_type IN ('approved', 'discontinued')
          AND license_type ~ :digit_re
    """)
    code_rows = (await conn.execute(all_codes_stmt, {"digit_re": r"^\d"})).fetchall()

    all_codes: set[str] = set()
    for (raw_type,) in code_rows:
        raw = raw_type.rstrip(",").strip()
        m = CODE_NAME_RE.match(raw)
        if m:
            all_codes.add(m.group(1))
        elif raw.isdigit():
            all_codes.add(raw)

    unmapped = [c for c in all_codes if c not in mapped]
    if not unmapped:
        return {}

    learned: dict[str, list[str]] = {}
    for code in unmapped:
        matches_stmt = text("""
            SELECT n.license_type AS text_type, COUNT(*) AS cnt
            FROM license_records a
            JOIN license_records n
                ON a.license_number = n.license_number
                AND n.section_type = 'new_application'
            WHERE SPLIT_PART(a.license_type, ',', 1) = :code
              AND a.section_type IN ('approved', 'discontinued')
            GROUP BY n.license_type
        """)
        matches = (await conn.execute(matches_stmt, {"code": code})).fetchall()
        if not matches:
            continue

        total = sum(r[1] for r in matches)
        type_freq: dict[str, int] = {}
        for text_type, cnt in matches:
            for t in text_type.split(";"):
                key = t.strip()
                type_freq[key] = type_freq.get(key, 0) + cnt

        always = [t for t, c in type_freq.items() if c == total and t]
        if not always:
            continue

        for name in always:
            eid = await ensure_endorsement(conn, name)
            await conn.execute(
                pg_insert(endorsement_codes)
                .values(code=code, endorsement_id=eid)
                .on_conflict_do_nothing()
            )
        learned[code] = always

    if learned:
        await _merge_placeholders(conn, learned)
    return learned


async def _merge_placeholders(conn: AsyncConnection, learned: dict[str, list[str]]) -> None:
    """If a code had a placeholder endorsement (name == code), migrate links."""
    for code, names in learned.items():
        placeholder = (
            await conn.execute(
                select(license_endorsements.c.id).where(license_endorsements.c.name == code)
            )
        ).fetchone()
        if not placeholder:
            continue
        pid = placeholder[0]
        first_eid = await ensure_endorsement(conn, names[0])
        await _merge_endorsement(conn, pid, first_eid)
        if len(names) > 1:
            rec_rows = (
                await conn.execute(
                    select(record_endorsements.c.record_id).where(
                        record_endorsements.c.endorsement_id == first_eid
                    )
                )
            ).fetchall()
            for (record_id,) in rec_rows:
                for name in names[1:]:
                    eid = await ensure_endorsement(conn, name)
                    await _link_endorsement(conn, record_id, eid)
