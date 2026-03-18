"""Endorsement seeding, repair, and backfill for WSLCB licensing tracker.

Startup/repair operations that are distinct from the core endorsement pipeline
(``endorsements.py``) and admin UI helpers (``endorsements_admin.py``):

- ``seed_endorsements`` — populate endorsement_codes from seed_code_map.json
- ``merge_mixed_case_endorsements`` — repair mixed-case duplicates
- ``repair_code_name_endorsements`` — migrate legacy "CODE, NAME" endorsements
- ``backfill`` — process records that have no endorsement links yet
- ``discover_code_mappings`` — cross-reference license numbers to learn new mappings

All functions in this module follow the caller-commits convention: they do not
call ``conn.commit()`` themselves.
"""

import json
import logging
import sqlite3
from pathlib import Path

from .endorsements import (
    CODE_NAME_RE,
    ensure_endorsement,
    link_endorsement,
    merge_endorsement,
    process_record,
)

logger = logging.getLogger(__name__)

# Seed data: loaded from seed_code_map.json at module init.
#
# Maps WSLCB numeric code strings → lists of endorsement names, built from
# cross-referencing license numbers that appear in both new-application (text)
# and approved/discontinued (numeric code) sections.  Most codes map 1-to-1.
# A handful map to multiple always-present endorsements (e.g. 320 always
# includes BEER DISTRIBUTOR + WINE DISTRIBUTOR).  Keys are string
# representations of WSLCB internal license class IDs, stored as TEXT in the DB.
#
# Edit seed_code_map.json (not this module) when adding/correcting seed mappings.
_SEED_CODE_MAP_PATH = Path(__file__).parent / "seed_code_map.json"
SEED_CODE_MAP: dict[str, list[str]] = json.loads(_SEED_CODE_MAP_PATH.read_text())


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def seed_endorsements(conn: sqlite3.Connection) -> int:
    """Populate license_endorsements and endorsement_codes from SEED_CODE_MAP.

    Safe to call repeatedly — skips existing rows.  After seeding, merges
    any placeholder endorsements (where the endorsement name equals the
    numeric code) that now have real mappings.

    Returns the number of new code mappings inserted.  Caller must commit.
    """
    inserted = 0
    for code, names in SEED_CODE_MAP.items():
        for name in names:
            eid = ensure_endorsement(conn, name)
            cur = conn.execute(
                """INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id)
                   VALUES (?, ?)""",
                (code, eid),
            )
            inserted += cur.rowcount

    # Merge any placeholder endorsements now that seed mappings exist.
    _merge_seeded_placeholders(conn)

    return inserted


def _merge_seeded_placeholders(conn: sqlite3.Connection) -> int:
    """Merge placeholder endorsements that now have real seed mappings.

    A placeholder endorsement has ``name == code`` (e.g. endorsement named
    ``"331"`` for code ``"331"``).  If ``seed_endorsements`` has since
    registered a real mapping for that code, migrate all record links from
    the placeholder to the real endorsement(s) and delete the placeholder.

    Returns the number of record links migrated.
    """
    placeholders = conn.execute("""
        SELECT le.id, le.name
        FROM license_endorsements le
        JOIN endorsement_codes ec ON ec.endorsement_id = le.id AND ec.code = le.name
        WHERE le.name GLOB '[0-9]*' AND le.name NOT GLOB '*[a-zA-Z]*'
          AND EXISTS (
              SELECT 1 FROM endorsement_codes ec2
              JOIN license_endorsements le2 ON le2.id = ec2.endorsement_id
              WHERE ec2.code = le.name AND le2.name != le.name
          )
    """).fetchall()
    if not placeholders:
        return 0

    migrated = 0
    for pid, code in placeholders:
        real_eids = [
            r[0]
            for r in conn.execute(
                """
            SELECT ec.endorsement_id FROM endorsement_codes ec
            JOIN license_endorsements le ON le.id = ec.endorsement_id
            WHERE ec.code = ? AND le.name != ?
        """,
                (code, code),
            ).fetchall()
        ]
        if not real_eids:
            continue

        count = merge_endorsement(conn, pid, real_eids[0])
        migrated += count
        if len(real_eids) > 1:
            records = conn.execute(
                "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
                (real_eids[0],),
            ).fetchall()
            for rec in records:
                for eid in real_eids[1:]:
                    link_endorsement(conn, rec[0], eid)

    if migrated:
        logger.info(
            "Merged %d record link(s) from %d placeholder endorsement(s).",
            migrated,
            len(placeholders),
        )
    return migrated


# ---------------------------------------------------------------------------
# Repair: merge mixed-case endorsement duplicates
# ---------------------------------------------------------------------------


def merge_mixed_case_endorsements(conn: sqlite3.Connection) -> int:
    """Merge endorsements whose names differ only by case.

    For each endorsement where ``name != UPPER(name)`` and an UPPER
    counterpart already exists, migrate all record links and code
    mappings to the canonical (upper-case) row via
    ``merge_endorsement()``, then delete the mixed-case row.  If no
    upper-case counterpart exists, the mixed-case row is simply
    renamed in place.

    Returns the number of endorsements fixed.  Caller must commit.
    """
    dupes = conn.execute("""
        SELECT mc.id AS mixed_id, mc.name AS mixed_name
        FROM license_endorsements mc
        WHERE mc.name != UPPER(mc.name)
    """).fetchall()

    if not dupes:
        return 0

    for row in dupes:
        mixed_id, mixed_name = row[0], row[1]
        upper_name = mixed_name.upper()

        upper_row = conn.execute(
            "SELECT id FROM license_endorsements WHERE name = ?",
            (upper_name,),
        ).fetchone()

        if not upper_row:
            conn.execute(
                "UPDATE license_endorsements SET name = ? WHERE id = ?",
                (upper_name, mixed_id),
            )
            logger.info("Renamed endorsement %r → %r (id=%d)", mixed_name, upper_name, mixed_id)
            continue

        merge_endorsement(conn, mixed_id, upper_row[0])
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


def repair_code_name_endorsements(conn: sqlite3.Connection) -> int:
    """Migrate record links from spurious ``CODE, NAME`` endorsements.

    Historical data used license_type values like ``"450, GROCERY STORE -
    BEER/WINE"`` which were stored as endorsement names verbatim.  This
    function re-resolves each one: if the embedded code is already mapped
    in ``endorsement_codes``, migrate to those endorsements; otherwise
    use the embedded name (creating the endorsement if needed) and
    register the code mapping.

    Also cleans up bogus ``endorsement_codes`` rows whose code column
    contains spaces (artifacts of ``discover_code_mappings`` running on
    ``CODE, NAME`` values).

    Returns the number of record links migrated.  Safe to call
    repeatedly — no-ops once all ``CODE, NAME`` endorsements are gone.
    Caller must commit.
    """
    bogus = conn.execute(
        "SELECT id, name FROM license_endorsements WHERE name GLOB '[0-9]*, *'"
    ).fetchall()
    if not bogus:
        deleted = _cleanup_space_codes(conn)
        if deleted:
            logger.info("Cleaned up %d bogus endorsement_codes row(s).", deleted)
        return 0

    migrated = 0
    for eid_old, full_name in bogus:
        m = CODE_NAME_RE.match(full_name)
        if not m:
            continue
        code, name = m.group(1), m.group(2).strip()

        mapped_eids = conn.execute(
            """SELECT ec.endorsement_id FROM endorsement_codes ec
               JOIN license_endorsements le ON le.id = ec.endorsement_id
               WHERE ec.code = ? AND le.name != ?""",
            (code, full_name),
        ).fetchall()

        if mapped_eids:
            target_eids = [r[0] for r in mapped_eids]
        else:
            target_eid = ensure_endorsement(conn, name)
            conn.execute(
                "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
                (code, target_eid),
            )
            target_eids = [target_eid]

        migrated += merge_endorsement(conn, eid_old, target_eids[0])
        if len(target_eids) > 1:
            records = conn.execute(
                "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
                (target_eids[0],),
            ).fetchall()
            for rec in records:
                for tgt in target_eids[1:]:
                    link_endorsement(conn, rec[0], tgt)

    _cleanup_space_codes(conn)

    if migrated:
        logger.info(
            "Repaired %d record-endorsement link(s) from %d 'CODE, NAME' endorsement(s).",
            migrated,
            len(bogus),
        )
    return migrated


def _cleanup_space_codes(conn: sqlite3.Connection) -> int:
    """Remove ``endorsement_codes`` rows whose code contains spaces.

    These are artifacts of ``discover_code_mappings()`` processing
    ``CODE, NAME`` license_type values via ``REPLACE(license_type, ',', '')``,
    producing codes like ``"379 Curbside/Delivery Endorsement"``.

    Returns the number of rows deleted.
    """
    cur = conn.execute("DELETE FROM endorsement_codes WHERE code LIKE '% %'")
    if cur.rowcount:
        logger.info(
            "Removed %d bogus endorsement_codes row(s) with spaces in code.",
            cur.rowcount,
        )
    return cur.rowcount


# ---------------------------------------------------------------------------
# Backfill: process records with no endorsement links
# ---------------------------------------------------------------------------


def backfill(conn: sqlite3.Connection) -> int:
    """Process all records that don't yet have endorsement links.

    Returns the number of records processed.  Caller must commit.
    """
    rows = conn.execute("""
        SELECT lr.id, lr.license_type, lr.section_type
        FROM license_records lr
        LEFT JOIN record_endorsements re ON re.record_id = lr.id
        WHERE re.record_id IS NULL
          AND lr.license_type IS NOT NULL AND lr.license_type != ''
    """).fetchall()

    for r in rows:
        process_record(conn, r["id"], r["license_type"])

    return len(rows)


# ---------------------------------------------------------------------------
# Discovery: cross-reference license numbers to learn new code→name mappings
# ---------------------------------------------------------------------------


def discover_code_mappings(conn: sqlite3.Connection) -> dict[str, list[str]]:  # noqa: C901
    """Cross-reference license numbers to learn new code→name mappings.

    For each unmapped numeric code, find new_application records sharing
    the same license_number.  When every matched text record contains the
    same endorsement(s), adopt that as the mapping.

    Returns {code: [name, ...]} for newly discovered mappings.  Caller must commit.
    """
    mapped = {
        r[0]
        for r in conn.execute("""
            SELECT DISTINCT ec.code
            FROM endorsement_codes ec
            JOIN license_endorsements le ON le.id = ec.endorsement_id
            WHERE le.name != ec.code
        """).fetchall()
    }

    all_codes: set[str] = set()
    rows = conn.execute("""
        SELECT DISTINCT license_type
        FROM license_records
        WHERE section_type IN ('approved', 'discontinued')
          AND license_type GLOB '[0-9]*'
    """).fetchall()
    for r in rows:
        raw = r[0].rstrip(",").strip()
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
        matches = conn.execute(
            """
            SELECT n.license_type AS text_type, COUNT(*) AS cnt
            FROM license_records a
            JOIN license_records n
                ON a.license_number = n.license_number
                AND n.section_type = 'new_application'
            WHERE SUBSTR(a.license_type, 1, INSTR(a.license_type, ',') - 1) = ?
              AND a.section_type IN ('approved', 'discontinued')
            GROUP BY n.license_type
        """,
            (code,),
        ).fetchall()
        if not matches:
            continue

        total = sum(r["cnt"] for r in matches)
        type_freq: dict[str, int] = {}
        for r in matches:
            for t in r["text_type"].split(";"):
                key = t.strip()
                type_freq[key] = type_freq.get(key, 0) + r["cnt"]

        always = [t for t, c in type_freq.items() if c == total and t]
        if not always:
            continue

        for name in always:
            eid = ensure_endorsement(conn, name)
            conn.execute(
                "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
                (code, eid),
            )
        learned[code] = always

    if learned:
        _merge_placeholders(conn, learned)
    return learned


def _merge_placeholders(conn: sqlite3.Connection, learned: dict[str, list[str]]) -> None:
    """If a code had a placeholder endorsement (name == code), migrate links."""
    for code, names in learned.items():
        placeholder = conn.execute(
            "SELECT id FROM license_endorsements WHERE name = ?", (code,)
        ).fetchone()
        if not placeholder:
            continue
        pid = placeholder[0]
        first_eid = ensure_endorsement(conn, names[0])
        merge_endorsement(conn, pid, first_eid)
        if len(names) > 1:
            records = conn.execute(
                "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
                (first_eid,),
            ).fetchall()
            for rec in records:
                for name in names[1:]:
                    eid = ensure_endorsement(conn, name)
                    link_endorsement(conn, rec[0], eid)
