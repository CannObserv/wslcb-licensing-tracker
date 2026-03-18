"""Entity (applicant) normalization for WSLCB licensing tracker.

Manages the `entities` and `record_entities` tables — extracting
individual people and organizations from semicolon-delimited applicant
strings, deduplicating by name, and linking them to license records.
"""

import logging
import re
import sqlite3
from datetime import UTC, datetime

from .db import clean_applicants_string, clean_entity_name, strip_duplicate_marker

logger = logging.getLogger(__name__)

# Patterns that indicate an organization rather than a person.
# Input is always uppercased by get_or_create_entity(), so no IGNORECASE needed.
_ORG_PATTERNS = re.compile(
    r"\b(LLC|L\.?L\.?C\.?|INC\.?|CORP\.?|CORPORATION|TRUST|LTD\.?|LIMITED"
    r"|PARTNERS|PARTNERSHIP|HOLDINGS|GROUP|ENTERPRISE|ENTERPRISES"
    r"|ASSOCIATION|FOUNDATION|COMPANY|CO\.|L\.?P\.?)\b"
)


# clean_entity_name, strip_duplicate_marker, and clean_applicants_string live in
# db.py (stable base layer) so schema.py migrations can import them without a
# schema → business-logic layering violation.  Re-exported here for callers that
# already import from entities.


# Meta-labels that WSLCB embeds in applicant lists as truncation notices.
# These are not real people or organizations and must be excluded from entity
# creation.  Both the canonical spelling and the typo variant are included.
ADDITIONAL_NAMES_MARKERS: frozenset[str] = frozenset(
    {
        "ADDITIONAL NAMES ON FILE",
        "ADDTIONAL NAMES ON FILE",  # typo variant present in WSLCB source
    }
)


def _classify_entity_type(name: str) -> str:
    """Classify an entity name as 'person' or 'organization'."""
    return "organization" if _ORG_PATTERNS.search(name) else "person"


def _entity_id_for_normalized(conn: sqlite3.Connection, normalized: str) -> int:
    """Look up or create an entity by already-normalized name (internal).

    Callers are responsible for passing a name that has already been run
    through ``clean_entity_name()`` so that no second normalization pass
    is needed.  Use :func:`get_or_create_entity` for external callers that
    supply raw names.
    """
    row = conn.execute("SELECT id FROM entities WHERE name = ?", (normalized,)).fetchone()
    if row:
        return row[0]
    entity_type = _classify_entity_type(normalized)
    cur = conn.execute(
        "INSERT INTO entities (name, entity_type) VALUES (?, ?)",
        (normalized, entity_type),
    )
    return cur.lastrowid


def get_or_create_entity(conn: sqlite3.Connection, name: str) -> int:
    """Return the entity id for *name*, creating if needed.

    Names are uppercased and cleaned of stray trailing punctuation.
    The WSLCB source is predominantly uppercase but occasionally uses
    mixed case or appends errant periods/commas.
    """
    normalized = clean_entity_name(name)
    if not normalized:
        msg = "Entity name must not be empty"
        raise ValueError(msg)
    return _entity_id_for_normalized(conn, normalized)


def parse_and_link_entities(
    conn: sqlite3.Connection,
    record_id: int,
    applicants_str: str,
    role: str = "applicant",
    *,
    delete_existing: bool = False,
) -> int:
    """Split applicants string, skip first element (business name), create entities and link.

    Assigns contiguous 0-based positions to successfully linked entities
    (skipped names do not leave gaps).  Returns the number of entities
    linked.

    Parameters
    ----------
    delete_existing:
        If True, delete any existing ``record_entities`` rows for this
        ``(record_id, role)`` pair before inserting.  Use this when
        reprocessing to ensure stale links are removed (idempotent mode).
    """
    if delete_existing:
        conn.execute(
            "DELETE FROM record_entities WHERE record_id = ? AND role = ?",
            (record_id, role),
        )
    if not applicants_str or ";" not in applicants_str:
        return 0
    parts = [p.strip() for p in applicants_str.split(";")]
    # First element is always the business name — skip it.
    # Strip DUPLICATE markers and deduplicate so annotated forms (e.g.
    # "ADAM (DUPLICATE) BENTON") resolve to the same entity as the clean
    # form ("ADAM BENTON") that typically appears in the same string.
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
            continue  # not a real entity — no position consumed
        # `name` is already the output of clean_entity_name(strip_duplicate_marker(...))
        # so skip the redundant normalization pass inside get_or_create_entity.
        entity_id = _entity_id_for_normalized(conn, name)
        cursor = conn.execute(
            """INSERT OR IGNORE INTO record_entities
               (record_id, entity_id, role, position)
               VALUES (?, ?, ?, ?)""",
            (record_id, entity_id, role, linked),
        )
        linked += cursor.rowcount
    return linked


def backfill_entities(conn: sqlite3.Connection) -> int:
    """Populate entities + record_entities for existing records, then run startup cleanup.

    1. Link entities for records that have applicants but no entity
       links yet.
    2. Call ``merge_duplicate_entities()`` to clean stale entity names,
       merge duplicates, and normalize string columns in
       ``license_records``.

    Returns the number of records processed in step 1.
    """
    rows = conn.execute("""
        SELECT lr.id, lr.applicants, lr.previous_applicants
        FROM license_records lr
        LEFT JOIN record_entities re ON re.record_id = lr.id
        WHERE re.record_id IS NULL
          AND (lr.applicants LIKE '%;%' OR lr.previous_applicants LIKE '%;%')
    """).fetchall()

    for r in rows:
        parse_and_link_entities(conn, r["id"], r["applicants"], "applicant")
        if r["previous_applicants"]:
            parse_and_link_entities(conn, r["id"], r["previous_applicants"], "previous_applicant")

    if rows:
        conn.commit()

    # Clean up any entities with stray trailing punctuation
    merge_duplicate_entities(conn)

    return len(rows)


def get_record_entities(
    conn: sqlite3.Connection, record_ids: list[int]
) -> dict[int, dict[str, list[dict]]]:
    """Batch-fetch entities for a list of record ids.

    Returns {record_id: {"applicant": [{"id": ..., "name": ..., "entity_type": ...}, ...],
                         "previous_applicant": [...]}}
    """
    if not record_ids:
        return {}
    chunk = 500
    result: dict[int, dict[str, list[dict]]] = {
        rid: {"applicant": [], "previous_applicant": []} for rid in record_ids
    }
    for i in range(0, len(record_ids), chunk):
        batch = record_ids[i : i + chunk]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(
            f"""
            SELECT re.record_id, re.role, re.position,
                   e.id AS entity_id, e.name, e.entity_type
            FROM record_entities re
            JOIN entities e ON e.id = re.entity_id
            WHERE re.record_id IN ({placeholders})
            ORDER BY re.record_id, re.role, re.position
        """,
            batch,
        ).fetchall()
        for r in rows:
            result[r["record_id"]][r["role"]].append(
                {
                    "id": r["entity_id"],
                    "name": r["name"],
                    "entity_type": r["entity_type"],
                }
            )
    return result


def get_entity_by_id(conn: sqlite3.Connection, entity_id: int) -> dict | None:
    """Fetch a single entity by id."""
    row = conn.execute(
        "SELECT id, name, entity_type, created_at FROM entities WHERE id = ?",
        (entity_id,),
    ).fetchone()
    return dict(row) if row else None


def clean_record_strings(conn: sqlite3.Connection) -> int:
    """Uppercase and strip trailing punctuation from name/applicant columns in license_records.

    Only updates rows that actually change.  Returns the number of rows
    updated.  Callers should commit afterward (this function does not
    commit, so it can participate in a larger transaction).

    Note: the ``raw_*`` shadow columns are intentionally left untouched.
    They preserve the as-parsed values from ingestion time (or the
    backfilled cleaned values for pre-existing records) and should
    never be overwritten by subsequent cleaning passes.
    """
    rows = conn.execute(
        """SELECT id, business_name, previous_business_name,
                  applicants, previous_applicants
           FROM license_records"""
    ).fetchall()
    updated = 0
    for r in rows:
        clean_biz = clean_entity_name(r["business_name"] or "")
        clean_prev_biz = clean_entity_name(r["previous_business_name"] or "")
        clean_app = clean_applicants_string(r["applicants"] or "")
        clean_prev = clean_applicants_string(r["previous_applicants"] or "")
        if (
            clean_biz != (r["business_name"] or "")
            or clean_prev_biz != (r["previous_business_name"] or "")
            or clean_app != (r["applicants"] or "")
            or clean_prev != (r["previous_applicants"] or "")
        ):
            conn.execute(
                """UPDATE license_records
                   SET business_name = ?,
                       previous_business_name = ?,
                       applicants = ?,
                       previous_applicants = ?
                   WHERE id = ?""",
                (clean_biz, clean_prev_biz, clean_app, clean_prev, r["id"]),
            )
            updated += 1
    if updated:
        logger.info("Cleaned strings in %d record(s)", updated)
    return updated


def merge_duplicate_entities(conn: sqlite3.Connection) -> int:
    """Find and merge entities whose names differ only by trailing punctuation or casing.

    Performs all work in a single transaction (committed at the end):

    1. Clean ``business_name``, ``previous_business_name``, ``applicants``,
       and ``previous_applicants`` on ``license_records`` via
       ``clean_record_strings()``.
    2. For each dirty entity, reassign its ``record_entities`` rows to
       the canonical (clean-named) entity, preserving the lower position
       on conflicts, then delete the dirty entity.
    3. Entities with no clean counterpart are renamed in place.

    Returns the number of entities merged or renamed.
    """
    # Clean business names and applicant strings in license_records
    # so FTS, CSV export, and display are all consistent.
    clean_record_strings(conn)

    # Find entities whose cleaned name differs from their stored name
    all_entities = conn.execute("SELECT id, name FROM entities ORDER BY id").fetchall()

    merged = 0
    for entity in all_entities:
        cleaned = clean_entity_name(entity["name"])
        if cleaned == entity["name"]:
            continue  # name is already clean
        if not cleaned:
            logger.warning(
                "Entity %d name reduces to empty after cleaning: %r", entity["id"], entity["name"]
            )
            continue

        # Find the canonical entity (clean name)
        canonical = conn.execute("SELECT id FROM entities WHERE name = ?", (cleaned,)).fetchone()

        dirty_id = entity["id"]

        if canonical:
            canon_id = canonical["id"]
            # Reassign record_entities from dirty → canonical.
            # Where the canonical entity already has a link for the
            # same (record, role), keep whichever has the lower
            # position (preserving source ordering) and drop the other.
            dirty_links = conn.execute(
                "SELECT record_id, role, position FROM record_entities WHERE entity_id = ?",
                (dirty_id,),
            ).fetchall()
            for link in dirty_links:
                existing = conn.execute(
                    """SELECT position FROM record_entities
                       WHERE record_id = ? AND entity_id = ? AND role = ?""",
                    (link["record_id"], canon_id, link["role"]),
                ).fetchone()
                if existing:
                    # Both linked — keep the lower position on canonical
                    if link["position"] < existing["position"]:
                        conn.execute(
                            """UPDATE record_entities SET position = ?
                               WHERE record_id = ? AND entity_id = ? AND role = ?""",
                            (link["position"], link["record_id"], canon_id, link["role"]),
                        )
                else:
                    # No conflict — reassign
                    conn.execute(
                        """UPDATE record_entities SET entity_id = ?
                           WHERE record_id = ? AND entity_id = ? AND role = ?""",
                        (canon_id, link["record_id"], dirty_id, link["role"]),
                    )
            # Delete any remaining dirty links (conflict cases)
            conn.execute(
                "DELETE FROM record_entities WHERE entity_id = ?",
                (dirty_id,),
            )
            # Delete the dirty entity
            conn.execute("DELETE FROM entities WHERE id = ?", (dirty_id,))
            logger.info("Merged entity %d %r → %d %r", dirty_id, entity["name"], canon_id, cleaned)
        else:
            # No canonical counterpart — just rename in place
            conn.execute(
                "UPDATE entities SET name = ? WHERE id = ?",
                (cleaned, dirty_id),
            )
            logger.info("Renamed entity %d: %r → %r", dirty_id, entity["name"], cleaned)
        merged += 1

    # Commit record-string cleaning and entity merges/renames together
    conn.commit()
    if merged:
        logger.info("Merged/renamed %d entities with stray trailing punctuation", merged)
    return merged


# Enrichment version written by reprocess_entities.
# Bump this integer when entity processing logic changes to enable
# selective re-processing of affected records.
# Stored as TEXT in record_enrichments.version; compare in SQL with
# CAST(version AS INTEGER) < _ENTITY_REPROCESS_VERSION.
_ENTITY_REPROCESS_VERSION = 2


def reprocess_entities(
    conn: sqlite3.Connection,
    *,
    record_id: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Regenerate ``record_entities`` for all or a subset of records.

    Treats ``record_entities`` as *derived* data: existing rows for each
    targeted ``(record_id, role)`` pair are deleted and rebuilt from
    ``license_records.applicants`` / ``previous_applicants`` using the
    current entity-normalization logic.  The ``record_enrichments``
    version stamp is updated to ``_ENTITY_REPROCESS_VERSION`` for every
    processed record.

    Parameters
    ----------
    conn:
        Open database connection.  The caller is responsible for committing
        (or rolling back) after this function returns.
    record_id:
        If given, only reprocess this single record.
    dry_run:
        If True, compute what *would* be done and return the counts without
        making any database changes.

    Returns:
    -------
    dict
        ``{"records_processed": int, "entities_linked": int}``
    """
    if record_id is not None:
        rows = conn.execute(
            "SELECT id, applicants, previous_applicants FROM license_records WHERE id = ?",
            (record_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, applicants, previous_applicants FROM license_records"
        ).fetchall()

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

        linked = parse_and_link_entities(conn, rid, applicants, "applicant", delete_existing=True)
        linked += parse_and_link_entities(
            conn, rid, previous_applicants, "previous_applicant", delete_existing=True
        )
        entities_linked += linked
        records_processed += 1

        # Update enrichment version stamp.
        conn.execute(
            """INSERT OR REPLACE INTO record_enrichments
               (record_id, step, completed_at, version)
               VALUES (?, 'entities', ?, ?)""",
            (rid, now, str(_ENTITY_REPROCESS_VERSION)),
        )

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
