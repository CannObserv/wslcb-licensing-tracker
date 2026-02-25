"""Entity (applicant) normalization for WSLCB licensing tracker.

Manages the `entities` and `record_entities` tables — extracting
individual people and organizations from semicolon-delimited applicant
strings, deduplicating by name, and linking them to license records.
"""
import logging
import re
import sqlite3

logger = logging.getLogger(__name__)

# Patterns that indicate an organization rather than a person.
# Input is always uppercased by get_or_create_entity(), so no IGNORECASE needed.
_ORG_PATTERNS = re.compile(
    r'\b(LLC|L\.?L\.?C\.?|INC\.?|CORP\.?|CORPORATION|TRUST|LTD\.?|LIMITED'
    r'|PARTNERS|PARTNERSHIP|HOLDINGS|GROUP|ENTERPRISE|ENTERPRISES'
    r'|ASSOCIATION|FOUNDATION|COMPANY|CO\.|L\.?P\.?)\b'
)


# Suffixes where a trailing period is legitimate and should be kept.
# Checked against the uppercased name.
_LEGIT_TRAILING_DOT = re.compile(
    r'(?:INC|LLC|L\.L\.C|LTD|CORP|JR|SR|CO|S\.P\.A|PTY|L\.P|F\.O\.E|U\.P|W\. & S)\.\s*$'
)


def _clean_entity_name(name: str) -> str:
    """Normalize an entity name: uppercase, strip whitespace, and remove
    stray trailing punctuation that isn't part of a recognized suffix.

    The WSLCB source occasionally appends periods or commas to names
    as data-entry artifacts (e.g., ``WOLDU ARAYA BERAKI.``).  This
    strips those while preserving legitimate endings like ``INC.`` or
    ``JR.``.
    """
    cleaned = name.strip().upper()
    # Iteratively strip trailing periods/commas that aren't legit suffixes
    while cleaned and cleaned[-1] in '.,' and not _LEGIT_TRAILING_DOT.search(cleaned):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def _classify_entity_type(name: str) -> str:
    """Classify an entity name as 'person' or 'organization'."""
    return "organization" if _ORG_PATTERNS.search(name) else "person"


def get_or_create_entity(conn: sqlite3.Connection, name: str) -> int:
    """Return the entity id for *name*, creating if needed.

    Names are uppercased and cleaned of stray trailing punctuation.
    The WSLCB source is predominantly uppercase but occasionally uses
    mixed case or appends errant periods/commas.
    """
    normalized = _clean_entity_name(name)
    if not normalized:
        raise ValueError("Entity name must not be empty")
    row = conn.execute(
        "SELECT id FROM entities WHERE name = ?", (normalized,)
    ).fetchone()
    if row:
        return row[0]
    entity_type = _classify_entity_type(normalized)
    cur = conn.execute(
        "INSERT INTO entities (name, entity_type) VALUES (?, ?)",
        (normalized, entity_type),
    )
    return cur.lastrowid


def _parse_and_link_entities(
    conn: sqlite3.Connection,
    record_id: int,
    applicants_str: str,
    role: str = "applicant",
) -> int:
    """Split a semicolon-delimited applicants string, skip the first
    element (business name), create entities, and link them to the record.

    Returns the number of entities linked.
    """
    if not applicants_str or ";" not in applicants_str:
        return 0
    parts = [p.strip() for p in applicants_str.split(";")]
    # First element is always the business name — skip it
    entity_names = [p for p in parts[1:] if p]
    linked = 0
    for position, name in enumerate(entity_names):
        entity_id = get_or_create_entity(conn, name)
        cursor = conn.execute(
            """INSERT OR IGNORE INTO record_entities
               (record_id, entity_id, role, position)
               VALUES (?, ?, ?, ?)""",
            (record_id, entity_id, role, position),
        )
        linked += cursor.rowcount
    return linked


def backfill_entities(conn: sqlite3.Connection) -> int:
    """Populate entities + record_entities for existing records.

    Only processes records that have applicants but no entity links yet.
    Returns the number of records processed.
    """
    rows = conn.execute("""
        SELECT lr.id, lr.applicants, lr.previous_applicants
        FROM license_records lr
        LEFT JOIN record_entities re ON re.record_id = lr.id
        WHERE re.record_id IS NULL
          AND (lr.applicants LIKE '%;%' OR lr.previous_applicants LIKE '%;%')
    """).fetchall()

    for r in rows:
        _parse_and_link_entities(conn, r["id"], r["applicants"], "applicant")
        if r["previous_applicants"]:
            _parse_and_link_entities(
                conn, r["id"], r["previous_applicants"], "previous_applicant"
            )

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
    CHUNK = 500
    result: dict[int, dict[str, list[dict]]] = {
        rid: {"applicant": [], "previous_applicant": []} for rid in record_ids
    }
    for i in range(0, len(record_ids), CHUNK):
        batch = record_ids[i:i + CHUNK]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(f"""
            SELECT re.record_id, re.role, re.position,
                   e.id AS entity_id, e.name, e.entity_type
            FROM record_entities re
            JOIN entities e ON e.id = re.entity_id
            WHERE re.record_id IN ({placeholders})
            ORDER BY re.record_id, re.role, re.position
        """, batch).fetchall()
        for r in rows:
            result[r["record_id"]][r["role"]].append({
                "id": r["entity_id"],
                "name": r["name"],
                "entity_type": r["entity_type"],
            })
    return result


def get_entity_by_id(conn: sqlite3.Connection, entity_id: int) -> dict | None:
    """Fetch a single entity by id."""
    row = conn.execute(
        "SELECT id, name, entity_type, created_at FROM entities WHERE id = ?",
        (entity_id,),
    ).fetchone()
    return dict(row) if row else None


def merge_duplicate_entities(conn: sqlite3.Connection) -> int:
    """Find and merge entities whose names differ only by stray trailing
    punctuation (e.g., ``WOLDU ARAYA BERAKI.`` → ``WOLDU ARAYA BERAKI``).

    For each dirty entity:
    1. Reassign its ``record_entities`` rows to the clean entity
       (skipping any that would violate the PK constraint — i.e., the
       clean entity is already linked to that record+role).
    2. Delete the dirty entity row.

    Returns the number of entities merged (deleted).
    """
    # Find entities whose cleaned name differs from their stored name
    all_entities = conn.execute(
        "SELECT id, name FROM entities ORDER BY id"
    ).fetchall()

    merged = 0
    for entity in all_entities:
        cleaned = _clean_entity_name(entity["name"])
        if cleaned == entity["name"]:
            continue  # name is already clean
        if not cleaned:
            logger.warning("Entity %d name reduces to empty after cleaning: %r",
                           entity["id"], entity["name"])
            continue

        # Find the canonical entity (clean name)
        canonical = conn.execute(
            "SELECT id FROM entities WHERE name = ?", (cleaned,)
        ).fetchone()

        dirty_id = entity["id"]

        if canonical:
            canon_id = canonical["id"]
            # Reassign record_entities: use INSERT OR IGNORE to skip
            # rows that would create PK conflicts (record already
            # linked to canonical entity with the same role)
            conn.execute(
                """UPDATE OR IGNORE record_entities
                   SET entity_id = ? WHERE entity_id = ?""",
                (canon_id, dirty_id),
            )
            # Delete any remaining rows that couldn't be reassigned
            # (because the canonical link already existed)
            conn.execute(
                "DELETE FROM record_entities WHERE entity_id = ?",
                (dirty_id,),
            )
            # Delete the dirty entity
            conn.execute("DELETE FROM entities WHERE id = ?", (dirty_id,))
            logger.info("Merged entity %d %r → %d %r",
                        dirty_id, entity["name"], canon_id, cleaned)
        else:
            # No canonical counterpart — just rename in place
            conn.execute(
                "UPDATE entities SET name = ? WHERE id = ?",
                (cleaned, dirty_id),
            )
            logger.info("Renamed entity %d: %r → %r",
                        dirty_id, entity["name"], cleaned)
        merged += 1

    if merged:
        conn.commit()
        logger.info("Merged/renamed %d entities with stray trailing punctuation",
                    merged)
    return merged
