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
# Checked against the uppercased name.  The ``(?<=\s|^)`` lookbehind
# ensures we match whole suffixes — e.g. " CO." but not "COSTCO.".
#
# Maintainer note: add new entries here when the WSLCB source uses a
# legitimate abbreviation that ends with a period.  The full list:
#   Business: INC, LLC, L.L.C, LTD, CORP, CO, L.P, L.L.P, PTY, P.C, N.A, P.A
#   Personal: JR, SR
#   Fraternal/other: S.P.A, F.O.E, U.P, D.B.A, W. & S
_LEGIT_TRAILING_DOT = re.compile(
    r'(?:(?<=\s)|(?<=^))'
    r'(?:INC|LLC|L\.L\.C|L\.L\.P|LTD|CORP|CO|L\.P|PTY'
    r'|JR|SR'
    r'|S\.P\.A|F\.O\.E|U\.P|D\.B\.A|P\.C|N\.A|P\.A'
    r'|W\. & S)'
    r'\.\s*$'
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


def clean_applicants_string(applicants: str) -> str:
    """Clean each semicolon-separated part of an applicants string.

    Applies ``_clean_entity_name()`` to every element (including the
    leading business-name element) so the stored string is consistent
    with entity names in the ``entities`` table.  Empty parts after
    cleaning are dropped.
    """
    if not applicants:
        return applicants
    parts = [_clean_entity_name(p) for p in applicants.split(";")]
    return "; ".join(p for p in parts if p)


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
        try:
            entity_id = get_or_create_entity(conn, name)
        except ValueError:
            logger.warning("Skipping empty entity name in record %d "
                           "(position %d, role %s, raw: %r)",
                           record_id, position, role, name)
            continue
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


def _clean_applicants_in_records(conn: sqlite3.Connection) -> int:
    """One-time cleanup: apply ``clean_applicants_string()`` to all
    ``applicants`` and ``previous_applicants`` values in ``license_records``.

    Only updates rows that actually change.  Returns the number of rows
    updated.
    """
    rows = conn.execute(
        "SELECT id, applicants, previous_applicants FROM license_records"
    ).fetchall()
    updated = 0
    for r in rows:
        clean_app = clean_applicants_string(r["applicants"] or "")
        clean_prev = clean_applicants_string(r["previous_applicants"] or "")
        if clean_app != (r["applicants"] or "") or clean_prev != (r["previous_applicants"] or ""):
            conn.execute(
                """UPDATE license_records
                   SET applicants = ?, previous_applicants = ?
                   WHERE id = ?""",
                (clean_app, clean_prev, r["id"]),
            )
            updated += 1
    if updated:
        conn.commit()
        logger.info("Cleaned applicant strings in %d record(s)", updated)
    return updated


def merge_duplicate_entities(conn: sqlite3.Connection) -> int:
    """Find and merge entities whose names differ only by stray trailing
    punctuation (e.g., ``WOLDU ARAYA BERAKI.`` → ``WOLDU ARAYA BERAKI``).

    Also cleans the ``applicants`` / ``previous_applicants`` string
    columns on ``license_records`` so stored values, FTS indexes, and
    CSV exports are consistent with the cleaned entity names.

    For each dirty entity:
    1. Reassign its ``record_entities`` rows to the clean entity
       (skipping any that would violate the PK constraint — i.e., the
       clean entity is already linked to that record+role).
    2. Delete the dirty entity row.

    Returns the number of entities merged (deleted).
    """
    # Clean stray punctuation in the applicants string columns too,
    # so FTS and CSV export stay consistent with entity names.
    _clean_applicants_in_records(conn)

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
