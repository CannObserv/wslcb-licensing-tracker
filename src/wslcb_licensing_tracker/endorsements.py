"""License endorsement normalization for WSLCB licensing tracker.

The WSLCB source page represents license types differently by section:
- New applications: semicolon-separated text names
  (e.g. "GROCERY STORE - BEER/WINE; SNACK BAR")
- Approved/discontinued (current): opaque integer codes (e.g. "450,")
- Approved/discontinued (historical, pre-2025): "CODE, NAME" format
  (e.g. "450, GROCERY STORE - BEER/WINE")

This module normalizes all three representations into a shared
`license_endorsements` lookup table and links records via a
`record_endorsements` junction table.  Code-to-name mappings are
seeded from historical cross-referencing and refined automatically
as new data arrives.

See Also:
- ``endorsements_seed`` — seeding, repair, and backfill operations
- ``endorsements_admin`` — admin UI helpers (duplicate detection, code-mapping CRUD)
- ``substances`` — regulated substance CRUD

Note: ``resolved_endorsements`` (maintained by :func:`process_record`) is
indexed by FTS5, so text search for endorsement names now also matches
approved/discontinued records whose raw ``license_type`` stores a numeric
code.  See GH #87.
"""

import logging
import re
import sqlite3
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

# Matches the historical "CODE, NAME" format used in approved/discontinued
# sections before ~2025 (e.g. "450, GROCERY STORE - BEER/WINE").  The
# first capturing group is the numeric code, the second is the name.
# Handles names that themselves contain commas (e.g. "< 250,000 LITERS").
CODE_NAME_RE = re.compile(r"^(\d+),\s+(.+)$")


# ---
# Endorsement CRUD helpers
# ---


def ensure_endorsement(conn: sqlite3.Connection, name: str) -> int:
    """Return the id for *name*, creating the row if needed.

    Names are upper-cased before lookup/insert for consistency.
    """
    name = name.upper()
    row = conn.execute("SELECT id FROM license_endorsements WHERE name = ?", (name,)).fetchone()
    if row:
        return row[0]
    cur = conn.execute("INSERT INTO license_endorsements (name) VALUES (?)", (name,))
    return cur.lastrowid


def link_endorsement(conn: sqlite3.Connection, record_id: int, endorsement_id: int) -> None:
    """Insert a record↔endorsement link, ignoring duplicates."""
    conn.execute(
        """INSERT OR IGNORE INTO record_endorsements (record_id, endorsement_id)
           VALUES (?, ?)""",
        (record_id, endorsement_id),
    )


# ---
# Shared merge helper
# ---


def merge_endorsement(
    conn: sqlite3.Connection,
    old_id: int,
    new_id: int,
    *,
    delete_old: bool = True,
) -> int:
    """Migrate all links from endorsement *old_id* to *new_id*.

    Migrates ``record_endorsements`` and ``endorsement_codes`` rows,
    then optionally deletes the old ``license_endorsements`` row.

    Returns the number of record links migrated.
    """
    # Migrate record_endorsements
    records = conn.execute(
        "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
        (old_id,),
    ).fetchall()
    for rec in records:
        conn.execute(
            "INSERT OR IGNORE INTO record_endorsements (record_id, endorsement_id) VALUES (?, ?)",
            (rec[0], new_id),
        )
    conn.execute("DELETE FROM record_endorsements WHERE endorsement_id = ?", (old_id,))

    # Migrate endorsement_codes
    codes = conn.execute(
        "SELECT code FROM endorsement_codes WHERE endorsement_id = ?",
        (old_id,),
    ).fetchall()
    for c in codes:
        conn.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            (c[0], new_id),
        )
    conn.execute("DELETE FROM endorsement_codes WHERE endorsement_id = ?", (old_id,))

    # Delete the old endorsement row
    if delete_old:
        conn.execute("DELETE FROM license_endorsements WHERE id = ?", (old_id,))

    return len(records)


# ---
# Processing: parse raw license_type into normalized endorsements
# ---


def _process_code(
    conn: sqlite3.Connection, record_id: int, code: str, fallback_name: str | None = None
) -> int:
    """Resolve a numeric code to endorsements and link to *record_id*.

    If the code is already mapped in ``endorsement_codes``, use those
    mappings.  Otherwise, if *fallback_name* is provided (from a
    ``CODE, NAME`` raw value), create the endorsement from the name
    and register the code mapping.  As a last resort, create a numeric
    placeholder endorsement.

    Returns the number of endorsements linked.
    """
    rows = conn.execute(
        "SELECT ec.endorsement_id FROM endorsement_codes ec WHERE ec.code = ?",
        (code,),
    ).fetchall()
    if rows:
        for r in rows:
            link_endorsement(conn, record_id, r[0])
        return len(rows)

    # Unknown code — use fallback name from CODE, NAME if available
    if fallback_name:
        eid = ensure_endorsement(conn, fallback_name)
        conn.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            (code, eid),
        )
        link_endorsement(conn, record_id, eid)
        return 1

    # No name available — create a numeric placeholder
    logger.info("Unknown code '%s' for record %d; creating placeholder.", code, record_id)
    eid = ensure_endorsement(conn, code)
    conn.execute(
        "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
        (code, eid),
    )
    link_endorsement(conn, record_id, eid)
    return 1


def _sync_resolved_endorsements(conn: sqlite3.Connection, record_id: int) -> None:
    """Write the denormalized resolved_endorsements string for *record_id*.

    Joins current ``record_endorsements`` rows to ``license_endorsements`` and
    stores the semicolon-joined names in ``license_records.resolved_endorsements``.
    Called by :func:`process_record` after all endorsement links are inserted.
    """
    row = conn.execute(
        """
        SELECT group_concat(le.name, '; ')
          FROM record_endorsements re
          JOIN license_endorsements le ON le.id = re.endorsement_id
         WHERE re.record_id = ?
         ORDER BY le.name
        """,
        (record_id,),
    ).fetchone()
    resolved = row[0] if row and row[0] is not None else ""
    conn.execute(
        "UPDATE license_records SET resolved_endorsements = ? WHERE id = ?",
        (resolved, record_id),
    )


def process_record(conn: sqlite3.Connection, record_id: int, raw_license_type: str) -> int:
    """Parse a record's raw license_type and create endorsement links.

    Idempotent: deletes any existing ``record_endorsements`` rows for
    *record_id* before inserting fresh ones, so calling this function
    multiple times on the same record is safe.

    Handles three formats:
    - Numeric code: ``"450,"`` → look up code in endorsement_codes
    - Code + name: ``"450, GROCERY STORE - BEER/WINE"`` → extract code,
      use the name as the endorsement (and register the code mapping)
    - Text names: ``"GROCERY STORE - BEER/WINE; SNACK BAR"`` → split on
      semicolons, each part is an endorsement name

    Returns the number of endorsements linked.
    """
    if not raw_license_type:
        return 0

    # Delete existing links so re-processing is idempotent.
    conn.execute("DELETE FROM record_endorsements WHERE record_id = ?", (record_id,))

    cleaned = raw_license_type.rstrip(",").strip()

    # Pure numeric code (e.g. "450" after stripping trailing comma)
    if cleaned.isdigit():
        linked = _process_code(conn, record_id, cleaned)
        _sync_resolved_endorsements(conn, record_id)
        return linked

    # Historical "CODE, NAME" format (e.g. "450, GROCERY STORE - BEER/WINE")
    m = CODE_NAME_RE.match(cleaned)
    if m:
        code, name = m.group(1), m.group(2).strip()
        linked = _process_code(conn, record_id, code, fallback_name=name)
        _sync_resolved_endorsements(conn, record_id)
        return linked

    # Text — split on semicolons
    linked = 0
    for part in raw_license_type.split(";"):
        name = part.strip()
        if name:
            eid = ensure_endorsement(conn, name)
            link_endorsement(conn, record_id, eid)
            linked += 1
    _sync_resolved_endorsements(conn, record_id)
    return linked


# Enrichment version written by reprocess_endorsements.
# Bump this integer when the endorsement processing logic changes
# to trigger selective re-processing of affected records.
# Stored as TEXT in record_enrichments.version; compare in SQL with
# CAST(version AS INTEGER) < _ENDORSEMENT_REPROCESS_VERSION.
_ENDORSEMENT_REPROCESS_VERSION = 2


def reprocess_endorsements(
    conn: sqlite3.Connection,
    *,
    record_id: int | None = None,
    code: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Regenerate record_endorsements for all or a subset of records.

    This treats ``record_endorsements`` as *derived* data: existing rows
    are deleted and rebuilt from ``license_records.license_type`` using the
    current ``endorsement_codes`` mappings.  The ``record_enrichments``
    version stamp is updated to ``_ENDORSEMENT_REPROCESS_VERSION`` for
    every processed record.

    Parameters
    ----------
    conn:
        Open database connection.  The caller is responsible for committing
        (or rolling back) after this function returns.  This allows the
        caller to pair reprocessing with other writes (e.g., audit log) in
        a single atomic transaction.
    record_id:
        If given, only reprocess this single record.
    code:
        If given, only reprocess records whose ``license_type`` column
        matches this numeric code (handles both ``"450,"`` and
        ``"450, NAME"`` formats).
    dry_run:
        If True, compute what *would* be done and return the counts without
        making any database changes.

    Returns:
    -------
    dict
        ``{"records_processed": int, "endorsements_linked": int}``
    """
    if record_id is not None:
        rows = conn.execute(
            "SELECT id, license_type FROM license_records WHERE id = ?",
            (record_id,),
        ).fetchall()
    elif code is not None:
        code_stripped = code.rstrip(",").strip()
        rows = conn.execute(
            """SELECT id, license_type FROM license_records
               WHERE RTRIM(license_type, ',') = ?
                  OR license_type GLOB ? || ', *'""",
            (code_stripped, code_stripped),
        ).fetchall()
    else:
        rows = conn.execute("SELECT id, license_type FROM license_records").fetchall()

    records_processed = 0
    endorsements_linked = 0
    now = datetime.now(UTC).isoformat()

    for row in rows:
        rid, license_type = row[0], row[1]
        if not license_type:
            continue

        if dry_run:
            records_processed += 1
            continue

        linked = process_record(conn, rid, license_type)
        endorsements_linked += linked
        records_processed += 1

        conn.execute(
            """INSERT OR REPLACE INTO record_enrichments
               (record_id, step, completed_at, version)
               VALUES (?, 'endorsements', ?, ?)""",
            (rid, now, str(_ENDORSEMENT_REPROCESS_VERSION)),
        )

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
# Query helpers (used by app.py)
# ---


def get_endorsement_options(conn: sqlite3.Connection) -> list[str]:
    """Distinct canonical endorsement names linked to at least one record.

    Aliases are resolved: if an endorsement has an alias row, its canonical
    name is used instead.  This deduplicates the filter dropdown so each
    semantic endorsement appears only once.
    """
    rows = conn.execute("""
        SELECT DISTINCT
            COALESCE(canonical.name, le.name) AS display_name
        FROM license_endorsements le
        JOIN record_endorsements re ON re.endorsement_id = le.id
        LEFT JOIN endorsement_aliases ea ON ea.endorsement_id = le.id
        LEFT JOIN license_endorsements canonical
               ON canonical.id = ea.canonical_endorsement_id
        ORDER BY display_name
    """).fetchall()
    return [r[0] for r in rows]


def get_record_endorsements(
    conn: sqlite3.Connection, record_ids: list[int]
) -> dict[int, list[str]]:
    """Batch-fetch canonical endorsement names for a list of record ids.

    Alias resolution is applied: if the endorsement linked to a record has an
    alias row, the canonical name is returned instead of the variant name.
    """
    if not record_ids:
        return {}
    chunk_size = 500
    result: dict[int, list[str]] = {rid: [] for rid in record_ids}
    for i in range(0, len(record_ids), chunk_size):
        batch = record_ids[i : i + chunk_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(
            f"""
            SELECT re.record_id,
                   COALESCE(canonical.name, le.name) AS display_name
            FROM record_endorsements re
            JOIN license_endorsements le ON le.id = re.endorsement_id
            LEFT JOIN endorsement_aliases ea ON ea.endorsement_id = le.id
            LEFT JOIN license_endorsements canonical
                   ON canonical.id = ea.canonical_endorsement_id
            WHERE re.record_id IN ({placeholders})
            ORDER BY re.record_id, display_name
        """,
            batch,
        ).fetchall()  # parameterized query, not injection risk
        for r in rows:
            result[r[0]].append(r[1])
    return result


# ---
# Alias management (admin interface helpers)
# ---


def resolve_endorsement(conn: sqlite3.Connection, endorsement_id: int) -> int:
    """Return the canonical endorsement ID for *endorsement_id*.

    If *endorsement_id* has an alias row, the ``canonical_endorsement_id``
    is returned.  Otherwise the same ID is returned unchanged.

    Parameters
    ----------
    conn:
        Open database connection.
    endorsement_id:
        Primary key of the endorsement to resolve.
    """
    row = conn.execute(
        "SELECT canonical_endorsement_id FROM endorsement_aliases WHERE endorsement_id = ?",
        (endorsement_id,),
    ).fetchone()
    return row[0] if row else endorsement_id


def set_canonical_endorsement(
    conn: sqlite3.Connection,
    canonical_id: int,
    variant_ids: list[int],
    created_by: str | None = None,
) -> int:
    """Create alias rows mapping each *variant_id* to *canonical_id*.

    Uses ``INSERT … ON CONFLICT(endorsement_id) DO UPDATE`` so re-running is
    idempotent: existing alias rows for the same ``endorsement_id`` are
    updated in place (preserving their ``id`` primary key).

    Parameters
    ----------
    conn:
        Open database connection — caller must commit.
    canonical_id:
        Primary key of the endorsement to mark as canonical.
    variant_ids:
        Primary keys of endorsements that should alias to *canonical_id*.
    created_by:
        Admin email to record in the audit trail column.

    Returns:
    -------
    int
        Number of alias rows written (created or updated).
    """
    written = 0
    for vid in variant_ids:
        if vid == canonical_id:
            continue  # never alias an endorsement to itself
        conn.execute(
            """
            INSERT INTO endorsement_aliases
                (endorsement_id, canonical_endorsement_id, created_by)
            VALUES (?, ?, ?)
            ON CONFLICT(endorsement_id) DO UPDATE SET
                canonical_endorsement_id = excluded.canonical_endorsement_id,
                created_by = excluded.created_by,
                created_at = datetime('now')
            """,
            (vid, canonical_id, created_by),
        )
        written += 1
    logger.info(
        "set_canonical: %d alias(es) → endorsement#%d by %s",
        written,
        canonical_id,
        created_by,
    )
    return written


def rename_endorsement(
    conn: sqlite3.Connection,
    endorsement_id: int,
    new_name: str,
    created_by: str | None = None,
) -> int:
    """Assign a text name to an endorsement (typically a bare numeric code).

    Creates a new ``license_endorsements`` row with *new_name* (or reuses one
    that already exists), then creates an alias row mapping *endorsement_id*
    to the new (canonical) ID.  The original row is left intact for history.

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

    # Reuse an existing endorsement with that name, or create one
    existing = conn.execute(
        "SELECT id FROM license_endorsements WHERE name = ?",
        (new_name,),
    ).fetchone()
    if existing:
        canonical_id = existing[0]
    else:
        canonical_id = conn.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)",
            (new_name,),
        ).lastrowid

    # Create the alias (idempotent)
    set_canonical_endorsement(
        conn,
        canonical_id=canonical_id,
        variant_ids=[endorsement_id],
        created_by=created_by,
    )
    logger.info(
        "rename_endorsement: #%d → '%s' (#%d) by %s",
        endorsement_id,
        new_name,
        canonical_id,
        created_by,
    )
    return canonical_id


def get_endorsement_groups(
    conn: sqlite3.Connection,
) -> list[dict]:
    """Return all endorsements grouped by numeric code for the admin UI.

    Returns a list of group dicts, each with keys:

    - ``code`` — the WSLCB numeric code string (or ``None`` for ungrouped)
    - ``endorsements`` — list of endorsement dicts with keys:
        - ``id``, ``name``, ``record_count``, ``is_canonical``, ``is_variant``,
          ``canonical_id`` (None if not a variant)

    Groups are ordered by code (numeric), with the ungrouped section last.
    """
    # Fetch all endorsements with record counts
    rows = conn.execute("""
        SELECT
            le.id,
            le.name,
            COUNT(re.record_id) AS record_count
        FROM license_endorsements le
        LEFT JOIN record_endorsements re ON re.endorsement_id = le.id
        GROUP BY le.id
        ORDER BY le.name
    """).fetchall()

    # Fetch alias map: variant_id → canonical_id
    alias_rows = conn.execute(
        "SELECT endorsement_id, canonical_endorsement_id FROM endorsement_aliases"
    ).fetchall()
    alias_map: dict[int, int] = {r[0]: r[1] for r in alias_rows}
    # Set of IDs that are canonical for at least one alias
    canonical_ids: set[int] = set(alias_map.values())

    # Fetch code memberships: endorsement_id → list[code]
    code_rows = conn.execute("SELECT endorsement_id, code FROM endorsement_codes").fetchall()
    eid_to_codes: dict[int, list[str]] = {}
    for eid, code in code_rows:
        eid_to_codes.setdefault(eid, []).append(code)

    # Build per-code buckets
    code_buckets: dict[str, list[dict]] = {}
    ungrouped: list[dict] = []

    for row in rows:
        eid, name, count = row
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
                # Use a shallow copy so each group's list holds independent
                # dicts even when one endorsement belongs to multiple codes.
                code_buckets.setdefault(code, []).append(dict(entry))
        else:
            ungrouped.append(dict(entry))

    # Sort groups numerically where possible
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
