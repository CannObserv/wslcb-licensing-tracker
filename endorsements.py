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

TODO: FTS currently indexes raw license_type values, which are numeric
codes for approved/discontinued records.  Text search for endorsement
names won't match those records — only the endorsement filter works.
Fixing this would require indexing resolved endorsement names in FTS.
"""
import logging
import re
import sqlite3

logger = logging.getLogger(__name__)

# Matches the historical "CODE, NAME" format used in approved/discontinued
# sections before ~2025 (e.g. "450, GROCERY STORE - BEER/WINE").  The
# first capturing group is the numeric code, the second is the name.
# Handles names that themselves contain commas (e.g. "< 250,000 LITERS").
_CODE_NAME_RE = re.compile(r"^(\d+),\s+(.+)$")

# ---
# Seed data: WSLCB code → endorsement name(s), built from cross-referencing
# license numbers that appear in both new-application (text) and
# approved/discontinued (numeric code) sections.
#
# Most codes map 1-to-1.  A handful map to multiple always-present
# endorsements (e.g. 320 always includes BEER DISTRIBUTOR + WINE DISTRIBUTOR).
# ---
# Keys are string representations of WSLCB internal license class IDs,
# stored as TEXT in the DB.
SEED_CODE_MAP: dict[str, list[str]] = {
    "0":   ["UNDEFINED"],
    "1":   ["SPECIAL OCCASION-PER DAY-PER LOC."],
    "2":   ["NON-PROFIT ARTS ORGANIZATION"],
    "3":   ["BED & BREAKFAST"],
    "4":   ["SERVE EMPLOYEES & GUESTS"],
    "13":  ["FARMERS MARKET FOR WINE"],
    "99":  ["TRIBAL MOA"],
    "14":  ["FARMERS MARKET FOR BEER"],
    "15":  ["FARMERS MARKET FOR BEER/WINE"],
    "18":  ["RETAIL CERTIFICATE HOLDER"],
    "56":  ["GROCERY STORE - BEER/WINE"],
    "63":  ["GROCERY STORE - BEER/WINE"],
    "98":  ["TRIBAL COMPACT"],
    "136": ["GROCERY STORE - BEER/WINE"],
    "320": ["BEER DISTRIBUTOR", "WINE DISTRIBUTOR"],
    "321": ["BEER IMPORTER"],
    "322": ["BONDED WINE WAREHOUSE"],
    "323": ["INTERSTATE COMMON CARRIER"],
    "325": ["DISTILL / RECTIFY"],
    "326": ["DOMESTIC BREWERY"],
    "327": ["DOMESTIC WINERY < 250,000 LITERS"],
    "328": ["DOMESTIC WINERY > 249,999 LITERS"],
    "329": ["FRUIT AND/OR WINE DISTILLERY"],
    "330": ["SPIRITS IMPORTER"],
    "331": ["MANUFACTURER - LIQUOR"],
    "332": ["MICROBREWERY"],
    "333": ["SHIPS CHANDLER"],
    "334": ["WINE DISTRIBUTOR"],
    "335": ["GROWER"],
    "336": ["WINE IMPORTER"],
    "337": ["DOMESTIC WINERY < 250,000 ADDL LOC"],
    "338": ["DOMESTIC WINERY > 249,999 ADDL LOC"],
    "340": ["BEER CERTIFICATE OF APPROVAL"],
    "341": ["WINE CERTIFICATE OF APPROVAL"],
    "342": ["AUTH REP COA US BEER"],
    "343": ["AUTH REP COA US WINE"],
    "344": ["AUTH REP COA FOREIGN BEER"],
    "345": ["AUTH REP COA FOREIGN WINE"],
    "346": ["COA SHIPPER TO CONSUMER"],
    "347": ["WINE SHIPPER TO CONSUMER"],
    "348": ["SHIP TO RETAILER", "SPIRITS COA"],
    "349": ["DIRECT SHIPMENT RECEIVER-IN/OUT WA"],
    "350": ["DIRECT SHIPMENT RECEIVER-IN WA ONLY"],
    "351": ["CRAFT DISTILLERY"],
    "352": ["MICROBREWERY WAREHOUSE"],
    "353": ["WINERY WAREHOUSE"],
    "354": ["SPIRITS DISTRIBUTOR"],
    "355": ["SPIRITS COA"],
    "356": ["AUTH REP US SPIRITS COA"],
    "357": ["AUTH REP FOREIGN SPIRITS COA"],
    "358": ["B/W ON PREMISES ENDORSEMENT"],
    "359": ["OFF-SITE SPIRITS TASTING ROOM"],
    "371": ["BEER/CIDER GROCERY GROWLERS"],
    "372": ["COMBO GROCERY OFF PREM S/B/W"],
    "373": ["SPIRITS WAREHOUSE"],
    "376": ["COMBO SPECIALTY OFF PREM S/B/W"],
    "379": ["TAKEOUT/DELIVERY"],
    "380": ["PREMIXED COCKTAILS/WINE TO-GO"],
    "381": ["GROWLERS TAKEOUT/DELIVERY"],
    "386": ["SE CANNABIS RETAILER"],
    "387": ["CANNABIS TRANSPORTATION"],
    "388": ["CANNABIS RESEARCH"],
    "390": ["CANNABIS PRODUCER TIER 1"],
    "391": ["CANNABIS PRODUCER TIER 2"],
    "392": ["CANNABIS PRODUCER TIER 3"],
    "393": ["CANNABIS PROCESSOR"],
    "394": ["CANNABIS RETAILER"],
    "398": ["CATERING BEER AND WINE ONLY"],
    "399": ["CATERING SPIRITS BEER WINE"],
    "400": ["SPIRITS RETAILER", "SPIRITS SAMPLING"],
    "402": ["BEER/WINE THEATER"],
    "403": ["SPIRITS/BEER/WINE THEATER"],
    "412": ["SOJU SERVICE"],
    "413": ["SENIOR CENTER"],
    "420": ["BEER/WINE GIFT DELIVERY"],
    "422": ["BEER/WINE SPECIALTY SHOP"],
    "424": ["SPIRITS/BR/WN REST LOUNGE +"],
    "425": ["SPIRITS/BR/WN REST LOUNGE + SEAS"],
    "426": ["SPIRITS/BR/WN REST LOUNGE -"],
    "427": ["SPIRITS/BR/WN REST LOUNGE - SEAS"],
    "430": ["SPIRITS/BR/WN REST CONVENTION CTR +"],
    "431": ["SPIRITS/BR/WN REST CONVENTION CTR -"],
    "433": ["SPIRITS/BR/WN REST AIRPORT BAR +"],
    "435": ["VIP AIRPORT LOUNGE"],
    "438": ["HOTEL"],
    "439": ["SPIRITS/BR/WN REST NONPUBLIC +"],
    "440": ["SPIRITS/BR/WN REST NONPUBLIC -"],
    "442": ["SPIRITS/BR/WN REST SERVICE BAR"],
    "450": ["GROCERY STORE - BEER/WINE"],
    "451": ["GROCERY STORE-RESTRICT FORT WINE"],
    "452": ["BEER/WINE REST - BEER"],
    "456": ["BEER/WINE REST - BEER W/TAPROOM"],
    "457": ["BEER/WINE REST - WINE"],
    "462": ["BEER/WINE REST - BEER/WINE"],
    "466": ["BEER/WINE REST-BEER/WINE W/TAPROOM"],
    "467": ["MOTEL"],
    "468": ["PRIVATE CLUB - BEER/WINE"],
    "469": ["PRIVATE CLUB - SPIRITS/BEER/WINE"],
    "473": ["SNACK BAR"],
    "477": ["SPORTS ENTERTAINMENT FACILITY"],
    "479": ["TAVERN - WINE"],
    "480": ["TAVERN - BEER/WINE"],
    "481": ["NIGHTCLUB"],
}


# ---
# Endorsement CRUD helpers
# ---

def _ensure_endorsement(conn: sqlite3.Connection, name: str) -> int:
    """Return the id for *name*, creating the row if needed.

    Names are upper-cased before lookup/insert for consistency.
    """
    name = name.upper()
    row = conn.execute(
        "SELECT id FROM license_endorsements WHERE name = ?", (name,)
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO license_endorsements (name) VALUES (?)", (name,)
    )
    return cur.lastrowid


def _link_endorsement(conn: sqlite3.Connection, record_id: int, endorsement_id: int):
    """Insert a record↔endorsement link, ignoring duplicates."""
    conn.execute(
        """INSERT OR IGNORE INTO record_endorsements (record_id, endorsement_id)
           VALUES (?, ?)""",
        (record_id, endorsement_id),
    )


# ---
# Shared merge helper
# ---


def _merge_endorsement(
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
            "INSERT OR IGNORE INTO record_endorsements (record_id, endorsement_id) "
            "VALUES (?, ?)",
            (rec[0], new_id),
        )
    conn.execute(
        "DELETE FROM record_endorsements WHERE endorsement_id = ?", (old_id,)
    )

    # Migrate endorsement_codes
    codes = conn.execute(
        "SELECT code FROM endorsement_codes WHERE endorsement_id = ?",
        (old_id,),
    ).fetchall()
    for c in codes:
        conn.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) "
            "VALUES (?, ?)",
            (c[0], new_id),
        )
    conn.execute(
        "DELETE FROM endorsement_codes WHERE endorsement_id = ?", (old_id,)
    )

    # Delete the old endorsement row
    if delete_old:
        conn.execute(
            "DELETE FROM license_endorsements WHERE id = ?", (old_id,)
        )

    return len(records)


# ---
# Schema seeding
# ---

def seed_endorsements(conn: sqlite3.Connection) -> int:
    """Populate license_endorsements and endorsement_codes from SEED_CODE_MAP.

    Safe to call repeatedly — skips existing rows.  After seeding, merges
    any placeholder endorsements (where the endorsement name equals the
    numeric code) that now have real mappings.

    Returns the number of new code mappings inserted.
    """
    inserted = 0
    for code, names in SEED_CODE_MAP.items():
        for name in names:
            eid = _ensure_endorsement(conn, name)
            cur = conn.execute(
                """INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id)
                   VALUES (?, ?)""",
                (code, eid),
            )
            inserted += cur.rowcount
    conn.commit()

    # Merge any placeholder endorsements now that seed mappings exist.
    _merge_seeded_placeholders(conn)

    return inserted


# ---
# Repair: merge mixed-case endorsement duplicates
# ---

def merge_mixed_case_endorsements(conn: sqlite3.Connection) -> int:
    """Merge endorsements whose names differ only by case.

    For each endorsement where ``name != UPPER(name)`` and an UPPER
    counterpart already exists, migrate all record links and code
    mappings to the canonical (upper-case) row via
    ``_merge_endorsement()``, then delete the mixed-case row.  If no
    upper-case counterpart exists, the mixed-case row is simply
    renamed in place.

    Returns the number of endorsements fixed.
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
            # No upper counterpart — rename in place
            conn.execute(
                "UPDATE license_endorsements SET name = ? WHERE id = ?",
                (upper_name, mixed_id),
            )
            logger.info("Renamed endorsement %r → %r (id=%d)",
                        mixed_name, upper_name, mixed_id)
            continue

        _merge_endorsement(conn, mixed_id, upper_row[0])
        logger.info("Merged endorsement %r (id=%d) into %r (id=%d)",
                    mixed_name, mixed_id, upper_name, upper_row[0])

    conn.commit()
    return len(dupes)


# ---
# Repair: migrate "CODE, NAME" endorsements to proper names
# ---

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
    """
    # Find all endorsements matching the CODE, NAME pattern.
    bogus = conn.execute(
        "SELECT id, name FROM license_endorsements WHERE name GLOB '[0-9]*, *'"
    ).fetchall()
    if not bogus:
        # Also clean up space-codes even if no CODE, NAME endorsements remain.
        deleted = _cleanup_space_codes(conn)
        if deleted:
            conn.commit()
        return 0

    migrated = 0
    for eid_old, full_name in bogus:
        m = _CODE_NAME_RE.match(full_name)
        if not m:
            continue
        code, name = m.group(1), m.group(2).strip()

        # Determine the target endorsement(s) for this code.
        # Prefer existing endorsement_codes mappings (from SEED_CODE_MAP)
        # so we converge on the canonical name.
        mapped_eids = conn.execute(
            """SELECT ec.endorsement_id FROM endorsement_codes ec
               JOIN license_endorsements le ON le.id = ec.endorsement_id
               WHERE ec.code = ? AND le.name != ?""",
            (code, full_name),
        ).fetchall()

        if mapped_eids:
            target_eids = [r[0] for r in mapped_eids]
        else:
            # No existing mapping — use the name from the CODE, NAME value.
            target_eid = _ensure_endorsement(conn, name)
            conn.execute(
                "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) "
                "VALUES (?, ?)",
                (code, target_eid),
            )
            target_eids = [target_eid]

        # Merge into first target, then add links to any extras.
        migrated += _merge_endorsement(conn, eid_old, target_eids[0])
        if len(target_eids) > 1:
            # Link records to the additional target endorsements.
            records = conn.execute(
                "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
                (target_eids[0],),
            ).fetchall()
            for rec in records:
                for tgt in target_eids[1:]:
                    _link_endorsement(conn, rec[0], tgt)

    # Clean up bogus endorsement_codes with spaces in the code column.
    _cleanup_space_codes(conn)

    if migrated:
        conn.commit()
        logger.info(
            "Repaired %d record-endorsement link(s) from %d 'CODE, NAME' "
            "endorsement(s).",
            migrated, len(bogus),
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


# ---
# Processing: parse raw license_type into normalized endorsements
# ---

def _process_code(conn: sqlite3.Connection, record_id: int,
                  code: str, fallback_name: str | None = None) -> int:
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
            _link_endorsement(conn, record_id, r[0])
        return len(rows)

    # Unknown code — use fallback name from CODE, NAME if available
    if fallback_name:
        eid = _ensure_endorsement(conn, fallback_name)
        conn.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            (code, eid),
        )
        _link_endorsement(conn, record_id, eid)
        return 1

    # No name available — create a numeric placeholder
    logger.info("Unknown code '%s' for record %d; creating placeholder.", code, record_id)
    eid = _ensure_endorsement(conn, code)
    conn.execute(
        "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
        (code, eid),
    )
    _link_endorsement(conn, record_id, eid)
    return 1


def process_record(conn: sqlite3.Connection, record_id: int,
                   raw_license_type: str) -> int:
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
    conn.execute(
        "DELETE FROM record_endorsements WHERE record_id = ?", (record_id,)
    )

    cleaned = raw_license_type.rstrip(",").strip()

    # Pure numeric code (e.g. "450" after stripping trailing comma)
    if cleaned.isdigit():
        return _process_code(conn, record_id, cleaned)

    # Historical "CODE, NAME" format (e.g. "450, GROCERY STORE - BEER/WINE")
    m = _CODE_NAME_RE.match(cleaned)
    if m:
        code, name = m.group(1), m.group(2).strip()
        return _process_code(conn, record_id, code, fallback_name=name)

    # Text — split on semicolons
    linked = 0
    for part in raw_license_type.split(";"):
        name = part.strip()
        if name:
            eid = _ensure_endorsement(conn, name)
            _link_endorsement(conn, record_id, eid)
            linked += 1
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

    Returns
    -------
    dict
        ``{"records_processed": int, "endorsements_linked": int}``
    """
    from datetime import datetime, timezone

    # Build the query to select target records.
    if record_id is not None:
        rows = conn.execute(
            "SELECT id, license_type FROM license_records WHERE id = ?",
            (record_id,),
        ).fetchall()
    elif code is not None:
        # Match both bare "CODE," and "CODE, NAME" forms.
        code_stripped = code.rstrip(",").strip()
        rows = conn.execute(
            """SELECT id, license_type FROM license_records
               WHERE RTRIM(license_type, ',') = ?
                  OR license_type GLOB ? || ', *'""",
            (code_stripped, code_stripped),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, license_type FROM license_records"
        ).fetchall()

    records_processed = 0
    endorsements_linked = 0
    now = datetime.now(timezone.utc).isoformat()

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

        # Update enrichment version stamp.
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
            records_processed, endorsements_linked,
        )

    return {"records_processed": records_processed, "endorsements_linked": endorsements_linked}


def backfill(conn: sqlite3.Connection) -> int:
    """Process all records that don't yet have endorsement links.

    Returns the number of records processed.
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

    if rows:
        conn.commit()
    return len(rows)


# ---
# Code-mapping discovery (run after each scrape)
# ---

def discover_code_mappings(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """Cross-reference license numbers to learn new code→name mappings.

    For each unmapped numeric code, find new_application records sharing
    the same license_number.  When every matched text record contains the
    same endorsement(s), adopt that as the mapping.

    Returns {code: [name, ...]} for newly discovered mappings.
    """
    # Codes that map to at least one real (non-placeholder) endorsement.
    # A placeholder endorsement has name == code (e.g. code "321" →
    # endorsement named "321"); these should be treated as unmapped so
    # we can resolve them when cross-reference data becomes available.
    mapped = set(
        r[0] for r in conn.execute("""
            SELECT DISTINCT ec.code
            FROM endorsement_codes ec
            JOIN license_endorsements le ON le.id = ec.endorsement_id
            WHERE le.name != ec.code
        """).fetchall()
    )

    # All numeric codes in the data.  Handles both "450," (pure code)
    # and "450, GROCERY STORE - BEER/WINE" (historical CODE, NAME).
    all_codes: set[str] = set()
    rows = conn.execute("""
        SELECT DISTINCT license_type
        FROM license_records
        WHERE section_type IN ('approved', 'discontinued')
          AND license_type GLOB '[0-9]*'
    """).fetchall()
    for r in rows:
        raw = r[0].rstrip(",").strip()
        m = _CODE_NAME_RE.match(raw)
        if m:
            all_codes.add(m.group(1))
        elif raw.isdigit():
            all_codes.add(raw)
    unmapped = [c for c in all_codes if c not in mapped]

    if not unmapped:
        return {}

    learned: dict[str, list[str]] = {}
    for code in unmapped:
        matches = conn.execute("""
            SELECT n.license_type AS text_type, COUNT(*) AS cnt
            FROM license_records a
            JOIN license_records n
                ON a.license_number = n.license_number
                AND n.section_type = 'new_application'
            WHERE SUBSTR(a.license_type, 1, INSTR(a.license_type, ',') - 1) = ?
              AND a.section_type IN ('approved', 'discontinued')
            GROUP BY n.license_type
        """, (code,)).fetchall()
        if not matches:
            continue

        total = sum(r["cnt"] for r in matches)
        type_freq: dict[str, int] = {}
        for r in matches:
            for t in r["text_type"].split(";"):
                key = t.strip()
                type_freq[key] = type_freq.get(key, 0) + r["cnt"]

        # Endorsements present in every single match
        always = [t for t, c in type_freq.items() if c == total and t]
        if not always:
            continue

        for name in always:
            eid = _ensure_endorsement(conn, name)
            conn.execute(
                "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
                (code, eid),
            )
        learned[code] = always

    if learned:
        # Resolve any placeholder endorsements that were just mapped
        _merge_placeholders(conn, learned)
        conn.commit()
    return learned


def _merge_placeholders(conn: sqlite3.Connection, learned: dict[str, list[str]]):
    """If a code had a placeholder endorsement (name == code), migrate links."""
    for code, names in learned.items():
        placeholder = conn.execute(
            "SELECT id FROM license_endorsements WHERE name = ?", (code,)
        ).fetchone()
        if not placeholder:
            continue
        pid = placeholder[0]
        # Merge into the first real endorsement
        first_eid = _ensure_endorsement(conn, names[0])
        _merge_endorsement(conn, pid, first_eid)
        # Link records to any additional endorsements
        if len(names) > 1:
            records = conn.execute(
                "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
                (first_eid,),
            ).fetchall()
            for rec in records:
                for name in names[1:]:
                    eid = _ensure_endorsement(conn, name)
                    _link_endorsement(conn, rec[0], eid)


def _merge_seeded_placeholders(conn: sqlite3.Connection) -> int:
    """Merge placeholder endorsements that now have real seed mappings.

    A placeholder endorsement has ``name == code`` (e.g. endorsement named
    ``"331"`` for code ``"331"``).  If ``seed_endorsements`` has since
    registered a real mapping for that code, migrate all record links from
    the placeholder to the real endorsement(s) and delete the placeholder.

    Returns the number of record links migrated.
    """
    # Find placeholder endorsements: name is purely numeric and matches a code
    # that also has at least one *real* (non-placeholder) endorsement.
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
        # Real endorsement(s) for this code
        real_eids = [r[0] for r in conn.execute("""
            SELECT ec.endorsement_id FROM endorsement_codes ec
            JOIN license_endorsements le ON le.id = ec.endorsement_id
            WHERE ec.code = ? AND le.name != ?
        """, (code, code)).fetchall()]
        if not real_eids:
            continue

        # Merge into first real endorsement
        count = _merge_endorsement(conn, pid, real_eids[0])
        migrated += count
        # Link records to any additional endorsements
        if len(real_eids) > 1:
            records = conn.execute(
                "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
                (real_eids[0],),
            ).fetchall()
            for rec in records:
                for eid in real_eids[1:]:
                    _link_endorsement(conn, rec[0], eid)

    if migrated:
        conn.commit()
        logger.info(
            "Merged %d record link(s) from %d placeholder endorsement(s).",
            migrated, len(placeholders),
        )
    return migrated


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


def get_record_endorsements(conn: sqlite3.Connection,
                            record_ids: list[int]) -> dict[int, list[str]]:
    """Batch-fetch canonical endorsement names for a list of record ids.

    Alias resolution is applied: if the endorsement linked to a record has an
    alias row, the canonical name is returned instead of the variant name.
    """
    if not record_ids:
        return {}
    CHUNK = 500
    result: dict[int, list[str]] = {rid: [] for rid in record_ids}
    for i in range(0, len(record_ids), CHUNK):
        batch = record_ids[i:i + CHUNK]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(f"""
            SELECT re.record_id,
                   COALESCE(canonical.name, le.name) AS display_name
            FROM record_endorsements re
            JOIN license_endorsements le ON le.id = re.endorsement_id
            LEFT JOIN endorsement_aliases ea ON ea.endorsement_id = le.id
            LEFT JOIN license_endorsements canonical
                   ON canonical.id = ea.canonical_endorsement_id
            WHERE re.record_id IN ({placeholders})
            ORDER BY re.record_id, display_name
        """, batch).fetchall()
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
        "SELECT canonical_endorsement_id FROM endorsement_aliases"
        " WHERE endorsement_id = ?",
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

    Returns
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
        written, canonical_id, created_by,
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

    Returns
    -------
    int
        Primary key of the canonical (named) endorsement.
    """
    if not new_name:
        raise ValueError("new_name must be a non-empty string")

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
        endorsement_id, new_name, canonical_id, created_by,
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
    code_rows = conn.execute(
        "SELECT endorsement_id, code FROM endorsement_codes"
    ).fetchall()
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
