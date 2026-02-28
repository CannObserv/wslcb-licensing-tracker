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
    mappings to the canonical (upper-case) row, then delete the
    mixed-case row.  If no upper-case counterpart exists, the
    mixed-case row is simply renamed in place.

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

        # Find or create the canonical upper-case row
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

        upper_id = upper_row[0]

        # Migrate record_endorsements links
        records = conn.execute(
            "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
            (mixed_id,),
        ).fetchall()
        for rec in records:
            conn.execute(
                """INSERT OR IGNORE INTO record_endorsements (record_id, endorsement_id)
                   VALUES (?, ?)""",
                (rec[0], upper_id),
            )
        conn.execute(
            "DELETE FROM record_endorsements WHERE endorsement_id = ?",
            (mixed_id,),
        )

        # Migrate endorsement_codes mappings
        codes = conn.execute(
            "SELECT code FROM endorsement_codes WHERE endorsement_id = ?",
            (mixed_id,),
        ).fetchall()
        for c in codes:
            conn.execute(
                """INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id)
                   VALUES (?, ?)""",
                (c[0], upper_id),
            )
        conn.execute(
            "DELETE FROM endorsement_codes WHERE endorsement_id = ?",
            (mixed_id,),
        )

        # Delete the mixed-case endorsement row
        conn.execute(
            "DELETE FROM license_endorsements WHERE id = ?", (mixed_id,)
        )
        logger.info("Merged endorsement %r (id=%d) into %r (id=%d)",
                    mixed_name, mixed_id, upper_name, upper_id)

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

        # Migrate all record links from the bogus endorsement to the target(s).
        records = conn.execute(
            "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
            (eid_old,),
        ).fetchall()
        for rec in records:
            for tgt in target_eids:
                _link_endorsement(conn, rec[0], tgt)
            migrated += 1

        # Remove old links and the bogus endorsement.
        conn.execute(
            "DELETE FROM record_endorsements WHERE endorsement_id = ?", (eid_old,)
        )
        conn.execute(
            "DELETE FROM endorsement_codes WHERE endorsement_id = ?", (eid_old,)
        )
        conn.execute(
            "DELETE FROM license_endorsements WHERE id = ?", (eid_old,)
        )

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
        # Point all record links from placeholder to the real endorsement(s)
        records = conn.execute(
            "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?", (pid,)
        ).fetchall()
        for rec in records:
            for name in names:
                eid = _ensure_endorsement(conn, name)
                _link_endorsement(conn, rec[0], eid)
        # Remove old links and placeholder
        conn.execute("DELETE FROM record_endorsements WHERE endorsement_id = ?", (pid,))
        conn.execute("DELETE FROM endorsement_codes WHERE endorsement_id = ?", (pid,))
        conn.execute("DELETE FROM license_endorsements WHERE id = ?", (pid,))


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

        records = conn.execute(
            "SELECT record_id FROM record_endorsements WHERE endorsement_id = ?",
            (pid,),
        ).fetchall()
        for rec in records:
            for eid in real_eids:
                _link_endorsement(conn, rec[0], eid)
            migrated += 1

        conn.execute("DELETE FROM record_endorsements WHERE endorsement_id = ?", (pid,))
        conn.execute("DELETE FROM endorsement_codes WHERE endorsement_id = ?", (pid,))
        conn.execute("DELETE FROM license_endorsements WHERE id = ?", (pid,))

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
    """Distinct endorsement names that are linked to at least one record."""
    rows = conn.execute("""
        SELECT DISTINCT le.name
        FROM license_endorsements le
        JOIN record_endorsements re ON re.endorsement_id = le.id
        ORDER BY le.name
    """).fetchall()
    return [r[0] for r in rows]


def get_record_endorsements(conn: sqlite3.Connection,
                            record_ids: list[int]) -> dict[int, list[str]]:
    """Batch-fetch endorsement names for a list of record ids."""
    if not record_ids:
        return {}
    CHUNK = 500
    result: dict[int, list[str]] = {rid: [] for rid in record_ids}
    for i in range(0, len(record_ids), CHUNK):
        batch = record_ids[i:i + CHUNK]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(f"""
            SELECT re.record_id, le.name
            FROM record_endorsements re
            JOIN license_endorsements le ON le.id = re.endorsement_id
            WHERE re.record_id IN ({placeholders})
            ORDER BY re.record_id, le.name
        """, batch).fetchall()
        for r in rows:
            result[r[0]].append(r[1])
    return result
