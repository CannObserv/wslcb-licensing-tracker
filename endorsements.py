"""License endorsement normalization for WSLCB licensing tracker.

The WSLCB source page represents license types differently by section:
- New applications: semicolon-separated text names
  (e.g. "GROCERY STORE - BEER/WINE; SNACK BAR")
- Approved/discontinued: opaque integer codes (e.g. "450,")

This module normalizes both representations into a shared
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
import sqlite3

logger = logging.getLogger(__name__)

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
    "2":   ["NON-PROFIT ARTS ORGANIZATION"],
    "14":  ["FARMERS MARKET FOR BEER"],
    "56":  ["GROCERY STORE - BEER/WINE"],
    "63":  ["GROCERY STORE - BEER/WINE"],
    "136": ["GROCERY STORE - BEER/WINE"],
    "320": ["BEER DISTRIBUTOR", "WINE DISTRIBUTOR"],
    "322": ["BONDED WINE WAREHOUSE"],
    "323": ["INTERSTATE COMMON CARRIER"],
    "325": ["DISTILL / RECTIFY"],
    "327": ["DOMESTIC WINERY < 250,000 LITERS"],
    "330": ["SPIRITS IMPORTER"],
    "332": ["MICROBREWERY"],
    "334": ["WINE DISTRIBUTOR"],
    "335": ["GROWER"],
    "336": ["WINE IMPORTER"],
    "337": ["DOMESTIC WINERY < 250,000 ADDL LOC"],
    "340": ["BEER CERTIFICATE OF APPROVAL"],
    "341": ["WINE CERTIFICATE OF APPROVAL"],
    "342": ["AUTH REP COA US BEER"],
    "343": ["AUTH REP COA US WINE"],
    "344": ["AUTH REP COA FOREIGN BEER"],
    "345": ["AUTH REP COA FOREIGN WINE"],
    "347": ["WINE SHIPPER TO CONSUMER"],
    "348": ["SHIP TO RETAILER", "SPIRITS COA"],
    "349": ["DIRECT SHIPMENT RECEIVER-IN/OUT WA"],
    "350": ["DIRECT SHIPMENT RECEIVER-IN WA ONLY"],
    "351": ["CRAFT DISTILLERY"],
    "353": ["WINERY WAREHOUSE"],
    "354": ["SPIRITS DISTRIBUTOR"],
    "355": ["SPIRITS COA"],
    "356": ["AUTH REP US SPIRITS COA"],
    "357": ["AUTH REP FOREIGN SPIRITS COA"],
    "359": ["OFF-SITE SPIRITS TASTING ROOM"],
    "371": ["BEER/CIDER GROCERY GROWLERS"],
    "372": ["COMBO GROCERY OFF PREM S/B/W"],
    "379": ["TAKEOUT/DELIVERY"],
    "380": ["PREMIXED COCKTAILS/WINE TO-GO"],
    "381": ["GROWLERS TAKEOUT/DELIVERY"],
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
    "426": ["SPIRITS/BR/WN REST LOUNGE -"],
    "438": ["HOTEL"],
    "439": ["SPIRITS/BR/WN REST NONPUBLIC +"],
    "442": ["SPIRITS/BR/WN REST SERVICE BAR"],
    "450": ["GROCERY STORE - BEER/WINE"],
    "452": ["BEER/WINE REST - BEER"],
    "457": ["BEER/WINE REST - WINE"],
    "462": ["BEER/WINE REST - BEER/WINE"],
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
    """Return the id for *name*, creating the row if needed."""
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

    Safe to call repeatedly — skips existing rows.
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
    return inserted


# ---
# Processing: parse raw license_type into normalized endorsements
# ---

def process_record(conn: sqlite3.Connection, record_id: int,
                   raw_license_type: str, section_type: str) -> int:
    """Parse a record's raw license_type and create endorsement links.

    For new_application records the value is semicolon-separated text.
    For approved/discontinued it is a numeric code.
    Returns the number of endorsements linked.
    """
    if not raw_license_type:
        return 0

    cleaned = raw_license_type.rstrip(",").strip()

    if cleaned.isdigit():
        # Numeric code — look up via endorsement_codes
        rows = conn.execute(
            """SELECT ec.endorsement_id
               FROM endorsement_codes ec
               WHERE ec.code = ?""",
            (cleaned,),
        ).fetchall()
        if rows:
            for r in rows:
                _link_endorsement(conn, record_id, r[0])
            return len(rows)
        # Unknown code — create a placeholder endorsement named after the code
        logger.info("Unknown code '%s' for record %d; creating placeholder.", cleaned, record_id)
        eid = _ensure_endorsement(conn, cleaned)
        conn.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            (cleaned, eid),
        )
        _link_endorsement(conn, record_id, eid)
        return 1

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
        process_record(conn, r["id"], r["license_type"], r["section_type"])

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

    # All numeric codes in the data
    all_codes = conn.execute("""
        SELECT DISTINCT REPLACE(license_type, ',', '') AS code
        FROM license_records
        WHERE section_type IN ('approved', 'discontinued')
          AND license_type GLOB '[0-9]*'
    """).fetchall()
    unmapped = [r[0] for r in all_codes if r[0] not in mapped]

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
            WHERE REPLACE(a.license_type, ',', '') = ?
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
