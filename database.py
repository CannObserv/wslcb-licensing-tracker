"""Database layer for WSLCB licensing tracker."""
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from endorsements import get_endorsement_options, get_record_endorsements

# Patterns that indicate an organization rather than a person.
# Input is always uppercased by get_or_create_entity(), so no IGNORECASE needed.
_ORG_PATTERNS = re.compile(
    r'\b(LLC|L\.?L\.?C\.?|INC\.?|CORP\.?|CORPORATION|TRUST|LTD\.?|LIMITED'
    r'|PARTNERS|PARTNERSHIP|HOLDINGS|GROUP|ENTERPRISE|ENTERPRISES'
    r'|ASSOCIATION|FOUNDATION|COMPANY|CO\.|L\.?P\.?)\b'
)

# All persistent data (DB + HTML snapshots) lives under DATA_DIR.
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent / "data"))
DB_PATH = DATA_DIR / "wslcb.db"


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def _normalize_raw_address(raw: str) -> str:
    """Normalize whitespace variants (NBSP → space) in raw address strings.

    The WSLCB source page sometimes uses non-breaking spaces (\\xa0)
    instead of regular spaces.  We normalize before lookup so that
    cosmetically-identical strings map to the same location row.
    """
    if not raw:
        return raw
    return re.sub(r'\xa0+', ' ', raw)


def init_db():
    """Create tables and indexes.  Safe to call repeatedly."""
    with get_db() as conn:
        # Create supporting tables first (no dependency on license_records schema)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_address TEXT NOT NULL,
                city TEXT DEFAULT '',
                state TEXT DEFAULT 'WA',
                zip_code TEXT DEFAULT '',
                address_line_1 TEXT DEFAULT '',
                address_line_2 TEXT DEFAULT '',
                std_city TEXT DEFAULT '',
                std_state TEXT DEFAULT '',
                std_zip TEXT DEFAULT '',
                address_validated_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(raw_address)
            );
            CREATE INDEX IF NOT EXISTS idx_locations_city ON locations(city);
            CREATE INDEX IF NOT EXISTS idx_locations_zip ON locations(zip_code);
            CREATE INDEX IF NOT EXISTS idx_locations_std_city ON locations(std_city);
            CREATE INDEX IF NOT EXISTS idx_locations_std_zip ON locations(std_zip);

            CREATE TABLE IF NOT EXISTS license_endorsements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS endorsement_codes (
                code TEXT NOT NULL,
                endorsement_id INTEGER NOT NULL REFERENCES license_endorsements(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (code, endorsement_id)
            );

            CREATE TABLE IF NOT EXISTS scrape_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                records_new INTEGER DEFAULT 0,
                records_approved INTEGER DEFAULT 0,
                records_discontinued INTEGER DEFAULT 0,
                records_skipped INTEGER DEFAULT 0,
                error_message TEXT,
                snapshot_path TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)

        # --- Migrations for existing databases ---
        from migrate_locations import migrate_to_locations
        migrate_to_locations(conn)

        # Create license_records with the new schema (fresh install only;
        # existing DBs are handled by _migrate_to_locations above which
        # rebuilds the table with the new schema).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS license_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_type TEXT NOT NULL,
                record_date TEXT NOT NULL,
                business_name TEXT,
                location_id INTEGER REFERENCES locations(id),
                applicants TEXT,
                license_type TEXT,
                application_type TEXT,
                license_number TEXT,
                contact_phone TEXT,
                previous_business_name TEXT DEFAULT '',
                previous_applicants TEXT DEFAULT '',
                previous_location_id INTEGER REFERENCES locations(id),
                scraped_at TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(section_type, record_date, license_number, application_type)
            )
        """)
        # Junction table for endorsements (needs license_records to exist)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS record_endorsements (
                record_id INTEGER NOT NULL REFERENCES license_records(id) ON DELETE CASCADE,
                endorsement_id INTEGER NOT NULL REFERENCES license_endorsements(id) ON DELETE CASCADE,
                PRIMARY KEY (record_id, endorsement_id)
            );
            CREATE INDEX IF NOT EXISTS idx_re_endorsement
                ON record_endorsements(endorsement_id);
        """)
        # Entity tables (applicant normalization)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                entity_type TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS record_entities (
                record_id INTEGER NOT NULL REFERENCES license_records(id) ON DELETE CASCADE,
                entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
                role TEXT NOT NULL DEFAULT 'applicant',
                position INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (record_id, entity_id, role)
            );
            CREATE INDEX IF NOT EXISTS idx_re_entity ON record_entities(entity_id);
            CREATE INDEX IF NOT EXISTS idx_re_role ON record_entities(role);
        """)
        # Indexes on license_records (safe after migration)
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_records_section ON license_records(section_type)",
            "CREATE INDEX IF NOT EXISTS idx_records_date ON license_records(record_date)",
            "CREATE INDEX IF NOT EXISTS idx_records_business ON license_records(business_name)",
            "CREATE INDEX IF NOT EXISTS idx_records_license_num ON license_records(license_number)",
            "CREATE INDEX IF NOT EXISTS idx_records_app_type ON license_records(application_type)",
            "CREATE INDEX IF NOT EXISTS idx_records_location ON license_records(location_id)",
            "CREATE INDEX IF NOT EXISTS idx_records_prev_location ON license_records(previous_location_id)",
        ]:
            conn.execute(idx_sql)

        # FTS (re)build
        _ensure_fts(conn)

        conn.commit()


# ------------------------------------------------------------------
# FTS5 full-text search
# ------------------------------------------------------------------

_FTS_COLUMNS = [
    "business_name", "business_location", "applicants",
    "license_type", "application_type", "license_number",
    "previous_business_name", "previous_applicants",
    "previous_business_location",
]


def _ensure_fts(conn: sqlite3.Connection) -> None:
    """Create or rebuild the FTS5 virtual table and its sync triggers.

    The FTS table is a *content-less external content* table that
    references a VIEW (license_records_fts_content) which JOINs
    license_records → locations to expose business_location and
    previous_business_location as text columns for indexing.
    """
    # The view that the FTS triggers read from
    conn.execute("DROP VIEW IF EXISTS license_records_fts_content")
    conn.execute("""
        CREATE VIEW license_records_fts_content AS
        SELECT
            lr.id,
            lr.business_name,
            COALESCE(loc.raw_address, '') AS business_location,
            lr.applicants,
            lr.license_type,
            lr.application_type,
            lr.license_number,
            COALESCE(lr.previous_business_name, '') AS previous_business_name,
            COALESCE(lr.previous_applicants, '') AS previous_applicants,
            COALESCE(ploc.raw_address, '') AS previous_business_location
        FROM license_records lr
        LEFT JOIN locations loc ON loc.id = lr.location_id
        LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id
    """)

    # Check if FTS table exists, has the right columns, and right content source
    needs_rebuild = False
    try:
        cur = conn.execute("SELECT * FROM license_records_fts LIMIT 0")
        current_cols = [desc[0] for desc in cur.description]
        if current_cols != _FTS_COLUMNS:
            needs_rebuild = True
        # Also check if the content source is the view (not the raw table)
        fts_sql = conn.execute(
            "SELECT sql FROM sqlite_schema WHERE name = 'license_records_fts'"
        ).fetchone()
        if fts_sql and "content='license_records_fts_content'" not in fts_sql[0]:
            needs_rebuild = True
    except sqlite3.OperationalError:
        needs_rebuild = True

    if needs_rebuild:
        print("Building FTS index...")
        conn.executescript("""
            DROP TRIGGER IF EXISTS license_records_ai;
            DROP TRIGGER IF EXISTS license_records_ad;
            DROP TRIGGER IF EXISTS license_records_au;
            DROP TRIGGER IF EXISTS license_records_bu;
            DROP TRIGGER IF EXISTS license_records_bd;
            DROP TABLE IF EXISTS license_records_fts;
        """)

    cols = ", ".join(_FTS_COLUMNS)
    conn.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS license_records_fts USING fts5(
            {cols},
            content='license_records_fts_content',
            content_rowid='id'
        )
    """)

    # Triggers keep FTS in sync with license_records.
    # INSERT uses an AFTER trigger that reads new values from the view.
    # UPDATE and DELETE use BEFORE triggers so the view is read *before*
    # the row changes — the 'delete' command needs the old indexed values.
    # UPDATE also has an AFTER trigger to insert the new values.
    new_vals = ", ".join(
        f"(SELECT {c} FROM license_records_fts_content WHERE id = new.id)"
        for c in _FTS_COLUMNS
    )

    conn.executescript(f"""
        DROP TRIGGER IF EXISTS license_records_ai;
        CREATE TRIGGER license_records_ai AFTER INSERT ON license_records BEGIN
            INSERT INTO license_records_fts(rowid, {cols})
            VALUES (new.id, {new_vals});
        END;

        DROP TRIGGER IF EXISTS license_records_bu;
        DROP TRIGGER IF EXISTS license_records_au;
        CREATE TRIGGER license_records_bu BEFORE UPDATE ON license_records BEGIN
            INSERT INTO license_records_fts(license_records_fts, rowid, {cols})
            SELECT 'delete', old.id, {cols} FROM license_records_fts_content WHERE id = old.id;
        END;
        CREATE TRIGGER license_records_au AFTER UPDATE ON license_records BEGIN
            INSERT INTO license_records_fts(rowid, {cols})
            VALUES (new.id, {new_vals});
        END;

        DROP TRIGGER IF EXISTS license_records_bd;
        DROP TRIGGER IF EXISTS license_records_ad;
        CREATE TRIGGER license_records_bd BEFORE DELETE ON license_records BEGIN
            INSERT INTO license_records_fts(license_records_fts, rowid, {cols})
            SELECT 'delete', old.id, {cols} FROM license_records_fts_content WHERE id = old.id;
        END;
    """)

    if needs_rebuild:
        conn.execute(f"""
            INSERT INTO license_records_fts(rowid, {cols})
            SELECT id, {cols} FROM license_records_fts_content
        """)
        print("FTS index built.")


# ------------------------------------------------------------------
# Location helpers
# ------------------------------------------------------------------

def get_or_create_location(
    conn: sqlite3.Connection,
    raw_address: str,
    city: str = "",
    state: str = "WA",
    zip_code: str = "",
) -> int | None:
    """Return the location id for *raw_address*, creating if needed.

    Returns None if raw_address is empty/None.
    """
    if not raw_address or not raw_address.strip():
        return None
    normalized = _normalize_raw_address(raw_address)
    row = conn.execute(
        "SELECT id FROM locations WHERE raw_address = ?", (normalized,)
    ).fetchone()
    if row:
        return row[0]
    cursor = conn.execute(
        """INSERT INTO locations (raw_address, city, state, zip_code)
           VALUES (?, ?, ?, ?)""",
        (normalized, city, state, zip_code),
    )
    return cursor.lastrowid


# ------------------------------------------------------------------
# Entity helpers
# ------------------------------------------------------------------

def _classify_entity_type(name: str) -> str:
    """Classify an entity name as 'person' or 'organization'."""
    return "organization" if _ORG_PATTERNS.search(name) else "person"


def get_or_create_entity(conn: sqlite3.Connection, name: str) -> int:
    """Return the entity id for *name*, creating if needed.

    Names are uppercased for consistency — the WSLCB source is
    predominantly uppercase but occasionally uses mixed case.
    """
    normalized = name.strip().upper()
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
        conn.execute(
            """INSERT OR IGNORE INTO record_entities
               (record_id, entity_id, role, position)
               VALUES (?, ?, ?, ?)""",
            (record_id, entity_id, role, position),
        )
        linked += 1
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


def get_entity_records(
    conn: sqlite3.Connection, entity_id: int
) -> list[dict]:
    """Fetch all records associated with an entity, with location data."""
    # Use DISTINCT to avoid duplicate rows when the same entity appears
    # in multiple roles (applicant + previous_applicant) on one record.
    rows = conn.execute(
        f"""SELECT DISTINCT {_RECORD_COLUMNS} {_RECORD_JOINS}
            JOIN record_entities re ON re.record_id = lr.id
            WHERE re.entity_id = ?
            ORDER BY lr.record_date DESC, lr.id DESC""",
        (entity_id,),
    ).fetchall()
    record_ids = [r["id"] for r in rows]
    endorsement_map = get_record_endorsements(conn, record_ids)
    entity_map = get_record_entities(conn, record_ids)
    results = []
    for r in rows:
        d = enrich_record(dict(r))
        d["endorsements"] = endorsement_map.get(d["id"], [])
        d["entities"] = entity_map.get(d["id"], {"applicant": [], "previous_applicant": []})
        results.append(d)
    return results


# ------------------------------------------------------------------
# Record CRUD
# ------------------------------------------------------------------

def enrich_record(record: dict) -> dict:
    """Add display-ready fields with standardized-first fallback.

    Works with joined query results that include location columns
    aliased as business_location, city, std_city, etc.
    """
    record["display_city"] = record.get("std_city") or record.get("city") or ""
    record["display_zip"] = record.get("std_zip") or record.get("zip_code") or ""
    record["display_previous_city"] = record.get("prev_std_city") or record.get("previous_city") or ""
    record["display_previous_zip"] = record.get("prev_std_zip") or record.get("previous_zip_code") or ""
    return record


def insert_record(conn: sqlite3.Connection, record: dict) -> int | None:
    """Insert a record, returning the new row id or None if duplicate.

    Automatically resolves (or creates) location rows for the primary
    and previous business addresses.  Checks for duplicates *before*
    creating locations to avoid orphaned location rows.
    """
    # Check for duplicate before creating locations — the UNIQUE
    # constraint is (section_type, record_date, license_number,
    # application_type).  This avoids creating orphan location rows
    # for the ~5 000 duplicate records skipped on each daily scrape.
    existing = conn.execute(
        """SELECT 1 FROM license_records
           WHERE section_type = :section_type
             AND record_date = :record_date
             AND license_number = :license_number
             AND application_type = :application_type
           LIMIT 1""",
        record,
    ).fetchone()
    if existing:
        return None

    location_id = get_or_create_location(
        conn,
        record.get("business_location", ""),
        city=record.get("city", ""),
        state=record.get("state", "WA"),
        zip_code=record.get("zip_code", ""),
    )
    previous_location_id = get_or_create_location(
        conn,
        record.get("previous_business_location", ""),
        city=record.get("previous_city", ""),
        state=record.get("previous_state", ""),
        zip_code=record.get("previous_zip_code", ""),
    )
    try:
        cursor = conn.execute(
            """INSERT INTO license_records
               (section_type, record_date, business_name, location_id,
                applicants, license_type, application_type, license_number,
                contact_phone, previous_business_name, previous_applicants,
                previous_location_id, scraped_at)
               VALUES (:section_type, :record_date, :business_name, :location_id,
                       :applicants, :license_type, :application_type, :license_number,
                       :contact_phone, :previous_business_name, :previous_applicants,
                       :previous_location_id, :scraped_at)""",
            {
                **record,
                "location_id": location_id,
                "previous_location_id": previous_location_id,
            },
        )
        record_id = cursor.lastrowid
        # Populate entity links
        _parse_and_link_entities(
            conn, record_id, record.get("applicants", ""), "applicant"
        )
        if record.get("previous_applicants"):
            _parse_and_link_entities(
                conn, record_id, record["previous_applicants"], "previous_applicant"
            )
        return record_id
    except sqlite3.IntegrityError:
        return None


# ------------------------------------------------------------------
# Queries (search, filters, stats)
# ------------------------------------------------------------------

# Column list and JOINs shared by all record queries.  Defined once
# so that _RECORD_SELECT and get_entity_records() stay in sync.
_RECORD_COLUMNS = """
    lr.id, lr.section_type, lr.record_date, lr.business_name,
    lr.applicants, lr.license_type, lr.application_type,
    lr.license_number, lr.contact_phone,
    lr.previous_business_name, lr.previous_applicants,
    lr.location_id, lr.previous_location_id,
    lr.scraped_at, lr.created_at,
    COALESCE(loc.raw_address, '') AS business_location,
    COALESCE(loc.city, '') AS city,
    COALESCE(loc.state, 'WA') AS state,
    COALESCE(loc.zip_code, '') AS zip_code,
    COALESCE(loc.address_line_1, '') AS address_line_1,
    COALESCE(loc.address_line_2, '') AS address_line_2,
    COALESCE(loc.std_city, '') AS std_city,
    COALESCE(loc.std_state, '') AS std_state,
    COALESCE(loc.std_zip, '') AS std_zip,
    loc.address_validated_at,
    COALESCE(ploc.raw_address, '') AS previous_business_location,
    COALESCE(ploc.city, '') AS previous_city,
    COALESCE(ploc.state, '') AS previous_state,
    COALESCE(ploc.zip_code, '') AS previous_zip_code,
    COALESCE(ploc.address_line_1, '') AS prev_address_line_1,
    COALESCE(ploc.address_line_2, '') AS prev_address_line_2,
    COALESCE(ploc.std_city, '') AS prev_std_city,
    COALESCE(ploc.std_state, '') AS prev_std_state,
    COALESCE(ploc.std_zip, '') AS prev_std_zip,
    ploc.address_validated_at AS prev_address_validated_at"""

_RECORD_JOINS = """
    FROM license_records lr
    LEFT JOIN locations loc ON loc.id = lr.location_id
    LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id"""

# Base SELECT that joins locations for display
_RECORD_SELECT = f"SELECT {_RECORD_COLUMNS} {_RECORD_JOINS}"


def search_records(
    conn: sqlite3.Connection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    page: int = 1,
    per_page: int = 50,
) -> tuple[list[dict], int]:
    """Search records with filters.  Returns (records, total_count)."""
    conditions = []
    params: list = []

    if query:
        safe_query = query.replace('"', '').replace("'", "")
        terms = safe_query.split()
        fts_query = " AND ".join(f'"{ t }"*' for t in terms if t)
        if fts_query:
            conditions.append(
                "lr.id IN (SELECT rowid FROM license_records_fts WHERE license_records_fts MATCH ?)"
            )
            params.append(fts_query)

    if section_type:
        conditions.append("lr.section_type = ?")
        params.append(section_type)

    if application_type:
        conditions.append("lr.application_type = ?")
        params.append(application_type)

    if endorsement:
        conditions.append("""
            lr.id IN (
                SELECT re.record_id FROM record_endorsements re
                JOIN license_endorsements le ON le.id = re.endorsement_id
                WHERE le.name = ?
            )""")
        params.append(endorsement)

    if city:
        conditions.append(
            "(COALESCE(NULLIF(loc.std_city, ''), loc.city) = ?"
            " OR COALESCE(NULLIF(ploc.std_city, ''), ploc.city) = ?)"
        )
        params.extend([city, city])

    if date_from:
        conditions.append("lr.record_date >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("lr.record_date <= ?")
        params.append(date_to)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Only JOIN locations in the count query when needed (city filter).
    # Without JOINs, SQLite can use a covering index scan.
    if city:
        count_sql = f"""
            SELECT COUNT(*) FROM license_records lr
            LEFT JOIN locations loc ON loc.id = lr.location_id
            LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id
            {where}
        """
    else:
        count_sql = f"SELECT COUNT(*) FROM license_records lr {where}"
    total = conn.execute(count_sql, params).fetchone()[0]

    offset = (page - 1) * per_page
    rows = conn.execute(
        f"""{_RECORD_SELECT}
            {where}
            ORDER BY lr.record_date DESC, lr.id DESC
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    record_ids = [r["id"] for r in rows]
    endorsement_map = get_record_endorsements(conn, record_ids)
    entity_map = get_record_entities(conn, record_ids)

    results = []
    for r in rows:
        d = enrich_record(dict(r))
        d["endorsements"] = endorsement_map.get(d["id"], [])
        d["entities"] = entity_map.get(d["id"], {"applicant": [], "previous_applicant": []})
        results.append(d)

    return results, total


def get_filter_options(conn: sqlite3.Connection) -> dict:
    """Get distinct values for filter dropdowns."""
    options: dict = {}
    for col in ["section_type", "application_type"]:
        rows = conn.execute(
            f"SELECT DISTINCT {col} FROM license_records "
            f"WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col}"
        ).fetchall()
        options[col] = [r[0] for r in rows]

    # City filter — only locations actually referenced by records
    rows = conn.execute(
        "SELECT DISTINCT display_city FROM ("
        "  SELECT COALESCE(NULLIF(l.std_city, ''), l.city) AS display_city"
        "  FROM locations l"
        "  WHERE l.id IN ("
        "    SELECT location_id FROM license_records WHERE location_id IS NOT NULL"
        "    UNION"
        "    SELECT previous_location_id FROM license_records WHERE previous_location_id IS NOT NULL"
        "  )"
        ") WHERE display_city IS NOT NULL AND display_city != '' ORDER BY display_city"
    ).fetchall()
    options["city"] = [r[0] for r in rows]

    options["endorsement"] = get_endorsement_options(conn)
    return options


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get summary statistics."""
    stats: dict = {}
    stats["total_records"] = conn.execute("SELECT COUNT(*) FROM license_records").fetchone()[0]
    for st in ["new_application", "approved", "discontinued"]:
        stats[f"{st}_count"] = conn.execute(
            "SELECT COUNT(*) FROM license_records WHERE section_type = ?", (st,)
        ).fetchone()[0]
    stats["date_range"] = conn.execute(
        "SELECT MIN(record_date), MAX(record_date) FROM license_records"
    ).fetchone()
    stats["last_scrape"] = conn.execute(
        "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    stats["unique_businesses"] = conn.execute(
        "SELECT COUNT(DISTINCT business_name) FROM license_records"
    ).fetchone()[0]
    stats["unique_licenses"] = conn.execute(
        "SELECT COUNT(DISTINCT license_number) FROM license_records"
    ).fetchone()[0]
    stats["unique_entities"] = conn.execute(
        "SELECT COUNT(*) FROM entities"
    ).fetchone()[0]
    return stats


def get_record_by_id(conn: sqlite3.Connection, record_id: int) -> dict | None:
    """Fetch a single record with location data joined."""
    row = conn.execute(
        f"{_RECORD_SELECT} WHERE lr.id = ?", (record_id,)
    ).fetchone()
    if not row:
        return None
    return enrich_record(dict(row))


def get_related_records(conn: sqlite3.Connection, license_number: str, exclude_id: int) -> list[dict]:
    """Fetch other records for the same license number."""
    rows = conn.execute(
        f"{_RECORD_SELECT} WHERE lr.license_number = ? AND lr.id != ? ORDER BY lr.record_date DESC",
        (license_number, exclude_id),
    ).fetchall()
    return [enrich_record(dict(r)) for r in rows]


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
