"""Database layer for WSLCB licensing tracker."""
import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime

DB_PATH = os.environ.get("WSLCB_DB_PATH", os.path.join(os.path.dirname(__file__), "wslcb.db"))


def get_connection() -> sqlite3.Connection:
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


def init_db():
    """Create tables and indexes."""
    with get_db() as conn:
        conn.executescript("""
            -- Mapping from numeric license-type codes to human-readable names.
            -- The WSLCB source page shows text names for new-application records
            -- but opaque integer codes for approved/discontinued records.
            -- This table is built automatically by cross-referencing license
            -- numbers that appear in both sections.
            CREATE TABLE IF NOT EXISTS license_type_map (
                code TEXT PRIMARY KEY,              -- numeric code (e.g. '450')
                label TEXT NOT NULL,                -- human-readable name (e.g. 'GROCERY STORE - BEER/WINE')
                confidence TEXT NOT NULL DEFAULT 'auto',  -- 'auto' or 'manual'
                match_count INTEGER DEFAULT 0,      -- how many cross-ref matches support this
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS license_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_type TEXT NOT NULL,          -- 'new_application', 'approved', 'discontinued'
                record_date TEXT NOT NULL,            -- the notification/approved/discontinued date
                business_name TEXT,
                business_location TEXT,
                applicants TEXT,                      -- semicolon-separated, only for new applications
                license_type TEXT,
                application_type TEXT,
                license_number TEXT,
                contact_phone TEXT,
                city TEXT,                            -- extracted from business_location
                state TEXT DEFAULT 'WA',
                zip_code TEXT,                        -- extracted from business_location
                scraped_at TEXT NOT NULL,             -- when we scraped this record
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(section_type, record_date, license_number, application_type)
            );

            CREATE INDEX IF NOT EXISTS idx_records_section ON license_records(section_type);
            CREATE INDEX IF NOT EXISTS idx_records_date ON license_records(record_date);
            CREATE INDEX IF NOT EXISTS idx_records_business ON license_records(business_name);
            CREATE INDEX IF NOT EXISTS idx_records_license_num ON license_records(license_number);
            CREATE INDEX IF NOT EXISTS idx_records_license_type ON license_records(license_type);
            CREATE INDEX IF NOT EXISTS idx_records_app_type ON license_records(application_type);
            CREATE INDEX IF NOT EXISTS idx_records_city ON license_records(city);
            CREATE INDEX IF NOT EXISTS idx_records_zip ON license_records(zip_code);

            CREATE VIRTUAL TABLE IF NOT EXISTS license_records_fts USING fts5(
                business_name,
                business_location,
                applicants,
                license_type,
                application_type,
                license_number,
                content='license_records',
                content_rowid='id'
            );

            -- Triggers to keep FTS in sync
            CREATE TRIGGER IF NOT EXISTS license_records_ai AFTER INSERT ON license_records BEGIN
                INSERT INTO license_records_fts(rowid, business_name, business_location, applicants, license_type, application_type, license_number)
                VALUES (new.id, new.business_name, new.business_location, new.applicants, new.license_type, new.application_type, new.license_number);
            END;

            CREATE TRIGGER IF NOT EXISTS license_records_ad AFTER DELETE ON license_records BEGIN
                INSERT INTO license_records_fts(license_records_fts, rowid, business_name, business_location, applicants, license_type, application_type, license_number)
                VALUES ('delete', old.id, old.business_name, old.business_location, old.applicants, old.license_type, old.application_type, old.license_number);
            END;

            CREATE TRIGGER IF NOT EXISTS license_records_au AFTER UPDATE ON license_records BEGIN
                INSERT INTO license_records_fts(license_records_fts, rowid, business_name, business_location, applicants, license_type, application_type, license_number)
                VALUES ('delete', old.id, old.business_name, old.business_location, old.applicants, old.license_type, old.application_type, old.license_number);
                INSERT INTO license_records_fts(rowid, business_name, business_location, applicants, license_type, application_type, license_number)
                VALUES (new.id, new.business_name, new.business_location, new.applicants, new.license_type, new.application_type, new.license_number);
            END;

            CREATE TABLE IF NOT EXISTS scrape_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',  -- 'running', 'success', 'error'
                records_new INTEGER DEFAULT 0,
                records_approved INTEGER DEFAULT 0,
                records_discontinued INTEGER DEFAULT 0,
                records_skipped INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """
        )
        conn.commit()


def insert_record(conn: sqlite3.Connection, record: dict) -> bool:
    """Insert a record, returning True if it was new (not a duplicate)."""
    try:
        conn.execute(
            """INSERT INTO license_records
               (section_type, record_date, business_name, business_location,
                applicants, license_type, application_type, license_number,
                contact_phone, city, state, zip_code, scraped_at)
               VALUES (:section_type, :record_date, :business_name, :business_location,
                       :applicants, :license_type, :application_type, :license_number,
                       :contact_phone, :city, :state, :zip_code, :scraped_at)""",
            record,
        )
        return True
    except sqlite3.IntegrityError:
        return False


def learn_license_type_mappings(conn: sqlite3.Connection) -> dict:
    """Discover code→label mappings by cross-referencing license numbers.

    For each numeric code in approved/discontinued records, find new_application
    records with the same license_number.  When the new_application has a single
    license type (no semicolons), that's a direct mapping.  Otherwise, find the
    license-type string that is present in *every* matched new_application record
    (the intersection).

    Returns dict of {code: label} for newly learned or updated mappings.
    """
    # Step 1: single-type matches (high confidence)
    rows = conn.execute("""
        SELECT
            REPLACE(a.license_type, ',', '') AS code,
            n.license_type AS text_type,
            COUNT(*) AS cnt
        FROM license_records a
        JOIN license_records n
            ON a.license_number = n.license_number
            AND n.section_type = 'new_application'
        WHERE a.section_type IN ('approved', 'discontinued')
          AND n.license_type NOT LIKE '%;%'
          AND a.license_type GLOB '[0-9]*'
        GROUP BY REPLACE(a.license_type, ',', ''), n.license_type
        ORDER BY cnt DESC
    """).fetchall()

    # Best single-type match per code
    best = {}  # code -> (label, count)
    for r in rows:
        code = r["code"]
        if code not in best or r["cnt"] > best[code][1]:
            best[code] = (r["text_type"], r["cnt"])

    # Step 2: for codes with NO single-type match, find the always-present type
    all_codes = conn.execute("""
        SELECT DISTINCT REPLACE(license_type, ',', '') AS code
        FROM license_records
        WHERE section_type IN ('approved', 'discontinued')
          AND license_type GLOB '[0-9]*'
    """).fetchall()
    missing_codes = [r["code"] for r in all_codes if r["code"] not in best]

    for code in missing_codes:
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

        # Split each matched text_type into individual types, count per-type
        from collections import Counter
        type_freq = Counter()
        for r in matches:
            for t in r["text_type"].split(";"):
                type_freq[t.strip()] += r["cnt"]

        # Types present in every single match
        always_present = [t for t, c in type_freq.items() if c == total]
        if len(always_present) == 1:
            best[code] = (always_present[0], total)

    # Step 3: upsert into license_type_map (don't overwrite manual entries)
    learned = {}
    for code, (label, cnt) in best.items():
        existing = conn.execute(
            "SELECT confidence, match_count FROM license_type_map WHERE code = ?", (code,)
        ).fetchone()
        if existing and existing["confidence"] == "manual":
            continue  # never overwrite manual
        if existing and existing["match_count"] >= cnt:
            continue  # no new evidence
        conn.execute(
            """INSERT INTO license_type_map (code, label, confidence, match_count, updated_at)
               VALUES (?, ?, 'auto', ?, datetime('now'))
               ON CONFLICT(code) DO UPDATE SET
                   label = excluded.label,
                   match_count = excluded.match_count,
                   updated_at = datetime('now')
                   WHERE confidence != 'manual'""",
            (code, label, cnt),
        )
        learned[code] = label

    if learned:
        conn.commit()

    return learned


def resolve_license_type(conn: sqlite3.Connection, raw_value: str) -> str:
    """Translate a numeric license-type code to its text label.

    Returns the original value if it's already text or has no mapping.
    """
    if not raw_value:
        return raw_value
    cleaned = raw_value.rstrip(",").strip()
    if not cleaned.isdigit():
        return raw_value  # already a text value
    row = conn.execute(
        "SELECT label FROM license_type_map WHERE code = ?", (cleaned,)
    ).fetchone()
    return row["label"] if row else raw_value


def get_license_type_map(conn: sqlite3.Connection) -> dict:
    """Return the full code→label mapping as a dict."""
    rows = conn.execute("SELECT code, label FROM license_type_map ORDER BY CAST(code AS INTEGER)").fetchall()
    return {r["code"]: r["label"] for r in rows}


def search_records(
    conn: sqlite3.Connection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    license_type: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    page: int = 1,
    per_page: int = 50,
) -> tuple[list[dict], int]:
    """Search records with filters. Returns (records, total_count)."""
    conditions = []
    params = []

    if query:
        # Use FTS for text search
        conditions.append(
            "lr.id IN (SELECT rowid FROM license_records_fts WHERE license_records_fts MATCH ?)"
        )
        # Escape special FTS characters and add prefix matching
        safe_query = query.replace('"', '').replace("'", "")
        terms = safe_query.split()
        fts_query = " AND ".join(f'"{t}"*' for t in terms if t)
        params.append(fts_query)

    if section_type:
        conditions.append("lr.section_type = ?")
        params.append(section_type)

    if application_type:
        conditions.append("lr.application_type = ?")
        params.append(application_type)

    if license_type:
        conditions.append("lr.license_type = ?")
        params.append(license_type)

    if city:
        conditions.append("lr.city = ?")
        params.append(city)

    if date_from:
        conditions.append("lr.record_date >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("lr.record_date <= ?")
        params.append(date_to)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Count
    count_sql = f"SELECT COUNT(*) FROM license_records lr {where}"
    total = conn.execute(count_sql, params).fetchone()[0]

    # Fetch page
    offset = (page - 1) * per_page
    data_sql = f"""SELECT lr.* FROM license_records lr
                   {where}
                   ORDER BY lr.record_date DESC, lr.id DESC
                   LIMIT ? OFFSET ?"""
    rows = conn.execute(data_sql, params + [per_page, offset]).fetchall()

    # Resolve numeric license-type codes to text labels
    type_map = get_license_type_map(conn)
    results = []
    for r in rows:
        d = dict(r)
        raw = (d.get("license_type") or "").rstrip(",").strip()
        if raw.isdigit() and raw in type_map:
            d["license_type_raw"] = d["license_type"]
            d["license_type"] = type_map[raw]
        results.append(d)

    return results, total


def get_filter_options(conn: sqlite3.Connection) -> dict:
    """Get distinct values for filter dropdowns."""
    options = {}
    for col in ["section_type", "application_type", "city"]:
        rows = conn.execute(
            f"SELECT DISTINCT {col} FROM license_records WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col}"
        ).fetchall()
        options[col] = [r[0] for r in rows]

    # For license_type, resolve numeric codes and deduplicate
    rows = conn.execute(
        "SELECT DISTINCT license_type FROM license_records WHERE license_type IS NOT NULL AND license_type != '' ORDER BY license_type"
    ).fetchall()
    type_map = get_license_type_map(conn)
    seen = set()
    lt_options = []
    for r in rows:
        raw = r[0]
        cleaned = raw.rstrip(",").strip()
        if cleaned.isdigit() and cleaned in type_map:
            resolved = type_map[cleaned]
        else:
            resolved = raw
        if resolved not in seen:
            seen.add(resolved)
            # Store as (display_value, raw_value) for the dropdown
            lt_options.append(raw)
    options["license_type"] = lt_options

    # Also provide a resolved display map for templates
    options["license_type_display"] = {}
    for raw in options["license_type"]:
        cleaned = raw.rstrip(",").strip()
        if cleaned.isdigit() and cleaned in type_map:
            options["license_type_display"][raw] = type_map[cleaned]

    return options


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get summary statistics."""
    stats = {}
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
    return stats


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
