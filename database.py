"""Database layer for WSLCB licensing tracker."""
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from endorsements import get_endorsement_options, get_record_endorsements

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


def init_db():
    """Create tables and indexes.  Safe to call repeatedly."""
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS license_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_type TEXT NOT NULL,
                record_date TEXT NOT NULL,
                business_name TEXT,
                business_location TEXT,
                applicants TEXT,
                license_type TEXT,
                application_type TEXT,
                license_number TEXT,
                contact_phone TEXT,
                city TEXT,
                state TEXT DEFAULT 'WA',
                zip_code TEXT,
                address_line_1 TEXT DEFAULT '',
                address_line_2 TEXT DEFAULT '',
                std_city TEXT DEFAULT '',
                std_state TEXT DEFAULT '',
                std_zip TEXT DEFAULT '',
                previous_business_name TEXT DEFAULT '',
                previous_applicants TEXT DEFAULT '',
                address_validated_at TEXT,
                scraped_at TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(section_type, record_date, license_number, application_type)
            );

            CREATE INDEX IF NOT EXISTS idx_records_section ON license_records(section_type);
            CREATE INDEX IF NOT EXISTS idx_records_date ON license_records(record_date);
            CREATE INDEX IF NOT EXISTS idx_records_business ON license_records(business_name);
            CREATE INDEX IF NOT EXISTS idx_records_license_num ON license_records(license_number);
            CREATE INDEX IF NOT EXISTS idx_records_app_type ON license_records(application_type);
            CREATE INDEX IF NOT EXISTS idx_records_city ON license_records(city);
            CREATE INDEX IF NOT EXISTS idx_records_zip ON license_records(zip_code);

            CREATE TABLE IF NOT EXISTS license_endorsements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            -- NOTE: ON DELETE CASCADE clauses below only take effect on fresh
            -- databases.  CREATE TABLE IF NOT EXISTS is a no-op on existing
            -- tables, so the running wslcb.db retains its original FK
            -- definitions.  Code that deletes from license_endorsements
            -- (e.g. _merge_placeholders) must manually clean up referencing
            -- rows to stay safe on both old and new schemas.
            CREATE TABLE IF NOT EXISTS endorsement_codes (
                code TEXT NOT NULL,
                endorsement_id INTEGER NOT NULL REFERENCES license_endorsements(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (code, endorsement_id)
            );
            CREATE TABLE IF NOT EXISTS record_endorsements (
                record_id INTEGER NOT NULL REFERENCES license_records(id) ON DELETE CASCADE,
                endorsement_id INTEGER NOT NULL REFERENCES license_endorsements(id) ON DELETE CASCADE,
                PRIMARY KEY (record_id, endorsement_id)
            );
            CREATE INDEX IF NOT EXISTS idx_re_endorsement
                ON record_endorsements(endorsement_id);

            CREATE VIRTUAL TABLE IF NOT EXISTS license_records_fts USING fts5(
                business_name,
                business_location,
                applicants,
                license_type,
                application_type,
                license_number,
                previous_business_name,
                content='license_records',
                content_rowid='id'
            );

            CREATE TRIGGER IF NOT EXISTS license_records_ai AFTER INSERT ON license_records BEGIN
                INSERT INTO license_records_fts(rowid, business_name, business_location, applicants, license_type, application_type, license_number, previous_business_name)
                VALUES (new.id, new.business_name, new.business_location, new.applicants, new.license_type, new.application_type, new.license_number, new.previous_business_name);
            END;

            CREATE TRIGGER IF NOT EXISTS license_records_ad AFTER DELETE ON license_records BEGIN
                INSERT INTO license_records_fts(license_records_fts, rowid, business_name, business_location, applicants, license_type, application_type, license_number, previous_business_name)
                VALUES ('delete', old.id, old.business_name, old.business_location, old.applicants, old.license_type, old.application_type, old.license_number, old.previous_business_name);
            END;

            CREATE TRIGGER IF NOT EXISTS license_records_au AFTER UPDATE ON license_records BEGIN
                INSERT INTO license_records_fts(license_records_fts, rowid, business_name, business_location, applicants, license_type, application_type, license_number, previous_business_name)
                VALUES ('delete', old.id, old.business_name, old.business_location, old.applicants, old.license_type, old.application_type, old.license_number, old.previous_business_name);
                INSERT INTO license_records_fts(rowid, business_name, business_location, applicants, license_type, application_type, license_number, previous_business_name)
                VALUES (new.id, new.business_name, new.business_location, new.applicants, new.license_type, new.application_type, new.license_number, new.previous_business_name);
            END;

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
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
        # Migration: drop old mapping table and obsolete indexes
        conn.execute("DROP TABLE IF EXISTS license_type_map")
        conn.execute("DROP INDEX IF EXISTS idx_records_license_type")

        # Migration: add snapshot_path column to scrape_log
        try:
            conn.execute("ALTER TABLE scrape_log ADD COLUMN snapshot_path TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Migration: add standardized address columns
        for col, typedef in [
            ("address_line_1", "TEXT DEFAULT ''"),
            ("address_line_2", "TEXT DEFAULT ''"),
            ("std_city", "TEXT DEFAULT ''"),
            ("std_state", "TEXT DEFAULT ''"),
            ("std_zip", "TEXT DEFAULT ''"),
            ("address_validated_at", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE license_records ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Migration: add assumption-related columns
        for col, typedef in [
            ("previous_business_name", "TEXT DEFAULT ''"),
            ("previous_applicants", "TEXT DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE license_records ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Index on std_city for filter dropdown performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_records_std_city ON license_records(std_city)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_records_std_zip ON license_records(std_zip)")

        # Migration: rebuild FTS table to include previous_business_name
        # Check if FTS already has the column by inspecting its structure
        _rebuild_fts_if_needed(conn)

        conn.commit()


# Columns that should be in the FTS table (in order).  Used by the
# migration to detect when the FTS schema is stale and rebuild it.
_FTS_COLUMNS = [
    "business_name", "business_location", "applicants",
    "license_type", "application_type", "license_number",
    "previous_business_name",
]


def _rebuild_fts_if_needed(conn: sqlite3.Connection) -> None:
    """Rebuild the FTS5 virtual table if its column set is outdated.

    FTS5 tables can't be ALTERed, so we drop and recreate when the
    expected column list doesn't match the live table.
    """
    # Introspect current FTS columns via a zero-result query
    try:
        cur = conn.execute("SELECT * FROM license_records_fts LIMIT 0")
        current_cols = [desc[0] for desc in cur.description]
    except sqlite3.OperationalError:
        # Table doesn't exist yet — will be created by the main schema
        return

    if current_cols == _FTS_COLUMNS:
        return  # Already up-to-date

    print(f"Rebuilding FTS index (columns {current_cols} → {_FTS_COLUMNS})")

    # Drop old triggers, table, then recreate
    conn.executescript("""
        DROP TRIGGER IF EXISTS license_records_ai;
        DROP TRIGGER IF EXISTS license_records_ad;
        DROP TRIGGER IF EXISTS license_records_au;
        DROP TABLE IF EXISTS license_records_fts;
    """)

    cols = ", ".join(_FTS_COLUMNS)
    new_cols = ", ".join(f"new.{c}" for c in _FTS_COLUMNS)
    old_cols = ", ".join(f"old.{c}" for c in _FTS_COLUMNS)

    conn.executescript(f"""
        CREATE VIRTUAL TABLE license_records_fts USING fts5(
            {cols},
            content='license_records',
            content_rowid='id'
        );

        CREATE TRIGGER license_records_ai AFTER INSERT ON license_records BEGIN
            INSERT INTO license_records_fts(rowid, {cols})
            VALUES (new.id, {new_cols});
        END;

        CREATE TRIGGER license_records_ad AFTER DELETE ON license_records BEGIN
            INSERT INTO license_records_fts(license_records_fts, rowid, {cols})
            VALUES ('delete', old.id, {old_cols});
        END;

        CREATE TRIGGER license_records_au AFTER UPDATE ON license_records BEGIN
            INSERT INTO license_records_fts(license_records_fts, rowid, {cols})
            VALUES ('delete', old.id, {old_cols});
            INSERT INTO license_records_fts(rowid, {cols})
            VALUES (new.id, {new_cols});
        END;
    """)

    # Repopulate from existing data
    conn.execute(f"""
        INSERT INTO license_records_fts(rowid, {cols})
        SELECT id, {cols} FROM license_records
    """)
    print("FTS index rebuilt.")


def enrich_record(record: dict) -> dict:
    """Add display_city and display_zip with standardized-first fallback.

    Mirrors the SQL pattern COALESCE(NULLIF(std_city, ''), city) so
    templates can use a single field without fallback logic.
    """
    record["display_city"] = record.get("std_city") or record.get("city") or ""
    record["display_zip"] = record.get("std_zip") or record.get("zip_code") or ""
    return record


def insert_record(conn: sqlite3.Connection, record: dict) -> int | None:
    """Insert a record, returning the new row id or None if duplicate."""
    try:
        cursor = conn.execute(
            """INSERT INTO license_records
               (section_type, record_date, business_name, business_location,
                applicants, license_type, application_type, license_number,
                contact_phone, city, state, zip_code,
                previous_business_name, previous_applicants, scraped_at)
               VALUES (:section_type, :record_date, :business_name, :business_location,
                       :applicants, :license_type, :application_type, :license_number,
                       :contact_phone, :city, :state, :zip_code,
                       :previous_business_name, :previous_applicants, :scraped_at)""",
            record,
        )
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None


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
        fts_query = " AND ".join(f'"{t}"*' for t in terms if t)
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
        conditions.append("COALESCE(NULLIF(lr.std_city, ''), lr.city) = ?")
        params.append(city)

    if date_from:
        conditions.append("lr.record_date >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("lr.record_date <= ?")
        params.append(date_to)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    total = conn.execute(
        f"SELECT COUNT(*) FROM license_records lr {where}", params
    ).fetchone()[0]

    offset = (page - 1) * per_page
    rows = conn.execute(
        f"""SELECT lr.* FROM license_records lr
            {where}
            ORDER BY lr.record_date DESC, lr.id DESC
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    # Batch-attach endorsement names
    record_ids = [r["id"] for r in rows]
    endorsement_map = get_record_endorsements(conn, record_ids)

    results = []
    for r in rows:
        d = enrich_record(dict(r))
        d["endorsements"] = endorsement_map.get(d["id"], [])
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

    # City filter uses standardized city with fallback to regex-parsed city
    rows = conn.execute(
        "SELECT DISTINCT COALESCE(NULLIF(std_city, ''), city) AS display_city "
        "FROM license_records "
        "WHERE COALESCE(NULLIF(std_city, ''), city) IS NOT NULL "
        "AND COALESCE(NULLIF(std_city, ''), city) != '' "
        "ORDER BY display_city"
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
    return stats


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
