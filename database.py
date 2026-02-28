"""Core database layer for WSLCB licensing tracker.

Schema creation, connection management, FTS5 setup, and location helpers.
Query functions live in ``queries.py``; entity helpers in ``entities.py``.
"""
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
import json

logger = logging.getLogger(__name__)

# Source type constants (fixed IDs — must match seed data in init_db)
SOURCE_TYPE_LIVE_SCRAPE = 1
SOURCE_TYPE_CO_ARCHIVE = 2
SOURCE_TYPE_INTERNET_ARCHIVE = 3
SOURCE_TYPE_CO_DIFF_ARCHIVE = 4
SOURCE_TYPE_MANUAL = 5

WSLCB_SOURCE_URL = "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp"

# All persistent data (DB + HTML snapshots) lives under DATA_DIR.
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent / "data"))
DB_PATH = DATA_DIR / "wslcb.db"


def get_connection(path: str | Path | None = None) -> sqlite3.Connection:
    """Open a connection to *path* (defaults to ``DB_PATH``).

    Passing ``":memory:"`` gives an ephemeral in-memory database —
    useful for tests.  When using the default on-disk path, the
    parent directory is created automatically.
    """
    effective = str(path) if path is not None else str(DB_PATH)
    if effective != ":memory:":
        Path(effective).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(effective)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db(path: str | Path | None = None):
    """Context manager wrapping :func:`get_connection`."""
    conn = get_connection(path)
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


def init_db(conn: sqlite3.Connection | None = None):
    """Create tables and indexes.  Safe to call repeatedly.

    If *conn* is provided it is used directly (and **not** closed);
    otherwise a new connection via :func:`get_db` is opened and
    closed automatically.  Passing an in-memory connection is the
    recommended pattern for tests.
    """
    if conn is not None:
        _init_db_on(conn)
        return conn
    with get_db() as _conn:
        _init_db_on(_conn)
    return None


def _init_db_on(conn: sqlite3.Connection) -> None:
    """Internal: run all DDL / migrations on *conn*."""
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

        CREATE TABLE IF NOT EXISTS source_types (
            id INTEGER PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL,
            description TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type_id INTEGER NOT NULL
                REFERENCES source_types(id),
            snapshot_path TEXT,
            url TEXT,
            captured_at TEXT,
            ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
            scrape_log_id INTEGER
                REFERENCES scrape_log(id),
            metadata TEXT NOT NULL DEFAULT '{}',
            UNIQUE(source_type_id, snapshot_path)
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
    # Application → outcome linking table
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS record_links (
            id INTEGER PRIMARY KEY,
            new_app_id INTEGER NOT NULL REFERENCES license_records(id) ON DELETE CASCADE,
            outcome_id INTEGER NOT NULL REFERENCES license_records(id) ON DELETE CASCADE,
            confidence TEXT NOT NULL CHECK (confidence IN ('high', 'medium', 'low')),
            days_gap INTEGER,
            linked_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(new_app_id, outcome_id)
        );
        CREATE INDEX IF NOT EXISTS idx_record_links_new
            ON record_links(new_app_id);
        CREATE INDEX IF NOT EXISTS idx_record_links_outcome
            ON record_links(outcome_id);
    """)
    # Source provenance junction table
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS record_sources (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            source_id INTEGER NOT NULL
                REFERENCES sources(id) ON DELETE CASCADE,
            role TEXT NOT NULL DEFAULT 'first_seen'
                CHECK(role IN ('first_seen', 'confirmed', 'repaired')),
            PRIMARY KEY (record_id, source_id, role)
        );
        CREATE INDEX IF NOT EXISTS idx_rs_source
            ON record_sources(source_id);
    """)
    # Seed the fixed source_types rows
    conn.executescript("""
        INSERT OR IGNORE INTO source_types (id, slug, label, description)
        VALUES
            (1, 'live_scrape',    'Live Scrape',
             'Direct scrape of the WSLCB licensing page'),
            (2, 'co_archive',     'CO Page Archive',
             'Cannabis Observer archived HTML snapshots'),
            (3, 'internet_archive','Internet Archive',
             'Wayback Machine snapshots'),
            (4, 'co_diff_archive','CO Diff Archive',
             'Cannabis Observer diff-detected change snapshots'),
            (5, 'manual',         'Manual Entry',
             'Manually entered or corrected records');
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

    # Migrate record_sources PK to include role (allows multiple
    # roles per record+source pair).  Only needed once; the new
    # CREATE TABLE above already uses the 3-column PK.
    try:
        rs_sql = conn.execute(
            "SELECT sql FROM sqlite_master"
            " WHERE type='table' AND name='record_sources'"
        ).fetchone()
        if rs_sql and "source_id, role)" not in rs_sql[0]:
            logger.info("Migrating record_sources PK to include role")
            conn.executescript("""
                CREATE TABLE record_sources_new (
                    record_id INTEGER NOT NULL
                        REFERENCES license_records(id) ON DELETE CASCADE,
                    source_id INTEGER NOT NULL
                        REFERENCES sources(id) ON DELETE CASCADE,
                    role TEXT NOT NULL DEFAULT 'first_seen'
                        CHECK(role IN ('first_seen', 'confirmed', 'repaired')),
                    PRIMARY KEY (record_id, source_id, role)
                );
                INSERT INTO record_sources_new
                    SELECT record_id, source_id, role
                    FROM record_sources;
                DROP TABLE record_sources;
                ALTER TABLE record_sources_new
                    RENAME TO record_sources;
                CREATE INDEX IF NOT EXISTS idx_rs_source
                    ON record_sources(source_id);
            """)
            logger.info("record_sources PK migration complete")
    except Exception:
        pass  # table doesn't exist yet or already migrated

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
        logger.info("Building FTS index...")
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
        logger.info("FTS index built.")


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


def get_or_create_source(
    conn: sqlite3.Connection,
    source_type_id: int,
    snapshot_path: str | None = None,
    url: str | None = None,
    captured_at: str | None = None,
    scrape_log_id: int | None = None,
    metadata: dict | None = None,
) -> int:
    """Return the source id for the given type + snapshot_path, creating if needed.

    Uses INSERT OR IGNORE followed by SELECT so the call is idempotent.
    The (source_type_id, snapshot_path) pair is the uniqueness key.

    When *snapshot_path* is None and *scrape_log_id* is provided (i.e. a live
    scrape whose snapshot save failed), the lookup uses *scrape_log_id* to
    distinguish distinct scrape events that both lack a snapshot.  Without
    this, all NULL-path scrapes would conflate into the first row.

    .. note:: SQLite treats NULLs as distinct in UNIQUE constraints, so when
       *snapshot_path* is None the SELECT uses ``IS NULL`` checks to find an
       existing row before inserting a new one.
    """
    meta_json = json.dumps(metadata) if metadata else "{}"
    if snapshot_path is not None:
        conn.execute(
            """INSERT OR IGNORE INTO sources
                   (source_type_id, snapshot_path, url, captured_at,
                    scrape_log_id, metadata)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (source_type_id, snapshot_path, url, captured_at,
             scrape_log_id, meta_json),
        )
        row = conn.execute(
            """SELECT id FROM sources
               WHERE source_type_id = ? AND snapshot_path = ?""",
            (source_type_id, snapshot_path),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"Source row vanished for type={source_type_id},"
                f" path={snapshot_path!r}"
            )
    else:
        # NULL snapshot_path — match on scrape_log_id when available so
        # distinct scrape events with failed snapshot saves each get their
        # own source row.
        if scrape_log_id is not None:
            row = conn.execute(
                """SELECT id FROM sources
                   WHERE source_type_id = ? AND snapshot_path IS NULL
                     AND scrape_log_id = ?""",
                (source_type_id, scrape_log_id),
            ).fetchone()
        else:
            row = conn.execute(
                """SELECT id FROM sources
                   WHERE source_type_id = ? AND snapshot_path IS NULL
                     AND scrape_log_id IS NULL""",
                (source_type_id,),
            ).fetchone()
        if row is None:
            cursor = conn.execute(
                """INSERT INTO sources
                       (source_type_id, snapshot_path, url, captured_at,
                        scrape_log_id, metadata)
                   VALUES (?, NULL, ?, ?, ?, ?)""",
                (source_type_id, url, captured_at,
                 scrape_log_id, meta_json),
            )
            return cursor.lastrowid
    return row[0]


def link_record_source(
    conn: sqlite3.Connection,
    record_id: int,
    source_id: int,
    role: str = "first_seen",
) -> None:
    """Link a license record to a source (idempotent).

    Does nothing if the (record_id, source_id) pair already exists.
    """
    conn.execute(
        """INSERT OR IGNORE INTO record_sources (record_id, source_id, role)
           VALUES (?, ?, ?)""",
        (record_id, source_id, role),
    )


if __name__ == "__main__":
    from log_config import setup_logging
    setup_logging()
    init_db()
    logger.info("Database initialized at %s", DB_PATH)
