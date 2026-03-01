"""Database schema, migrations, and FTS setup for the WSLCB licensing tracker.

All DDL, data seeding, and schema migrations live here.  Connection
management lives in ``db.py``.

Migrations use SQLite's ``PRAGMA user_version`` for tracking.  Adding a
new migration is a matter of appending a ``(version, name, callable)``
tuple to :data:`MIGRATIONS`.
"""
import logging
import sqlite3
from collections.abc import Callable

from db import get_db

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# FTS5 configuration
# ------------------------------------------------------------------

_FTS_COLUMNS = [
    "business_name", "business_location", "applicants",
    "license_type", "application_type", "license_number",
    "previous_business_name", "previous_applicants",
    "previous_business_location",
]


# ------------------------------------------------------------------
# Migration definitions
# ------------------------------------------------------------------

def _m001_baseline(conn: sqlite3.Connection) -> None:
    """Create all tables for a fresh database.

    For existing databases this migration is skipped entirely (detected
    by :func:`migrate` when tables already exist and user_version == 0).
    """
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
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (code, endorsement_id)
        );

        -- Note: pre-existing DBs have snapshot_path as a trailing ALTER TABLE
        -- column rather than inline.  Functionally identical; only the
        -- sqlite_master DDL text differs.
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
            content_hash TEXT,
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
            raw_business_name TEXT,
            raw_previous_business_name TEXT,
            raw_applicants TEXT,
            raw_previous_applicants TEXT,
            scraped_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(section_type, record_date, license_number, application_type)
        );

        CREATE TABLE IF NOT EXISTS record_endorsements (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            PRIMARY KEY (record_id, endorsement_id)
        );
        CREATE INDEX IF NOT EXISTS idx_re_endorsement
            ON record_endorsements(endorsement_id);

        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            entity_type TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS record_entities (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            entity_id INTEGER NOT NULL
                REFERENCES entities(id) ON DELETE CASCADE,
            role TEXT NOT NULL DEFAULT 'applicant',
            position INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (record_id, entity_id, role)
        );
        CREATE INDEX IF NOT EXISTS idx_re_entity ON record_entities(entity_id);
        CREATE INDEX IF NOT EXISTS idx_re_role ON record_entities(role);

        CREATE TABLE IF NOT EXISTS record_links (
            id INTEGER PRIMARY KEY,
            new_app_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            outcome_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            confidence TEXT NOT NULL
                CHECK (confidence IN ('high', 'medium', 'low')),
            days_gap INTEGER,
            linked_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(new_app_id, outcome_id)
        );
        CREATE INDEX IF NOT EXISTS idx_record_links_new
            ON record_links(new_app_id);
        CREATE INDEX IF NOT EXISTS idx_record_links_outcome
            ON record_links(outcome_id);

        CREATE TABLE IF NOT EXISTS record_enrichments (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            step TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT '1',
            PRIMARY KEY (record_id, step)
        );

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

    # Indexes on license_records
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


def _m002_enrichment_tracking(conn: sqlite3.Connection) -> None:
    """Add enrichment tracking table and raw value shadow columns.

    Phase 3.1: ``record_enrichments`` tracks which enrichment steps
    have been applied to each record (endorsements, entities, address,
    outcome_link).  This enables targeted re-processing.

    Phase 3.2: ``raw_*`` shadow columns preserve the as-parsed values
    before name cleaning (uppercase, punctuation stripping).  Going
    forward new records store originals here; for existing records
    the cleaned value is backfilled (we've lost the originals).
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS record_enrichments (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            step TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT '1',
            PRIMARY KEY (record_id, step)
        )
    """)

    # Add raw_* shadow columns if they don't already exist (fresh DBs
    # created with baseline already have them inline).
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(license_records)").fetchall()
    }
    for col in (
        "raw_business_name",
        "raw_previous_business_name",
        "raw_applicants",
        "raw_previous_applicants",
    ):
        if col not in existing:
            conn.execute(
                f"ALTER TABLE license_records ADD COLUMN {col} TEXT"
            )

    # Backfill raw_* columns with current cleaned values for existing records
    conn.execute("""
        UPDATE license_records SET
            raw_business_name = business_name,
            raw_previous_business_name = previous_business_name,
            raw_applicants = applicants,
            raw_previous_applicants = previous_applicants
        WHERE raw_business_name IS NULL
    """)


def _m003_content_hash(conn: sqlite3.Connection) -> None:
    """Add content_hash column to scrape_log for duplicate detection.

    Stores the SHA-256 hex digest of fetched HTML so the scraper can
    short-circuit when the WSLCB page hasn't changed since the last run.
    """
    # Guard: scrape_log may not exist in minimal test databases that
    # only create license_records before running migrate().
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "scrape_log" not in tables:
        return

    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(scrape_log)").fetchall()
    }
    if "content_hash" not in existing:
        conn.execute(
            "ALTER TABLE scrape_log ADD COLUMN content_hash TEXT"
        )


# -- Migration registry ------------------------------------------------

# Migration registry.
#
# Prior ad-hoc migrations (inline ALTER TABLE blocks, migrate_locations.py,
# record_sources PK rebuild) are all subsumed by the existing-DB stamp in
# migrate(): databases created before this framework get stamped to
# _EXISTING_DB_STAMP_VERSION (currently 1), then newer migrations run
# normally.  See _EXISTING_DB_STAMP_VERSION for details.
MIGRATIONS: list[tuple[int, str, Callable[[sqlite3.Connection], None]]] = [
    (1, "baseline", _m001_baseline),
    (2, "enrichment_tracking", _m002_enrichment_tracking),
    (3, "content_hash", _m003_content_hash),
]


# ------------------------------------------------------------------
# Migration runner
# ------------------------------------------------------------------

def _get_user_version(conn: sqlite3.Connection) -> int:
    """Return the current ``PRAGMA user_version``."""
    return conn.execute("PRAGMA user_version").fetchone()[0]


def _set_user_version(conn: sqlite3.Connection, version: int) -> None:
    """Set ``PRAGMA user_version`` (not transactional in SQLite)."""
    conn.execute(f"PRAGMA user_version = {version}")


def _database_has_tables(conn: sqlite3.Connection) -> bool:
    """Return True if the database already has application tables.

    Used to detect an existing database whose ``user_version`` is still 0
    (created before the migration framework was introduced).
    """
    row = conn.execute(
        "SELECT count(*) FROM sqlite_master"
        " WHERE type='table' AND name='license_records'"
    ).fetchone()
    return row[0] > 0


# The highest migration version that is fully subsumed by the pre-framework
# schema.  Existing databases (tables present, user_version == 0) are
# stamped to this version and then the migration loop runs everything
# above it.  Bump this ONLY if a new migration is purely a stamp for
# existing DBs (i.e., the DDL already exists in the wild).
_EXISTING_DB_STAMP_VERSION = 1


def migrate(conn: sqlite3.Connection) -> int:
    """Run all pending migrations and return the final version.

    Existing databases (tables present, ``user_version == 0``) are
    stamped to :data:`_EXISTING_DB_STAMP_VERSION` (the last version
    subsumed by their existing schema), then the migration loop runs
    any newer migrations normally.
    """
    current = _get_user_version(conn)

    # Existing database created before migration framework
    if current == 0 and _database_has_tables(conn):
        logger.info(
            "Existing database detected; stamping user_version to %d",
            _EXISTING_DB_STAMP_VERSION,
        )
        _set_user_version(conn, _EXISTING_DB_STAMP_VERSION)
        current = _EXISTING_DB_STAMP_VERSION

    for version, name, fn in MIGRATIONS:
        if version > current:
            logger.info("Running migration %d: %s", version, name)
            fn(conn)
            _set_user_version(conn, version)
            conn.commit()
            current = version

    return current


# ------------------------------------------------------------------
# FTS5 full-text search
# ------------------------------------------------------------------

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
# Public entry point
# ------------------------------------------------------------------

def init_db(conn: sqlite3.Connection | None = None) -> sqlite3.Connection | None:
    """Create tables, run migrations, and build FTS.  Safe to call repeatedly.

    If *conn* is provided it is used directly (and **not** closed);
    otherwise a new connection via :func:`db.get_db` is opened and
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
    """Internal: run migrations + FTS setup on *conn*."""
    migrate(conn)
    _ensure_fts(conn)
    conn.commit()
