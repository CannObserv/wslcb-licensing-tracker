"""Tests for schema.py — migration framework and FTS setup.

All tests use in-memory SQLite.  The ``db`` fixture from conftest.py
already exercises the full init_db() path; these tests focus on the
migration machinery itself.
"""
import sqlite3

import pytest

from db import get_connection
from schema import (
    MIGRATIONS,
    _get_user_version,
    _set_user_version,
    _database_has_tables,
    init_db,
    migrate,
)


class TestUserVersion:
    """PRAGMA user_version read/write helpers."""

    def test_default_is_zero(self):
        conn = get_connection(":memory:")
        assert _get_user_version(conn) == 0
        conn.close()

    def test_set_and_get(self):
        conn = get_connection(":memory:")
        _set_user_version(conn, 42)
        assert _get_user_version(conn) == 42
        conn.close()


class TestDatabaseHasTables:
    """Detection of pre-existing databases."""

    def test_empty_database(self):
        conn = get_connection(":memory:")
        assert _database_has_tables(conn) is False
        conn.close()

    def test_after_init(self, db):
        assert _database_has_tables(db) is True


class TestMigrate:
    """Migration runner behavior."""

    def test_fresh_database_runs_baseline(self):
        """A brand-new database gets all migrations applied."""
        conn = get_connection(":memory:")
        version = migrate(conn)
        assert version == MIGRATIONS[-1][0]
        assert _get_user_version(conn) == version
        # Tables should exist
        assert _database_has_tables(conn) is True
        conn.close()

    def test_existing_database_stamps_and_runs_new_migrations(self):
        """An existing DB (tables present, user_version=0) gets stamped
        past baseline, then newer migrations run normally."""
        conn = get_connection(":memory:")
        # Simulate an existing database by creating the sentinel table
        conn.execute("""
            CREATE TABLE license_records (
                id INTEGER PRIMARY KEY,
                section_type TEXT NOT NULL,
                record_date TEXT NOT NULL,
                business_name TEXT,
                license_number TEXT,
                application_type TEXT,
                previous_business_name TEXT DEFAULT '',
                previous_applicants TEXT DEFAULT '',
                applicants TEXT,
                scraped_at TEXT NOT NULL,
                UNIQUE(section_type, record_date, license_number, application_type)
            )
        """)
        conn.commit()
        assert _get_user_version(conn) == 0

        version = migrate(conn)
        assert version == MIGRATIONS[-1][0]
        assert _get_user_version(conn) == MIGRATIONS[-1][0]
        # Verify baseline was skipped — tables it would create don't exist
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "locations" not in tables, "baseline ran when it should have been skipped"
        # But migration 002 DID run — record_enrichments exists
        assert "record_enrichments" in tables, "migration 002 should have run"
        # And raw_* columns were added
        cur = conn.execute("SELECT * FROM license_records LIMIT 0")
        cols = {desc[0] for desc in cur.description}
        assert "raw_business_name" in cols
        conn.close()

    def test_already_current_is_noop(self, db):
        """Running migrate on an already-current DB does nothing."""
        v1 = _get_user_version(db)
        v2 = migrate(db)
        assert v1 == v2

    def test_idempotent(self):
        """Running migrate twice on a fresh DB is safe."""
        conn = get_connection(":memory:")
        migrate(conn)
        migrate(conn)  # should not raise
        assert _get_user_version(conn) == MIGRATIONS[-1][0]
        conn.close()


class TestInitDb:
    """init_db() from schema.py."""

    def test_sets_user_version(self):
        conn = get_connection(":memory:")
        init_db(conn)
        assert _get_user_version(conn) >= 1
        conn.close()

    def test_fts_created(self):
        conn = get_connection(":memory:")
        init_db(conn)
        cur = conn.execute("SELECT * FROM license_records_fts LIMIT 0")
        cols = [desc[0] for desc in cur.description]
        assert "business_name" in cols
        assert "license_number" in cols
        conn.close()

    def test_returns_conn_when_passed(self):
        conn = get_connection(":memory:")
        result = init_db(conn)
        assert result is conn
        conn.close()

    def test_returns_none_when_no_conn(self, tmp_path, monkeypatch):
        import db as db_mod
        monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
        monkeypatch.setattr(db_mod, "DB_PATH", tmp_path / "test.db")
        result = init_db()
        assert result is None


class TestMigrationRegistry:
    """Sanity checks on the MIGRATIONS list."""

    def test_versions_are_sequential(self):
        versions = [v for v, _, _ in MIGRATIONS]
        assert versions == list(range(1, len(MIGRATIONS) + 1))

    def test_names_are_unique(self):
        names = [n for _, n, _ in MIGRATIONS]
        assert len(names) == len(set(names))

    def test_callables_are_callable(self):
        for _, _, fn in MIGRATIONS:
            assert callable(fn)


class TestMigration002RecordEnrichments:
    """Migration 002: record_enrichments table and raw_* shadow columns."""

    def test_record_enrichments_table_exists(self, db):
        """The record_enrichments table should exist after init_db."""
        row = db.execute(
            "SELECT count(*) FROM sqlite_master "
            "WHERE type='table' AND name='record_enrichments'"
        ).fetchone()
        assert row[0] == 1

    def test_record_enrichments_columns(self, db):
        """record_enrichments should have the expected columns."""
        cur = db.execute("SELECT * FROM record_enrichments LIMIT 0")
        cols = {desc[0] for desc in cur.description}
        assert cols == {"record_id", "step", "completed_at", "version"}

    def test_record_enrichments_primary_key(self, db):
        """Inserting the same (record_id, step) should raise IntegrityError."""
        import sqlite3

        # Create a minimal record to reference
        db.execute(
            "INSERT INTO license_records "
            "(section_type, record_date, license_number, application_type, scraped_at) "
            "VALUES ('approved', '2025-01-01', '999999', 'TEST', '2025-01-01T00:00:00')"
        )
        rid = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        db.execute(
            "INSERT INTO record_enrichments (record_id, step, completed_at) "
            "VALUES (?, 'endorsements', '2025-01-01T00:00:00')",
            (rid,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO record_enrichments (record_id, step, completed_at) "
                "VALUES (?, 'endorsements', '2025-01-02T00:00:00')",
                (rid,),
            )

    def test_record_enrichments_cascade_delete(self, db):
        """Deleting a record should cascade to record_enrichments."""
        db.execute(
            "INSERT INTO license_records "
            "(section_type, record_date, license_number, application_type, scraped_at) "
            "VALUES ('approved', '2025-01-01', '888888', 'TEST', '2025-01-01T00:00:00')"
        )
        rid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO record_enrichments (record_id, step, completed_at) "
            "VALUES (?, 'endorsements', '2025-01-01T00:00:00')",
            (rid,),
        )
        db.execute("DELETE FROM license_records WHERE id = ?", (rid,))
        row = db.execute(
            "SELECT count(*) FROM record_enrichments WHERE record_id = ?",
            (rid,),
        ).fetchone()
        assert row[0] == 0

    def test_raw_columns_exist_on_license_records(self, db):
        """license_records should have raw_* shadow columns."""
        cur = db.execute("SELECT * FROM license_records LIMIT 0")
        cols = {desc[0] for desc in cur.description}
        for col in [
            "raw_business_name",
            "raw_previous_business_name",
            "raw_applicants",
            "raw_previous_applicants",
        ]:
            assert col in cols, f"Missing column: {col}"

    def test_fresh_db_reaches_version_2(self):
        """A fresh DB should run both baseline and m002."""
        conn = get_connection(":memory:")
        migrate(conn)
        v1 = _get_user_version(conn)

        assert v1 >= 2
        # Verify table exists
        row = conn.execute(
            "SELECT count(*) FROM sqlite_master "
            "WHERE type='table' AND name='record_enrichments'"
        ).fetchone()
        assert row[0] == 1
        conn.close()


class TestMigration003ContentHash:
    """Migration 003 adds content_hash column to scrape_log."""

    def test_column_exists_after_migration(self, db):
        """content_hash column should exist in scrape_log after init_db."""
        cols = {
            row[1]
            for row in db.execute("PRAGMA table_info(scrape_log)").fetchall()
        }
        assert "content_hash" in cols

    def test_column_is_nullable(self, db):
        """content_hash should allow NULL (for legacy rows)."""
        db.execute(
            "INSERT INTO scrape_log (started_at, status) VALUES ('2025-01-01', 'success')"
        )
        row = db.execute(
            "SELECT content_hash FROM scrape_log WHERE started_at = '2025-01-01'"
        ).fetchone()
        assert row[0] is None

    def test_fresh_db_has_column_inline(self, db):
        """Fresh databases get content_hash from baseline, not ALTER TABLE."""
        sql = db.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'scrape_log'"
        ).fetchone()[0]
        assert "content_hash" in sql

    def test_migration_on_existing_db(self):
        """Existing DB without content_hash gets it via ALTER TABLE."""
        from db import get_connection

        conn = get_connection(":memory:")
        # Simulate an existing DB at version 2 (before this migration)
        conn.execute("PRAGMA user_version = 2")
        conn.executescript("""
            CREATE TABLE license_records (
                id INTEGER PRIMARY KEY,
                section_type TEXT, record_date TEXT,
                license_number TEXT, application_type TEXT,
                scraped_at TEXT,
                UNIQUE(section_type, record_date, license_number, application_type)
            );
            CREATE TABLE scrape_log (
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

        migrate(conn)

        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(scrape_log)").fetchall()
        }
        assert "content_hash" in cols
        conn.close()


class TestMigration004AddressValidatorV1:
    """Migration 004: rename std_state/std_zip, add std_country."""

    def test_fresh_db_has_renamed_columns(self, db):
        """Fresh DB baseline has std_region, std_postal_code, std_country."""
        cols = {
            row[1]
            for row in db.execute("PRAGMA table_info(locations)").fetchall()
        }
        assert "std_region" in cols
        assert "std_postal_code" in cols
        assert "std_country" in cols
        assert "std_state" not in cols
        assert "std_zip" not in cols

    def test_fresh_db_has_postal_code_index(self, db):
        """Fresh DB has idx_locations_std_postal_code, not the old std_zip index."""
        indexes = {
            row[1]
            for row in db.execute(
                "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='locations'"
            ).fetchall()
        }
        assert "idx_locations_std_postal_code" in indexes
        assert "idx_locations_std_zip" not in indexes

    def test_migration_renames_columns_on_existing_db(self):
        """Existing DB with old column names gets them renamed by migration 004."""
        from db import get_connection
        from schema import migrate

        conn = get_connection(":memory:")
        conn.execute("PRAGMA user_version = 3")
        conn.executescript("""
            CREATE TABLE locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_address TEXT NOT NULL UNIQUE,
                city TEXT DEFAULT '',
                state TEXT DEFAULT 'WA',
                zip_code TEXT DEFAULT '',
                address_line_1 TEXT DEFAULT '',
                address_line_2 TEXT DEFAULT '',
                std_city TEXT DEFAULT '',
                std_state TEXT DEFAULT '',
                std_zip TEXT DEFAULT '',
                address_validated_at TEXT
            );
            CREATE INDEX idx_locations_std_zip ON locations(std_zip);
        """)
        migrate(conn)

        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(locations)").fetchall()
        }
        assert "std_region" in cols
        assert "std_postal_code" in cols
        assert "std_country" in cols
        assert "std_state" not in cols
        assert "std_zip" not in cols
        conn.close()

    def test_migration_backfills_country_for_validated_rows(self):
        """Existing validated rows get std_country = 'US' after migration 004."""
        from db import get_connection
        from schema import migrate

        conn = get_connection(":memory:")
        conn.execute("PRAGMA user_version = 3")
        conn.executescript("""
            CREATE TABLE locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_address TEXT NOT NULL UNIQUE,
                city TEXT DEFAULT '',
                state TEXT DEFAULT 'WA',
                zip_code TEXT DEFAULT '',
                address_line_1 TEXT DEFAULT '',
                address_line_2 TEXT DEFAULT '',
                std_city TEXT DEFAULT '',
                std_state TEXT DEFAULT '',
                std_zip TEXT DEFAULT '',
                address_validated_at TEXT
            );
        """)
        conn.execute(
            "INSERT INTO locations (raw_address, std_state, address_validated_at)"
            " VALUES ('123 MAIN ST, SEATTLE, WA 98101', 'WA', '2025-01-01T00:00:00+00:00')"
        )
        conn.execute(
            "INSERT INTO locations (raw_address) VALUES ('456 OAK AVE, TACOMA, WA 98401')"
        )
        conn.commit()

        migrate(conn)

        rows = conn.execute(
            "SELECT raw_address, std_country FROM locations ORDER BY id"
        ).fetchall()
        # Validated row gets 'US'; unvalidated row stays ''
        assert rows[0][1] == "US"
        assert rows[1][1] == ""
        conn.close()

    def test_migration_skips_gracefully_without_locations_table(self):
        """Migration 004 does not crash when locations table doesn't exist."""
        from db import get_connection
        from schema import migrate

        conn = get_connection(":memory:")
        conn.execute("PRAGMA user_version = 3")
        conn.executescript("""
            CREATE TABLE license_records (
                id INTEGER PRIMARY KEY,
                section_type TEXT, record_date TEXT,
                license_number TEXT, application_type TEXT,
                scraped_at TEXT,
                UNIQUE(section_type, record_date, license_number, application_type)
            );
        """)
        # Should not raise
        migrate(conn)
        conn.close()


class TestMigration008EndorsementDismissedSuggestions:
    """Tests for migration 008: endorsement_dismissed_suggestions table."""

    def test_table_created_on_fresh_db(self):
        from db import get_connection
        from schema import init_db

        conn = get_connection(":memory:")
        init_db(conn)
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "endorsement_dismissed_suggestions" in tables
        conn.close()

    def test_migration_adds_table_to_existing_db(self):
        """Migration 008 adds the table to a DB that only has migrations 1-7."""
        from db import get_connection
        from schema import migrate

        conn = get_connection(":memory:")
        # Bootstrap at version 7 (endorsement_aliases exists, dismissed doesn't)
        conn.executescript("""
            PRAGMA user_version = 7;
            CREATE TABLE license_endorsements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE endorsement_aliases (
                id INTEGER PRIMARY KEY,
                endorsement_id INTEGER NOT NULL
                    REFERENCES license_endorsements(id) ON DELETE CASCADE,
                canonical_endorsement_id INTEGER NOT NULL
                    REFERENCES license_endorsements(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                created_by TEXT,
                UNIQUE(endorsement_id)
            );
        """)
        migrate(conn)
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "endorsement_dismissed_suggestions" in tables
        assert conn.execute("PRAGMA user_version").fetchone()[0] == MIGRATIONS[-1][0]
        conn.close()

    def test_dismissed_suggestions_schema(self):
        from db import get_connection
        from schema import init_db

        conn = get_connection(":memory:")
        init_db(conn)
        # Insert two endorsements and a dismissed pair
        conn.execute("INSERT INTO license_endorsements (name) VALUES ('A'), ('B')")
        ids = [r[0] for r in conn.execute("SELECT id FROM license_endorsements").fetchall()]
        a, b = min(ids), max(ids)
        conn.execute(
            "INSERT INTO endorsement_dismissed_suggestions "
            "(endorsement_id_a, endorsement_id_b, dismissed_by) VALUES (?, ?, 'test')",
            (a, b),
        )
        conn.commit()
        row = conn.execute(
            "SELECT dismissed_by FROM endorsement_dismissed_suggestions "
            "WHERE endorsement_id_a = ? AND endorsement_id_b = ?",
            (a, b),
        ).fetchone()
        assert row[0] == "test"
        conn.close()

    def test_check_constraint_enforces_id_order(self):
        """CHECK (endorsement_id_a < endorsement_id_b) is enforced."""
        import sqlite3 as sqlite_mod
        from db import get_connection
        from schema import init_db

        conn = get_connection(":memory:")
        init_db(conn)
        conn.execute("INSERT INTO license_endorsements (name) VALUES ('X'), ('Y')")
        ids = [r[0] for r in conn.execute("SELECT id FROM license_endorsements").fetchall()]
        a, b = min(ids), max(ids)
        # Inserting with a > b should fail the CHECK constraint
        with pytest.raises(sqlite_mod.IntegrityError):
            conn.execute(
                "INSERT INTO endorsement_dismissed_suggestions "
                "(endorsement_id_a, endorsement_id_b, dismissed_by) VALUES (?, ?, 'bad')",
                (b, a),
            )
        conn.close()


class TestMigration009RegulatedSubstances:
    """Migration 009 — regulated_substances + junction table."""

    def test_tables_exist_on_fresh_db(self, db):
        tables = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "regulated_substances" in tables
        assert "regulated_substance_endorsements" in tables

    def test_index_exists_on_fresh_db(self, db):
        indexes = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
        assert "idx_rse_endorsement" in indexes

    def test_migration_adds_tables_to_existing_db(self):
        """Existing DB (user_version=0, tables present) gets migrated."""
        from schema import migrate, _get_user_version
        conn = get_connection(":memory:")
        # Simulate pre-existing DB by running baseline only
        from schema import _m001_baseline
        _m001_baseline(conn)
        conn.commit()
        version = migrate(conn)
        assert version == MIGRATIONS[-1][0]
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "regulated_substances" in tables
        assert "regulated_substance_endorsements" in tables
        conn.close()

    def test_seed_cannabis_substance_exists(self, db):
        row = db.execute(
            "SELECT id FROM regulated_substances WHERE name = 'Cannabis'"
        ).fetchone()
        assert row is not None

    def test_seed_alcohol_substance_exists(self, db):
        row = db.execute(
            "SELECT id FROM regulated_substances WHERE name = 'Alcohol'"
        ).fetchone()
        assert row is not None

    def test_seed_cannabis_has_endorsements_after_endorsement_seed(self, db):
        """After seeding endorsements and re-running the migration, Cannabis
        should have all its expected endorsements linked."""
        from endorsements import seed_endorsements
        from schema import _m009_regulated_substances
        seed_endorsements(db)
        db.commit()
        # Re-run seed (idempotent via INSERT OR IGNORE)
        _m009_regulated_substances(db)
        db.commit()
        sub_id = db.execute(
            "SELECT id FROM regulated_substances WHERE name = 'Cannabis'"
        ).fetchone()[0]
        count = db.execute(
            "SELECT COUNT(*) FROM regulated_substance_endorsements WHERE substance_id = ?",
            (sub_id,),
        ).fetchone()[0]
        # Cannabis retailer + processor + tiers 1-3 + transportation + research
        # + retail cert holder + SE cannabis retailer + tribal compact = 10 from seed.
        # SE RETAIL CERTIFICATE HOLDER and MEDICAL CANNABIS ENDORSEMENT are
        # auto-discovered from live data and added when the scraper runs.
        assert count >= 10

    def test_undefined_endorsement_has_no_substance(self, db):
        from endorsements import seed_endorsements, _ensure_endorsement
        seed_endorsements(db)
        undef_id = _ensure_endorsement(db, "UNDEFINED")
        db.commit()
        from schema import _m009_regulated_substances
        _m009_regulated_substances(db)
        db.commit()
        row = db.execute(
            "SELECT 1 FROM regulated_substance_endorsements WHERE endorsement_id = ?",
            (undef_id,),
        ).fetchone()
        assert row is None
