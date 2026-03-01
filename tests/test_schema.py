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

    def test_migration_on_existing_db(self):
        """Migration 002 should add table and columns to an existing DB."""
        # Simulate a DB that was at version 1 (baseline already applied)
        conn = get_connection(":memory:")
        migrate(conn)  # applies baseline
        v1 = _get_user_version(conn)

        # Verify we got past version 1 (002 applied)
        assert v1 >= 2
        # Verify table exists
        row = conn.execute(
            "SELECT count(*) FROM sqlite_master "
            "WHERE type='table' AND name='record_enrichments'"
        ).fetchone()
        assert row[0] == 1
        conn.close()
