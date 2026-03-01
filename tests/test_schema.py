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

    def test_existing_database_stamps_without_rerun(self):
        """An existing DB (tables present, user_version=0) gets stamped
        without re-running baseline DDL."""
        conn = get_connection(":memory:")
        # Simulate an existing database by creating the sentinel table
        conn.execute("""
            CREATE TABLE license_records (
                id INTEGER PRIMARY KEY,
                section_type TEXT NOT NULL,
                record_date TEXT NOT NULL,
                license_number TEXT,
                application_type TEXT,
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
