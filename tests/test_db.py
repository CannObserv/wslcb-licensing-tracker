"""Tests for db.py — connection management and constants.

These test the thin base layer: connections, pragmas, raw address
normalization, and the constants that other modules depend on.
"""
import sqlite3

import pytest

from db import (
    DATA_DIR,
    DB_PATH,
    SOURCE_TYPE_CO_ARCHIVE,
    SOURCE_TYPE_CO_DIFF_ARCHIVE,
    SOURCE_TYPE_INTERNET_ARCHIVE,
    SOURCE_TYPE_LIVE_SCRAPE,
    SOURCE_TYPE_MANUAL,
    WSLCB_SOURCE_URL,
    _normalize_raw_address,
    get_connection,
    get_db,
)


class TestConstants:
    """Source type IDs and paths are stable."""

    def test_source_type_ids(self):
        assert SOURCE_TYPE_LIVE_SCRAPE == 1
        assert SOURCE_TYPE_CO_ARCHIVE == 2
        assert SOURCE_TYPE_INTERNET_ARCHIVE == 3
        assert SOURCE_TYPE_CO_DIFF_ARCHIVE == 4
        assert SOURCE_TYPE_MANUAL == 5

    def test_wslcb_url(self):
        assert "lcb.wa.gov" in WSLCB_SOURCE_URL

    def test_data_dir_is_path(self):
        from pathlib import Path
        assert isinstance(DATA_DIR, Path)

    def test_db_path_under_data_dir(self):
        assert str(DB_PATH).startswith(str(DATA_DIR))


class TestGetConnection:
    def test_memory_connection(self):
        conn = get_connection(":memory:")
        assert conn is not None
        conn.execute("SELECT 1")
        conn.close()

    def test_row_factory_set(self):
        conn = get_connection(":memory:")
        assert conn.row_factory == sqlite3.Row
        conn.close()

    def test_foreign_keys_enabled(self):
        conn = get_connection(":memory:")
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1
        conn.close()

    def test_wal_mode(self):
        """WAL mode is requested (in-memory may report 'memory')."""
        conn = get_connection(":memory:")
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode in ("wal", "memory")
        conn.close()


class TestGetDb:
    def test_context_manager(self):
        with get_db(":memory:") as conn:
            conn.execute("SELECT 1")


class TestNormalizeRawAddress:
    def test_nbsp_replaced(self):
        assert _normalize_raw_address("123\xa0MAIN\xa0ST") == "123 MAIN ST"

    def test_regular_space_unchanged(self):
        assert _normalize_raw_address("123 MAIN ST") == "123 MAIN ST"

    def test_empty_passthrough(self):
        assert _normalize_raw_address("") == ""

    def test_none_passthrough(self):
        assert _normalize_raw_address(None) is None
