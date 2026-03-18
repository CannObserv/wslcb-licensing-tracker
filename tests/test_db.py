"""Tests for db.py — connection management and constants.

Covers connections, pragmas, raw address normalization, and the
constants that other modules depend on.  Location/source/provenance
helper functions are tested in test_database.py.
"""
import sqlite3

import pytest

from wslcb_licensing_tracker.db import (
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

    def test_busy_timeout_set(self):
        """Python's default sqlite3.connect(timeout=5.0) maps to busy_timeout=5000ms.
        Documented here so a future reduction of the Python timeout doesn't silently break this."""
        conn = get_connection(":memory:")
        timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        assert timeout >= 5000
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


# ── strip_duplicate_marker ──────────────────────────────────────────

class TestStripDuplicateMarker:
    """Tests for the pure strip_duplicate_marker() helper."""

    def _strip(self, name):
        from wslcb_licensing_tracker.db import strip_duplicate_marker
        return strip_duplicate_marker(name)

    def test_parenthesized(self):
        assert self._strip("ADAM (DUPLICATE) BENTON") == "ADAM BENTON"

    def test_parenthesized_numbered_2(self):
        assert self._strip("KATIE (DUPLICATE 2) DAVIS") == "KATIE DAVIS"

    def test_parenthesized_numbered_3(self):
        assert self._strip("KATIE (DUPLICATE 3) DAVIS") == "KATIE DAVIS"

    def test_inline_mid(self):
        assert self._strip("ANNA MARIE DUPLICATE ADAMS") == "ANNA MARIE ADAMS"

    def test_inline_prefix(self):
        assert self._strip("DUPLICATE ITALIAN SUPPLY, LLC") == "ITALIAN SUPPLY, LLC"

    def test_asterisk_variant(self):
        assert self._strip("PAUL *DUPLICATE* SONG") == "PAUL SONG"

    def test_trailing_with_paren(self):
        # "JAY WON (DUPLICATE)" -> "JAY WON"
        assert self._strip("JAY WON (DUPLICATE)") == "JAY WON"

    def test_unclosed_paren(self):
        # WSLCB source has one case with an unclosed open paren:
        # 'ELIZABETH (DUPLICATE A MATTHEWS'
        assert self._strip("ELIZABETH (DUPLICATE A MATTHEWS") == "ELIZABETH A MATTHEWS"

    def test_no_marker_unchanged(self):
        assert self._strip("ALICE SMITH") == "ALICE SMITH"

    def test_collapse_extra_spaces(self):
        """Double spaces left after DUPLICATE removal are collapsed to one."""
        # Double space before AND after the marker: after removal 'ANNA  MARIE'
        # must be collapsed to 'ANNA MARIE'.
        assert self._strip("ANNA  DUPLICATE  MARIE") == "ANNA MARIE"

    def test_result_has_no_double_space(self):
        assert "  " not in self._strip("NEALY DUPLICATE EVANS")


# ── clean_applicants_string ──────────────────────────────────────────

class TestCleanApplicantsStringDuplicate:
    """DUPLICATE markers must be stripped by clean_applicants_string()."""

    def _clean(self, s):
        from wslcb_licensing_tracker.db import clean_applicants_string
        return clean_applicants_string(s)

    def test_parenthesized_marker_stripped(self):
        result = self._clean("BIZ; ADAM (DUPLICATE) BENTON; ADAM BENTON")
        # Both tokens clean to "ADAM BENTON"; duplicates are collapsed
        assert "(DUPLICATE)" not in result
        assert "DUPLICATE" not in result

    def test_inline_marker_stripped(self):
        result = self._clean("BIZ; NEALY DUPLICATE EVANS; NEALY EVANS")
        assert "DUPLICATE" not in result

    def test_deduplication_after_stripping(self):
        """When stripping leaves two identical tokens, only one is kept."""
        result = self._clean("BIZ; ADAM (DUPLICATE) BENTON; ADAM BENTON")
        parts = [p.strip() for p in result.split(";")] if result else []
        assert parts.count("ADAM BENTON") == 1

    def test_only_duplicate_token_synthesizes_clean_name(self):
        """A lone DUPLICATE-annotated token should yield the stripped clean name."""
        result = self._clean("BIZ; LORIE DUPLICATE FAZIO")
        assert "LORIE FAZIO" in result
        assert "DUPLICATE" not in result
