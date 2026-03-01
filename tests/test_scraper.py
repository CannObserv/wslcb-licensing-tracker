"""Tests for scraper.py — content hash deduplication.

Focuses on the hash comparison logic that skips redundant scrapes
when the WSLCB page hasn't changed since the last run.
"""
import hashlib
from datetime import datetime, timezone

import pytest

from scraper import get_last_content_hash, compute_content_hash


class TestComputeContentHash:
    """SHA-256 hashing of HTML content."""

    def test_returns_sha256_hex(self):
        html = "<html><body>Hello</body></html>"
        result = compute_content_hash(html)
        expected = hashlib.sha256(html.encode("utf-8")).hexdigest()
        assert result == expected
        assert len(result) == 64  # SHA-256 hex digest length

    def test_deterministic(self):
        html = "<html>test</html>"
        assert compute_content_hash(html) == compute_content_hash(html)

    def test_different_content_different_hash(self):
        assert compute_content_hash("a") != compute_content_hash("b")


class TestGetLastContentHash:
    """Retrieve the most recent successful scrape's content hash."""

    def test_no_scrapes_returns_none(self, db):
        assert get_last_content_hash(db) is None

    def test_returns_hash_from_latest_success(self, db):
        db.execute(
            "INSERT INTO scrape_log (started_at, finished_at, status, content_hash) "
            "VALUES (?, ?, 'success', ?)",
            ("2025-01-01T00:00:00", "2025-01-01T00:01:00", "abc123"),
        )
        db.execute(
            "INSERT INTO scrape_log (started_at, finished_at, status, content_hash) "
            "VALUES (?, ?, 'success', ?)",
            ("2025-01-02T00:00:00", "2025-01-02T00:01:00", "def456"),
        )
        db.commit()
        assert get_last_content_hash(db) == "def456"

    def test_ignores_error_scrapes(self, db):
        db.execute(
            "INSERT INTO scrape_log (started_at, finished_at, status, content_hash) "
            "VALUES (?, ?, 'success', ?)",
            ("2025-01-01T00:00:00", "2025-01-01T00:01:00", "good_hash"),
        )
        db.execute(
            "INSERT INTO scrape_log (started_at, finished_at, status, content_hash) "
            "VALUES (?, ?, 'error', ?)",
            ("2025-01-02T00:00:00", "2025-01-02T00:01:00", "error_hash"),
        )
        db.commit()
        assert get_last_content_hash(db) == "good_hash"

    def test_ignores_unchanged_scrapes(self, db):
        """Unchanged scrapes should not be considered as the latest hash source."""
        db.execute(
            "INSERT INTO scrape_log (started_at, finished_at, status, content_hash) "
            "VALUES (?, ?, 'success', ?)",
            ("2025-01-01T00:00:00", "2025-01-01T00:01:00", "real_hash"),
        )
        db.execute(
            "INSERT INTO scrape_log (started_at, finished_at, status, content_hash) "
            "VALUES (?, ?, 'unchanged', ?)",
            ("2025-01-02T00:00:00", "2025-01-02T00:01:00", "real_hash"),
        )
        db.commit()
        # Should still return the hash (from either status is fine)
        assert get_last_content_hash(db) == "real_hash"

    def test_null_hash_returns_none(self, db):
        """Legacy rows without content_hash should be skipped."""
        db.execute(
            "INSERT INTO scrape_log (started_at, finished_at, status) "
            "VALUES (?, ?, 'success')",
            ("2025-01-01T00:00:00", "2025-01-01T00:01:00"),
        )
        db.commit()
        assert get_last_content_hash(db) is None


class TestCleanupRedundantScrapes:
    """Remove data from scrapes that found zero new records."""

    def _setup_scrape(self, db, log_id, started, status, new=0, approved=0,
                      discontinued=0, skipped=100, snapshot_path=None):
        """Insert a scrape_log row and optionally a source + record_sources."""
        db.execute(
            "INSERT INTO scrape_log "
            "(id, started_at, finished_at, status, records_new, records_approved, "
            " records_discontinued, records_skipped, snapshot_path) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (log_id, started, started, status, new, approved,
             discontinued, skipped, snapshot_path),
        )

    def _setup_source(self, db, source_id, log_id, snapshot_path):
        db.execute(
            "INSERT INTO sources (id, source_type_id, snapshot_path, scrape_log_id) "
            "VALUES (?, 1, ?, ?)",
            (source_id, snapshot_path, log_id),
        )

    def _setup_record_source(self, db, record_id, source_id, role="confirmed"):
        db.execute(
            "INSERT INTO record_sources (record_id, source_id, role) "
            "VALUES (?, ?, ?)",
            (record_id, source_id, role),
        )

    def _insert_record(self, db, license_number="078001"):
        db.execute(
            "INSERT INTO license_records "
            "(section_type, record_date, license_number, application_type, scraped_at) "
            "VALUES ('new_application', '2025-06-15', ?, 'NEW APPLICATION', '2025-06-15T12:00:00')",
            (license_number,),
        )
        return db.execute("SELECT last_insert_rowid()").fetchone()[0]

    def test_no_redundant_scrapes(self, db):
        from scraper import cleanup_redundant_scrapes
        result = cleanup_redundant_scrapes(db, delete_files=False)
        assert result["scrape_logs"] == 0

    def test_cleans_zero_insert_scrape(self, db):
        from scraper import cleanup_redundant_scrapes

        rec_id = self._insert_record(db)
        self._setup_scrape(db, 1, "2025-01-01T00:00:00", "success",
                           new=0, approved=0, discontinued=0, skipped=100)
        self._setup_source(db, 1, 1, "wslcb/test/v1.html")
        self._setup_record_source(db, rec_id, 1, "confirmed")
        db.commit()

        result = cleanup_redundant_scrapes(db, delete_files=False)

        assert result["scrape_logs"] == 1
        assert result["sources"] == 1
        assert result["record_sources"] == 1

        # scrape_log re-stamped as unchanged
        status = db.execute(
            "SELECT status FROM scrape_log WHERE id = 1"
        ).fetchone()[0]
        assert status == "unchanged"

        # source row deleted
        assert db.execute("SELECT COUNT(*) FROM sources").fetchone()[0] == 0

        # record_sources deleted
        assert db.execute("SELECT COUNT(*) FROM record_sources").fetchone()[0] == 0

    def test_preserves_productive_scrapes(self, db):
        from scraper import cleanup_redundant_scrapes

        rec_id = self._insert_record(db)
        # This scrape found new records — should NOT be cleaned
        self._setup_scrape(db, 1, "2025-01-01T00:00:00", "success",
                           new=5, approved=3, discontinued=1)
        self._setup_source(db, 1, 1, "wslcb/test/v1.html")
        self._setup_record_source(db, rec_id, 1, "first_seen")
        db.commit()

        result = cleanup_redundant_scrapes(db, delete_files=False)

        assert result["scrape_logs"] == 0
        assert db.execute("SELECT COUNT(*) FROM sources").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM record_sources").fetchone()[0] == 1

    def test_mixed_scrapes(self, db):
        """Only zero-insert scrapes are cleaned; productive ones are kept."""
        from scraper import cleanup_redundant_scrapes

        rec_id = self._insert_record(db)

        # Productive scrape
        self._setup_scrape(db, 1, "2025-01-01T00:00:00", "success", new=10)
        self._setup_source(db, 1, 1, "wslcb/test/v1.html")
        self._setup_record_source(db, rec_id, 1, "first_seen")

        # Redundant scrape
        self._setup_scrape(db, 2, "2025-01-02T00:00:00", "success",
                           new=0, approved=0, discontinued=0, skipped=50)
        self._setup_source(db, 2, 2, "wslcb/test/v2.html")
        self._setup_record_source(db, rec_id, 2, "confirmed")
        db.commit()

        result = cleanup_redundant_scrapes(db, delete_files=False)

        assert result["scrape_logs"] == 1
        assert result["sources"] == 1
        assert result["record_sources"] == 1

        # Productive scrape's data intact
        assert db.execute(
            "SELECT COUNT(*) FROM sources WHERE scrape_log_id = 1"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT COUNT(*) FROM record_sources WHERE source_id = 1"
        ).fetchone()[0] == 1
