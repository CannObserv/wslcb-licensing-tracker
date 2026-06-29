"""Tests for pg_scraper.py — pure helper logic and behavioral DB tests."""

import gzip
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text

from wslcb_licensing_tracker.pg_scraper import (
    cleanup_redundant_scrapes,
    compute_content_hash,
    get_last_content_hash,
    save_html_snapshot,
)

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)


def test_compute_content_hash_is_deterministic():
    """compute_content_hash returns the same hash for the same input."""
    assert compute_content_hash("<html>test</html>") == compute_content_hash("<html>test</html>")


def test_get_last_content_hash_is_importable():
    """pg_scraper.get_last_content_hash is an async function."""
    import inspect

    assert inspect.iscoroutinefunction(get_last_content_hash)


@pytest.mark.asyncio
async def test_get_last_content_hash_returns_none_when_no_eligible_rows():
    """get_last_content_hash returns None when no eligible rows exist."""
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_result
    assert await get_last_content_hash(mock_conn) is None


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_cleanup_redundant_scrapes_removes_unchanged_rows(pg_engine):
    """cleanup_redundant_scrapes deletes unchanged scrape_log rows with no linked sources."""
    async with pg_engine.connect() as conn:
        row = await conn.execute(
            text(
                "INSERT INTO scrape_log (started_at, status) "
                "VALUES (:started_at, 'unchanged') RETURNING id"
            ),
            {"started_at": datetime.now(UTC)},
        )
        inserted_id = row.scalar_one()
        await conn.commit()

    result = await cleanup_redundant_scrapes(pg_engine, delete_files=False)

    assert result["scrape_logs"] >= 1
    assert result["files"] == 0

    # Verify the specific row was removed
    async with pg_engine.connect() as conn:
        remaining = await conn.execute(
            text("SELECT id FROM scrape_log WHERE id = :id"),
            {"id": inserted_id},
        )
        assert remaining.fetchone() is None


# ── save_html_snapshot ───────────────────────────────────────────────


def test_save_html_snapshot_creates_gz(tmp_path, monkeypatch):
    """save_html_snapshot writes a .html.gz file, not .html."""
    import wslcb_licensing_tracker.pg_scraper as mod

    monkeypatch.setattr(mod, "DATA_DIR", tmp_path)
    html = "<html><body>test</body></html>"
    scrape_time = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

    path = save_html_snapshot(html, scrape_time)

    assert path.name.endswith(".html.gz"), f"Expected .html.gz, got {path.name}"
    assert path.exists()
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        assert fh.read() == html


def test_save_html_snapshot_increments_version(tmp_path, monkeypatch):
    """save_html_snapshot increments version when same-date file exists."""
    import wslcb_licensing_tracker.pg_scraper as mod

    monkeypatch.setattr(mod, "DATA_DIR", tmp_path)
    scrape_time = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

    p1 = save_html_snapshot("v1", scrape_time)
    p2 = save_html_snapshot("v2", scrape_time)

    assert "v1" in p1.name
    assert "v2" in p2.name
