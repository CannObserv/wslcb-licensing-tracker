"""Tests for pg_scraper.py — pure helper logic and behavioral DB tests."""

import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text

from wslcb_licensing_tracker.pg_scraper import (
    cleanup_redundant_scrapes,
    compute_content_hash,
    get_last_content_hash,
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
@pytest.mark.asyncio
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
