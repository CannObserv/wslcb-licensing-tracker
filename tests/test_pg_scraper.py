"""Tests for pg_scraper.py — pure helper logic and behavioral DB tests."""

import os

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


@_needs_db
@pytest.mark.asyncio
async def test_get_last_content_hash_returns_none_on_empty_table(pg_conn):
    """get_last_content_hash returns None when scrape_log has no eligible rows."""
    await pg_conn.execute(text("DELETE FROM scrape_log"))
    result = await get_last_content_hash(pg_conn)
    assert result is None


@_needs_db
@pytest.mark.asyncio
async def test_cleanup_redundant_scrapes_removes_unchanged_rows(pg_engine):
    """cleanup_redundant_scrapes deletes unchanged scrape_log rows with no linked sources."""
    async with pg_engine.connect() as conn:
        row = await conn.execute(
            text(
                "INSERT INTO scrape_log (started_at, status) "
                "VALUES (now()::text, 'unchanged') RETURNING id"
            )
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
