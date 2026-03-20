"""Tests for async database engine and connection management.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
Tests are skipped automatically when the env var is absent.
"""

import os

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from wslcb_licensing_tracker.database import create_engine_from_env, get_database_url


@pytest.fixture
def test_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set — skipping PostgreSQL tests")
    return url


@pytest.fixture
async def pg_engine(test_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_url)
    engine = create_engine_from_env()
    yield engine
    await engine.dispose()


def test_get_database_url_default(monkeypatch):
    """Returns fallback URL when DATABASE_URL not set."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    url = get_database_url()
    assert "postgresql" in url


def test_get_database_url_from_env(monkeypatch):
    """Returns DATABASE_URL from environment."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pw@host/db")
    assert get_database_url() == "postgresql+asyncpg://user:pw@host/db"


async def test_engine_connects(pg_engine):
    """Engine can open a connection and run a query."""
    async with pg_engine.connect() as conn:
        result = await conn.execute(text("SELECT 1 AS val"))
        row = result.mappings().one()
        assert row["val"] == 1


async def test_get_db_yields_async_connection(pg_engine, monkeypatch):
    """get_db() yields an AsyncConnection."""
    from wslcb_licensing_tracker.database import get_db

    async with get_db(pg_engine) as conn:
        assert isinstance(conn, AsyncConnection)
        result = await conn.execute(text("SELECT 42 AS answer"))
        row = result.mappings().one()
        assert row["answer"] == 42
