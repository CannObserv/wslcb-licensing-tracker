"""Tests for async database engine and connection management.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
Tests are skipped automatically when the env var is absent.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from wslcb_licensing_tracker.database import create_engine_from_env, get_database_url, get_db


def test_get_database_url_default(monkeypatch):
    """Returns fallback URL when DATABASE_URL not set."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    url = get_database_url()
    assert "postgresql" in url


def test_get_database_url_from_env(monkeypatch):
    """Returns DATABASE_URL from environment."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pw@host/db")
    assert get_database_url() == "postgresql+asyncpg://user:pw@host/db"


@pytest.mark.asyncio(loop_scope="session")
async def test_engine_connects(pg_engine):
    """Engine can open a connection and run a query."""
    async with pg_engine.connect() as conn:
        result = await conn.execute(text("SELECT 1 AS val"))
        row = result.mappings().one()
        assert row["val"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_get_db_yields_async_connection(pg_engine, monkeypatch):
    """get_db() yields an AsyncConnection."""
    async with get_db(pg_engine) as conn:
        assert isinstance(conn, AsyncConnection)
        result = await conn.execute(text("SELECT 42 AS answer"))
        row = result.mappings().one()
        assert row["answer"] == 42


@pytest.mark.asyncio(loop_scope="session")
async def test_alembic_baseline_creates_all_tables(pg_engine):
    """After running the baseline migration, all 20 app tables + alembic_version exist."""
    # Migrations already applied by pg_engine fixture — just verify the tables exist
    async with pg_engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ))
        table_names = {row[0] for row in result}

    count = len(table_names)
    # 20 app tables + 1 alembic_version
    assert count == 21

    EXPECTED_TABLES = {
        "locations", "license_endorsements", "endorsement_codes", "scrape_log",
        "source_types", "sources", "license_records", "record_endorsements",
        "entities", "record_entities", "record_links", "record_enrichments",
        "record_sources", "admin_users", "admin_audit_log", "endorsement_aliases",
        "endorsement_dismissed_suggestions", "regulated_substances",
        "regulated_substance_endorsements", "data_migrations", "alembic_version",
    }
    assert table_names == EXPECTED_TABLES
