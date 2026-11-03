"""Tests for async database engine and connection management.

Requires ``config.TEST_DATABASE_URL_ENV`` pointing at a running PostgreSQL instance.
Tests are skipped automatically when the env var is absent.
"""

import logging

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncConnection

from wslcb_licensing_tracker.config import DATABASE_URL_ENV
from wslcb_licensing_tracker.engine import create_engine_from_env, get_database_url, get_db

PROD = "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"


@pytest.fixture
def clean(monkeypatch):
    monkeypatch.delenv(DATABASE_URL_ENV, raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    return monkeypatch


def test_get_database_url_default(clean):
    """Unset, the localhost default: the CLI on the prod host relies on it."""
    assert get_database_url() == PROD


def test_get_database_url_from_env(clean):
    clean.setenv(DATABASE_URL_ENV, "postgresql+asyncpg://user:pw@host/db")
    assert get_database_url() == "postgresql+asyncpg://user:pw@host/db"


def test_get_database_url_new_name_beats_legacy(clean):
    """No-prod guard (#204): a legacy prod URL can't override the new name."""
    clean.setenv(DATABASE_URL_ENV, "postgresql+asyncpg://user:pw@host/db_test")
    clean.setenv("DATABASE_URL", PROD)
    assert get_database_url().endswith("/db_test")


def test_get_database_url_legacy_fallback(clean):
    clean.setenv("DATABASE_URL", "postgresql+asyncpg://user:pw@host/legacy")
    assert get_database_url().endswith("/legacy")


def test_get_database_url_set_but_empty_is_not_the_default(clean):
    """No-prod guard (#204): an empty value must not become the prod default."""
    clean.setenv(DATABASE_URL_ENV, "")
    assert get_database_url() == ""


def test_engine_logs_never_carry_the_password(clean, caplog):
    clean.setenv(DATABASE_URL_ENV, "postgresql+asyncpg://u:s3cret@host/db")
    with caplog.at_level(logging.DEBUG, logger="wslcb_licensing_tracker.engine"):
        create_engine_from_env()
    clean.setenv(DATABASE_URL_ENV, "not a url s3cret")
    with (
        caplog.at_level(logging.DEBUG, logger="wslcb_licensing_tracker.engine"),
        pytest.raises(ArgumentError),
    ):
        create_engine_from_env()
    assert caplog.records
    assert "s3cret" not in caplog.text


def test_pg_engine_targets_the_test_database(pg_engine, pg_url):
    """No-prod guard (#204): the session engine is the test DB, never prod's."""
    assert pg_engine.url.database == make_url(pg_url).database
    assert pg_engine.url.database != make_url(PROD).database


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
    """After running all migrations, all 23 app tables + alembic_version exist."""
    # Migrations already applied by pg_engine fixture — just verify the tables exist
    async with pg_engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            )
        )
        table_names = {row[0] for row in result}

    count = len(table_names)
    # 23 app tables + 1 alembic_version
    assert count == 24

    EXPECTED_TABLES = {
        "locations",
        "license_endorsements",
        "endorsement_codes",
        "scrape_log",
        "source_types",
        "sources",
        "license_records",
        "record_endorsements",
        "entities",
        "record_entities",
        "record_links",
        "record_enrichments",
        "record_sources",
        "admin_users",
        "admin_audit_log",
        "endorsement_aliases",
        "endorsement_dismissed_suggestions",
        "regulated_substances",
        "regulated_substance_endorsements",
        "data_migrations",
        "entity_classifications",
        "entity_review_flags",
        "classification_runs",
        "alembic_version",
    }
    assert table_names == EXPECTED_TABLES
