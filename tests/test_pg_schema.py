"""Tests for PostgreSQL schema initialization and introspection helpers.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
"""

import pytest

from wslcb_licensing_tracker.pg_schema import _column_exists, _table_exists, init_db


@pytest.mark.asyncio(loop_scope="session")
async def test_init_db_creates_tables(pg_engine):
    """init_db runs Alembic migrations and creates all expected tables."""
    # pg_engine fixture already ran init_db via Alembic, so we just verify
    # the function is importable and callable (idempotent — safe to call again)
    await init_db(pg_engine)

    # Verify key tables exist
    from sqlalchemy import text

    async with pg_engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ))
        tables = {row[0] for row in result}

    assert "license_records" in tables
    assert "locations" in tables
    assert "sources" in tables
    assert "record_sources" in tables


@pytest.mark.asyncio(loop_scope="session")
async def test_table_exists_true(pg_conn):
    """_table_exists returns True for an existing table."""
    assert await _table_exists(pg_conn, "license_records") is True


@pytest.mark.asyncio(loop_scope="session")
async def test_table_exists_false(pg_conn):
    """_table_exists returns False for a non-existent table."""
    assert await _table_exists(pg_conn, "no_such_table_xyz") is False


@pytest.mark.asyncio(loop_scope="session")
async def test_column_exists_true(pg_conn):
    """_column_exists returns True for an existing column."""
    assert await _column_exists(pg_conn, "license_records", "business_name") is True


@pytest.mark.asyncio(loop_scope="session")
async def test_column_exists_false_column(pg_conn):
    """_column_exists returns False for a non-existent column."""
    assert await _column_exists(pg_conn, "license_records", "no_such_column") is False


@pytest.mark.asyncio(loop_scope="session")
async def test_column_exists_false_table(pg_conn):
    """_column_exists returns False when the table doesn't exist."""
    assert await _column_exists(pg_conn, "no_such_table", "id") is False
