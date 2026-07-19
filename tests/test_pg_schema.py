"""Tests for PostgreSQL schema initialization.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
init_db is also exercised by the pg_engine fixture itself, which calls it
to migrate the test database.
"""

import pytest
from sqlalchemy import text

from wslcb_licensing_tracker.pg_schema import init_db


@pytest.mark.asyncio(loop_scope="session")
async def test_init_db_creates_tables(pg_engine):
    """init_db runs Alembic migrations, is idempotent, and creates all expected tables."""
    # pg_engine already ran init_db once — calling again must be a no-op.
    await init_db(pg_engine)

    async with pg_engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            )
        )
        tables = {row[0] for row in result}

    assert "license_records" in tables
    assert "locations" in tables
    assert "sources" in tables
    assert "record_sources" in tables
