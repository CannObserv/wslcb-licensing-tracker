"""PostgreSQL schema initialization and introspection helpers.

Uses Alembic to run migrations. Replaces the SQLite PRAGMA user_version
migration framework in schema.py.
"""

import logging

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)


async def init_db(engine: AsyncEngine) -> None:
    """Run all pending Alembic migrations.

    Idempotent — safe to call on every startup. Alembic tracks applied
    revisions in the ``alembic_version`` table and skips already-applied
    migrations.
    """

    def _run_upgrade(connection: object) -> None:
        cfg = Config("alembic.ini")
        cfg.attributes["connection"] = connection
        command.upgrade(cfg, "head")

    async with engine.connect() as conn:
        await conn.run_sync(_run_upgrade)
        await conn.commit()


async def _table_exists(conn: AsyncConnection, name: str) -> bool:
    """Return True if *name* is a table in the public schema."""
    result = await conn.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = :name"
        ),
        {"name": name},
    )
    return result.first() is not None


async def _column_exists(conn: AsyncConnection, table: str, column: str) -> bool:
    """Return True if *column* exists on *table*. Returns False when *table* is absent."""
    result = await conn.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :table AND column_name = :column"
        ),
        {"table": table, "column": column},
    )
    return result.first() is not None
