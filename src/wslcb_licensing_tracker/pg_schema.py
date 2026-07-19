"""PostgreSQL schema initialization: run Alembic migrations programmatically."""

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)


async def init_db(engine: AsyncEngine) -> None:
    """Run all pending Alembic migrations.

    Idempotent — safe to call on every startup. Alembic tracks applied
    revisions in the ``alembic_version`` table and skips already-applied
    migrations.
    """

    def _run_upgrade(connection: Connection) -> None:
        cfg = Config(str(Path(__file__).parent.parent.parent / "alembic.ini"))
        cfg.attributes["connection"] = connection
        command.upgrade(cfg, "head")

    logger.debug("Running Alembic migrations")
    async with engine.connect() as conn:
        await conn.run_sync(_run_upgrade)
        await conn.commit()
    logger.info("Database migrations complete")
