"""Data migration framework for the WSLCB licensing tracker (PostgreSQL).

Tracks one-time data-only migrations in the ``data_migrations`` table.
Replaces unconditional startup repair calls in the FastAPI lifespan.

Each migration runs exactly once per database — subsequent runs are
no-ops (idempotent). Migrations run in registration order.

Usage::

    engine = create_engine_from_env()
    await run_pending_migrations(engine)
"""

import logging
from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .models import data_migrations
from .pg_endorsements_seed import (
    backfill,
    merge_mixed_case_endorsements,
    repair_code_name_endorsements,
    seed_endorsements,
)
from .pg_entities import backfill_entities
from .pg_link_records import build_all_links

logger = logging.getLogger(__name__)


async def _run_build_all_links(conn: AsyncConnection) -> None:
    """Wrapper for build_all_links that logs stats and discards the return value."""
    stats = await build_all_links(conn)
    if stats.get("total"):
        logger.info(
            "Record linking: %d links (%d high, %d medium)",
            stats["total"],
            stats.get("high", 0),
            stats.get("medium", 0),
        )


_MIGRATIONS: list[tuple[str, Callable]] = [
    ("0001_seed_endorsements", seed_endorsements),
    ("0002_repair_code_name_endorsements", repair_code_name_endorsements),
    ("0003_merge_mixed_case_endorsements", merge_mixed_case_endorsements),
    ("0004_backfill_endorsements", backfill),
    ("0005_backfill_entities", backfill_entities),
    ("0006_build_record_links", _run_build_all_links),
]


async def run_pending_migrations(engine: AsyncEngine) -> None:
    """Run any data migrations that have not yet been applied.

    Each migration runs exactly once per database. Already-applied
    migrations are skipped. Runs migrations in registration order.

    Raises on the first migration failure (does not suppress).
    """
    async with engine.connect() as conn:
        applied_rows = (await conn.execute(select(data_migrations.c.name))).fetchall()
        applied = {row[0] for row in applied_rows}

    for name, fn in _MIGRATIONS:
        if name in applied:
            logger.debug("Data migration %r already applied — skipping", name)
            continue

        logger.info("Running data migration: %r", name)
        try:
            async with engine.connect() as conn:
                await fn(conn)
                await conn.execute(
                    pg_insert(data_migrations)
                    .values(name=name)
                    .on_conflict_do_nothing(index_elements=["name"])
                )
                await conn.commit()
        except Exception:
            logger.exception("Data migration %r failed", name)
            raise

        logger.info("%r complete", name)
