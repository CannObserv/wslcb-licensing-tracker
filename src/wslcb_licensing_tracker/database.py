"""Async database engine and connection management for PostgreSQL.

Replaces the synchronous SQLite connection layer in db.py. Provides an
AsyncEngine with connection pooling and a get_db() async context manager
for use as a FastAPI dependency or standalone async caller.

db.py is retained for constants and text utilities.
"""

import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

logger = logging.getLogger(__name__)

_DEFAULT_DATABASE_URL = "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"


def get_database_url() -> str:
    """Return DATABASE_URL from environment, with a localhost default."""
    return os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL)


def create_engine_from_env(
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
) -> AsyncEngine:
    """Create an AsyncEngine using DATABASE_URL from the environment.

    Call once at application startup; store on ``app.state.engine``.
    """
    return create_async_engine(
        get_database_url(),
        pool_size=pool_size,
        max_overflow=max_overflow,
        echo=echo,
    )


@asynccontextmanager
async def get_db(engine: AsyncEngine) -> AsyncGenerator[AsyncConnection, None]:
    """Yield an AsyncConnection from the pool.

    Usage as FastAPI dependency::

        async def get_db_dep(request: Request) -> AsyncGenerator[AsyncConnection, None]:
            async with get_db(request.app.state.engine) as conn:
                yield conn

    Usage in non-web code::

        engine = create_engine_from_env()
        async with get_db(engine) as conn:
            result = await conn.execute(select(license_records))
    """
    async with engine.connect() as conn:
        yield conn
