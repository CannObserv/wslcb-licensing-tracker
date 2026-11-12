"""Async database engine and connection management for PostgreSQL.

Provides an AsyncEngine with connection pooling and a get_db() async
context manager for use as a FastAPI dependency or standalone async
caller.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from .config import DATABASE_URL_ENV, env

logger = logging.getLogger(__name__)

_DEFAULT_DATABASE_URL = "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"


def get_database_url() -> str:
    """Return ``DATABASE_URL_ENV``, else the localhost default.

    The default is the prod/dev ``wslcb``: the CLI on the prod host runs unset.
    """
    return env(DATABASE_URL_ENV, _DEFAULT_DATABASE_URL)  # set but empty is not the default


def create_engine_from_env(
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
) -> AsyncEngine:
    """Create an AsyncEngine from ``get_database_url()``.

    Call once at application startup; store on ``app.state.engine``.
    """
    url = get_database_url()
    try:
        engine = create_async_engine(url, pool_size=pool_size, max_overflow=max_overflow, echo=echo)
    except ArgumentError as exc:
        # Never the URL or the message, which quotes it: it carries the password.
        logger.critical("invalid %s: %s", DATABASE_URL_ENV, type(exc).__name__)
        raise
    logger.debug("created async engine: %s", engine.url)  # str(URL) masks the password
    return engine


@asynccontextmanager
async def get_db(engine: AsyncEngine) -> AsyncGenerator[AsyncConnection, None]:
    """Yield an AsyncConnection from the pool.

    Usage as FastAPI dependency (in app.py)::

        async def get_db_dep(request: Request):
            async with get_db(request.app.state.engine) as conn:
                yield conn

        # Then use with: conn: AsyncConnection = Depends(get_db_dep)

    Usage in non-web code::

        engine = create_engine_from_env()
        async with get_db(engine) as conn:
            result = await conn.execute(select(license_records))
    """
    async with engine.connect() as conn:
        yield conn
