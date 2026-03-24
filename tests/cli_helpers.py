"""Shared helpers for CLI test modules."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock


def mock_async_engine():
    """Return a MagicMock engine whose dispose() is awaitable."""
    engine = MagicMock()
    engine.dispose = AsyncMock()
    return engine


@asynccontextmanager
async def _mock_get_db_ctx(*_a, **_kw):
    conn = AsyncMock()
    yield conn


def mock_get_db(*_args, **_kwargs):
    """Return an async context manager yielding an AsyncMock connection."""
    return _mock_get_db_ctx()
