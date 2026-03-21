"""Tests for pg_integrity.py — async PostgreSQL integrity check functions."""

import os

import pytest
import pytest_asyncio

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)

from wslcb_licensing_tracker.pg_integrity import (
    check_endorsement_anomalies,
    check_orphaned_locations,
    check_unenriched_records,
)


@pytest.fixture
async def conn():
    """Async DB connection from TEST_DATABASE_URL."""
    from sqlalchemy.ext.asyncio import create_async_engine

    url = os.environ["TEST_DATABASE_URL"]
    engine = create_async_engine(url)
    async with engine.connect() as c:
        yield c
    await engine.dispose()


@pytest.mark.asyncio
async def test_check_orphaned_locations_returns_dict(conn):
    result = await check_orphaned_locations(conn)
    assert "count" in result
    assert "details" in result
    assert isinstance(result["count"], int)
    assert isinstance(result["details"], list)


@pytest.mark.asyncio
async def test_check_unenriched_records_returns_dict(conn):
    result = await check_unenriched_records(conn)
    for key in ("no_endorsements", "no_entities", "no_provenance", "no_enrichment_tracking"):
        assert key in result
        assert isinstance(result[key], int)


@pytest.mark.asyncio
async def test_check_endorsement_anomalies_returns_dict(conn):
    result = await check_endorsement_anomalies(conn)
    for key in ("unresolved_codes", "placeholder_endorsements"):
        assert key in result
        assert isinstance(result[key], int)
