"""Tests for PostgreSQL database helpers (pg_db.py).

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
"""

import pytest

from wslcb_licensing_tracker.pg_db import get_or_create_location


class TestPgGetOrCreateLocation:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_new_location(self, pg_conn):
        """Creates a new location and returns its ID."""
        loc_id = await get_or_create_location(pg_conn, "123 MAIN ST, SEATTLE, WA 98101")
        assert loc_id is not None
        assert isinstance(loc_id, int)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_same_id_for_duplicate(self, pg_conn):
        """Returns existing ID when same raw_address is inserted again."""
        id1 = await get_or_create_location(pg_conn, "456 ELM AVE, TACOMA, WA 98402")
        id2 = await get_or_create_location(pg_conn, "456 ELM AVE, TACOMA, WA 98402")
        assert id1 == id2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_for_empty(self, pg_conn):
        """Returns None for empty/None/whitespace-only addresses."""
        assert await get_or_create_location(pg_conn, "") is None
        assert await get_or_create_location(pg_conn, None) is None
        assert await get_or_create_location(pg_conn, "   ") is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_nbsp_normalization(self, pg_conn):
        """Non-breaking spaces are normalized to regular spaces."""
        id1 = await get_or_create_location(pg_conn, "100\xa0MAIN\xa0ST")
        id2 = await get_or_create_location(pg_conn, "100 MAIN ST")
        assert id1 == id2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stores_city_state_zip(self, pg_conn):
        """City, state, and zip_code are stored correctly."""
        from sqlalchemy import select
        from wslcb_licensing_tracker.models import locations

        loc_id = await get_or_create_location(
            pg_conn, "789 OAK DR, OLYMPIA, WA 98501",
            city="OLYMPIA", state="WA", zip_code="98501",
        )
        result = await pg_conn.execute(
            select(locations.c.city, locations.c.state, locations.c.zip_code)
            .where(locations.c.id == loc_id)
        )
        row = result.one()
        assert row.city == "OLYMPIA"
        assert row.state == "WA"
        assert row.zip_code == "98501"
