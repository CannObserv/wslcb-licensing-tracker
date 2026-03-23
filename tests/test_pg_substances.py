"""Tests for pg_substances.py — async regulated substance CRUD."""

import pytest
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.models import license_endorsements
from wslcb_licensing_tracker.pg_substances import (
    add_substance,
    get_regulated_substances,
    get_substance_endorsement_ids,
    remove_substance,
    set_substance_endorsements,
)


class TestAddSubstance:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_int_id(self, pg_conn):
        sid = await add_substance(pg_conn, "TEST SUBSTANCE", 10)
        assert isinstance(sid, int) and sid > 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_regulated_substances_includes_new(self, pg_conn):
        await add_substance(pg_conn, "CANNABIS FLOWER", 1)
        results = await get_regulated_substances(pg_conn)
        names = [r["name"] for r in results]
        assert "CANNABIS FLOWER" in names

    @pytest.mark.asyncio(loop_scope="session")
    async def test_remove_returns_name(self, pg_conn):
        sid = await add_substance(pg_conn, "TO REMOVE", 99)
        name = await remove_substance(pg_conn, sid)
        assert name == "TO REMOVE"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_set_and_get_endorsement_ids(self, pg_conn):
        # Create an endorsement row directly
        eid = (
            await pg_conn.execute(
                pg_insert(license_endorsements)
                .values(name="TEST ENDORSEMENT X")
                .on_conflict_do_nothing()
                .returning(license_endorsements.c.id)
            )
        ).scalar_one()
        sid = await add_substance(pg_conn, "SUBSTANCE WITH ENDORSEMENT", 5)
        await set_substance_endorsements(pg_conn, sid, [eid])
        ids = await get_substance_endorsement_ids(pg_conn, sid)
        assert eid in ids
