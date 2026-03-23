"""Tests for pg_entities.py — async entity normalization."""

import pytest

from wslcb_licensing_tracker.pg_entities import (
    get_or_create_entity,
    get_record_entities,
    merge_duplicate_entities,
    parse_and_link_entities,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestGetOrCreateEntity:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_and_returns_id(self, pg_conn):
        eid = await get_or_create_entity(pg_conn, "john doe")
        assert isinstance(eid, int)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn):
        eid1 = await get_or_create_entity(pg_conn, "JANE SMITH")
        eid2 = await get_or_create_entity(pg_conn, "jane smith")
        assert eid1 == eid2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_raises_on_empty_name(self, pg_conn):
        with pytest.raises(ValueError):
            await get_or_create_entity(pg_conn, "")


class TestParseAndLinkEntities:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_links_applicants(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_001"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        count = await parse_and_link_entities(
            pg_conn,
            record_id,
            "ACME CANNABIS CO; JOHN DOE; JANE SMITH",
            role="applicant",
        )
        # Skips first element (business name), links JOHN DOE + JANE SMITH
        assert count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_record_entities(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_002"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await parse_and_link_entities(
            pg_conn,
            record_id,
            "ACME CANNABIS CO; JOHN DOE",
            role="applicant",
        )
        entity_map = await get_record_entities(pg_conn, [record_id])
        assert record_id in entity_map
        applicants = entity_map[record_id].get("applicant", [])
        assert len(applicants) >= 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_003"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await parse_and_link_entities(pg_conn, record_id, "JOHN DOE; JANE SMITH", role="applicant")
        await parse_and_link_entities(
            pg_conn,
            record_id,
            "JOHN DOE; JANE SMITH",
            role="applicant",
            delete_existing=True,
        )
        entity_map = await get_record_entities(pg_conn, [record_id])
        applicants = entity_map[record_id].get("applicant", [])
        # First element ("JOHN DOE") is the business name and is skipped;
        # only JANE SMITH is linked. Idempotency: should be exactly 1, not 2.
        assert len(applicants) == 1


class TestMergeDuplicateEntities:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_int(self, pg_conn):
        count = await merge_duplicate_entities(pg_conn)
        assert isinstance(count, int)
        assert count >= 0
