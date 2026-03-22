"""Tests for pg_endorsements_admin.py — async endorsement admin helpers."""
import pytest
from wslcb_licensing_tracker.pg_endorsements_admin import (
    endorsement_similarity,
    get_endorsement_list,
    get_code_mappings,
    add_code_mapping,
    remove_code_mapping,
    create_code,
    suggest_duplicate_endorsements,
    dismiss_suggestion,
)
from wslcb_licensing_tracker.pg_endorsements import ensure_endorsement


class TestEndorsementSimilarity:
    def test_identical_names(self):
        assert endorsement_similarity("CANNABIS RETAILER", "CANNABIS RETAILER") == 1.0

    def test_different_names(self):
        assert endorsement_similarity("SPIRITS RETAILER", "BEER DISTRIBUTOR") < 0.5


class TestGetEndorsementList:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_list(self, pg_conn):
        await ensure_endorsement(pg_conn, "ADMIN TEST ENDORSEMENT")
        result = await get_endorsement_list(pg_conn)
        assert isinstance(result, list)
        assert len(result) > 0
        assert "id" in result[0] and "name" in result[0]


class TestCodeMappingCrud:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_add_and_remove_code_mapping(self, pg_conn):
        eid = await ensure_endorsement(pg_conn, "CODE MAPPING TEST")
        await add_code_mapping(pg_conn, "9999", eid)
        mappings = await get_code_mappings(pg_conn)
        codes = [m["code"] for m in mappings]
        assert "9999" in codes
        await remove_code_mapping(pg_conn, "9999")
        mappings2 = await get_code_mappings(pg_conn)
        assert "9999" not in [m["code"] for m in mappings2]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_code(self, pg_conn):
        await create_code(pg_conn, "9998", "CREATED CODE TEST")
        mappings = await get_code_mappings(pg_conn)
        assert "9998" in [m["code"] for m in mappings]
