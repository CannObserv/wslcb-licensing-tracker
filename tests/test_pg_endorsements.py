"""Tests for pg_endorsements.py — async endorsement pipeline."""

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.models import endorsement_codes, license_endorsements
from wslcb_licensing_tracker.pg_endorsements import (
    ensure_endorsement,
    get_endorsement_groups,
    get_endorsement_options,
    get_record_endorsements,
    process_record,
    remove_alias,
    rename_endorsement,
    resolve_endorsement,
    set_canonical_endorsement,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestEnsureEndorsement:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_and_returns_id(self, pg_conn):
        eid = await ensure_endorsement(pg_conn, "cannabis retailer")
        assert isinstance(eid, int)
        # Second call returns same id (idempotent)
        eid2 = await ensure_endorsement(pg_conn, "CANNABIS RETAILER")
        assert eid == eid2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_uppercases_name(self, pg_conn):
        eid = await ensure_endorsement(pg_conn, "spirits store")
        row = (
            await pg_conn.execute(
                select(license_endorsements.c.name).where(license_endorsements.c.id == eid)
            )
        ).scalar_one()
        assert row == "SPIRITS STORE"


class TestProcessRecord:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_text_semicolon_format(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "endorse_001"
        standard_new_application["license_type"] = "CANNABIS RETAILER; DELIVERY"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        linked = await process_record(pg_conn, record_id, "CANNABIS RETAILER; DELIVERY")
        assert linked == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_numeric_code_format(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "endorse_002"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        # Seed a code mapping first
        eid = await ensure_endorsement(pg_conn, "GROCERY STORE - BEER/WINE")
        await pg_conn.execute(
            pg_insert(endorsement_codes)
            .values(code="450", endorsement_id=eid)
            .on_conflict_do_nothing()
        )
        linked = await process_record(pg_conn, record_id, "450,")
        assert linked >= 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "endorse_003"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await process_record(pg_conn, record_id, "SPIRITS RETAILER")
        await process_record(pg_conn, record_id, "SPIRITS RETAILER")
        # Calling twice should not duplicate rows
        endorsements = await get_record_endorsements(pg_conn, [record_id])
        assert endorsements[record_id].count("SPIRITS RETAILER") == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_code_name_format(self, pg_conn, standard_new_application):
        """Test CODE, NAME format creates endorsement and maps the code."""
        standard_new_application["license_number"] = "endorse_004"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        linked = await process_record(pg_conn, record_id, "450, GROCERY STORE - BEER/WINE")
        assert linked == 1
        endorsements = await get_record_endorsements(pg_conn, [record_id])
        assert "GROCERY STORE - BEER/WINE" in endorsements[record_id]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_empty_returns_zero(self, pg_conn, standard_new_application):
        """Empty license_type returns 0 and leaves no links."""
        standard_new_application["license_number"] = "endorse_005"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        linked = await process_record(pg_conn, record_id, "")
        assert linked == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_unknown_numeric_creates_placeholder(self, pg_conn, standard_new_application):
        """Unknown numeric code with no mapping creates a placeholder endorsement."""
        standard_new_application["license_number"] = "endorse_006"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        linked = await process_record(pg_conn, record_id, "9999,")
        assert linked == 1
        endorsements = await get_record_endorsements(pg_conn, [record_id])
        # Placeholder endorsement named "9999"
        assert "9999" in endorsements[record_id]


class TestAliasManagement:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_resolve_with_alias(self, pg_conn):
        variant_id = await ensure_endorsement(pg_conn, "BEER AND WINE VARIANT")
        canonical_id = await ensure_endorsement(pg_conn, "BEER AND WINE CANONICAL")
        await set_canonical_endorsement(pg_conn, canonical_id, [variant_id])
        resolved = await resolve_endorsement(pg_conn, variant_id)
        assert resolved == canonical_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_remove_alias(self, pg_conn):
        v_id = await ensure_endorsement(pg_conn, "REMOVE ALIAS VARIANT")
        c_id = await ensure_endorsement(pg_conn, "REMOVE ALIAS CANONICAL")
        await set_canonical_endorsement(pg_conn, c_id, [v_id])
        await remove_alias(pg_conn, v_id)
        resolved = await resolve_endorsement(pg_conn, v_id)
        assert resolved == v_id  # back to self

    @pytest.mark.asyncio(loop_scope="session")
    async def test_remove_alias_raises_when_none(self, pg_conn):
        """remove_alias raises ValueError when no alias exists."""
        eid = await ensure_endorsement(pg_conn, "STANDALONE ENDORSEMENT")
        with pytest.raises(ValueError, match="has no alias"):
            await remove_alias(pg_conn, eid)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_resolve_no_alias_returns_self(self, pg_conn):
        eid = await ensure_endorsement(pg_conn, "STANDALONE NO ALIAS")
        resolved = await resolve_endorsement(pg_conn, eid)
        assert resolved == eid

    @pytest.mark.asyncio(loop_scope="session")
    async def test_set_canonical_skips_self(self, pg_conn):
        """set_canonical_endorsement silently skips self-aliasing."""
        eid = await ensure_endorsement(pg_conn, "SELF ALIAS TEST")
        written = await set_canonical_endorsement(pg_conn, eid, [eid])
        assert written == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_rename_endorsement(self, pg_conn):
        """rename_endorsement creates a named canonical and aliases the original."""
        code_eid = await ensure_endorsement(pg_conn, "777")
        canonical_id = await rename_endorsement(pg_conn, code_eid, "SPIRITS WHOLESALE")
        assert canonical_id != code_eid
        resolved = await resolve_endorsement(pg_conn, code_eid)
        assert resolved == canonical_id


class TestGetEndorsementOptions:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_names_for_linked_records(self, pg_conn, standard_new_application):
        """get_endorsement_options returns sorted canonical names."""
        standard_new_application["license_number"] = "opt_001"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await process_record(pg_conn, record_id, "WINE SHOP; BEER STORE")
        options = await get_endorsement_options(pg_conn)
        assert "WINE SHOP" in options
        assert "BEER STORE" in options
        # Sorted
        assert options == sorted(options)


class TestGetEndorsementGroups:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_list(self, pg_conn):
        """get_endorsement_groups returns a list of group dicts."""
        groups = await get_endorsement_groups(pg_conn)
        assert isinstance(groups, list)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_groups_have_required_keys(self, pg_conn, standard_new_application):
        """Each group has 'code' and 'endorsements' keys."""
        standard_new_application["license_number"] = "grp_001"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        eid = await ensure_endorsement(pg_conn, "GROUPED ENDORSEMENT TEST")
        await pg_conn.execute(
            pg_insert(endorsement_codes)
            .values(code="123", endorsement_id=eid)
            .on_conflict_do_nothing()
        )
        await process_record(pg_conn, record_id, "GROUPED ENDORSEMENT TEST")
        groups = await get_endorsement_groups(pg_conn)
        for grp in groups:
            assert "code" in grp
            assert "endorsements" in grp
            for entry in grp["endorsements"]:
                for key in ("id", "name", "record_count", "is_canonical", "is_variant"):
                    assert key in entry
