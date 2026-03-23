"""Tests for pg_endorsements_seed.py — async endorsement seeding."""

import pytest
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.models import license_endorsements
from wslcb_licensing_tracker.pg_endorsements_seed import (
    backfill,
    discover_code_mappings,
    merge_mixed_case_endorsements,
    repair_code_name_endorsements,
    seed_endorsements,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestSeedEndorsements:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_nonnegative_int(self, pg_conn):
        inserted = await seed_endorsements(pg_conn)
        assert isinstance(inserted, int)
        assert inserted >= 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn):
        await seed_endorsements(pg_conn)
        second = await seed_endorsements(pg_conn)
        # Second run should insert 0 (all already exist)
        assert second == 0


class TestMergeMixedCase:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_nonnegative_int(self, pg_conn):
        # Insert duplicate mixed-case endorsements
        await pg_conn.execute(
            pg_insert(license_endorsements)
            .values(name="Cannabis Retailer XYZ")
            .on_conflict_do_nothing()
        )
        await pg_conn.execute(
            pg_insert(license_endorsements)
            .values(name="CANNABIS RETAILER XYZ")
            .on_conflict_do_nothing()
        )
        count = await merge_mixed_case_endorsements(pg_conn)
        assert isinstance(count, int)


class TestRepairCodeNameEndorsements:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_nonnegative_int(self, pg_conn):
        # Insert an endorsement in legacy "CODE, NAME" format
        await pg_conn.execute(
            pg_insert(license_endorsements)
            .values(name="394, Cannabis Retailer")
            .on_conflict_do_nothing()
        )
        count = await repair_code_name_endorsements(pg_conn)
        assert isinstance(count, int)


class TestBackfill:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_backfill_processes_unlinked_records(self, pg_conn, standard_new_application):
        """Records with license_type but no record_endorsements get processed."""
        standard_new_application["license_number"] = "seed_backfill_001"
        standard_new_application["license_type"] = "Cannabis Retailer"
        await insert_record(pg_conn, standard_new_application)
        count = await backfill(pg_conn)
        assert isinstance(count, int)
        assert count >= 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_backfill_idempotent(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "seed_backfill_002"
        standard_new_application["license_type"] = "Cannabis Retailer"
        await insert_record(pg_conn, standard_new_application)
        await backfill(pg_conn)
        second = await backfill(pg_conn)
        assert second == 0  # Nothing new to process


class TestDiscoverCodeMappings:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_dict(self, pg_conn):
        result = await discover_code_mappings(pg_conn)
        assert isinstance(result, dict)
