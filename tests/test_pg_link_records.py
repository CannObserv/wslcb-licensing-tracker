"""Tests for pg_link_records.py — async application-outcome linking."""
import pytest
from wslcb_licensing_tracker.pg_db import outcome_filter_sql
from wslcb_licensing_tracker.pg_link_records import (
    build_all_links,
    link_new_record,
    get_outcome_status,
    get_record_links_bulk,
    get_reverse_link_info,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestGetOutcomeStatus:
    """Pure Python — no DB needed."""

    def test_non_new_application_returns_none_status(self):
        record = {"section_type": "approved", "application_type": "NEW APPLICATION"}
        assert get_outcome_status(record, None)["status"] is None

    def test_non_linkable_type(self):
        record = {"section_type": "new_application", "application_type": "EXTENSION"}
        assert get_outcome_status(record, None)["status"] is None


class TestOutcomeFilterSql:
    """Pure Python — no DB needed."""

    def test_approved_filter(self):
        frags = outcome_filter_sql("approved")
        assert len(frags) >= 1
        assert any("approved" in f for f in frags)

    def test_unknown_status_returns_empty(self):
        assert outcome_filter_sql("nonexistent") == []


class TestGetRecordLinksBulk:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_dict_keyed_by_new_app_id(self, pg_conn):
        result = await get_record_links_bulk(pg_conn, [])
        assert result == {}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_link_for_known_ids(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "bulk_link_001"
        standard_new_application["record_date"] = "2025-01-10"
        new_app = dict(standard_new_application)
        new_app_id, _ = await insert_record(pg_conn, new_app)

        approved = dict(standard_new_application)
        approved["section_type"] = "approved"
        approved["record_date"] = "2025-01-15"
        approved["license_number"] = "bulk_link_001"
        await insert_record(pg_conn, approved)
        await build_all_links(pg_conn)

        result = await get_record_links_bulk(pg_conn, [new_app_id])
        assert new_app_id in result


class TestBuildAllLinks:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_summary_dict(self, pg_conn):
        result = await build_all_links(pg_conn)
        assert "high" in result and "medium" in result and "total" in result
        assert result["total"] == result["high"] + result["medium"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_links_matching_records(self, pg_conn, standard_new_application):
        # Insert a new_application
        standard_new_application["license_number"] = "link_001"
        standard_new_application["record_date"] = "2025-01-10"
        new_app = dict(standard_new_application)
        await insert_record(pg_conn, new_app)

        # Insert matching approved record
        approved = dict(standard_new_application)
        approved["section_type"] = "approved"
        approved["record_date"] = "2025-01-15"
        approved["license_number"] = "link_001"
        await insert_record(pg_conn, approved)

        result = await build_all_links(pg_conn)
        assert result["total"] >= 1
