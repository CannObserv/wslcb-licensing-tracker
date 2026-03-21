"""Tests for pg_address_validator.py — async address validation DB layer."""
import pytest
from unittest.mock import patch
from sqlalchemy import select
from wslcb_licensing_tracker.models import locations
from wslcb_licensing_tracker.pg_address_validator import standardize_location
from wslcb_licensing_tracker.pg_db import get_or_create_location


class TestStandardizeLocation:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_updates_std_columns_on_success(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "123 MAIN ST, SEATTLE, WA 98101")
        mock_result = {
            "std_address_line_1": "123 MAIN ST",
            "std_address_line_2": "",
            "std_city": "SEATTLE",
            "std_region": "WA",
            "std_postal_code": "98101",
            "std_country": "US",
        }
        with patch(
            "wslcb_licensing_tracker.pg_address_validator.standardize",
            return_value=mock_result,
        ):
            result = await standardize_location(pg_conn, loc_id, "123 MAIN ST, SEATTLE, WA 98101")
        assert result is True
        row = (await pg_conn.execute(
            select(locations.c.std_city).where(locations.c.id == loc_id)
        )).scalar_one()
        assert row == "SEATTLE"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_api_error(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "BAD ADDRESS ONLY")
        with patch(
            "wslcb_licensing_tracker.pg_address_validator.standardize",
            return_value=None,
        ):
            result = await standardize_location(pg_conn, loc_id, "BAD ADDRESS ONLY")
        assert result is False
