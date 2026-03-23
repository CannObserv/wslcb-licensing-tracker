"""Tests for pg_address_validator.py — async address validation DB layer."""

from unittest.mock import patch

import pytest
from sqlalchemy import select

from wslcb_licensing_tracker.models import locations
from wslcb_licensing_tracker.pg_address_validator import standardize_location, validate_location
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
        row = (
            await pg_conn.execute(select(locations.c.std_city).where(locations.c.id == loc_id))
        ).scalar_one()
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


class TestValidateLocation:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_when_validation_disabled(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "456 OAK AVE, SPOKANE, WA 99201")
        with patch(
            "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
            return_value=False,
        ):
            result = await validate_location(pg_conn, loc_id, "456 OAK AVE, SPOKANE, WA 99201")
        assert result is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_writes_address_validated_at_on_confirmed(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "789 PINE ST, TACOMA, WA 98401")
        mock_result = {
            "address_line_1": "789 PINE ST",
            "address_line_2": "",
            "city": "TACOMA",
            "region": "WA",
            "postal_code": "98401",
            "country": "US",
            "validated": "789 PINE ST, TACOMA WA 98401",
            "latitude": 47.2529,
            "longitude": -122.4443,
            "validation": {"status": "confirmed", "dpv_match_code": "Y"},
        }
        with (
            patch(
                "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.pg_address_validator.validate", return_value=mock_result
            ),
        ):
            result = await validate_location(pg_conn, loc_id, "789 PINE ST, TACOMA, WA 98401")
        assert result is True
        row = (
            await pg_conn.execute(
                select(locations.c.address_validated_at).where(locations.c.id == loc_id)
            )
        ).scalar_one()
        assert row is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_api_error(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "UNVALIDATABLE ADDRESS")
        with (
            patch(
                "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
                return_value=True,
            ),
            patch("wslcb_licensing_tracker.pg_address_validator.validate", return_value=None),
        ):
            result = await validate_location(pg_conn, loc_id, "UNVALIDATABLE ADDRESS")
        assert result is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_not_confirmed_writes_status_but_not_validated_at(self, pg_conn):
        # API responds but cannot confirm the address (no address_line_1 in response).
        # Should write validation_status/dpv_match_code, leave address_validated_at NULL,
        # and return False.
        loc_id = await get_or_create_location(pg_conn, "AMBIGUOUS RD, NOWHERE, WA 99999")
        mock_result = {
            "validation": {"status": "not_confirmed", "dpv_match_code": "N"},
        }
        with (
            patch(
                "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.pg_address_validator.validate", return_value=mock_result
            ),
        ):
            result = await validate_location(pg_conn, loc_id, "AMBIGUOUS RD, NOWHERE, WA 99999")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(locations.c.validation_status, locations.c.address_validated_at).where(
                        locations.c.id == loc_id
                    )
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] == "not_confirmed"
        assert row["address_validated_at"] is None
