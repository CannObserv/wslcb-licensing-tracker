"""Tests for pg_address_validator.py — async address validation DB layer."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select

from wslcb_licensing_tracker.models import locations
from wslcb_licensing_tracker.pg_address_validator import (
    DEFAULT_RETRY_AFTER,
    MAX_RETRIES,
    _parse_retry_after,
    _post_with_retry,
    standardize,
    standardize_location,
    validate,
    validate_location,
)
from wslcb_licensing_tracker.pg_db import get_or_create_location


class TestStandardizeLocation:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_updates_std_columns_on_success(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "123 MAIN ST, SEATTLE, WA 98101")
        mock_result = {
            "address_line_1": "123 MAIN ST",
            "address_line_2": "",
            "city": "SEATTLE",
            "region": "WA",
            "postal_code": "98101",
            "country": "US",
            "standardized": "123 MAIN ST, SEATTLE WA 98101",
        }
        with patch(
            "wslcb_licensing_tracker.pg_address_validator.standardize",
            return_value=mock_result,
        ):
            result = await standardize_location(pg_conn, loc_id, "123 MAIN ST, SEATTLE, WA 98101")
        assert result is True
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.std_city,
                        locations.c.std_address_string,
                        locations.c.validation_status,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["std_city"] == "SEATTLE"
        assert row["std_address_string"] == "123 MAIN ST, SEATTLE WA 98101"
        assert row["validation_status"] == "standardized"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_api_error(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "BAD ADDRESS ONLY")
        with patch(
            "wslcb_licensing_tracker.pg_address_validator.standardize",
            return_value=None,
        ):
            result = await standardize_location(pg_conn, loc_id, "BAD ADDRESS ONLY")
        assert result is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_sanitizes_country_code(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "456 ELM ST, TACOMA, WA 98401")
        mock_result = {
            "address_line_1": "456 ELM ST",
            "address_line_2": "",
            "city": "TACOMA",
            "region": "WA",
            "postal_code": "98401",
            "country": "United States",  # not ISO alpha-2
            "standardized": "456 ELM ST, TACOMA WA 98401",
        }
        with patch(
            "wslcb_licensing_tracker.pg_address_validator.standardize",
            return_value=mock_result,
        ):
            result = await standardize_location(pg_conn, loc_id, "456 ELM ST, TACOMA, WA 98401")
        assert result is True
        row = (
            await pg_conn.execute(select(locations.c.std_country).where(locations.c.id == loc_id))
        ).scalar_one()
        assert row == ""


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


class TestParseRetryAfter:
    def test_parses_numeric_header(self):
        response = httpx.Response(429, headers={"Retry-After": "3"})
        assert _parse_retry_after(response) == 3.0

    def test_parses_float_header(self):
        response = httpx.Response(429, headers={"Retry-After": "1.5"})
        assert _parse_retry_after(response) == 1.5

    def test_missing_header_returns_default(self):
        response = httpx.Response(429)
        assert _parse_retry_after(response) == DEFAULT_RETRY_AFTER

    def test_unparseable_header_returns_default(self):
        response = httpx.Response(429, headers={"Retry-After": "not-a-number"})
        assert _parse_retry_after(response) == DEFAULT_RETRY_AFTER

    def test_clamps_to_minimum(self):
        response = httpx.Response(429, headers={"Retry-After": "0"})
        assert _parse_retry_after(response) == 0.5


class TestPostWithRetry:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_response_on_success(self):
        mock_response = httpx.Response(200, json={"ok": True})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = mock_response

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is not None
        assert result.status_code == 200

    @pytest.mark.asyncio(loop_scope="session")
    async def test_retries_on_429_then_succeeds(self):
        retry_response = httpx.Response(429, headers={"Retry-After": "0.01"})
        ok_response = httpx.Response(200, json={"ok": True})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = [retry_response, ok_response]

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is not None
        assert result.status_code == 200
        assert mock_client.post.call_count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_exhausts_retries_on_persistent_429(self):
        retry_response = httpx.Response(429, headers={"Retry-After": "0.01"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = retry_response

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is None
        assert mock_client.post.call_count == MAX_RETRIES

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_on_timeout(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = httpx.TimeoutException("timed out")

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_on_http_error(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = httpx.ConnectError("connection refused")

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is None


class TestStandardizeHTTP:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_without_api_key(self):
        with patch("wslcb_licensing_tracker.pg_address_validator._load_api_key", return_value=""):
            result = await standardize("123 MAIN ST")
        assert result is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_data_on_success(self):
        expected = {"address_line_1": "123 MAIN ST", "city": "SEATTLE", "warnings": []}
        mock_response = httpx.Response(200, json=expected)
        with (
            patch("wslcb_licensing_tracker.pg_address_validator._load_api_key", return_value="key"),
            patch(
                "wslcb_licensing_tracker.pg_address_validator._post_with_retry",
                return_value=mock_response,
            ),
        ):
            result = await standardize("123 MAIN ST")
        assert result == expected


class TestValidateHTTP:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_without_api_key(self):
        with patch("wslcb_licensing_tracker.pg_address_validator._load_api_key", return_value=""):
            result = await validate("123 MAIN ST")
        assert result is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_when_post_returns_none(self):
        with (
            patch("wslcb_licensing_tracker.pg_address_validator._load_api_key", return_value="key"),
            patch(
                "wslcb_licensing_tracker.pg_address_validator._post_with_retry",
                return_value=None,
            ),
        ):
            result = await validate("123 MAIN ST")
        assert result is None
