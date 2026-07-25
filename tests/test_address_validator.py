"""Tests for the address validation stack: transport client and DB layer."""

import os
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select, update

from wslcb_licensing_tracker.address_client import (
    DEFAULT_RETRY_AFTER,
    HTTP_INTERNAL_SERVER_ERROR,
    HTTP_TOO_MANY_REQUESTS,
    MAX_RETRIES,
    MAX_RETRY_AFTER,
    _parse_retry_after,
    _post_with_retry,
    standardize,
    validate,
)
from wslcb_licensing_tracker.address_validator import (
    DAILY_VALIDATION_LIMIT,
    VALIDATION_TTL_DAYS,
    _validate_batch,
    backfill_addresses,
    process_location,
    standardize_location,
    validate_location,
)
from wslcb_licensing_tracker.db import get_or_create_location
from wslcb_licensing_tracker.models import locations


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
            "wslcb_licensing_tracker.address_validator.standardize",
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
            "wslcb_licensing_tracker.address_validator.standardize",
            return_value=None,
        ):
            result = await standardize_location(pg_conn, loc_id, "BAD ADDRESS ONLY")
        assert result is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_null_address_line_2_is_written_as_null(self, pg_conn):
        """API returning address_line_2: null writes NULL to the column (not empty string).

        dict.get("address_line_2", "") returns None when the key is present with a
        null value — the fallback default only applies when the key is absent.
        Migration 0004 made the column nullable so this no longer raises
        NotNullViolationError.
        """
        loc_id = await get_or_create_location(pg_conn, "800 NULL LINE ST, SEATTLE, WA 98101")
        mock_result = {
            "address_line_1": "800 NULL LINE ST",
            "address_line_2": None,  # key present, value null — as returned by the API
            "city": "SEATTLE",
            "region": "WA",
            "postal_code": "98101",
            "country": "US",
            "standardized": "800 NULL LINE ST  SEATTLE, WA 98101",
        }
        with patch(
            "wslcb_licensing_tracker.address_validator.standardize",
            return_value=mock_result,
        ):
            result = await standardize_location(
                pg_conn, loc_id, "800 NULL LINE ST, SEATTLE, WA 98101"
            )
        assert result is True
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.std_address_line_1,
                        locations.c.std_address_line_2,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["std_address_line_1"] == "800 NULL LINE ST"
        assert row["std_address_line_2"] is None

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
            "wslcb_licensing_tracker.address_validator.standardize",
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
            "wslcb_licensing_tracker.address_validator.is_validation_enabled",
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
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch("wslcb_licensing_tracker.address_validator.validate", return_value=mock_result),
        ):
            result = await validate_location(pg_conn, loc_id, "789 PINE ST, TACOMA, WA 98401")
        assert result is True
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.address_validated_at,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["address_validated_at"] is not None
        assert row["address_validation_attempted_at"] is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_api_error(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "UNVALIDATABLE ADDRESS")
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch("wslcb_licensing_tracker.address_validator.validate", return_value=None),
        ):
            result = await validate_location(pg_conn, loc_id, "UNVALIDATABLE ADDRESS")
        assert result is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_not_confirmed_writes_status_but_not_validated_at(self, pg_conn):
        # v2: not_confirmed returns address_line_1="" (empty string, not absent/None).
        # Should write validation_status/dpv_match_code, leave address_validated_at NULL,
        # and return False.
        loc_id = await get_or_create_location(pg_conn, "AMBIGUOUS RD, NOWHERE, WA 99999")
        mock_result = {
            "address_line_1": "",  # v2 shape: empty string on failure
            "validation": {"status": "not_confirmed", "dpv_match_code": "N"},
        }
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch("wslcb_licensing_tracker.address_validator.validate", return_value=mock_result),
        ):
            result = await validate_location(pg_conn, loc_id, "AMBIGUOUS RD, NOWHERE, WA 99999")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.validation_status,
                        locations.c.address_validated_at,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] == "not_confirmed"
        assert row["address_validated_at"] is None
        assert row["address_validation_attempted_at"] is not None


class TestParseRetryAfter:
    def test_parses_numeric_header(self):
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "3"})
        assert _parse_retry_after(response) == 3.0

    def test_parses_float_header(self):
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "1.5"})
        assert _parse_retry_after(response) == 1.5

    def test_missing_header_returns_default(self):
        response = httpx.Response(429)
        assert _parse_retry_after(response) == DEFAULT_RETRY_AFTER

    def test_unparseable_header_returns_default(self):
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "not-a-number"})
        assert _parse_retry_after(response) == DEFAULT_RETRY_AFTER

    def test_clamps_to_minimum(self):
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "0"})
        assert _parse_retry_after(response) == 0.5

    def test_clamps_to_maximum(self):
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "3600"})
        assert _parse_retry_after(response) == MAX_RETRY_AFTER

    def test_logs_warning_when_clamped_to_maximum(self, caplog):
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "3600"})
        with caplog.at_level("WARNING"):
            _parse_retry_after(response)
        assert any(
            "exceeds cap" in r.message or "clamp" in r.message.lower() for r in caplog.records
        )

    def test_value_at_cap_is_returned_unchanged(self):
        response = httpx.Response(
            HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": str(MAX_RETRY_AFTER)}
        )
        assert _parse_retry_after(response) == MAX_RETRY_AFTER


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
        retry_response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "0.01"})
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
        retry_response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "0.01"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = retry_response

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is None
        assert mock_client.post.call_count == MAX_RETRIES

    @pytest.mark.asyncio(loop_scope="session")
    async def test_retries_on_500_then_succeeds(self):
        error_response = httpx.Response(HTTP_INTERNAL_SERVER_ERROR)
        ok_response = httpx.Response(200, json={"ok": True})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = [error_response, ok_response]

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is not None
        assert result.status_code == 200
        assert mock_client.post.call_count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_exhausts_retries_on_persistent_500(self):
        error_response = httpx.Response(HTTP_INTERNAL_SERVER_ERROR)
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = error_response

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is None
        assert mock_client.post.call_count == MAX_RETRIES

    @pytest.mark.asyncio(loop_scope="session")
    async def test_backoff_wait_never_exceeds_max_retry_after(self):
        # Adversarial Retry-After plus the doubling backoff multiplier must never
        # sleep longer than MAX_RETRY_AFTER on any single retry.
        retry_response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "3600"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = retry_response

        with patch(
            "wslcb_licensing_tracker.address_client.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep:
            result = await _post_with_retry(
                "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
            )
        assert result is None
        assert mock_sleep.call_count == MAX_RETRIES
        for call in mock_sleep.call_args_list:
            assert call.args[0] <= MAX_RETRY_AFTER

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
    async def test_uses_v2_url(self):
        """standardize() must post to /api/v2/standardize, not /api/v1/."""
        mock_response = httpx.Response(200, json={"address_line_1": "123 MAIN ST", "warnings": []})
        with (
            patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
            patch(
                "wslcb_licensing_tracker.address_client._post_with_retry",
                return_value=mock_response,
            ) as mock_post,
        ):
            await standardize("123 MAIN ST")
        url_called = mock_post.call_args[0][0]
        assert "/api/v2/standardize" in url_called

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_without_api_key(self):
        with patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": ""}):
            result = await standardize("123 MAIN ST")
        assert result is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_data_on_success(self):
        expected = {"address_line_1": "123 MAIN ST", "city": "SEATTLE", "warnings": []}
        mock_response = httpx.Response(200, json=expected)
        with (
            patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
            patch(
                "wslcb_licensing_tracker.address_client._post_with_retry",
                return_value=mock_response,
            ),
        ):
            result = await standardize("123 MAIN ST")
        assert result == expected


class TestValidateHTTP:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_uses_v2_url(self):
        """validate() must post to /api/v2/validate, not /api/v1/."""
        mock_response = httpx.Response(200, json={"address_line_1": "123 MAIN ST", "warnings": []})
        with (
            patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
            patch(
                "wslcb_licensing_tracker.address_client._post_with_retry",
                return_value=mock_response,
            ) as mock_post,
        ):
            await validate("123 MAIN ST")
        url_called = mock_post.call_args[0][0]
        assert "/api/v2/validate" in url_called

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_without_api_key(self):
        with patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": ""}):
            result = await validate("123 MAIN ST")
        assert result is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_when_post_returns_none(self):
        with (
            patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
            patch(
                "wslcb_licensing_tracker.address_client._post_with_retry",
                return_value=None,
            ),
        ):
            result = await validate("123 MAIN ST")
        assert result is None


# ---------------------------------------------------------------------------
# process_location — unified dispatcher
# ---------------------------------------------------------------------------


MOCK_VALIDATE_RESULT = {
    "address_line_1": "100 MAIN ST",
    "address_line_2": "STE 1",
    "city": "OLYMPIA",
    "region": "WA",
    "postal_code": "98501",
    "country": "US",
    "validated": "100 MAIN ST STE 1, OLYMPIA WA 98501",
    "validation": {"status": "confirmed", "dpv_match_code": "Y"},
    "latitude": 47.0379,
    "longitude": -122.9007,
    "warnings": [],
}


class TestProcessLocation:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_validation_on_writes_all_columns_in_one_call(self, pg_conn):
        """When validation is enabled, process_location calls /validate once
        and writes std_*, validation, and both timestamps."""
        loc_id = await get_or_create_location(pg_conn, "100 MAIN ST STE 1, OLYMPIA, WA 98501")
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.validate",
                return_value=MOCK_VALIDATE_RESULT,
            ) as mock_val,
        ):
            result = await process_location(pg_conn, loc_id, "100 MAIN ST STE 1, OLYMPIA, WA 98501")
        assert result is True
        mock_val.assert_called_once()

        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.std_city,
                        locations.c.std_address_string,
                        locations.c.validation_status,
                        locations.c.dpv_match_code,
                        locations.c.latitude,
                        locations.c.address_standardized_at,
                        locations.c.address_validated_at,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["std_city"] == "OLYMPIA"
        assert row["std_address_string"] == "100 MAIN ST STE 1, OLYMPIA WA 98501"
        assert row["validation_status"] == "confirmed"
        assert row["dpv_match_code"] == "Y"
        assert row["latitude"] == 47.0379
        assert row["address_standardized_at"] is not None
        assert row["address_validated_at"] is not None
        assert row["address_validation_attempted_at"] is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_validation_off_calls_standardize_only(self, pg_conn):
        """When validation is disabled, process_location calls /standardize."""
        loc_id = await get_or_create_location(pg_conn, "200 ELM ST, TACOMA, WA 98401")
        mock_std = {
            "address_line_1": "200 ELM ST",
            "address_line_2": "",
            "city": "TACOMA",
            "region": "WA",
            "postal_code": "98401",
            "country": "US",
            "standardized": "200 ELM ST, TACOMA WA 98401",
        }
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=False,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.standardize",
                return_value=mock_std,
            ) as mock_s,
        ):
            result = await process_location(pg_conn, loc_id, "200 ELM ST, TACOMA, WA 98401")
        assert result is True
        mock_s.assert_called_once()

        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.std_city,
                        locations.c.validation_status,
                        locations.c.address_standardized_at,
                        locations.c.address_validated_at,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["std_city"] == "TACOMA"
        assert row["validation_status"] == "standardized"
        assert row["address_standardized_at"] is not None
        assert row["address_validated_at"] is None  # not set when validation off
        # standardize is not a validation attempt — attempted_at stays NULL
        assert row["address_validation_attempted_at"] is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_not_confirmed_writes_status_only(self, pg_conn):
        """v2 not_confirmed: address_line_1='' — writes status and dpv only, returns False."""
        loc_id = await get_or_create_location(pg_conn, "NOWHERE RD, BADTOWN, WA 00000")
        mock_result = {
            "address_line_1": "",  # v2 shape: empty string on failure
            "validation": {"status": "not_confirmed", "dpv_match_code": "N"},
            "warnings": [],
        }
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.validate",
                return_value=mock_result,
            ),
        ):
            result = await process_location(pg_conn, loc_id, "NOWHERE RD, BADTOWN, WA 00000")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.validation_status,
                        locations.c.address_validated_at,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] == "not_confirmed"
        assert row["address_validated_at"] is None
        # a validation was attempted even though it did not confirm
        assert row["address_validation_attempted_at"] is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_renewal_not_confirmed_is_non_destructive(self, pg_conn):
        """Re-checking an already-confirmed row that now returns not_confirmed must
        preserve std_* and address_validated_at, update status/dpv, and bump
        address_validation_attempted_at (#150)."""
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        loc_id = await get_or_create_location(pg_conn, "1 CONFIRMED WAY, SEATTLE, WA 98101")
        old = datetime.now(UTC) - timedelta(days=200)
        # Seed a prior good confirmation.
        await pg_conn.execute(
            update(locations)
            .where(locations.c.id == loc_id)
            .values(
                std_address_line_1="1 CONFIRMED WAY",
                std_address_string="1 CONFIRMED WAY, SEATTLE WA 98101",
                validation_status="confirmed",
                dpv_match_code="Y",
                address_standardized_at=old,
                address_validated_at=old,
                address_validation_attempted_at=old,
            )
        )
        mock_result = {
            "address_line_1": "",  # not_confirmed on re-check
            "validation": {"status": "not_confirmed", "dpv_match_code": "N"},
        }
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.validate",
                return_value=mock_result,
            ),
        ):
            result = await process_location(pg_conn, loc_id, "1 CONFIRMED WAY, SEATTLE, WA 98101")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.std_address_line_1,
                        locations.c.std_address_string,
                        locations.c.validation_status,
                        locations.c.dpv_match_code,
                        locations.c.address_validated_at,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        # non-destructive: prior confirmation data preserved
        assert row["std_address_line_1"] == "1 CONFIRMED WAY"
        assert row["std_address_string"] == "1 CONFIRMED WAY, SEATTLE WA 98101"
        assert row["address_validated_at"] == old
        # re-check outcome recorded
        assert row["validation_status"] == "not_confirmed"
        assert row["dpv_match_code"] == "N"
        # attempted_at bumped so the row backs off a full TTL instead of churning
        assert row["address_validation_attempted_at"] > old

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_empty_address(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "")
        result = await process_location(pg_conn, loc_id, "")
        assert result is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_api_failure(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "300 FAIL ST, SEATTLE, WA 98101")
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.validate",
                return_value=None,
            ),
        ):
            result = await process_location(pg_conn, loc_id, "300 FAIL ST, SEATTLE, WA 98101")
        assert result is False


# ---------------------------------------------------------------------------
# _validate_batch — savepoint + periodic commit resilience
# ---------------------------------------------------------------------------


class TestValidateBatch:
    """Batch tests use pg_engine (not pg_conn) because _validate_batch commits internally."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_continues_after_row_failure(self, pg_engine):
        """A failing row should not prevent subsequent rows from succeeding."""
        async with pg_engine.connect() as conn:
            loc_ok = await get_or_create_location(conn, "400 GOOD ST, SEATTLE, WA 98101")
            loc_bad = await get_or_create_location(conn, "500 BAD ST, SEATTLE, WA 98102")
            loc_ok2 = await get_or_create_location(conn, "600 FINE ST, SEATTLE, WA 98103")
            await conn.commit()

        call_count = 0

        async def mock_process(conn, location_id, address, client=None):
            nonlocal call_count
            call_count += 1
            if location_id == loc_bad:
                raise RuntimeError("Simulated DB error")
            await conn.execute(
                update(locations)
                .where(locations.c.id == location_id)
                .values(validation_status="test_ok")
            )
            return True

        rows = [
            {"id": loc_ok, "raw_address": "400 GOOD ST, SEATTLE, WA 98101"},
            {"id": loc_bad, "raw_address": "500 BAD ST, SEATTLE, WA 98102"},
            {"id": loc_ok2, "raw_address": "600 FINE ST, SEATTLE, WA 98103"},
        ]

        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator.process_location",
                side_effect=mock_process,
            ):
                result = await _validate_batch(
                    conn, rows, "Test batch", batch_size=100, rate_limit=0
                )

            assert result == 2  # 2 succeeded, 1 failed
            assert call_count == 3  # all 3 were attempted

            # Verify the good rows were committed
            for lid in (loc_ok, loc_ok2):
                status = (
                    await conn.execute(
                        select(locations.c.validation_status).where(locations.c.id == lid)
                    )
                ).scalar_one()
                assert status == "test_ok"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_commits_at_batch_size_boundary(self, pg_engine):
        """Verify periodic commit happens at batch_size intervals."""
        async with pg_engine.connect() as conn:
            locs = []
            for i in range(5):
                lid = await get_or_create_location(conn, f"{700 + i} TEST ST, SEATTLE, WA 9810{i}")
                locs.append({"id": lid, "raw_address": f"{700 + i} TEST ST, SEATTLE, WA 9810{i}"})
            await conn.commit()

        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator.process_location",
                return_value=True,
            ):
                result = await _validate_batch(
                    conn, locs, "Batch commit test", batch_size=2, rate_limit=0
                )

            assert result == 5

    @pytest.mark.asyncio(loop_scope="session")
    async def test_recovers_from_aborted_outer_transaction(self, pg_engine):
        """When a row raises an error whose .orig contains InFailedSQLTransactionError,
        _validate_batch rolls back the outer transaction and continues processing
        subsequent rows rather than cascading the failure to every remaining row."""
        async with pg_engine.connect() as conn:
            loc_before = await get_or_create_location(conn, "900 BEFORE ST, SEATTLE, WA 98101")
            loc_abort = await get_or_create_location(conn, "901 ABORT ST, SEATTLE, WA 98102")
            loc_after = await get_or_create_location(conn, "902 AFTER ST, SEATTLE, WA 98103")
            await conn.commit()

        call_count = 0

        class _FakeAbortError(Exception):
            """Mimics the sqlalchemy DBAPIError shape produced by asyncpg in production."""

            def __init__(self):
                super().__init__("transaction aborted")
                # orig is the asyncpg adapter wrapper; its str() contains the
                # asyncpg exception class name.
                self.orig = Exception(
                    "<class 'asyncpg.exceptions.InFailedSQLTransactionError'>: "
                    "current transaction is aborted, commands ignored"
                )

        async def mock_process(conn, location_id, address, client=None):
            nonlocal call_count
            call_count += 1
            if location_id == loc_abort:
                raise _FakeAbortError
            await conn.execute(
                update(locations)
                .where(locations.c.id == location_id)
                .values(validation_status="recovered_ok")
            )
            return True

        rows = [
            {"id": loc_before, "raw_address": "900 BEFORE ST, SEATTLE, WA 98101"},
            {"id": loc_abort, "raw_address": "901 ABORT ST, SEATTLE, WA 98102"},
            {"id": loc_after, "raw_address": "902 AFTER ST, SEATTLE, WA 98103"},
        ]

        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator.process_location",
                side_effect=mock_process,
            ):
                result = await _validate_batch(
                    conn, rows, "Rollback recovery test", batch_size=100, rate_limit=0
                )

        # loc_abort triggered rollback; loc_before and loc_after both returned True.
        assert result == 2
        assert call_count == 3

        # Verify committed DB state: rollback undoes loc_before's uncommitted write;
        # loc_after's write (in the new transaction after recovery) is committed.
        async with pg_engine.connect() as conn:
            statuses = {
                row["id"]: row["validation_status"]
                for row in (
                    await conn.execute(
                        select(locations.c.id, locations.c.validation_status).where(
                            locations.c.id.in_([loc_before, loc_abort, loc_after])
                        )
                    )
                ).mappings()
            }
        assert statuses[loc_before] is None  # rolled back
        assert statuses[loc_abort] is None  # never updated
        assert statuses[loc_after] == "recovered_ok"  # committed after recovery


# ---------------------------------------------------------------------------
# backfill_addresses — TTL-based renewal of already-validated locations (#150)
# ---------------------------------------------------------------------------


class TestBackfillTTL:
    """backfill_addresses renews validations older than VALIDATION_TTL_DAYS, keyed
    on address_validation_attempted_at, mode-aware, and bounded by a daily ceiling.

    Uses pg_engine because backfill_addresses -> _validate_batch commits internally.
    process_location is mocked to capture which location ids the selector surfaces
    (and to leave attempted_at untouched, so the ceiling math is deterministic).
    """

    @staticmethod
    def _capture():
        processed: list[int] = []

        async def mock_process(conn, location_id, address, client=None):
            processed.append(location_id)
            return True

        return processed, mock_process

    @pytest.mark.asyncio(loop_scope="session")
    async def test_enabled_selects_stale_and_null_skips_fresh(self, pg_engine):
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        now = datetime.now(UTC)
        stale = now - timedelta(days=VALIDATION_TTL_DAYS + 1)
        fresh = now - timedelta(days=VALIDATION_TTL_DAYS - 1)

        async with pg_engine.connect() as conn:
            loc_stale = await get_or_create_location(conn, "1 STALE ST, SEATTLE, WA 98101")
            loc_fresh = await get_or_create_location(conn, "2 FRESH ST, SEATTLE, WA 98102")
            loc_null = await get_or_create_location(conn, "3 NEVER ST, SEATTLE, WA 98103")
            # stale: attempted long ago -> must be renewed
            await conn.execute(
                update(locations)
                .where(locations.c.id == loc_stale)
                .values(address_standardized_at=stale, address_validation_attempted_at=stale)
            )
            # fresh: attempted recently -> must be skipped
            await conn.execute(
                update(locations)
                .where(locations.c.id == loc_fresh)
                .values(address_standardized_at=fresh, address_validation_attempted_at=fresh)
            )
            # null: never attempted -> must be selected
            await conn.commit()

        processed, mock_process = self._capture()
        async with pg_engine.connect() as conn:
            with (
                patch(
                    "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                    return_value=True,
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.get_api_key",
                    return_value="test-key",
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.process_location",
                    side_effect=mock_process,
                ),
            ):
                await backfill_addresses(conn, rate_limit=0)

        assert loc_stale in processed  # renewed past TTL
        assert loc_null in processed  # never attempted
        assert loc_fresh not in processed  # still within TTL

    @pytest.mark.asyncio(loop_scope="session")
    async def test_enabled_ignores_stale_standardized_when_recently_attempted(self, pg_engine):
        """A not_confirmed row (std_at NULL) with a recent attempt must NOT re-select
        while validation is enabled — attempted_at, not standardized_at, is the key."""
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        recent = datetime.now(UTC) - timedelta(days=1)
        async with pg_engine.connect() as conn:
            loc = await get_or_create_location(conn, "9 NOSTD RD, SEATTLE, WA 98199")
            await conn.execute(
                update(locations)
                .where(locations.c.id == loc)
                .values(
                    validation_status="not_confirmed",
                    address_standardized_at=None,
                    address_validation_attempted_at=recent,
                )
            )
            await conn.commit()

        processed, mock_process = self._capture()
        async with pg_engine.connect() as conn:
            with (
                patch(
                    "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                    return_value=True,
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.get_api_key",
                    return_value="test-key",
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.process_location",
                    side_effect=mock_process,
                ),
            ):
                await backfill_addresses(conn, rate_limit=0)

        assert loc not in processed  # no churn despite std_at NULL

    @pytest.mark.asyncio(loop_scope="session")
    async def test_disabled_selects_on_standardized_at(self, pg_engine):
        """Validation disabled: key on standardized_at IS NULL, not attempted_at."""
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        stale = datetime.now(UTC) - timedelta(days=VALIDATION_TTL_DAYS + 1)
        async with pg_engine.connect() as conn:
            loc_unstd = await get_or_create_location(conn, "4 UNSTD ST, SEATTLE, WA 98104")
            loc_std = await get_or_create_location(conn, "5 STD ST, SEATTLE, WA 98105")
            # std done + attempt stale: irrelevant to disabled mode -> must be skipped
            await conn.execute(
                update(locations)
                .where(locations.c.id == loc_std)
                .values(address_standardized_at=stale, address_validation_attempted_at=stale)
            )
            await conn.commit()

        processed, mock_process = self._capture()
        async with pg_engine.connect() as conn:
            with (
                patch(
                    "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                    return_value=False,
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.get_api_key",
                    return_value="test-key",
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.process_location",
                    side_effect=mock_process,
                ),
            ):
                await backfill_addresses(conn, rate_limit=0)

        assert loc_unstd in processed  # never standardized
        assert loc_std not in processed  # standardized already; attempted_at irrelevant

    @pytest.mark.asyncio(loop_scope="session")
    async def test_daily_ceiling_clamps_to_remaining_budget(self, pg_engine):
        from datetime import timedelta

        from sqlalchemy import func

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        stale = datetime.now(UTC) - timedelta(days=VALIDATION_TTL_DAYS + 1)
        day_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

        async with pg_engine.connect() as conn:
            used_before = (
                await conn.execute(
                    select(func.count())
                    .select_from(locations)
                    .where(locations.c.address_validation_attempted_at >= day_start)
                )
            ).scalar_one()
            eligible = []
            for i in range(5):
                addr = f"{600 + i} CEIL ST, SEATTLE, WA 981{i:02}"
                lid = await get_or_create_location(conn, addr)
                await conn.execute(
                    update(locations)
                    .where(locations.c.id == lid)
                    .values(address_standardized_at=stale, address_validation_attempted_at=stale)
                )
                eligible.append(lid)
            await conn.commit()

        cap = 3
        processed, mock_process = self._capture()
        async with pg_engine.connect() as conn:
            with (
                patch(
                    "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                    return_value=True,
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.get_api_key",
                    return_value="test-key",
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.process_location",
                    side_effect=mock_process,
                ),
            ):
                # budget = daily_limit - used_before = cap
                await backfill_addresses(conn, rate_limit=0, daily_limit=used_before + cap)

        assert len(processed) == cap  # LIMIT clamped to remaining budget

    @pytest.mark.asyncio(loop_scope="session")
    async def test_daily_ceiling_zero_budget_processes_nothing(self, pg_engine):
        from datetime import timedelta

        from sqlalchemy import func

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        stale = datetime.now(UTC) - timedelta(days=VALIDATION_TTL_DAYS + 1)
        day_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

        async with pg_engine.connect() as conn:
            lid = await get_or_create_location(conn, "700 ZERO ST, SEATTLE, WA 98107")
            await conn.execute(
                update(locations)
                .where(locations.c.id == lid)
                .values(address_standardized_at=stale, address_validation_attempted_at=stale)
            )
            await conn.commit()
            used_before = (
                await conn.execute(
                    select(func.count())
                    .select_from(locations)
                    .where(locations.c.address_validation_attempted_at >= day_start)
                )
            ).scalar_one()

        processed, mock_process = self._capture()
        async with pg_engine.connect() as conn:
            with (
                patch(
                    "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                    return_value=True,
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.get_api_key",
                    return_value="test-key",
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.process_location",
                    side_effect=mock_process,
                ),
            ):
                await backfill_addresses(conn, rate_limit=0, daily_limit=used_before)

        assert processed == []  # zero budget -> nothing processed

    @pytest.mark.asyncio(loop_scope="session")
    async def test_nulls_first_prioritizes_never_attempted(self, pg_engine):
        """When the budget is smaller than the eligible set, never-attempted
        (attempted_at IS NULL) rows are processed before stale ones — so new
        locations are not starved during the renewal wave."""
        from datetime import timedelta

        from sqlalchemy import func

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        stale = datetime.now(UTC) - timedelta(days=VALIDATION_TTL_DAYS + 1)
        day_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

        async with pg_engine.connect() as conn:
            used_before = (
                await conn.execute(
                    select(func.count())
                    .select_from(locations)
                    .where(locations.c.address_validation_attempted_at >= day_start)
                )
            ).scalar_one()
            # 2 never-attempted (attempted_at NULL by default) + 3 stale.
            for i in range(2):
                await get_or_create_location(conn, f"{80 + i} NEW WAY, SEATTLE, WA 98108")
            for i in range(3):
                lid = await get_or_create_location(conn, f"{90 + i} OLD WAY, SEATTLE, WA 98109")
                await conn.execute(
                    update(locations)
                    .where(locations.c.id == lid)
                    .values(address_standardized_at=stale, address_validation_attempted_at=stale)
                )
            await conn.commit()

        processed, mock_process = self._capture()
        async with pg_engine.connect() as conn:
            with (
                patch(
                    "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                    return_value=True,
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.get_api_key",
                    return_value="test-key",
                ),
                patch(
                    "wslcb_licensing_tracker.address_validator.process_location",
                    side_effect=mock_process,
                ),
            ):
                # budget = 2; there are >= 2 never-attempted rows, which sort first
                await backfill_addresses(conn, rate_limit=0, daily_limit=used_before + 2)

            assert len(processed) == 2
            # every processed row must be a never-attempted (NULL) row
            attempted = (
                (
                    await conn.execute(
                        select(locations.c.address_validation_attempted_at).where(
                            locations.c.id.in_(processed)
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert all(a is None for a in attempted)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_default_daily_limit_is_constant(self):
        assert DAILY_VALIDATION_LIMIT == 5000
