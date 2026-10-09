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
    QuotaExhaustedError,
    _parse_retry_after,
    _post_with_retry,
    standardize,
    validate,
)
from wslcb_licensing_tracker.address_validator import (
    DAILY_VALIDATION_LIMIT,
    MAX_CONSECUTIVE_NO_ANSWER,
    VALIDATION_TTL_DAYS,
    LocationOutcome,
    _process_location,
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
    async def test_uppercases_std_text(self, pg_conn):
        """Mixed-case components are stored uppercase, Pub 28 style (#188)."""
        loc_id = await get_or_create_location(pg_conn, "451 KRAMER RD, UNDERWOOD, WA 98651")
        mock_result = {
            "address_line_1": "451 Kramer Rd",
            "address_line_2": "Ste b",
            "city": "Underwood",
            "region": "wa",
            "postal_code": "98651",
            "country": "us",
            "standardized": "451 Kramer Rd  Underwood, WA 98651",
        }
        with patch(
            "wslcb_licensing_tracker.address_validator.standardize",
            return_value=mock_result,
        ):
            await standardize_location(pg_conn, loc_id, "451 KRAMER RD, UNDERWOOD, WA 98651")
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.std_address_line_1,
                        locations.c.std_address_line_2,
                        locations.c.std_city,
                        locations.c.std_region,
                        locations.c.std_country,
                        locations.c.std_address_string,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert dict(row) == {
            "std_address_line_1": "451 KRAMER RD",
            "std_address_line_2": "STE B",
            "std_city": "UNDERWOOD",
            "std_region": "WA",
            "std_country": "US",
            "std_address_string": "451 KRAMER RD  UNDERWOOD, WA 98651",
        }

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

    @pytest.mark.asyncio(loop_scope="session")
    async def test_unavailable_without_provider_writes_nothing(self, pg_conn):
        # 'unavailable' with no provider means the validator has none configured —
        # a service-side state, not an answer: leave the row for retry (#187).
        loc_id = await get_or_create_location(pg_conn, "12 OUTAGE LN, YAKIMA, WA 98901")
        mock_result = {
            "address_line_1": "",
            "validation": {"status": "unavailable", "dpv_match_code": None, "provider": None},
        }
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch("wslcb_licensing_tracker.address_validator.validate", return_value=mock_result),
        ):
            result = await validate_location(pg_conn, loc_id, "12 OUTAGE LN, YAKIMA, WA 98901")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.validation_status,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] is None
        assert row["address_validation_attempted_at"] is None


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

    def test_long_value_is_returned_unclamped(self):
        # The caller decides what a long wait means (#187, address-validator#270).
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "3600"})
        assert _parse_retry_after(response) == 3600.0

    @pytest.mark.parametrize("raw", ["nan", "inf", "-inf", "1e400"])
    def test_non_finite_header_returns_default(self, raw):
        # asyncio.sleep(nan) never returns: one bad header would hang the backfill.
        response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": raw})
        assert _parse_retry_after(response) == DEFAULT_RETRY_AFTER

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
        # A Retry-After under the cap, doubled by the backoff multiplier, must never
        # sleep longer than MAX_RETRY_AFTER on any single retry.
        retry_response = httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "40"})
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
        # No sleep after the final attempt — there is nothing left to wait for.
        assert mock_sleep.call_count == MAX_RETRIES - 1
        for call in mock_sleep.call_args_list:
            assert call.args[0] <= MAX_RETRY_AFTER

    @pytest.mark.parametrize("status", [HTTP_TOO_MANY_REQUESTS, 503])
    @pytest.mark.asyncio(loop_scope="session")
    async def test_retry_after_over_cap_raises_without_sleeping(self, status):
        # Over MAX_RETRY_AFTER means a daily quota is out (address-validator#270):
        # no retry inside the cap could succeed, so stop instead of sleeping (#187).
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = httpx.Response(status, headers={"Retry-After": "35000"})

        with (
            patch(
                "wslcb_licensing_tracker.address_client.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
            pytest.raises(QuotaExhaustedError) as excinfo,
        ):
            await _post_with_retry(
                "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
            )
        assert excinfo.value.retry_after == 35000.0
        assert excinfo.value.status == status
        assert mock_client.post.call_count == 1
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_retry_after_at_cap_is_waited_out(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = [
            httpx.Response(HTTP_TOO_MANY_REQUESTS, headers={"Retry-After": "60"}),
            httpx.Response(200, json={"ok": True}),
        ]

        with patch(
            "wslcb_licensing_tracker.address_client.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep:
            result = await _post_with_retry(
                "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
            )
        assert result is not None
        mock_sleep.assert_awaited_once_with(MAX_RETRY_AFTER)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_retries_on_timeout_then_succeeds(self):
        # A timeout is transient: back off and retry, don't give up on attempt 1 (#183).
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = [
            httpx.TimeoutException("timed out"),
            httpx.Response(200, json={"ok": True}),
        ]

        with patch("wslcb_licensing_tracker.address_client.asyncio.sleep", new_callable=AsyncMock):
            result = await _post_with_retry(
                "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
            )
        assert result is not None
        assert result.status_code == 200
        assert mock_client.post.call_count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_exhausts_retries_on_persistent_connect_error(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = httpx.ConnectError("connection refused")

        with patch("wslcb_licensing_tracker.address_client.asyncio.sleep", new_callable=AsyncMock):
            result = await _post_with_retry(
                "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
            )
        assert result is None
        assert mock_client.post.call_count == MAX_RETRIES

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_on_non_transport_http_error(self):
        # Not a transient network failure — retrying would not help.
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = httpx.TooManyRedirects("loop")

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is None
        assert mock_client.post.call_count == 1

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize("status", [502, 503, 504])
    async def test_retries_on_gateway_errors(self, status):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = [
            httpx.Response(status),
            httpx.Response(200, json={"ok": True}),
        ]

        with patch("wslcb_licensing_tracker.address_client.asyncio.sleep", new_callable=AsyncMock):
            result = await _post_with_retry(
                "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
            )
        assert result is not None
        assert result.status_code == 200
        assert mock_client.post.call_count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_does_not_retry_client_errors(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = httpx.Response(422)

        result = await _post_with_retry(
            "http://test/api", {"address": "x"}, {"X-API-Key": "k"}, mock_client, "test"
        )
        assert result is not None
        assert result.status_code == 422
        assert mock_client.post.call_count == 1


class TestQuotaExhaustedPropagates:
    """Single-row helpers write nothing and let the batch decide to stop (#187)."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_validate_location_propagates_and_writes_nothing(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "12 QUOTA ST, OLYMPIA, WA 98501")
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.validate",
                side_effect=QuotaExhaustedError(35000.0, HTTP_TOO_MANY_REQUESTS),
            ),
            pytest.raises(QuotaExhaustedError),
        ):
            await validate_location(pg_conn, loc_id, "12 QUOTA ST, OLYMPIA, WA 98501")
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.validation_status,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] is None
        assert row["address_validation_attempted_at"] is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_standardize_location_propagates(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "14 QUOTA ST, OLYMPIA, WA 98501")
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.standardize",
                side_effect=QuotaExhaustedError(35000.0, HTTP_TOO_MANY_REQUESTS),
            ),
            pytest.raises(QuotaExhaustedError),
        ):
            await standardize_location(pg_conn, loc_id, "14 QUOTA ST, OLYMPIA, WA 98501")


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
    async def test_non_200_log_names_ref_not_address(self, caplog):
        # Addresses are PII: logs carry the caller's ref, never the address (#183).
        with (
            patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
            patch(
                "wslcb_licensing_tracker.address_client._post_with_retry",
                return_value=httpx.Response(422),
            ),
            caplog.at_level("WARNING"),
        ):
            result = await validate("123 SECRET ST, SEATTLE, WA", log_ref=4242)
        assert result is None
        messages = [r.getMessage() for r in caplog.records]
        assert messages
        assert not any("SECRET" in m for m in messages)
        assert any("4242" in m and "422" in m for m in messages)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_api_warning_log_names_ref_not_address(self, caplog):
        body = {"address_line_1": "", "warnings": ["Missing secondary unit"]}
        with (
            patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
            patch(
                "wslcb_licensing_tracker.address_client._post_with_retry",
                return_value=httpx.Response(200, json=body),
            ),
            caplog.at_level("WARNING"),
        ):
            await validate("123 SECRET ST, SEATTLE, WA", log_ref=4242)
        messages = [r.getMessage() for r in caplog.records]
        assert not any("SECRET" in m for m in messages)
        assert any("4242" in m and "Missing secondary unit" in m for m in messages)

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
        # Logs name the row, never the address (#183).
        assert mock_val.call_args.kwargs["log_ref"] == loc_id

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
        # Logs name the row, never the address (#183).
        assert mock_s.call_args.kwargs["log_ref"] == loc_id

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
    async def test_unavailable_without_provider_on_new_row_writes_nothing(self, pg_conn):
        """No provider configured: leaves attempted_at NULL so the next backfill
        retries (#187)."""
        loc_id = await get_or_create_location(pg_conn, "14 OUTAGE LN, YAKIMA, WA 98901")
        mock_result = {
            "address_line_1": "",
            "validation": {"status": "unavailable", "dpv_match_code": None, "provider": None},
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
            result = await process_location(pg_conn, loc_id, "14 OUTAGE LN, YAKIMA, WA 98901")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.validation_status,
                        locations.c.dpv_match_code,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] is None
        assert row["dpv_match_code"] is None
        assert row["address_validation_attempted_at"] is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_renewal_unavailable_without_provider_leaves_row_untouched(self, pg_conn):
        """A re-check the validator can't serve (no provider configured) must not
        overwrite the confirmed status/dpv nor bump attempted_at — the row stays
        due for renewal (#187)."""
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        loc_id = await get_or_create_location(pg_conn, "2 CONFIRMED WAY, SEATTLE, WA 98101")
        old = datetime.now(UTC) - timedelta(days=200)
        await pg_conn.execute(
            update(locations)
            .where(locations.c.id == loc_id)
            .values(
                std_address_line_1="2 CONFIRMED WAY",
                validation_status="confirmed",
                dpv_match_code="Y",
                address_standardized_at=old,
                address_validated_at=old,
                address_validation_attempted_at=old,
            )
        )
        mock_result = {
            "address_line_1": "",
            "validation": {"status": "unavailable", "dpv_match_code": None, "provider": None},
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
            result = await process_location(pg_conn, loc_id, "2 CONFIRMED WAY, SEATTLE, WA 98101")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(
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
        assert row["validation_status"] == "confirmed"
        assert row["dpv_match_code"] == "Y"
        assert row["address_validated_at"] == old
        assert row["address_validation_attempted_at"] == old

    # USPS answered HTTP 200 with a blank DPV ("no delivery-point determination").
    # Deterministic per address — the same input gets the same answer every call
    # (CannObserv/address-validator#250) — so it is an answer, not an outage (#187).
    USPS_NO_DETERMINATION = {
        "address_line_1": "301 E HARBOR AVE",
        "city": "WESTPORT",
        "region": "WA",
        "postal_code": "98595",
        "validation": {"status": "unavailable", "dpv_match_code": None, "provider": "usps"},
        "warnings": [],
    }

    @pytest.mark.asyncio(loop_scope="session")
    async def test_usps_no_determination_on_new_row_records_attempt(self, pg_conn):
        """The row waits a full TTL instead of heading every backfill (#187)."""
        loc_id = await get_or_create_location(pg_conn, "301 E HARBOR AVE, WESTPORT, WA 98595")
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.validate",
                return_value=self.USPS_NO_DETERMINATION,
            ),
        ):
            outcome = await _process_location(
                pg_conn, loc_id, "301 E HARBOR AVE, WESTPORT, WA 98595"
            )
        # An answer: it must not count toward the no-answer breaker.
        assert outcome is LocationOutcome.RECORDED
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.validation_status,
                        locations.c.dpv_match_code,
                        locations.c.std_address_line_1,
                        locations.c.address_validated_at,
                        locations.c.address_validation_attempted_at,
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] == "unavailable"  # no prior status to keep
        assert row["dpv_match_code"] is None
        assert row["std_address_line_1"] == ""  # not a confirmation: no overlay
        assert row["address_validated_at"] is None
        assert row["address_validation_attempted_at"] is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_renewal_usps_no_determination_keeps_prior_status(self, pg_conn):
        """Re-checking a confirmed row that now gets no determination keeps its
        status, dpv and std_* (no new information) but records the attempt (#187)."""
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        loc_id = await get_or_create_location(pg_conn, "3 CONFIRMED WAY, SEATTLE, WA 98101")
        old = datetime.now(UTC) - timedelta(days=200)
        await pg_conn.execute(
            update(locations)
            .where(locations.c.id == loc_id)
            .values(
                std_address_line_1="3 CONFIRMED WAY",
                validation_status="confirmed",
                dpv_match_code="Y",
                address_standardized_at=old,
                address_validated_at=old,
                address_validation_attempted_at=old,
            )
        )
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.address_validator.validate",
                return_value=self.USPS_NO_DETERMINATION,
            ),
        ):
            result = await process_location(pg_conn, loc_id, "3 CONFIRMED WAY, SEATTLE, WA 98101")
        assert result is False
        row = (
            (
                await pg_conn.execute(
                    select(
                        locations.c.std_address_line_1,
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
        assert row["std_address_line_1"] == "3 CONFIRMED WAY"
        assert row["validation_status"] == "confirmed"
        assert row["dpv_match_code"] == "Y"
        assert row["address_validated_at"] == old
        assert row["address_validation_attempted_at"] > old

    # address-validator contract v2 (CannObserv/address-validator#250, deployed
    # 2026-09-30): nobody determined the address — a final answer, cached upstream.
    UNDETERMINED = {
        "address_line_1": "",
        "validation": {"status": "undetermined", "dpv_match_code": None, "provider": "usps"},
        "warnings": [],
    }
    # Upstream retry warning: a fallback provider failed along the way, so the
    # answer is not cached and a later retry may do better. Since
    # CannObserv/address-validator#275 it rides on invalid/not_found too (#191).
    RETRY_WARNING = (
        "Validation incomplete while a fallback provider was unreachable;"
        " a later retry may produce a determination"
    )
    UNDETERMINED_RETRYABLE = {
        "address_line_1": "",
        "validation": {"status": "undetermined", "dpv_match_code": None, "provider": "usps"},
        "warnings": [RETRY_WARNING],
    }
    # USPS 429/5xx, then a Google verdict with no DPV code.
    NOT_FOUND = {
        "address_line_1": "",
        "validation": {"status": "not_found", "dpv_match_code": None, "provider": "google"},
        "warnings": [],
    }
    NOT_FOUND_RETRYABLE = {**NOT_FOUND, "warnings": [RETRY_WARNING]}
    INVALID_RETRYABLE = {
        "address_line_1": "",
        "validation": {"status": "invalid", "dpv_match_code": None, "provider": "google"},
        "warnings": [RETRY_WARNING],
    }
    # A Google-grade confirmation: no USPS DPV code, may have altered the street/ZIP
    # (CannObserv/address-validator#258).
    GOOGLE_CONFIRMED = {
        "address_line_1": "19501",
        "address_line_2": "",
        "city": "Woodinville",
        "region": "WA",
        "postal_code": "98072-0000",
        "country": "US",
        "validated": "19501 Woodinville WA 98072-0000",
        "latitude": 47.75,
        "longitude": -122.16,
        "validation": {"status": "confirmed", "dpv_match_code": None, "provider": "google"},
        "warnings": ["One or more address components are unconfirmed"],
    }

    @staticmethod
    async def _seed_usps_confirmed(conn, address, line_1):
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        loc_id = await get_or_create_location(conn, address)
        old = datetime.now(UTC) - timedelta(days=200)
        await conn.execute(
            update(locations)
            .where(locations.c.id == loc_id)
            .values(
                std_address_line_1=line_1,
                std_city="WOODINVILLE",
                validation_status="confirmed",
                dpv_match_code="Y",
                address_standardized_at=old,
                address_validated_at=old,
                address_validation_attempted_at=old,
            )
        )
        return loc_id, old

    async def _run(self, conn, loc_id, address, result):
        with (
            patch(
                "wslcb_licensing_tracker.address_validator.is_validation_enabled",
                return_value=True,
            ),
            patch("wslcb_licensing_tracker.address_validator.validate", return_value=result),
        ):
            return await _process_location(conn, loc_id, address)

    async def _row(self, conn, loc_id):
        return (
            (
                await conn.execute(
                    select(
                        locations.c.std_address_line_1,
                        locations.c.std_city,
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

    @pytest.mark.asyncio(loop_scope="session")
    async def test_undetermined_on_renewal_keeps_prior_status(self, pg_conn):
        """'undetermined' is an answer with nothing to replace a confirmation (#187)."""
        addr = "5 KEEP WAY, WOODINVILLE, WA 98072"
        loc_id, old = await self._seed_usps_confirmed(pg_conn, addr, "5 KEEP WAY")
        outcome = await self._run(pg_conn, loc_id, addr, self.UNDETERMINED)
        assert outcome is LocationOutcome.RECORDED
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] == "confirmed"
        assert row["dpv_match_code"] == "Y"
        assert row["address_validated_at"] == old
        assert row["address_validation_attempted_at"] > old

    @pytest.mark.asyncio(loop_scope="session")
    async def test_undetermined_on_new_row_records_status(self, pg_conn):
        addr = "6 FRESH WAY, WOODINVILLE, WA 98072"
        loc_id = await get_or_create_location(pg_conn, addr)
        outcome = await self._run(pg_conn, loc_id, addr, self.UNDETERMINED)
        assert outcome is LocationOutcome.RECORDED
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] == "undetermined"
        assert row["address_validation_attempted_at"] is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_undetermined_with_retry_warning_writes_nothing(self, pg_conn):
        """A fallback provider was unreachable: not final, so not stamped (#187)."""
        addr = "7 LATER WAY, WOODINVILLE, WA 98072"
        loc_id = await get_or_create_location(pg_conn, addr)
        outcome = await self._run(pg_conn, loc_id, addr, self.UNDETERMINED_RETRYABLE)
        assert outcome is LocationOutcome.RETRY_LATER
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] is None
        assert row["address_validation_attempted_at"] is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_google_confirmation_never_replaces_usps_confirmation(self, pg_conn):
        """A DPV-less confirmation must not overwrite a USPS-confirmed address
        (CannObserv/address-validator#258); it only records the attempt."""
        addr = "19501 & 19495 144TH AVE NE STE, WOODINVILLE, WA 98072"
        loc_id, old = await self._seed_usps_confirmed(pg_conn, addr, "19501 144TH AVE NE")
        outcome = await self._run(pg_conn, loc_id, addr, self.GOOGLE_CONFIRMED)
        assert outcome is LocationOutcome.RECORDED
        row = await self._row(pg_conn, loc_id)
        assert row["std_address_line_1"] == "19501 144TH AVE NE"
        assert row["std_city"] == "WOODINVILLE"
        assert row["validation_status"] == "confirmed"
        assert row["dpv_match_code"] == "Y"
        assert row["address_validated_at"] == old
        assert row["address_validation_attempted_at"] > old

    @pytest.mark.asyncio(loop_scope="session")
    async def test_google_confirmation_fills_a_never_confirmed_row(self, pg_conn):
        """With no USPS confirmation to protect, a Google confirmation still lands."""
        addr = "19502 NOWHERE AVE NE, WOODINVILLE, WA 98072"
        loc_id = await get_or_create_location(pg_conn, addr)
        outcome = await self._run(pg_conn, loc_id, addr, self.GOOGLE_CONFIRMED)
        assert outcome is LocationOutcome.WRITTEN
        row = await self._row(pg_conn, loc_id)
        # Google's mixed case is stored uppercase, matching USPS answers (#188).
        assert row["std_city"] == "WOODINVILLE"
        assert row["std_address_string"] == "19501 WOODINVILLE WA 98072-0000"
        assert row["validation_status"] == "confirmed"
        assert row["address_validated_at"] is not None

    @staticmethod
    async def _seed_dpv_cleared(conn, address, line_1, status):
        """A once-confirmed row whose DPV code a later non-confirming answer cleared.

        Pre-#183 transient stamping and DPV-less invalid/not_found re-checks both
        write dpv_match_code NULL but keep address_validated_at and std_* (#189).
        """
        from datetime import timedelta

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        loc_id = await get_or_create_location(conn, address)
        old = datetime.now(UTC) - timedelta(days=200)
        await conn.execute(
            update(locations)
            .where(locations.c.id == loc_id)
            .values(
                std_address_line_1=line_1,
                std_city="WOODINVILLE",
                validation_status=status,
                dpv_match_code=None,
                address_standardized_at=old,
                address_validated_at=old,
                address_validation_attempted_at=old,
            )
        )
        return loc_id, old

    @pytest.mark.parametrize("status", ["unavailable", "invalid", "not_found", "not_confirmed"])
    @pytest.mark.asyncio(loop_scope="session")
    async def test_google_confirmation_keeps_std_whose_dpv_was_cleared(self, pg_conn, status):
        """Losing the DPV code must not lose the guard: the std_* came from a
        confirmation, so a DPV-less answer only records the attempt (#189)."""
        addr = f"8 CLEARED WAY {status.upper()}, WOODINVILLE, WA 98072"
        loc_id, old = await self._seed_dpv_cleared(pg_conn, addr, "8 CLEARED WAY", status)
        outcome = await self._run(pg_conn, loc_id, addr, self.GOOGLE_CONFIRMED)
        assert outcome is LocationOutcome.RECORDED
        row = await self._row(pg_conn, loc_id)
        assert row["std_address_line_1"] == "8 CLEARED WAY"
        assert row["validation_status"] == status
        assert row["dpv_match_code"] is None
        assert row["address_validated_at"] == old
        assert row["address_validation_attempted_at"] > old

    @pytest.mark.asyncio(loop_scope="session")
    async def test_google_confirmation_replaces_a_google_confirmation(self, pg_conn):
        """A DPV-less confirmed row has nothing USPS to protect (#189)."""
        addr = "9 GOOGLE WAY, WOODINVILLE, WA 98072"
        loc_id, old = await self._seed_dpv_cleared(pg_conn, addr, "9 GOOGLE WAY", "confirmed")
        outcome = await self._run(pg_conn, loc_id, addr, self.GOOGLE_CONFIRMED)
        assert outcome is LocationOutcome.WRITTEN
        row = await self._row(pg_conn, loc_id)
        assert row["std_address_line_1"] == "19501"
        assert row["address_validated_at"] > old

    @pytest.mark.asyncio(loop_scope="session")
    async def test_usps_confirmation_replaces_a_dpv_cleared_row(self, pg_conn):
        """The guard only stops DPV-less answers; a USPS DPV answer always lands."""
        addr = "10 USPS WAY, WOODINVILLE, WA 98072"
        loc_id, old = await self._seed_dpv_cleared(pg_conn, addr, "10 OLD WAY", "unavailable")
        usps = {
            **self.GOOGLE_CONFIRMED,
            "address_line_1": "10 USPS WAY",
            "validation": {"status": "confirmed", "dpv_match_code": "Y", "provider": "usps"},
        }
        outcome = await self._run(pg_conn, loc_id, addr, usps)
        assert outcome is LocationOutcome.WRITTEN
        row = await self._row(pg_conn, loc_id)
        assert row["std_address_line_1"] == "10 USPS WAY"
        assert row["validation_status"] == "confirmed"
        assert row["dpv_match_code"] == "Y"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_undetermined_replaces_legacy_unavailable_status(self, pg_conn):
        """Under contract v2 'unavailable' means no provider configured; a row
        stamped with the pre-v2 USPS 'unavailable' takes the v2 answer (#189)."""
        addr = "11 LEGACY WAY, WOODINVILLE, WA 98072"
        loc_id, old = await self._seed_dpv_cleared(pg_conn, addr, "11 LEGACY WAY", "unavailable")
        outcome = await self._run(pg_conn, loc_id, addr, self.UNDETERMINED)
        assert outcome is LocationOutcome.RECORDED
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] == "undetermined"
        assert row["std_address_line_1"] == "11 LEGACY WAY"
        assert row["address_validated_at"] == old
        assert row["address_validation_attempted_at"] > old

    @pytest.mark.asyncio(loop_scope="session")
    async def test_undetermined_keeps_a_not_found_status(self, pg_conn):
        """Only a no-determination status gives way; a real answer is kept (#187)."""
        addr = "12 KEPT WAY, WOODINVILLE, WA 98072"
        loc_id, _ = await self._seed_dpv_cleared(pg_conn, addr, "12 KEPT WAY", "not_found")
        await self._run(pg_conn, loc_id, addr, self.UNDETERMINED)
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] == "not_found"

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize("result_name", ["NOT_FOUND_RETRYABLE", "INVALID_RETRYABLE"])
    async def test_verdict_with_retry_warning_writes_nothing(self, pg_conn, result_name):
        """A hinted invalid/not_found is not final: not stamped for a TTL (#191)."""
        addr = f"13 {result_name} WAY, WOODINVILLE, WA 98072"
        loc_id = await get_or_create_location(pg_conn, addr)
        outcome = await self._run(pg_conn, loc_id, addr, getattr(self, result_name))
        assert outcome is LocationOutcome.RETRY_LATER
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] is None
        assert row["dpv_match_code"] is None
        assert row["address_validation_attempted_at"] is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_not_found_without_retry_warning_is_recorded(self, pg_conn):
        addr = "14 FINAL WAY, WOODINVILLE, WA 98072"
        loc_id = await get_or_create_location(pg_conn, addr)
        outcome = await self._run(pg_conn, loc_id, addr, self.NOT_FOUND)
        assert outcome is LocationOutcome.RECORDED
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] == "not_found"
        assert row["address_validation_attempted_at"] is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_hinted_not_found_on_renewal_keeps_row_due(self, pg_conn):
        """A renewal that gets a hinted verdict keeps its old attempt stamp, so
        it stays at the front of the queue for the next run (#191)."""
        addr = "15 RENEW WAY, WOODINVILLE, WA 98072"
        loc_id, old = await self._seed_usps_confirmed(pg_conn, addr, "15 RENEW WAY")
        outcome = await self._run(pg_conn, loc_id, addr, self.NOT_FOUND_RETRYABLE)
        assert outcome is LocationOutcome.RETRY_LATER
        row = await self._row(pg_conn, loc_id)
        assert row["validation_status"] == "confirmed"
        assert row["dpv_match_code"] == "Y"
        assert row["address_validation_attempted_at"] == old

    @pytest.mark.asyncio(loop_scope="session")
    async def test_dpv_less_confirmation_with_retry_warning_writes_nothing(self, pg_conn):
        """USPS was out, so a Google-grade confirmation is not parked for a TTL (#191)."""
        addr = "16 GOOGLE WAY, WOODINVILLE, WA 98072"
        loc_id = await get_or_create_location(pg_conn, addr)
        before = await self._row(pg_conn, loc_id)
        result = {**self.GOOGLE_CONFIRMED, "warnings": [self.RETRY_WARNING]}
        outcome = await self._run(pg_conn, loc_id, addr, result)
        assert outcome is LocationOutcome.RETRY_LATER
        assert await self._row(pg_conn, loc_id) == before

    @pytest.mark.asyncio(loop_scope="session")
    async def test_no_provider_with_retry_warning_is_still_no_answer(self, pg_conn):
        """No provider configured counts toward the breaker, hint or not (#183, #191)."""
        addr = "17 NOBODY WAY, WOODINVILLE, WA 98072"
        loc_id = await get_or_create_location(pg_conn, addr)
        result = {
            "address_line_1": "",
            "validation": {"status": "unavailable", "dpv_match_code": None, "provider": None},
            "warnings": [self.RETRY_WARNING],
        }
        outcome = await self._run(pg_conn, loc_id, addr, result)
        assert outcome is LocationOutcome.NO_ANSWER

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
            return LocationOutcome.WRITTEN

        rows = [
            {"id": loc_ok, "raw_address": "400 GOOD ST, SEATTLE, WA 98101"},
            {"id": loc_bad, "raw_address": "500 BAD ST, SEATTLE, WA 98102"},
            {"id": loc_ok2, "raw_address": "600 FINE ST, SEATTLE, WA 98103"},
        ]

        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator._process_location",
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
                "wslcb_licensing_tracker.address_validator._process_location",
                return_value=LocationOutcome.WRITTEN,
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
            return LocationOutcome.WRITTEN

        rows = [
            {"id": loc_before, "raw_address": "900 BEFORE ST, SEATTLE, WA 98101"},
            {"id": loc_abort, "raw_address": "901 ABORT ST, SEATTLE, WA 98102"},
            {"id": loc_after, "raw_address": "902 AFTER ST, SEATTLE, WA 98103"},
        ]

        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator._process_location",
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

    @staticmethod
    def _rows(n):
        # The breaker never touches the DB, so ids need not exist.
        return [{"id": 900_000 + i, "raw_address": f"{i} ANY ST, SEATTLE, WA"} for i in range(n)]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stops_after_consecutive_no_answers(self, pg_engine, caplog):
        """A provider outage must not burn through the whole batch: unanswered rows
        are invisible to the daily budget, so the breaker bounds the calls (#183)."""
        rows = self._rows(MAX_CONSECUTIVE_NO_ANSWER + 5)
        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator._process_location",
                return_value=LocationOutcome.NO_ANSWER,
            ) as mock_process:
                with caplog.at_level("INFO"):
                    result = await _validate_batch(conn, rows, "Outage", rate_limit=0)
        assert result == 0
        assert mock_process.call_count == MAX_CONSECUTIVE_NO_ANSWER
        # Untried rows are not failures: the summary counts what was attempted.
        done = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Done:")]
        assert done == [f"Done: {MAX_CONSECUTIVE_NO_ANSWER}/{len(rows)} attempted, 0 succeeded"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_any_answer_resets_the_breaker(self, pg_engine):
        streak = [LocationOutcome.NO_ANSWER] * (MAX_CONSECUTIVE_NO_ANSWER - 1)
        outcomes = [*streak, LocationOutcome.RECORDED, *streak, LocationOutcome.WRITTEN]
        rows = self._rows(len(outcomes))
        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator._process_location",
                side_effect=outcomes,
            ) as mock_process:
                result = await _validate_batch(conn, rows, "Flaky", rate_limit=0)
        assert result == 1
        assert mock_process.call_count == len(outcomes)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_retry_later_does_not_trip_the_breaker(self, pg_engine):
        """The validator answered; only its fallback was out. Not an outage (#187)."""
        rows = self._rows(MAX_CONSECUTIVE_NO_ANSWER + 5)
        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator._process_location",
                return_value=LocationOutcome.RETRY_LATER,
            ) as mock_process:
                await _validate_batch(conn, rows, "Fallback out", rate_limit=0)
        assert mock_process.call_count == len(rows)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stops_at_once_when_quota_exhausted(self, pg_engine, caplog):
        """A Retry-After past the cap means a daily quota is out: stop on the first
        one rather than spend MAX_CONSECUTIVE_NO_ANSWER rows finding out (#187)."""
        rows = self._rows(5)
        outcomes = [LocationOutcome.WRITTEN, QuotaExhaustedError(35000.0, HTTP_TOO_MANY_REQUESTS)]
        async with pg_engine.connect() as conn:
            with patch(
                "wslcb_licensing_tracker.address_validator._process_location",
                side_effect=outcomes,
            ) as mock_process:
                with caplog.at_level("INFO"):
                    result = await _validate_batch(conn, rows, "Quota", rate_limit=0)
        assert result == 1
        assert mock_process.call_count == len(outcomes)
        stops = [r for r in caplog.records if r.getMessage().startswith("Stopping:")]
        assert len(stops) == 1
        assert stops[0].levelname == "WARNING"
        # The status is logged: a 5xx past the cap is not necessarily a quota.
        assert "HTTP 429" in stops[0].getMessage()
        assert "35000" in stops[0].getMessage()
        # Not a row failure: no savepoint-rollback warning for it.
        assert not any("Savepoint rollback" in r.getMessage() for r in caplog.records)


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
            return LocationOutcome.WRITTEN

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
                    "wslcb_licensing_tracker.address_validator._process_location",
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
                    "wslcb_licensing_tracker.address_validator._process_location",
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
                    "wslcb_licensing_tracker.address_validator._process_location",
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
                    "wslcb_licensing_tracker.address_validator._process_location",
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
                    "wslcb_licensing_tracker.address_validator._process_location",
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
                    "wslcb_licensing_tracker.address_validator._process_location",
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
    async def test_never_attempted_newest_first(self, pg_engine):
        """Among never-attempted rows the newest location goes first, so a fresh
        scrape's locations are not queued behind old rows that keep getting no
        answer (#187)."""
        from sqlalchemy import func

        from wslcb_licensing_tracker.address_validator import UTC, datetime

        day_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        async with pg_engine.connect() as conn:
            used_before = (
                await conn.execute(
                    select(func.count())
                    .select_from(locations)
                    .where(locations.c.address_validation_attempted_at >= day_start)
                )
            ).scalar_one()
            ids = [
                await get_or_create_location(conn, f"{70 + i} QUEUE WAY, SEATTLE, WA 98107")
                for i in range(3)
            ]
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
                    "wslcb_licensing_tracker.address_validator._process_location",
                    side_effect=mock_process,
                ),
            ):
                await backfill_addresses(conn, rate_limit=0, daily_limit=used_before + 1)
        assert processed == [max(ids)]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_default_daily_limit_is_constant(self):
        assert DAILY_VALIDATION_LIMIT == 5000
