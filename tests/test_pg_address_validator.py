"""Tests for pg_address_validator.py — async address validation DB layer."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select, update

from wslcb_licensing_tracker.models import locations
from wslcb_licensing_tracker.pg_address_validator import (
    DEFAULT_RETRY_AFTER,
    HTTP_TOO_MANY_REQUESTS,
    MAX_RETRIES,
    _parse_retry_after,
    _post_with_retry,
    _validate_batch,
    process_location,
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
                "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.pg_address_validator.validate",
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
                "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
                return_value=False,
            ),
            patch(
                "wslcb_licensing_tracker.pg_address_validator.standardize",
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

    @pytest.mark.asyncio(loop_scope="session")
    async def test_not_confirmed_writes_status_only(self, pg_conn):
        """When /validate returns not_confirmed (no address_line_1), writes
        status and dpv only, returns False."""
        loc_id = await get_or_create_location(pg_conn, "NOWHERE RD, BADTOWN, WA 00000")
        mock_result = {
            "validation": {"status": "not_confirmed", "dpv_match_code": "N"},
            "warnings": [],
        }
        with (
            patch(
                "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.pg_address_validator.validate",
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
                    ).where(locations.c.id == loc_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["validation_status"] == "not_confirmed"
        assert row["address_validated_at"] is None

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
                "wslcb_licensing_tracker.pg_address_validator._is_validation_enabled",
                return_value=True,
            ),
            patch(
                "wslcb_licensing_tracker.pg_address_validator.validate",
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
                "wslcb_licensing_tracker.pg_address_validator.process_location",
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
                "wslcb_licensing_tracker.pg_address_validator.process_location",
                return_value=True,
            ):
                result = await _validate_batch(
                    conn, locs, "Batch commit test", batch_size=2, rate_limit=0
                )

            assert result == 5
