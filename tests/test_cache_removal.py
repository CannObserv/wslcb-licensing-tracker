"""Tests verifying in-process caches have been removed (#99).

get_filter_options(), get_cities_for_state(), and get_stats() must
always hit the database — no stale in-process copies.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch


def _make_conn_mock():
    """Build an AsyncConnection mock whose execute() is async but fetchall/mappings are sync."""
    conn = AsyncMock()
    result = MagicMock()
    result.fetchall.return_value = []
    result.mappings.return_value.first.return_value = None
    conn.execute.return_value = result
    return conn


class TestNoCacheFilterOptions:
    """get_filter_options always queries DB, never returns stale data."""

    @pytest.mark.asyncio
    async def test_consecutive_calls_hit_db_each_time(self):
        """Two calls should each execute queries — no TTL short-circuit."""
        conn = _make_conn_mock()

        with patch(
            "wslcb_licensing_tracker.pg_queries_filter.get_endorsement_options",
            new_callable=AsyncMock,
            return_value=[],
        ), patch(
            "wslcb_licensing_tracker.pg_queries_filter.get_regulated_substances",
            new_callable=AsyncMock,
            return_value=[],
        ):
            from wslcb_licensing_tracker.pg_queries_filter import get_filter_options

            await get_filter_options(conn)
            call_count_after_first = conn.execute.await_count

            await get_filter_options(conn)
            call_count_after_second = conn.execute.await_count

        # Second call must also execute queries (not return cached)
        assert call_count_after_second > call_count_after_first


class TestNoCacheCities:
    """get_cities_for_state always queries DB."""

    @pytest.mark.asyncio
    async def test_consecutive_calls_hit_db_each_time(self):
        conn = _make_conn_mock()

        from wslcb_licensing_tracker.pg_queries_filter import get_cities_for_state

        await get_cities_for_state(conn, "WA")
        call_count_after_first = conn.execute.await_count

        await get_cities_for_state(conn, "WA")
        call_count_after_second = conn.execute.await_count

        assert call_count_after_second > call_count_after_first


class TestNoCacheStats:
    """get_stats always queries DB."""

    @pytest.mark.asyncio
    async def test_consecutive_calls_hit_db_each_time(self):
        conn = _make_conn_mock()

        # get_stats calls execute 3 times per invocation (agg, pipeline, scrape)
        # We need mappings().first() to return appropriate dicts
        agg_mapping = {
            "total_records": 1, "new_application_count": 0,
            "approved_count": 0, "discontinued_count": 0,
            "min_date": None, "max_date": None,
            "unique_businesses": 0, "unique_licenses": 0,
            "unique_entities": 0,
        }
        pipeline_mapping = {
            "total": 0, "approved": 0, "discontinued": 0,
            "pending": 0, "data_gap": 0, "unknown": 0,
        }

        result_mock = MagicMock()
        result_mock.mappings.return_value.first.side_effect = [
            agg_mapping, pipeline_mapping, None,
            agg_mapping, pipeline_mapping, None,
        ]
        conn.execute.return_value = result_mock

        from wslcb_licensing_tracker.pg_queries_stats import get_stats

        await get_stats(conn)
        call_count_after_first = conn.execute.await_count

        await get_stats(conn)
        call_count_after_second = conn.execute.await_count

        assert call_count_after_second > call_count_after_first


class TestInvalidateFunctionsExist:
    """invalidate_filter_cache and invalidate_stats_cache still callable (no-ops)."""

    def test_invalidate_filter_cache_is_callable(self):
        from wslcb_licensing_tracker.pg_queries_filter import invalidate_filter_cache
        # Should not raise
        invalidate_filter_cache()

    def test_invalidate_stats_cache_is_callable(self):
        from wslcb_licensing_tracker.pg_queries_stats import invalidate_stats_cache
        invalidate_stats_cache()
