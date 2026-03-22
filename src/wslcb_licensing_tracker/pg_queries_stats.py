"""Dashboard statistics queries for WSLCB licensing tracker.

Contains:
- get_stats() — cached summary statistics (TTL: 60 s)
- _get_pipeline_stats() — application pipeline outcome breakdown
- invalidate_stats_cache() — clear the stats cache; called by
  pg_queries_filter.invalidate_filter_cache()
"""

import logging
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .pg_db import DATA_GAP_CUTOFF, LINKABLE_TYPES, PENDING_CUTOFF_DAYS

logger = logging.getLogger(__name__)

_stats_cache: dict = {}
_STATS_CACHE_TTL = 60  # seconds (1 minute)

_LINKABLE_TYPES_CSV = ", ".join(f"'{t}'" for t in LINKABLE_TYPES)


def invalidate_stats_cache() -> None:
    """Clear the in-process stats cache."""
    _stats_cache.clear()


async def _get_pipeline_stats(conn: AsyncConnection) -> dict:
    """Compute application pipeline outcome breakdown in a single query.

    Uses CTEs for the linked-record subsets and SUM(CASE WHEN ...) so all
    five status counts plus the linkable total are computed in one pass.
    """
    row = (
        (
            await conn.execute(
                text(f"""
        WITH linked AS (
            SELECT new_app_id FROM record_links
        ),
        approved_linked AS (
            SELECT rl.new_app_id
            FROM record_links rl
            JOIN license_records o ON o.id = rl.outcome_id
            WHERE o.section_type = 'approved'
        ),
        discontinued_linked AS (
            SELECT rl.new_app_id
            FROM record_links rl
            JOIN license_records o ON o.id = rl.outcome_id
            WHERE o.section_type = 'discontinued'
        )
        SELECT
            SUM(CASE WHEN lr.section_type = 'new_application'
                AND lr.application_type IN ({_LINKABLE_TYPES_CSV})
                THEN 1 ELSE 0 END) AS total,
            SUM(CASE WHEN lr.id IN (SELECT new_app_id FROM approved_linked)
                THEN 1 ELSE 0 END) AS approved,
            SUM(CASE WHEN lr.id IN (SELECT new_app_id FROM discontinued_linked)
                THEN 1 ELSE 0 END) AS discontinued,
            SUM(CASE WHEN lr.section_type = 'new_application'
                AND lr.application_type IN ({_LINKABLE_TYPES_CSV})
                AND lr.id NOT IN (SELECT new_app_id FROM linked)
                AND lr.record_date::date >= CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'
                AND NOT (lr.application_type = 'NEW APPLICATION'
                         AND lr.record_date > '{DATA_GAP_CUTOFF}')
                THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN lr.section_type = 'new_application'
                AND lr.application_type = 'NEW APPLICATION'
                AND lr.record_date > '{DATA_GAP_CUTOFF}'
                AND lr.id NOT IN (SELECT new_app_id FROM linked)
                THEN 1 ELSE 0 END) AS data_gap,
            SUM(CASE WHEN lr.section_type = 'new_application'
                AND lr.application_type IN ({_LINKABLE_TYPES_CSV})
                AND lr.id NOT IN (SELECT new_app_id FROM linked)
                AND lr.record_date::date < CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'
                AND NOT (lr.application_type = 'NEW APPLICATION'
                         AND lr.record_date > '{DATA_GAP_CUTOFF}')
                THEN 1 ELSE 0 END) AS unknown
        FROM license_records lr
    """)
            )
        )
        .mappings()
        .first()
    )

    return {
        "total": (row["total"] or 0) if row else 0,
        "approved": (row["approved"] or 0) if row else 0,
        "discontinued": (row["discontinued"] or 0) if row else 0,
        "pending": (row["pending"] or 0) if row else 0,
        "data_gap": (row["data_gap"] or 0) if row else 0,
        "unknown": (row["unknown"] or 0) if row else 0,
    }


async def get_stats(conn: AsyncConnection) -> dict:
    """Get summary statistics (cached, 1-min TTL).

    All aggregates are computed in two queries:
    1. A single SELECT over license_records combining section-type counts,
       date range, COUNT(DISTINCT ...), and a scalar subquery for entity count.
    2. A single pipeline query using SUM(CASE WHEN ...) over CTEs.

    A third query fetches the most-recent scrape_log row.
    """
    now = time.monotonic()
    if _stats_cache and now - _stats_cache["ts"] < _STATS_CACHE_TTL:
        return _stats_cache["data"]

    agg_result = await conn.execute(
        text("""
        SELECT
            COUNT(*) AS total_records,
            SUM(CASE WHEN section_type = 'new_application'
                THEN 1 ELSE 0 END) AS new_application_count,
            SUM(CASE WHEN section_type = 'approved' THEN 1 ELSE 0 END) AS approved_count,
            SUM(CASE WHEN section_type = 'discontinued'
                THEN 1 ELSE 0 END) AS discontinued_count,
            MIN(record_date) AS min_date,
            MAX(record_date) AS max_date,
            COUNT(DISTINCT business_name) AS unique_businesses,
            COUNT(DISTINCT license_number) AS unique_licenses,
            (SELECT COUNT(*) FROM entities) AS unique_entities
        FROM license_records
    """)
    )
    agg = agg_result.mappings().first()

    pipeline = await _get_pipeline_stats(conn)

    scrape_result = await conn.execute(text("SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"))
    scrape_row = scrape_result.mappings().first()
    last_scrape = dict(scrape_row) if scrape_row else None

    result = {
        "total_records": agg["total_records"] if agg else 0,
        "new_application_count": agg["new_application_count"] if agg else 0,
        "approved_count": agg["approved_count"] if agg else 0,
        "discontinued_count": agg["discontinued_count"] if agg else 0,
        "date_range": (agg["min_date"], agg["max_date"]) if agg else (None, None),
        "unique_businesses": agg["unique_businesses"] if agg else 0,
        "unique_licenses": agg["unique_licenses"] if agg else 0,
        "unique_entities": agg["unique_entities"] if agg else 0,
        "last_scrape": last_scrape,
        "pipeline": pipeline,
    }
    _stats_cache["data"] = result
    _stats_cache["ts"] = now
    return result
