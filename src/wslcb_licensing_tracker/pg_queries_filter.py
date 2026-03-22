"""Filter dropdown data and TTL caches for WSLCB licensing tracker.

Contains:
- get_filter_options() — cached dropdown data for search page
- get_cities_for_state() — cached city list for a given state
- invalidate_filter_cache() — clear all query-layer caches (filter,
  city, and stats)
"""

import logging
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .pg_db import US_STATES
from .pg_endorsements import get_endorsement_options
from .pg_queries_stats import invalidate_stats_cache
from .pg_substances import get_regulated_substances

logger = logging.getLogger(__name__)

# In-process cache for filter dropdown options.
_filter_cache: dict = {}
_FILTER_CACHE_TTL = 300  # seconds (5 minutes)

# Per-state city list cache.
_city_cache: dict[str, tuple[float, list[str]]] = {}

_LOCATION_IDS_SUBQUERY = (
    "SELECT location_id FROM license_records WHERE location_id IS NOT NULL"
    " UNION "
    "SELECT previous_location_id FROM license_records WHERE previous_location_id IS NOT NULL"
)


def invalidate_filter_cache() -> None:
    """Clear all in-process filter caches.

    Call after any admin mutation that changes endorsements, regulated
    substances, or locations so the next search page load reflects the
    current state rather than a stale snapshot.
    """
    _filter_cache.clear()
    _city_cache.clear()
    invalidate_stats_cache()


async def get_filter_options(conn: AsyncConnection) -> dict:
    """Get distinct values for filter dropdowns (cached, 5-min TTL)."""
    now = time.monotonic()
    if _filter_cache and now - _filter_cache["ts"] < _FILTER_CACHE_TTL:
        return _filter_cache["data"]

    options: dict = {}
    for col in ["section_type", "application_type"]:
        result = await conn.execute(
            text(
                f"SELECT DISTINCT {col} FROM license_records"
                f" WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col}"
            )
        )
        options[col] = [r[0] for r in result.fetchall()]

    # States: only valid US state codes that appear in the data.
    state_keys = ", ".join(f"'{k}'" for k in US_STATES)
    result = await conn.execute(
        text(f"""
            SELECT DISTINCT display_state FROM (
                SELECT COALESCE(NULLIF(l.std_region, ''), l.state) AS display_state
                FROM locations l
                WHERE l.id IN ({_LOCATION_IDS_SUBQUERY})
            ) s WHERE display_state IN ({state_keys})
            ORDER BY display_state
        """)
    )
    options["state"] = [{"code": r[0], "name": US_STATES[r[0]]} for r in result.fetchall()]

    options["endorsement"] = await get_endorsement_options(conn)
    options["regulated_substance"] = await get_regulated_substances(conn)
    _filter_cache["data"] = options
    _filter_cache["ts"] = now
    return options


async def get_cities_for_state(conn: AsyncConnection, state: str) -> list[str]:
    """Return distinct display cities for locations in *state*.

    Only returns cities from locations referenced by at least one
    license record. Results are cached for _FILTER_CACHE_TTL seconds per state.
    """
    now = time.monotonic()
    cached = _city_cache.get(state)
    if cached and now - cached[0] < _FILTER_CACHE_TTL:
        return cached[1]

    result = await conn.execute(
        text(f"""
            SELECT DISTINCT display_city FROM (
                SELECT COALESCE(NULLIF(l.std_city, ''), l.city) AS display_city,
                       COALESCE(NULLIF(l.std_region, ''), l.state) AS display_state
                FROM locations l
                WHERE l.id IN ({_LOCATION_IDS_SUBQUERY})
            ) s WHERE display_state = :state
              AND display_city IS NOT NULL AND display_city != ''
            ORDER BY display_city
        """),
        {"state": state},
    )
    cities = [r[0] for r in result.fetchall()]
    _city_cache[state] = (now, cities)
    return cities
