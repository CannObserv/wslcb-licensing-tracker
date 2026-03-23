"""Filter dropdown data for WSLCB licensing tracker.

Contains:
- get_filter_options() — dropdown data for search page
- get_cities_for_state() — city list for a given state
- invalidate_filter_cache() — legacy no-op, retained for call-site compat

No in-process caching (#99).  The underlying queries are indexed and
run in <10 ms, so the TTL cache was removed to eliminate silent
inconsistency across multiple uvicorn workers.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .pg_db import US_STATES
from .pg_endorsements import get_endorsement_options
from .pg_queries_stats import invalidate_stats_cache
from .pg_substances import get_regulated_substances

logger = logging.getLogger(__name__)

_LOCATION_IDS_SUBQUERY = (
    "SELECT location_id FROM license_records WHERE location_id IS NOT NULL"
    " UNION "
    "SELECT previous_location_id FROM license_records WHERE previous_location_id IS NOT NULL"
)


def invalidate_filter_cache() -> None:
    """No-op retained for backward compatibility.

    Previously cleared in-process TTL caches.  Caches were removed in
    #99 — every call now hits the database directly.  Admin mutation
    call-sites still invoke this; removing those calls is not worth the
    churn.
    """
    invalidate_stats_cache()


async def get_filter_options(conn: AsyncConnection) -> dict:
    """Get distinct values for filter dropdowns.

    Always queries the database — no in-process cache.
    """
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
    return options


async def get_cities_for_state(conn: AsyncConnection, state: str) -> list[str]:
    """Return distinct display cities for locations in *state*.

    Only returns cities from locations referenced by at least one
    license record.  Always queries the database — no in-process cache.
    """
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
    return [r[0] for r in result.fetchall()]
