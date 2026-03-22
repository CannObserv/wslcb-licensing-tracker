"""Flat record export queries for WSLCB licensing tracker.

Contains:
- export_records() — list export with filters (up to 100k rows)
- export_records_cursor() — streaming async generator for CSV export

Both functions use the same inlined SQL query that bakes in endorsements
via STRING_AGG, outcome links via correlated subqueries, and display-city
fallbacks — all in one query. No entity hydration (unused in CSV output).
"""

import logging
import time
from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .pg_db import DATA_GAP_CUTOFF, LINKABLE_TYPES, PENDING_CUTOFF_DAYS
from .pg_queries_search import _build_where_clause

logger = logging.getLogger(__name__)

_LINKABLE_TYPES_CSV = ", ".join(f"'{t}'" for t in LINKABLE_TYPES)

_EXPORT_SELECT = f"""
    SELECT
        lr.id, lr.section_type, lr.record_date, lr.business_name,
        lr.applicants, lr.license_type, lr.application_type,
        lr.license_number, lr.contact_phone,
        lr.previous_business_name, lr.previous_applicants,
        COALESCE(loc.raw_address, '')  AS business_location,
        COALESCE(loc.std_address_line_1, '') AS std_address_line_1,
        COALESCE(loc.std_address_line_2, '') AS std_address_line_2,
        COALESCE(loc.city, '')         AS city,
        COALESCE(loc.state, 'WA')      AS state,
        COALESCE(loc.zip_code, '')     AS zip_code,
        COALESCE(loc.std_city, '')        AS std_city,
        COALESCE(loc.std_region, '')      AS std_region,
        COALESCE(loc.std_postal_code, '') AS std_postal_code,
        COALESCE(loc.std_country, '')     AS std_country,
        COALESCE(ploc.raw_address, '') AS previous_business_location,
        COALESCE(ploc.std_address_line_1, '') AS prev_std_address_line_1,
        COALESCE(ploc.std_address_line_2, '') AS prev_std_address_line_2,
        COALESCE(ploc.std_city, '')        AS prev_std_city,
        COALESCE(ploc.std_region, '')      AS prev_std_region,
        COALESCE(ploc.std_postal_code, '') AS prev_std_postal_code,
        (
            SELECT STRING_AGG(display_name, '; ' ORDER BY display_name) FROM (
                SELECT COALESCE(canonical.name, le.name) AS display_name
                FROM record_endorsements re
                JOIN license_endorsements le ON le.id = re.endorsement_id
                LEFT JOIN endorsement_aliases ea ON ea.endorsement_id = le.id
                LEFT JOIN license_endorsements canonical
                       ON canonical.id = ea.canonical_endorsement_id
                WHERE re.record_id = lr.id
            ) _endorsements_sub
        ) AS endorsements,
        -- Correlated subqueries for best outcome link
        (
            SELECT rl.days_gap
            FROM record_links rl
            WHERE rl.new_app_id = lr.id
            ORDER BY (rl.confidence = 'high') DESC, rl.id
            LIMIT 1
        ) AS days_to_outcome,
        (
            SELECT olr.record_date
            FROM record_links rl
            JOIN license_records olr ON olr.id = rl.outcome_id
            WHERE rl.new_app_id = lr.id
            ORDER BY (rl.confidence = 'high') DESC, rl.id
            LIMIT 1
        ) AS outcome_date,
        CASE (
            SELECT olr.section_type
            FROM record_links rl
            JOIN license_records olr ON olr.id = rl.outcome_id
            WHERE rl.new_app_id = lr.id
            ORDER BY (rl.confidence = 'high') DESC, rl.id
            LIMIT 1
        )
            WHEN 'approved'     THEN 'approved'
            WHEN 'discontinued' THEN 'discontinued'
            ELSE CASE
                WHEN lr.section_type != 'new_application' THEN NULL
                WHEN lr.application_type NOT IN ({_LINKABLE_TYPES_CSV})
                     THEN NULL
                WHEN lr.application_type = 'NEW APPLICATION'
                     AND lr.record_date > '{DATA_GAP_CUTOFF}' THEN 'data_gap'
                WHEN lr.record_date::date >= CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'
                     THEN 'pending'
                ELSE 'unknown'
            END
        END AS outcome_status
    FROM license_records lr
    LEFT JOIN locations loc  ON loc.id  = lr.location_id
    LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id
"""


async def export_records(  # noqa: PLR0913
    conn: AsyncConnection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",
    endorsements: list[str] | None = None,
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
    limit: int = 100_000,
) -> list[dict]:
    """Lightweight export query returning flat dicts ready for CSV.

    Inlines endorsements (via STRING_AGG), outcome links, and display-city
    fallbacks directly in SQL. Skips entity hydration entirely.
    Returns a plain list of dicts (no total count).
    """
    t0 = time.perf_counter()
    where, params, _ = await _build_where_clause(
        conn,
        query=query,
        section_type=section_type,
        application_type=application_type,
        endorsement=endorsement,
        endorsements=endorsements,
        state=state,
        city=city,
        date_from=date_from,
        date_to=date_to,
        outcome_status=outcome_status,
    )
    result = await conn.execute(
        text(f"{_EXPORT_SELECT} {where} ORDER BY lr.record_date DESC, lr.id DESC LIMIT :limit"),
        {**params, "limit": limit},
    )
    results = [dict(r) for r in result.mappings().all()]
    logger.debug("export_records: %d records, %.3fs", len(results), time.perf_counter() - t0)
    return results


async def export_records_cursor(  # noqa: PLR0913
    conn: AsyncConnection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",
    endorsements: list[str] | None = None,
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
    limit: int = 100_000,
) -> AsyncGenerator[dict, None]:
    """Streaming variant of export_records.

    Yields one dict per row. Accepts the same filter arguments as
    export_records(). Use in HTTP handlers returning a StreamingResponse
    to avoid holding the full result set in memory.
    """
    where, params, _ = await _build_where_clause(
        conn,
        query=query,
        section_type=section_type,
        application_type=application_type,
        endorsement=endorsement,
        endorsements=endorsements,
        state=state,
        city=city,
        date_from=date_from,
        date_to=date_to,
        outcome_status=outcome_status,
    )
    async with conn.stream(
        text(f"{_EXPORT_SELECT} {where} ORDER BY lr.record_date DESC, lr.id DESC LIMIT :limit"),
        {**params, "limit": limit},
    ) as stream:
        async for row in stream.mappings():
            yield dict(row)
