"""Async PostgreSQL query functions for WSLCB licensing tracker.

Async equivalent of queries.py. Ports search, filter, stats, and read
queries to SQLAlchemy Core with named parameters for PostgreSQL.

Contains:
- enrich_record() — pure Python display-field helper
- search_records() — paginated search with filters
- export_records() / export_records_cursor() — flat export queries
- get_filter_options() / get_cities_for_state() — dropdown helpers
- get_stats() — dashboard statistics (cached)
- get_record_by_id() / get_related_records() — single-record lookups
- get_entity_records() / get_entities() — entity-centric queries
- invalidate_filter_cache() — cache invalidation
"""

import logging
import time
from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .db import US_STATES
from .display import format_outcome
from .pg_endorsements import get_endorsement_options, get_record_endorsements
from .pg_entities import get_record_entities
from .pg_link_records import (
    DATA_GAP_CUTOFF,
    LINKABLE_TYPES,
    PENDING_CUTOFF_DAYS,
    get_outcome_status,
    get_record_links_bulk,
    outcome_filter_sql,
)
from .pg_substances import get_regulated_substances

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Shared column list and JOINs for all record queries
# ------------------------------------------------------------------

RECORD_COLUMNS = """
    lr.id, lr.section_type, lr.record_date, lr.business_name,
    lr.applicants, lr.license_type, lr.application_type,
    lr.license_number, lr.contact_phone,
    lr.previous_business_name, lr.previous_applicants,
    lr.location_id, lr.previous_location_id,
    lr.has_additional_names,
    lr.scraped_at, lr.created_at,
    COALESCE(loc.raw_address, '') AS business_location,
    COALESCE(loc.city, '') AS city,
    COALESCE(loc.state, 'WA') AS state,
    COALESCE(loc.zip_code, '') AS zip_code,
    COALESCE(loc.std_address_line_1, '') AS std_address_line_1,
    COALESCE(loc.std_address_line_2, '') AS std_address_line_2,
    COALESCE(loc.std_city, '') AS std_city,
    COALESCE(loc.std_region, '') AS std_region,
    COALESCE(loc.std_postal_code, '') AS std_postal_code,
    COALESCE(loc.std_country, '') AS std_country,
    loc.address_standardized_at,
    loc.address_validated_at,
    COALESCE(ploc.raw_address, '') AS previous_business_location,
    COALESCE(ploc.city, '') AS previous_city,
    COALESCE(ploc.state, '') AS previous_state,
    COALESCE(ploc.zip_code, '') AS previous_zip_code,
    COALESCE(ploc.std_address_line_1, '') AS prev_std_address_line_1,
    COALESCE(ploc.std_address_line_2, '') AS prev_std_address_line_2,
    COALESCE(ploc.std_city, '') AS prev_std_city,
    COALESCE(ploc.std_region, '') AS prev_std_region,
    COALESCE(ploc.std_postal_code, '') AS prev_std_postal_code,
    COALESCE(ploc.std_country, '') AS prev_std_country,
    ploc.address_standardized_at AS prev_address_standardized_at,
    ploc.address_validated_at AS prev_address_validated_at"""

RECORD_JOINS = """
    FROM license_records lr
    LEFT JOIN locations loc ON loc.id = lr.location_id
    LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id"""

_RECORD_SELECT = f"SELECT {RECORD_COLUMNS} {RECORD_JOINS}"


# ------------------------------------------------------------------
# Record enrichment helpers
# ------------------------------------------------------------------


def enrich_record(record: dict) -> dict:
    """Add display-ready fields with standardized-first fallback.

    Works with joined query results that include location columns
    aliased as business_location, city, std_city, std_region, std_postal_code, etc.
    """
    record["display_city"] = record.get("std_city") or record.get("city") or ""
    record["display_zip"] = record.get("std_postal_code") or record.get("zip_code") or ""
    record["display_previous_city"] = (
        record.get("prev_std_city") or record.get("previous_city") or ""
    )
    record["display_previous_zip"] = (
        record.get("prev_std_postal_code") or record.get("previous_zip_code") or ""
    )
    return record


async def _hydrate_records(conn: AsyncConnection, rows: list) -> list[dict]:
    """Enrich DB rows/dicts with endorsements, entities, and display fields.

    Accepts dicts or mapping objects. Shared by search_records(),
    get_entity_records(), and get_record_by_id().
    Also attaches outcome_status for new_application records.
    """
    if not rows:
        return []
    record_ids = [r["id"] for r in rows]
    endorsement_map = await get_record_endorsements(conn, record_ids)
    entity_map = await get_record_entities(conn, record_ids)

    # Bulk-fetch outcome links for new_application records
    new_app_ids = [r["id"] for r in rows if r["section_type"] == "new_application"]
    link_map = await get_record_links_bulk(conn, new_app_ids) if new_app_ids else {}

    results = []
    for r in rows:
        d = enrich_record(r if isinstance(r, dict) else dict(r))
        d["endorsements"] = endorsement_map.get(d["id"], [])
        d["entities"] = entity_map.get(d["id"], {"applicant": [], "previous_applicant": []})
        d["outcome_status"] = format_outcome(get_outcome_status(d, link_map.get(d["id"])))
        results.append(d)
    return results


# ------------------------------------------------------------------
# Search and filter queries
# ------------------------------------------------------------------


async def _resolve_endorsement_ids(conn: AsyncConnection, name: str) -> list[int]:
    """Return all endorsement IDs that map to *name* under alias resolution.

    Returns a list with the canonical ID for *name* plus any variant IDs
    that alias to it. Returns an empty list when no endorsement matches.
    """
    result = await conn.execute(
        text("SELECT id FROM license_endorsements WHERE name = :name"),
        {"name": name},
    )
    row = result.first()
    if not row:
        return []
    canonical_id = row[0]
    result2 = await conn.execute(
        text(
            "SELECT endorsement_id FROM endorsement_aliases WHERE canonical_endorsement_id = :cid"
        ),
        {"cid": canonical_id},
    )
    variant_ids = [r[0] for r in result2.fetchall()]
    return [canonical_id, *variant_ids]


async def _build_where_clause(  # noqa: C901, PLR0912, PLR0913
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
) -> tuple[str, dict, bool]:
    """Build a WHERE clause from search/filter parameters.

    Returns (where_sql, params_dict, needs_location_join).
    where_sql includes the WHERE keyword, or is empty when there are no conditions.

    Uses named parameters (:name style) for SQLAlchemy text().
    Endorsement IN clauses inline integer IDs as literals (safe — from DB).
    """
    conditions: list[str] = []
    params: dict = {}
    needs_location_join = False

    if query:
        q_like = f"%{query}%"
        # Phase 5: replace with tsvector @@ plainto_tsquery() for full-text search
        conditions.append(
            "(lr.business_name ILIKE :q_like"
            " OR lr.applicants ILIKE :q_like"
            " OR lr.resolved_endorsements ILIKE :q_like)"
        )
        params["q_like"] = q_like

    if section_type:
        conditions.append("lr.section_type = :section_type")
        params["section_type"] = section_type

    if application_type:
        conditions.append("lr.application_type = :application_type")
        params["application_type"] = application_type

    # Resolve endorsement filter: prefer multi-value list, fall back to scalar.
    _enames: list[str] = (
        endorsements if endorsements is not None else ([endorsement] if endorsement else [])
    )
    if _enames:
        all_eids: list[int] = []
        any_unknown = False
        for ename in _enames:
            ids = await _resolve_endorsement_ids(conn, ename)
            if ids:
                all_eids.extend(ids)
            else:
                any_unknown = True
        if all_eids:
            unique_eids = list(dict.fromkeys(all_eids))
            id_list = ", ".join(str(i) for i in unique_eids)
            conditions.append(
                f"lr.id IN (SELECT record_id FROM record_endorsements"
                f" WHERE endorsement_id IN ({id_list}))"
            )
        elif any_unknown:
            # Every requested endorsement name was unknown — force zero results.
            conditions.append("1 = 0")

    if state:
        needs_location_join = True
        conditions.append(
            "(COALESCE(NULLIF(loc.std_region, ''), loc.state) = :state"
            " OR COALESCE(NULLIF(ploc.std_region, ''), ploc.state) = :state)"
        )
        params["state"] = state

    # City names aren't unique across states, so require a state filter.
    if city and state:
        needs_location_join = True
        conditions.append(
            "(COALESCE(NULLIF(loc.std_city, ''), loc.city) = :city"
            " OR COALESCE(NULLIF(ploc.std_city, ''), ploc.city) = :city)"
        )
        params["city"] = city

    if date_from:
        conditions.append("lr.record_date >= :date_from")
        params["date_from"] = date_from

    if date_to:
        conditions.append("lr.record_date <= :date_to")
        params["date_to"] = date_to

    if outcome_status:
        frags = outcome_filter_sql(outcome_status, record_alias="lr")
        conditions.extend(frags)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params, needs_location_join


async def search_records(  # noqa: PLR0913
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
    page: int = 1,
    per_page: int = 50,
) -> tuple[list[dict], int]:
    """Search records with filters. Returns (records, total_count)."""
    t0 = time.perf_counter()
    where, params, needs_location_join = await _build_where_clause(
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

    # Only JOIN locations in the count query when needed (state/city filter).
    if needs_location_join:
        count_sql = (
            "SELECT COUNT(*) FROM license_records lr"
            " LEFT JOIN locations loc ON loc.id = lr.location_id"
            " LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id"
            f" {where}"
        )
    else:
        count_sql = f"SELECT COUNT(*) FROM license_records lr {where}"

    count_result = await conn.execute(text(count_sql), params)
    total = count_result.scalar_one()

    offset = (page - 1) * per_page
    rows_result = await conn.execute(
        text(
            f"{_RECORD_SELECT} {where}"
            " ORDER BY lr.record_date DESC, lr.id DESC"
            " LIMIT :limit OFFSET :offset"
        ),
        {**params, "limit": per_page, "offset": offset},
    )
    rows = [dict(r) for r in rows_result.mappings().all()]
    results = await _hydrate_records(conn, rows)
    logger.debug(
        "search_records: %d/%d records, page %d, %.3fs",
        len(results),
        total,
        page,
        time.perf_counter() - t0,
    )
    return results, total


# ------------------------------------------------------------------
# Lightweight export query
# ------------------------------------------------------------------

_LINKABLE_TYPES_CSV = ", ".join(f"'{t}'" for t in LINKABLE_TYPES)

# SQL constant for the export query — PostgreSQL version.
# Bakes in DATA_GAP_CUTOFF, PENDING_CUTOFF_DAYS, and LINKABLE_TYPES at import time.
# Inlines endorsements via STRING_AGG, outcome links via correlated subqueries,
# and display-city fallbacks — all in one query.
# Skips entity hydration entirely (unused in CSV output).
#
# Phase 5: replace endorsement subquery with tsvector search for full-text queries
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
    logger.debug(
        "export_records: %d records, %.3fs",
        len(results),
        time.perf_counter() - t0,
    )
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
    async with await conn.stream(
        text(f"{_EXPORT_SELECT} {where} ORDER BY lr.record_date DESC, lr.id DESC LIMIT :limit"),
        {**params, "limit": limit},
    ) as stream:
        async for row in stream.mappings():
            yield dict(row)


# ------------------------------------------------------------------
# Filter option caches
# ------------------------------------------------------------------

# In-process cache for filter dropdown options.
_filter_cache: dict = {}
_FILTER_CACHE_TTL = 300  # seconds (5 minutes)

# Short-TTL cache for dashboard stats.
_stats_cache: dict = {}
_STATS_CACHE_TTL = 60  # seconds (1 minute)

# Per-state city list cache.
_city_cache: dict[str, tuple[float, list[str]]] = {}


def invalidate_filter_cache() -> None:
    """Clear all in-process filter caches.

    Call after any admin mutation that changes endorsements, regulated
    substances, or locations so the next search page load reflects the
    current state rather than a stale snapshot.
    """
    _filter_cache.clear()
    _city_cache.clear()
    _stats_cache.clear()


_LOCATION_IDS_SUBQUERY = (
    "SELECT location_id FROM license_records WHERE location_id IS NOT NULL"
    " UNION "
    "SELECT previous_location_id FROM license_records WHERE previous_location_id IS NOT NULL"
)


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


# ------------------------------------------------------------------
# Dashboard statistics
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# Single-record lookups
# ------------------------------------------------------------------


async def get_record_by_id(conn: AsyncConnection, record_id: int) -> dict | None:
    """Fetch a single record with location data joined and hydrated."""
    result = await conn.execute(
        text(f"{_RECORD_SELECT} WHERE lr.id = :id"),
        {"id": record_id},
    )
    row = result.mappings().first()
    if row is None:
        return None
    rows = [dict(row)]
    hydrated = await _hydrate_records(conn, rows)
    return hydrated[0]


async def get_related_records(conn: AsyncConnection, record: dict) -> list[dict]:
    """Fetch other records for the same license number."""
    result = await conn.execute(
        text(
            f"{_RECORD_SELECT}"
            " WHERE lr.license_number = :ln AND lr.id != :id"
            " ORDER BY lr.record_date DESC"
        ),
        {"ln": record["license_number"], "id": record["id"]},
    )
    return [dict(r) for r in result.mappings().all()]


async def get_entity_records(conn: AsyncConnection, entity_id: int) -> list[dict]:
    """Fetch all records associated with an entity, with location data."""
    result = await conn.execute(
        text(
            f"SELECT DISTINCT {RECORD_COLUMNS} {RECORD_JOINS}"
            " JOIN record_entities re ON re.record_id = lr.id"
            " WHERE re.entity_id = :entity_id"
            " ORDER BY lr.record_date DESC, lr.id DESC"
        ),
        {"entity_id": entity_id},
    )
    rows = [dict(r) for r in result.mappings().all()]
    return await _hydrate_records(conn, rows)


async def get_entities(  # noqa: PLR0913
    conn: AsyncConnection,
    *,
    q: str | None = None,
    entity_type: str | None = None,
    sort: str = "count",
    page: int = 1,
    per_page: int = 50,
) -> dict:
    """Return a paginated, searchable list of all entities.

    Parameters
    ----------
    q:
        Optional name substring (case-insensitive ILIKE search).
    entity_type:
        Optional filter: 'person', 'organization', or None for all.
    sort:
        'count' (default) — most active first (record_count DESC, name ASC);
        'name' — alphabetical (name ASC).
    page:
        1-based page number.
    per_page:
        Rows per page (default 50).

    Returns a dict with keys ``entities`` (list of row dicts) and ``total`` (int).
    """
    where_parts = []
    params: dict = {}
    if q:
        where_parts.append("e.name ILIKE :q_like")
        params["q_like"] = f"%{q}%"
    if entity_type is not None:
        where_parts.append("e.entity_type = :entity_type")
        params["entity_type"] = entity_type

    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    order_clause = (
        "ORDER BY record_count DESC, e.name ASC" if sort != "name" else "ORDER BY e.name ASC"
    )

    base_sql = f"""
        SELECT e.id, e.name, e.entity_type, COUNT(re.record_id) AS record_count
        FROM entities e
        LEFT JOIN record_entities re ON re.entity_id = e.id
        {where_clause}
        GROUP BY e.id
    """

    total_result = await conn.execute(
        text(f"SELECT COUNT(*) FROM ({base_sql}) sub"),
        params,
    )
    total = total_result.scalar_one()

    page = max(1, page)
    offset = (page - 1) * per_page
    rows_result = await conn.execute(
        text(f"{base_sql} {order_clause} LIMIT :limit OFFSET :offset"),
        {**params, "limit": per_page, "offset": offset},
    )
    return {"entities": [dict(r) for r in rows_result.mappings().all()], "total": total}


async def get_record_link(
    conn: AsyncConnection,
    record_id: int,
) -> dict | None:
    """Fetch the best outcome link for a new_application record.

    Returns a dict with outcome_id, confidence, days_gap,
    outcome_date, outcome_section_type, or None.
    """
    row = (
        (
            await conn.execute(
                text("""
                SELECT rl.outcome_id, rl.confidence, rl.days_gap,
                       lr.record_date AS outcome_date,
                       lr.section_type AS outcome_section_type
                FROM record_links rl
                JOIN license_records lr ON lr.id = rl.outcome_id
                WHERE rl.new_app_id = :record_id
                ORDER BY (rl.confidence = 'high') DESC
                LIMIT 1
            """),
                {"record_id": record_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    return dict(row) if row else None


# Public alias for external callers (e.g. app.py record_detail route).
hydrate_records = _hydrate_records
