"""Core search and single-record lookup queries for WSLCB licensing tracker.

Contains:
- RECORD_COLUMNS, RECORD_JOINS — shared SQL fragments (imported by
  pg_queries_entity and pg_queries_export)
- _resolve_endorsement_ids() — endorsement alias resolution for filter
- _build_where_clause() — parametric WHERE clause builder
- search_records() — paginated search with filters
- get_record_by_id() — single record with full hydration
- get_related_records() — related records by license number (no hydration)
- get_record_source_link() — record↔source existence check
- get_source_by_id() — source row with source_type slug and label
- get_record_link() — best outcome link for a new_application record
"""

import logging
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .pg_db import outcome_filter_sql
from .pg_queries_hydrate import _hydrate_records

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
        conditions.append(
            "(lr.search_vector @@ plainto_tsquery('english', :q_fts)"
            " OR lr.business_name % :q_trgm"
            " OR lr.applicants % :q_trgm)"
        )
        params["q_fts"] = query
        params["q_trgm"] = query

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
    order_by = (
        "ts_rank(lr.search_vector, plainto_tsquery('english', :q_fts)) DESC,"
        " lr.record_date DESC, lr.id DESC"
        if query
        else "lr.record_date DESC, lr.id DESC"
    )
    rows_result = await conn.execute(
        text(f"{_RECORD_SELECT} {where} ORDER BY {order_by} LIMIT :limit OFFSET :offset"),
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


async def get_record_source_link(conn: AsyncConnection, record_id: int, source_id: int) -> bool:
    """Return True if a record_sources row links record_id to source_id."""
    row = (
        await conn.execute(
            text(
                "SELECT 1 FROM record_sources"
                " WHERE record_id = :record_id AND source_id = :source_id"
            ),
            {"record_id": record_id, "source_id": source_id},
        )
    ).one_or_none()
    return row is not None


async def get_source_by_id(conn: AsyncConnection, source_id: int) -> dict | None:
    """Fetch a single source row joined with its source_type slug and label.

    Returns a dict with keys: id, source_type (slug), source_label, snapshot_path,
    url, captured_at, metadata.  Returns None if not found.
    """
    row = (
        (
            await conn.execute(
                text("""
                    SELECT s.id,
                           st.slug  AS source_type,
                           st.label AS source_label,
                           s.snapshot_path,
                           s.url,
                           s.captured_at,
                           s.metadata
                    FROM sources s
                    JOIN source_types st ON st.id = s.source_type_id
                    WHERE s.id = :source_id
                """),
                {"source_id": source_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    return dict(row) if row is not None else None


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
