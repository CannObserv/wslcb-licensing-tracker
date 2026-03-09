"""Query functions for WSLCB licensing tracker.

Contains search, filter, stats, and read queries that combine data from
multiple tables (records, locations, endorsements, entities).  Thin
read layer on top of the core schema in ``schema.py``.

Record insertion lives in ``pipeline.py`` (``insert_record``); source
provenance queries live in ``db.py`` (``get_primary_source``,
``get_record_sources``); the US state constant lives in ``db.py``
(``US_STATES``).

All three are re-exported here for backward compatibility.
"""
import logging
import sqlite3
import time
from collections.abc import Iterator

from .endorsements import (
    get_endorsement_options, get_record_endorsements, get_regulated_substances,
)
from .entities import (
    get_record_entities,
)
from .db import (  # noqa: F401 — re-exports
    US_STATES,
    get_primary_source,
    get_record_sources,
)
from .pipeline import insert_record  # noqa: F401 — re-export
from .display import format_outcome
from .link_records import (
    get_outcome_status,
    PENDING_CUTOFF_DAYS,
    DATA_GAP_CUTOFF,
    LINKABLE_TYPES,
    outcome_filter_sql,
)

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
    COALESCE(loc.address_line_1, '') AS address_line_1,
    COALESCE(loc.address_line_2, '') AS address_line_2,
    COALESCE(loc.std_city, '') AS std_city,
    COALESCE(loc.std_region, '') AS std_region,
    COALESCE(loc.std_postal_code, '') AS std_postal_code,
    COALESCE(loc.std_country, '') AS std_country,
    loc.address_validated_at,
    COALESCE(ploc.raw_address, '') AS previous_business_location,
    COALESCE(ploc.city, '') AS previous_city,
    COALESCE(ploc.state, '') AS previous_state,
    COALESCE(ploc.zip_code, '') AS previous_zip_code,
    COALESCE(ploc.address_line_1, '') AS prev_address_line_1,
    COALESCE(ploc.address_line_2, '') AS prev_address_line_2,
    COALESCE(ploc.std_city, '') AS prev_std_city,
    COALESCE(ploc.std_region, '') AS prev_std_region,
    COALESCE(ploc.std_postal_code, '') AS prev_std_postal_code,
    COALESCE(ploc.std_country, '') AS prev_std_country,
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
    record["display_previous_city"] = record.get("prev_std_city") or record.get("previous_city") or ""
    record["display_previous_zip"] = record.get("prev_std_postal_code") or record.get("previous_zip_code") or ""
    return record


def hydrate_records(
    conn: sqlite3.Connection, rows: list,
) -> list[dict]:
    """Enrich DB rows/dicts with endorsements, entities, and display fields.

    Accepts sqlite3.Row objects or plain dicts.  Shared by
    search_records(), get_entity_records(), and app.py record_detail().
    Also attaches ``outcome_status`` for new_application records.
    """
    if not rows:
        return []
    record_ids = [r["id"] for r in rows]
    endorsement_map = get_record_endorsements(conn, record_ids)
    entity_map = get_record_entities(conn, record_ids)

    # Bulk-fetch outcome links for new_application records
    new_app_ids = [
        r["id"] for r in rows if r["section_type"] == "new_application"
    ]
    link_map = get_record_links_bulk(conn, new_app_ids) if new_app_ids else {}

    results = []
    for r in rows:
        d = enrich_record(r if isinstance(r, dict) else dict(r))
        d["endorsements"] = endorsement_map.get(d["id"], [])
        d["entities"] = entity_map.get(
            d["id"], {"applicant": [], "previous_applicant": []}
        )
        d["outcome_status"] = format_outcome(get_outcome_status(
            d, link_map.get(d["id"]),
        ))
        results.append(d)
    return results


# ------------------------------------------------------------------
# Search and filter queries
# ------------------------------------------------------------------

def _resolve_endorsement_ids(
    conn: sqlite3.Connection,
    name: str,
) -> list[int]:
    """Return all endorsement IDs that map to *name* under alias resolution.

    Returns a list containing the direct endorsement ID for *name* plus any
    variant IDs that alias to it via ``endorsement_aliases``.  Uses
    ``idx_re_endorsement`` for fast record lookups — never scans
    ``record_endorsements``.

    Returns an empty list when no endorsement with that name exists.
    """
    row = conn.execute(
        "SELECT id FROM license_endorsements WHERE name = ?",
        (name,),
    ).fetchone()
    if not row:
        return []
    canonical_id = row[0]
    variant_ids = [
        r[0] for r in conn.execute(
            "SELECT endorsement_id FROM endorsement_aliases"
            " WHERE canonical_endorsement_id = ?",
            (canonical_id,),
        ).fetchall()
    ]
    return [canonical_id] + variant_ids


def _build_where_clause(
    conn: sqlite3.Connection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",       # legacy single-value (kept for compat)
    endorsements: list[str] | None = None,  # multi-value; supersedes endorsement
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
) -> tuple[str, list, bool]:
    """Build a WHERE clause from search/filter parameters.

    Returns ``(where_sql, params, needs_location_join)``.
    ``where_sql`` includes the ``WHERE`` keyword, or is empty when
    there are no conditions.

    The endorsement filter accepts either the legacy scalar *endorsement*
    parameter or the new *endorsements* list.  When *endorsements* is
    provided it takes precedence; when *endorsement* is provided as a
    non-empty string it is wrapped in a single-element list for uniform
    handling.  An empty list means "no endorsement filter".

    Filtering is alias-aware: canonical endorsements automatically include
    records linked to any of their variants.  OR semantics apply across
    multiple endorsement names — records matching *any* of the requested
    endorsements are returned.

    **Mixed known/unknown names:** if the list contains both recognised and
    unrecognised endorsement names, the filter matches records linked to
    the recognised names and silently ignores the unknown ones.  Only
    when *every* name in the list is unknown does the filter force zero
    results (``1 = 0``).
    """
    conditions: list[str] = []
    params: list = []
    needs_location_join = False

    if query:
        safe_query = query.replace('"', '').replace("'", "")
        terms = safe_query.split()
        fts_query = " AND ".join(f'"{ t }"*' for t in terms if t)
        if fts_query:
            conditions.append(
                "lr.id IN (SELECT rowid FROM license_records_fts WHERE license_records_fts MATCH ?)"
            )
            params.append(fts_query)

    if section_type:
        conditions.append("lr.section_type = ?")
        params.append(section_type)

    if application_type:
        conditions.append("lr.application_type = ?")
        params.append(application_type)

    # Resolve endorsement filter: prefer multi-value list, fall back to scalar.
    _enames: list[str] = endorsements if endorsements is not None else (
        [endorsement] if endorsement else []
    )
    if _enames:
        all_eids: list[int] = []
        any_unknown = False
        for ename in _enames:
            ids = _resolve_endorsement_ids(conn, ename)
            if ids:
                all_eids.extend(ids)
            else:
                any_unknown = True
        if all_eids:
            # De-duplicate IDs before building the IN clause.
            unique_eids = list(dict.fromkeys(all_eids))
            placeholders = ",".join("?" * len(unique_eids))
            conditions.append(
                f"lr.id IN ("
                f"SELECT record_id FROM record_endorsements"
                f" WHERE endorsement_id IN ({placeholders}))"
            )
            params.extend(unique_eids)
        elif any_unknown:
            # Every requested endorsement name was unknown — force zero results.
            conditions.append("1 = 0")

    if state:
        needs_location_join = True
        conditions.append(
            "(COALESCE(NULLIF(loc.std_region, ''), loc.state) = ?"
            " OR COALESCE(NULLIF(ploc.std_region, ''), ploc.state) = ?)"
        )
        params.extend([state, state])

    # City names aren't unique across states, so require a state filter.
    if city and state:
        needs_location_join = True
        conditions.append(
            "(COALESCE(NULLIF(loc.std_city, ''), loc.city) = ?"
            " OR COALESCE(NULLIF(ploc.std_city, ''), ploc.city) = ?)"
        )
        params.extend([city, city])

    if date_from:
        conditions.append("lr.record_date >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("lr.record_date <= ?")
        params.append(date_to)

    if outcome_status:
        conditions.extend(outcome_filter_sql(outcome_status))

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params, needs_location_join


def search_records(
    conn: sqlite3.Connection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",           # legacy scalar; use endorsements for multi-select
    endorsements: list[str] | None = None,
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
    page: int = 1,
    per_page: int = 50,
) -> tuple[list[dict], int]:
    """Search records with filters.  Returns (records, total_count)."""
    t0 = time.perf_counter()
    where, params, needs_location_join = _build_where_clause(
        conn,
        query=query, section_type=section_type,
        application_type=application_type, endorsement=endorsement,
        endorsements=endorsements,
        state=state, city=city, date_from=date_from, date_to=date_to,
        outcome_status=outcome_status,
    )

    # Only JOIN locations in the count query when needed (state/city filter).
    if needs_location_join:
        count_sql = f"""
            SELECT COUNT(*) FROM license_records lr
            LEFT JOIN locations loc ON loc.id = lr.location_id
            LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id
            {where}
        """
    else:
        count_sql = f"SELECT COUNT(*) FROM license_records lr {where}"
    total = conn.execute(count_sql, params).fetchone()[0]

    offset = (page - 1) * per_page
    rows = conn.execute(
        f"""{_RECORD_SELECT}
            {where}
            ORDER BY lr.record_date DESC, lr.id DESC
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    results = hydrate_records(conn, rows)
    logger.debug(
        "search_records: %d/%d records, page %d, %.3fs",
        len(results), total, page, time.perf_counter() - t0,
    )
    return results, total


# ------------------------------------------------------------------
# Lightweight export query
# ------------------------------------------------------------------

# Module-level constants derived from link_records — computed once so
# _EXPORT_SELECT can be built as a true constant without .format() calls.
_LINKABLE_TYPES_CSV = ", ".join(f"'{t}'" for t in LINKABLE_TYPES)

# SQL constant for the export query: bakes in DATA_GAP_CUTOFF,
# PENDING_CUTOFF_DAYS, and LINKABLE_TYPES at import time (no runtime
# .format()).  Inlines endorsements via GROUP_CONCAT, outcome links via
# correlated subqueries, and display-city fallbacks — all in one query.
# Skips entity hydration entirely (unused in CSV output).
#
# outcome_status uses CASE (subquery) WHEN form to evaluate the
# section_type lookup once rather than twice.
_EXPORT_SELECT = f"""
    SELECT
        lr.id, lr.section_type, lr.record_date, lr.business_name,
        lr.applicants, lr.license_type, lr.application_type,
        lr.license_number, lr.contact_phone,
        lr.previous_business_name, lr.previous_applicants,
        COALESCE(loc.raw_address, '')  AS business_location,
        COALESCE(loc.address_line_1, '') AS address_line_1,
        COALESCE(loc.address_line_2, '') AS address_line_2,
        COALESCE(loc.city, '')         AS city,
        COALESCE(loc.state, 'WA')      AS state,
        COALESCE(loc.zip_code, '')     AS zip_code,
        COALESCE(loc.std_city, '')        AS std_city,
        COALESCE(loc.std_region, '')      AS std_region,
        COALESCE(loc.std_postal_code, '') AS std_postal_code,
        COALESCE(loc.std_country, '')     AS std_country,
        COALESCE(ploc.raw_address, '') AS previous_business_location,
        COALESCE(ploc.address_line_1, '') AS prev_address_line_1,
        COALESCE(ploc.address_line_2, '') AS prev_address_line_2,
        COALESCE(ploc.std_city, '')        AS prev_std_city,
        COALESCE(ploc.std_region, '')      AS prev_std_region,
        COALESCE(ploc.std_postal_code, '') AS prev_std_postal_code,
        (
            SELECT GROUP_CONCAT(display_name, '; ') FROM (
                SELECT COALESCE(canonical.name, le.name) AS display_name
                FROM record_endorsements re
                JOIN license_endorsements le ON le.id = re.endorsement_id
                LEFT JOIN endorsement_aliases ea ON ea.endorsement_id = le.id
                LEFT JOIN license_endorsements canonical
                       ON canonical.id = ea.canonical_endorsement_id
                WHERE re.record_id = lr.id
                ORDER BY display_name
            )
        ) AS endorsements,
        -- Correlated subqueries for best outcome link (one index seek per row via
        -- idx_record_links_new; avoids materialising the full record_links table
        -- before the outer WHERE is applied).
        (
            SELECT rl.days_gap
            FROM record_links rl
            WHERE rl.new_app_id = lr.id
            ORDER BY rl.confidence = 'high' DESC, rl.rowid
            LIMIT 1
        ) AS days_to_outcome,
        (
            SELECT olr.record_date
            FROM record_links rl
            JOIN license_records olr ON olr.id = rl.outcome_id
            WHERE rl.new_app_id = lr.id
            ORDER BY rl.confidence = 'high' DESC, rl.rowid
            LIMIT 1
        ) AS outcome_date,
        -- CASE expr form evaluates the subquery once; WHEN/THEN branches only
        -- need the already-computed value, so SQLite runs one index seek here
        -- instead of two.
        CASE (
            SELECT olr.section_type
            FROM record_links rl
            JOIN license_records olr ON olr.id = rl.outcome_id
            WHERE rl.new_app_id = lr.id
            ORDER BY rl.confidence = 'high' DESC, rl.rowid
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
                WHEN lr.record_date >= date('now', '-{PENDING_CUTOFF_DAYS} days')
                     THEN 'pending'
                ELSE 'unknown'
            END
        END AS outcome_status
    FROM license_records lr
    LEFT JOIN locations loc  ON loc.id  = lr.location_id
    LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id
"""


def export_records(
    conn: sqlite3.Connection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",           # legacy scalar
    endorsements: list[str] | None = None,
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
    limit: int = 100_000,
) -> list[dict]:
    """Lightweight export query returning flat dicts ready for CSV.

    Unlike :func:`search_records`, this function inlines endorsements
    (via ``GROUP_CONCAT``), outcome links, and display-city fallbacks
    directly in SQL and skips entity hydration entirely.  Returns a
    plain list of dicts (no total count).
    """
    t0 = time.perf_counter()
    where, params, _ = _build_where_clause(
        conn,
        query=query, section_type=section_type,
        application_type=application_type, endorsement=endorsement,
        endorsements=endorsements,
        state=state, city=city, date_from=date_from, date_to=date_to,
        outcome_status=outcome_status,
    )

    rows = conn.execute(
        f"""{_EXPORT_SELECT}
            {where}
            ORDER BY lr.record_date DESC, lr.id DESC
            LIMIT ?""",
        params + [limit],
    ).fetchall()

    results = [dict(r) for r in rows]
    logger.debug(
        "export_records: %d records, %.3fs",
        len(results), time.perf_counter() - t0,
    )
    return results


def export_records_cursor(
    conn: sqlite3.Connection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",           # legacy scalar
    endorsements: list[str] | None = None,
    state: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    outcome_status: str = "",
    limit: int = 100_000,
) -> Iterator[dict]:
    """Streaming variant of :func:`export_records`.

    Yields one ``dict`` per row directly from the SQLite cursor without
    materialising all rows into a list.  Accepts the same filter
    arguments as :func:`export_records`.

    Use this in HTTP handlers that return a ``StreamingResponse`` to
    avoid holding the full result set in memory.
    """
    where, params, _ = _build_where_clause(
        conn,
        query=query, section_type=section_type,
        application_type=application_type, endorsement=endorsement,
        endorsements=endorsements,
        state=state, city=city, date_from=date_from, date_to=date_to,
        outcome_status=outcome_status,
    )
    cursor = conn.execute(
        f"""{_EXPORT_SELECT}
            {where}
            ORDER BY lr.record_date DESC, lr.id DESC
            LIMIT ?""",
        params + [limit],
    )
    for row in cursor:
        yield dict(row)


# In-process cache for filter dropdown options.  The underlying data
# changes at most twice daily (scraper runs), so a short TTL avoids
# running the ~10 ms city-list query on every search page load.
_filter_cache: dict = {}  # {"data": ..., "ts": float}
_FILTER_CACHE_TTL = 300  # seconds (5 minutes)


def invalidate_filter_cache() -> None:
    """Clear the in-process filter option cache.

    Call after any admin mutation that changes endorsements or regulated
    substances so the next search page load reflects the current state
    rather than a stale snapshot.
    """
    _filter_cache.clear()


_LOCATION_IDS_SUBQUERY = (
    "SELECT location_id FROM license_records WHERE location_id IS NOT NULL"
    " UNION "
    "SELECT previous_location_id FROM license_records WHERE previous_location_id IS NOT NULL"
)


def get_filter_options(conn: sqlite3.Connection) -> dict:
    """Get distinct values for filter dropdowns (cached, 5-min TTL)."""
    now = time.monotonic()
    if _filter_cache and now - _filter_cache["ts"] < _FILTER_CACHE_TTL:
        return _filter_cache["data"]
    options: dict = {}
    for col in ["section_type", "application_type"]:
        rows = conn.execute(
            f"SELECT DISTINCT {col} FROM license_records "
            f"WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col}"
        ).fetchall()
        options[col] = [r[0] for r in rows]

    # States: only valid US state codes that appear in the data.
    rows = conn.execute(
        f"SELECT DISTINCT display_state FROM ("
        f"  SELECT COALESCE(NULLIF(l.std_region, ''), l.state) AS display_state"
        f"  FROM locations l"
        f"  WHERE l.id IN ({_LOCATION_IDS_SUBQUERY})"
        f") WHERE display_state IN ({','.join('?' for _ in US_STATES)})"
        f" ORDER BY display_state",
        list(US_STATES.keys()),
    ).fetchall()
    options["state"] = [
        {"code": r[0], "name": US_STATES[r[0]]} for r in rows
    ]

    options["endorsement"] = get_endorsement_options(conn)
    options["regulated_substance"] = get_regulated_substances(conn)
    _filter_cache["data"] = options
    _filter_cache["ts"] = now
    return options


# Per-state city list cache.  Same TTL as filter options; keyed by
# state code so each state is fetched at most once per window.
_city_cache: dict[str, tuple[float, list[str]]] = {}  # {state: (ts, cities)}


def get_cities_for_state(
    conn: sqlite3.Connection, state: str,
) -> list[str]:
    """Return distinct display cities for locations in *state*.

    Only returns cities from locations referenced by at least one
    license record.  Results are cached for ``_FILTER_CACHE_TTL``
    seconds per state.
    """
    now = time.monotonic()
    cached = _city_cache.get(state)
    if cached and now - cached[0] < _FILTER_CACHE_TTL:
        return cached[1]

    rows = conn.execute(
        f"SELECT DISTINCT display_city FROM ("
        f"  SELECT COALESCE(NULLIF(l.std_city, ''), l.city) AS display_city,"
        f"         COALESCE(NULLIF(l.std_region, ''), l.state) AS display_state"
        f"  FROM locations l"
        f"  WHERE l.id IN ({_LOCATION_IDS_SUBQUERY})"
        f") WHERE display_state = ? AND display_city IS NOT NULL"
        f"  AND display_city != '' ORDER BY display_city",
        (state,),
    ).fetchall()
    cities = [r[0] for r in rows]
    _city_cache[state] = (now, cities)
    return cities


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get summary statistics.

    Cheap aggregates (COUNT, SUM, MIN, MAX) are combined into a single
    query.  The two COUNT(DISTINCT ...) calls remain separate because
    combining them forces a slower full-table scan in SQLite.
    """
    agg = conn.execute("""
        SELECT
            COUNT(*) AS total_records,
            SUM(CASE WHEN section_type = 'new_application' THEN 1 ELSE 0 END) AS new_application_count,
            SUM(CASE WHEN section_type = 'approved' THEN 1 ELSE 0 END) AS approved_count,
            SUM(CASE WHEN section_type = 'discontinued' THEN 1 ELSE 0 END) AS discontinued_count,
            MIN(record_date) AS min_date,
            MAX(record_date) AS max_date
        FROM license_records
    """).fetchone()

    # Application pipeline stats (record_links)
    pipeline = _get_pipeline_stats(conn)

    return {
        "total_records": agg["total_records"],
        "new_application_count": agg["new_application_count"],
        "approved_count": agg["approved_count"],
        "discontinued_count": agg["discontinued_count"],
        "date_range": (agg["min_date"], agg["max_date"]),
        "unique_businesses": conn.execute(
            "SELECT COUNT(DISTINCT business_name) FROM license_records"
        ).fetchone()[0],
        "unique_licenses": conn.execute(
            "SELECT COUNT(DISTINCT license_number) FROM license_records"
        ).fetchone()[0],
        "unique_entities": conn.execute(
            "SELECT COUNT(*) FROM entities"
        ).fetchone()[0],
        "last_scrape": conn.execute(
            "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
        ).fetchone(),
        "pipeline": pipeline,
    }


def _get_pipeline_stats(conn: sqlite3.Connection) -> dict:
    """Compute application pipeline outcome breakdown."""
    total = conn.execute(f"""
        SELECT COUNT(*) FROM license_records
        WHERE section_type = 'new_application'
          AND application_type IN ({_LINKABLE_TYPES_CSV})
    """).fetchone()[0]

    counts = {}
    for status in ("approved", "discontinued", "pending", "data_gap", "unknown"):
        clauses = outcome_filter_sql(status, record_alias="lr")
        where = " AND ".join(clauses)
        counts[status] = conn.execute(
            f"SELECT COUNT(*) FROM license_records lr WHERE {where}"
        ).fetchone()[0]

    return {
        "total": total,
        **counts,
    }


# ------------------------------------------------------------------
# Single-record lookups
# ------------------------------------------------------------------

def get_record_by_id(conn: sqlite3.Connection, record_id: int) -> dict | None:
    """Fetch a single record with location data joined."""
    row = conn.execute(
        f"{_RECORD_SELECT} WHERE lr.id = ?", (record_id,)
    ).fetchone()
    return dict(row) if row else None


def get_related_records(
    conn: sqlite3.Connection, license_number: str, exclude_id: int
) -> list[dict]:
    """Fetch other records for the same license number."""
    rows = conn.execute(
        f"{_RECORD_SELECT} WHERE lr.license_number = ? AND lr.id != ? ORDER BY lr.record_date DESC",
        (license_number, exclude_id),
    ).fetchall()
    return [dict(r) for r in rows]


def get_entities(
    conn: sqlite3.Connection,
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
        Optional name substring (case-insensitive LIKE search).
    entity_type:
        Optional filter: ``'person'``, ``'organization'``, or ``None`` for all.
    sort:
        ``'count'`` (default) — most active first (record_count DESC, name ASC);
        ``'name'`` — alphabetical (name ASC).
    page:
        1-based page number.
    per_page:
        Rows per page (default 50).

    Returns
    -------
    dict with keys ``entities`` (list of row dicts) and ``total`` (int).
    """
    like_param = f"%{q}%" if q else None

    where_parts = []
    params: list = []
    if like_param is not None:
        where_parts.append("e.name LIKE ? COLLATE NOCASE")
        params.append(like_param)
    if entity_type is not None:
        where_parts.append("e.entity_type = ?")
        params.append(entity_type)

    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    order_clause = (
        "ORDER BY record_count DESC, e.name ASC"
        if sort != "name"
        else "ORDER BY e.name ASC"
    )

    base_sql = f"""
        SELECT e.id, e.name, e.entity_type,
               COUNT(re.record_id) AS record_count
        FROM entities e
        LEFT JOIN record_entities re ON re.entity_id = e.id
        {where_clause}
        GROUP BY e.id
    """

    total_sql = f"SELECT COUNT(*) FROM ({base_sql})"
    total: int = conn.execute(total_sql, params).fetchone()[0]

    page = max(1, page)
    offset = (page - 1) * per_page
    rows_sql = f"{base_sql} {order_clause} LIMIT ? OFFSET ?"
    rows = conn.execute(rows_sql, params + [per_page, offset]).fetchall()

    return {
        "entities": [dict(r) for r in rows],
        "total": total,
    }


def get_entity_records(
    conn: sqlite3.Connection, entity_id: int
) -> list[dict]:
    """Fetch all records associated with an entity, with location data."""
    rows = conn.execute(
        f"""SELECT DISTINCT {RECORD_COLUMNS} {RECORD_JOINS}
            JOIN record_entities re ON re.record_id = lr.id
            WHERE re.entity_id = ?
            ORDER BY lr.record_date DESC, lr.id DESC""",
        (entity_id,),
    ).fetchall()
    return hydrate_records(conn, rows)


def get_record_link(conn: sqlite3.Connection, record_id: int) -> dict | None:
    """Fetch the outcome link for a new_application record.

    Returns a dict with outcome_id, confidence, days_gap,
    outcome_date, outcome_section_type, or None.
    """
    row = conn.execute(
        """SELECT rl.outcome_id, rl.confidence, rl.days_gap,
                  lr.record_date AS outcome_date,
                  lr.section_type AS outcome_section_type
           FROM record_links rl
           JOIN license_records lr ON lr.id = rl.outcome_id
           WHERE rl.new_app_id = ?
           ORDER BY rl.confidence = 'high' DESC
           LIMIT 1""",
        (record_id,),
    ).fetchone()
    return dict(row) if row else None


def get_record_links_bulk(
    conn: sqlite3.Connection, record_ids: list[int],
) -> dict[int, dict]:
    """Fetch outcome links for multiple new_application records.

    Returns {new_app_id: {outcome_id, confidence, days_gap,
    outcome_date, outcome_section_type}}.
    """
    if not record_ids:
        return {}
    placeholders = ",".join("?" for _ in record_ids)
    rows = conn.execute(
        f"""SELECT rl.new_app_id, rl.outcome_id, rl.confidence, rl.days_gap,
                   lr.record_date AS outcome_date,
                   lr.section_type AS outcome_section_type
            FROM record_links rl
            JOIN license_records lr ON lr.id = rl.outcome_id
            WHERE rl.new_app_id IN ({placeholders})""",
        record_ids,
    ).fetchall()
    result: dict[int, dict] = {}
    for r in rows:
        nid = r["new_app_id"]
        # Prefer high confidence if there are multiple
        if nid not in result or r["confidence"] == "high":
            result[nid] = dict(r)
    return result



