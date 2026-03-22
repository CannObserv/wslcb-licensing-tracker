# pg_queries Module Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Break `pg_queries.py` (926 lines) into 6 focused sub-modules with narrow import surfaces; delete `pg_queries.py`; update all callers.

**Architecture:** Each new module owns one concern. `pg_queries_hydrate.py` is the single acknowledged integration layer that calls domain modules. Constants (`DATA_GAP_CUTOFF`, `LINKABLE_TYPES`, `PENDING_CUTOFF_DAYS`, `outcome_filter_sql`) move to `pg_db.py` so stats/export/search modules import only from `pg_db`, not from domain modules. Work is done in `.worktrees/101-pg-queries-split` on branch `feature/101-pg-queries-split`. New modules are created while `pg_queries.py` still exists so tests keep passing throughout; the final task swaps imports and deletes the old file.

**Tech Stack:** Python 3.12, SQLAlchemy 2.0 Core async, asyncpg, FastAPI, pytest-asyncio

**Design doc:** `docs/plans/2026-03-22-pg-queries-module-split-design.md`

---

### Task 1: Move pipeline constants and outcome_filter_sql to pg_db.py

**Files:**
- Modify: `src/wslcb_licensing_tracker/pg_db.py` (add constants + function at bottom)
- Modify: `src/wslcb_licensing_tracker/pg_link_records.py` (replace definitions with imports)

- [ ] **Step 1: Add constants and outcome_filter_sql to pg_db.py**

Append to the bottom of `src/wslcb_licensing_tracker/pg_db.py` (after the `US_STATES` dict):

```python
# ---------------------------------------------------------------------------
# Application pipeline constants (shared by pg_link_records, pg_queries_*)
# ---------------------------------------------------------------------------

# Cutoff: applications older than this with no outcome -> 'unknown'
PENDING_CUTOFF_DAYS = 180

# Date after which NEW APPLICATION approvals stopped being published
DATA_GAP_CUTOFF = "2025-05-12"

# All application types eligible for outcome linking
LINKABLE_TYPES: frozenset[str] = frozenset({
    "RENEWAL",
    "NEW APPLICATION",
    "ASSUMPTION",
    "ADDED/CHANGE OF CLASS",
    "CHANGE OF CORPORATE OFFICER",
    "CHANGE OF LOCATION",
    "RESUME BUSINESS",
    "IN LIEU",
    "DISC. LIQUOR SALES",
})


def outcome_filter_sql(
    status: str,
    record_alias: str = "lr",
) -> list[str]:
    """Return SQL WHERE-clause fragments for an outcome_status filter.

    Each element is a standalone condition to be ANDed into the query.
    The *record_alias* must be the table alias for ``license_records``.

    Valid *status* values: ``'approved'``, ``'discontinued'``,
    ``'pending'``, ``'data_gap'``, ``'unknown'``.
    Returns an empty list for unrecognised values.
    """
    r = record_alias
    linkable = ", ".join(f"'{t}'" for t in LINKABLE_TYPES)
    not_linked = f"NOT EXISTS (SELECT 1 FROM record_links rl WHERE rl.new_app_id = {r}.id)"
    not_data_gap = (
        f"NOT ({r}.application_type = 'NEW APPLICATION' AND {r}.record_date > '{DATA_GAP_CUTOFF}')"
    )

    if status == "approved":
        return [
            f"{r}.id IN (SELECT rl.new_app_id FROM record_links rl "
            "JOIN license_records o ON o.id = rl.outcome_id "
            "WHERE o.section_type = 'approved')",
        ]
    if status == "discontinued":
        return [
            f"{r}.id IN (SELECT rl.new_app_id FROM record_links rl "
            "JOIN license_records o ON o.id = rl.outcome_id "
            "WHERE o.section_type = 'discontinued')",
        ]
    if status == "pending":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type IN ({linkable})",
            not_linked,
            f"{r}.record_date::date >= CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'",
            not_data_gap,
        ]
    if status == "data_gap":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type = 'NEW APPLICATION'",
            f"{r}.record_date > '{DATA_GAP_CUTOFF}'",
            not_linked,
        ]
    if status == "unknown":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type IN ({linkable})",
            not_linked,
            f"{r}.record_date::date < CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'",
            not_data_gap,
        ]
    return []
```

- [ ] **Step 2: Update pg_link_records.py to import from pg_db**

Replace lines 22–104 in `pg_link_records.py` (the `DATE_TOLERANCE_DAYS` constant stays; remove `PENDING_CUTOFF_DAYS`, `DATA_GAP_CUTOFF`, `_APPROVAL_LINK_TYPES`, `_DISC_LINK_TYPE`, `LINKABLE_TYPES`, and the `outcome_filter_sql` function definition; add imports):

```python
DATE_TOLERANCE_DAYS = 7

from .pg_db import DATA_GAP_CUTOFF, LINKABLE_TYPES, PENDING_CUTOFF_DAYS, outcome_filter_sql  # noqa: E402
```

Place the import right after `DATE_TOLERANCE_DAYS` (before `logger`). Then remove the now-redundant definitions of `_APPROVAL_LINK_TYPES`, `_DISC_LINK_TYPE`, `LINKABLE_TYPES`, `PENDING_CUTOFF_DAYS`, `DATA_GAP_CUTOFF`, and `outcome_filter_sql` from `pg_link_records.py`.

Keep `get_outcome_status`, `get_record_links_bulk`, `get_reverse_link_info`, `build_all_links`, `get_record_links_bulk` — all the async functions stay in `pg_link_records.py`.

- [ ] **Step 3: Run tests — must pass**

```bash
cd /home/exedev/wslcb-licensing-tracker/.worktrees/101-pg-queries-split
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures (same as baseline).

- [ ] **Step 4: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_db.py src/wslcb_licensing_tracker/pg_link_records.py
git commit -m "#101 refactor: move pipeline constants and outcome_filter_sql to pg_db"
```

---

### Task 2: Create pg_queries_hydrate.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_queries_hydrate.py`

- [ ] **Step 1: Create the file**

`src/wslcb_licensing_tracker/pg_queries_hydrate.py`:

```python
"""Record enrichment and hydration for WSLCB licensing tracker.

Integration layer: assembles composite record dicts from endorsement,
entity, and link-records domain modules. Intentionally crosses module
boundaries — this is the single acknowledged fan-in point for the
query layer.

Contains:
- enrich_record() — pure Python display-field helper
- hydrate_records() / _hydrate_records() — bulk record enrichment
"""

import logging

from sqlalchemy.ext.asyncio import AsyncConnection

from .display import format_outcome
from .pg_endorsements import get_record_endorsements
from .pg_entities import get_record_entities
from .pg_link_records import get_outcome_status, get_record_links_bulk

logger = logging.getLogger(__name__)


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


# Public alias for external callers (e.g. app.py record_detail route).
hydrate_records = _hydrate_records
```

- [ ] **Step 2: Run tests — must pass**

```bash
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures.

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_queries_hydrate.py
git commit -m "#101 feat: add pg_queries_hydrate with enrich_record and _hydrate_records"
```

---

### Task 3: Create pg_queries_stats.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_queries_stats.py`

- [ ] **Step 1: Create the file**

`src/wslcb_licensing_tracker/pg_queries_stats.py`:

```python
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
```

- [ ] **Step 2: Run tests — must pass**

```bash
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures.

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_queries_stats.py
git commit -m "#101 feat: add pg_queries_stats with get_stats and invalidate_stats_cache"
```

---

### Task 4: Create pg_queries_filter.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_queries_filter.py`

- [ ] **Step 1: Create the file**

`src/wslcb_licensing_tracker/pg_queries_filter.py`:

```python
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
```

- [ ] **Step 2: Run tests — must pass**

```bash
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures.

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_queries_filter.py
git commit -m "#101 feat: add pg_queries_filter with get_filter_options and invalidate_filter_cache"
```

---

### Task 5: Create pg_queries_export.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_queries_export.py`

- [ ] **Step 1: Create the file**

`src/wslcb_licensing_tracker/pg_queries_export.py`:

```python
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
    async with await conn.stream(
        text(f"{_EXPORT_SELECT} {where} ORDER BY lr.record_date DESC, lr.id DESC LIMIT :limit"),
        {**params, "limit": limit},
    ) as stream:
        async for row in stream.mappings():
            yield dict(row)
```

- [ ] **Step 2: Run tests — must pass**

```bash
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures.

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_queries_export.py
git commit -m "#101 feat: add pg_queries_export with export_records and export_records_cursor"
```

---

### Task 6: Create pg_queries_search.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_queries_search.py`

- [ ] **Step 1: Create the file**

`src/wslcb_licensing_tracker/pg_queries_search.py`:

```python
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
```

- [ ] **Step 2: Run tests — must pass**

```bash
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures.

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_queries_search.py
git commit -m "#101 feat: add pg_queries_search with search_records and single-record lookups"
```

---

### Task 7: Create pg_queries_entity.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_queries_entity.py`

- [ ] **Step 1: Create the file**

`src/wslcb_licensing_tracker/pg_queries_entity.py`:

```python
"""Entity-centric record queries for WSLCB licensing tracker.

Contains:
- get_entity_records() — all hydrated records associated with an entity
- get_entities() — paginated, searchable entity list
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .pg_queries_hydrate import _hydrate_records
from .pg_queries_search import RECORD_COLUMNS, RECORD_JOINS

logger = logging.getLogger(__name__)


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
```

- [ ] **Step 2: Run tests — must pass**

```bash
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures.

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_queries_entity.py
git commit -m "#101 feat: add pg_queries_entity with get_entity_records and get_entities"
```

---

### Task 8: Update all callers, delete pg_queries.py, final verification

**Files:**
- Modify: `src/wslcb_licensing_tracker/app.py`
- Modify: `src/wslcb_licensing_tracker/api_routes.py`
- Modify: `src/wslcb_licensing_tracker/admin_routes.py`
- Modify: `tests/test_pg_queries.py`
- Delete: `src/wslcb_licensing_tracker/pg_queries.py`

- [ ] **Step 1: Update app.py imports**

Replace the existing `pg_queries` import block in `app.py` (lines 38–51):

```python
# Before:
from .pg_queries import (
    get_cities_for_state,
    get_entities,
    get_entity_records,
    get_filter_options,
    get_record_by_id,
    get_record_link,
    get_record_source_link,
    get_related_records,
    get_source_by_id,
    get_stats,
    hydrate_records,
    search_records,
)

# After:
from .pg_queries_entity import get_entities, get_entity_records
from .pg_queries_filter import get_cities_for_state, get_filter_options
from .pg_queries_hydrate import hydrate_records
from .pg_queries_search import (
    get_record_by_id,
    get_record_link,
    get_record_source_link,
    get_related_records,
    get_source_by_id,
    search_records,
)
from .pg_queries_stats import get_stats
```

- [ ] **Step 2: Update api_routes.py imports**

Replace line 25 in `api_routes.py`:

```python
# Before:
from .pg_queries import export_records_cursor, get_cities_for_state, get_stats

# After:
from .pg_queries_export import export_records_cursor
from .pg_queries_filter import get_cities_for_state
from .pg_queries_stats import get_stats
```

- [ ] **Step 3: Update admin_routes.py imports**

Replace line 53 in `admin_routes.py`:

```python
# Before:
from .pg_queries import invalidate_filter_cache as invalidate_all_filter_caches

# After:
from .pg_queries_filter import invalidate_filter_cache as invalidate_all_filter_caches
```

- [ ] **Step 4: Update tests/test_pg_queries.py imports**

Replace lines 3–18:

```python
# Before:
from wslcb_licensing_tracker.pg_queries import (
    search_records,
    export_records,
    export_records_cursor,
    get_filter_options,
    get_cities_for_state,
    get_stats,
    enrich_record,
    get_record_by_id,
    get_related_records,
    get_entity_records,
    get_entities,
    get_source_by_id,
    get_record_source_link,
    invalidate_filter_cache,
)

# After:
from wslcb_licensing_tracker.pg_queries_export import export_records, export_records_cursor
from wslcb_licensing_tracker.pg_queries_entity import get_entity_records, get_entities
from wslcb_licensing_tracker.pg_queries_filter import (
    get_cities_for_state,
    get_filter_options,
    invalidate_filter_cache,
)
from wslcb_licensing_tracker.pg_queries_hydrate import enrich_record
from wslcb_licensing_tracker.pg_queries_search import (
    get_record_by_id,
    get_record_source_link,
    get_related_records,
    get_source_by_id,
)
from wslcb_licensing_tracker.pg_queries_stats import get_stats
```

Also update the test module docstring on line 1:

```python
"""Tests for pg_queries_* modules — async search and read queries."""
```

- [ ] **Step 5: Delete pg_queries.py**

```bash
rm src/wslcb_licensing_tracker/pg_queries.py
```

- [ ] **Step 6: Run full test suite — must pass**

```bash
uv run pytest tests/ -v -q
```

Expected: 273 passed, 0 failures.

- [ ] **Step 7: Verify line counts — all new modules must be ≤ 500 lines**

```bash
wc -l src/wslcb_licensing_tracker/pg_queries_*.py
```

Expected: all under 500 lines.

- [ ] **Step 8: Verify pg_queries_search and pg_queries_stats/export have no domain module imports**

```bash
grep "from .pg_endorsements\|from .pg_entities\|from .pg_link_records\|from .pg_substances" \
    src/wslcb_licensing_tracker/pg_queries_search.py \
    src/wslcb_licensing_tracker/pg_queries_stats.py \
    src/wslcb_licensing_tracker/pg_queries_export.py \
    src/wslcb_licensing_tracker/pg_queries_entity.py
```

Expected: no output (no matches).

- [ ] **Step 9: Commit**

```bash
git add src/wslcb_licensing_tracker/app.py \
        src/wslcb_licensing_tracker/api_routes.py \
        src/wslcb_licensing_tracker/admin_routes.py \
        tests/test_pg_queries.py
git rm src/wslcb_licensing_tracker/pg_queries.py
git commit -m "$(cat <<'EOF'
#101 refactor: split pg_queries into focused sub-modules, update all callers

Splits 926-line pg_queries.py into 6 focused modules:
- pg_queries_hydrate: enrich_record, hydrate_records (integration layer)
- pg_queries_filter: get_filter_options, get_cities_for_state, invalidate_filter_cache
- pg_queries_stats: get_stats (imports pg_db constants only)
- pg_queries_export: export_records, export_records_cursor (imports pg_db constants only)
- pg_queries_search: search_records, single-record lookups (imports pg_db only)
- pg_queries_entity: get_entity_records, get_entities

Moves DATA_GAP_CUTOFF, LINKABLE_TYPES, PENDING_CUTOFF_DAYS, outcome_filter_sql
to pg_db.py. Deletes pg_queries.py. Updates app.py, api_routes.py,
admin_routes.py, tests/test_pg_queries.py.

(closes #101)
EOF
)"
```
