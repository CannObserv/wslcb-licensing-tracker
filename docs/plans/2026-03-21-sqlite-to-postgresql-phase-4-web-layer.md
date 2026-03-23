# Phase 4 — Web Layer Implementation Plan

**Date:** 2026-03-21
**Issue:** #94
**Branch:** `feat-postgresql` (worktree at `.worktrees/feat-postgresql/`)
**Preceding phase:** Phase 3 (Query Layer) — complete, committed, pushed

---

## Goal

Port `app.py`, `api_routes.py`, and `admin_routes.py` to async SQLAlchemy / PostgreSQL.
Move startup repair operations out of the FastAPI lifespan and into the `data_migration.py` framework (resolves #85).

## Files changed

| File | Action |
|---|---|
| `src/wslcb_licensing_tracker/pg_queries.py` | Add `get_record_link()` (missed in Phase 3) |
| `src/wslcb_licensing_tracker/pg_entities.py` | Add `get_entity_by_id()` and `backfill_entities()` |
| `src/wslcb_licensing_tracker/data_migration.py` | **New** — data migration framework |
| `src/wslcb_licensing_tracker/pg_integrity.py` | **New** — async port of 3 check functions |
| `src/wslcb_licensing_tracker/app.py` | Rewrite: async lifespan + async routes |
| `src/wslcb_licensing_tracker/api_routes.py` | Rewrite: async routes + async CSV export |
| `src/wslcb_licensing_tracker/admin_routes.py` | Rewrite: async routes + pg_* imports |
| `tests/test_data_migration.py` | **New** — data migration tests |
| `tests/test_pg_integrity.py` | **New** — integrity check tests |
| `AGENTS.md` | Add new modules to Key Files table |

---

## Design constraints (from design doc)

- `app.state.engine` stores the shared `AsyncEngine` (pool_size=5, max_overflow=10)
- `get_db_dep(request: Request)` is the FastAPI dependency yielding `AsyncConnection`
- Lifespan: `engine = create_engine_from_env(); app.state.engine = engine; await run_pending_migrations(engine); yield; await engine.dispose()`
- All startup repairs become data migrations (idempotent, run-once via `data_migrations` table)
- `admin_auth.py` — unchanged (no DB access)
- Tests: in-memory PG or mock — no network, no disk DB; use `asyncio_mode = "auto"` (already in `pyproject.toml`)

---

## Task 1 — Missing helpers + data_migration.py

**Files:** `pg_queries.py`, `pg_entities.py`, `data_migration.py`, `tests/test_data_migration.py`

### 1a. Add `get_record_link` to `pg_queries.py`

The SQLite `queries.py` has `get_record_link(conn, record_id) -> dict | None`.
Add the async equivalent at the end of `pg_queries.py`:

```python
async def get_record_link(
    conn: AsyncConnection,
    record_id: int,
) -> dict | None:
    """Fetch the best outcome link for a new_application record.

    Returns a dict with outcome_id, confidence, days_gap,
    outcome_date, outcome_section_type, or None.
    """
    row = (
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
    ).mappings().one_or_none()
    return dict(row) if row else None
```

### 1b. Add `get_entity_by_id` and `backfill_entities` to `pg_entities.py`

`get_entity_by_id` — simple lookup used by `entity_detail` route:

```python
async def get_entity_by_id(
    conn: AsyncConnection,
    entity_id: int,
) -> dict | None:
    """Fetch a single entity by id."""
    row = (
        await conn.execute(
            select(
                entities.c.id,
                entities.c.name,
                entities.c.entity_type,
                entities.c.created_at,
            ).where(entities.c.id == entity_id)
        )
    ).mappings().one_or_none()
    return dict(row) if row else None
```

`backfill_entities` — async port of `entities.backfill_entities`. Used in data migration 5:

```python
async def backfill_entities(conn: AsyncConnection) -> int:
    """Populate entities + record_entities for existing records, then run startup cleanup.

    1. Link entities for records that have applicants but no entity
       links yet.
    2. Call merge_duplicate_entities() to clean stale entity names.

    Returns the number of records processed. Caller must commit.
    """
    stmt = text("""
        SELECT lr.id, lr.applicants, lr.previous_applicants
        FROM license_records lr
        LEFT JOIN record_entities re ON re.record_id = lr.id
        WHERE re.record_id IS NULL
          AND (lr.applicants LIKE '%;%' OR lr.previous_applicants LIKE '%;%')
    """)
    rows = (await conn.execute(stmt)).fetchall()

    for r in rows:
        await parse_and_link_entities(conn, r[0], r[1], "applicant")
        if r[2]:
            await parse_and_link_entities(conn, r[0], r[2], "previous_applicant")

    if rows:
        await merge_duplicate_entities(conn)

    return len(rows)
```

**Note:** `parse_and_link_entities` and `merge_duplicate_entities` already exist in `pg_entities.py`.
Add the `entities` table import from `.models` if not already present.

### 1c. Create `data_migration.py`

```python
"""Data migration framework for PostgreSQL startup repairs.

Replaces unconditional startup repair calls in app.py lifespan.
Each migration runs exactly once — tracked in the ``data_migrations`` table.

Usage::

    engine = create_engine_from_env()
    await run_pending_migrations(engine)
"""
```

Structure:
- `_MIGRATIONS: list[tuple[str, Callable[[AsyncConnection], Awaitable[None]]]]`
- Each entry: `(migration_name, async_fn)` where `async_fn` accepts `AsyncConnection`, performs work, does NOT commit
- `run_pending_migrations(engine: AsyncEngine) -> None`:
  1. Fetch already-applied migration names from `data_migrations` table
  2. For each migration in `_MIGRATIONS` not yet applied: open connection, call fn, insert row into `data_migrations`, commit

Registered migrations (in order):

| Name | Function | Source |
|---|---|---|
| `"0001_seed_endorsements"` | `pg_endorsements_seed.seed_endorsements` | |
| `"0002_repair_code_name_endorsements"` | `pg_endorsements_seed.repair_code_name_endorsements` | |
| `"0003_merge_mixed_case_endorsements"` | `pg_endorsements_seed.merge_mixed_case_endorsements` | |
| `"0004_backfill_endorsements"` | `pg_endorsements_seed.backfill` | |
| `"0005_backfill_entities"` | `pg_entities.backfill_entities` | |
| `"0006_build_record_links"` | `pg_link_records.build_all_links` | builds initial links; incremental updates done by scraper |

`run_pending_migrations` implementation:
```python
async def run_pending_migrations(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        applied = {
            row[0]
            for row in (
                await conn.execute(select(data_migrations.c.name))
            ).fetchall()
        }

    for name, fn in _MIGRATIONS:
        if name in applied:
            logger.debug("data migration %r already applied, skipping", name)
            continue
        logger.info("running data migration: %r", name)
        async with engine.connect() as conn:
            await fn(conn)
            await conn.execute(
                pg_insert(data_migrations)
                .values(name=name)
                .on_conflict_do_nothing()
            )
            await conn.commit()
        logger.info("data migration %r complete", name)
```

**Error handling:** If a migration raises, log the exception and re-raise (let lifespan fail visibly — better than silently broken state).

### Tests for Task 1

`tests/test_data_migration.py`:

- `test_run_pending_migrations_creates_rows` — mock each migration fn as a no-op coroutine, call `run_pending_migrations`, assert all 6 migration names inserted into a real in-memory structure (mock `data_migrations` table via `AsyncMock`)
- `test_run_pending_migrations_skips_already_applied` — pre-populate `applied` with one name, assert that fn is not called for it
- `test_get_record_link_returns_none_for_missing` — unit test with `AsyncMock` connection
- `test_get_entity_by_id_returns_none_for_missing` — unit test with `AsyncMock` connection

**Use `unittest.mock.AsyncMock` and `MagicMock` — no live DB required.** Tests must pass with `uv run pytest tests/ -v` without `TEST_DATABASE_URL`.

---

## Task 2 — `pg_integrity.py`

**Files:** `pg_integrity.py`, `tests/test_pg_integrity.py`

Async port of the three check functions used by `admin_routes.admin_dashboard`.
**Only port these three** — `fix_orphaned_locations`, `run_all_checks`, `check_entity_duplicates`, `check_broken_fks` are Phase 6 (CLI).

```python
"""Async PostgreSQL database integrity checks for the WSLCB licensing tracker.

Async port of the check functions used by admin_routes.admin_dashboard.
Full integrity suite (fix_orphaned_locations, run_all_checks) is Phase 6 (CLI).
"""
```

### `check_orphaned_locations(conn: AsyncConnection) -> dict`

```sql
SELECT l.id, l.raw_address
FROM locations l
WHERE l.id NOT IN (
    SELECT DISTINCT location_id FROM license_records WHERE location_id IS NOT NULL
    UNION
    SELECT DISTINCT previous_location_id FROM license_records
    WHERE previous_location_id IS NOT NULL
)
```

Returns `{"count": int, "details": [{"id": ..., "raw_address": ...}]}`.

### `check_unenriched_records(conn: AsyncConnection) -> dict`

Four sub-queries (all identical to SQLite version except use `ILIKE '%;%'` instead of `LIKE '%;%'` — actually LIKE works fine in PG, no change needed):

- `no_endorsements`: COUNT where `license_type IS NOT NULL AND license_type != ''` AND no `record_endorsements` row
- `no_entities`: COUNT where `applicants LIKE '%;%'` AND no `record_entities` row
- `no_provenance`: COUNT where no `record_sources` row
- `no_enrichment_tracking`: COUNT where no `record_enrichments` row for step `'endorsements'`

Returns `{"no_endorsements": int, "no_entities": int, "no_provenance": int, "no_enrichment_tracking": int}`.

### `check_endorsement_anomalies(conn: AsyncConnection) -> dict`

- `unresolved_codes`: COUNT where `license_type ~ '^\d'` (PG regex — replaces `GLOB '[0-9]*'`) AND not in `record_endorsements`
- `placeholder_endorsements`: COUNT where `name ~ '^\d+$'` (replaces `GLOB '[0-9]*' AND name NOT GLOB '*[a-zA-Z]*'`)

Returns `{"unresolved_codes": int, "placeholder_endorsements": int}`.

### Tests

`tests/test_pg_integrity.py` — skip without `TEST_DATABASE_URL`:

- `test_check_orphaned_locations_empty` — no orphans → `{"count": 0, "details": []}`
- `test_check_unenriched_records_empty` — no unenriched → all zeros
- `test_check_endorsement_anomalies_empty` — no anomalies → all zeros

---

## Task 3 — Port `app.py`

**Files:** `app.py`, `tests/test_app.py` (update if exists, or create)

**Approach:** Edit `app.py` in-place (we're on the feature branch — no need for `pg_app.py`).

### Import changes

Remove:
```python
from .db import DATA_DIR, get_db, get_record_sources
from .endorsements_seed import (backfill, merge_mixed_case_endorsements, repair_code_name_endorsements, seed_endorsements)
from .entities import backfill_entities, get_entity_by_id
from .link_records import build_all_links, get_outcome_status, get_reverse_link_info
from .queries import (get_cities_for_state, get_entities, get_entity_records, get_filter_options,
    get_record_by_id, get_record_link, get_related_records, get_stats, hydrate_records, search_records)
from .schema import init_db
```

Add:
```python
from .data_migration import run_pending_migrations
from .database import create_engine_from_env, get_db
from .db import DATA_DIR
from .pg_db import get_record_sources
from .pg_entities import get_entity_by_id, get_entity_records
from .pg_integrity import check_endorsement_anomalies, check_orphaned_locations, check_unenriched_records  # used in admin_routes; kept here for re-export convenience
from .pg_link_records import get_outcome_status, get_reverse_link_info
from .pg_queries import (
    get_cities_for_state, get_entities, get_filter_options,
    get_record_by_id, get_record_link, get_related_records,
    get_stats, hydrate_records, search_records,
)
```

**Note:** `get_entity_records` is in `pg_queries.py`, not `pg_entities.py`. Check and import from the right place.

### Lifespan

Replace the lifespan entirely:

```python
@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize engine and run pending data migrations on startup."""
    setup_logging()
    engine = create_engine_from_env()
    _app.state.engine = engine
    await run_pending_migrations(engine)
    yield
    await engine.dispose()
```

### FastAPI DB dependency

Add after the `app = FastAPI(...)` line:

```python
async def get_db_dep(request: Request) -> AsyncGenerator[AsyncConnection, None]:
    """FastAPI dependency yielding an AsyncConnection from the engine pool."""
    async with get_db(request.app.state.engine) as conn:
        yield conn
```

**Note:** `get_db` in `database.py` is an `asynccontextmanager` that takes `engine`. The dependency wraps it.

### Route changes

For each route, change:
- `with get_db() as conn:` → `async with get_db(request.app.state.engine) as conn:`
- All `conn.execute(...)` calls that are already async stay as-is (they come from pg_* functions)

**`record_detail` signature change:**
```python
# OLD:
related_rows = get_related_records(conn, record["license_number"], record_id)
# NEW:
related_rows = await get_related_records(conn, record)
```

Also update:
```python
hydrated = await hydrate_records(conn, [record, *related_rows])
sources = await get_record_sources(conn, record_id)
link = await get_record_link(conn, record_id)
reverse_link = await get_reverse_link_info(conn, record)
```

**`source_viewer` route** — replace raw `conn.execute("... WHERE s.id = ?", ...)` with async SQLAlchemy:

```python
from sqlalchemy import select, text
from .models import sources as sources_table, source_types, record_sources as record_sources_table

# Replace the three conn.execute calls:

# 1. source lookup:
source_row = (
    await conn.execute(
        select(
            sources_table.c.id,
            source_types.c.slug.label("source_type"),
            source_types.c.label.label("source_label"),
            sources_table.c.snapshot_path,
            sources_table.c.url,
            sources_table.c.captured_at,
            sources_table.c.metadata,
        )
        .select_from(sources_table.join(source_types, source_types.c.id == sources_table.c.source_type_id))
        .where(sources_table.c.id == source_id)
    )
).mappings().one_or_none()
if source_row is None:
    raise HTTPException(status_code=404, detail="Source not found")

# 2. record lookup (already uses get_record_by_id):
record = await get_record_by_id(conn, record_id)

# 3. record_sources link check:
link_row = (
    await conn.execute(
        select(text("1"))
        .select_from(record_sources_table)
        .where(
            record_sources_table.c.record_id == record_id,
            record_sources_table.c.source_id == source_id,
        )
    )
).one_or_none()
if link_row is None:
    raise HTTPException(status_code=404, detail="Source not linked to record")
```

**All other routes** just need `await` on each query call and the connection context switch.

### Models import check

`source_viewer` needs `sources`, `source_types`, `record_sources` from `.models`. Check if they're named `record_sources` or `sources_records` in models.py before writing.

### Tests

Check existing `tests/test_app.py`. If it patches `app.get_db` with SQLite mocks, update tests to:
- Patch `app.app.state.engine` with an `AsyncMock`
- Or use `TestClient` with `dependency_overrides`

---

## Task 4 — Port `api_routes.py`

**Files:** `api_routes.py`, `tests/test_api_routes.py` (update)

### Import changes

Remove:
```python
from .db import US_STATES, get_db
from .queries import export_records_cursor, get_cities_for_state, get_stats
```

Add:
```python
from .database import get_db
from .db import US_STATES
from .pg_queries import export_records_cursor, get_cities_for_state, get_stats
```

Add dependency:
```python
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection

async def _get_db(request: Request):
    async with get_db(request.app.state.engine) as conn:
        yield conn

Conn = Annotated[AsyncConnection, Depends(_get_db)]
```

### Route changes

**`api_cities`:**
```python
@router.get("/cities")
async def api_cities(state: str = "", conn: Conn = None) -> JSONResponse:
    ...
    cities = await get_cities_for_state(conn, state)
```

**`api_stats`:**
```python
@router.get("/stats")
async def api_stats(conn: Conn = None) -> JSONResponse:
    stats = await get_stats(conn)
```

**`api_health`:**
```python
@router.get("/health")
async def api_health(request: Request) -> JSONResponse:
    try:
        async with get_db(request.app.state.engine) as conn:
            await conn.execute(text("SELECT 1"))
        return JSONResponse({"ok": True, "message": "Healthy", "data": {"db": "ok"}})
    except Exception as exc:
        ...
```

**`export_csv`** — async streaming CSV:

The sync `_csv_generator()` cannot be used with `StreamingResponse` when it contains `async for`. Replace with an async generator:

```python
@router.get("/export")
async def export_csv(
    request: Request,
    ...
) -> StreamingResponse:
    if not state:
        city = ""

    async def _async_csv_generator():
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=_EXPORT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()

        async with get_db(request.app.state.engine) as conn:
            async for record in export_records_cursor(
                conn,
                query=q, section_type=section_type, application_type=application_type,
                endorsements=endorsement, state=state, city=city,
                date_from=date_from, date_to=date_to, outcome_status=outcome_status,
            ):
                buf.seek(0)
                buf.truncate(0)
                writer.writerow({k: record.get(k, "") or "" for k in _EXPORT_FIELDNAMES})
                yield buf.getvalue()

    return StreamingResponse(
        _async_csv_generator(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=wslcb_records.csv"},
    )
```

**Note:** `StreamingResponse` in FastAPI/Starlette does support async generators natively.
Add `from sqlalchemy import text` for the health check.

### Tests

Update `tests/test_api_routes.py`:
- All `patch("api_routes.get_db")` → patch the engine on `request.app.state.engine`
- Or override `_get_db` dependency in `app.dependency_overrides`
- Tests that mock `get_stats`, `get_cities_for_state`, `export_records_cursor` still work if those are patched at module level

---

## Task 5 — Port `admin_routes.py`

**Files:** `admin_routes.py`, `tests/test_admin_routes.py` (update if exists)

This is the largest port. Many raw SQL queries with `?` params must be converted to `text()` with named params.

### Import changes

Remove:
```python
import sqlite3
from .db import get_db
from .endorsements import (process_record, remove_alias, rename_endorsement, reprocess_endorsements, set_canonical_endorsement)
from .endorsements_admin import (add_code_mapping, create_code, dismiss_suggestion, get_code_mappings, get_endorsement_list, remove_code_mapping, suggest_duplicate_endorsements)
from .integrity import (check_endorsement_anomalies, check_orphaned_locations, check_unenriched_records)
from .queries import invalidate_all_filter_caches
from .substances import (add_substance, get_regulated_substances, remove_substance, set_substance_endorsements)
```

Add:
```python
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection
from .admin_audit import get_audit_log  # sync version OK for now — see note below
from .database import get_db
from .models import admin_users, license_endorsements, license_records, record_endorsements
from .pg_admin_audit import get_audit_log, log_action
from .pg_endorsements import process_record, remove_alias, rename_endorsement, set_canonical_endorsement
from .pg_endorsements_admin import (add_code_mapping, create_code, dismiss_suggestion, get_code_mappings, get_endorsement_list, remove_code_mapping, suggest_duplicate_endorsements)
from .pg_endorsements import reprocess_endorsements
from .pg_integrity import check_endorsement_anomalies, check_orphaned_locations, check_unenriched_records
from .pg_queries import invalidate_all_filter_caches
from .pg_substances import add_substance, get_regulated_substances, remove_substance, set_substance_endorsements
```

**Note:** Check if `reprocess_endorsements` is in `pg_endorsements.py`. If not, it may need to be added.

Add dependency (same pattern as api_routes):
```python
async def _get_db(request: Request):
    async with get_db(request.app.state.engine) as conn:
        yield conn

Conn = Annotated[AsyncConnection, Depends(_get_db)]
```

Remove `_get_db_conn()` helper at the bottom (it's a dead SQLite artifact).

### Raw SQL queries to port

All `conn.execute("... WHERE col = ?", (val,))` must become `await conn.execute(text("... WHERE col = :param"), {"param": val})`.

Specific cases in `admin_dashboard`:

```python
# SQLite datetime functions → PG:
# datetime('now', '-1 day') → NOW() - INTERVAL '1 day'
# datetime('now', '-7 days') → NOW() - INTERVAL '7 days'
# julianday arithmetic → EXTRACT(EPOCH FROM ...)

agg = (await conn.execute(text("""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN section_type = 'new_application' THEN 1 ELSE 0 END) AS new_apps,
        SUM(CASE WHEN section_type = 'approved' THEN 1 ELSE 0 END) AS approved,
        SUM(CASE WHEN section_type = 'discontinued' THEN 1 ELSE 0 END) AS discontinued
    FROM license_records
"""))).mappings().one()

recent = (await conn.execute(text("""
    SELECT
        SUM(CASE WHEN created_at::timestamptz >= NOW() - INTERVAL '1 day' THEN 1 ELSE 0 END) AS last_24h,
        SUM(CASE WHEN created_at::timestamptz >= NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) AS last_7d
    FROM license_records
"""))).mappings().one()

scrapes_raw = (await conn.execute(text("""
    SELECT id, status, records_new, records_approved, records_discontinued,
           records_skipped, started_at, finished_at,
           ROUND(EXTRACT(EPOCH FROM (finished_at::timestamptz - started_at::timestamptz))) AS duration_secs
    FROM scrape_log
    ORDER BY id DESC LIMIT 5
"""))).mappings().all()

user_count = (await conn.execute(
    select(func.count()).select_from(admin_users)
)).scalar_one()
```

**`admin_users` route:**
```python
users = (await conn.execute(
    select(
        admin_users.c.id, admin_users.c.email, admin_users.c.role,
        admin_users.c.created_at, admin_users.c.created_by,
    ).order_by(admin_users.c.created_at)
)).mappings().all()
```

**`admin_users_add`:**
```python
existing = (await conn.execute(
    select(admin_users.c.id).where(func.lower(admin_users.c.email) == email.lower())
)).one_or_none()
...
result = await conn.execute(
    pg_insert(admin_users)
    .values(email=email, role="admin", created_by=admin["email"])
    .returning(admin_users.c.id)
)
new_id = result.scalar_one()
await log_action(conn, ...)
await conn.commit()
```

**`admin_users_remove`:**
```python
row = (await conn.execute(
    select(admin_users.c.id).where(func.lower(admin_users.c.email) == email.lower())
)).one_or_none()
...
await conn.execute(
    admin_users.delete().where(func.lower(admin_users.c.email) == email.lower())
)
await log_action(conn, ...)
await conn.commit()
```

**`admin_unalias_endorsement`** — multiple raw SQL queries, all need `text()` + named params + `await`:
```python
exists = (await conn.execute(
    select(license_endorsements.c.id).where(license_endorsements.c.id == endorsement_id)
)).one_or_none()

alias_row = (await conn.execute(
    select(license_endorsements.c.id)  # placeholder — use endorsement_aliases table
    .where(...)
)).one_or_none()
```

Check what columns `endorsement_aliases` has in `models.py` before writing this route.

**`admin_alias_endorsement`** — raw SQL lookups:
```python
canonical_name = (await conn.execute(
    select(license_endorsements.c.name).where(license_endorsements.c.id == canonical_id)
)).scalar_one_or_none() or str(canonical_id)

variant_names = []
for vid in variant_ids:
    name = (await conn.execute(
        select(license_endorsements.c.name).where(license_endorsements.c.id == vid)
    )).scalar_one_or_none()
    if name:
        variant_names.append(name)
```

**`admin_rename_endorsement`:**
```python
old_name = (await conn.execute(
    select(license_endorsements.c.name).where(license_endorsements.c.id == endorsement_id)
)).scalar_one_or_none() or str(endorsement_id)
```

**`admin_unalias_endorsement`** affected records query:
```python
affected = (await conn.execute(
    select(license_records.c.id, license_records.c.license_type)
    .select_from(
        license_records.join(record_endorsements, record_endorsements.c.record_id == license_records.c.id)
    )
    .where(record_endorsements.c.endorsement_id == endorsement_id)
)).fetchall()
for record_id, raw_license_type in affected:
    await process_record(conn, record_id, raw_license_type or "")
```

**`admin_substance_add`** — COALESCE/MAX query:
```python
from sqlalchemy import func
from .models import regulated_substances

display_order = (await conn.execute(
    select(func.coalesce(func.max(regulated_substances.c.display_order), 0) + 1)
)).scalar_one()
```

**`_get_db_conn`** at the bottom — delete entirely.

**All `conn.commit()` calls** — change to `await conn.commit()`.

**`log_action` calls** — since `pg_admin_audit.log_action` is async, add `await`.

**`reprocess_endorsements`** — check if it exists in `pg_endorsements.py`. If not, add it:
```python
async def reprocess_endorsements(
    conn: AsyncConnection,
    code: str | None = None,
    record_id: int | None = None,
) -> int:
    """Reprocess endorsements for records matching code or record_id filter."""
    ...
```
(Port from `endorsements.py:reprocess_endorsements`. Checks `pg_endorsements.py` first.)

### Tests

`tests/test_admin_routes.py` — if it exists, update to patch `admin_routes.get_db` dep with async mock.
Key test: `admin_dashboard` renders without error when all check functions return zeros.

---

## Task 6 — AGENTS.md update

**File:** `AGENTS.md`

Add to Key Files table:

| File | Purpose |
|---|---|
| `data_migration.py` | *(PostgreSQL migration — Phase 4)* Data migration framework. `run_pending_migrations(engine)` — runs startup repairs as idempotent one-time migrations tracked in `data_migrations` table. Called from lifespan. |
| `pg_integrity.py` | *(PostgreSQL migration — Phase 4)* Async port of 3 integrity check functions used by admin dashboard: `check_orphaned_locations()`, `check_unenriched_records()`, `check_endorsement_anomalies()`. Full CLI suite is Phase 6. |

Update `app.py` entry to note: "Lifespan runs `await run_pending_migrations(engine)` only. All startup repairs moved to `data_migration.py`."

Update `pg_queries.py` entry: "Also includes `get_record_link()` (added Phase 4)."

Update `pg_entities.py` entry: "Also includes `get_entity_by_id()` and `backfill_entities()` (added Phase 4)."

---

## Pre-flight check

Before each task commit, run:
```bash
cd /home/exedev/wslcb-licensing-tracker/.worktrees/feat-postgresql
uv run pytest tests/ -v
```

All tests must pass. Tests requiring `TEST_DATABASE_URL` are expected to skip.

## Commit convention

```
#94 feat: Phase 4 — <short description>
```

---

## Key reference: models.py table names

For `source_viewer` in `app.py`, check models.py for exact table variable names:
- `sources` (or `sources_table`) — confirmed as `sources` in models.py
- `source_types` — confirm name
- `record_sources` — confirm name

Run: `grep "^[a-z].*= Table(" src/wslcb_licensing_tracker/models.py` to list all table vars.
