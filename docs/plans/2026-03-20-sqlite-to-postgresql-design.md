# SQLite → PostgreSQL Migration Design

**Date:** 2026-03-20
**Status:** Approved

## Goal

Migrate the WSLCB Licensing Tracker from SQLite to a locally hosted PostgreSQL instance. Simultaneously adopt SQLAlchemy Core (async) + asyncpg, Alembic for schema migrations, and PostgreSQL full-text search (tsvector + pg_trgm).

Resolves: #6 (async DB access / connection pooling), #85 (data migration regime for startup repairs)

---

## Approved Approach: Layer-by-Layer (Approach B)

Work outward from the DB layer in discrete phases. Each phase is committed and tested before the next begins. Cutover via maintenance window — no dual-write complexity needed.

### Phase 1 — Foundation

- `models.py` — all 20 tables as SQLAlchemy `Table(...)` definitions (Core, no ORM mappers)
- `database.py` — `AsyncEngine`, `async_sessionmaker`, `get_db()` dependency; connection URL from `/etc/wslcb-licensing-tracker/env`
- Alembic setup at project root; `alembic/versions/0001_baseline.py` is the full PostgreSQL schema (equivalent to running all 15 SQLite migrations); all future DDL goes through Alembic revisions
- `data_migrations` table introduced alongside Alembic for data-only migrations (resolves #85)
- `db.py` retained for constants (`SOURCE_ROLE_PRIORITY`, `US_STATES`, text utilities) to avoid breaking downstream imports

**New deps:** `sqlalchemy[asyncio]>=2.0`, `alembic>=1.13`, `asyncpg`, `psycopg[binary]` (for Alembic's sync env)

### Phase 2 — Core Data Layer

Port `db.py` helpers, `schema.py`, `pipeline.py` to async SQLAlchemy Core.

Key translation patterns:

| SQLite | PostgreSQL / SQLAlchemy |
|---|---|
| `INTEGER PRIMARY KEY AUTOINCREMENT` | `Column(Integer, Identity())` |
| `INSERT OR IGNORE` | `insert().on_conflict_do_nothing()` |
| `INSERT OR REPLACE` | `insert().on_conflict_do_update()` |
| `lastrowid` | `.returning(table.c.id)` |
| `TEXT` JSON blobs | `JSONB` |
| `COLLATE NOCASE` index | `text_pattern_ops` + `func.lower()` |
| `strftime(...)` defaults | `server_default=func.now()` |

Caller-commits convention preserved — functions receive `AsyncConnection`, callers call `await conn.commit()`.

### Phase 3 — Query Layer

Port `queries.py`, `link_records.py`, `endorsements.py`, `endorsements_seed.py`, `endorsements_admin.py`, `entities.py`, `substances.py`, `address_validator.py`, `admin_audit.py` to async SQLAlchemy Core expression language.

`result.mappings().all()` returns `RowMapping` objects — dict-like, same access pattern as `sqlite3.Row`; minimal changes to callers.

### Phase 4 — Web Layer

Port `app.py`, `api_routes.py`, `admin_routes.py`:

- All route handlers `def` → `async def`
- Shared `AsyncEngine` on `app.state.engine`; `get_db()` yields `AsyncConnection` from pool (pool_size=5, max_overflow=10)
- Startup repairs (`repair_code_name_endorsements`, `merge_mixed_case_endorsements`, `backfill`, `backfill_entities`) moved out of lifespan and into the data migration framework (resolves #85); lifespan runs `await run_pending_migrations(engine)` only
- `admin_auth.py` — no changes (no DB access)

### Phase 5 — Full-Text Search

Replace FTS5 virtual table with:

- **`tsvector` column** on `license_records` (stored, GIN index) — weighted: business_name (A), city (B), resolved_endorsements + license_type (C), applicants + address (D)
- **`pg_trgm` GIN index** on `license_records.business_name` and `applicants` for partial/fuzzy matching
- PostgreSQL trigger (DDL in Alembic baseline) maintains `search_vector` on INSERT/UPDATE — replaces FTS5 auto-sync triggers
- `resolved_endorsements` column retained; trigger picks up updates from `reprocess-endorsements` automatically
- Search query combines tsvector `@@` match (ranked by `ts_rank`) OR trgm similarity > 0.2 threshold

### Phase 6 — Ops + Cutover

Port CLI (`cli.py`), backfill scripts, scraper, `integrity.py`, `rebuild.py`.

**Cutover sequence (maintenance window):**

```
1. systemctl stop wslcb-scraper.timer wslcb-web.service
2. wslcb check                        # verify SQLite integrity pre-migration
3. python scripts/sqlite_to_pg.py     # one-time data migration
4. wslcb check --pg                   # verify row counts + provenance chains
5. systemctl start wslcb-web.service
6. curl /api/v1/health                # smoke test
7. systemctl start wslcb-scraper.timer
```

`scripts/sqlite_to_pg.py` — standalone script (not part of app): reads `data/wslcb.db` via sqlite3, inserts into PostgreSQL in FK-dependency order, rebuilds `search_vector` after load.

**Rollback:** `data/wslcb.db` kept in place; revert to SQLite branch. Nothing destructive to existing files.

**Post-cutover:** run `wslcb rebuild-links` and `wslcb reprocess-endorsements` to verify derived data; archive `data/wslcb.db`.

---

## Testing Strategy

- Replace in-memory SQLite fixture with `pytest-asyncio` + `asyncpg` test DB (local PostgreSQL, test schema created per test run)
- Existing fixture structure (`standard_new_application`, `assumption_record`, etc.) unchanged
- Each phase must pass full test suite before next phase begins
- FTS: run identical search queries against both DBs during development, compare result sets

---

## Out of Scope

- ORM mappers / declarative base — Core only
- Read replicas or multi-node setup
- Async rewrite of `parser.py` (no DB access)
- Changing the public API surface (`/api/v1/*` responses unchanged)
