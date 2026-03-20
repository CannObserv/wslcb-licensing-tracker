# Phase 2: Core Data Layer — SQLite → PostgreSQL Migration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Date:** 2026-03-20
**GitHub issue:** #94
**Branch:** `feat/94-postgresql-migration` (worktree at `.worktrees/feat-postgresql/`)
**Run all tests:** `uv run pytest tests/ -v`
**Run PG tests only:** `TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_db.py tests/test_pg_schema.py tests/test_pg_pipeline.py -v`

**Goal:** Port `db.py` helpers, `schema.py`, and `pipeline.py` to async SQLAlchemy Core as NEW parallel modules (`pg_db.py`, `pg_schema.py`, `pg_pipeline.py`). The original SQLite modules remain untouched — all 632 existing tests continue to pass. SQLite modules are removed in Phase 6 (cutover).

**Architecture:** Each new module receives an `AsyncConnection` (caller-commits convention preserved). Uses `models.py` table objects from Phase 1. PostgreSQL idioms: `pg_insert().on_conflict_do_nothing()`, `.returning()`, `on_conflict_do_update()`. Entity linking and enrichment steps beyond basic insertion are STUBBED with `# TODO Phase 3` comments — Phase 3 ports the query/endorsement/entity layer.

**Tech Stack:** Python 3.12, SQLAlchemy 2.0 Core (async), asyncpg, Alembic, pytest-asyncio

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `alembic/env.py` | Modify | Add pre-existing connection check in `run_migrations_online()` |
| `tests/conftest.py` | Modify | Add `pg_url`, `pg_engine`, `pg_conn` fixtures |
| `tests/test_pg_database.py` | Modify | Remove local `pg_engine`/`test_url` fixtures (now in conftest) |
| `tests/test_pg_schema.py` | Create | Tests for `pg_schema.init_db`, `_table_exists`, `_column_exists` |
| `tests/test_pg_db.py` | Create | Tests for location/source/provenance helpers |
| `tests/test_pg_pipeline.py` | Create | Tests for `insert_record`, `ingest_record`, `ingest_batch` |
| `src/wslcb_licensing_tracker/pg_schema.py` | Create | `init_db(engine)`, `_table_exists`, `_column_exists` |
| `src/wslcb_licensing_tracker/pg_db.py` | Create | Async location/source/provenance helpers |
| `src/wslcb_licensing_tracker/pg_pipeline.py` | Create | Async `insert_record`, `ingest_record`, `ingest_batch` |

---

## Task 1: Fix alembic/env.py and promote PG test fixtures to conftest.py

**Files:**
- Modify: `.worktrees/feat-postgresql/alembic/env.py`
- Modify: `.worktrees/feat-postgresql/tests/conftest.py`
- Modify: `.worktrees/feat-postgresql/tests/test_pg_database.py`

### Step 1: Fix alembic/env.py to use pre-existing connection when provided

The current `run_migrations_online()` always creates a new async engine. It needs to check
`config.attributes.get("connection")` first — the standard Alembic pattern for passing a
pre-existing sync connection (e.g., from `conn.run_sync()`). Without this fix, `pg_schema.init_db()`
sets `cfg.attributes["connection"]` but the attribute is silently ignored.

In `alembic/env.py`, replace `run_migrations_online()`:

```python
def run_migrations_online() -> None:
    """Run migrations against a live database connection.

    When ``config.attributes["connection"]`` is set (e.g., from a
    ``conn.run_sync()`` callback), use it directly.  Otherwise, create a new
    async engine from the configured URL.
    """
    connection = config.attributes.get("connection")
    if connection is not None:
        do_run_migrations(connection)
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(run_async_migrations())
    else:
        loop.run_until_complete(run_async_migrations())
```

- [ ] **Step 1a: Apply the fix**

Edit `alembic/env.py`: replace the `run_migrations_online` function body as shown above.

- [ ] **Step 1b: Verify existing Alembic migration still works**

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" \
  uv run alembic -x database_url="$TEST_DATABASE_URL" upgrade head
```

Expected: `INFO  [alembic.runtime.migration] Running upgrade  -> 0001, baseline_postgresql_schema`
(or `INFO  [alembic.runtime.migration] No new upgrade operations to perform` if already applied)

### Step 2: Add PG fixtures to conftest.py

Append to the end of `tests/conftest.py`:

```python
import os
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


@pytest.fixture(scope="session")
def pg_url() -> str | None:
    """PostgreSQL connection URL from TEST_DATABASE_URL env var."""
    return os.environ.get("TEST_DATABASE_URL")


@pytest_asyncio.fixture(scope="session")
async def pg_engine(pg_url) -> AsyncGenerator[AsyncEngine, None]:
    """Session-scoped async engine with Alembic migrations applied.

    Skips all PG tests when TEST_DATABASE_URL is not set.
    """
    if not pg_url:
        pytest.skip("TEST_DATABASE_URL not set — skipping PostgreSQL tests")

    from wslcb_licensing_tracker.database import create_engine_from_env

    os.environ["DATABASE_URL"] = pg_url
    engine = create_engine_from_env()

    # Run Alembic migrations
    from alembic import command
    from alembic.config import Config

    def _run_upgrade(connection):
        cfg = Config("alembic.ini")
        cfg.attributes["connection"] = connection
        command.upgrade(cfg, "head")

    async with engine.connect() as conn:
        await conn.run_sync(_run_upgrade)
        await conn.commit()

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def pg_conn(pg_engine: AsyncEngine) -> AsyncGenerator[AsyncConnection, None]:
    """AsyncConnection in a rolled-back transaction for test isolation.

    Each test gets a clean transaction that is rolled back after the test,
    so tests never see each other's data. Suitable for helpers and
    insert_record tests that don't commit internally.

    For ingest_batch (which commits), use pg_engine directly.
    """
    async with pg_engine.connect() as conn:
        trans = await conn.begin()
        yield conn
        await trans.rollback()
```

### Step 3: Remove local fixtures from test_pg_database.py

In `tests/test_pg_database.py`, remove:
- The `test_url` fixture (lines 18-22)
- The `pg_engine` fixture (lines 25-29)
- The `import asyncio` (line 1, if no longer needed — keep it; it's used in `test_alembic_baseline_creates_all_tables`)
- The `import os` (line 2, if no longer needed — keep it; the conftest handles `os.environ` now)

Update `test_alembic_baseline_creates_all_tables` to accept `pg_engine` and `pg_url` (renamed from `test_url`):

```python
async def test_alembic_baseline_creates_all_tables(pg_engine, pg_url):
```

Replace the `test_url` reference inside the function with `pg_url`.

### Step 4: Verify existing PG tests still pass

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_database.py -v
```

Expected: all 4 tests pass (2 unit, 2 integration requiring PG).

### Step 5: Verify SQLite tests still pass

```bash
cd .worktrees/feat-postgresql
uv run pytest tests/ -v --ignore=tests/test_pg_database.py -k "not pg"
```

Expected: all 632 existing tests pass.

### Commit

```
#94 chore: fix alembic env.py; promote pg_engine/pg_conn fixtures to conftest.py
```

---

## Task 2: pg_schema.py — Alembic-based init_db and introspection helpers

**Files:**
- Create: `.worktrees/feat-postgresql/tests/test_pg_schema.py`
- Create: `.worktrees/feat-postgresql/src/wslcb_licensing_tracker/pg_schema.py`

### Step 1: Write failing tests

Create `tests/test_pg_schema.py`:

```python
"""Tests for PostgreSQL schema initialization and introspection helpers.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
"""

from wslcb_licensing_tracker.pg_schema import _column_exists, _table_exists, init_db


async def test_init_db_creates_tables(pg_engine):
    """init_db runs Alembic migrations and creates all expected tables."""
    # pg_engine fixture already ran init_db via Alembic, so we just verify
    # the function is importable and callable (idempotent — safe to call again)
    await init_db(pg_engine)

    # Verify key tables exist
    from sqlalchemy import text

    async with pg_engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ))
        tables = {row[0] for row in result}

    assert "license_records" in tables
    assert "locations" in tables
    assert "sources" in tables
    assert "record_sources" in tables


async def test_table_exists_true(pg_conn):
    """_table_exists returns True for an existing table."""
    assert await _table_exists(pg_conn, "license_records") is True


async def test_table_exists_false(pg_conn):
    """_table_exists returns False for a non-existent table."""
    assert await _table_exists(pg_conn, "no_such_table_xyz") is False


async def test_column_exists_true(pg_conn):
    """_column_exists returns True for an existing column."""
    assert await _column_exists(pg_conn, "license_records", "business_name") is True


async def test_column_exists_false_column(pg_conn):
    """_column_exists returns False for a non-existent column."""
    assert await _column_exists(pg_conn, "license_records", "no_such_column") is False


async def test_column_exists_false_table(pg_conn):
    """_column_exists returns False when the table doesn't exist."""
    assert await _column_exists(pg_conn, "no_such_table", "id") is False
```

### Step 2: Run to verify they fail

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_schema.py -v
```

Expected: `ModuleNotFoundError: No module named 'wslcb_licensing_tracker.pg_schema'`

### Step 3: Implement pg_schema.py

Create `src/wslcb_licensing_tracker/pg_schema.py`:

```python
"""PostgreSQL schema initialization and introspection helpers.

Uses Alembic to run migrations. Replaces the SQLite PRAGMA user_version
migration framework in schema.py.
"""

import logging

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)


async def init_db(engine: AsyncEngine) -> None:
    """Run all pending Alembic migrations.

    Idempotent — safe to call on every startup. Alembic tracks applied
    revisions in the ``alembic_version`` table and skips already-applied
    migrations.
    """

    def _run_upgrade(connection):
        cfg = Config("alembic.ini")
        cfg.attributes["connection"] = connection
        command.upgrade(cfg, "head")

    async with engine.connect() as conn:
        await conn.run_sync(_run_upgrade)
        await conn.commit()


async def _table_exists(conn: AsyncConnection, name: str) -> bool:
    """Return True if *name* is a table in the public schema."""
    result = await conn.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = :name"
        ),
        {"name": name},
    )
    return result.first() is not None


async def _column_exists(conn: AsyncConnection, table: str, column: str) -> bool:
    """Return True if *column* exists on *table*. Returns False when *table* is absent."""
    result = await conn.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :table AND column_name = :column"
        ),
        {"table": table, "column": column},
    )
    return result.first() is not None
```

### Step 4: Run tests — verify they pass

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_schema.py -v
```

Expected: 6 tests pass.

### Commit

```
#94 feat: add pg_schema.py with Alembic init_db and introspection helpers
```

---

## Task 3: pg_db.py — Location helpers

**Files:**
- Create: `.worktrees/feat-postgresql/tests/test_pg_db.py` (location tests only)
- Create: `.worktrees/feat-postgresql/src/wslcb_licensing_tracker/pg_db.py` (location helpers only)

### Step 1: Write failing tests

Create `tests/test_pg_db.py`:

```python
"""Tests for PostgreSQL database helpers (pg_db.py).

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
"""

import pytest

from wslcb_licensing_tracker.pg_db import (
    get_or_create_location,
    get_or_create_source,
    get_primary_source,
    get_record_sources,
    link_record_source,
)


class TestPgGetOrCreateLocation:
    async def test_creates_new_location(self, pg_conn):
        """Creates a new location and returns its ID."""
        loc_id = await get_or_create_location(pg_conn, "123 MAIN ST, SEATTLE, WA 98101")
        assert loc_id is not None
        assert isinstance(loc_id, int)

    async def test_returns_same_id_for_duplicate(self, pg_conn):
        """Returns existing ID when same raw_address is inserted again."""
        id1 = await get_or_create_location(pg_conn, "456 ELM AVE, TACOMA, WA 98402")
        id2 = await get_or_create_location(pg_conn, "456 ELM AVE, TACOMA, WA 98402")
        assert id1 == id2

    async def test_returns_none_for_empty(self, pg_conn):
        """Returns None for empty/None/whitespace-only addresses."""
        assert await get_or_create_location(pg_conn, "") is None
        assert await get_or_create_location(pg_conn, None) is None
        assert await get_or_create_location(pg_conn, "   ") is None

    async def test_nbsp_normalization(self, pg_conn):
        """Non-breaking spaces are normalized to regular spaces."""
        id1 = await get_or_create_location(pg_conn, "100\xa0MAIN\xa0ST")
        id2 = await get_or_create_location(pg_conn, "100 MAIN ST")
        assert id1 == id2

    async def test_stores_city_state_zip(self, pg_conn):
        """City, state, and zip_code are stored correctly."""
        from sqlalchemy import select
        from wslcb_licensing_tracker.models import locations

        loc_id = await get_or_create_location(
            pg_conn, "789 OAK DR, OLYMPIA, WA 98501",
            city="OLYMPIA", state="WA", zip_code="98501",
        )
        result = await pg_conn.execute(
            select(locations.c.city, locations.c.state, locations.c.zip_code)
            .where(locations.c.id == loc_id)
        )
        row = result.one()
        assert row.city == "OLYMPIA"
        assert row.state == "WA"
        assert row.zip_code == "98501"
```

### Step 2: Run to verify they fail

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_db.py::TestPgGetOrCreateLocation -v
```

Expected: `ImportError` — `pg_db` module does not exist yet.

### Step 3: Implement location helpers in pg_db.py

Create `src/wslcb_licensing_tracker/pg_db.py`:

```python
"""Async PostgreSQL database helpers for the WSLCB licensing tracker.

Async equivalents of the location, source, and provenance helpers in db.py.
Uses SQLAlchemy Core expressions against the table objects in models.py.

Pure-string helpers (_normalize_raw_address, clean_entity_name, etc.) are
re-imported from db.py — they have no DB dependency.
"""

import json
import logging

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .db import SOURCE_ROLE_PRIORITY, _normalize_raw_address
from .models import locations, record_sources, source_types, sources

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Location helpers
# ------------------------------------------------------------------


async def get_or_create_location(
    conn: AsyncConnection,
    raw_address: str | None,
    city: str = "",
    state: str = "WA",
    zip_code: str = "",
) -> int | None:
    """Return the location id for *raw_address*, creating if needed.

    Returns None if raw_address is empty/None/whitespace-only.
    Uses INSERT ... ON CONFLICT DO NOTHING + RETURNING for atomicity.
    """
    if not raw_address or not raw_address.strip():
        return None

    normalized = _normalize_raw_address(raw_address)

    # Try insert; ON CONFLICT returns no row
    stmt = (
        pg_insert(locations)
        .values(raw_address=normalized, city=city, state=state, zip_code=zip_code)
        .on_conflict_do_nothing(index_elements=["raw_address"])
        .returning(locations.c.id)
    )
    result = await conn.execute(stmt)
    row = result.first()
    if row:
        return row[0]

    # Conflict — fetch existing
    result = await conn.execute(
        select(locations.c.id).where(locations.c.raw_address == normalized)
    )
    return result.scalar_one()
```

### Step 4: Run tests — verify they pass

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_db.py::TestPgGetOrCreateLocation -v
```

Expected: 5 tests pass.

### Commit

```
#94 feat: add pg_db.py with async get_or_create_location
```

---

## Task 4: pg_db.py — Source and provenance helpers

**Files:**
- Modify: `.worktrees/feat-postgresql/tests/test_pg_db.py` (add source/provenance test classes)
- Modify: `.worktrees/feat-postgresql/src/wslcb_licensing_tracker/pg_db.py` (add source/provenance functions)

### Step 1: Write failing tests

Append to `tests/test_pg_db.py`:

```python
class TestPgGetOrCreateSource:
    async def _seed_source_type(self, pg_conn):
        """Insert the live_scrape source type (id=1) for tests."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from wslcb_licensing_tracker.models import source_types

        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )

    async def test_creates_source(self, pg_conn):
        """Creates a source row and returns its ID."""
        await self._seed_source_type(pg_conn)
        src_id = await get_or_create_source(
            pg_conn, source_type_id=1,
            snapshot_path="data/wslcb/2025/2025-06-15/page.html",
        )
        assert isinstance(src_id, int)

    async def test_idempotent_with_path(self, pg_conn):
        """Same (type, path) returns same ID."""
        await self._seed_source_type(pg_conn)
        path = "data/wslcb/2025/2025-06-15/page.html"
        id1 = await get_or_create_source(pg_conn, source_type_id=1, snapshot_path=path)
        id2 = await get_or_create_source(pg_conn, source_type_id=1, snapshot_path=path)
        assert id1 == id2

    async def test_null_path_with_scrape_log_id(self, pg_conn):
        """NULL snapshot_path with different scrape_log_ids → distinct rows."""
        await self._seed_source_type(pg_conn)
        id1 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=10)
        id2 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=20)
        assert id1 != id2

    async def test_null_path_same_scrape_log_id(self, pg_conn):
        """NULL snapshot_path with same scrape_log_id → same row."""
        await self._seed_source_type(pg_conn)
        id1 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=10)
        id2 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=10)
        assert id1 == id2

    async def test_null_path_null_scrape_log(self, pg_conn):
        """NULL snapshot_path + NULL scrape_log_id → same row on repeat."""
        await self._seed_source_type(pg_conn)
        id1 = await get_or_create_source(pg_conn, source_type_id=1)
        id2 = await get_or_create_source(pg_conn, source_type_id=1)
        assert id1 == id2


class TestPgLinkRecordSource:
    async def _seed_data(self, pg_conn):
        """Create a source type, source, and minimal license record for linking tests."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from wslcb_licensing_tracker.models import license_records, source_types, sources

        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        result = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/link.html")
            .on_conflict_do_nothing()
            .returning(sources.c.id)
        )
        row = result.first()
        if row:
            source_id = row[0]
        else:
            r = await pg_conn.execute(
                select(sources.c.id).where(sources.c.snapshot_path == "test/link.html")
            )
            source_id = r.scalar_one()

        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="999999",
                application_type="NEW APPLICATION",
                scraped_at="2025-06-15T12:00:00+00:00",
            )
            .on_conflict_do_nothing()
            .returning(license_records.c.id)
        )
        row = result.first()
        if row:
            record_id = row[0]
        else:
            r = await pg_conn.execute(
                select(license_records.c.id)
                .where(license_records.c.license_number == "999999")
            )
            record_id = r.scalar_one()

        return record_id, source_id

    async def test_link_and_idempotent(self, pg_conn):
        """Linking is idempotent — second call does not raise."""
        record_id, source_id = await self._seed_data(pg_conn)
        await link_record_source(pg_conn, record_id, source_id, "first_seen")
        await link_record_source(pg_conn, record_id, source_id, "first_seen")

        result = await pg_conn.execute(
            select(record_sources)
            .where(record_sources.c.record_id == record_id)
        )
        assert len(result.all()) == 1

    async def test_multiple_roles(self, pg_conn):
        """Same record+source with different roles creates distinct rows."""
        record_id, source_id = await self._seed_data(pg_conn)
        await link_record_source(pg_conn, record_id, source_id, "first_seen")
        await link_record_source(pg_conn, record_id, source_id, "confirmed")

        result = await pg_conn.execute(
            select(record_sources)
            .where(record_sources.c.record_id == record_id)
        )
        assert len(result.all()) == 2


class TestPgGetPrimarySource:
    async def _seed_full(self, pg_conn):
        """Create source types, sources, a record, and link them."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from wslcb_licensing_tracker.models import license_records, source_types, sources

        # Seed source types
        for st in [
            {"id": 1, "slug": "live_scrape", "label": "Live Scrape"},
            {"id": 2, "slug": "co_archive", "label": "CO Archive"},
        ]:
            await pg_conn.execute(
                pg_insert(source_types).values(**st).on_conflict_do_nothing()
            )

        # Create a record
        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="888888",
                application_type="NEW APPLICATION",
                scraped_at="2025-06-15T12:00:00+00:00",
            )
            .on_conflict_do_nothing()
            .returning(license_records.c.id)
        )
        row = result.first()
        if row:
            record_id = row[0]
        else:
            r = await pg_conn.execute(
                select(license_records.c.id)
                .where(license_records.c.license_number == "888888")
            )
            record_id = r.scalar_one()

        return record_id

    async def test_returns_none_for_no_sources(self, pg_conn):
        """Returns None when record has no linked sources."""
        record_id = await self._seed_full(pg_conn)
        result = await get_primary_source(pg_conn, record_id)
        assert result is None

    async def test_returns_best_source(self, pg_conn):
        """Returns first_seen source over confirmed source."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        record_id = await self._seed_full(pg_conn)

        # Create two sources
        r1 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/first.html", captured_at="2025-06-15T10:00:00")
            .returning(sources.c.id)
        )
        first_id = r1.scalar_one()

        r2 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=2, snapshot_path="test/confirmed.html", captured_at="2025-06-15T11:00:00")
            .returning(sources.c.id)
        )
        confirmed_id = r2.scalar_one()

        # Link: first as confirmed, second as first_seen (reversed order to test priority)
        await link_record_source(pg_conn, record_id, confirmed_id, "confirmed")
        await link_record_source(pg_conn, record_id, first_id, "first_seen")

        best = await get_primary_source(pg_conn, record_id)
        assert best is not None
        assert best["role"] == "first_seen"
        assert best["id"] == first_id


class TestPgGetRecordSources:
    async def test_returns_empty_list(self, pg_conn):
        """Returns empty list for record with no sources."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from wslcb_licensing_tracker.models import license_records

        # Seed source types
        from wslcb_licensing_tracker.models import source_types
        await pg_conn.execute(
            pg_insert(source_types).values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )

        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="777777",
                application_type="NEW APPLICATION",
                scraped_at="2025-06-15T12:00:00+00:00",
            )
            .returning(license_records.c.id)
        )
        record_id = result.scalar_one()

        sources_list = await get_record_sources(pg_conn, record_id)
        assert sources_list == []

    async def test_returns_all_sources(self, pg_conn):
        """Returns all linked sources newest-first."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from wslcb_licensing_tracker.models import license_records, source_types

        await pg_conn.execute(
            pg_insert(source_types).values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )

        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="666666",
                application_type="NEW APPLICATION",
                scraped_at="2025-06-15T12:00:00+00:00",
            )
            .returning(license_records.c.id)
        )
        record_id = result.scalar_one()

        # Create two sources with different captured_at
        r1 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/old.html", captured_at="2025-06-14T10:00:00")
            .returning(sources.c.id)
        )
        old_id = r1.scalar_one()

        r2 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/new.html", captured_at="2025-06-15T10:00:00")
            .returning(sources.c.id)
        )
        new_id = r2.scalar_one()

        await link_record_source(pg_conn, record_id, old_id, "first_seen")
        await link_record_source(pg_conn, record_id, new_id, "confirmed")

        results = await get_record_sources(pg_conn, record_id)
        assert len(results) == 2
        # Newest first
        assert results[0]["id"] == new_id
        assert results[1]["id"] == old_id
        # Metadata is parsed dict
        assert isinstance(results[0]["metadata"], dict)
```

### Step 2: Run to verify they fail

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_db.py -v -k "not Location"
```

Expected: `ImportError` — `get_or_create_source` not yet defined (or exists but not all functions).

### Step 3: Implement source and provenance helpers

Append to `src/wslcb_licensing_tracker/pg_db.py`:

```python
# ------------------------------------------------------------------
# Source helpers
# ------------------------------------------------------------------


async def get_or_create_source(
    conn: AsyncConnection,
    source_type_id: int,
    snapshot_path: str | None = None,
    url: str | None = None,
    captured_at: str | None = None,
    scrape_log_id: int | None = None,
    metadata: dict | None = None,
) -> int:
    """Return the source id for the given type + snapshot_path, creating if needed.

    When snapshot_path is not None, uses ON CONFLICT DO NOTHING on the
    (source_type_id, snapshot_path) unique constraint.

    When snapshot_path is None, PostgreSQL treats NULLs as distinct in
    UNIQUE constraints, so we use manual lookup-before-insert logic
    (same as the SQLite version in db.py).
    """
    meta_json = json.dumps(metadata) if metadata else "{}"

    if snapshot_path is not None:
        stmt = (
            pg_insert(sources)
            .values(
                source_type_id=source_type_id,
                snapshot_path=snapshot_path,
                url=url,
                captured_at=captured_at,
                scrape_log_id=scrape_log_id,
                metadata=meta_json,
            )
            .on_conflict_do_nothing(constraint="uq_sources_type_path")
            .returning(sources.c.id)
        )
        result = await conn.execute(stmt)
        row = result.first()
        if row:
            return row[0]
        # Conflict — fetch existing
        result = await conn.execute(
            select(sources.c.id).where(
                sources.c.source_type_id == source_type_id,
                sources.c.snapshot_path == snapshot_path,
            )
        )
        row = result.first()
        if row is None:
            msg = f"Source row vanished for type={source_type_id}, path={snapshot_path!r}"
            raise RuntimeError(msg)
        return row[0]

    # NULL snapshot_path — manual lookup
    if scrape_log_id is not None:
        result = await conn.execute(
            select(sources.c.id).where(
                sources.c.source_type_id == source_type_id,
                sources.c.snapshot_path.is_(None),
                sources.c.scrape_log_id == scrape_log_id,
            )
        )
    else:
        result = await conn.execute(
            select(sources.c.id).where(
                sources.c.source_type_id == source_type_id,
                sources.c.snapshot_path.is_(None),
                sources.c.scrape_log_id.is_(None),
            )
        )
    row = result.first()
    if row:
        return row[0]

    # Insert new
    stmt = (
        sources.insert()
        .values(
            source_type_id=source_type_id,
            snapshot_path=None,
            url=url,
            captured_at=captured_at,
            scrape_log_id=scrape_log_id,
            metadata=meta_json,
        )
        .returning(sources.c.id)
    )
    result = await conn.execute(stmt)
    return result.scalar_one()


async def link_record_source(
    conn: AsyncConnection,
    record_id: int,
    source_id: int,
    role: str = "first_seen",
) -> None:
    """Link a license record to a source (idempotent).

    Does nothing if the (record_id, source_id, role) combination already exists.
    """
    stmt = (
        pg_insert(record_sources)
        .values(record_id=record_id, source_id=source_id, role=role)
        .on_conflict_do_nothing()
    )
    await conn.execute(stmt)


# ------------------------------------------------------------------
# Provenance query helpers
# ------------------------------------------------------------------


async def get_primary_source(
    conn: AsyncConnection,
    record_id: int,
) -> dict | None:
    """Return the single most-relevant source for a record, or None.

    Priority order:
    1. Role: first_seen > repaired > confirmed
    2. Within a role: sources with a non-NULL snapshot_path first
    3. Newest captured_at as tiebreaker
    """
    stmt = text("""
        SELECT s.id, st.slug AS source_type, st.label AS source_label,
               s.snapshot_path, s.url, s.captured_at, s.ingested_at,
               s.metadata, rs.role
        FROM record_sources rs
        JOIN sources s ON s.id = rs.source_id
        JOIN source_types st ON st.id = s.source_type_id
        WHERE rs.record_id = :record_id
        ORDER BY s.captured_at DESC
    """)
    result = await conn.execute(stmt, {"record_id": record_id})
    rows = result.mappings().all()

    if not rows:
        return None

    best = None
    best_priority = (999, 999)
    for r in rows:
        d = dict(r)
        role_rank = SOURCE_ROLE_PRIORITY.get(d["role"], 2)
        no_snap = 0 if d["snapshot_path"] else 1
        priority = (role_rank, no_snap)
        if best is None or priority < best_priority:
            best = d
            best_priority = priority

    if best is not None:
        raw = best.get("metadata")
        best["metadata"] = json.loads(raw) if raw else {}
    return best


async def get_record_sources(
    conn: AsyncConnection,
    record_id: int,
) -> list[dict]:
    """Return provenance sources for a record, newest first."""
    stmt = text("""
        SELECT s.id, st.slug AS source_type, st.label AS source_label,
               s.snapshot_path, s.url, s.captured_at, s.ingested_at,
               s.metadata, rs.role
        FROM record_sources rs
        JOIN sources s ON s.id = rs.source_id
        JOIN source_types st ON st.id = s.source_type_id
        WHERE rs.record_id = :record_id
        ORDER BY s.captured_at DESC
    """)
    result = await conn.execute(stmt, {"record_id": record_id})
    results = []
    for r in result.mappings().all():
        d = dict(r)
        raw = d.get("metadata")
        d["metadata"] = json.loads(raw) if raw else {}
        results.append(d)
    return results
```

### Step 4: Run all pg_db tests — verify they pass

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_db.py -v
```

Expected: all 17 tests pass (5 location + 5 source + 2 link + 2 primary + 3 record_sources).

### Commit

```
#94 feat: add source and provenance helpers to pg_db.py
```

---

## Task 5: pg_pipeline.py — insert_record

**Files:**
- Create: `.worktrees/feat-postgresql/tests/test_pg_pipeline.py` (insert_record tests only)
- Create: `.worktrees/feat-postgresql/src/wslcb_licensing_tracker/pg_pipeline.py`

### Step 1: Write failing tests

Create `tests/test_pg_pipeline.py`:

```python
"""Tests for the async PostgreSQL ingestion pipeline (pg_pipeline.py).

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
"""

from sqlalchemy import select

from wslcb_licensing_tracker.models import license_records, locations
from wslcb_licensing_tracker.pg_pipeline import (
    BatchResult,
    IngestOptions,
    IngestResult,
    insert_record,
    ingest_record,
    ingest_batch,
)


class TestPgInsertRecord:
    async def _seed_source_types(self, pg_conn):
        """Seed source_types for tests that need provenance."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from wslcb_licensing_tracker.models import source_types

        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )

    async def test_inserts_new_record(self, pg_conn, standard_new_application):
        """Inserts a new record and returns (id, True)."""
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id, is_new = result
        assert is_new is True
        assert isinstance(record_id, int)

    async def test_duplicate_returns_false(self, pg_conn, standard_new_application):
        """Inserting the same record twice returns (id, False)."""
        result1 = await insert_record(pg_conn, standard_new_application)
        result2 = await insert_record(pg_conn, standard_new_application)
        assert result1 is not None
        assert result2 is not None
        assert result1[0] == result2[0]  # same ID
        assert result2[1] is False

    async def test_name_cleaning(self, pg_conn, standard_new_application):
        """Business name is uppercased and cleaned."""
        standard_new_application["business_name"] = "  acme cannabis co.  "
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(license_records.c.business_name)
            .where(license_records.c.id == record_id)
        )).one()
        assert row.business_name == "ACME CANNABIS CO"  # trailing dot stripped, uppercased

    async def test_creates_location(self, pg_conn, standard_new_application):
        """insert_record creates a location row and links it."""
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(license_records.c.location_id)
            .where(license_records.c.id == record_id)
        )).one()
        assert row.location_id is not None

        loc = (await pg_conn.execute(
            select(locations.c.raw_address)
            .where(locations.c.id == row.location_id)
        )).one()
        assert loc.raw_address == "123 MAIN ST, SEATTLE, WA 98101"

    async def test_preserves_raw_values(self, pg_conn, standard_new_application):
        """Raw shadow columns preserve the original (pre-cleaned) values."""
        standard_new_application["business_name"] = "acme cannabis co."
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(
                license_records.c.business_name,
                license_records.c.raw_business_name,
            ).where(license_records.c.id == record_id)
        )).one()
        assert row.business_name == "ACME CANNABIS CO"
        assert row.raw_business_name == "acme cannabis co."

    async def test_has_additional_names_flag(self, pg_conn):
        """has_additional_names is set to 1 when applicants contain the marker."""
        record = {
            "section_type": "new_application",
            "record_date": "2025-07-01",
            "business_name": "TEST BIZ",
            "business_location": "100 TEST ST",
            "applicants": "TEST BIZ; ADDITIONAL NAMES ON FILE",
            "license_type": "TEST",
            "application_type": "NEW APPLICATION",
            "license_number": "999001",
            "contact_phone": "",
            "city": "",
            "state": "WA",
            "zip_code": "",
            "previous_business_name": "",
            "previous_applicants": "",
            "previous_business_location": "",
            "previous_city": "",
            "previous_state": "",
            "previous_zip_code": "",
            "scraped_at": "2025-07-01T12:00:00+00:00",
        }
        result = await insert_record(pg_conn, record)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(license_records.c.has_additional_names)
            .where(license_records.c.id == record_id)
        )).one()
        assert row.has_additional_names == 1
```

### Step 2: Run to verify they fail

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_pipeline.py::TestPgInsertRecord -v
```

Expected: `ModuleNotFoundError: No module named 'wslcb_licensing_tracker.pg_pipeline'`

### Step 3: Implement pg_pipeline.py

Create `src/wslcb_licensing_tracker/pg_pipeline.py`:

```python
"""Async PostgreSQL ingestion pipeline for WSLCB licensing tracker.

Async equivalent of pipeline.py. Uses SQLAlchemy Core expressions and
the table objects from models.py.

Entity linking is STUBBED — Phase 3 ports entities.py and endorsements.py.
Address validation and outcome linking are also STUBBED for Phase 3.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from .db import clean_applicants_string, clean_entity_name
from .entities import ADDITIONAL_NAMES_MARKERS
from .models import license_records, record_enrichments
from .pg_db import get_or_create_location, link_record_source

logger = logging.getLogger(__name__)


@dataclass
class IngestOptions:
    """Configuration for the ingestion pipeline."""

    validate_addresses: bool = True
    link_outcomes: bool = True
    source_id: int | None = None
    source_role: str = "first_seen"
    batch_size: int = 200


@dataclass
class IngestResult:
    """Result of ingesting a single record."""

    record_id: int | None = None
    is_new: bool = False


@dataclass
class BatchResult:
    """Aggregate result of a batch ingestion."""

    inserted: int = 0
    skipped: int = 0
    errors: int = 0
    record_ids: list[int] = field(default_factory=list)


# ------------------------------------------------------------------
# Enrichment step names (canonical values for record_enrichments.step)
# ------------------------------------------------------------------

STEP_ENDORSEMENTS = "endorsements"
STEP_ENTITIES = "entities"
STEP_ADDRESS = "address"
STEP_OUTCOME_LINK = "outcome_link"


def _applicants_have_additional_names(*applicant_strings: str | None) -> bool:
    """Return True if any applicant string contains an ADDITIONAL NAMES ON FILE marker."""
    for s in applicant_strings:
        if not s:
            continue
        if any(part.strip() in ADDITIONAL_NAMES_MARKERS for part in s.split(";")):
            return True
    return False


async def _record_enrichment(
    conn: AsyncConnection,
    record_id: int,
    step: str,
    version: str = "1",
) -> None:
    """Record that an enrichment step completed for a record.

    Uses ON CONFLICT DO UPDATE so re-running a step updates the timestamp.
    """
    now = datetime.now(UTC).isoformat()
    stmt = (
        pg_insert(record_enrichments)
        .values(record_id=record_id, step=step, completed_at=now, version=version)
        .on_conflict_do_update(
            index_elements=["record_id", "step"],
            set_={"completed_at": now, "version": version},
        )
    )
    await conn.execute(stmt)


async def insert_record(
    conn: AsyncConnection,
    record: dict,
) -> tuple[int, bool] | None:
    """Insert a record, returning (id, is_new) or None on error.

    Returns (new_id, True) for freshly inserted records and
    (existing_id, False) when a duplicate is detected.

    Entity linking is STUBBED — Phase 3 adds parse_and_link_entities.
    """
    # Check for existing (dedup by natural key)
    existing = await conn.execute(
        select(license_records.c.id).where(
            license_records.c.section_type == record["section_type"],
            license_records.c.record_date == record["record_date"],
            license_records.c.license_number == record["license_number"],
            license_records.c.application_type == record["application_type"],
        ).limit(1)
    )
    row = existing.first()
    if row:
        return (row[0], False)

    # Resolve locations
    location_id = await get_or_create_location(
        conn,
        record.get("business_location", ""),
        city=record.get("city", ""),
        state=record.get("state", "WA"),
        zip_code=record.get("zip_code", ""),
    )
    previous_location_id = await get_or_create_location(
        conn,
        record.get("previous_business_location", ""),
        city=record.get("previous_city", ""),
        state=record.get("previous_state", ""),
        zip_code=record.get("previous_zip_code", ""),
    )

    # Clean names
    cleaned_biz = clean_entity_name(record.get("business_name", ""))
    cleaned_prev_biz = clean_entity_name(record.get("previous_business_name", ""))
    cleaned_applicants = clean_applicants_string(record.get("applicants", ""))
    cleaned_prev_applicants = clean_applicants_string(record.get("previous_applicants", ""))

    # Preserve raw values
    raw_biz = record.get("business_name", "")
    raw_prev_biz = record.get("previous_business_name", "")
    raw_applicants = record.get("applicants", "")
    raw_prev_applicants = record.get("previous_applicants", "")

    has_additional_names = int(
        _applicants_have_additional_names(cleaned_applicants, cleaned_prev_applicants)
    )

    try:
        stmt = (
            insert(license_records)
            .values(
                section_type=record["section_type"],
                record_date=record["record_date"],
                business_name=cleaned_biz,
                location_id=location_id,
                applicants=cleaned_applicants,
                license_type=record.get("license_type", ""),
                application_type=record["application_type"],
                license_number=record.get("license_number", ""),
                contact_phone=record.get("contact_phone", ""),
                previous_business_name=cleaned_prev_biz,
                previous_applicants=cleaned_prev_applicants,
                previous_location_id=previous_location_id,
                raw_business_name=raw_biz,
                raw_previous_business_name=raw_prev_biz,
                raw_applicants=raw_applicants,
                raw_previous_applicants=raw_prev_applicants,
                has_additional_names=has_additional_names,
                scraped_at=record["scraped_at"],
            )
            .returning(license_records.c.id)
        )
        result = await conn.execute(stmt)
        record_id = result.scalar_one()

        # TODO Phase 3: await parse_and_link_entities(conn, record_id, cleaned_applicants, "applicant")
        # TODO Phase 3: if cleaned_prev_applicants:
        #     await parse_and_link_entities(conn, record_id, cleaned_prev_applicants, "previous_applicant")

    except IntegrityError:
        return None
    else:
        return (record_id, True)


async def ingest_record(
    conn: AsyncConnection,
    record: dict,
    options: IngestOptions,
) -> IngestResult | None:
    """Insert a raw record and run all enrichment steps.

    Returns an IngestResult on success (both new and duplicate),
    or None on unexpected error.

    Steps 2-5 only run for newly inserted records; duplicates get
    provenance linked with role 'confirmed' and skip other steps.
    """
    # Step 1: Insert record (dedup, locations, name cleaning)
    try:
        result = await insert_record(conn, record)
    except Exception:
        logger.exception(
            "Error inserting record: %s/%s/#%s",
            record.get("section_type"),
            record.get("record_date"),
            record.get("license_number"),
        )
        return None

    if result is None:
        return None

    record_id, is_new = result

    if is_new:
        # STUB Phase 3: process_record (endorsements)
        # try:
        #     await process_record(conn, record_id, record.get("license_type", ""))
        #     await _record_enrichment(conn, record_id, STEP_ENDORSEMENTS)
        # except Exception:
        #     logger.exception("Error processing endorsements for record %d", record_id)

        # Entity linking is STUBBED — tracked as if completed
        # TODO Phase 3: await parse_and_link_entities(conn, record_id, ...)
        await _record_enrichment(conn, record_id, STEP_ENTITIES)

        # Step 3: Link provenance (first_seen)
        if options.source_id is not None:
            try:
                await link_record_source(
                    conn,
                    record_id,
                    options.source_id,
                    options.source_role,
                )
            except Exception:
                logger.exception(
                    "Error linking provenance for record %d",
                    record_id,
                )

        # STUB Phase 3: validate_record (address validation)
        # STUB Phase 3: link_new_record (outcome linking)

    # Duplicate — link provenance as 'confirmed'
    elif options.source_id is not None:
        try:
            await link_record_source(
                conn,
                record_id,
                options.source_id,
                "confirmed",
            )
        except Exception:
            logger.exception(
                "Error linking confirmed provenance for record %d",
                record_id,
            )

    return IngestResult(record_id=record_id, is_new=is_new)


async def ingest_batch(
    conn: AsyncConnection,
    records: list[dict],
    options: IngestOptions,
) -> BatchResult:
    """Ingest multiple records with progress logging and batch commits.

    Commits every options.batch_size records to allow recovery from
    interruption.
    """
    result = BatchResult()

    for i, rec in enumerate(records):
        ir = await ingest_record(conn, rec, options)
        if ir is None:
            result.errors += 1
        elif ir.is_new:
            result.inserted += 1
            result.record_ids.append(ir.record_id)
        else:
            result.skipped += 1

        if (i + 1) % options.batch_size == 0:
            await conn.commit()
            logger.debug(
                "  progress: %d / %d (inserted=%d, skipped=%d, errors=%d)",
                i + 1,
                len(records),
                result.inserted,
                result.skipped,
                result.errors,
            )

    await conn.commit()
    return result
```

### Step 4: Run tests — verify they pass

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_pipeline.py::TestPgInsertRecord -v
```

Expected: 6 tests pass.

### Commit

```
#94 feat: add pg_pipeline.py with async insert_record
```

---

## Task 6: pg_pipeline.py — ingest_record and ingest_batch tests

**Files:**
- Modify: `.worktrees/feat-postgresql/tests/test_pg_pipeline.py` (add ingest tests)

### Step 1: Write ingest_record tests

Append to `tests/test_pg_pipeline.py`:

```python
class TestPgIngestRecord:
    async def _seed_source(self, pg_conn):
        """Create source type + source row, return source_id."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from wslcb_licensing_tracker.models import source_types, sources

        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        result = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/ingest.html")
            .on_conflict_do_nothing()
            .returning(sources.c.id)
        )
        row = result.first()
        if row:
            return row[0]
        from wslcb_licensing_tracker.models import sources as src_table
        r = await pg_conn.execute(
            select(src_table.c.id).where(src_table.c.snapshot_path == "test/ingest.html")
        )
        return r.scalar_one()

    async def test_inserts_new_record(self, pg_conn, standard_new_application):
        """ingest_record inserts a new record."""
        options = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = await ingest_record(pg_conn, standard_new_application, options)
        assert result is not None
        assert result.is_new is True
        assert result.record_id is not None

    async def test_duplicate_confirmed_provenance(self, pg_conn, standard_new_application):
        """Duplicate record gets provenance linked as 'confirmed'."""
        source_id = await self._seed_source(pg_conn)
        options = IngestOptions(
            validate_addresses=False,
            link_outcomes=False,
            source_id=source_id,
        )
        r1 = await ingest_record(pg_conn, standard_new_application, options)
        r2 = await ingest_record(pg_conn, standard_new_application, options)
        assert r1.is_new is True
        assert r2.is_new is False
        assert r1.record_id == r2.record_id

        # Verify 'confirmed' provenance link exists
        from wslcb_licensing_tracker.models import record_sources
        result = await pg_conn.execute(
            select(record_sources.c.role).where(
                record_sources.c.record_id == r2.record_id,
                record_sources.c.role == "confirmed",
            )
        )
        assert result.first() is not None

    async def test_links_provenance(self, pg_conn, standard_new_application):
        """New record gets provenance linked as 'first_seen'."""
        source_id = await self._seed_source(pg_conn)
        options = IngestOptions(
            validate_addresses=False,
            link_outcomes=False,
            source_id=source_id,
            source_role="first_seen",
        )
        r = await ingest_record(pg_conn, standard_new_application, options)
        assert r.is_new is True

        from wslcb_licensing_tracker.models import record_sources
        result = await pg_conn.execute(
            select(record_sources.c.role).where(
                record_sources.c.record_id == r.record_id,
                record_sources.c.role == "first_seen",
            )
        )
        assert result.first() is not None

    async def test_enrichment_tracked(self, pg_conn, standard_new_application):
        """Entity enrichment step is tracked for new records."""
        options = IngestOptions(validate_addresses=False, link_outcomes=False)
        r = await ingest_record(pg_conn, standard_new_application, options)
        assert r.is_new is True

        from wslcb_licensing_tracker.models import record_enrichments
        result = await pg_conn.execute(
            select(record_enrichments.c.step).where(
                record_enrichments.c.record_id == r.record_id,
            )
        )
        steps = [row[0] for row in result]
        assert "entities" in steps
```

### Step 2: Run ingest_record tests

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_pipeline.py::TestPgIngestRecord -v
```

Expected: 4 tests pass (the implementation already exists from Task 5).

### Step 3: Write ingest_batch test

Append to `tests/test_pg_pipeline.py`:

```python
async def test_pg_ingest_batch(pg_engine, standard_new_application):
    """ingest_batch processes multiple records and commits."""
    # ingest_batch commits internally, so we use pg_engine directly
    # and clean up after ourselves
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from wslcb_licensing_tracker.models import source_types

    async with pg_engine.connect() as conn:
        await conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        await conn.commit()

    records = []
    for i in range(5):
        rec = dict(standard_new_application)
        rec["license_number"] = f"BATCH{i:04d}"
        records.append(rec)

    options = IngestOptions(
        validate_addresses=False,
        link_outcomes=False,
        batch_size=2,
    )

    async with pg_engine.connect() as conn:
        try:
            result = await ingest_batch(conn, records, options)
            assert result.inserted == 5
            assert result.skipped == 0
            assert result.errors == 0
            assert len(result.record_ids) == 5
        finally:
            # Clean up committed data
            await conn.execute(
                license_records.delete().where(
                    license_records.c.license_number.like("BATCH%")
                )
            )
            await conn.commit()
```

### Step 4: Run all pipeline tests

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_pipeline.py -v
```

Expected: all 11 tests pass.

### Commit

```
#94 test: add ingest_record and ingest_batch tests for pg_pipeline.py
```

---

## Task 7: Full test suite verification

**Files:** None (verification only)

### Step 1: Run all PG tests

```bash
cd .worktrees/feat-postgresql
TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" uv run pytest tests/test_pg_db.py tests/test_pg_schema.py tests/test_pg_pipeline.py tests/test_pg_database.py -v
```

Expected: all PG tests pass (~34 tests).

### Step 2: Run all SQLite tests (regression check)

```bash
cd .worktrees/feat-postgresql
uv run pytest tests/ -v --ignore=tests/test_pg_db.py --ignore=tests/test_pg_schema.py --ignore=tests/test_pg_pipeline.py --ignore=tests/test_pg_database.py
```

Expected: all 632 existing tests pass. No regressions.

### Step 3: Run linting

```bash
cd .worktrees/feat-postgresql
uv run ruff check src/wslcb_licensing_tracker/pg_db.py src/wslcb_licensing_tracker/pg_schema.py src/wslcb_licensing_tracker/pg_pipeline.py
```

Expected: no errors.

### Commit (if any fixes needed)

```
#94 chore: fix lint issues in pg modules
```

---

## Summary of deliverables

| New file | Functions | Test file | Test count |
|---|---|---|---|
| `pg_schema.py` | `init_db`, `_table_exists`, `_column_exists` | `test_pg_schema.py` | 6 |
| `pg_db.py` | `get_or_create_location`, `get_or_create_source`, `link_record_source`, `get_primary_source`, `get_record_sources` | `test_pg_db.py` | 17 |
| `pg_pipeline.py` | `insert_record`, `ingest_record`, `ingest_batch`, `_record_enrichment`, `IngestOptions`, `IngestResult`, `BatchResult` | `test_pg_pipeline.py` | 11 |

**Total new tests:** ~34
**Existing tests affected:** 0 (all SQLite tests remain unchanged)

## Phase 3 handoff

Phase 2 leaves these stubs for Phase 3 to fill in:

- `pg_pipeline.py`: entity linking (`parse_and_link_entities`), endorsement processing (`process_record`), address validation (`validate_record`), outcome linking (`link_new_record`)
- No async versions of `queries.py`, `endorsements.py`, `entities.py`, `link_records.py`, `address_validator.py` yet
