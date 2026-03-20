# PostgreSQL Migration — Phase 1: Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add SQLAlchemy Core metadata, AsyncEngine, and Alembic with a full PostgreSQL baseline migration — without modifying any existing code.

**Architecture:** New `models.py` defines all 20 tables as SQLAlchemy `Table` objects. New `database.py` provides `AsyncEngine` and `get_db()`. Alembic manages all future DDL changes from a baseline that encodes the final state of the 15 SQLite migrations. Existing SQLite code is untouched — this phase only adds new files.

**Tech Stack:** SQLAlchemy 2.0 (Core, async), asyncpg, psycopg[binary] (Alembic sync env), Alembic 1.13+, PostgreSQL 16+

**Design doc:** `docs/plans/2026-03-20-sqlite-to-postgresql-design.md`
**Branch:** `feat/94-postgresql-migration`
**Worktree:** `.worktrees/feat-postgresql/`

---

## Prerequisite: Install and configure PostgreSQL

Not a TDD task — system setup. Do this before starting Task 1.

```bash
# Install PostgreSQL
sudo apt-get update
sudo apt-get install -y postgresql postgresql-client

# Start service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create app user and database (dev-only credentials; production credentials
# live in /etc/wslcb-licensing-tracker/env — do not use these in production)
sudo -u postgres psql -c "CREATE USER wslcb WITH PASSWORD 'wslcb' CREATEDB;"
sudo -u postgres psql -c "CREATE DATABASE wslcb OWNER wslcb;"

# Create test database
sudo -u postgres psql -c "CREATE DATABASE wslcb_test OWNER wslcb;"

# Verify
psql postgresql://wslcb:wslcb@localhost/wslcb -c "SELECT version();"
```

Set environment variables (add to `/etc/wslcb-licensing-tracker/env` for production, export locally for dev):

```bash
export DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"
export TEST_DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test"
```

---

## Task 1: Add dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add new production and dev dependencies**

Edit `pyproject.toml` — add to `dependencies`:

```toml
dependencies = [
    # ... existing deps ...
    "sqlalchemy[asyncio]>=2.0,<3",
    "alembic>=1.13,<2",
    "asyncpg>=0.30,<1",
    "psycopg[binary]>=3.2,<4",
]
```

Add `pytest-asyncio` is already present. No new dev deps needed.

- [ ] **Step 2: Sync and verify**

```bash
uv sync --dev
uv run python -c "import sqlalchemy; import alembic; import asyncpg; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Run existing tests to confirm nothing broke**

```bash
uv run pytest tests/ -q
```

Expected: `622 passed`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "#94 chore: add sqlalchemy, alembic, asyncpg dependencies"
```

---

## Task 2: Create `models.py`

All 20 SQLAlchemy `Table` definitions. No ORM mappers. No FTS columns (those come in Phase 5).

**Files:**
- Create: `src/wslcb_licensing_tracker/models.py`
- Create: `tests/test_models.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_models.py`:

```python
"""Tests for SQLAlchemy table metadata definitions."""

from wslcb_licensing_tracker import models


EXPECTED_TABLES = {
    "locations",
    "license_endorsements",
    "endorsement_codes",
    "scrape_log",
    "source_types",
    "sources",
    "license_records",
    "record_endorsements",
    "entities",
    "record_entities",
    "record_links",
    "record_enrichments",
    "record_sources",
    "admin_users",
    "admin_audit_log",
    "endorsement_aliases",
    "endorsement_dismissed_suggestions",
    "regulated_substances",
    "regulated_substance_endorsements",
    "data_migrations",
}


def test_all_tables_present():
    """metadata contains all expected table names."""
    assert set(models.metadata.tables.keys()) == EXPECTED_TABLES


def test_locations_columns():
    """locations table has all expected columns."""
    cols = {c.name for c in models.locations.c}
    assert "id" in cols
    assert "raw_address" in cols
    assert "city" in cols
    assert "std_region" in cols
    assert "std_postal_code" in cols
    assert "latitude" in cols


def test_license_records_columns():
    """license_records has unique constraint columns."""
    cols = {c.name for c in models.license_records.c}
    assert {"section_type", "record_date", "license_number", "application_type"} <= cols
    assert "resolved_endorsements" in cols
    assert "has_additional_names" in cols


def test_sources_metadata_column():
    """sources.metadata column is present."""
    cols = {c.name for c in models.sources.c}
    assert "metadata" in cols


def test_data_migrations_columns():
    """data_migrations table has id, name, applied_at."""
    cols = {c.name for c in models.data_migrations.c}
    assert cols == {"id", "name", "applied_at"}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_models.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` for `models`

- [ ] **Step 3: Implement `models.py`**

Create `src/wslcb_licensing_tracker/models.py`:

```python
"""SQLAlchemy Core table definitions for the WSLCB licensing tracker (PostgreSQL).

All table metadata lives here. Modules that need table objects import them
directly: ``from wslcb_licensing_tracker.models import license_records``.

No ORM mappers — all Table objects, no declarative_base(). FTS columns
(search_vector) are added in Phase 5.
"""

from sqlalchemy import (
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy import Identity

metadata = MetaData()

locations = Table(
    "locations",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("raw_address", Text, nullable=False),
    Column("city", Text, server_default="''"),
    Column("state", Text, server_default="'WA'"),
    Column("zip_code", Text, server_default="''"),
    Column("std_address_line_1", Text, server_default="''"),
    Column("std_address_line_2", Text, server_default="''"),
    Column("std_city", Text, server_default="''"),
    Column("std_region", Text, server_default="''"),
    Column("std_postal_code", Text, server_default="''"),
    Column("std_country", Text, server_default="''"),
    Column("std_address_string", Text),
    Column("validation_status", Text),
    Column("dpv_match_code", Text),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("address_standardized_at", Text),
    Column("address_validated_at", Text),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("raw_address", name="uq_locations_raw_address"),
    Index("idx_locations_city", "city"),
    Index("idx_locations_zip", "zip_code"),
    Index("idx_locations_std_city", "std_city"),
    Index("idx_locations_std_postal_code", "std_postal_code"),
)

license_endorsements = Table(
    "license_endorsements",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("name", name="uq_license_endorsements_name"),
)

endorsement_codes = Table(
    "endorsement_codes",
    metadata,
    Column("code", Text, nullable=False, primary_key=True),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
)

scrape_log = Table(
    "scrape_log",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("started_at", Text, nullable=False),
    Column("finished_at", Text),
    Column("status", Text, nullable=False, server_default="'running'"),
    Column("records_new", Integer, server_default="0"),
    Column("records_approved", Integer, server_default="0"),
    Column("records_discontinued", Integer, server_default="0"),
    Column("records_skipped", Integer, server_default="0"),
    Column("error_message", Text),
    Column("snapshot_path", Text),
    Column("content_hash", Text),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
)

source_types = Table(
    "source_types",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("slug", Text, nullable=False),
    Column("label", Text, nullable=False),
    Column("description", Text, server_default="''"),
    UniqueConstraint("slug", name="uq_source_types_slug"),
)

sources = Table(
    "sources",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column(
        "source_type_id",
        Integer,
        ForeignKey("source_types.id"),
        nullable=False,
    ),
    Column("snapshot_path", Text),
    Column("url", Text),
    Column("captured_at", Text),
    Column("ingested_at", Text, nullable=False, server_default="now()::text"),
    Column("scrape_log_id", Integer, ForeignKey("scrape_log.id")),
    Column("metadata", Text, nullable=False, server_default="'{}'"),
    UniqueConstraint("source_type_id", "snapshot_path", name="uq_sources_type_path"),
)

license_records = Table(
    "license_records",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("section_type", Text, nullable=False),
    Column("record_date", Text, nullable=False),
    Column("business_name", Text),
    Column("location_id", Integer, ForeignKey("locations.id")),
    Column("applicants", Text),
    Column("license_type", Text),
    Column("application_type", Text),
    Column("license_number", Text),
    Column("contact_phone", Text),
    Column("previous_business_name", Text, server_default="''"),
    Column("previous_applicants", Text, server_default="''"),
    Column("previous_location_id", Integer, ForeignKey("locations.id")),
    Column("raw_business_name", Text),
    Column("raw_previous_business_name", Text),
    Column("raw_applicants", Text),
    Column("raw_previous_applicants", Text),
    Column("has_additional_names", Integer, nullable=False, server_default="0"),
    Column("resolved_endorsements", Text, nullable=False, server_default="''"),
    Column("scraped_at", Text, nullable=False),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint(
        "section_type",
        "record_date",
        "license_number",
        "application_type",
        name="uq_license_records_natural_key",
    ),
    Index("idx_records_section", "section_type"),
    Index("idx_records_date", "record_date"),
    Index("idx_records_business", "business_name"),
    Index("idx_records_license_num", "license_number"),
    Index("idx_records_app_type", "application_type"),
    Index("idx_records_location", "location_id"),
    Index("idx_records_prev_location", "previous_location_id"),
)

record_endorsements = Table(
    "record_endorsements",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Index("idx_re_endorsement", "endorsement_id"),
)

entities = Table(
    "entities",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("entity_type", Text, nullable=False, server_default="''"),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("name", name="uq_entities_name"),
    # NOTE: migration creates this as a functional index: lower(name).
    # SQLAlchemy Core can't express functional indexes inline on Table objects,
    # so this plain Index intentionally differs from the migration DDL.
    # Do NOT "fix" this to match — alembic autogenerate will flag the
    # discrepancy but it is harmless and expected.
    Index("idx_entities_name_lower", "name"),
)

record_entities = Table(
    "record_entities",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "entity_id",
        Integer,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("role", Text, nullable=False, server_default="'applicant'", primary_key=True),
    Column("position", Integer, nullable=False, server_default="0"),
    Index("idx_re_entity", "entity_id"),
    Index("idx_re_role", "role"),
)

record_links = Table(
    "record_links",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column(
        "new_app_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "outcome_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "confidence",
        Text,
        nullable=False,
    ),
    Column("days_gap", Integer),
    Column("linked_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("new_app_id", "outcome_id", name="uq_record_links"),
    CheckConstraint("confidence IN ('high', 'medium', 'low')", name="ck_record_links_confidence"),
    Index("idx_record_links_new", "new_app_id"),
    Index("idx_record_links_outcome", "outcome_id"),
)

record_enrichments = Table(
    "record_enrichments",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("step", Text, nullable=False, primary_key=True),
    Column("completed_at", Text, nullable=False),
    Column("version", Text, nullable=False, server_default="'1'"),
)

record_sources = Table(
    "record_sources",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "source_id",
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "role",
        Text,
        nullable=False,
        server_default="'first_seen'",
        primary_key=True,
    ),
    CheckConstraint(
        "role IN ('first_seen', 'confirmed', 'repaired')", name="ck_record_sources_role"
    ),
    Index("idx_rs_source", "source_id"),
)

admin_users = Table(
    "admin_users",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("email", Text, nullable=False),
    Column("role", Text, nullable=False, server_default="'admin'"),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    Column("created_by", Text, nullable=False, server_default="'system'"),
    UniqueConstraint("email", name="uq_admin_users_email"),
)

admin_audit_log = Table(
    "admin_audit_log",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("admin_email", Text, nullable=False),
    Column("action", Text, nullable=False),
    Column("target_type", Text, nullable=False),
    Column("target_id", Integer),
    Column("details", Text),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
)

endorsement_aliases = Table(
    "endorsement_aliases",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "canonical_endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    Column("created_by", Text),
    UniqueConstraint("endorsement_id", name="uq_endorsement_aliases_endorsement"),
)

endorsement_dismissed_suggestions = Table(
    "endorsement_dismissed_suggestions",
    metadata,
    Column(
        "endorsement_id_a",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "endorsement_id_b",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("dismissed_by", Text, nullable=False),
    Column("dismissed_at", Text, nullable=False, server_default="now()::text"),
    CheckConstraint(
        "endorsement_id_a < endorsement_id_b",
        name="ck_dismissed_suggestions_order",
    ),
)

regulated_substances = Table(
    "regulated_substances",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("display_order", Integer, nullable=False, server_default="0"),
    UniqueConstraint("name", name="uq_regulated_substances_name"),
)

regulated_substance_endorsements = Table(
    "regulated_substance_endorsements",
    metadata,
    Column(
        "substance_id",
        Integer,
        ForeignKey("regulated_substances.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Index("idx_rse_endorsement", "endorsement_id"),
)

# Tracks completed data-only migrations (distinct from Alembic schema revisions).
# Replaces the unconditional startup repair calls — see design doc Phase 4 / issue #85.
data_migrations = Table(
    "data_migrations",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("applied_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("name", name="uq_data_migrations_name"),
)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_models.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Run full test suite to confirm no regressions**

```bash
uv run pytest tests/ -q
```

Expected: `627 passed` (622 + 5 new)

- [ ] **Step 6: Commit**

```bash
git add src/wslcb_licensing_tracker/models.py tests/test_models.py
git commit -m "#94 feat: add SQLAlchemy Core table metadata (models.py)"
```

---

## Task 3: Create `database.py`

AsyncEngine factory and `get_db()` dependency.

**Files:**
- Create: `src/wslcb_licensing_tracker/database.py`
- Create: `tests/test_database.py`

- [ ] **Step 1: Write the failing test**

Note: `pyproject.toml` already has `asyncio_mode = "auto"` — all `async def test_*` functions are automatically treated as async tests. No `@pytest.mark.asyncio` decorators needed.

Create `tests/test_database.py`:

```python
"""Tests for async database engine and connection management.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
Tests are skipped automatically when the env var is absent.
"""

import os

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from wslcb_licensing_tracker.database import create_engine_from_env, get_database_url


@pytest.fixture
def test_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set — skipping PostgreSQL tests")
    return url


@pytest.fixture
async def pg_engine(test_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_url)
    engine = create_engine_from_env()
    yield engine
    await engine.dispose()


def test_get_database_url_default(monkeypatch):
    """Returns fallback URL when DATABASE_URL not set."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    url = get_database_url()
    assert "postgresql" in url


def test_get_database_url_from_env(monkeypatch):
    """Returns DATABASE_URL from environment."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pw@host/db")
    assert get_database_url() == "postgresql+asyncpg://user:pw@host/db"


async def test_engine_connects(pg_engine):
    """Engine can open a connection and run a query."""
    async with pg_engine.connect() as conn:
        result = await conn.execute(text("SELECT 1 AS val"))
        row = result.mappings().one()
        assert row["val"] == 1


async def test_get_db_yields_async_connection(pg_engine, monkeypatch):
    """get_db() yields an AsyncConnection."""
    from wslcb_licensing_tracker.database import get_db

    async with get_db(pg_engine) as conn:
        assert isinstance(conn, AsyncConnection)
        result = await conn.execute(text("SELECT 42 AS answer"))
        row = result.mappings().one()
        assert row["answer"] == 42
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_database.py -v
```

Expected: `ImportError` on `database` module (non-PG tests fail, PG tests skip if no URL set)

- [ ] **Step 3: Implement `database.py`**

Create `src/wslcb_licensing_tracker/database.py`:

```python
"""Async database engine and connection management for PostgreSQL.

Replaces the synchronous SQLite connection layer in db.py. Provides an
AsyncEngine with connection pooling and a get_db() async context manager
for use as a FastAPI dependency or standalone async caller.

db.py is retained for constants and text utilities.
"""

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

logger_name = __name__

_DEFAULT_DATABASE_URL = "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"


def get_database_url() -> str:
    """Return DATABASE_URL from environment, with a localhost default."""
    return os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL)


def create_engine_from_env(
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
) -> AsyncEngine:
    """Create an AsyncEngine using DATABASE_URL from the environment.

    Call once at application startup; store on ``app.state.engine``.
    """
    return create_async_engine(
        get_database_url(),
        pool_size=pool_size,
        max_overflow=max_overflow,
        echo=echo,
    )


@asynccontextmanager
async def get_db(engine: AsyncEngine) -> AsyncGenerator[AsyncConnection, None]:
    """Yield an AsyncConnection from the pool.

    Usage as FastAPI dependency::

        async def get_db_dep(request: Request) -> AsyncGenerator[AsyncConnection, None]:
            async with get_db(request.app.state.engine) as conn:
                yield conn

    Usage in non-web code::

        engine = create_engine_from_env()
        async with get_db(engine) as conn:
            result = await conn.execute(select(license_records))
    """
    async with engine.connect() as conn:
        yield conn
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_database.py -v
```

Expected: `test_get_database_url_default` and `test_get_database_url_from_env` pass; PG-connection tests skip or pass depending on `TEST_DATABASE_URL`.

If `TEST_DATABASE_URL` is set, all 4 tests should pass.

- [ ] **Step 5: Run full suite**

```bash
uv run pytest tests/ -q
```

Expected: all pass (+ new tests)

- [ ] **Step 6: Commit**

```bash
git add src/wslcb_licensing_tracker/database.py tests/test_database.py
git commit -m "#94 feat: add async database engine and get_db() (database.py)"
```

---

## Task 4: Initialize Alembic

One-time setup — no TDD. Creates the Alembic directory structure and configures `env.py` for async migrations.

**Files:**
- Create: `alembic/` (via `alembic init`)
- Modify: `alembic/env.py`
- Modify: `alembic/alembic.ini` (rename to `alembic.ini` at project root)

- [ ] **Step 1: Run alembic init**

Run from the worktree root (`.worktrees/feat-postgresql/`):

```bash
uv run alembic init alembic
```

Expected: creates `alembic/`, `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako`, `alembic/versions/`

- [ ] **Step 2: Configure `alembic.ini`**

In `alembic.ini`, set `sqlalchemy.url` to a placeholder (overridden in `env.py`):

```ini
sqlalchemy.url = postgresql+asyncpg://wslcb:wslcb@localhost/wslcb
```

- [ ] **Step 3: Replace `alembic/env.py` with async-compatible version**

```python
"""Alembic migration environment — async SQLAlchemy + asyncpg."""

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from wslcb_licensing_tracker.models import metadata

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata


def get_url() -> str:
    """Return DATABASE_URL from environment (falls back to alembic.ini value)."""
    return os.environ.get(
        "DATABASE_URL",
        config.get_main_option("sqlalchemy.url", "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"),
    )


def run_migrations_offline() -> None:
    """Run migrations without a live DB connection (generates SQL)."""
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:  # noqa: ANN001
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(get_url())
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 4: Verify Alembic can see the metadata**

```bash
DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" \
  uv run alembic current
```

Expected: `INFO  [alembic.runtime.migration] Context impl PostgreSQLImpl.`
No errors about missing tables — just "current revision" output.

- [ ] **Step 5: Commit**

```bash
git add alembic/ alembic.ini
git commit -m "#94 chore: initialize Alembic with async PostgreSQL env"
```

---

## Task 5: Write and verify baseline migration

Full PostgreSQL schema in a single Alembic revision — equivalent to running all 15 SQLite migrations plus the new `data_migrations` table.

**Files:**
- Create: `alembic/versions/0001_baseline_postgresql_schema.py`

- [ ] **Step 1: Create the baseline revision file**

```bash
uv run alembic revision --rev-id 0001 -m "baseline_postgresql_schema"
```

This creates `alembic/versions/0001_baseline_postgresql_schema.py`.

- [ ] **Step 2: Write `upgrade()` with full schema DDL**

Replace the generated file content:

```python
"""Baseline PostgreSQL schema — equivalent to all 15 SQLite migrations.

Revision ID: 0001
Revises:
Create Date: 2026-03-20
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE locations (
            id          SERIAL PRIMARY KEY,
            raw_address TEXT NOT NULL,
            city        TEXT NOT NULL DEFAULT '',
            state       TEXT NOT NULL DEFAULT 'WA',
            zip_code    TEXT NOT NULL DEFAULT '',
            std_address_line_1      TEXT NOT NULL DEFAULT '',
            std_address_line_2      TEXT NOT NULL DEFAULT '',
            std_city                TEXT NOT NULL DEFAULT '',
            std_region              TEXT NOT NULL DEFAULT '',
            std_postal_code         TEXT NOT NULL DEFAULT '',
            std_country             TEXT NOT NULL DEFAULT '',
            std_address_string      TEXT,
            validation_status       TEXT,
            dpv_match_code          TEXT,
            latitude                DOUBLE PRECISION,
            longitude               DOUBLE PRECISION,
            address_standardized_at TEXT,
            address_validated_at    TEXT,
            created_at              TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_locations_raw_address UNIQUE (raw_address)
        )
    """)
    op.execute("CREATE INDEX idx_locations_city ON locations(city)")
    op.execute("CREATE INDEX idx_locations_zip ON locations(zip_code)")
    op.execute("CREATE INDEX idx_locations_std_city ON locations(std_city)")
    op.execute("CREATE INDEX idx_locations_std_postal_code ON locations(std_postal_code)")

    op.execute("""
        CREATE TABLE license_endorsements (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_license_endorsements_name UNIQUE (name)
        )
    """)

    op.execute("""
        CREATE TABLE endorsement_codes (
            code           TEXT    NOT NULL,
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            created_at     TEXT NOT NULL DEFAULT now()::text,
            PRIMARY KEY (code, endorsement_id)
        )
    """)

    op.execute("""
        CREATE TABLE scrape_log (
            id                    SERIAL PRIMARY KEY,
            started_at            TEXT NOT NULL,
            finished_at           TEXT,
            status                TEXT NOT NULL DEFAULT 'running',
            records_new           INTEGER DEFAULT 0,
            records_approved      INTEGER DEFAULT 0,
            records_discontinued  INTEGER DEFAULT 0,
            records_skipped       INTEGER DEFAULT 0,
            error_message         TEXT,
            snapshot_path         TEXT,
            content_hash          TEXT,
            created_at            TEXT NOT NULL DEFAULT now()::text
        )
    """)

    op.execute("""
        CREATE TABLE source_types (
            id          INTEGER PRIMARY KEY,
            slug        TEXT NOT NULL,
            label       TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            CONSTRAINT uq_source_types_slug UNIQUE (slug)
        )
    """)

    op.execute("""
        INSERT INTO source_types (id, slug, label, description) VALUES
            (1, 'live_scrape',     'Live Scrape',       'Direct scrape of the WSLCB licensing page'),
            (2, 'co_archive',      'CO Page Archive',   'Cannabis Observer archived HTML snapshots'),
            (3, 'internet_archive','Internet Archive',  'Wayback Machine snapshots'),
            (4, 'co_diff_archive', 'CO Diff Archive',   'Cannabis Observer diff-detected change snapshots'),
            (5, 'manual',          'Manual Entry',      'Manually entered or corrected records')
    """)

    op.execute("""
        CREATE TABLE sources (
            id             SERIAL PRIMARY KEY,
            source_type_id INTEGER NOT NULL REFERENCES source_types(id),
            snapshot_path  TEXT,
            url            TEXT,
            captured_at    TEXT,
            ingested_at    TEXT NOT NULL DEFAULT now()::text,
            scrape_log_id  INTEGER REFERENCES scrape_log(id),
            metadata       TEXT NOT NULL DEFAULT '{}',
            CONSTRAINT uq_sources_type_path UNIQUE (source_type_id, snapshot_path)
        )
    """)

    op.execute("""
        CREATE TABLE license_records (
            id                       SERIAL PRIMARY KEY,
            section_type             TEXT NOT NULL,
            record_date              TEXT NOT NULL,
            business_name            TEXT,
            location_id              INTEGER REFERENCES locations(id),
            applicants               TEXT,
            license_type             TEXT,
            application_type         TEXT,
            license_number           TEXT,
            contact_phone            TEXT,
            previous_business_name   TEXT NOT NULL DEFAULT '',
            previous_applicants      TEXT NOT NULL DEFAULT '',
            previous_location_id     INTEGER REFERENCES locations(id),
            raw_business_name        TEXT,
            raw_previous_business_name TEXT,
            raw_applicants           TEXT,
            raw_previous_applicants  TEXT,
            has_additional_names     INTEGER NOT NULL DEFAULT 0,
            resolved_endorsements    TEXT NOT NULL DEFAULT '',
            scraped_at               TEXT NOT NULL,
            created_at               TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_license_records_natural_key
                UNIQUE (section_type, record_date, license_number, application_type)
        )
    """)
    op.execute("CREATE INDEX idx_records_section ON license_records(section_type)")
    op.execute("CREATE INDEX idx_records_date ON license_records(record_date)")
    op.execute("CREATE INDEX idx_records_business ON license_records(business_name)")
    op.execute("CREATE INDEX idx_records_license_num ON license_records(license_number)")
    op.execute("CREATE INDEX idx_records_app_type ON license_records(application_type)")
    op.execute("CREATE INDEX idx_records_location ON license_records(location_id)")
    op.execute("CREATE INDEX idx_records_prev_location ON license_records(previous_location_id)")

    op.execute("""
        CREATE TABLE record_endorsements (
            record_id      INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            PRIMARY KEY (record_id, endorsement_id)
        )
    """)
    op.execute("CREATE INDEX idx_re_endorsement ON record_endorsements(endorsement_id)")

    op.execute("""
        CREATE TABLE entities (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            entity_type TEXT NOT NULL DEFAULT '',
            created_at  TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_entities_name UNIQUE (name)
        )
    """)
    op.execute("CREATE INDEX idx_entities_name_lower ON entities(lower(name))")

    op.execute("""
        CREATE TABLE record_entities (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            entity_id INTEGER NOT NULL
                REFERENCES entities(id) ON DELETE CASCADE,
            role      TEXT NOT NULL DEFAULT 'applicant',
            position  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (record_id, entity_id, role)
        )
    """)
    op.execute("CREATE INDEX idx_re_entity ON record_entities(entity_id)")
    op.execute("CREATE INDEX idx_re_role ON record_entities(role)")

    op.execute("""
        CREATE TABLE record_links (
            id         SERIAL PRIMARY KEY,
            new_app_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            outcome_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            confidence TEXT NOT NULL
                CHECK (confidence IN ('high', 'medium', 'low')),
            days_gap   INTEGER,
            linked_at  TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_record_links UNIQUE (new_app_id, outcome_id)
        )
    """)
    op.execute("CREATE INDEX idx_record_links_new ON record_links(new_app_id)")
    op.execute("CREATE INDEX idx_record_links_outcome ON record_links(outcome_id)")

    op.execute("""
        CREATE TABLE record_enrichments (
            record_id    INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            step         TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            version      TEXT NOT NULL DEFAULT '1',
            PRIMARY KEY (record_id, step)
        )
    """)

    op.execute("""
        CREATE TABLE record_sources (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            source_id INTEGER NOT NULL
                REFERENCES sources(id) ON DELETE CASCADE,
            role      TEXT NOT NULL DEFAULT 'first_seen'
                CHECK (role IN ('first_seen', 'confirmed', 'repaired')),
            PRIMARY KEY (record_id, source_id, role)
        )
    """)
    op.execute("CREATE INDEX idx_rs_source ON record_sources(source_id)")

    op.execute("""
        CREATE TABLE admin_users (
            id         SERIAL PRIMARY KEY,
            email      TEXT NOT NULL,
            role       TEXT NOT NULL DEFAULT 'admin',
            created_at TEXT NOT NULL DEFAULT now()::text,
            created_by TEXT NOT NULL DEFAULT 'system',
            CONSTRAINT uq_admin_users_email UNIQUE (lower(email))
        )
    """)

    op.execute("""
        CREATE TABLE admin_audit_log (
            id          SERIAL PRIMARY KEY,
            admin_email TEXT NOT NULL,
            action      TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id   INTEGER,
            details     TEXT,
            created_at  TEXT NOT NULL DEFAULT now()::text
        )
    """)

    op.execute("""
        CREATE TABLE endorsement_aliases (
            id                      SERIAL PRIMARY KEY,
            endorsement_id          INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            canonical_endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            created_at              TEXT NOT NULL DEFAULT now()::text,
            created_by              TEXT,
            CONSTRAINT uq_endorsement_aliases_endorsement UNIQUE (endorsement_id)
        )
    """)

    op.execute("""
        CREATE TABLE endorsement_dismissed_suggestions (
            endorsement_id_a INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            endorsement_id_b INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            dismissed_by     TEXT NOT NULL,
            dismissed_at     TEXT NOT NULL DEFAULT now()::text,
            PRIMARY KEY (endorsement_id_a, endorsement_id_b),
            CHECK (endorsement_id_a < endorsement_id_b)
        )
    """)

    op.execute("""
        CREATE TABLE regulated_substances (
            id            SERIAL PRIMARY KEY,
            name          TEXT NOT NULL,
            display_order INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT uq_regulated_substances_name UNIQUE (lower(name))
        )
    """)

    op.execute("""
        CREATE TABLE regulated_substance_endorsements (
            substance_id   INTEGER NOT NULL
                REFERENCES regulated_substances(id) ON DELETE CASCADE,
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            PRIMARY KEY (substance_id, endorsement_id)
        )
    """)
    op.execute(
        "CREATE INDEX idx_rse_endorsement ON regulated_substance_endorsements(endorsement_id)"
    )

    # Tracks completed data-only migrations (issue #85).
    op.execute("""
        CREATE TABLE data_migrations (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_data_migrations_name UNIQUE (name)
        )
    """)


def downgrade() -> None:
    tables = [
        "data_migrations",
        "regulated_substance_endorsements",
        "regulated_substances",
        "endorsement_dismissed_suggestions",
        "endorsement_aliases",
        "admin_audit_log",
        "admin_users",
        "record_sources",
        "record_enrichments",
        "record_links",
        "record_entities",
        "entities",
        "record_endorsements",
        "license_records",
        "sources",
        "source_types",
        "scrape_log",
        "endorsement_codes",
        "license_endorsements",
        "locations",
    ]
    for t in tables:
        op.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
```

- [ ] **Step 3: Run the migration against the test database**

```bash
DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" \
  uv run alembic upgrade head
```

Expected: `Running upgrade  -> 0001, baseline_postgresql_schema`

- [ ] **Step 4: Verify all tables were created**

```bash
psql postgresql://wslcb:wslcb@localhost/wslcb_test \
  -c "\dt" | grep -c "public"
```

Expected: `20` (20 tables)

- [ ] **Step 5: Write integration test for migration**

Add to `tests/test_database.py`:

```python
async def test_alembic_baseline_creates_all_tables(pg_engine):
    """After running the baseline migration, all 20 tables exist."""
    from alembic.config import Config
    from alembic import command

    # Run alembic upgrade head against the test engine
    alembic_cfg = Config("alembic.ini")
    test_url = os.environ.get("TEST_DATABASE_URL")
    alembic_cfg.set_main_option("sqlalchemy.url", test_url)

    # Run in thread to avoid blocking event loop
    import asyncio
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: command.upgrade(alembic_cfg, "head")
    )

    # Verify table count via information_schema
    async with pg_engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ))
        count = result.scalar()
    # 20 app tables + 1 alembic_version
    assert count == 21
```

- [ ] **Step 6: Run all tests**

```bash
uv run pytest tests/ -q
```

Expected: all pass

- [ ] **Step 7: Test downgrade round-trip**

```bash
DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" \
  uv run alembic downgrade base
DATABASE_URL="postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test" \
  uv run alembic upgrade head
```

Expected: both commands succeed with no errors.

- [ ] **Step 8: Commit**

```bash
git add alembic/versions/0001_baseline_postgresql_schema.py
git commit -m "#94 feat: add Alembic baseline migration with full PostgreSQL schema"
```

---

## Task 6: Push and verify

- [ ] **Step 1: Run full test suite one final time**

```bash
uv run pytest tests/ -v 2>&1 | tail -20
```

Expected: all pass

- [ ] **Step 2: Push feature branch**

```bash
git push -u origin feat/94-postgresql-migration
```

- [ ] **Step 3: Verify on GitHub**

Check that CI passes (if configured). The branch is ready for Phase 2 review.

---

## Phase 2 preview

Phase 2 ports `db.py` helpers and `pipeline.py` to async SQLAlchemy Core. It depends on this foundation being complete and all tests green.

Plan file: `docs/plans/2026-03-20-sqlite-to-postgresql-phase-2-core-data-layer.md` (written when Phase 1 is done)
