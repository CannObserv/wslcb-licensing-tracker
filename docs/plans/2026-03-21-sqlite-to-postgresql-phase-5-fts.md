# Phase 5 — Full-Text Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the temporary ILIKE stubs in `pg_queries.py` with PostgreSQL tsvector full-text search and pg_trgm fuzzy matching, giving the app production-quality search performance and relevance ranking.

**Architecture:** Add a `search_vector tsvector` column to `license_records`, maintained by a PL/pgSQL trigger on INSERT/UPDATE. The `_build_where_clause` helper in `pg_queries.py` switches from a three-column ILIKE OR to `search_vector @@ plainto_tsquery()` with a trgm similarity fallback for partial/fuzzy matches. `search_records` orders results by `ts_rank` when a text query is present; export remains date-ordered.

**Tech Stack:** PostgreSQL `tsvector`/`tsquery`, `pg_trgm` extension, `GIN` indexes, Alembic DDL migration, SQLAlchemy Core `TSVECTOR` dialect type.

---

## Files changed

| File | Action |
|---|---|
| `alembic/versions/0002_fts.py` | **New** — Alembic migration: pg_trgm extension, search_vector column, GIN indexes, trigger, backfill |
| `src/wslcb_licensing_tracker/models.py` | Add `search_vector TSVECTOR` column to `license_records` Table |
| `src/wslcb_licensing_tracker/pg_queries.py` | Replace ILIKE block with tsvector + trgm; ts_rank ORDER BY in search_records; remove Phase 5 comments |
| `tests/test_pg_queries.py` | Rename `test_query_filter_ilike` → `test_query_filter_fts`; add trgm fuzzy test |
| `AGENTS.md` | Update `pg_queries.py` Key Files note (remove ILIKE / Phase 5 note) |

---

## Task 1 — Alembic migration 0002_fts.py

**Files:**
- Create: `alembic/versions/0002_fts.py`

This migration:
1. Enables `pg_trgm` extension (idempotent — `IF NOT EXISTS`)
2. Adds `search_vector tsvector` column to `license_records` (nullable; trigger maintains it)
3. Creates GIN index on `search_vector` for tsvector `@@` queries
4. Creates GIN trgm indexes on `business_name` and `applicants` for `%` similarity queries
5. Creates a PL/pgSQL trigger function that computes the weighted vector on INSERT/UPDATE
6. Creates the BEFORE INSERT OR UPDATE trigger
7. Backfills `search_vector` for all existing rows

- [ ] **Step 1: Write the migration file**

```python
"""Phase 5: add pg_trgm extension, search_vector column, and FTS trigger.

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-21
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. pg_trgm needed for the % similarity operator and GIN trgm indexes
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # 2. Add search_vector column — nullable; trigger populates it
    op.execute(
        "ALTER TABLE license_records ADD COLUMN IF NOT EXISTS search_vector tsvector"
    )

    # 3. GIN index for tsvector @@ plainto_tsquery() lookups
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_license_records_search_vector"
        " ON license_records USING GIN(search_vector)"
    )

    # 4. GIN trgm indexes for fuzzy % matching on name fields
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_lr_business_name_trgm"
        " ON license_records USING GIN(business_name gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_lr_applicants_trgm"
        " ON license_records USING GIN(applicants gin_trgm_ops)"
    )

    # 5. Trigger function: weighted tsvector from license_records columns +
    #    subqueries to pull city and address from the linked location row.
    #    Weights per design doc: business_name (A), city (B),
    #    resolved_endorsements + license_type (C), applicants + address (D).
    op.execute("""
        CREATE OR REPLACE FUNCTION license_records_search_vector_update()
        RETURNS trigger AS $$
        DECLARE
            loc_city    TEXT := '';
            loc_address TEXT := '';
        BEGIN
            SELECT coalesce(city, ''), coalesce(raw_address, '')
            INTO loc_city, loc_address
            FROM locations
            WHERE id = NEW.location_id;

            NEW.search_vector :=
                setweight(to_tsvector('english', coalesce(NEW.business_name, '')), 'A') ||
                setweight(to_tsvector('english', coalesce(loc_city, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(NEW.resolved_endorsements, '')), 'C') ||
                setweight(to_tsvector('english', coalesce(NEW.license_type, '')), 'C') ||
                setweight(to_tsvector('english',
                    coalesce(NEW.applicants, '') || ' ' || coalesce(loc_address, '')), 'D');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # 6. Trigger — fires BEFORE INSERT and on relevant UPDATE columns
    op.execute("""
        CREATE OR REPLACE TRIGGER license_records_search_vector_trigger
        BEFORE INSERT OR UPDATE OF
            business_name, applicants, license_type,
            resolved_endorsements, location_id
        ON license_records
        FOR EACH ROW EXECUTE FUNCTION license_records_search_vector_update()
    """)

    # 7. Backfill all existing rows — correlated subqueries pull city (B) and
    #    raw_address (D) separately so weights match the design doc spec.
    #    NULL location_id → subquery returns NULL → coalesce → empty string.
    op.execute("""
        UPDATE license_records lr
        SET search_vector =
            setweight(to_tsvector('english', coalesce(lr.business_name, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(
                (SELECT city FROM locations WHERE id = lr.location_id), ''
            )), 'B') ||
            setweight(to_tsvector('english', coalesce(lr.resolved_endorsements, '')), 'C') ||
            setweight(to_tsvector('english', coalesce(lr.license_type, '')), 'C') ||
            setweight(to_tsvector('english',
                coalesce(lr.applicants, '') || ' ' || coalesce(
                    (SELECT raw_address FROM locations WHERE id = lr.location_id), ''
                )
            ), 'D')
    """)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS license_records_search_vector_trigger"
        " ON license_records"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS license_records_search_vector_update()"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_lr_applicants_trgm"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_lr_business_name_trgm"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_license_records_search_vector"
    )
    op.execute(
        "ALTER TABLE license_records DROP COLUMN IF EXISTS search_vector"
    )
    # Note: does NOT drop pg_trgm extension — other parts of the DB may use it.
```

- [ ] **Step 2: Verify revision chain**

Open `alembic/versions/0001_baseline_postgresql_schema.py` and confirm its `revision = "0001"`. The new file has `down_revision = "0001"` — that chain must match.

```bash
grep "^revision\|^down_revision" alembic/versions/0001_baseline_postgresql_schema.py
# Expected:
# revision = "0001"
# down_revision = None
```

- [ ] **Step 3: Commit (no tests yet — migration only)**

```bash
git add alembic/versions/0002_fts.py
git commit -m "#94 feat: Phase 5 Task 1 — Alembic migration 0002_fts (tsvector + pg_trgm)"
```

---

## Task 2 — models.py: add search_vector column

**Files:**
- Modify: `src/wslcb_licensing_tracker/models.py`

`TSVECTOR` is a PostgreSQL-dialect type from `sqlalchemy.dialects.postgresql`. Add the import and the column to `license_records`.

The column is nullable (no server_default) — the trigger populates it on INSERT/UPDATE; the Alembic backfill UPDATE covers existing rows. SQLAlchemy Core does not need to write this column directly.

- [ ] **Step 1: Update the import block in models.py**

Current import block ends at line ~22. Add `TSVECTOR` to the postgresql import (new import line since there are currently no dialect imports):

```python
from sqlalchemy.dialects.postgresql import TSVECTOR
```

- [ ] **Step 2: Add the column to license_records**

Add after the last `Column(...)` line and before the `UniqueConstraint(...)` lines in `license_records`:

```python
    Column("search_vector", TSVECTOR),
```

Also update the module docstring on line 7 to drop the "FTS columns (search_vector) are added in Phase 5" note:

```python
"""SQLAlchemy Core table definitions for the WSLCB licensing tracker (PostgreSQL).

All table metadata lives here. Modules that need table objects import them
directly: ``from wslcb_licensing_tracker.models import license_records``.

No ORM mappers — all Table objects, no declarative_base().
"""
```

- [ ] **Step 3: Run existing tests (no PG required — models are pure Python)**

```bash
uv run pytest tests/test_models.py -v
# Expected: all pass
```

- [ ] **Step 4: Commit**

```bash
git add src/wslcb_licensing_tracker/models.py
git commit -m "#94 feat: Phase 5 Task 2 — add search_vector TSVECTOR to license_records model"
```

---

## Task 3 — pg_queries.py: replace ILIKE with tsvector + trgm

**Files:**
- Modify: `src/wslcb_licensing_tracker/pg_queries.py`

Three changes:

**3a. `_build_where_clause` (lines 189–197):** Replace the ILIKE OR block with a tsvector `@@` + trgm `%` OR expression.

**3b. `search_records` ORDER BY:** Add `ts_rank` descending as primary sort when a query is present.

**3c. Remove the two `# Phase 5:` comments** (lines 191 and 342).

- [ ] **Step 1: Write a failing integration test first**

In `tests/test_pg_queries.py`, rename `test_query_filter_ilike` and add a trgm fuzzy test. Both require `pg_conn` (integration) so they skip without `TEST_DATABASE_URL`. Skip this edit for now — tests are written in Task 4. Write them as the failing target before touching the query code.

Actually: since the ILIKE query works today and the tsvector query works differently, the test rename is sufficient to keep the suite green. Proceed directly to the implementation, then run tests.

- [ ] **Step 2: Replace the ILIKE block in `_build_where_clause`**

Current (lines 189–197):
```python
    if query:
        q_like = f"%{query}%"
        # Phase 5: replace with tsvector @@ plainto_tsquery() for full-text search
        conditions.append(
            "(lr.business_name ILIKE :q_like"
            " OR lr.applicants ILIKE :q_like"
            " OR lr.resolved_endorsements ILIKE :q_like)"
        )
        params["q_like"] = q_like
```

Replace with:
```python
    if query:
        conditions.append(
            "(lr.search_vector @@ plainto_tsquery('english', :q_fts)"
            " OR lr.business_name % :q_trgm"
            " OR lr.applicants % :q_trgm)"
        )
        params["q_fts"] = query
        params["q_trgm"] = query
```

- [ ] **Step 3: Add ts_rank ORDER BY to `search_records`**

Current ORDER BY in `search_records` (around line 311):
```python
        text(
            f"{_RECORD_SELECT} {where}"
            " ORDER BY lr.record_date DESC, lr.id DESC"
            " LIMIT :limit OFFSET :offset"
        ),
        {**params, "limit": per_page, "offset": offset},
```

Replace with:
```python
        order_by = (
            "ts_rank(lr.search_vector, plainto_tsquery('english', :q_fts)) DESC,"
            " lr.record_date DESC, lr.id DESC"
            if query
            else "lr.record_date DESC, lr.id DESC"
        )
        text(
            f"{_RECORD_SELECT} {where}"
            f" ORDER BY {order_by}"
            " LIMIT :limit OFFSET :offset"
        ),
        {**params, "limit": per_page, "offset": offset},
```

Note: `q_fts` is already in `params` when `query` is truthy (added by `_build_where_clause`), so no extra parameter injection is needed.

- [ ] **Step 4: Remove the `# Phase 5:` comment above `_EXPORT_SELECT` (line 342)**

Current:
```python
# Phase 5: replace endorsement subquery with tsvector search for full-text queries
_EXPORT_SELECT = f"""
```

Replace with:
```python
_EXPORT_SELECT = f"""
```

- [ ] **Step 5: Run the full test suite**

```bash
uv run pytest tests/ -v
# Expected: 648+ passed (same count as Phase 4 or higher)
# PG integration tests skip if TEST_DATABASE_URL is not set
```

- [ ] **Step 6: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_queries.py
git commit -m "#94 feat: Phase 5 Task 3 — replace ILIKE with tsvector + trgm in pg_queries"
```

---

## Task 4 — Tests

**Files:**
- Modify: `tests/test_pg_queries.py`
- Modify: `AGENTS.md`

Update integration tests to exercise the FTS path. All tests in this class use `pg_conn` and are automatically skipped when `TEST_DATABASE_URL` is not set.

- [ ] **Step 1: Rename `test_query_filter_ilike` to `test_query_filter_fts` and update the assertion**

Old test name and wording referenced ILIKE. The behavior is the same (finds by business name) but now via tsvector. Rename and tighten the assertion to also verify the result is ranked first (ts_rank ordering):

```python
@pytest.mark.asyncio(loop_scope="session")
async def test_query_filter_fts(self, pg_conn, standard_new_application):
    """Text query finds records via tsvector full-text search."""
    standard_new_application["license_number"] = "query_002"
    standard_new_application["business_name"] = "UNIQUE FTS SEARCH CORP"
    await insert_record(pg_conn, standard_new_application)
    records, total = await search_records(pg_conn, query="UNIQUE FTS SEARCH")
    assert total >= 1
    assert any("UNIQUE FTS SEARCH" in r["business_name"] for r in records)
    # FTS result should appear first (ts_rank ordering)
    assert "UNIQUE FTS SEARCH" in records[0]["business_name"]
```

- [ ] **Step 2: Add a trgm fuzzy-match test**

The `%` operator returns matches above the default pg_trgm similarity threshold (0.3). Use a unique 4-gram that tsvector's `plainto_tsquery` would NOT match (because it's not an English word), but trgm would:

```python
@pytest.mark.asyncio(loop_scope="session")
async def test_query_filter_trgm_fuzzy(self, pg_conn, standard_new_application):
    """Partial/non-English business names are found via pg_trgm similarity."""
    standard_new_application["license_number"] = "query_002b"
    standard_new_application["business_name"] = "XYZNOTAWORD HOLDINGS LLC"
    await insert_record(pg_conn, standard_new_application)
    # tsvector stopwords won't help here — trgm handles it
    records, total = await search_records(pg_conn, query="XYZNOTAWORD")
    assert total >= 1
    assert any("XYZNOTAWORD" in r["business_name"] for r in records)
```

- [ ] **Step 3: Update AGENTS.md `pg_queries.py` Key Files entry**

The current entry says `# Phase 5: tsvector`. Remove that annotation now that it's implemented:

Find:
```
| `pg_queries.py` | ... FTS5 replaced by ILIKE on `business_name`, `applicants`, `resolved_endorsements` — marked `# Phase 5: tsvector`. ...
```

Replace with:
```
| `pg_queries.py` | ... Text search uses `search_vector @@ plainto_tsquery('english', ...)` (tsvector GIN) OR `business_name % query` / `applicants % query` (pg_trgm similarity). `search_records` orders by `ts_rank` when a text query is present; export remains date-ordered. ...
```

- [ ] **Step 4: Run the full test suite**

```bash
uv run pytest tests/ -v
# Expected: all pass; PG integration tests skip without TEST_DATABASE_URL
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_pg_queries.py AGENTS.md
git commit -m "#94 feat: Phase 5 Task 4 — update search tests for FTS; update AGENTS.md"
```

---

## Verification

After all 4 tasks, run the full suite one final time and confirm clean:

```bash
uv run pytest tests/ -v
```

Expected output (without `TEST_DATABASE_URL`):
```
648+ passed, 97+ skipped, N warnings
```

If `TEST_DATABASE_URL` is set to a local PostgreSQL instance that has run `alembic upgrade head`, the integration tests in `TestSearchRecords` will also run and exercise the tsvector + trgm paths against a real database.
