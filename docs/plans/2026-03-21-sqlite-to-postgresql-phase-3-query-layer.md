# SQLite → PostgreSQL Phase 3 — Query Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port all query-layer modules to async SQLAlchemy Core, producing pg_* async equivalents for each of the 9 SQLite modules.

**Architecture:** Same parallel-module pattern as Phase 2. Each SQLite module gets a `pg_*` async equivalent. SQLite modules unchanged. `pg_queries.py` imports from other Phase 3 `pg_*` modules. Text search uses ILIKE as FTS5 stopgap until Phase 5 tsvector.

**Tech Stack:** Python 3.12, SQLAlchemy 2.0 Core async, asyncpg, pytest-asyncio session-scoped

---

## Quick-reference: SQL translation patterns

| SQLite | PostgreSQL / SQLAlchemy |
|---|---|
| `INSERT OR IGNORE` | `pg_insert(t).on_conflict_do_nothing()` |
| `INSERT OR REPLACE` | `pg_insert(t).on_conflict_do_update(...)` |
| `cursor.lastrowid` | `.returning(t.c.id)` + scalar fetch |
| `group_concat(col, '; ')` | `func.string_agg(col, text("'; '"))` |
| `GLOB '[0-9]*'` | `col.op('~')(r'^\d+$')` |
| `date(col, '-7 days')` | `func.date(col) - text("interval '7 days'")` |
| `date(col, '+N days')` | `func.date(col) + text(f"interval '{n} days'")` |
| `date('now', '-N days')` | `func.current_date() - text(f"interval '{n} days'")` |
| `fetchall()` → `r["col"]` | `(await conn.execute(stmt)).mappings().all()` |
| `fetchone()` → `r["col"]` | `(await conn.execute(stmt)).mappings().first()` |
| FTS5 `MATCH ?` | `col.ilike(f'%{q}%')` — mark `# TODO Phase 5: tsvector` |
| `GROUP_CONCAT` in subquery | `STRING_AGG` with `ORDER BY` inside aggregate |
| `COLLATE NOCASE` | `ILIKE` / `lower(col) = lower(param)` |
| `ORDER BY confidence = 'high' DESC` | `ORDER BY (confidence = 'high') DESC` (PostgreSQL allows boolean expressions) |

## Key imports for every pg_* module

```python
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import select, text, func, delete
from .models import <relevant_table_objects>
```

---

## Task 1 — pg_admin_audit.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_admin_audit.py`
- Test: `tests/test_pg_admin_audit.py`

### Step 1a — Write failing test

```python
# tests/test_pg_admin_audit.py
"""Tests for pg_admin_audit.py — async audit log helpers."""
import pytest
from wslcb_licensing_tracker.pg_admin_audit import log_action, get_audit_log


class TestLogAction:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_int_id(self, pg_conn):
        row_id = await log_action(
            pg_conn, "test@example.com", "endorsement.test",
            "endorsement", target_id=1, details={"key": "value"}
        )
        assert isinstance(row_id, int)
        assert row_id > 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_null_details(self, pg_conn):
        row_id = await log_action(
            pg_conn, "test@example.com", "endorsement.noop", "endorsement"
        )
        assert isinstance(row_id, int)


class TestGetAuditLog:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_rows_and_count(self, pg_conn):
        await log_action(pg_conn, "a@example.com", "test.action", "record", target_id=99)
        rows, total = await get_audit_log(pg_conn)
        assert total >= 1
        assert isinstance(rows, list)
        assert all("details_parsed" in r for r in rows)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_filter_by_action(self, pg_conn):
        await log_action(pg_conn, "b@example.com", "unique.action.xyz", "record")
        rows, total = await get_audit_log(pg_conn, filters={"action": "unique.action.xyz"})
        assert total >= 1
        assert all(r["action"] == "unique.action.xyz" for r in rows)
```

- [ ] Run `uv run pytest tests/test_pg_admin_audit.py -v` — confirm `ImportError` (module does not exist yet)

### Step 1b — Implement

```python
# src/wslcb_licensing_tracker/pg_admin_audit.py
"""Async PostgreSQL admin audit log helpers.

Async equivalent of admin_audit.py. Uses SQLAlchemy Core + asyncpg.

Public API
----------
- log_action  — insert one audit row, return id
- get_audit_log — paginated retrieval with optional filters
"""
import json
import logging
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import admin_audit_log

logger = logging.getLogger(__name__)


async def log_action(
    conn: AsyncConnection,
    email: str,
    action: str,
    target_type: str,
    target_id: int | None = None,
    details: dict[str, Any] | None = None,
) -> int:
    """Insert one audit log row and return its id. Caller commits."""
    details_json: str | None = json.dumps(details) if details is not None else None
    stmt = (
        pg_insert(admin_audit_log)
        .values(
            admin_email=email,
            action=action,
            target_type=target_type,
            target_id=target_id,
            details=details_json,
        )
        .returning(admin_audit_log.c.id)
    )
    row_id = (await conn.execute(stmt)).scalar_one()
    logger.debug("Audit log: %s by %s on %s#%s", action, email, target_type, target_id)
    return row_id


async def get_audit_log(
    conn: AsyncConnection,
    page: int = 1,
    per_page: int = 50,
    filters: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return a paginated slice of the audit log.

    filters keys: action, target_type, admin_email, date_from, date_to (YYYY-MM-DD).
    Returns (rows, total_count). Each row has a 'details_parsed' key.
    """
    filters = filters or {}
    where_parts: list[str] = []
    params: dict[str, Any] = {}

    if filters.get("action"):
        where_parts.append("action = :action")
        params["action"] = filters["action"]
    if filters.get("target_type"):
        where_parts.append("target_type = :target_type")
        params["target_type"] = filters["target_type"]
    if filters.get("admin_email"):
        where_parts.append("lower(admin_email) = lower(:admin_email)")
        params["admin_email"] = filters["admin_email"]
    if filters.get("date_from"):
        where_parts.append("created_at::date >= :date_from")
        params["date_from"] = filters["date_from"]
    if filters.get("date_to"):
        where_parts.append("created_at::date <= :date_to")
        params["date_to"] = filters["date_to"]

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    count_row = (
        await conn.execute(
            text(f"SELECT COUNT(*) FROM admin_audit_log {where_sql}"),
            params,
        )
    ).scalar_one()
    total_count = int(count_row)

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset
    rows_raw = (
        await conn.execute(
            text(
                f"""
                SELECT id, admin_email, action, target_type, target_id,
                       details, created_at
                FROM admin_audit_log
                {where_sql}
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            params,
        )
    ).mappings().all()

    rows = []
    for r in rows_raw:
        details_raw = r["details"]
        rows.append({
            "id": r["id"],
            "admin_email": r["admin_email"],
            "action": r["action"],
            "target_type": r["target_type"],
            "target_id": r["target_id"],
            "details": details_raw,
            "details_parsed": json.loads(details_raw) if details_raw else None,
            "created_at": r["created_at"],
        })
    return rows, total_count
```

- [ ] Run `uv run pytest tests/test_pg_admin_audit.py -v` — all green

### Step 1c — Commit

```bash
cd /home/exedev/wslcb-licensing-tracker/.worktrees/feat-postgresql
git add src/wslcb_licensing_tracker/pg_admin_audit.py tests/test_pg_admin_audit.py
git commit -m "#94 feat: add pg_admin_audit — async audit log helpers"
```

---

## Task 2 — pg_substances.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_substances.py`
- Test: `tests/test_pg_substances.py`

### Step 2a — Write failing test

```python
# tests/test_pg_substances.py
"""Tests for pg_substances.py — async regulated substance CRUD."""
import pytest
from wslcb_licensing_tracker.pg_substances import (
    add_substance, get_regulated_substances,
    get_substance_endorsement_ids, remove_substance, set_substance_endorsements,
)
from wslcb_licensing_tracker.models import license_endorsements
from sqlalchemy.dialects.postgresql import insert as pg_insert


class TestAddSubstance:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_int_id(self, pg_conn):
        sid = await add_substance(pg_conn, "TEST SUBSTANCE", 10)
        assert isinstance(sid, int) and sid > 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_regulated_substances_includes_new(self, pg_conn):
        await add_substance(pg_conn, "CANNABIS FLOWER", 1)
        results = await get_regulated_substances(pg_conn)
        names = [r["name"] for r in results]
        assert "CANNABIS FLOWER" in names

    @pytest.mark.asyncio(loop_scope="session")
    async def test_remove_returns_name(self, pg_conn):
        sid = await add_substance(pg_conn, "TO REMOVE", 99)
        name = await remove_substance(pg_conn, sid)
        assert name == "TO REMOVE"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_set_and_get_endorsement_ids(self, pg_conn):
        # Create an endorsement row directly
        eid = (await pg_conn.execute(
            pg_insert(license_endorsements).values(name="TEST ENDORSEMENT X")
            .on_conflict_do_nothing()
            .returning(license_endorsements.c.id)
        )).scalar_one()
        sid = await add_substance(pg_conn, "SUBSTANCE WITH ENDORSEMENT", 5)
        await set_substance_endorsements(pg_conn, sid, [eid])
        ids = await get_substance_endorsement_ids(pg_conn, sid)
        assert eid in ids
```

- [ ] Run `uv run pytest tests/test_pg_substances.py -v` — confirm `ImportError`

### Step 2b — Implement

Key points:
- `get_regulated_substances`: Use a single query with `func.string_agg` and LEFT JOIN, then group by substance. Alternatively, use two queries (substances + per-substance endorsements) matching the SQLite approach for clarity.
- `add_substance`: `pg_insert(regulated_substances).values(...).returning(regulated_substances.c.id)`
- `set_substance_endorsements`: `delete(regulated_substance_endorsements).where(...)` then batch insert with `on_conflict_do_nothing()`
- `remove_substance`: SELECT name first, then DELETE (cascade handles junction rows if FK cascade is set; otherwise delete junction rows manually first)

```python
# src/wslcb_licensing_tracker/pg_substances.py
"""Async PostgreSQL regulated substance CRUD.

Async equivalent of substances.py. No admin_audit dependency — caller logs.
"""
import logging

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import (
    license_endorsements,
    regulated_substance_endorsements,
    regulated_substances,
)

logger = logging.getLogger(__name__)


async def get_regulated_substances(conn: AsyncConnection) -> list[dict]:
    """All substances ordered by display_order, each with endorsement names list."""
    rows = (
        await conn.execute(
            select(
                regulated_substances.c.id,
                regulated_substances.c.name,
                regulated_substances.c.display_order,
            ).order_by(regulated_substances.c.display_order, regulated_substances.c.name)
        )
    ).mappings().all()

    results = []
    for row in rows:
        enames = (
            await conn.execute(
                select(license_endorsements.c.name)
                .join(
                    regulated_substance_endorsements,
                    regulated_substance_endorsements.c.endorsement_id == license_endorsements.c.id,
                )
                .where(regulated_substance_endorsements.c.substance_id == row["id"])
                .order_by(license_endorsements.c.name)
            )
        ).scalars().all()
        results.append({
            "id": row["id"],
            "name": row["name"],
            "display_order": row["display_order"],
            "endorsements": list(enames),
        })
    return results


async def get_substance_endorsement_ids(conn: AsyncConnection, substance_id: int) -> list[int]:
    """Endorsement IDs linked to substance_id."""
    rows = (
        await conn.execute(
            select(regulated_substance_endorsements.c.endorsement_id).where(
                regulated_substance_endorsements.c.substance_id == substance_id
            )
        )
    ).scalars().all()
    return list(rows)


async def set_substance_endorsements(
    conn: AsyncConnection,
    substance_id: int,
    endorsement_ids: list[int],
) -> None:
    """Replace full endorsement list for substance_id. Caller commits + audits."""
    await conn.execute(
        delete(regulated_substance_endorsements).where(
            regulated_substance_endorsements.c.substance_id == substance_id
        )
    )
    for eid in endorsement_ids:
        await conn.execute(
            pg_insert(regulated_substance_endorsements)
            .values(substance_id=substance_id, endorsement_id=eid)
            .on_conflict_do_nothing()
        )


async def add_substance(conn: AsyncConnection, name: str, display_order: int) -> int:
    """Insert a new regulated substance and return its id. Caller commits + audits."""
    return (
        await conn.execute(
            pg_insert(regulated_substances)
            .values(name=name, display_order=display_order)
            .returning(regulated_substances.c.id)
        )
    ).scalar_one()


async def remove_substance(conn: AsyncConnection, substance_id: int) -> str | None:
    """Delete a regulated substance. Returns name for audit logging, or None if not found."""
    row = (
        await conn.execute(
            select(regulated_substances.c.name).where(
                regulated_substances.c.id == substance_id
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    # Delete junction rows first (in case no CASCADE is configured)
    await conn.execute(
        delete(regulated_substance_endorsements).where(
            regulated_substance_endorsements.c.substance_id == substance_id
        )
    )
    await conn.execute(
        delete(regulated_substances).where(regulated_substances.c.id == substance_id)
    )
    return row
```

- [ ] Run `uv run pytest tests/test_pg_substances.py -v` — all green

### Step 2c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_substances.py tests/test_pg_substances.py
git commit -m "#94 feat: add pg_substances — async regulated substance CRUD"
```

---

## Task 3 — pg_endorsements.py

This is the most complex module. It ports `endorsements.py` including the full parsing pipeline, alias management, and the `_sync_resolved_endorsements` UPDATE.

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_endorsements.py`
- Test: `tests/test_pg_endorsements.py`

### Step 3a — Write failing tests

```python
# tests/test_pg_endorsements.py
"""Tests for pg_endorsements.py — async endorsement pipeline."""
import pytest
from sqlalchemy.dialects.postgresql import insert as pg_insert
from wslcb_licensing_tracker.models import license_endorsements, license_records
from wslcb_licensing_tracker.pg_endorsements import (
    ensure_endorsement,
    get_endorsement_groups,
    get_endorsement_options,
    get_record_endorsements,
    process_record,
    remove_alias,
    rename_endorsement,
    resolve_endorsement,
    set_canonical_endorsement,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestEnsureEndorsement:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_and_returns_id(self, pg_conn):
        eid = await ensure_endorsement(pg_conn, "cannabis retailer")
        assert isinstance(eid, int)
        # Second call returns same id (idempotent)
        eid2 = await ensure_endorsement(pg_conn, "CANNABIS RETAILER")
        assert eid == eid2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_uppercases_name(self, pg_conn):
        eid = await ensure_endorsement(pg_conn, "spirits store")
        from sqlalchemy import select
        row = (await pg_conn.execute(
            select(license_endorsements.c.name).where(license_endorsements.c.id == eid)
        )).scalar_one()
        assert row == "SPIRITS STORE"


class TestProcessRecord:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_text_semicolon_format(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "endorse_001"
        standard_new_application["license_type"] = "CANNABIS RETAILER; DELIVERY"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        linked = await process_record(pg_conn, record_id, "CANNABIS RETAILER; DELIVERY")
        assert linked == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_numeric_code_format(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "endorse_002"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        # Seed a code mapping first
        eid = await ensure_endorsement(pg_conn, "GROCERY STORE - BEER/WINE")
        await pg_conn.execute(
            pg_insert(
                __import__('wslcb_licensing_tracker.models', fromlist=['endorsement_codes']).endorsement_codes
            ).values(code="450", endorsement_id=eid).on_conflict_do_nothing()
        )
        linked = await process_record(pg_conn, record_id, "450,")
        assert linked >= 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "endorse_003"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await process_record(pg_conn, record_id, "SPIRITS RETAILER")
        await process_record(pg_conn, record_id, "SPIRITS RETAILER")
        # Calling twice should not duplicate rows
        endorsements = await get_record_endorsements(pg_conn, [record_id])
        assert endorsements[record_id].count("SPIRITS RETAILER") == 1


class TestAliasManagement:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_resolve_with_alias(self, pg_conn):
        variant_id = await ensure_endorsement(pg_conn, "BEER AND WINE VARIANT")
        canonical_id = await ensure_endorsement(pg_conn, "BEER AND WINE CANONICAL")
        await set_canonical_endorsement(pg_conn, canonical_id, [variant_id])
        resolved = await resolve_endorsement(pg_conn, variant_id)
        assert resolved == canonical_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_remove_alias(self, pg_conn):
        v_id = await ensure_endorsement(pg_conn, "REMOVE ALIAS VARIANT")
        c_id = await ensure_endorsement(pg_conn, "REMOVE ALIAS CANONICAL")
        await set_canonical_endorsement(pg_conn, c_id, [v_id])
        await remove_alias(pg_conn, v_id)
        resolved = await resolve_endorsement(pg_conn, v_id)
        assert resolved == v_id  # back to self
```

- [ ] Run `uv run pytest tests/test_pg_endorsements.py -v` — confirm `ImportError`

### Step 3b — Implement

Key implementation notes:

**`ensure_endorsement`** — ON CONFLICT DO NOTHING + RETURNING + fallback SELECT:
```python
async def ensure_endorsement(conn: AsyncConnection, name: str) -> int:
    name = name.upper()
    stmt = (
        pg_insert(license_endorsements)
        .values(name=name)
        .on_conflict_do_nothing(index_elements=["name"])
        .returning(license_endorsements.c.id)
    )
    row_id = (await conn.execute(stmt)).scalar_one_or_none()
    if row_id is None:
        # Row already existed; fetch it
        row_id = (await conn.execute(
            select(license_endorsements.c.id).where(license_endorsements.c.id == name)
        )).scalar_one()
        # Correction: use .where(license_endorsements.c.name == name)
    return row_id
```

**Numeric code detection** — replace `cleaned.isdigit()` with regex-based check (handles the stripped value, which `.isdigit()` also handles — keep `.isdigit()` for the cleaned value since `GLOB '[0-9]*'` is only needed in SQL queries in seed/admin modules).

**`_sync_resolved_endorsements`** — use `text()` with PostgreSQL syntax:
```python
async def _sync_resolved_endorsements(conn: AsyncConnection, record_id: int) -> None:
    stmt = text("""
        UPDATE license_records
        SET resolved_endorsements = COALESCE((
            SELECT STRING_AGG(
                COALESCE(
                    (SELECT le2.name FROM license_endorsements le2
                     JOIN endorsement_aliases ea
                       ON ea.canonical_endorsement_id = le2.id
                     WHERE ea.endorsement_id = le.id
                     LIMIT 1),
                    le.name
                ), '; ' ORDER BY le.name
            )
            FROM record_endorsements re
            JOIN license_endorsements le ON le.id = re.endorsement_id
            WHERE re.record_id = :record_id
        ), '')
        WHERE id = :record_id
    """)
    await conn.execute(stmt, {"record_id": record_id})
```

**`set_canonical_endorsement`** — use `pg_insert(...).on_conflict_do_update(index_elements=["endorsement_id"], set_={...})`. The `datetime('now')` SQLite expression becomes `func.now()`.

**`reprocess_endorsements`** — for the `code` filter, replace `GLOB ? || ', *'` with a LIKE pattern: `license_type LIKE :code_prefix` where `code_prefix = f"{code_stripped}, %"`.

**`get_record_endorsements`** — replace chunked `IN (?,?,?)` with `IN :ids` using SQLAlchemy's `in_()` operator on the column, which handles chunking automatically:
```python
stmt = (
    select(
        record_endorsements.c.record_id,
        func.coalesce(canonical_le.c.name, le.c.name).label("display_name"),
    )
    .select_from(record_endorsements)
    .join(le, le.c.id == record_endorsements.c.endorsement_id)
    .outerjoin(ea, ea.c.endorsement_id == le.c.id)
    .outerjoin(canonical_le, canonical_le.c.id == ea.c.canonical_endorsement_id)
    .where(record_endorsements.c.record_id.in_(batch))
    .order_by(record_endorsements.c.record_id, text("display_name"))
)
```
Use aliased table objects for the self-join on `license_endorsements`.

**`get_endorsement_groups`** — pure Python aggregation; the three queries (endorsements + aliases + codes) translate directly with `.mappings().all()`.

- [ ] Run `uv run pytest tests/test_pg_endorsements.py -v` — all green
- [ ] Run `uv run pytest tests/ -v` — full suite green

### Step 3c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_endorsements.py tests/test_pg_endorsements.py
git commit -m "#94 feat: add pg_endorsements — async endorsement pipeline"
```

---

## Task 4 — pg_endorsements_seed.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_endorsements_seed.py`
- Test: `tests/test_pg_endorsements_seed.py`

### Step 4a — Write failing test

```python
# tests/test_pg_endorsements_seed.py
"""Tests for pg_endorsements_seed.py — async endorsement seeding."""
import pytest
from wslcb_licensing_tracker.pg_endorsements_seed import (
    seed_endorsements,
    merge_mixed_case_endorsements,
)


class TestSeedEndorsements:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_nonnegative_int(self, pg_conn):
        inserted = await seed_endorsements(pg_conn)
        assert isinstance(inserted, int)
        assert inserted >= 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn):
        first = await seed_endorsements(pg_conn)
        second = await seed_endorsements(pg_conn)
        # Second run should insert 0 (all already exist)
        assert second == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_merge_mixed_case(self, pg_conn):
        # Seed creates uppercase endorsements; merge_mixed_case should return int
        count = await merge_mixed_case_endorsements(pg_conn)
        assert isinstance(count, int)
```

- [ ] Run `uv run pytest tests/test_pg_endorsements_seed.py -v` — confirm `ImportError`

### Step 4b — Implement

Key notes:
- Import `SEED_CODE_MAP` from `.endorsements_seed` (static data, no DB, safe cross-import).
- Import `ensure_endorsement`, `merge_endorsement` from `.pg_endorsements`.
- `seed_endorsements`: iterate `SEED_CODE_MAP`, call `ensure_endorsement`, then `pg_insert(endorsement_codes).values(...).on_conflict_do_nothing()`. Track `rowcount` via `result.rowcount`.
- `_merge_seeded_placeholders`: SELECT endorsements whose `name` matches a code string via `col.op('~')(r'^\d+$')`:
  ```python
  stmt = (
      select(license_endorsements.c.id, license_endorsements.c.name)
      .where(license_endorsements.c.name.op('~')(r'^\d+$'))
  )
  ```
  Then for each, check if a real endorsement exists for that code and merge.
- `merge_mixed_case_endorsements`: find pairs where `upper(le.name) = upper(le2.name)` and `le.id != le2.id`. Keep the uppercase one as canonical. Use `text()` SQL for the self-join detection query.
- `repair_code_name_endorsements`: find endorsements whose name matches `CODE_NAME_RE` pattern. Use `le.name.op('~')(r'^\d+,\s+.+$')` to detect them.
- `backfill`: SELECT records with no `record_endorsements` rows and non-empty `license_type`. Use LEFT JOIN / NOT EXISTS pattern.
- `discover_code_mappings`: cross-reference license numbers between new_application (text names) and approved (numeric codes). Pure query, returns dict.

- [ ] Run `uv run pytest tests/test_pg_endorsements_seed.py -v` — all green

### Step 4c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_endorsements_seed.py tests/test_pg_endorsements_seed.py
git commit -m "#94 feat: add pg_endorsements_seed — async endorsement seeding and repair"
```

---

## Task 5 — pg_endorsements_admin.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_endorsements_admin.py`
- Test: `tests/test_pg_endorsements_admin.py`

### Step 5a — Write failing test

```python
# tests/test_pg_endorsements_admin.py
"""Tests for pg_endorsements_admin.py — async endorsement admin helpers."""
import pytest
from wslcb_licensing_tracker.pg_endorsements_admin import (
    endorsement_similarity,
    get_endorsement_list,
    get_code_mappings,
    add_code_mapping,
    remove_code_mapping,
    create_code,
    suggest_duplicate_endorsements,
    dismiss_suggestion,
)
from wslcb_licensing_tracker.pg_endorsements import ensure_endorsement


class TestEndorsementSimilarity:
    def test_identical_names(self):
        assert endorsement_similarity("CANNABIS RETAILER", "CANNABIS RETAILER") == 1.0

    def test_different_names(self):
        assert endorsement_similarity("SPIRITS RETAILER", "BEER DISTRIBUTOR") < 0.5


class TestGetEndorsementList:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_list(self, pg_conn):
        await ensure_endorsement(pg_conn, "ADMIN TEST ENDORSEMENT")
        result = await get_endorsement_list(pg_conn)
        assert isinstance(result, list)
        assert len(result) > 0
        assert "id" in result[0] and "name" in result[0]


class TestCodeMappingCrud:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_add_and_remove_code_mapping(self, pg_conn):
        eid = await ensure_endorsement(pg_conn, "CODE MAPPING TEST")
        await add_code_mapping(pg_conn, "9999", eid)
        mappings = await get_code_mappings(pg_conn)
        codes = [m["code"] for m in mappings]
        assert "9999" in codes
        await remove_code_mapping(pg_conn, "9999")
        mappings2 = await get_code_mappings(pg_conn)
        assert "9999" not in [m["code"] for m in mappings2]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_code(self, pg_conn):
        await create_code(pg_conn, "9998", "CREATED CODE TEST")
        mappings = await get_code_mappings(pg_conn)
        assert "9998" in [m["code"] for m in mappings]
```

- [ ] Run `uv run pytest tests/test_pg_endorsements_admin.py -v` — confirm `ImportError`

### Step 5b — Implement

Key notes:
- `endorsement_similarity` — pure Python, copy verbatim from `endorsements_admin.py`. No `async`.
- `get_endorsement_list` — `SELECT le.id, le.name, string_agg(ec.code, '; ' ORDER BY ec.code) AS codes FROM license_endorsements le LEFT JOIN endorsement_codes ec ON ec.endorsement_id = le.id GROUP BY le.id ORDER BY le.name`. Use `func.string_agg`.
- `suggest_duplicate_endorsements` — pure Python after fetching all endorsements; compute similarity matrix in-memory (same as SQLite version).
- `dismiss_suggestion` — swap `id_a, id_b` if `id_a > id_b`:
  ```python
  if id_a > id_b:
      id_a, id_b = id_b, id_a
  ```
  Then `pg_insert(endorsement_dismissed_suggestions).values(...).on_conflict_do_nothing()`.
- `get_code_mappings` — JOIN `endorsement_codes` with `license_endorsements`.
- `add_code_mapping` — `pg_insert(endorsement_codes).values(code=code, endorsement_id=endorsement_id).on_conflict_do_nothing()`.
- `remove_code_mapping` — `delete(endorsement_codes).where(endorsement_codes.c.code == code)`.
- `create_code` — call `ensure_endorsement` for the name, then `add_code_mapping`.

- [ ] Run `uv run pytest tests/test_pg_endorsements_admin.py -v` — all green

### Step 5c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_endorsements_admin.py tests/test_pg_endorsements_admin.py
git commit -m "#94 feat: add pg_endorsements_admin — async endorsement admin helpers"
```

---

## Task 6 — pg_entities.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_entities.py`
- Test: `tests/test_pg_entities.py`

### Step 6a — Write failing test

```python
# tests/test_pg_entities.py
"""Tests for pg_entities.py — async entity normalization."""
import pytest
from wslcb_licensing_tracker.pg_entities import (
    get_or_create_entity,
    parse_and_link_entities,
    get_record_entities,
    merge_duplicate_entities,
    reprocess_entities,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestGetOrCreateEntity:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_and_returns_id(self, pg_conn):
        eid = await get_or_create_entity(pg_conn, "john doe")
        assert isinstance(eid, int)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn):
        eid1 = await get_or_create_entity(pg_conn, "JANE SMITH")
        eid2 = await get_or_create_entity(pg_conn, "jane smith")
        assert eid1 == eid2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_raises_on_empty_name(self, pg_conn):
        with pytest.raises(ValueError):
            await get_or_create_entity(pg_conn, "")


class TestParseAndLinkEntities:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_links_applicants(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_001"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        count = await parse_and_link_entities(
            pg_conn, record_id,
            "ACME CANNABIS CO; JOHN DOE; JANE SMITH",
            role="applicant",
        )
        # Skips first element (business name), links JOHN DOE + JANE SMITH
        assert count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_record_entities(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_002"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await parse_and_link_entities(
            pg_conn, record_id,
            "ACME CANNABIS CO; JOHN DOE",
            role="applicant",
        )
        entity_map = await get_record_entities(pg_conn, [record_id])
        assert record_id in entity_map
        applicants = entity_map[record_id].get("applicant", [])
        assert any(e["name"] == "JOHN DOE" for e in applicants)
```

- [ ] Run `uv run pytest tests/test_pg_entities.py -v` — confirm `ImportError`

### Step 6b — Implement

Key notes:
- `_ENTITY_REPROCESS_VERSION = 2` — same constant as SQLite module.
- `get_or_create_entity`: call `clean_entity_name` (imported from `.db`), then `pg_insert(entities).values(...).on_conflict_do_nothing(index_elements=["name"]).returning(entities.c.id)` + fallback SELECT.
- `parse_and_link_entities`: split on `";"`, skip first element, skip `ADDITIONAL_NAMES_MARKERS` entries. Use `pg_insert(record_entities).values(...).on_conflict_do_nothing()`.
- `get_record_entities`: batch fetch with `record_entities.c.record_id.in_(record_ids)`. Group by `(record_id, role)` in Python.
- `merge_duplicate_entities`: detect duplicates via `lower(name)` match. Use `text()` SQL for the self-join.
- `reprocess_entities`: SELECT records with `record_enrichments` version `< _ENTITY_REPROCESS_VERSION` or no enrichment row. Use `text()` or SQLAlchemy expressions with a LEFT JOIN on `record_enrichments`.

- [ ] Run `uv run pytest tests/test_pg_entities.py -v` — all green

### Step 6c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_entities.py tests/test_pg_entities.py
git commit -m "#94 feat: add pg_entities — async entity normalization"
```

---

## Task 7 — pg_address_validator.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_address_validator.py`
- Test: `tests/test_pg_address_validator.py`

### Step 7a — Write failing test

```python
# tests/test_pg_address_validator.py
"""Tests for pg_address_validator.py — async address validation DB layer."""
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import select
from wslcb_licensing_tracker.models import locations
from wslcb_licensing_tracker.pg_address_validator import standardize_location
from wslcb_licensing_tracker.pg_db import get_or_create_location


class TestStandardizeLocation:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_updates_std_columns_on_success(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "123 MAIN ST, SEATTLE, WA 98101")
        mock_result = {
            "std_address_line_1": "123 MAIN ST",
            "std_address_line_2": "",
            "std_city": "SEATTLE",
            "std_region": "WA",
            "std_postal_code": "98101",
            "std_country": "US",
        }
        with patch(
            "wslcb_licensing_tracker.pg_address_validator.standardize",
            return_value=mock_result,
        ):
            result = await standardize_location(pg_conn, loc_id, "123 MAIN ST, SEATTLE, WA 98101")
        assert result is True
        row = (await pg_conn.execute(
            select(locations.c.std_city).where(locations.c.id == loc_id)
        )).scalar_one()
        assert row == "SEATTLE"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_api_error(self, pg_conn):
        loc_id = await get_or_create_location(pg_conn, "BAD ADDRESS ONLY")
        with patch(
            "wslcb_licensing_tracker.pg_address_validator.standardize",
            side_effect=Exception("API error"),
        ):
            result = await standardize_location(pg_conn, loc_id, "BAD ADDRESS ONLY")
        assert result is False
```

- [ ] Run `uv run pytest tests/test_pg_address_validator.py -v` — confirm `ImportError`

### Step 7b — Implement

Key notes:
- Copy `_load_api_key`, `_env_candidates`, `_cached_api_key`, `standardize`, `validate` (all pure HTTP/file functions) verbatim from `address_validator.py`. These are not async.
- For DB operations, wrap the synchronous HTTP call in `asyncio.to_thread`:
  ```python
  import asyncio
  result = await asyncio.to_thread(standardize, raw_address, client)
  ```
- `standardize_location`: fetch location row, call `asyncio.to_thread(standardize, ...)`, then `await conn.execute(update(locations).where(...).values(...))`.
- `validate_location`: same pattern with `validate`.
- `validate_record` / `validate_previous_location`: fetch `location_id` from `license_records`, then call `standardize_location` / `validate_location`.
- `backfill_addresses` / `refresh_addresses` / `refresh_specific_addresses`: SELECT unprocessed locations, iterate, call `standardize_location` per row with optional rate-limiting via `asyncio.sleep`.
- Check `ENABLE_ADDRESS_VALIDATION` env var (same as SQLite version) before calling `validate_location`.

```python
# Pattern for all DB UPDATE writes:
from sqlalchemy import update

await conn.execute(
    update(locations)
    .where(locations.c.id == location_id)
    .values(
        std_address_line_1=result.get("std_address_line_1", ""),
        # ... other std_ fields ...
        address_standardized_at=datetime.now(UTC).isoformat(),
    )
)
```

- [ ] Run `uv run pytest tests/test_pg_address_validator.py -v` — all green

### Step 7c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_address_validator.py tests/test_pg_address_validator.py
git commit -m "#94 feat: add pg_address_validator — async address validation DB layer"
```

---

## Task 8 — pg_link_records.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_link_records.py`
- Test: `tests/test_pg_link_records.py`

### Step 8a — Write failing test

```python
# tests/test_pg_link_records.py
"""Tests for pg_link_records.py — async application-outcome linking."""
import pytest
from wslcb_licensing_tracker.pg_link_records import (
    build_all_links,
    link_new_record,
    get_outcome_status,
    get_reverse_link_info,
    outcome_filter_sql,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestGetOutcomeStatus:
    """Pure Python — no DB needed."""

    def test_non_new_application_returns_none_status(self):
        record = {"section_type": "approved", "application_type": "NEW APPLICATION"}
        assert get_outcome_status(record, None)["status"] is None

    def test_non_linkable_type(self):
        record = {"section_type": "new_application", "application_type": "EXTENSION"}
        assert get_outcome_status(record, None)["status"] is None


class TestOutcomeFilterSql:
    """Pure Python — no DB needed."""

    def test_approved_filter(self):
        frags = outcome_filter_sql("approved")
        assert len(frags) >= 1
        assert any("approved" in f for f in frags)

    def test_unknown_status_returns_empty(self):
        assert outcome_filter_sql("nonexistent") == []


class TestBuildAllLinks:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_summary_dict(self, pg_conn):
        result = await build_all_links(pg_conn)
        assert "high" in result and "medium" in result and "total" in result
        assert result["total"] == result["high"] + result["medium"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_links_matching_records(self, pg_conn, standard_new_application):
        # Insert a new_application
        standard_new_application["license_number"] = "link_001"
        standard_new_application["record_date"] = "2025-01-10"
        new_app = dict(standard_new_application)
        await insert_record(pg_conn, new_app)

        # Insert matching approved record
        approved = dict(standard_new_application)
        approved["section_type"] = "approved"
        approved["record_date"] = "2025-01-15"
        approved["license_number"] = "link_001"
        await insert_record(pg_conn, approved)

        result = await build_all_links(pg_conn)
        assert result["total"] >= 1
```

- [ ] Run `uv run pytest tests/test_pg_link_records.py -v` — confirm `ImportError`

### Step 8b — Implement

Key notes:
- `get_outcome_status` — pure Python, copy verbatim from `link_records.py`.
- `outcome_filter_sql` — pure Python, copy verbatim. The returned SQL fragments are used in `pg_queries.py` as raw SQL strings inside `text()` clauses.
- `build_all_links`: replace `DELETE FROM record_links` with `text("TRUNCATE record_links")` for performance (or `delete(record_links)`).
- `_link_section`: translate the correlated subquery from SQLite to PostgreSQL. The key date arithmetic:
  ```python
  # SQLite: date(na.record_date, '-7 days')
  # PostgreSQL: na.record_date::date - interval '7 days'
  # In text() SQL:
  "record_date::date - interval '7 days'"
  ```
  Use `text()` for the full nested subquery SQL since it has complex correlated subqueries that are harder to express in SQLAlchemy Core expressions. Include parameterized `:tolerance` binding.
- `_insert_link`: translate the UPDATE for `previous_location_id` — same logic, use `text()`.
- `link_new_record` / `_link_incremental`: same structure as SQLite; translate date arithmetic in the SELECT queries.
- `get_reverse_link_info`: straightforward JOIN query.
- `get_record_links_bulk` (used by `pg_queries.py`): batch fetch `record_links` with JOIN to get `outcome_section_type`, `outcome_date` etc. for a list of `new_app_id`s.

The `_link_section` forward/backward pass queries can be expressed as `text()`:
```python
forward_sql = text(f"""
    SELECT na.id AS new_app_id, (
        SELECT out.id FROM license_records out
        WHERE out.section_type = :out_section
          AND out.license_number = na.license_number
          AND {fwd_type_match}
          AND out.record_date::date >= na.record_date::date - interval '{DATE_TOLERANCE_DAYS} days'
        ORDER BY out.record_date ASC, out.id ASC
        LIMIT 1
    ) AS outcome_id
    FROM license_records na
    WHERE na.section_type = 'new_application'
      AND {na_where}
""")
```

- [ ] Run `uv run pytest tests/test_pg_link_records.py -v` — all green

### Step 8c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_link_records.py tests/test_pg_link_records.py
git commit -m "#94 feat: add pg_link_records — async application-outcome linking"
```

---

## Task 9 — pg_queries.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_queries.py`
- Test: `tests/test_pg_queries.py`

### Step 9a — Write failing test

```python
# tests/test_pg_queries.py
"""Tests for pg_queries.py — async search and read queries."""
import pytest
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
    get_entities,
    invalidate_filter_cache,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestEnrichRecord:
    """Pure Python — no DB."""
    def test_adds_display_fields(self):
        record = {"std_city": "SEATTLE", "city": "seattle",
                  "std_postal_code": "98101", "zip_code": "98101",
                  "prev_std_city": "", "previous_city": "TACOMA",
                  "prev_std_postal_code": "", "previous_zip_code": "98402"}
        enriched = enrich_record(record)
        assert enriched["display_city"] == "SEATTLE"
        assert enriched["display_previous_city"] == "TACOMA"


class TestSearchRecords:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_tuple_list_int(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_001"
        await insert_record(pg_conn, standard_new_application)
        records, total = await search_records(pg_conn)
        assert isinstance(records, list)
        assert isinstance(total, int)
        assert total >= 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_query_filter_ilike(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_002"
        standard_new_application["business_name"] = "UNIQUE ILIKE SEARCH CORP"
        await insert_record(pg_conn, standard_new_application)
        records, total = await search_records(pg_conn, query="UNIQUE ILIKE SEARCH")
        assert total >= 1
        assert any("UNIQUE ILIKE SEARCH" in r["business_name"] for r in records)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_section_type_filter(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_003"
        await insert_record(pg_conn, standard_new_application)
        records, total = await search_records(pg_conn, section_type="new_application")
        assert all(r["section_type"] == "new_application" for r in records)


class TestExportRecords:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_list_of_dicts(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_004"
        await insert_record(pg_conn, standard_new_application)
        results = await export_records(pg_conn)
        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)
        assert all("business_name" in r for r in results)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_export_cursor_is_async_generator(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_005"
        await insert_record(pg_conn, standard_new_application)
        rows = []
        async for row in export_records_cursor(pg_conn):
            rows.append(row)
        assert len(rows) >= 1


class TestGetStats:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_stats_dict(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_006"
        await insert_record(pg_conn, standard_new_application)
        stats = await get_stats(pg_conn)
        assert "total_records" in stats
        assert stats["total_records"] >= 1

class TestGetRecordById:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetches_existing(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_007"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        record = await get_record_by_id(pg_conn, record_id)
        assert record is not None
        assert record["id"] == record_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_for_missing(self, pg_conn):
        record = await get_record_by_id(pg_conn, 999999999)
        assert record is None
```

- [ ] Run `uv run pytest tests/test_pg_queries.py -v` — confirm `ImportError`

### Step 9b — Implement

Key implementation notes:

**Imports at top of `pg_queries.py`:**
```python
from .pg_endorsements import get_endorsement_options, get_record_endorsements
from .pg_entities import get_record_entities
from .pg_link_records import (
    DATA_GAP_CUTOFF, LINKABLE_TYPES, PENDING_CUTOFF_DAYS,
    get_outcome_status, get_record_links_bulk, outcome_filter_sql,
)
from .pg_substances import get_regulated_substances
from .db import US_STATES
from .display import format_outcome
```

**FTS5 → ILIKE translation for `_build_where_clause`:**
```python
if query:
    q_like = f"%{query}%"
    # TODO Phase 5: replace with tsvector @@ plainto_tsquery()
    conditions.append(
        "(lr.business_name ILIKE :q_like "
        "OR lr.applicants ILIKE :q_like "
        "OR lr.resolved_endorsements ILIKE :q_like)"
    )
    params["q_like"] = q_like
```

**`_build_where_clause`** — switch from positional `?` params to named `:name` params for SQLAlchemy `text()` compatibility. Build the final `text(where_sql)` with a `params` dict.

**`outcome_filter_sql` fragments** — the returned strings from `pg_link_records.outcome_filter_sql` contain SQLite-style `date('now', ...)` calls. These need PG-compatible equivalents. Override in `pg_queries._build_where_clause`:
```python
if outcome_status:
    # outcome_filter_sql returns SQLite SQL fragments; rewrite date() calls for PG
    frags = outcome_filter_sql(outcome_status, record_alias="lr")
    for frag in frags:
        frag_pg = frag.replace(
            f"date('now', '-{PENDING_CUTOFF_DAYS} days')",
            f"CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'",
        )
        conditions.append(frag_pg)
```

**`RECORD_COLUMNS` and `RECORD_JOINS`** — copy verbatim from `queries.py` (uses standard SQL compatible with PostgreSQL).

**`export_records_cursor`** — return `AsyncGenerator[dict, None]`:
```python
async def export_records_cursor(conn: AsyncConnection, ...) -> AsyncGenerator[dict, None]:
    where_sql, params = _build_where_clause(...)
    result = await conn.execute(text(f"{_EXPORT_SELECT} {where_sql} ORDER BY ..."), params)
    async for row in result:  # SQLAlchemy async cursor supports async iteration
        yield dict(row._mapping)
```

**`_EXPORT_SELECT`** — translate the inline SQL:
- `GROUP_CONCAT(display_name, '; ')` → `STRING_AGG(display_name, '; ' ORDER BY display_name)`
- `date('now', '-N days')` → `CURRENT_DATE - interval 'N days'`
- `ORDER BY rl.confidence = 'high' DESC` → `ORDER BY (rl.confidence = 'high') DESC` (PostgreSQL supports boolean expressions in ORDER BY)
- Mark the whole block with `# TODO Phase 5: replace endorsement subquery with tsvector search`

**`get_stats`** — the `_get_pipeline_stats` CTE query needs `date('now', ...)` replaced with `CURRENT_DATE - interval '...'`.

**`get_stats` last_scrape** — `SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1` translates directly; use `text()`.

**Caching** — keep `_filter_cache`, `_city_cache`, `_stats_cache` dicts with `time.monotonic()` pattern identical to SQLite version. `invalidate_filter_cache` clears all three.

**`get_entities`** — translate `LIKE ? COLLATE NOCASE` → `ILIKE :q_like`. The `ORDER BY record_count DESC` subquery wrapper translates directly.

**`get_entity_records`** — JOIN `record_entities` to `license_records` to get all records for an entity.

- [ ] Run `uv run pytest tests/test_pg_queries.py -v` — all green
- [ ] Run `uv run pytest tests/ -v` — full suite green

### Step 9c — Commit

```bash
git add src/wslcb_licensing_tracker/pg_queries.py tests/test_pg_queries.py
git commit -m "#94 feat: add pg_queries — async search and query layer"
```

---

## Task 10 — AGENTS.md Key Files update

**Files:**
- Modify: `AGENTS.md`

### Step 10a — Add Phase 3 entries

Add the following rows to the Key Files table in `AGENTS.md`, after the existing Phase 2 entries (`pg_db.py`, `pg_pipeline.py`, `pg_schema.py`):

| File | Purpose / Non-obvious notes |
|---|---|
| `pg_admin_audit.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `admin_audit.py`. `log_action()` uses `pg_insert(...).returning(id)`. `get_audit_log()` uses named `text()` params; `admin_email` filter uses `lower()` instead of `COLLATE NOCASE`. |
| `pg_substances.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `substances.py`. `get_regulated_substances()` runs two queries (substances + per-substance endorsements). `remove_substance()` manually deletes junction rows before the parent (no CASCADE assumption). |
| `pg_endorsements.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `endorsements.py`. `ensure_endorsement()` uses ON CONFLICT DO NOTHING + RETURNING with fallback SELECT. `_sync_resolved_endorsements()` uses a single `text()` UPDATE with `STRING_AGG(...ORDER BY)`. Alias self-join uses aliased table objects. Numeric code detection uses `.isdigit()` on the stripped Python value (no SQL GLOB needed). |
| `pg_endorsements_seed.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `endorsements_seed.py`. Imports static `SEED_CODE_MAP` from `endorsements_seed` (no DB at import time). Placeholder detection uses `col.op('~')(r'^\d+$')` instead of `GLOB '[0-9]*'`. |
| `pg_endorsements_admin.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `endorsements_admin.py`. `endorsement_similarity()` is pure Python — copied verbatim, not async. `dismiss_suggestion()` swaps `id_a/id_b` if `id_a > id_b` to enforce the `id_a < id_b` constraint. `get_endorsement_list()` uses `STRING_AGG` for code aggregation. |
| `pg_entities.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `entities.py`. `get_or_create_entity()` uses ON CONFLICT DO NOTHING + RETURNING + fallback SELECT. `_ENTITY_REPROCESS_VERSION = 2` constant preserved. |
| `pg_address_validator.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `address_validator.py`. Pure HTTP functions (`_load_api_key`, `standardize`, `validate`) copied verbatim. HTTP calls wrapped in `asyncio.to_thread()`. DB writes use `update(locations).where(...).values(...)`. |
| `pg_link_records.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `link_records.py`. `get_outcome_status()` and `outcome_filter_sql()` are pure Python — copied verbatim. Bidirectional linking queries use `text()` with PG date arithmetic (`record_date::date - interval 'N days'`). `build_all_links()` uses `TRUNCATE record_links` (or `delete(record_links)`). |
| `pg_queries.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `queries.py`. FTS5 replaced by ILIKE on `business_name`, `applicants`, `resolved_endorsements` — marked `# TODO Phase 5: tsvector`. `export_records_cursor()` is `AsyncGenerator[dict, None]`. `outcome_filter_sql` fragments have `date('now', ...)` rewritten to `CURRENT_DATE - interval '...'`. In-memory TTL caches (`_filter_cache`, `_city_cache`, `_stats_cache`) preserved unchanged. |

- [ ] Add entries to `AGENTS.md` Key Files table
- [ ] Run `uv run pytest tests/ -v` — full suite green

### Step 10b — Commit

```bash
git add AGENTS.md
git commit -m "#94 docs: update AGENTS.md with Phase 3 pg_* module entries"
git push
```

---

## Phase 3 completion checklist

- [ ] Task 1: `pg_admin_audit.py` — committed
- [ ] Task 2: `pg_substances.py` — committed
- [ ] Task 3: `pg_endorsements.py` — committed (most complex)
- [ ] Task 4: `pg_endorsements_seed.py` — committed
- [ ] Task 5: `pg_endorsements_admin.py` — committed
- [ ] Task 6: `pg_entities.py` — committed
- [ ] Task 7: `pg_address_validator.py` — committed
- [ ] Task 8: `pg_link_records.py` — committed
- [ ] Task 9: `pg_queries.py` — committed (imports all Phase 3 modules)
- [ ] Task 10: `AGENTS.md` updated — committed and pushed
- [ ] `uv run pytest tests/ -v` — full suite passes

**After Phase 3:** The `pg_pipeline.py` stubs for entity linking (`STEP_ENTITIES`), address validation (`STEP_ADDRESS`), and outcome linking (`STEP_OUTCOME_LINK`) can be wired up using the new pg_* modules. That wiring is a Phase 3.5 follow-up task or can be included in Phase 4 (route layer).
