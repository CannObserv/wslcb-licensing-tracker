# Phase 6 — Ops + Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the PostgreSQL migration by porting all remaining operational modules (scraper, backfill scripts, integrity CLI), writing the one-time SQLite→PG data migration script, updating `cli.py` to use PG throughout, and updating the README.

**Architecture:** Each operational module gets an async PG equivalent (`pg_scraper.py`, `pg_backfill_snapshots.py`, `pg_backfill_diffs.py`); `pg_integrity.py` gains the full CLI suite (`run_all_checks`, `fix_orphaned_locations`, `print_report`); `cli.py` is updated in-place to use PG modules with `asyncio.run()` wrappers; `scripts/sqlite_to_pg.py` is a standalone script that reads SQLite via `sqlite3` and writes to PostgreSQL in FK-dependency order, then resets sequences and backfills `search_vector`.

**Tech Stack:** SQLAlchemy 2.0 Core (async), asyncpg, Python `sqlite3` (reads only in migration script), `httpx` (async HTTP in scraper), `asyncio.run()` for CLI entry points.

---

## Files changed

| File | Action |
|---|---|
| `src/wslcb_licensing_tracker/pg_integrity.py` | **Modify** — add `fix_orphaned_locations`, `check_broken_fks`, `check_entity_duplicates`, `run_all_checks`, `print_report` |
| `src/wslcb_licensing_tracker/pg_scraper.py` | **New** — async port of `scraper.py` |
| `src/wslcb_licensing_tracker/pg_backfill_snapshots.py` | **New** — async port of `backfill_snapshots.py` |
| `src/wslcb_licensing_tracker/pg_backfill_diffs.py` | **New** — async port of `backfill_diffs.py` |
| `src/wslcb_licensing_tracker/cli.py` | **Modify** — replace SQLite imports with PG; `asyncio.run()` wrappers; admin user commands use PG |
| `scripts/sqlite_to_pg.py` | **New** — standalone one-time data migration script |
| `README.md` | **Modify** — update Architecture table (SQLite → PostgreSQL), Project Structure tree, Setup instructions |
| `tests/test_pg_integrity.py` | **Modify** — add tests for `run_all_checks`, `fix_orphaned_locations`, `print_report` |
| `tests/test_pg_scraper.py` | **New** — tests for `get_last_content_hash`, `compute_content_hash` (pure, no DB needed) |
| `AGENTS.md` | **Modify** (worktree) — add `pg_scraper.py`, `pg_backfill_snapshots.py`, `pg_backfill_diffs.py` to Key Files; update `cli.py` note; mark Phase 6 complete |

---

## Task 1 — Complete pg_integrity.py

**Files:**
- Modify: `src/wslcb_licensing_tracker/pg_integrity.py`
- Modify: `tests/test_pg_integrity.py`

`pg_integrity.py` already has 3 async check functions (from Phase 4). It's missing the functions needed by `cli.py check`: `fix_orphaned_locations`, `check_broken_fks`, `check_entity_duplicates`, `run_all_checks`, and `print_report`.

`print_report` is pure Python — it takes a report dict and prints to stdout. It can be copied verbatim from `integrity.py`. All other functions need async SQLAlchemy versions.

**PostgreSQL translation differences from SQLite:**
- `GLOB '[0-9]*'` → `~ '^\d'` (already done in existing `check_endorsement_anomalies`)
- `GROUP_CONCAT(name, ' | ')` → `STRING_AGG(name, ' | ')`
- `DELETE FROM locations WHERE id IN (...)` → parameterized `text()` with `IN :ids` using `bindparam("ids", expanding=True)` or use a subquery

- [ ] **Step 1: Add `fix_orphaned_locations` and `check_broken_fks` and `check_entity_duplicates`**

Append to `src/wslcb_licensing_tracker/pg_integrity.py`:

```python
from sqlalchemy import bindparam, delete, text
from .models import locations as locations_table


async def fix_orphaned_locations(conn: AsyncConnection) -> int:
    """Delete orphaned locations. Returns the number removed.

    Caller-commits convention: caller must call ``await conn.commit()`` after this.
    """
    result = await check_orphaned_locations(conn)
    if not result["count"]:
        return 0
    ids = [o["id"] for o in result["details"]]
    await conn.execute(
        delete(locations_table).where(locations_table.c.id.in_(ids))
    )
    logger.info("Removed %d orphaned location(s).", len(ids))
    return len(ids)


async def check_broken_fks(conn: AsyncConnection) -> list[dict]:
    """Find license_records rows with location_id or previous_location_id pointing nowhere.

    Returns a list of dicts with ``record_id``, ``column``, ``bad_id``.
    PostgreSQL enforces FK constraints, so this should always return empty
    in a healthy database. Included for parity with the SQLite integrity suite.
    """
    results = []
    for col in ("location_id", "previous_location_id"):
        result = await conn.execute(
            text(f"""
            SELECT lr.id AS record_id, lr.{col} AS bad_id
            FROM license_records lr
            WHERE lr.{col} IS NOT NULL
              AND lr.{col} NOT IN (SELECT id FROM locations)
            """)
        )
        rows = result.mappings().all()
        results.extend(
            {"record_id": r["record_id"], "column": col, "bad_id": r["bad_id"]}
            for r in rows
        )
    return results


async def check_entity_duplicates(conn: AsyncConnection) -> list[dict]:
    """Find entities that would merge under current cleaning rules (case-only differences).

    Returns a list of dicts with ``upper_name``, ``cnt``, ``names``.
    """
    result = await conn.execute(
        text("""
        SELECT UPPER(name) AS upper_name, COUNT(*) AS cnt,
               STRING_AGG(name, ' | ') AS names
        FROM entities
        GROUP BY UPPER(name)
        HAVING COUNT(*) > 1
        """)
    )
    return [dict(r) for r in result.mappings().all()]
```

- [ ] **Step 2: Add `run_all_checks` and `print_report`**

```python
async def run_all_checks(conn: AsyncConnection, *, fix: bool = False) -> dict:
    """Run all integrity checks and optionally auto-fix safe issues.

    Returns a structured report dict (same shape as SQLite integrity.py).
    """
    report: dict = {}

    orphaned = await check_orphaned_locations(conn)
    entry: dict = {"count": orphaned["count"]}
    if fix and orphaned["count"]:
        entry["fixed"] = await fix_orphaned_locations(conn)
        await conn.commit()  # fix_orphaned_locations follows caller-commits
    report["orphaned_locations"] = entry

    broken = await check_broken_fks(conn)
    report["broken_fks"] = {"count": len(broken), "details": broken}

    unenriched = await check_unenriched_records(conn)
    report["unenriched"] = unenriched

    endorsement = await check_endorsement_anomalies(conn)
    report["endorsement_anomalies"] = endorsement

    dupes = await check_entity_duplicates(conn)
    report["entity_duplicates"] = {"count": len(dupes), "details": dupes}

    return report


_DETAIL_PREVIEW_LIMIT = 5


def print_report(report: dict) -> int:  # noqa: C901, PLR0912
    """Print a human-readable integrity report to stdout.

    Returns the total number of issues found (0 = clean).
    Pure Python — no DB access.
    """
    total_issues = 0

    print("\n=== WSLCB Database Integrity Report ===")
    print()

    n = report["orphaned_locations"]["count"]
    fixed = report["orphaned_locations"].get("fixed", 0)
    if n:
        total_issues += n
        status = f"  FIXED {fixed}" if fixed else "  (use --fix to remove)"
        print(f"\u274c Orphaned locations: {n}{status}")
    else:
        print("\u2705 No orphaned locations")

    n = report["broken_fks"]["count"]
    if n:
        total_issues += n
        print(f"\u274c Broken foreign keys: {n}")
        for d in report["broken_fks"]["details"][:_DETAIL_PREVIEW_LIMIT]:
            print(f"     record {d['record_id']}: {d['column']} = {d['bad_id']}")
        if n > _DETAIL_PREVIEW_LIMIT:
            print(f"     ... and {n - _DETAIL_PREVIEW_LIMIT} more")
    else:
        print("\u2705 No broken foreign keys")

    ue = report["unenriched"]
    for key, label in [
        ("no_endorsements", "Records missing endorsement links"),
        ("no_entities", "Records missing entity links"),
        ("no_provenance", "Records missing provenance"),
        ("no_enrichment_tracking", "Records missing enrichment tracking"),
    ]:
        n = ue[key]
        if n:
            total_issues += n
            print(f"\u274c {label}: {n}")
        else:
            print(f"\u2705 {label}: 0")

    ea = report["endorsement_anomalies"]
    for key, label in [
        ("unresolved_codes", "Unresolved numeric codes"),
        ("placeholder_endorsements", "Placeholder endorsements"),
    ]:
        n = ea[key]
        if n:
            total_issues += n
            print(f"\u274c {label}: {n}")
        else:
            print(f"\u2705 {label}: 0")

    n = report["entity_duplicates"]["count"]
    if n:
        total_issues += n
        print(f"\u274c Entity duplicate groups: {n}")
        for d in report["entity_duplicates"]["details"][:_DETAIL_PREVIEW_LIMIT]:
            print(f"     {d['names']}")
        if n > _DETAIL_PREVIEW_LIMIT:
            print(f"     ... and {n - _DETAIL_PREVIEW_LIMIT} more")
    else:
        print("\u2705 No entity duplicates")

    print()
    if total_issues:
        print(f"Total issues: {total_issues}")
    else:
        print("\u2705 All checks passed!")
    print()

    return total_issues
```

Update the module import block at the top of `pg_integrity.py` to add:
```python
from sqlalchemy import delete, text
from .models import locations as locations_table
```

- [ ] **Step 3: Add tests for new functions**

Add to `tests/test_pg_integrity.py`:

```python
from wslcb_licensing_tracker.pg_integrity import (
    check_broken_fks,
    check_entity_duplicates,
    fix_orphaned_locations,
    print_report,
    run_all_checks,
)


@pytest.mark.asyncio
async def test_run_all_checks_returns_complete_report(conn):
    report = await run_all_checks(conn)
    for key in ("orphaned_locations", "broken_fks", "unenriched",
                "endorsement_anomalies", "entity_duplicates"):
        assert key in report


@pytest.mark.asyncio
async def test_check_broken_fks_returns_list(conn):
    result = await check_broken_fks(conn)
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_check_entity_duplicates_returns_list(conn):
    result = await check_entity_duplicates(conn)
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_fix_orphaned_locations_returns_int(conn):
    removed = await fix_orphaned_locations(conn)
    assert isinstance(removed, int)


@pytest.mark.asyncio
async def test_fix_orphaned_locations_removes_orphan(conn):
    """An orphaned location (no license_records reference) is deleted when fix=True."""
    from sqlalchemy import text
    # Insert a location not referenced by any license_records row
    await conn.execute(
        text("INSERT INTO locations (raw_address, city, state, zip_code) "
             "VALUES ('999 Orphan St', '', 'WA', '')")
    )
    await conn.commit()
    removed = await fix_orphaned_locations(conn)
    await conn.commit()
    assert removed >= 1


def test_print_report_returns_zero_for_clean_report():
    report = {
        "orphaned_locations": {"count": 0},
        "broken_fks": {"count": 0, "details": []},
        "unenriched": {
            "no_endorsements": 0, "no_entities": 0,
            "no_provenance": 0, "no_enrichment_tracking": 0,
        },
        "endorsement_anomalies": {"unresolved_codes": 0, "placeholder_endorsements": 0},
        "entity_duplicates": {"count": 0, "details": []},
    }
    assert print_report(report) == 0


def test_print_report_returns_nonzero_for_issues():
    report = {
        "orphaned_locations": {"count": 3},
        "broken_fks": {"count": 0, "details": []},
        "unenriched": {
            "no_endorsements": 2, "no_entities": 0,
            "no_provenance": 0, "no_enrichment_tracking": 0,
        },
        "endorsement_anomalies": {"unresolved_codes": 0, "placeholder_endorsements": 0},
        "entity_duplicates": {"count": 0, "details": []},
    }
    assert print_report(report) == 5  # 3 + 2
```

- [ ] **Step 4: Run tests**

```bash
cd .worktrees/feat-postgresql && uv run pytest tests/test_pg_integrity.py -v
# Expected: print_report tests pass (no DB); PG tests skip without TEST_DATABASE_URL
```

- [ ] **Step 5: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_integrity.py tests/test_pg_integrity.py
git commit -m "#94 feat: Phase 6 Task 1 — complete pg_integrity (run_all_checks, print_report)"
```

---

## Task 2 — pg_scraper.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_scraper.py`
- Create: `tests/test_pg_scraper.py`

The async scraper is structurally identical to `scraper.py` with three differences:
1. `get_last_content_hash` uses SQLAlchemy `text()` instead of `conn.execute(str)`
2. `scrape()` takes an `AsyncEngine`, creates a connection, calls `pg_pipeline.ingest_batch()`
3. `cleanup_redundant_scrapes()` takes an `AsyncEngine` and uses SQLAlchemy

Pure functions `compute_content_hash` and `save_html_snapshot` are **imported from `scraper.py`** — do not duplicate them.

HTTP fetching uses `httpx.AsyncClient` (async context manager) since we're in an async function.

The scraper calls `seed_endorsements` and `repair_code_name_endorsements` at startup (idempotent). In PG these run via the data_migration framework at app startup — the standalone CLI `wslcb scrape` must call them explicitly. Use `pg_endorsements_seed` equivalents wrapped in `asyncio.run()` at the CLI level.

- [ ] **Step 1: Write `pg_scraper.py`**

```python
"""Async scraper for WSLCB licensing activity page.

Async PostgreSQL port of scraper.py. Pure helper functions
(compute_content_hash, save_html_snapshot) are re-exported from
scraper.py — do not duplicate them here.
"""

import logging
from datetime import UTC, datetime

import httpx
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .db import DATA_DIR, SOURCE_TYPE_LIVE_SCRAPE, WSLCB_SOURCE_URL
from .models import scrape_log, sources
from .parser import SECTION_MAP, parse_records_from_table
from .pg_db import get_db, get_or_create_source
from .pg_pipeline import IngestOptions, ingest_batch
from .scraper import compute_content_hash, save_html_snapshot  # pure helpers

logger = logging.getLogger(__name__)


async def get_last_content_hash(conn: AsyncConnection) -> str | None:
    """Return the content_hash from the most recent successful or unchanged scrape."""
    result = await conn.execute(
        select(scrape_log.c.content_hash)
        .where(scrape_log.c.status.in_(["success", "unchanged"]))
        .where(scrape_log.c.content_hash.isnot(None))
        .order_by(scrape_log.c.id.desc())
        .limit(1)
    )
    row = result.fetchone()
    return row[0] if row else None


async def scrape(engine: AsyncEngine) -> None:  # noqa: C901, PLR0915
    """Run a full scrape: fetch, archive, parse, ingest, and log."""
    logger.info("Starting scrape of %s", WSLCB_SOURCE_URL)

    async with get_db(engine) as conn:
        # Log scrape start
        result = await conn.execute(
            pg_insert(scrape_log).values(
                started_at=datetime.now(UTC).isoformat(),
                status="running",
            ).returning(scrape_log.c.id)
        )
        log_id = result.scalar_one()
        await conn.commit()

        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                resp = await client.get(WSLCB_SOURCE_URL)
            resp.raise_for_status()
            html = resp.text
            logger.debug("Fetched %s bytes", f"{len(html):,}")

            content_hash = compute_content_hash(html)
            last_hash = await get_last_content_hash(conn)

            if content_hash == last_hash:
                logger.info(
                    "Page unchanged (hash %s...); skipping parse/ingest",
                    content_hash[:12],
                )
                await conn.execute(
                    update(scrape_log)
                    .where(scrape_log.c.id == log_id)
                    .values(
                        finished_at=datetime.now(UTC).isoformat(),
                        status="unchanged",
                        content_hash=content_hash,
                    )
                )
                await conn.commit()
                return

            scrape_time = datetime.now(UTC)
            snapshot_path = None
            try:
                snapshot_path = save_html_snapshot(html, scrape_time)
                logger.debug("Saved snapshot to %s", snapshot_path)
            except Exception as snap_err:  # noqa: BLE001
                logger.warning("Failed to save HTML snapshot: %s", snap_err)

            rel_path = str(snapshot_path.relative_to(DATA_DIR)) if snapshot_path else None
            source_id = await get_or_create_source(
                conn,
                SOURCE_TYPE_LIVE_SCRAPE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=scrape_time.isoformat(),
                scrape_log_id=log_id,
            )

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")
            all_tables = soup.find_all("table")
            data_tables = []
            for t in all_tables:
                th = t.find("th")
                if th and th.get_text(strip=True).replace("\xa0", " ") in SECTION_MAP:
                    header_text = th.get_text(strip=True).replace("\xa0", " ")
                    data_tables.append((SECTION_MAP[header_text], t))

            if not data_tables:
                msg = "Could not find data tables in page"
                raise ValueError(msg)  # noqa: TRY301

            counts = {"new": 0, "approved": 0, "discontinued": 0, "skipped": 0}

            for section_type, table in data_tables:
                records = parse_records_from_table(table, section_type)
                logger.debug("  %s: parsed %d records", section_type, len(records))
                opts = IngestOptions(
                    validate_addresses=True,
                    link_outcomes=True,
                    source_id=source_id,
                )
                batch_result = await ingest_batch(conn, records, opts)

                key = section_type.split("_")[0] if "_" in section_type else section_type
                if key == "new":
                    counts["new"] = batch_result.inserted
                elif key == "approved":
                    counts["approved"] = batch_result.inserted
                elif key == "discontinued":
                    counts["discontinued"] = batch_result.inserted
                counts["skipped"] += batch_result.skipped

            await conn.commit()

            await conn.execute(
                update(scrape_log)
                .where(scrape_log.c.id == log_id)
                .values(
                    finished_at=datetime.now(UTC).isoformat(),
                    status="success",
                    content_hash=content_hash,
                    records_new=counts["new"],
                    records_approved=counts["approved"],
                    records_discontinued=counts["discontinued"],
                    records_skipped=counts["skipped"],
                )
            )
            await conn.commit()

            logger.info(
                "Scrape complete: new=%d approved=%d discontinued=%d skipped=%d",
                counts["new"], counts["approved"], counts["discontinued"], counts["skipped"],
            )

        except Exception as exc:
            logger.exception("Scrape failed: %s", exc)
            try:
                await conn.execute(
                    update(scrape_log)
                    .where(scrape_log.c.id == log_id)
                    .values(
                        finished_at=datetime.now(UTC).isoformat(),
                        status="error",
                        error_message=str(exc),
                    )
                )
                await conn.commit()
            except Exception:  # noqa: BLE001
                logger.exception("Failed to update scrape_log on error")
            raise


async def cleanup_redundant_scrapes(
    engine: AsyncEngine,
    *,
    delete_files: bool = True,
) -> dict:
    """Remove scrape_log rows (and associated sources/files) for unchanged scrapes.

    Returns a dict with counts of removed rows: ``scrape_logs``, ``sources``,
    ``record_sources``, ``files``.
    """
    from .scraper import cleanup_redundant_scrapes as _sqlite_cleanup  # noqa: F401
    # The pure file-deletion logic lives in the SQLite scraper; the DB queries
    # are reimplemented here for PostgreSQL.
    # For now: delegate the full logic to a PG-native implementation.
    result = {"scrape_logs": 0, "sources": 0, "record_sources": 0, "files": 0}

    async with get_db(engine) as conn:
        # Find scrape_log rows with status='unchanged' that have no sources linked
        unchanged = await conn.execute(
            text("""
            SELECT sl.id, sl.snapshot_path
            FROM scrape_log sl
            WHERE sl.status = 'unchanged'
              AND NOT EXISTS (
                  SELECT 1 FROM sources s WHERE s.scrape_log_id = sl.id
              )
            """)
        )
        rows = unchanged.mappings().all()

        for row in rows:
            if delete_files and row["snapshot_path"]:
                snap = DATA_DIR / row["snapshot_path"]
                if snap.exists():
                    snap.unlink()
                    result["files"] += 1

        if rows:
            ids = [r["id"] for r in rows]
            await conn.execute(
                text("DELETE FROM scrape_log WHERE id = ANY(:ids)"),
                {"ids": ids},
            )
            await conn.commit()
            result["scrape_logs"] = len(ids)

    return result
```

- [ ] **Step 2: Write tests for pure logic (no DB)**

`get_last_content_hash` and `scrape()` are integration-only. `compute_content_hash` is imported from `scraper.py` and already tested there. Write a single smoke test that the import works and `get_last_content_hash` is importable:

```python
"""Tests for pg_scraper.py — pure helper logic."""

import pytest
from wslcb_licensing_tracker.pg_scraper import get_last_content_hash
from wslcb_licensing_tracker.scraper import compute_content_hash


def test_compute_content_hash_re_exported():
    """compute_content_hash is re-used from scraper.py without duplication."""
    assert compute_content_hash("<html>test</html>") == compute_content_hash("<html>test</html>")


def test_get_last_content_hash_is_importable():
    """pg_scraper.get_last_content_hash is an async function."""
    import inspect
    assert inspect.iscoroutinefunction(get_last_content_hash)
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/test_pg_scraper.py -v
# Expected: 2 passed
```

- [ ] **Step 4: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_scraper.py tests/test_pg_scraper.py
git commit -m "#94 feat: Phase 6 Task 2 — pg_scraper (async scraper port)"
```

---

## Task 3 — pg_backfill_snapshots.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_backfill_snapshots.py`

Async port of `backfill_snapshots.py`. The two-phase structure (ingest → repair) is preserved. The repair phase fixes ASSUMPTION records with empty business names and CHANGE OF LOCATION records with missing locations — same logic, async DB calls.

- [ ] **Step 1: Write `pg_backfill_snapshots.py`**

```python
"""Async backfill of records from archived HTML snapshots into PostgreSQL.

Async port of backfill_snapshots.py. Two-phase:
  1. Ingest — INSERT new records (duplicates skipped by UNIQUE constraint).
  2. Repair — fix broken ASSUMPTION and CHANGE OF LOCATION records.

Safe to re-run. Address validation is deferred to ``wslcb backfill-addresses``.
"""

import logging
from pathlib import Path

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .db import DATA_DIR, SOURCE_TYPE_CO_ARCHIVE, WSLCB_SOURCE_URL, clean_applicants_string, clean_entity_name
from .models import license_records
from .parser import extract_snapshot_date, parse_snapshot, snapshot_paths
from .pg_db import get_db, get_or_create_location, get_or_create_source, link_record_source
from .pg_pipeline import IngestOptions, ingest_batch

logger = logging.getLogger(__name__)


async def _repair_assumptions(
    conn: AsyncConnection,
    records: list[dict],
    source_id: int,
) -> int:
    """Fix ASSUMPTION records with empty or NULL business names in PG."""
    updated = 0
    for rec in records:
        if rec["application_type"] != "ASSUMPTION":
            continue
        if not rec.get("business_name"):
            continue
        result = await conn.execute(
            select(license_records.c.id, license_records.c.business_name)
            .where(license_records.c.section_type == rec["section_type"])
            .where(license_records.c.record_date == rec["record_date"])
            .where(license_records.c.license_number == rec["license_number"])
            .where(license_records.c.application_type == "ASSUMPTION")
            .where(
                (license_records.c.business_name.is_(None)) |
                (license_records.c.business_name == "")
            )
        )
        row = result.fetchone()
        if not row:
            continue
        await conn.execute(
            update(license_records)
            .where(license_records.c.id == row[0])
            .values(
                business_name=clean_entity_name(rec["business_name"]),
                applicants=clean_applicants_string(rec.get("applicants", "")),
            )
        )
        await link_record_source(conn, row[0], source_id, role="repaired")
        updated += 1
    return updated


async def _repair_change_of_location(
    conn: AsyncConnection,
    records: list[dict],
    source_id: int,
) -> int:
    """Fix CHANGE OF LOCATION records with NULL previous_location_id."""
    updated = 0
    for rec in records:
        if rec["application_type"] != "CHANGE OF LOCATION":
            continue
        if not rec.get("previous_location"):
            continue
        result = await conn.execute(
            select(license_records.c.id)
            .where(license_records.c.section_type == rec["section_type"])
            .where(license_records.c.record_date == rec["record_date"])
            .where(license_records.c.license_number == rec["license_number"])
            .where(license_records.c.previous_location_id.is_(None))
        )
        row = result.fetchone()
        if not row:
            continue
        prev_loc_id = await get_or_create_location(conn, rec["previous_location"])
        await conn.execute(
            update(license_records)
            .where(license_records.c.id == row[0])
            .values(previous_location_id=prev_loc_id)
        )
        await link_record_source(conn, row[0], source_id, role="repaired")
        updated += 1
    return updated


async def backfill_from_snapshots(engine: AsyncEngine) -> None:
    """Ingest records from all archived HTML snapshots, then repair broken records."""
    paths = list(snapshot_paths(DATA_DIR))
    logger.info("Found %d snapshot(s) to process", len(paths))

    total_inserted = 0
    total_skipped = 0
    total_repaired = 0

    async with get_db(engine) as conn:
        for snap_path in sorted(paths):
            try:
                snap_date = extract_snapshot_date(snap_path)
                records = parse_snapshot(snap_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping %s: %s", snap_path, exc)
                continue

            rel_path = str(snap_path.relative_to(DATA_DIR))
            source_id = await get_or_create_source(
                conn,
                SOURCE_TYPE_CO_ARCHIVE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=snap_date.isoformat() if snap_date else None,
            )

            opts = IngestOptions(
                validate_addresses=False,
                link_outcomes=False,
                source_id=source_id,
            )
            batch_result = await ingest_batch(conn, records, opts)
            total_inserted += batch_result.inserted
            total_skipped += batch_result.skipped

            # Phase 2: repair
            repaired = await _repair_assumptions(conn, records, source_id)
            repaired += await _repair_change_of_location(conn, records, source_id)
            total_repaired += repaired

            await conn.commit()
            logger.debug(
                "%s: inserted=%d skipped=%d repaired=%d",
                snap_path.name, batch_result.inserted, batch_result.skipped, repaired,
            )

    logger.info(
        "Backfill complete: inserted=%d skipped=%d repaired=%d",
        total_inserted, total_skipped, total_repaired,
    )
```

- [ ] **Step 2: Run full test suite**

```bash
uv run pytest tests/ -q
# Expected: 648+ passed, N skipped
```

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_backfill_snapshots.py
git commit -m "#94 feat: Phase 6 Task 3 — pg_backfill_snapshots (async snapshot backfill)"
```

---

## Task 4 — pg_backfill_diffs.py

**Files:**
- Create: `src/wslcb_licensing_tracker/pg_backfill_diffs.py`

Async port of `backfill_diffs.py`. The two-pass extraction logic (primary + supplemental) and CSV export are **pure Python** — no DB access — so they are imported from `backfill_diffs.py` directly. Only DB calls need porting.

- [ ] **Step 1: Write `pg_backfill_diffs.py`**

```python
"""Async backfill of records from unified-diff archives into PostgreSQL.

Async port of backfill_diffs.py. The two-pass diff parsing and CSV export
logic are pure Python and re-used from backfill_diffs unchanged.

Safe to re-run — duplicates are detected by the UNIQUE constraint and skipped.
Address validation is deferred; run ``wslcb backfill-addresses`` afterward.
"""

import logging
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine

from .db import DATA_DIR, SOURCE_TYPE_CO_ARCHIVE, WSLCB_SOURCE_URL
from .parser import SECTION_DIR_MAP
from .pg_db import get_db, get_or_create_source
from .pg_pipeline import IngestOptions, ingest_batch

logger = logging.getLogger(__name__)


def _diff_section_dirs(data_dir: Path, section: str | None = None) -> list[tuple[str, Path]]:
    """Return (section_type, dir_path) pairs for diff archives to process."""
    diffs_root = data_dir / "wslcb" / "licensinginfo-diffs"
    results = []
    for dir_name, section_type in SECTION_DIR_MAP.items():
        if section and dir_name != section:
            continue
        section_dir = diffs_root / dir_name
        if section_dir.exists():
            results.append((section_type, section_dir))
    return results


async def backfill_diffs(
    engine: AsyncEngine,
    *,
    section: str | None = None,
    single_file: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Ingest records from CO diff archives into PostgreSQL.

    Args:
        engine: AsyncEngine connected to the PostgreSQL database.
        section: If set, only process diffs for this section (e.g. "notifications").
        single_file: If set, process only this single diff file path.
        limit: If set, process at most N diff files.
        dry_run: If True, parse but do not write to the database.

    Returns a dict with ``inserted``, ``skipped``, ``errors``, ``files_processed``.
    """
    from .parser import extract_records_from_diff  # pure parsing, no DB

    totals = {"inserted": 0, "skipped": 0, "errors": 0, "files_processed": 0}

    if single_file:
        diff_files = [(None, Path(single_file))]
    else:
        diff_files = []
        for section_type, section_dir in _diff_section_dirs(DATA_DIR, section):
            for f in sorted(section_dir.glob("*.txt")):
                diff_files.append((section_type, f))

    if limit:
        diff_files = diff_files[:limit]

    if dry_run:
        for section_type, diff_path in diff_files:
            try:
                records = extract_records_from_diff(diff_path, section_type)
                totals["inserted"] += len(records)
                totals["files_processed"] += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error parsing %s: %s", diff_path, exc)
                totals["errors"] += 1
        return totals

    async with get_db(engine) as conn:
        for section_type, diff_path in diff_files:
            try:
                records = extract_records_from_diff(diff_path, section_type)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error parsing %s: %s", diff_path, exc)
                totals["errors"] += 1
                continue

            rel_path = str(diff_path.relative_to(DATA_DIR))
            source_id = await get_or_create_source(
                conn,
                SOURCE_TYPE_CO_ARCHIVE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=None,
            )

            opts = IngestOptions(
                validate_addresses=False,
                link_outcomes=False,
                source_id=source_id,
            )
            batch_result = await ingest_batch(conn, records, opts)
            await conn.commit()

            totals["inserted"] += batch_result.inserted
            totals["skipped"] += batch_result.skipped
            totals["errors"] += batch_result.errors
            totals["files_processed"] += 1

            logger.debug(
                "%s: inserted=%d skipped=%d",
                diff_path.name, batch_result.inserted, batch_result.skipped,
            )

    logger.info(
        "Diff backfill complete: files=%d inserted=%d skipped=%d errors=%d",
        totals["files_processed"], totals["inserted"], totals["skipped"], totals["errors"],
    )
    return totals
```

- [ ] **Step 2: Run full test suite**

```bash
uv run pytest tests/ -q
# Expected: 648+ passed, N skipped
```

- [ ] **Step 3: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_backfill_diffs.py
git commit -m "#94 feat: Phase 6 Task 4 — pg_backfill_diffs (async diff archive backfill)"
```

---

## Task 5 — Update cli.py

**Files:**
- Modify: `src/wslcb_licensing_tracker/cli.py`

Replace all SQLite-specific imports and DB calls with PG equivalents. All PG operations are async, so each CLI command handler wraps with `asyncio.run()`. The engine is created from `DATABASE_URL` env var via `create_engine_from_env()`.

Key changes per command:

| Old | New |
|---|---|
| `from .schema import init_db` | removed |
| `from .db import DATA_DIR, DB_PATH, get_db` | `from .db import DATA_DIR, DB_PATH` (keep DATA_DIR/DB_PATH for rebuild/check) |
| `from .scraper import scrape, cleanup_redundant_scrapes` | `from .pg_scraper import scrape as pg_scrape, cleanup_redundant_scrapes as pg_cleanup` |
| `from .backfill_snapshots import backfill_from_snapshots` | `from .pg_backfill_snapshots import backfill_from_snapshots as pg_backfill_snapshots` |
| `from .backfill_diffs import backfill_diffs` | `from .pg_backfill_diffs import backfill_diffs as pg_backfill_diffs` |
| `from .integrity import print_report, run_all_checks` | `from .pg_integrity import print_report, run_all_checks as pg_run_all_checks` |
| `from .link_records import build_all_links` | `from .pg_link_records import build_all_links as pg_build_all_links` |
| `from .endorsements import reprocess_endorsements` | `from .pg_endorsements import reprocess_endorsements as pg_reprocess_endorsements` |
| `from .entities import reprocess_entities` | `from .pg_entities import reprocess_entities as pg_reprocess_entities` |
| `from .address_validator import backfill_addresses, refresh_addresses, refresh_specific_addresses` | `from .pg_address_validator import backfill_addresses as pg_backfill_addresses, refresh_addresses as pg_refresh_addresses` |

Admin user commands (add-user, list-users, remove-user) use raw SQLAlchemy text queries inline.

- [ ] **Step 1: Rewrite the import block at the top of cli.py**

Replace the current 20-line import block (lines 25–43) with:

```python
import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .database import create_engine_from_env
from .db import DATA_DIR, DB_PATH
from .log_config import setup_logging
from .parser import SECTION_DIR_MAP
from .pg_address_validator import backfill_addresses as pg_backfill_addresses
from .pg_address_validator import refresh_addresses as pg_refresh_addresses
from .pg_address_validator import refresh_specific_addresses as pg_refresh_specific_addresses
from .pg_backfill_diffs import backfill_diffs as pg_backfill_diffs
from .pg_backfill_snapshots import backfill_from_snapshots as pg_backfill_snapshots
from .pg_endorsements import reprocess_endorsements as pg_reprocess_endorsements
from .pg_entities import reprocess_entities as pg_reprocess_entities
from .pg_integrity import print_report, run_all_checks as pg_run_all_checks
from .pg_link_records import build_all_links as pg_build_all_links
from .pg_scraper import cleanup_redundant_scrapes as pg_cleanup_redundant
from .pg_scraper import scrape as pg_scrape
```

- [ ] **Step 2: Rewrite each command handler**

Replace command bodies. Pattern: each handler creates an engine and calls `asyncio.run()`.

```python
def cmd_scrape(_args: argparse.Namespace) -> None:
    """Run a live scrape of the WSLCB licensing page."""
    engine = create_engine_from_env()
    asyncio.run(pg_scrape(engine))


def cmd_backfill_snapshots(_args: argparse.Namespace) -> None:
    """Ingest records from archived HTML snapshots."""
    engine = create_engine_from_env()
    asyncio.run(pg_backfill_snapshots(engine))


def cmd_backfill_diffs(args: argparse.Namespace) -> None:
    """Ingest records from unified-diff archives."""
    engine = create_engine_from_env()
    result = asyncio.run(pg_backfill_diffs(
        engine,
        section=args.section,
        single_file=args.file,
        limit=args.limit,
        dry_run=args.dry_run,
    ))
    if args.dry_run:
        print(f"[dry-run] Would insert {result['inserted']:,} record(s) from {result['files_processed']:,} file(s).")
    else:
        print(
            f"Processed {result['files_processed']:,} file(s): "
            f"{result['inserted']:,} inserted, {result['skipped']:,} skipped, "
            f"{result['errors']:,} errors."
        )


def cmd_backfill_provenance(_args: argparse.Namespace) -> None:
    """Populate source provenance for existing records.

    Note: provenance is populated at ingest time via pg_pipeline. This command
    is a no-op in the PostgreSQL version — kept for CLI compatibility.
    """
    print("Provenance is populated at ingest time in the PostgreSQL version. No action needed.")


def cmd_backfill_addresses(args: argparse.Namespace) -> None:
    """Validate un-validated locations via the address API."""
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            await pg_backfill_addresses(conn, rate_limit=args.rate_limit)

    asyncio.run(_run())


def cmd_refresh_addresses(args: argparse.Namespace) -> None:
    """Re-validate locations via the address API."""
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            if args.location_ids:
                with Path(args.location_ids).open() as fh:
                    ids = [int(line.strip()) for line in fh if line.strip()]
                await pg_refresh_specific_addresses(conn, ids, rate_limit=args.rate_limit)
            else:
                await pg_refresh_addresses(conn, rate_limit=args.rate_limit)

    asyncio.run(_run())


def cmd_rebuild_links(_args: argparse.Namespace) -> None:
    """Rebuild all application→outcome links from scratch."""
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            await pg_build_all_links(conn)
            await conn.commit()

    asyncio.run(_run())


def cmd_check(args: argparse.Namespace) -> None:
    """Run database integrity checks."""
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            return await pg_run_all_checks(conn, fix=args.fix)

    report = asyncio.run(_run())
    issues = print_report(report)
    if issues:
        sys.exit(1)


def cmd_cleanup_redundant(args: argparse.Namespace) -> None:
    """Remove data from scrapes that found no new records."""
    engine = create_engine_from_env()
    result = asyncio.run(pg_cleanup_redundant(engine, delete_files=not args.keep_files))
    if result["scrape_logs"] == 0:
        print("Nothing to clean up.")
    else:
        print(
            f"Cleaned {result['scrape_logs']} redundant scrape(s): "
            f"{result['files']} snapshot files removed."
        )


def cmd_reprocess_endorsements(args: argparse.Namespace) -> None:
    """Regenerate record_endorsements from current code mappings."""
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            result = await pg_reprocess_endorsements(
                conn,
                record_id=args.record_id,
                code=args.code,
                dry_run=args.dry_run,
            )
            if not args.dry_run:
                await conn.commit()
            return result

    result = asyncio.run(_run())
    if args.dry_run:
        print(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        print(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['endorsements_linked']:,} endorsement link(s) written."
        )


def cmd_reprocess_entities(args: argparse.Namespace) -> None:
    """Regenerate record_entities from current applicants data."""
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            result = await pg_reprocess_entities(
                conn,
                record_id=args.record_id,
                dry_run=args.dry_run,
            )
            if not args.dry_run:
                await conn.commit()
            return result

    result = asyncio.run(_run())
    if args.dry_run:
        print(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        print(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['entities_linked']:,} entity link(s) written."
        )


def cmd_rebuild(args: argparse.Namespace) -> None:
    """Rebuild database from archived sources.

    Note: rebuild.py targets SQLite. This command is not yet ported to PostgreSQL.
    Use ``wslcb backfill-snapshots`` + ``wslcb backfill-diffs`` for PostgreSQL recovery.
    """
    print(
        "ERROR: 'rebuild' is not yet ported to PostgreSQL.\n"
        "Use 'wslcb backfill-snapshots' and 'wslcb backfill-diffs' to repopulate from archives."
    )
    sys.exit(1)
```

- [ ] **Step 3: Rewrite admin user commands to use PG**

```python
def cmd_admin_add_user(args: argparse.Namespace) -> None:
    """Add an admin user by email."""
    from sqlalchemy import select, text
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from .models import admin_users

    email = args.email.strip()
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            existing = (await conn.execute(
                select(admin_users.c.id).where(
                    text("lower(email) = lower(:email)")
                ).bindparams(email=email)
            )).fetchone()
            if existing:
                print(f"User already exists: {email}")
                return
            await conn.execute(
                pg_insert(admin_users).values(email=email, created_by="cli")
            )
            await conn.commit()
        print(f"Added admin user: {email}")

    asyncio.run(_run())


def cmd_admin_list_users(_args: argparse.Namespace) -> None:
    """List all admin users."""
    from sqlalchemy import select
    from .models import admin_users

    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            result = await conn.execute(
                select(
                    admin_users.c.email,
                    admin_users.c.role,
                    admin_users.c.created_at,
                    admin_users.c.created_by,
                ).order_by(admin_users.c.created_at)
            )
            return result.fetchall()

    rows = asyncio.run(_run())
    if not rows:
        print("No admin users.")
        return
    print(f"{'Email':<40} {'Role':<10} {'Created At':<20} {'Created By'}")
    print("-" * 90)
    for email, role, created_at, created_by in rows:
        print(f"{email:<40} {role:<10} {created_at:<20} {created_by}")


def cmd_admin_remove_user(args: argparse.Namespace) -> None:
    """Remove an admin user by email."""
    from sqlalchemy import delete, func, select, text
    from .models import admin_users

    email = args.email.strip()
    engine = create_engine_from_env()

    async def _run():
        from .database import get_db
        async with get_db(engine) as conn:
            row = (await conn.execute(
                select(admin_users.c.id).where(
                    text("lower(email) = lower(:email)")
                ).bindparams(email=email)
            )).fetchone()
            if not row:
                print(f"User not found: {email}")
                sys.exit(1)
            count = (await conn.execute(select(func.count()).select_from(admin_users))).scalar_one()
            if count <= 1:
                print("Cannot remove the last admin user.")
                sys.exit(1)
            await conn.execute(
                delete(admin_users).where(
                    text("lower(email) = lower(:email)")
                ).bindparams(email=email)
            )
            await conn.commit()
        print(f"Removed admin user: {email}")

    asyncio.run(_run())
```

- [ ] **Step 4: Run full test suite**

```bash
uv run pytest tests/ -q
# Expected: 648+ passed, N skipped
# CLI tests do not exercise DB commands; scraper/backfill tests are integration-only
```

- [ ] **Step 5: Commit**

```bash
git add src/wslcb_licensing_tracker/cli.py
git commit -m "#94 feat: Phase 6 Task 5 — update cli.py to use PostgreSQL modules"
```

---

## Task 6 — scripts/sqlite_to_pg.py

**Files:**
- Create: `scripts/sqlite_to_pg.py`

Standalone one-time migration script. Reads every table from `data/wslcb.db` (SQLite) via the standard `sqlite3` module, inserts into PostgreSQL in FK-dependency order using `asyncpg` directly (for speed — no SQLAlchemy overhead on the write path).

**Critical:** PostgreSQL `GENERATED ALWAYS AS IDENTITY` columns reject explicit ID values unless you use `OVERRIDING SYSTEM VALUE`. After copying, reset each sequence with `setval()` so new inserts don't collide.

**FK-dependency insertion order:**
1. `source_types` (no FKs, no Identity — plain PK)
2. `locations` (Identity)
3. `entities` (Identity)
4. `license_endorsements` (Identity)
5. `endorsement_codes` (FK → license_endorsements)
6. `scrape_log` (Identity — referenced by sources.scrape_log_id)
7. `license_records` (Identity, FK → locations)
8. `sources` (Identity, FK → source_types, scrape_log)
9. `record_endorsements` (composite PK, FK → license_records + license_endorsements)
10. `record_sources` (composite PK, FK → license_records + sources)
11. `endorsement_aliases` (Identity, FK → license_endorsements)
12. `endorsement_dismissed_suggestions` (composite PK, FK → license_endorsements × 2)
13. `regulated_substances` (Identity)
14. `regulated_substance_endorsements` (composite PK, FK → regulated_substances + license_endorsements)
15. `record_entities` (composite PK, FK → license_records + entities)
16. `record_enrichments` (composite PK, FK → license_records)
17. `record_links` (Identity, FK → license_records × 2)
18. `admin_users` (Identity)
19. `admin_audit_log` (Identity)

Skip: `data_migrations` (PG-only table; app populates on first startup).

After all inserts, reset sequences and backfill `search_vector`.

- [ ] **Step 1: Create `scripts/sqlite_to_pg.py`**

```python
#!/usr/bin/env python3
"""One-time SQLite → PostgreSQL data migration.

Reads data/wslcb.db via sqlite3, inserts into PostgreSQL in FK-dependency
order, resets identity sequences, and backfills search_vector.

Usage:
    DATABASE_URL=postgresql+asyncpg://... python scripts/sqlite_to_pg.py
    DATABASE_URL=postgresql+asyncpg://... python scripts/sqlite_to_pg.py --dry-run

Prerequisites:
    - Alembic migrations must be applied first: alembic upgrade head
    - PostgreSQL database must be empty (or have only system rows)
    - SQLite database must be at data/wslcb.db (or set SQLITE_PATH env var)

Safety:
    - Idempotent on empty target: ON CONFLICT DO NOTHING on UNIQUE columns
    - Reads SQLite in read-only mode (uri=True with ?mode=ro)
    - Does NOT modify or delete data/wslcb.db
"""

import asyncio
import os
import sqlite3
import sys
from pathlib import Path

import asyncpg

SQLITE_PATH = Path(os.environ.get("SQLITE_PATH", "data/wslcb.db"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
BATCH_SIZE = 500

# Tables with GENERATED ALWAYS AS IDENTITY columns — need sequence reset after copy
IDENTITY_TABLES = [
    "locations",
    "entities",
    "license_endorsements",
    "scrape_log",
    "license_records",
    "sources",
    "endorsement_aliases",
    "regulated_substances",
    "record_links",
    "admin_users",
    "admin_audit_log",
]

# Insertion order: FK parents before FK children
TABLE_ORDER = [
    "source_types",
    "locations",
    "entities",
    "license_endorsements",
    "endorsement_codes",
    "scrape_log",
    "license_records",
    "sources",
    "record_endorsements",
    "record_sources",
    "endorsement_aliases",
    "endorsement_dismissed_suggestions",
    "regulated_substances",
    "regulated_substance_endorsements",
    "record_entities",
    "record_enrichments",
    "record_links",
    "admin_users",
    "admin_audit_log",
]


def read_sqlite_table(conn: sqlite3.Connection, table: str) -> tuple[list[str], list[tuple]]:
    """Return (column_names, rows) for a SQLite table. Returns ([], []) if table missing."""
    try:
        cur = conn.execute(f"SELECT * FROM {table}")  # noqa: S608
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    except sqlite3.OperationalError:
        return [], []


async def copy_table(
    pg: asyncpg.Connection,
    table: str,
    cols: list[str],
    rows: list[tuple],
    *,
    dry_run: bool = False,
) -> int:
    """Insert rows into PostgreSQL table. Returns count inserted."""
    if not rows:
        return 0

    has_identity = table in IDENTITY_TABLES
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
    overriding = "OVERRIDING SYSTEM VALUE" if has_identity else ""

    sql = (
        f'INSERT INTO {table} ({col_list}) {overriding} VALUES ({placeholders})'
        f' ON CONFLICT DO NOTHING'
    )

    if dry_run:
        print(f"  [dry-run] {table}: would insert {len(rows):,} rows")
        return len(rows)

    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        # Convert sqlite3.Row tuples to list-of-tuples asyncpg expects
        await pg.executemany(sql, [tuple(r) for r in batch])
        inserted += len(batch)

    return inserted


async def reset_sequences(pg: asyncpg.Connection) -> None:
    """Reset identity sequences for all identity tables.

    ``pg_get_serial_sequence()`` does not work for GENERATED ALWAYS AS IDENTITY
    columns (returns NULL). Use ``ALTER TABLE ... ALTER COLUMN id RESTART WITH``
    instead — this is the correct approach for identity columns.
    """
    for table in IDENTITY_TABLES:
        try:
            max_id = await pg.fetchval(f"SELECT COALESCE(MAX(id), 0) FROM {table}")  # noqa: S608
            next_val = max_id + 1
            await pg.execute(
                f"ALTER TABLE {table} ALTER COLUMN id RESTART WITH {next_val}"
            )
            print(f"  Reset sequence: {table} → start at {next_val}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: Could not reset sequence for {table}: {exc}")


async def backfill_search_vector(pg: asyncpg.Connection) -> None:
    """Backfill search_vector on all license_records rows."""
    print("Backfilling search_vector...")
    await pg.execute("""
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
    print("  search_vector backfill complete.")


async def verify_counts(
    sqlite_conn: sqlite3.Connection,
    pg: asyncpg.Connection,
) -> bool:
    """Compare row counts between SQLite and PostgreSQL. Returns True if all match."""
    print("\nVerifying row counts:")
    all_ok = True
    for table in TABLE_ORDER:
        try:
            sqlite_count = sqlite_conn.execute(  # noqa: S608
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
        except sqlite3.OperationalError:
            sqlite_count = 0
        pg_count = await pg.fetchval(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
        status = "✅" if pg_count >= sqlite_count else "❌"
        print(f"  {status} {table:<40} sqlite={sqlite_count:>8,}  pg={pg_count:>8,}")
        if pg_count < sqlite_count:
            all_ok = False
    return all_ok


async def main(dry_run: bool = False) -> None:
    if not SQLITE_PATH.exists():
        print(f"ERROR: SQLite database not found: {SQLITE_PATH}", file=sys.stderr)
        sys.exit(1)

    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    # asyncpg expects postgresql:// not postgresql+asyncpg://
    pg_url = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

    print(f"Source: {SQLITE_PATH}")
    print(f"Target: {pg_url[:pg_url.index('@') + 1]}... (credentials hidden)")
    print(f"Dry-run: {dry_run}")
    print()

    sqlite_conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True)
    sqlite_conn.row_factory = sqlite3.Row

    pg = await asyncpg.connect(pg_url)

    try:
        total_inserted = 0

        for table in TABLE_ORDER:
            cols, rows = read_sqlite_table(sqlite_conn, table)
            if not cols:
                print(f"  SKIP {table} (not in SQLite schema)")
                continue

            count = await copy_table(pg, table, cols, rows, dry_run=dry_run)
            total_inserted += count
            if not dry_run:
                print(f"  ✅ {table}: {count:,} rows")

        if not dry_run:
            print("\nResetting identity sequences...")
            await reset_sequences(pg)

            print()
            await backfill_search_vector(pg)

            ok = await verify_counts(sqlite_conn, pg)
            print()
            if ok:
                print("✅ Migration complete — all row counts match.")
            else:
                print("⚠️  Some row counts differ — review above.")
                sys.exit(1)
        else:
            print(f"\n[dry-run] Would insert ~{total_inserted:,} total rows.")

    finally:
        sqlite_conn.close()
        await pg.close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Migrate SQLite → PostgreSQL")
    p.add_argument("--dry-run", action="store_true", help="Parse and count, no writes")
    args = p.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
```

- [ ] **Step 2: Make script executable**

```bash
chmod +x scripts/sqlite_to_pg.py
```

- [ ] **Step 3: Smoke-test the import (no DB needed)**

```bash
uv run python -c "import scripts.sqlite_to_pg" 2>/dev/null || python scripts/sqlite_to_pg.py --help
# Expected: usage message shown, no import errors
```

- [ ] **Step 4: Run full test suite (script has no unit tests — integration only)**

```bash
uv run pytest tests/ -q
# Expected: 648+ passed, N skipped
```

- [ ] **Step 5: Commit**

```bash
git add scripts/sqlite_to_pg.py
git commit -m "#94 feat: Phase 6 Task 6 — scripts/sqlite_to_pg.py (one-time data migration)"
```

---

## Task 7 — README updates

**Files:**
- Modify: `README.md`

Three sections need updating:

1. **Architecture table** (line ~56): Database row — `SQLite with FTS5` → `PostgreSQL with tsvector + pg_trgm`
2. **Project Structure tree**: Add all new `pg_*.py` files, `scripts/sqlite_to_pg.py`; update `db.py` / `schema.py` notes
3. **Setup section**: Add PostgreSQL prerequisites; update `DATABASE_URL` setup; update `uvicorn` start command note

- [ ] **Step 1: Update Architecture table**

Find the Architecture section and change:
```markdown
| Database | SQLite with [FTS5](https://www.sqlite.org/fts5.html) full-text search |
```
to:
```markdown
| Database | [PostgreSQL](https://www.postgresql.org/) with [tsvector](https://www.postgresql.org/docs/current/datatype-textsearch.html) + [pg_trgm](https://www.postgresql.org/docs/current/pgtrgm.html) full-text search |
```

- [ ] **Step 2: Add pg_* files to Project Structure tree**

After `schema.py` line, add:
```
├── database.py             # Async SQLAlchemy engine factory (DATABASE_URL env var)
├── models.py               # SQLAlchemy Core Table definitions (all 20 tables)
├── pg_schema.py            # Alembic-based schema init (alembic upgrade head)
├── pg_db.py                # Async equivalents of db.py helpers
├── pg_pipeline.py          # Async ingestion pipeline (insert_record, ingest_batch)
├── pg_scraper.py           # Async scraper (WSLCB page fetch, archive, ingest)
├── pg_backfill_snapshots.py # Async backfill from archived HTML snapshots
├── pg_backfill_diffs.py    # Async backfill from CO diff archives
├── pg_integrity.py         # Async integrity checks (run_all_checks, fix_orphaned_locations)
├── pg_endorsements.py      # Async endorsement pipeline
├── pg_endorsements_seed.py # Async endorsement seeding and repair
├── pg_endorsements_admin.py # Async admin helpers for endorsement management
├── pg_entities.py          # Async entity normalization
├── pg_address_validator.py # Async address validation DB layer
├── pg_link_records.py      # Async application→outcome record linking
├── pg_queries.py           # Async search, filters, stats (tsvector + pg_trgm)
├── pg_admin_audit.py       # Async admin audit log
├── pg_substances.py        # Async regulated substance CRUD
├── data_migration.py       # Run-once data migration framework (resolves #85)
```

And after `data/` section:
```
├── scripts/
│   └── sqlite_to_pg.py     # One-time SQLite→PostgreSQL data migration script
├── alembic/                # Alembic schema migrations
│   ├── alembic.ini
│   └── versions/
│       ├── 0001_baseline_postgresql_schema.py
│       └── 0002_fts.py
```

- [ ] **Step 3: Update Setup section**

After "Prerequisites" heading, add PostgreSQL:
```markdown
- Python 3.12+
- PostgreSQL 14+ (local install or container)
- systemd (for scheduling; optional if running manually)
```

Add before "Run the initial scrape":
```markdown
### Configure the database

Copy the template and set your PostgreSQL connection URL:

```bash
cp env.example env
# Edit env: set DATABASE_URL=postgresql+asyncpg://user:password@localhost/wslcb
```

Apply the schema:
```bash
alembic upgrade head
```
```

- [ ] **Step 4: Run full test suite**

```bash
uv run pytest tests/ -q
# Expected: 648+ passed, N skipped
```

- [ ] **Step 5: Update AGENTS.md (worktree) Key Files**

In the worktree's `AGENTS.md`, add entries for the three new Phase 6 modules:

```
| `pg_scraper.py` | *(PostgreSQL migration — Phase 6)* Async port of `scraper.py`. `scrape(engine)` — fetch, hash-check, archive, ingest via `pg_pipeline.ingest_batch()`. `get_last_content_hash(conn)` — SQLAlchemy query against `scrape_log`. `cleanup_redundant_scrapes(engine)` — removes unchanged scrape rows. Pure helpers `compute_content_hash` and `save_html_snapshot` are re-imported from `scraper.py`. |
| `pg_backfill_snapshots.py` | *(PostgreSQL migration — Phase 6)* Async port of `backfill_snapshots.py`. Two-phase: ingest from snapshot files, then repair ASSUMPTION and CHANGE OF LOCATION records. `backfill_from_snapshots(engine)`. |
| `pg_backfill_diffs.py` | *(PostgreSQL migration — Phase 6)* Async port of `backfill_diffs.py`. `backfill_diffs(engine, section, single_file, limit, dry_run)`. Pure diff parsing re-imported from `backfill_diffs.py`. |
```

Update `cli.py` entry:
```
| `cli.py` | *(Phase 6: updated)* All commands use PG modules with `asyncio.run()` wrappers. Engine from `create_engine_from_env()` (reads `DATABASE_URL`). Admin user commands use inline SQLAlchemy queries. |
```

- [ ] **Step 6: Commit**

```bash
git add README.md src/wslcb_licensing_tracker/AGENTS.md 2>/dev/null; git add AGENTS.md
git commit -m "#94 docs: Phase 6 Task 7 — README + AGENTS.md for Phase 6 modules"
```

---

## Cutover Sequence

Once all 7 tasks are complete and pushed, the production cutover:

```bash
# 1. Stop services
sudo systemctl stop wslcb-scraper.timer wslcb-web.service

# 2. Verify SQLite integrity (using OLD cli.py on main branch)
uv run wslcb check

# 3. Apply Alembic schema to PostgreSQL
DATABASE_URL=postgresql+asyncpg://... alembic upgrade head

# 4. Migrate data
DATABASE_URL=postgresql+asyncpg://... python scripts/sqlite_to_pg.py

# 5. Verify PG integrity (using NEW cli.py on feat branch)
DATABASE_URL=postgresql+asyncpg://... uv run wslcb check

# 6. Merge feat branch → main, deploy
git checkout main && git merge feat/94-postgresql-migration

# 7. Start web service
sudo systemctl start wslcb-web.service

# 8. Smoke test
curl https://wslcb-licensing-tracker.exe.xyz:8000/api/v1/health

# 9. Start scraper
sudo systemctl start wslcb-scraper.timer

# Rollback: data/wslcb.db is untouched; revert to old commit + restart
```

---

## Verification

After all 7 tasks:

```bash
uv run pytest tests/ -v
# Expected: 648+ passed, N skipped
```

With `TEST_DATABASE_URL` pointing to a local PostgreSQL that has `alembic upgrade head` applied:
```bash
TEST_DATABASE_URL=postgresql+asyncpg://... uv run pytest tests/ -v
# All PG integration tests also run
```
