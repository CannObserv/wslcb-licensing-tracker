# Design: Behavioral Tests for pg_scraper.py

**Issue:** #95
**Date:** 2026-03-22

## Goal

Add behavioral test coverage for `pg_scraper.py`. The existing tests are smoke-only (hash determinism + coroutine check). Two functions need real coverage: `get_last_content_hash` and `cleanup_redundant_scrapes`.

## Approved Approach

Two new `_needs_db` tests in `tests/test_pg_scraper.py`, using the `pg_conn` and `pg_engine` fixtures from `conftest.py`.

### Test 1 — `get_last_content_hash` returns `None` on empty table

- Use `pg_conn` (rolled-back transaction, no persistence side-effects)
- `DELETE FROM scrape_log` within the transaction to guarantee an empty table
- Call `get_last_content_hash(pg_conn)`, assert result is `None`

### Test 2 — `cleanup_redundant_scrapes` removes unchanged rows + returns correct counts

- Must use `pg_engine` (the function manages its own connection and commits internally)
- Insert one `scrape_log` row with `status='unchanged'` and no linked source
- Call `cleanup_redundant_scrapes(pg_engine, delete_files=False)`
- Assert `result["scrape_logs"] == 1`
- No teardown needed — the cleanup itself removes the row

## Key Decisions

- `pg_conn` vs `pg_engine`: `get_last_content_hash` takes a `conn`, so `pg_conn` is correct. `cleanup_redundant_scrapes` takes an `engine` and commits internally, so `pg_conn`'s rollback isolation is incompatible — use `pg_engine` directly.
- `delete_files=False`: avoids touching the filesystem in tests.
- No test for "skips rows with linked sources" — out of scope per acceptance criteria.
