# Recovery rehearsals

Dated record of backup drills and what each proved. The procedure is
[`RECOVERY.md`](RECOVERY.md) → *Drill*. Newest first.

## Every test run

`tests/test_backup_restore_rehearsal.py`, against `TEST_DATABASE_URL`:

- **Round trip** — a real `pg_dump` of the migrated test database passes the
  backup's own verification, ships to a fake bucket, comes back by
  `--latest` with its sha256 and `pg_restore` checks, and restores into an
  empty scratch database with the same schema version and the same row counts
  in `alembic_version`, `license_records`, `sources`, `record_sources`,
  `scrape_log`.
- **Truncated dump refused** — a real archive cut at 60 % fails verification.
- **Failed restore leaves the target untouched** — a mid-restore collision rolls
  back the whole `--single-transaction` load.

## Log

| Date | By | Object | What it proved | Notes |
|---|---|---|---|---|
| 2026-10-09 | agent | (test DB) | `pg_dump` through stdout appends a second TOC (123,728 vs 62,474 bytes on the test DB); a 60 %–99 % cut of it passes `--list` *and* `-f /dev/null`, yet restores identically — the cut took only the trailing copy. With `--file` every cut 30 %–99 % fails. The job writes with `--file`. | pre-install; no bucket yet |
