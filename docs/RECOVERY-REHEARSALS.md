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
| 2026-10-09 | timer | `daily/…/20261009T152111Z.dump` | **First scheduled run** (15:21:11Z → 15:21:22Z, 11 s): steady state — new daily, no second monthly, archive 4,861 `unchanged`. #184's one-off `wslcb-20260929T155657Z.dump` + `globals-…sql` shredded after it. | still no check-in |
| 2026-10-09 | agent | `daily/…/20261009T145806Z.dump` | **First drill.** `restore --latest --prefix wslcb-licensing-tracker --into wslcb_drill --run-as postgres` in 13.9 s. Go/no-go: alembic head `0007` = metadata; all 21 tables' row counts equal the dump's own `COPY` counts (`license_records` 122,736 = production; `record_sources` 2,458,154; `sources` 87,897); `wslcb db check` reports exactly production's result (5 placeholder endorsements, pre-existing). `restore-archive --path remediation-backups`: 28 files, `diff -r`-identical to live `data/`. | scratch DB dropped afterwards |
| 2026-10-09 | agent | first run | **First real run** (hand-started 14:58:05Z, done 15:12:14Z, ~14 min): dump 33,900,288 bytes to `daily/` and — first of the month — `monthly/`; archive 4,861 uploaded, 0 failed. Create-only probe before it: create ok, overwrite and delete **403** in both buckets. | unit memory ~141 MB peak observed mid-run; journal kept no `Consumed` line (volatile journal, #186). No check-in yet: monitor pending on CannObserv/status |
| 2026-10-09 | agent | (test DB) | `pg_dump` through stdout appends a second TOC (123,728 vs 62,474 bytes on the test DB); a 60 %–99 % cut of it passes `--list` *and* `-f /dev/null`, yet restores identically — the cut took only the trailing copy. With `--file` every cut 30 %–99 % fails. The job writes with `--file`. | pre-install; no bucket yet |
