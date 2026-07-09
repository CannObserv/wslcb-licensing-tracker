# Compress licensinginfo-diffs archive — design

## Goal

Reclaim disk space consumed by `data/wslcb/licensinginfo-diffs/`, currently 3.1G
across 4,429 uncompressed `.txt` unified-diff files (2022-09 → 2025-12). This is
now the single largest consumer on disk, larger than `licensinginfo/` was before
issue #136 compressed it.

## Context

`licensinginfo-diffs/` is a **one-time historical archive import**, not an
actively-growing directory. File mtimes cluster around 2026-02-26 (bulk load),
regardless of the diff's filename date. `pg_backfill_diffs.py` is a backfill
script — the live scraper (`pg_scraper.py`) writes HTML snapshots directly and
does not generate new diff files. So unlike #136, there's no writer-side change
needed to stop future growth; this is purely a one-time compression.

A sample diff file compresses at ~15:1 (30KB → 2KB gzip), consistent with the
17:1 ratio #136 found for HTML snapshots. Expected recovery: ~3.1G → ~200-300M.

## Approach

Mirror the #136 pattern (`compress-snapshots`), but factor the extension-tolerant
glob/read logic into a shared helper instead of copy-pasting a second time —
the read side (`parser._read_snapshot` vs. the diff read path) was already
duplicated logic per code review; adding a third copy compounds it.

**Changes:**

1. **New CLI command** `wslcb ingest compress-diffs [--dry-run]` in `cli.py`,
   sibling to `compress-snapshots`. Same semantics: gzip existing `.txt` files
   to `.txt.gz`, delete orphaned `.txt` when a `.txt.gz` sibling already exists,
   `--dry-run` reports counts/bytes without writing.
2. **Shared helper** for extension-tolerant glob + gzip read, used by both
   `compress-snapshots` and `compress-diffs` (and their respective readers).
3. **`parser.extract_records_from_diff()`** gains a `.txt.gz` fallback,
   mirroring `_read_snapshot()`'s pattern for stale-path tolerance.
4. **`pg_backfill_diffs._diff_section_dirs()`** glob extended to match both
   `*.txt` and `*.txt.gz`.
5. **`AGENTS.md`** — extend the "Frozen vs. Derived Data Contract" /
   snapshot-path note to cover diff archives, so the compressed-file contract
   is documented in one place for both directories.

## Out of scope

- No change to how new diffs are generated (nothing in this codebase generates
  them going forward).
- No DB schema change — `sources.snapshot_path`-style path tolerance already
  covers extension mismatches via the fallback-read pattern.
- Retention/deletion of old diff data — out of scope per the "never delete
  historical data" contract in AGENTS.md; this is compression only.

## Verification

- `wslcb ingest compress-diffs --dry-run` reports expected file count/bytes
  before running for real.
- `wslcb backfill-diffs` (re-run against a compressed section) still parses
  correctly via the `.txt.gz` fallback.
- Existing `TestCompressSnapshots`-style test coverage extended for the diffs
  command (dry-run, orphan cleanup, idempotency).
