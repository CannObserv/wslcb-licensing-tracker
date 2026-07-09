---
title: Compress licensinginfo-diffs archive
date: 2026-07-09
status: draft
---

# Compress licensinginfo-diffs archive

## Problem

`data/wslcb/licensinginfo-diffs/` holds 3.1G of uncompressed `.txt` unified-diff
archives (2022-09 → 2025-12), now the largest disk consumer in the repo — bigger
than `licensinginfo/` was before #136 compressed it. A sample file compresses
at ~15:1. See design doc: `docs/plans/2026-07-09-compress-diffs-design.md`.

## Approach

Mirror the #136 pattern (`compress-snapshots`) with a new `compress-diffs`
command, but factor the two pieces of logic that would otherwise be
copy-pasted a third time into shared helpers:

1. **Byte-shuffling** (gzip-compress, unlink original, orphan cleanup,
   dry-run reporting) — extract `compress_snapshots`'s inline loop in
   `cli.py` into a private `_compress_files(paths, dry_run)` helper, used by
   both `compress-snapshots` and `compress-diffs`.
2. **Extension-tolerant read** — extract `parser._read_snapshot`'s
   gz-fallback logic into a generic `_read_text_maybe_gz(path)`, used by both
   `_read_snapshot` (kept as a thin wrapper, since `parse_snapshot` calls it
   by that name) and `extract_records_from_diff`.
3. **Extension-tolerant glob** — extract `snapshot_paths`'s shadow-dedup glob
   logic into a generic `_dedup_glob(dir_path, pattern)`, used by
   `snapshot_paths` and a new helper for `_diff_section_dirs` callers.

## Tradeoffs / alternatives

- **Copy-paste `compress-diffs` wholesale** (no shared helpers) — rejected;
  the design doc explicitly calls out that the read-path logic is already
  duplicated once (HTML vs. would-be diff path) and a third copy compounds
  the maintenance burden for no benefit.
- **One generic `compress` command with a `--target` flag** covering both
  directories — rejected; conflates two different data domains (HTML
  snapshots vs. diff archives) with different downstream readers and section
  structure, for a command that's only ever invoked twice, once per domain.

## Steps

1. **Red:** add failing tests in `tests/test_parser.py` for
   `_read_text_maybe_gz` (extracted from `_read_snapshot`) and for
   `extract_records_from_diff` reading a `.txt.gz` file / falling back when
   the plain `.txt` is missing — mirrors `TestReadSnapshot` /
   `TestParseSnapshotGz`.
2. **Green:** extract `_read_text_maybe_gz` in `parser.py`, reimplement
   `_read_snapshot` as a thin wrapper over it, update
   `extract_records_from_diff` to use it instead of `filepath.read_text(...)`.
3. **Red:** add failing tests for a new `_dedup_glob` helper and for
   `_diff_section_dirs`-driven file discovery finding `.txt.gz` files
   (mirrors `TestSnapshotPaths.test_gz_shadows_html_sibling`).
4. **Green:** extract `_dedup_glob(dir_path, pattern)` in `parser.py`;
   reimplement `snapshot_paths()` on top of it; update
   `pg_backfill_diffs.py`'s `section_dir.glob("*.txt")` call (line 62) to use
   it for `*.txt` + `*.txt.gz`.
5. **Red:** add failing tests in `tests/test_cli.py` — `TestCompressDiffs`
   class mirroring `TestCompressSnapshots` (no-files, dry-run, compress,
   rerun-skips, orphan-cleanup, dry-run-reports-orphan) against
   `wslcb/licensinginfo-diffs/<section>/*.txt`.
6. **Green:** extract `_compress_files(paths, dry_run)` from
   `compress_snapshots`'s body in `cli.py`; reimplement `compress_snapshots`
   on top of it; add `compress_diffs` CLI command
   (`wslcb ingest compress-diffs [--dry-run]`) globbing
   `wslcb/licensinginfo-diffs/**/*.txt`.
7. Update `AGENTS.md`'s "Frozen vs. Derived Data Contract" /
   `sources.snapshot_path` note to state the same fallback contract applies
   to diff archives (`.txt` → `.txt.gz`, no DB path migration).
8. Run `uv run pytest tests/ -v` — full suite green.
9. Dry-run against the real archive
   (`uv run wslcb ingest compress-diffs --dry-run`), confirm reported
   count/bytes match expectations (~4,429 files, ~3.1G), then run for real
   and confirm `du -sh data/wslcb/licensinginfo-diffs` drops to the expected
   ~200-300M range.
10. Commit, push branch, open PR referencing #137.

## Open questions / risks

- None — this is a mechanical extension of an already-shipped, reviewed
  pattern (#136), scoped down to compression only per the design doc (no
  writer-side change needed, since diffs are a frozen historical import).
