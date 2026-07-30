# #154 — Replay-generated source provenance extracts

**Status:** approved (issue #154, review comment 2026-07-30)
**Depends on:** #151 (chain replay, merged; remediation executed 2026-07-30)

## Problem

73,436 of 112,143 records have diff-file-only provenance, and their source
viewer is broken twice over:

1. **Wrong branch:** `backfill_diffs.py` stamps diff-derived sources
   `SOURCE_TYPE_CO_ARCHIVE`, so `app.py`'s `co_diff_archive` check routes
   diff paths to `extract_tbody_from_snapshot`, which parses diff text as
   HTML and finds nothing.
2. **Structural:** even on the right branch, `extract_tbody_from_diff`
   re-parses raw `+`/`-` line streams and can only find records whose
   complete `<tbody>` block sits inside a single run — `exit` origin works,
   `entry` is partial, `mutation`/`state`/`final` never work.

Chain replay (`diff_replay.replay_diff_chain`) reconstructs full page
states deterministically; the reconstructed block is the honest rendering
of what the source showed. Generate per-record extracts from it.

## Data-layer facts (verified 2026-07-30, production DB)

- The same 4,429 diff files exist as source rows under **both** `co_archive`
  (replay backfill) and `co_diff_archive` (older per-diff backfill) — 8,858
  rows; `get_or_create_source` uniqueness is `(source_type_id, snapshot_path)`.
  Both sets are frozen provenance; neither is retagged. The viewer must
  tolerate either stamping → dispatch on the *file suffix*, not source_type.
- Replayed blocks are `<tbody>`..`</tbody>` line groups inclusive
  (`_iter_known_blocks` / `_complete_blocks_in_run`), so an extract file is
  exactly the joined group — no re-wrapping needed.

## Design

### Extract files (derived data — regenerable)

- Path: `data/wslcb/licensinginfo-replay/<section_dir>/<record_date>/<license>-<slug(application_type)>.html.gz`
  where `<section_dir>` is the diff-archive dir name (`notifications` /
  `approvals` / `discontinued`) and `slug()` lowercases and collapses
  non-alphanumerics to `-`.
- Content: gzip of the reconstructed block lines joined with `\n`
  (first-evidencing state; block content is immutable for its page
  lifetime, so no last-seen variant — YAGNI).
- Named by natural key, not content hash: exactly one extract per record
  (collector is first-source-wins), stable across regeneration — a replay
  bugfix overwrites in place and never strands `sources.snapshot_path`.
  This moots the dedupe open question.
- Disk: ~73k × 1–2 KB gz ≈ 75–150 MB; date-sharded dirs avoid a flat tree.

### Provenance model

- New source type: `SOURCE_TYPE_CO_REPLAY = 6`, slug `co_replay`, label
  "CO Replay Extract", description "Reconstructed from CO diff chain
  replay". Seeded in `db.py` `SOURCE_TYPE_ROWS` + additive alembic data
  migration (0006), per the documented pattern.
- One `sources` row per extract: `snapshot_path` = extract path,
  `captured_at` = evidencing diff timestamp, `metadata` =
  `{"origin": ..., "evidencing_file": ..., "generated_at": ...}`.
- `record_sources` link role: `replay_extract`.
- Existing diff-file source rows stay as raw evidence (multi-origin);
  extracts add, not replace. Boundary: extract *files* are derived and
  regenerable; the `co_replay` *source rows* are frozen provenance —
  stable natural-key paths keep those two facts compatible.

### Generator

`replay_extracts.py` — `generate_replay_extracts(engine, *, section=None,
dry_run=False)`, CLI `wslcb ingest generate-replay-extracts`:

1. Per section: `replay_diff_chain(files, section_type, keep_raw=True)` —
   collector retains the raw block lines on each record (`raw_lines` key;
   `insert_record` uses `.get()` so the extra key is inert elsewhere).
2. For each replayed record, look up the DB record by natural key
   (`section_type, record_date, license_number, application_type`). No
   match (post-remediation phantom) → count `unmatched`, skip.
3. Write extract file (overwrite in place — idempotent).
4. `get_or_create_source(SOURCE_TYPE_CO_REPLAY, snapshot_path, ...)` +
   `link_record_source(record_id, source_id, role="replay_extract")` —
   both idempotent.

Rerunnable at any time; regenerates the full corpus. Recorded in the
AGENTS.md Derived table.

### Viewer (`app.py` source_viewer)

Dispatch order:

1. `source_type == "co_replay"` → read the extract file directly (gz
   fallback per the `_read_snapshot` contract); content is already an
   isolated `<tbody>`.
2. `snapshot_path` ends `.txt`/`.txt.gz` → `extract_tbody_from_diff`
   (fixes defect 1 for either historical stamping).
3. Else → `extract_tbody_from_snapshot`.

`strip_anchor_tags` still applies to all branches. `display.py` gains a
`co_replay` entry in the source-type display map.

## Sequencing

1. Defect-1 suffix dispatch fix (shippable alone).
2. Migration + display entry.
3. Collector raw-lines retention.
4. Generator + CLI.
5. `co_replay` viewer branch.
6. Production run post-#151-remediation (done 2026-07-30) → verify live.
