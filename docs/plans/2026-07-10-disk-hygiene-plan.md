---
title: Weekly disk-hygiene job (wslcb ops disk-hygiene)
date: 2026-07-10
status: draft
---

# Weekly disk-hygiene job

## Problem

Recurring disk pressure on the 30G root FS (see #137). No scheduled job
exists to prune the unbounded-growth vectors identified in the audit:
stale VS Code server builds/extensions/VSIX cache, npm/uv/pre-commit
caches, orphaned worktree dirs. Design doc:
`docs/plans/2026-07-09-disk-hygiene-design.md` (refreshed after the sister
project's address-validator#162 shipped — 4 CR rounds, 17 findings, all
ported into this plan up front).

## Approach

New `src/wslcb_licensing_tracker/disk_hygiene.py`: pure, unit-testable
selection functions (what to prune) separated from a thin orchestrator
(`run_disk_hygiene()`) that performs the actual removals fail-open. A new
`wslcb ops disk-hygiene [--dry-run]` CLI command wires it up, scheduled via
a new `.timer` targeting the existing `wslcb-task@.service` template plus a
systemd drop-in for `Nice=10`/`IOSchedulingClass=idle`.

## Tradeoffs / alternatives

- **Port the bash script directly** — rejected in the original design
  discussion: this repo is Python-first with TDD as a hard convention: a
  bash script gets no unit coverage here (unlike address-validator, which
  had to hand-build a standalone shell test rig from scratch).
- **One monolithic function doing detection + removal inline** — rejected;
  the sister project's CR rounds show these selection rules (version
  parsing, lru.json handling, grace windows) are exactly where correctness
  bugs live. Pure, separately-testable selection functions make each rule
  a one-assertion unit test instead of a full sandboxed run.

## Steps

1. **Red/Green:** `disk_hygiene.py` — `select_vscode_servers_to_prune(servers_dir, keep_count=2, is_running=<callable>)`. Reads `lru.json`; returns `(to_remove, warnings)`. Empty/unreadable `lru.json` → `([], [warning])`, never delete-everything. Never removes a build `is_running()` reports true for, regardless of LRU position. Tests: normal keep-2, missing lru.json, empty lru.json, corrupt JSON, running build outside keep-2 is preserved.
2. **Red/Green:** `select_extensions_to_prune(ext_dir)`. Parses `publisher.name-X.Y.Z[-suffix]` dirs, groups by prefix, keeps highest parsed semver (mtime tiebreak only for equal versions). Tests: multiple versions same extension, single version (nothing pruned), malformed dir name (skipped, not crashed).
3. **Red/Green:** `select_aged_paths(dir_path, max_age_days, now=None)` — generic age-based selector reused for VSIX cache (14d) and `_npx` (30d) entries; `now` defaults to `datetime.now(timezone.utc)`, injectable for deterministic tests via `os.utime` on fixture files.
4. **Red/Green:** `find_orphaned_worktrees(repo_root, worktree_dirs, grace_minutes=30, now=None)`. Runs `git worktree list --porcelain`, parses `worktree <path>` lines by prefix-strip (not split-on-whitespace — space-safe, per #162's CR finding). Excludes any dir younger than `grace_minutes`. Tests: orphaned old dir removed, registered dir kept, dir with spaces in its name correctly matched, dir younger than grace window kept even though orphaned.
5. **Red/Green:** `_remove_path(path, dry_run) -> int` (bytes freed, 0 on dry-run/failure) and `_run_subprocess_step(cmd, dry_run, label)` for `uv cache prune`/`pre-commit gc`/`npm cache clean --force` — both fail-open: log a warning via `logger.warning`, never raise. Tests: successful removal, removal of nonexistent path (no-op), removal failure (simulated via a read-only parent dir or mock) logs warning and continues.
6. **Red/Green:** `check_usage_threshold(threshold_pct=75)` — `shutil.disk_usage("/")`, `logger.warning` at/above threshold. Test with a monkeypatched `shutil.disk_usage`.
7. **Green:** `compress_data_stragglers(dry_run)` in `disk_hygiene.py` — reuses `cli._compress_files` against `cli.SNAPSHOT_GLOB`/`cli.DIFF_GLOB` (new module-level constants in `cli.py`, extracted from the literals already in `compress_snapshots`/`compress_diffs`, single source of truth). No new compression logic.
8. **Green:** `run_disk_hygiene(dry_run) -> dict` orchestrator — calls each section in order, aggregates freed bytes + warnings, logs before/after `shutil.disk_usage("/")`. Every removal call goes through `_remove_path`/`_run_subprocess_step`, so no single failure aborts the run.
9. **Red/Green:** CLI wiring — new `ops` group in `cli.py`, `wslcb ops disk-hygiene [--dry-run]` command, add `(disk_hygiene_cmd, "disk-hygiene")` to `_ALIASES`. Tests mirror `TestCompressSnapshots`'s CliRunner pattern.
10. **Infra:** `infra/wslcb-disk-hygiene.timer` (`OnCalendar=Sun *-*-* 03:00:00 America/Los_Angeles`, `Persistent=true`, `RandomizedDelaySec=300`, targeting `wslcb-task@disk-hygiene.service`) + `infra/wslcb-task@disk-hygiene.service.d/override.conf` (`Nice=10`, `IOSchedulingClass=idle`).
11. **Docs:** `docs/DEPLOYMENT.md` scheduled-timers section entry; `AGENTS.md` quick-ops / Common Tasks line.
12. Run `uv run pytest tests/ -v` — full suite green. `ruff check` / `ruff format --check` clean.
13. Manual dry-run on this VM (`wslcb ops disk-hygiene --dry-run`), review the reclaim estimate, then a real run; confirm `df` delta and that the VS Code remote session survives (the running build must never be pruned).
14. Install the timer/service units, `sudo systemctl daemon-reload`, enable the timer; confirm `systemctl list-timers` shows the scheduled run.

## Open questions / risks

- The VS Code "is a build running" check needs a real process match
  (`pgrep`-equivalent) rather than just "is this the current session's
  build" — the exe.dev VM may have exactly one active session normally, but
  the check should be correct regardless. Will verify against the actual
  running build (confirmed earlier in this session:
  `Stable-fc3def6774c76082adf699d366f31a557ce5573f`) during manual testing.
- `~/.npm` cache pruning is genuine parity with #162 but this repo's JS
  footprint is much smaller (Tailwind build only) — expect this section to
  reclaim little to nothing here; keeping it anyway for consistency and
  because the cache will still grow over time from `npm install` runs.
