# Weekly disk-hygiene job — design

## Goal

Recurring disk pressure on the 30G root FS (same-day audit: 13G used, 3.9G
manually reclaimed — stale SQLite backups + stale VS Code server builds, see
[#137](https://github.com/CannObserv/wslcb-licensing-tracker/issues/137)).
Add a scheduled job so this doesn't require a manual audit every time.

Ported from the sister project's design
([address-validator#162](https://github.com/CannObserv/address-validator/issues/162)),
which hit near-identical numbers the same day — same exe.dev VM environment
template, same VS Code server accumulation pattern. Each project runs on its
own VM, so no shared-infra conflict; this is a parallel adoption, not a
shared job.

## Approach

Unlike the sister project's standalone bash script, this repo already has:
- A Python CLI (`wslcb <subcommand>`) with an established `ingest`/`db`/`admin`
  group structure and a top-level alias mechanism for systemd compat
  (`_ALIASES` in `cli.py`)
- A generic `wslcb-task@.service` oneshot systemd template already used for
  scheduled CLI invocations (`scrape`, `backfill-addresses`)
- AGENTS.md conventions requiring `logging` over `print()`, TDD test coverage,
  and Python over ad hoc bash for operational logic

So this reuses that infrastructure instead of porting the bash script
verbatim: a new `wslcb ops disk-hygiene` command, scheduled via a new
`.timer` file pointed at the *existing* `wslcb-task@.service` template — no
new `.service` file needed.

## Scope

**New module:** `src/wslcb_licensing_tracker/disk_hygiene.py`. Age/version
selection logic (which vscode-server builds to keep, which extension version
is newest, which worktrees are orphaned) implemented as pure functions over
listing + metadata, so they're unit-testable without touching the real
`~/.vscode-server`. Thin CLI wiring in `cli.py` under a new `ops` group,
aliased at top level (`disk-hygiene`) for `wslcb-task@%i` compat.

**Env/cache pruning** (parity with address-validator#162):
1. `~/.vscode-server/cli/servers/*` + matching `code-<hash>` dirs — keep the
   2 newest by mtime plus whichever is currently running (PID-checked)
2. `~/.vscode-server/extensions/*` — keep only the newest version per
   extension family
3. `~/.vscode-server/data/CachedExtensionVSIXs/*` older than 14 days
4. `~/.npm/_cacache`, `~/.npm/_npx` older than 30 days
5. `uv cache prune`, `pre-commit gc` (shell out to each)
6. Orphaned `.worktrees/*` — dirs with no matching `git worktree list` entry

**Data hygiene** (this repo's addition beyond #162, not in the sister
design): scan `data/wslcb/licensinginfo/**/*.html` and
`data/wslcb/licensinginfo-diffs/**/*.txt` for uncompressed stragglers. If
found, call the existing `compress_snapshots()` (and `compress_diffs()` once
[#137](https://github.com/CannObserv/wslcb-licensing-tracker/issues/137)
lands) in-process rather than reimplementing compression logic here.

**Threshold warning:** `shutil.disk_usage("/")`, `logger.warning` when used
fraction is ≥75%. Log a before/after summary line with bytes reclaimed.

**Dry-run:** `--dry-run` threaded through every prune step, same convention
as `compress-snapshots`.

**Scheduling:** new `infra/wslcb-disk-hygiene.timer` (weekly,
`Persistent=true`, mirroring `wslcb-address-validation.timer`'s style),
targeting `wslcb-task@disk-hygiene.service` (existing generic template, no
new `.service` file). Add `(disk_hygiene, "disk-hygiene")` to `_ALIASES` in
`cli.py`.

## Out of scope

- Docker pruning, Postgres retention, journald (matches #162)
- Any change to what compress-snapshots/compress-diffs actually do — this
  job only detects stragglers and calls the existing commands

## Verification

- `wslcb ops disk-hygiene --dry-run` reports what would be pruned/reclaimed
  without mutating anything
- Unit tests for keep/prune selection logic (vscode-server versions,
  extension versions, orphaned worktrees) using `tmp_path` fixtures
- Manual run on this VM to confirm before/after `df` matches the dry-run
  estimate
