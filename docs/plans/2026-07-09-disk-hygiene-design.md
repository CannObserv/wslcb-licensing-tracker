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

**Updated 2026-07-10** after address-validator#162 shipped (PRs #163–#169,
4 CR rounds, 17 findings). Their bash implementation hit several correctness
bugs worth designing around from the start rather than rediscovering — see
"Correctness lessons from #162" below. Their actual shipped script/units were
read directly (`infra/disk-hygiene.sh`, `.service`, `.timer`) to ground this
update, not just their original issue text.

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
   2 newest per `lru.json`'s ordering plus whichever has a live process
   (`pgrep -f "cli/servers/<name>/"`-equivalent check)
2. `~/.vscode-server/extensions/*` — keep only the newest version per
   extension family, comparing parsed semver (mtime as tiebreak only)
3. `~/.vscode-server/data/CachedExtensionVSIXs/*` older than 14 days
4. `~/.npm/_cacache`, `~/.npm/_npx` older than 30 days
5. `uv cache prune`, `pre-commit gc` (shell out to each)
6. Orphaned `.worktrees/*` — dirs with no matching `git worktree list
   --porcelain` entry (parsed by stripping the `worktree ` line prefix, not
   split-on-whitespace, so paths containing spaces can't invert the
   orphan-protection check) **and** older than a 30-minute in-flight grace
   window, so a worktree mid-creation is never swept

### Correctness lessons from #162 (apply from the start, not as a follow-up CR)

Their bash implementation hit these as *live* bugs across 4 CR rounds — all
apply equally to a Python port:

- **Fail-open removal.** Their first live run aborted mid-script when a
  leaked worktree's root-owned `__pycache__` failed to `rm -rf` under
  `set -e`. Every removal here must be wrapped (try/except in Python),
  logging a warning and continuing rather than aborting the whole run.
- **Unreadable/empty `lru.json` → skip the vscode-server-builds section
  entirely**, don't delete everything not explicitly kept. An empty
  keep-list must not be treated as "keep nothing" — that would delete the
  currently-running build too.
- **Version-sorted extension pruning**, not lexical or mtime-only — parse
  each extension dir's semver suffix, sort on that, mtime only as a tiebreak
  for equal versions.
- **Worktree path parsing must be space-safe** (see item 6 above) — their
  `awk '{print $2}'` truncated paths containing spaces, silently inverting
  the orphan check for those paths.

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

**Scheduling:** new `infra/wslcb-disk-hygiene.timer`, matching this repo's
existing timer convention (`America/Los_Angeles`, `Persistent=true`,
`RandomizedDelaySec=300` — see `wslcb-scraper.timer` /
`wslcb-address-validation.timer`) rather than #162's UTC-only style:

```
OnCalendar=Sun *-*-* 03:00:00 America/Los_Angeles
```

Sunday 03:00 sits between the existing `backfill-addresses` (Sun 02:00) and
`scrape` (00:30/06:30) slots — off-peak, no collision. Targets
`wslcb-task@disk-hygiene.service` (existing generic template, no new
`.service` file). Add `(disk_hygiene, "disk-hygiene")` to `_ALIASES` in
`cli.py`.

**Priority:** #162's unit sets `Nice=10` + `IOSchedulingClass=idle` — this
job should be genuinely idle-priority background work. Since
`wslcb-task@.service` is shared with time-sensitive jobs (`scrape`,
`backfill-addresses`), apply this via a systemd drop-in scoped to just the
disk-hygiene instance rather than deprioritizing the shared template:
`infra/wslcb-task@disk-hygiene.service.d/override.conf`.

**Docs:** mirror #162's doc updates — a scheduled-timers section in
`docs/DEPLOYMENT.md` and a quick-ops line in `AGENTS.md`.

## Out of scope

- Docker pruning, Postgres retention, journald (matches #162)
- Any change to what compress-snapshots/compress-diffs actually do — this
  job only detects stragglers and calls the existing commands

## Verification

- `wslcb ops disk-hygiene --dry-run` reports what would be pruned/reclaimed
  without mutating anything
- Unit tests for keep/prune selection logic (vscode-server versions incl.
  unreadable/empty `lru.json`, extension version-sort, orphaned worktrees
  incl. the 30-min grace window and space-containing paths, fail-open
  removal) using `tmp_path` fixtures — this repo's native pytest/TDD
  convention covers what #162 had to hand-build a standalone shell test rig
  for
- Manual run on this VM to confirm before/after `df` matches the dry-run
  estimate, and that VS Code remote still connects afterward
