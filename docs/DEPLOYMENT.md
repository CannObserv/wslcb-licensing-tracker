# Deployment

Operations reference for the exe.dev VM deployment.

## Services

| Service | Purpose |
|---|---|
| `wslcb-web.service` | uvicorn on port 8000; `ExecStartPost` polls `/api/v1/health` up to 5×(3 s) |
| `wslcb-scraper.timer` | Fires twice daily at 12:30 AM and 6:30 AM Pacific, ±5 min jitter |
| `wslcb-task@.service` | Systemd template for oneshot tasks; instance name = CLI subcommand |
| `wslcb-healthcheck.service` + `.timer` | curl `/api/v1/health` every 5 min; restarts `wslcb-web` on failure |
| `wslcb-address-validation.timer` | Weekly address backfill + TTL renewal, Sunday 2:00 AM Pacific, ±5 min jitter |
| `wslcb-disk-hygiene.timer` | Weekly cache/worktree/data-straggler cleanup, Sunday 3:00 AM Pacific, ±5 min jitter (#138) |

### Task service instances

```
wslcb-task@scrape
wslcb-task@refresh-addresses
wslcb-task@backfill-addresses
wslcb-task@backfill-snapshots
wslcb-task@backfill-provenance
wslcb-task@rebuild-links
wslcb-task@disk-hygiene
```

`wslcb-task@disk-hygiene.service` additionally gets a
`wslcb-task@disk-hygiene.service.d/override.conf` drop-in (`Nice=10`,
`IOSchedulingClass=idle`) — this is genuinely idle-priority background
work, scoped to just this instance so scrape/backfill-addresses keep
default scheduling priority.

### Sudoers rule (install once)

```bash
sudo cp infra/sudoers.d-wslcb-healthcheck /etc/sudoers.d/wslcb-healthcheck
sudo chmod 440 /etc/sudoers.d/wslcb-healthcheck
```

Grants `exedev` passwordless `sudo /usr/bin/systemctl restart wslcb-web.service`.

## After changing service files

```bash
sudo cp infra/wslcb-web.service infra/wslcb-task@.service infra/wslcb-scraper.timer \
     infra/wslcb-address-validation.timer infra/wslcb-disk-hygiene.timer \
     infra/wslcb-healthcheck.service infra/wslcb-healthcheck.timer /etc/systemd/system/
sudo cp -r infra/wslcb-task@disk-hygiene.service.d /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-address-validation.timer
sudo systemctl enable --now wslcb-disk-hygiene.timer
sudo systemctl enable --now wslcb-healthcheck.timer
sudo systemctl restart wslcb-web.service
```

## Memory pressure (#175)

This VM has **7.2 GiB and no swap**, and the production service shares it with
interactive agent sessions. That combination has a specific failure mode, seen
on a sibling host on 2026-09-16: nothing gets OOM-killed. exe.dev session
processes inherit `oom_score_adj` **-1000** from `exe-init`/`sshd`, so the
kernel killer can never pick the session or anything it launches — under real
exhaustion it takes the production service instead, while the kernel fails
*atomic* allocations in unrelated processes (`tailscaled`, `ksoftirqd`). A
cgroup cap on a process the killer won't touch **stalls** it rather than
killing it, so the reservation has to go on the service, not the session.

Three independent pieces, none of which substitutes for another:

| Piece | File | Applies |
|---|---|---|
| Service reservation — `MemoryLow=256M`, `OOMScoreAdjust=-700` | `infra/wslcb-web.service` | `sudo cp` + `daemon-reload` + restart (see above) |
| **Parent slice grant — `MemoryLow=512M`** (without it the row above is inert) | `infra/system.slice.d-10-wslcb-memory.conf` | `sudo install -D -m 644 infra/system.slice.d-10-wslcb-memory.conf /etc/systemd/system/system.slice.d/10-wslcb-memory.conf && sudo systemctl daemon-reload` |
| Kernel atomic-allocation reserve — `vm.min_free_kbytes=65536` | `infra/sysctl.d-60-wslcb-memory.conf` | `sudo install -m 644 infra/sysctl.d-60-wslcb-memory.conf /etc/sysctl.d/60-wslcb-memory.conf && sudo sysctl --system` |
| Userspace OOM killer, acts before the kernel | `infra/default-earlyoom` | `sudo apt install earlyoom && sudo install -m 644 infra/default-earlyoom /etc/default/earlyoom && sudo systemctl enable --now earlyoom && sudo systemctl restart earlyoom` — the **restart is required**, see below — then `journalctl -u earlyoom -n 12 --no-pager` to confirm the parsed thresholds and both regexes |

`MemoryLow=` does not work alone. cgroup v2 limits a unit's effective low
protection by *every* ancestor's, and `system.slice` ships with `memory.low=0`
— so `min(256M, 0) = 0`. Normally the `memory_recursiveprot` mount option makes
protection propagate without per-level config, and systemd ≥ 247 sets it when
it mounts the hierarchy; on this VM `exe-init` mounts cgroup2 first, as bare
`rw`, so it is absent. Verify before trusting the reservation:

```bash
grep cgroup2 /proc/self/mountinfo               # memory_recursiveprot present?
cat /sys/fs/cgroup/system.slice/memory.low      # must be >= the child's grant
```

Both readings are why the slice drop-in is a required row above, not an
optional hardening step.

`OOMScoreAdjust=-700` is **calibrated, not arbitrary**: earlyoom 1.7 floors a
`--prefer` match's score at 300, so the web service only wins if it sits below
that. Measured on this host: adj 0 → ~674, -500 → 341 (still loses), -700 →
~208. Re-measure with systemd's own view of the main PID:

```bash
cat /proc/$(systemctl show -p MainPID --value wslcb-web.service)/oom_score
```

Do **not** reach for `pgrep -f 'uvicorn wslcb'` here: `-f` matches against full
command lines, so any shell whose own command contains that string matches
itself first and reports `-1000`/`0` (the session's inherited values) — which
reads exactly like a correctly-protected service. That misfire happened during
this rollout. Re-measure
if either the service's footprint or the earlyoom config changes — the two are
coupled, and changing one alone silently re-arms the bug.

### Verifying earlyoom actually took the config

`apt install earlyoom` **starts the daemon itself**, before any config is in
place. `systemctl enable --now` is then a no-op on an already-active unit, so
the daemon keeps running with Debian's stock `EARLYOOM_ARGS="-r 3600"` — no
`--prefer`, no `--avoid`, and default 10%/5% thresholds. It reports `active`
and `enabled` throughout, which is why this needs checking rather than
assuming. An explicit `systemctl restart earlyoom` is what applies the config.

The journal is the only place the daemon says what it actually parsed. All
four lines must be present:

```
Preferring to kill process names that match regex '^(node|npm|npx)'
Will avoid killing process names that match regex '^(uvicorn|sshd|systemd|exe-init)'
sending SIGTERM when mem <=  6.00% and swap <= 10.00%,
        SIGKILL when mem <=  3.00% and swap <=  5.00%
```

Two missing regex lines and 10.00%/5.00% thresholds mean the stock config is
still loaded. Confirmed working on this host with earlyoom v1.7 on 2026-09-19.

### Don't let a tool install at launch

The measured peak on this class of host was never the workload — it was the
*install*. A cold `npx -y --prefer-online socraticode@latest` reached 1.2 G at
the cgroup, with all 126 `MemoryHigh` throttle events in the install and none
in the indexing that followed; the same work from a pinned, pre-installed entry
peaked at 75 MB. `--prefer-online` revalidates against the registry on every
launch, so a warm cache is not a warm path.

SocratiCode is therefore **pinned** on this host — one deliberate, capped
install instead of one per launch:

```bash
npm view socraticode version        # pick a literal; never @latest
systemd-run --user --scope -p MemoryHigh=1200M -p MemoryMax=1536M -p CPUQuota=100% \
  -- npm install --prefix ~/.socraticode/pin socraticode@<version>
node skills-vendor/gregoryfoster-skills/skills/init-socraticode/scripts/mcp-driver.mjs resolve
```

`resolve` prints which path won without starting a server; it should report
`pinned install v<version>`, not `npx`. The pin lives outside the repo
(`~/.socraticode/pin`) and is inert when absent, so a fresh clone resolves
exactly as before.

**Known limitation:** pinning the driver does not pin the *session*. Claude
Code cannot override a plugin's MCP command, so the plugin keeps launching
`socraticode@latest` for its own server. `.claude/hooks/socraticode-health.sh`
measures that gap and reports a defect when the two differ by a minor or major
release — a patch apart stays quiet, since a pin is meant to lag.

## Logging

Under systemd (non-TTY), all output is JSON lines — `timestamp`, `level`, `name`, `message`. Captured by the journal. Uvicorn access/error logs routed through the same formatter.

```bash
journalctl -u wslcb-web.service -f
journalctl -u 'wslcb-task@scrape.service' -f
```

## Address Validation

External API at `https://address-validator.exe.xyz:8000`.

- API key: `ADDRESS_VALIDATOR_API_KEY` in `/etc/wslcb-licensing-tracker/.env` (owned `root:exedev`, mode `640`)
- `ENABLE_ADDRESS_VALIDATION=true` enables DPV validation; otherwise only standardization runs
- Services load env via `EnvironmentFile=/etc/wslcb-licensing-tracker/.env`

### Renewal TTL + pacing

Validated addresses are not frozen forever (#150). `backfill-addresses` runs **after
every scrape (twice daily)** and on the **weekly timer**; it renews any location
whose `address_validation_attempted_at` is older than `VALIDATION_TTL_DAYS`
(180 days, in `address_validator.py`), oldest first, so upstream validator/USPS
improvements are picked up without manual intervention.

Scheduling keys on `address_validation_attempted_at` (stamped on every `/validate`
call, pass or fail), **not** `address_validated_at` (which stays pure
"last confirmed" provenance). So each row is re-checked at most once per TTL, and a
not_confirmed/unavailable re-check is **non-destructive** — it leaves `std_*` and
`address_validated_at` intact and simply records the attempt.

**Pacing + daily ceiling** keep us inside upstream limits (USPS 10K/day; a 429 falls
over to Google at 160/day):

- `--rate-limit` defaults to **1.0** (1 req/s).
- `DAILY_VALIDATION_LIMIT = 5000` caps `/validate` calls per UTC day across all
  automatic runs combined (both scrape hooks + the weekly timer). The cap is
  measured by counting rows with `attempted_at >= start-of-UTC-day`, so a manual
  `refresh-addresses` run the same day also counts against it.

Because the initial ~59K validated rows come due in a tight window, the ceiling
spreads the first renewal wave over ~12 days rather than one giant run. To renew on
demand outside the TTL (e.g. a single known-stale row), use
`refresh-addresses --location-ids` (manual, **not** daily-capped).

### Common address commands

```bash
# Backfill un-processed locations + renew TTL-stale validations
# (runs automatically after every scrape; manual invocation for catch-up only)
uv run wslcb ingest backfill-addresses

# Re-standardize all locations
sudo systemctl start 'wslcb-task@refresh-addresses.service'
journalctl -u 'wslcb-task@refresh-addresses.service' -f
# or manually:
uv run wslcb ingest refresh-addresses
```

## Testing

### PostgreSQL integration tests

PG integration tests require `TEST_DATABASE_URL` pointing at a test database.

```bash
TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/testdb uv run pytest tests/ -v
```

Without `TEST_DATABASE_URL` the PG test suite is **skipped** (not failed) — safe for local dev without a DB.

**In CI**, set both env vars to make missing `TEST_DATABASE_URL` a hard failure that aborts the session:

```bash
REQUIRE_PG_TESTS=1 TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/testdb uv run pytest tests/ -v
```

With `REQUIRE_PG_TESTS=1`, if `TEST_DATABASE_URL` is missing, `pytest_sessionstart` calls `pytest.exit(returncode=1)` before any collection or test execution — catching all skip paths including `@pytest.mark.skipif` and standalone connection fixtures.

## Caching

Filter dropdowns and dashboard statistics have **no in-process cache** (#99).
Every request hits the database directly.  The underlying queries are indexed
and complete in <10 ms, so the previous TTL caches were removed to eliminate
silent inconsistency when running multiple uvicorn workers.

`invalidate_filter_cache()` and `invalidate_stats_cache()` still exist as
no-ops so admin mutation call-sites compile without changes.

Worker count has no effect on data freshness — safe to scale workers freely.

## Environment

- Virtualenv at `.venv/` (managed by `uv sync`). If project directory moves, recreate.
- All persistent data in `./data/` (gitignored).

### BUILD_ID

Set automatically by `wslcb-web.service` at startup — `ExecStartPre` writes the short git SHA to `/run/wslcb-build-id`. Used for static asset cache-busting (`?v=<build_id>`) and shown in `/api/v1/health` response and page footer. Falls back to `"dev"` if unset.

## Day-to-day Operations

### Server lifecycle

| Situation | Action |
|---|---|
| Python or template change | `sudo systemctl restart wslcb-web.service` |
| Service file change (existing) | `sudo cp infra/*.service infra/*.timer /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart wslcb-web.service` |
| Service file change (new timer added) | above + `sudo systemctl enable --now <timer-name>.timer` for each new timer |
| Memory-pressure config change | **Not covered by the globs above** — `infra/` also holds a slice drop-in, a sysctl conf and an earlyoom default, none of them `*.service`/`*.timer`. See [Memory pressure](#memory-pressure-175) for the four apply commands. |
| CSS change | `scripts/build-css.sh` (pre-commit hook does this automatically) |
| DB schema change | `uv run alembic upgrade head` (no service restart needed) |
| Test in a worktree | `uv run uvicorn wslcb_licensing_tracker.app:app --host 0.0.0.0 --port 8001 --log-config src/wslcb_licensing_tracker/log_config.json` |
| Stale process on port 8000 | `sudo systemctl restart wslcb-web.service` — never kill manually |

### Application commands

```bash
# Integrity
uv run wslcb db check
uv run wslcb db check --fix

# Data repair
uv run wslcb db rebuild-links
uv run wslcb db reprocess-endorsements [--code 394] [--record-id 12345] [--dry-run]
uv run wslcb db reprocess-entities [--record-id 12345] [--dry-run]

# Backfill
uv run wslcb ingest backfill-snapshots
uv run wslcb ingest backfill-diffs [--section notifications] [--limit 100] [--dry-run]
# Replay-generated provenance extracts (#154). ~43 min for the full corpus
# (chain replay dominates). Idempotent; regenerate after a #151-style
# remediation pass or any diff_replay change that alters reconstructed state.
uv run wslcb ingest generate-replay-extracts [--section notifications] [--dry-run]
uv run wslcb db cleanup-redundant

# Admin users
wslcb admin add-user you@example.com
wslcb admin list-users
wslcb admin remove-user you@example.com

# Disk hygiene (weekly cache/worktree/data-straggler cleanup, #138)
uv run wslcb ops disk-hygiene --dry-run
uv run wslcb ops disk-hygiene
```
