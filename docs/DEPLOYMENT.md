# Deployment

Operations reference for the exe.dev VM deployment.

## Services

| Service | Purpose |
|---|---|
| `wslcb-web.service` | uvicorn on port 8000; `ExecStartPost` polls `/api/v1/health` up to 20×(3 s) |
| `wslcb-scraper.timer` | Fires twice daily at 12:30 AM and 6:30 AM Pacific, ±5 min jitter; each run checks in to co-status ([`MONITORING.md`](MONITORING.md), #196) |
| `wslcb-task@.service` | Systemd template for oneshot tasks; instance name = CLI subcommand |
| `wslcb-healthcheck.service` + `.timer` | curl `/api/v1/health` every 5 min; restarts `wslcb-web` on failure. Liveness only; a stale scrape stays 200 (#197) |
| `wslcb-address-validation.timer` | Daily `renew-addresses` (new, due and early renewals; #195), 2:00 AM Pacific, ±5 min jitter. Weekly `backfill-addresses` before #195 |
| `wslcb-entity-classification.timer` | Weekly Jev classification, Sunday 4:00 AM Pacific ±5 min; the classify monitor's dead-man (#207). Installed from [`MONITORING.md`](MONITORING.md), not the copy block |
| `wslcb-compress-stragglers.timer` | Weekly `ingest compress-stragglers`: gzips any uncompressed snapshot/diff left in `data/`, Sunday 3:00 AM Pacific, ±5 min jitter (#200; was #138's disk hygiene) |
| `cannobserv-disk-gc.timer` + `cannobserv-disk-headroom.timer` | The cohort's disk GC (daily 05:15 UTC) and headroom sensor (every 10 min), Provisioner's, installed by `maintaining-host-disk` under `/usr/local/lib`, configured in `/etc/cannobserv/disk-gc/` (#200): [`DISK_GC.md`](DISK_GC.md) |
| `wslcb-backup.service` + `.timer` | Nightly DB dump + `data/` archive to GCS, 08:17 AM Pacific ±10 min; dynamic user, keys as credentials, co-status dead-man check-in (#185). Install, restore and drills: [`RECOVERY.md`](RECOVERY.md) — not part of the copy block below, as it needs its key files first |

### Task service instances

```
wslcb-task@scrape
wslcb-task@import-relayed
wslcb-task@refresh-addresses
wslcb-task@backfill-addresses
wslcb-task@renew-addresses
wslcb-task@backfill-snapshots
wslcb-task@backfill-provenance
wslcb-task@rebuild-links
wslcb-task@compress-stragglers
wslcb-task@classify-entities
```

`wslcb-task@compress-stragglers.service` additionally gets a
`wslcb-task@compress-stragglers.service.d/override.conf` drop-in (`Nice=10`,
`IOSchedulingClass=idle`) — this is genuinely idle-priority background
work, scoped to just this instance so scrape/backfill-addresses keep
default scheduling priority.

`wslcb-task@renew-addresses.service` gets a `timeout.conf` drop-in
(`TimeoutStartSec=90min`, #195): the template's timeout is infinite, and a
hung daily run would block every later firing.

`wslcb-task@scrape.service` and `wslcb-task@classify-entities.service` each get
a `checkin.conf` drop-in (the co-status check-in key, tailnet ordering, a 2 h or
1 h timeout). They are **not** in the copy block below, because each needs its
key file first; install them from [`MONITORING.md`](MONITORING.md).

`wslcb-task@import-relayed` (relay, never checks in):
[`RELAY.md`](RELAY.md).

### Sudoers rule (install once)

```bash
sudo cp infra/sudoers.d-wslcb-healthcheck /etc/sudoers.d/wslcb-healthcheck
sudo chmod 440 /etc/sudoers.d/wslcb-healthcheck
```

Grants `exedev` passwordless `sudo /usr/bin/systemctl restart wslcb-web.service`.

## After changing service files

```bash
sudo cp infra/wslcb-web.service infra/wslcb-task@.service infra/wslcb-scraper.timer \
     infra/wslcb-address-validation.timer infra/wslcb-compress-stragglers.timer \
     infra/wslcb-healthcheck.service infra/wslcb-healthcheck.timer /etc/systemd/system/
sudo cp -r infra/wslcb-task@compress-stragglers.service.d infra/wslcb-task@import-relayed.service.d \
     infra/wslcb-task@renew-addresses.service.d /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-address-validation.timer
sudo systemctl enable --now wslcb-compress-stragglers.timer
sudo systemctl enable --now wslcb-healthcheck.timer
sudo systemctl restart wslcb-web.service
```

A change to `[Unit]` ordering alone (`After=`, `Wants=`) takes effect on
`daemon-reload`, so the restart can wait. `wslcb-web.service` starts after
`postgresql.service`, which Ubuntu's `postgresql@.service` clusters precede.
That's ordering only (#184; `tests/test_infra_web_unit.py`):
- There's no `Wants=`, or the healthcheck's auto-restart would start a cluster
  an operator stopped on purpose.
- There's never a `Requires=`, `BindsTo=` or `PartOf=`, because a Postgres
  restart must not stop the app.

## Memory pressure

No swap, a co-hosted prod service, and earlyoom: the budget, the configs in
`infra/`, how to verify them, and the SocratiCode pins are in
[`MEMORY_PRESSURE.md`](MEMORY_PRESSURE.md) (#175).

## OS security updates

The monthly `patching-hosts` run (#192): its knob, needrestart's list mode,
held steps, NodeSource's Node.js and the after-run checks are in
[`PATCHING.md`](PATCHING.md).

## Logging

Under systemd (non-TTY), all output is JSON lines — `timestamp`, `level`, `name`, `message`. Captured by the journal. Uvicorn access/error logs routed through the same formatter.
Why every uvicorn invocation passes `--log-config`: [`LOGGING.md`](LOGGING.md).

```bash
journalctl -u wslcb-web.service -f
journalctl -u 'wslcb-task@scrape.service' -f
```

## Journal

Persistent journald (#186) and PID 1's log target (#254): installs, checks,
retention and reverts are in [`LOGGING.md`](LOGGING.md#journal).

## Address Validation

The address-validator client's key and flags, renewal TTL + pacing (#195) and
the address commands: [`ADDRESS_VALIDATION.md`](ADDRESS_VALIDATION.md).

## Entity classification (Jev)

Optional TypeSafe Jev pass (#205): `jev_client.py` is the transport,
`jev_classifier.py` the orchestration; the regex stays the fallback.
- **Vars:** `CO_WSLCB_LICENSING_TRACKER_*` in the prod `.env`. `.env.example`
  is canonical.
- **The flag:** `…_ENABLE_JEV_CLASSIFICATION`, off by default, gates *calls*
  only. Stored answers keep resolving.

**When it runs.** After each scrape, and after each relayed import that
ingested a capture. Either way, before the address backfill, and it never
fails the ingest. Also from the weekly timer, and by hand. A run:
- sends only entities with no Jev answer yet (classify once), capped per run;
- skips operator-classified entities, tombstones and noise names the purge would delete;
- skips if another run holds the advisory lock;
- writes a `classification_runs` row.

```bash
uv run wslcb db classify-entities --dry-run      # count + est. cost; no calls, flag ignored
uv run wslcb db classify-entities                # the timer's run; exit 1 on any failure
uv run wslcb db classify-entities --refresh-model M | --refresh-all | --ids-file PATH  # paid
uv run wslcb db ignore-jev [--model M] [--undo]  # reversible rollback; never bulk-delete jev rows
```

**One-off backfill** (#210, ≈ 2,700 requests): `--limit` above the entity
count lifts the cap. Run it as a transient unit, so it outlives ssh:

```bash
sudo systemd-run --unit=wslcb-jev-backfill --uid=exedev \
  --working-directory=/home/exedev/wslcb-licensing-tracker \
  -p EnvironmentFile=/etc/wslcb-licensing-tracker/.env \
  /home/exedev/wslcb-licensing-tracker/.venv/bin/python -m wslcb_licensing_tracker.cli \
  db classify-entities --limit 100000
```

It has no `checkin-key`, so its check-in logs as not configured (or
half-configured). That is expected: the weekly timer is the dead-man.

**Operator review (#208).** Admins get `/admin/entities/review` (flags, low
confidence, `not_a_name`; a separate *Subtype unknown* tab), a panel on each
entity page, and a dashboard section (health, spend, queue counts). Every
action is audited. **Not a name** (queue cards and the entity page) makes the
entity a tombstone (#261): its record links go at once, nothing links it
again, and the public gets a 404 for its page. *Clear manual override* links
it back: [`TOMBSTONES.md`](TOMBSTONES.md).
**Re-classify with Jev** is one paid call: 409 while the flag is off, 503
while another run holds the lock, 502 (audited) past a 90 s deadline.

**Entity links (#264):** Links panel, review **Links** tab, `entity.alias_*`
audit rows: [`ENTITY_ALIASES.md`](ENTITY_ALIASES.md) "Operator UI".

## Testing

### PostgreSQL integration tests

PG integration tests require `CO_WSLCB_LICENSING_TRACKER_TEST_DATABASE_URL` pointing at a test database.

```bash
CO_WSLCB_LICENSING_TRACKER_TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/testdb uv run pytest tests/ -v
```

Without it the PG test suite is **skipped** (not failed) — safe for local dev without a DB.

**In CI**, set `…_REQUIRE_PG_TESTS=1` as well to make a missing `…_TEST_DATABASE_URL` a hard failure that aborts the session:

```bash
CO_WSLCB_LICENSING_TRACKER_REQUIRE_PG_TESTS=1 CO_WSLCB_LICENSING_TRACKER_TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/testdb uv run pytest tests/ -v
```

With `…_REQUIRE_PG_TESTS=1`, if `…_TEST_DATABASE_URL` is missing, `pytest_sessionstart` calls `pytest.exit(returncode=1)` before any collection or test execution — catching all skip paths including `@pytest.mark.skipif` and standalone connection fixtures.

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
- `.venv/` keeps the dev group on purpose (#224): the test gate and pre-commit run from it, and `--no-dev` wouldn't stick anyway, because `uv run` reinstalls default groups. Audit it with `uv audit --frozen` (dev included), not `--no-dev`.
- `node_modules/` holds jsdom, for `npm test` only; `wslcb-web` never loads it. After a `package-lock.json` change, run `npm ci`, then `npm audit` (#229).
- All persistent data in `./data/` (gitignored).
- Every env var is prefixed `CO_WSLCB_LICENSING_TRACKER_` (#204), and `.env.example` is the canonical list. `…_<name>` below abbreviates it. The pre-#204 names (`DATABASE_URL`, `WSLCB_BACKUP_BUCKET`, …) are no longer read (#244): set alone, they leave the variable unset — `alembic` and the CLI then target the localhost `wslcb` default.

### `…_BUILD_ID`

Set automatically by `wslcb-web.service` at startup — `ExecStartPre` writes the short git SHA to `/run/wslcb-build-id`. Used for static asset cache-busting (`?v=<build_id>`) and shown in `/api/v1/health` response and page footer. Falls back to `"dev"` if unset.

## Day-to-day Operations

### Server lifecycle

| Situation | Action |
|---|---|
| Python or template change | `sudo systemctl restart wslcb-web.service` |
| Service file change (existing) | `sudo cp infra/*.service infra/*.timer /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart wslcb-web.service` |
| Service file change (new timer added) | above + `sudo systemctl enable --now <timer-name>.timer` for each new timer |
| Memory-pressure config change | **Not covered by the globs above** — `infra/` also holds a slice drop-in, a sysctl conf and an earlyoom default, none of them `*.service`/`*.timer`. See [`MEMORY_PRESSURE.md`](MEMORY_PRESSURE.md) for the four apply commands. |
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
uv run wslcb db reprocess-entities [--record-id 12345 ...] [--dry-run]   # --record-id repeatable (#269)
uv run wslcb db seed-couple-splits [--dry-run] [--admin-email E]  # one-off: the approved #246 couple verdicts (#269); back up first (COUPLE_SPLITS.md); rerun is a no-op
uv run wslcb db restore-tombstones --backup DIR [--dry-run] [--admin-email E]  # one-off: recreate non-names a pre-#261 reclassify purged (IL / IL IL from #214's backup) as tombstones (TOMBSTONES.md); rerun is a no-op
uv run wslcb db reclassify-entities [--dry-run]   # regex rows + re-resolve classifications; drop regex-noise entities, unlink tombstones (#203, #206, #261)
uv run wslcb db relink-entities [--dry-run]       # rebuild rule-made alias rows (format key, middle names), link keys + record-link parties; idempotent (#262, #263, #265)
uv run wslcb db strip-wac-suffixes [--dry-run]    # one-off: rename/merge entities named '… PER WAC 314-…' (#211); commits only if verified
uv run wslcb db ignore-jev [--model ID] [--undo] [--dry-run]  # reversible Jev rollback: entities fall back to manual/regex (#206)

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

# Data stragglers: any uncompressed snapshot/diff left in data/ (the weekly
# timer's job; #200). Tooling garbage is the disk GC's: docs/DISK_GC.md.
uv run wslcb ingest compress-stragglers --dry-run
uv run wslcb ingest compress-stragglers
```
