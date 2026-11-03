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

The exeuntu image masks the apt timers, so patching is a scheduled monthly run
under the vendored `patching-hosts` skill (#192; first run by hand in #184,
gregoryfoster/skills#313). Its host knob, `.skills/patching-hosts`, declares the
window, a UTC `quiet` range per timer firing (derived from `infra/*.timer` by
`tests/test_patching_hosts_knob.py`, #223), the in-flight gate, the restarter
and the Postgres databases; see [`SKILLS.md`](SKILLS.md#patching-hosts). Its
`backup` line makes a fresh `wslcb-backup.service` run (#185) the recovery
point for `wslcb` (#237), in place of a second `pg_dump`. Its two third-party
origins are followed, each in its own held step and scoped by pins: Tailscale
(`origin pkgs.tailscale.com follow`, `infra/preferences.d-tailscale`,
[`RECOVERY.md`](RECOVERY.md#tailnet), #220) and NodeSource
(`origin deb.nodesource.com follow`, [Node.js](#nodejs) below, #239).
apt's needrestart hook (`/etc/apt/apt.conf.d/99needrestart`)
runs `/usr/lib/needrestart/apt-pinvoke -m u … || true`, which calls needrestart;
Ubuntu turns that into *automatic* restarts unless `$nrconf{restart}` is set.
Postgres, `wslcb-web` and dockerd all map libc6/libssl3t64, and a Postgres
restart 503s `/api/v1/health`, which `wslcb-healthcheck` answers with its own
`systemctl restart wslcb-web`. `infra/needrestart.conf.d/wslcb.conf` sets
`'l'`, so an apply only lists what needs restarting; restarts happen in a
window, under the owner's approval.

```bash
sudo install -m 644 infra/needrestart.conf.d/wslcb.conf /etc/needrestart/conf.d/
sudo needrestart -m u -b -r l   # must print "Disabling Ubuntu mode, explicit restart mode configured"
```

- That line proves the *file's* key loaded; `-r l` keeps the proof itself from
  restarting anything. Never run the proof without it.
- Belt and braces on every apply: `sudo NEEDRESTART_MODE=l choom -n 0 -- unattended-upgrade -v`
  (`choom` because the session inherits adj -1000 from exe.dev's sshd).
- Maintainer scripts are out of its reach: `postgresql-16` restarts the cluster
  and `containerd` may restart itself. Hold them as separate steps, with
  `wslcb-healthcheck.timer` stopped across the Postgres one.
- `tests/test_needrestart_config.py` evaluates the file as Perl, and on the
  host pins the installed copy and the live `conf.d` chain.

### Node.js

Node runs SocratiCode's MCP server and the JS test harness (`npm run test:js`),
never a service: no `infra/` unit runs it, and the web process loads none. It
comes from NodeSource's `node_26.x` apt repo (Node 26, end of life 2029-04-30),
which replaced universe's end-of-life `nodejs` 18 and Ubuntu's `npm` (#239).

```bash
sudo install -m 644 infra/nodesource-repo.gpg.asc /etc/apt/keyrings/nodesource.asc
sudo install -m 644 infra/sources.list.d-nodesource.sources /etc/apt/sources.list.d/nodesource.sources
sudo install -m 644 infra/preferences.d-nodesource /etc/apt/preferences.d/nodesource
sudo apt-get update
apt-cache policy nodejs | sed -n 1,5p   # Candidate 26.x-1nodesource1; its version row says 600
sudo apt-get install nodejs         # bundles npm and npx
```

That install is for a host without Ubuntu's `npm`. Over it, NodeSource's
`nodejs` conflicts `npm` away but leaves its ~285 orphans; #239's swap purges
the measured set (`prune-plan.sh --take-orphans`) in the same transaction, with
`APT::Get::AutomaticRemove=false`. Never `--autoremove`: it also takes
unrelated orphans.

- Don't run NodeSource's `setup_26.x`: it pins `nodejs` and `nsolid` at 600
  with no catch-all, which the probe reports as `unscoped:deb.nodesource.com`.
  `tests/test_patching_hosts_knob.py` holds the key's fingerprint, the source
  line and the pins.
- **After every `nodejs` step**, the bundled npm must still find SocratiCode's
  pinned tree, `~/.npm/_npx/<sha512(spec)[:16]>`. A changed key re-installs it
  at the next session launch, #175's memory peak. npm 9.2.0 and 11.20.0 derive
  it identically. `--no` refuses an install, so this exits non-zero rather than
  installing. On a miss it still leaves an empty `_npx/<key>` dir; `rmdir` it.

  ```bash
  SPEC=$(node -p 'require("./.claude/settings.json").env.SOCRATICODE_SPEC')
  (cd ~ && npm exec --no --package="$SPEC" -- true) && echo pinned tree found
  ```

- Running SocratiCode servers keep the old binary until their session restarts.
- Rollback: remove the three files, `apt-get update`, then reinstall universe's
  `nodejs npm` (end of life, the reason for #239).

## Logging

Under systemd (non-TTY), all output is JSON lines — `timestamp`, `level`, `name`, `message`. Captured by the journal. Uvicorn access/error logs routed through the same formatter.

```bash
journalctl -u wslcb-web.service -f
journalctl -u 'wslcb-task@scrape.service' -f
```

## Journal

Persistent since 2026-10-28 (#186), capped at 512M. The Feb-2026 exeuntu image masks
`systemd-journal-flush.service`, so journald stayed in `/run` despite the
image's `Storage=persistent`, and the #184 and #220 reboots each lost a boot.
The fix is the **unmask**: systemd ignores drop-ins on a masked unit, and
`sysinit.target.wants/` already pulls the vendor unit in (`is-enabled` then
reads `static`). `infra/journald.conf.d/wslcb.conf` tracks the setting and the
cap. #200's disk-gc layer adds `zz-cannobserv-journal.conf` with the same 512M
but no `Storage=`, so its cap duplicates ours while our `Storage=persistent`
stays the repo's only tracked copy: keep the file.

```bash
sudo install -m 644 infra/journald.conf.d/wslcb.conf /etc/systemd/journald.conf.d/
sudo systemctl unmask systemd-journal-flush.service
sudo systemctl restart systemd-journald     # stdout streams survive: fd store
sudo systemctl start systemd-journal-flush.service
```

- **Verify:** the flush is `static` and `active`, `/run/systemd/journal/flushed`
  exists, `/var/log/journal/<machine-id>/system.journal` starts at the boot's
  first line, `journalctl --disk-usage` < 512M, a `curl` of `/api/v1/health`
  logs a fresh `uvicorn.access` line, and the probe's `journal.verdict` is
  `persistent`. `tests/test_journald_config.py` passes on the host, 0 skipped.
  `wslcb-web` and `wslcb-task@` run as `exedev`, so their lines land in
  `user-1000.journal`, not `system.journal`; `journalctl -u` reads both.
  Reboot survival shows only after the next one: `journalctl --list-boots` > 1
  boot, `journalctl -b -1 -n 5` ends in `Journal stopped`.
- **Retention:** ~1.5 MB/day (16M in 10.4 d, 24M in 17 d), so 512M ≈ a year.
- **Personal data** (2026-10-28): no personal email addresses (only unit names
  and kernel boot lines).
  `wslcb-web` access lines carry the client socket (127.0.0.1, bar one) and the
  path and query string of searches over public data, kept as long as the
  journal. `sudo journalctl --vacuum-time=<age>` shortens it.
- **Re-mask:** only a rebuild from the Feb-2026 image; dpkg never writes masks
  to `/etc/systemd/system`. The host test and the probe (`journal:volatile`)
  both catch it.

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
- skips operator-classified entities and noise names the purge would delete;
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
action is audited. **Not a name** means the next `reclassify-entities`
deletes the entity with all its rows; only the audit log remains.
**Re-classify with Jev** is one paid call: 409 while the flag is off, 503
while another run holds the lock, 502 (audited) past a 90 s deadline.

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
- `.venv/` keeps the dev group on purpose (#224): the test gate and pre-commit run from it, and `--no-dev` wouldn't stick anyway, because `uv run` reinstalls default groups. Audit it with `uv audit --frozen` (dev included), not `--no-dev`.
- `node_modules/` holds jsdom, for `npm test` only; `wslcb-web` never loads it. After a `package-lock.json` change, run `npm ci`, then `npm audit` (#229).
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
uv run wslcb db reprocess-entities [--record-id 12345] [--dry-run]
uv run wslcb db reclassify-entities [--dry-run]   # regex rows + re-resolve classifications; drop non-name entities (#203, #206)
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
