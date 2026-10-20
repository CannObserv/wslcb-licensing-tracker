# Relayed scrape: ingest owner-captured snapshots during the WSLCB block — design

2026-10-20 · tracks #198

## Intent

Since 2026-10-13, `licensinginfo.lcb.wa.gov` has returned an ELB 403 to this
host and to other datacenter IPs (#198). The page still loads from the owner's
residential connection. Until WSLCB lifts the block, the owner's laptop
fetches the page and relays it to the VM, which ingests it the way a scrape
would. The source page is a rolling 30-day window, so records the gap misses
start falling off for good after about 2026-11-11 (30 days after the last
successful fetch). This closes the gap now, not after outreach resolves.

## Goals

- Twice-daily captures from the owner's laptop, at the usual times, with no
  manual step once installed. A manual one-off run must work too (the same
  script, run by hand).
- Records ingested from a relayed capture are clearly attributed to it, on the
  public record page and in queries.
- Relayed runs show up in the run history alongside server scrapes.
- After the block lifts, turning the relay off is one step on the laptop.

## Non-goals

- Working around the block by other means (UA change, proxy, other egress).
  Those are out of scope by owner direction (#198).
- Generic upload of arbitrary files or historical backfill. This is for the
  live page only.
- Any web/HTTP upload endpoint. Transport is SSH.

## Anti-goals (failure even if it "works")

- A relayed run masks the block in co-status. The scrape monitor
  (`co-wslcb-scrape`, #196) must keep reporting the server's own 403 fault
  until the server's fetch itself succeeds.
- A bad capture (403 page, login/error page, truncated download) is ingested,
  or logged as a `success` with zero records.
- The owner's home IP, hostname, or any personal identifier lands in the DB or
  on the public site.
- Relayed pages are recorded as `live_scrape`.

## Decided

**Provenance (owner's choice).** A new source type, `relayed_scrape`, added the
way `co_replay` was: a `SOURCE_TYPE_ROWS` entry in `db.py` plus an Alembic data
migration. Its public label shows as a chip on the record detail page, like
"Live Scrape". My call: label "Relayed Capture", description roughly
"WSLCB page captured off-host by the operator and relayed to the server
during an IP block".

**`sources` row** for a relayed capture, filled like a live scrape's:
- `url`: the WSLCB page.
- `captured_at`: the laptop's fetch time, not the import time.
- `snapshot_path`: in the normal archive tree.
- `scrape_log_id`: the run's `scrape_log` row.
- `metadata`: minimal. My call: `{"relay": "owner-laptop"}` plus the reported
  fetch time. No IP, no hostname.

**Shared change detection.** Relayed and server captures share one
last-content-hash, the existing `get_last_content_hash()` over `scrape_log`.
Whichever arrives second with the same page logs `unchanged`.

**Freshness and monitoring (agreed).**
- The server's scheduled scrape keeps running twice a day. It keeps 403ing,
  co-status holds one open fault with a daily reminder, and the first server
  success sends "has cleared". That is the owner's signal to turn the relay
  off.
- Relayed imports **do not** check in to co-status.
- Relayed `success`/`unchanged` **do** count as fresh data for `/api/v1/health`
  (`scrape_freshness`) and for the alert's `last_success_at` /
  `window_expires`. The data really was captured. Accepted consequence:
  `consecutive_errors` in the server's alert resets after each relay.
- `scrape_log` must record which path produced each row (server vs relay), so
  the run history and any later query can tell them apart. A column is the
  obvious way. See builder's call below.
- My call, on how the health numbers read the two paths:
  - **Freshness** (`last_success_at`, `stale`, `window_expires`) takes the
    latest success from either path. For a relay, it uses the capture time,
    not the import time, because freshness is about how old the data is.
  - **Failure counters** (`consecutive_errors`, `last_attempt_status`) read
    server rows only. They describe whether the server can reach WSLCB.
  - A rejected relay doesn't touch the server's numbers. It surfaces as a
    non-zero exit on the laptop and an `error` row tagged as a relay.
  - The import rejects a capture more than 6 h old (a queued or retried
    capture), so stale pages never refresh the freshness reading.
  - This changes how `scrape_health()` / `scrape_freshness()` filter. The
    #197 JSON shape stays the same.

**Validation before ingest.** The import rejects a file that isn't the real
report: wrong or missing expected structure, implausibly small, or zero parsed
records. A rejection logs `scrape_log` status `error` with a clear message and
exits non-zero, so the laptop script sees it. The thresholds are the builder's
call, grounded in `docs/SOURCE_PAGE.md` and real snapshots (about 13.5 MB, three
sections).

**Post-ingest work** matches a scrape's: after a `success`, run the same
address backfill the scheduled scrape runs, non-fatal on failure.

**Laptop side.** The laptop runs macOS 27 (owner). The owner confirmed plain
`curl` gets a 200 from the residential IP, so the script uses curl's default
User-Agent.
- A small shell script in the repo (e.g. `scripts/relay-capture.sh`).
  - It fetches the page with `curl`, never a browser "Save as", because
    browsers rewrite HTML.
  - It fails on non-200 or an empty body. It ships the file over `scp`/`ssh`
    and triggers the import.
  - It writes in BSD userland (macOS `date`/`stat`).
  - VM host and paths are configurable, not hard-coded to the owner's machine.
- Scheduled by a **launchd LaunchAgent**, not cron. The plist lives in the
  repo next to the script and is installed to `~/Library/LaunchAgents/`.
  - Why: a LaunchAgent runs in the owner's login session, so it inherits the
    macOS `ssh-agent` socket and can use Keychain-stored passphrases. cron
    runs outside that session and can't reach either.
  - Times: `StartCalendarInterval` at **00:40 and 06:40** local time (my call:
    10 min after the server's 00:30/06:30 PT, so the server wins when both
    succeed). This assumes the laptop's clock is on Pacific time; docs note it.
  - The laptop is awake at those times (owner). If it was asleep, launchd runs
    a missed job on wake, and the 6 h age limit rejects it only if it is truly
    stale.
- **SSH identity** (owner's choice): a dedicated key, e.g.
  `~/.ssh/id_ed25519_wslcb_relay`.
  - Loaded into the macOS agent with `ssh-add --apple-use-keychain`, and an
    `~/.ssh/config` stanza for the VM host with `IdentitiesOnly yes`,
    `UseKeychain yes`, `AddKeysToAgent yes`. That's the same pattern the owner
    already uses for `gh` in VS Code.
  - exe.dev SSH supports no per-key command restriction (its sshd, not
    `authorized_keys`), so this key has full `exedev` access, and with it
    sudo. The owner accepts this.
  - Registering it as an additional key on the exe.dev account is the owner's
    step. exe.dev allows any number of keys per account (owner), so it can be
    revoked on its own without touching the owner's everyday key.
  - Docs give the install steps: key, agent, ssh config, plist,
    `launchctl bootstrap gui/$UID …`, and a one-off `launchctl kickstart` test.

**Retirement.** When co-status sends "has cleared", the owner unloads the
LaunchAgent (`launchctl bootout`). My call: the import command, source type, and script stay in the
repo, dormant and documented, for the next block. Nothing to tear down
server-side.

## Constraints

- Frozen-data contract (AGENTS.md): `sources`/`record_sources` rows are
  provenance and are never edited after ingest. The new type is additive.
- Production config is `/etc/wslcb-licensing-tracker/.env` (`root:exedev`,
  0640). The import must run with it, as the scheduled task does via
  `wslcb-task@.service`'s `EnvironmentFile`. `exedev` has passwordless sudo.
- Logging: `logging` only, JSON to journald (AGENTS.md § Logging).
- TDD. Tests in `tests/test_<module>.py`, no network, PG via `pg_conn`/`pg_engine`.
- Docs: `docs/MONITORING.md` (relay section, its interaction with the scrape
  monitor), `docs/DEPLOYMENT.md` (command + laptop setup), `docs/SCHEMA.md`
  (new source type, any `scrape_log` column). Keep AGENTS.md within budget.
  At most a one-line pointer there.

## Left to the builder

- How the import is invoked over SSH. Options: a direct CLI call (e.g. `wslcb
  ingest import-relayed <file> --captured-at …`) with the prod env loaded, or an
  inbox directory processed by a `wslcb-task@…` instance started with `sudo
  systemctl start`. My lean: the inbox + systemd route, for journald logging,
  the same env and timeout handling, and laptop retries that queue naturally.
  Either is acceptable if logs land in the journal.
- How `captured_at` travels: filename timestamp, argument, or sidecar.
- How `scrape_log` marks the path: new column + migration vs inferring through
  `sources`. A column is preferred, since `unchanged` runs have no `sources`
  row.
- Exact validation thresholds and the structure markers checked.
- How much of `scraper.scrape()` to factor out so live and relayed share one
  archive → hash → parse → ingest → log path, not a copy.
- Concurrency guard if a server scrape and an import overlap. Unlikely, given
  the 10-minute stagger.
