---
title: Relayed scrape — ingest owner-captured WSLCB snapshots during the #198 block
date: 2026-10-20
status: draft
---

# Relayed scrape

Design (approved 2026-10-20): [`docs/specs/2026-10-20-relayed-scrape-design.md`](../specs/2026-10-20-relayed-scrape-design.md).

## Problem

WSLCB has 403'd this host since 2026-10-13 (#198), so nothing has been
ingested since then. The owner's laptop can still fetch the page. Records
start falling out of the 30-day source window for good after about
2026-11-11, so the relay has to be live well before then. Every step below
lands on `main` within days, not weeks.

## Approach

Move the scraper's archive → hash → parse → ingest → log tail out of
`scraper.scrape()` into one shared function. The live scrape and a new relay
import both call it. The relay import runs as `wslcb-task@import-relayed`.
It drains an **inbox directory** that the laptop fills over `scp`.

For each file, oldest first, the import:
- reads `captured_at` from the filename
- rejects captures over 6 h old, or that fail validation (size floor, all
  three section headers, >0 parsed records), moving them to `rejected/`
- ingests the rest under the new `relayed_scrape` source type, with
  `scrape_log.origin = 'relay'` and `scrape_log.captured_at`
- runs the post-scrape address backfill after a success

It never checks in to co-status. The template instance gets no check-in
drop-in.

`scrape_health()` / `scrape_freshness()` change how they read the log:
- **Freshness:** the latest success from either path, dated
  `coalesce(captured_at, finished_at, started_at)`.
- **Failure counters:** `origin = 'server'` rows only.
- **`get_last_content_hash()`:** stays path-agnostic.

Laptop side:
- `scripts/relay/relay-capture.sh`, written for macOS's BSD tools:
  1. `curl -f` the page, then check it's non-empty.
  2. gzip it as `YYYYMMDDTHHMMSSZ.html.gz`.
  3. `scp` it to the inbox as `*.part`, then `mv` it into place over `ssh`.
  4. `ssh … sudo systemctl start wslcb-task@import-relayed.service`, which
     blocks, so the script exits with the import's result.
- A LaunchAgent plist at 00:40 and 06:40 local time.

## Tradeoffs / alternatives

- **Direct CLI over ssh (`wslcb ingest import-relayed <file>`)**: rejected.
  It would need the prod `.env` loaded into an ssh shell, and its logs would
  land outside journald. The systemd instance gets the template's
  `EnvironmentFile` and its journal for free. A queued inbox also tolerates
  laptop retries.
- **Inbox under `data/`**: rejected. The nightly backup mirrors `data/`
  create-only to GCS, so inbox files would be archived twice. The inbox goes
  outside it: `~/wslcb-relay-inbox/`, overridable by `WSLCB_RELAY_INBOX`.
- **Infer the path from `sources.source_type_id`**: rejected. `unchanged` and
  `error` runs have no `sources` row. A `scrape_log.origin` column is the only
  thing that covers every row.
- **Separate validator for the live scrape too**: not now. The live path
  already fails on missing tables. Widening it changes the server's alert
  behaviour, which this work doesn't need.

## Steps

1. **Issue + branch.** File a GH issue, "Relayed scrape: ingest
   owner-captured snapshots during the #198 block", linking the spec, plan,
   and #198. Create a worktree (`using-git-worktrees`, `worktree_venv=none`
   per memory).
   *Done when:* the issue number exists and the branch is checked out.
2. **Schema (TDD).** Migration `0008`:
   - adds `source_types` row 7 `relayed_scrape` / "Relayed Capture"
   - adds `scrape_log.origin` (text NOT NULL default `'server'`, CHECK in
     `server`, `relay`)
   - adds `scrape_log.captured_at` (timestamptz NULL)
   - mirrored in `db.py` `SOURCE_TYPE_ROWS` + `models.py`

   *Done when:* tests assert the row, the column defaults, and the CHECK
   constraint, and `alembic upgrade head` / `downgrade -1` round-trip on the
   test DB.
3. **Refactor `scraper.scrape()`** into fetch + a shared
   `ingest_page(conn, html, log_id, *, source_type, captured_at, metadata)`.
   No behaviour change.
   *Done when:* `test_scraper.py` / `test_cli_scrape.py` pass unmodified, and
   a new test pins the `sources` row the live path writes.
4. **Relay import (TDD).** New `relay.py`, plus a `wslcb ingest
   import-relayed` command with a top-level alias (for `wslcb-task@%i`).
   *Done when:* tests cover
   - a valid capture → `success`, a `relayed_scrape` source, `origin =
     'relay'`, `captured_at` set
   - the same page twice → `unchanged`
   - a 403 page, a truncated file, and a 7 h old capture → `rejected/`, an
     `error` row, non-zero exit
   - an empty inbox → a no-op, exit 0
   - several files → processed oldest-first
   - no co-status check-in on any path
5. **Health semantics (TDD).** Change `scrape_health()`, `scrape_freshness()`
   and the CLI's `_scrape_history()` per Approach. The #197 JSON shape stays
   the same.
   *Done when:* tests cover
   - relay success between server 403s → fresh, `consecutive_errors` keeps
     counting server errors
   - relay rejection → server numbers untouched
   - freshness dated by `captured_at`
6. **Laptop script + plist.** Both under `scripts/relay/`.
   *Done when:*
   - `bash -n` / shellcheck pass
   - a structural test (like `test_infra_scrape_unit.py`) pins the plist's
     00:40/06:40 `StartCalendarInterval`, the script's use of `systemctl
     start wslcb-task@import-relayed.service`, and the `.part`-then-`mv`
     upload
   - a dry run with the transport stubbed shows the steps in order
7. **Docs.**
   - `docs/MONITORING.md`: relay section, how it reads in `/health`, and why
     it never checks in
   - `docs/DEPLOYMENT.md`: VM inbox setup, and the laptop install (key,
     `ssh-add --apple-use-keychain`, ssh config stanza, plist,
     `launchctl bootstrap`/`kickstart`/`bootout`)
     *Revised during execution:* the runbook went to a new `docs/RELAY.md`.
     DEPLOYMENT.md had only 65 tokens of budget left, so it gets just the
     task instance and copy-block line.
   - `docs/SCHEMA.md`: migration 0008
   - `.env.example`: `WSLCB_RELAY_INBOX`
   - `.skills/doc-sensitive-paths` / `doc-sections` if `relay.py` should
     trigger the doc gate
   - AGENTS.md: at most a one-line pointer

   *Done when:* the context-budget guard is quiet and
   `test_doc_sensitive_paths.py` passes.
8. **Review, ship, deploy.**
   - CR pass (`reviewing-code-python-fastapi`), then merge to `main`
   - `alembic upgrade head`, restart `wslcb-web`, create the inbox
   - **Hand-off to owner:** create the key, register it on exe.dev, and
     install the LaunchAgent. Then `launchctl kickstart` once.

   *Done when:* a relayed capture ingests on prod, its chip shows "Relayed
   Capture" on a record page, `/api/v1/health` reads fresh, the co-status
   scrape fault is still open, and #198 has a comment recording the first
   relay.

## Open questions / risks

- **Deadline.** ~2026-11-11. If steps 2–5 slip, a stopgap would be the owner
  `scp`-ing one capture plus a manual `wslcb ingest import-relayed` after step
  4. That's worth doing the moment step 4 merges, before the laptop
  automation exists.
- **Silent relay stoppage.** If the laptop job stops, nothing pages; it shows
  only as `/health` staleness after 26 h (accepted in the design). Revisit if
  it bites.
- **Laptop timezone.** The plist times are local. If the laptop leaves
  Pacific time, captures shift. That's harmless while the 6 h age limit and
  the hash check hold.
- **`sudo` over ssh** needs a non-interactive `sudo -n` on the VM. `exedev`
  has passwordless sudo today. If that ever changes, add a narrow sudoers
  rule for this one unit, like `infra/sudoers.d-wslcb-healthcheck`.
