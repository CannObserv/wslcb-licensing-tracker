# Post-Scrape Address Standardization + Weekly Timer

**Issue:** #113
**Date:** 2026-03-23

## Goal

Ensure new locations get `std_*` columns populated promptly after scraping,
with a weekly safety net for anything that falls through.

## Approved Approach

### 1. Chain `backfill_addresses()` after scrape in `cli.py`

In `cmd_scrape()`, after the scrape completes successfully, call
`backfill_addresses(conn, rate_limit=args.rate_limit)` as a second step.
Add `--rate-limit` flag to the `scrape` subcommand (same default 0.1s).

If backfill fails, log a warning but do not fail the scrape exit code —
the weekly timer catches failures.

### 2. Weekly systemd timer

New timer unit `deploy/wslcb-address-validation.timer` using the existing
`wslcb-task@` template:

- `Unit=wslcb-task@backfill-addresses.service`
- `OnCalendar=Sun *-*-* 02:00:00 America/Los_Angeles`
- `Persistent=true`, `RandomizedDelaySec=300`
- Consistent with `wslcb-scraper.timer` conventions

No new service file needed.

### 3. Remove unused `validate_addresses` from `IngestOptions`

The `validate_addresses` field on `IngestOptions` is declared and set by
callers but never checked in the pipeline. Remove it from the dataclass
and all call sites that set it.

### 4. Documentation

Update `docs/DEPLOYMENT.md` with the new timer unit and enable instructions.

### 5. Tests

Test in `test_cli.py` (or equivalent) confirming `cmd_scrape` calls
`backfill_addresses` after scrape completes. Mock both scrape and backfill.

## Key Decisions

- **No inline standardization** — scrape stays focused on scraping. Address
  work happens as a post-scrape step in the same CLI handler.
- **No local regex fallback** — standardization always goes through the
  Address Validator HTTP endpoint.
- **No changes to `ingest_record()` or pipeline internals** — the stub
  comment and `STEP_ADDRESS` constant remain for potential future use.
- **Backfill failure is non-fatal** — logged as warning, does not affect
  scrape exit code. Weekly timer is the safety net.

## Out of Scope

- Inline HTTP calls during ingestion
- Local regex address parsing
- Changes to `standardize_location()` or `validate_location()` behavior
