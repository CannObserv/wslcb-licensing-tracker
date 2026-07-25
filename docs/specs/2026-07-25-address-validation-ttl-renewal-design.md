# Address Validation — TTL Renewal, Respectful Pacing, Non-Destructive Re-checks

**Date:** 2026-07-25
**Issue:** #150
**Status:** Design approved; supersedes the initial `validated_at < cutoff` selector committed in `0a79cee`.

## Problem

Validated addresses were never re-checked. `backfill_addresses()` only selected
locations with a NULL `address_standardized_at`/`address_validated_at`, so once a
row was validated it was frozen forever. Real drift observed on location 575706
(`STE 101` dropped as `confirmed_bad_secondary` in May, resolves cleanly now that
upstream improved).

The first fix (add `OR address_validated_at < cutoff` to the selector) exposed two
deeper problems:

1. **Wrong cadence + unbounded load.** `backfill_addresses` runs post-scrape
   **twice daily** (`cli.py` `scrape` hook), not just on the weekly timer. The
   selector is unbounded. Prod has ~59K rows validated in a concentrated
   Feb–Apr 2026 burst (oldest `2026-02-21`), so ~59K rows cross the 180-day TTL in
   a narrow window (~late Aug 2026) and would be re-validated in a single run —
   multiple hours per scrape, tens of thousands of upstream calls at once.

2. **`address_validated_at` is overloaded and unreliable.** Every `standardized`
   row (58,716) has a `validated_at` set that is *older* than its
   `standardized_at` — the fingerprint of a standardize-only pass (validation
   toggled off ~Mar 13) that overwrote `validation_status` but left the old
   timestamp. So `validated_at` does not reliably mean "currently confirmed"; it
   means "confirmed at some point, never cleared since." Using it for renewal
   scheduling conflates two distinct questions: *when did we last confirm?*
   (provenance) vs *when should we re-check?* (scheduling).

## Upstream constraints

- USPS: **10,000 calls/day**. On 429 the validator falls over to Google, which is
  capped at **160 calls/day** — so a 429 storm effectively takes validation
  offline for the day.
- Therefore renewal must be paced (≈1 req/s) and bounded by a **daily ceiling
  comfortably under 10K** (5,000) that never triggers a 429.

## Design

### 1. Schema — separate scheduling from provenance

Add a column to `locations`:

- `address_validation_attempted_at TIMESTAMPTZ NULL` — the last time a `/validate`
  call was made for this row, **regardless of outcome** (pass or fail).

`address_validated_at` reverts to a pure provenance meaning: the last time a
`/validate` call returned a **confirmed** result. Never written on failure, never
cleared.

**Migration data backfill:** set `address_validation_attempted_at :=
address_validated_at` for every row where `address_validated_at IS NOT NULL`. This
makes the renewal wave track the original validation distribution (the ~59K
Feb–Apr rows come due ~Aug–Oct, spread by the ceiling) instead of the entire table
becoming eligible the instant the column ships.

### 2. Write path — `process_location` / `validate_location`

On **every** `/validate` call, set `attempted_at = now()` in **both** branches:

- **confirmed / confirmed_missing_secondary / confirmed_bad_secondary:** overlay
  `std_*`, `validation_status`, `dpv_match_code`, `latitude`, `longitude`,
  `address_standardized_at`, `address_validated_at = now()` (unchanged) **plus
  `attempted_at = now()`**.
- **not_confirmed / unavailable / other:** write `validation_status` +
  `dpv_match_code` + **`attempted_at = now()`** only. `std_*` and `validated_at`
  are left intact.

Consequence — **renewal is non-destructive**: a re-check that fails to confirm
never overwrites a prior good confirmation's `std_*` data or `validated_at`. A
transient `unavailable` cannot degrade a good record. Because `attempted_at` was
bumped, the row backs off a full TTL instead of re-selecting every run.

`standardize_location` (validation disabled) does **not** set `attempted_at` — it
is a standardization, not a validation attempt.

### 3. Selector — `backfill_addresses` (mode-aware)

Scheduling key differs by mode so neither mode churns:

- **Validation enabled** (prod) — key purely on `attempted_at`:
  ```sql
  WHERE (address_validation_attempted_at IS NULL
      OR address_validation_attempted_at < now() - INTERVAL '180 days')
    AND raw_address IS NOT NULL AND raw_address <> ''
  ORDER BY address_validation_attempted_at ASC NULLS FIRST
  LIMIT <remaining daily budget>
  ```
- **Validation disabled** (dev) — key on `standardized_at` (no `/validate` calls
  happen, so `attempted_at` is never set and would select everything every run):
  ```sql
  WHERE address_standardized_at IS NULL
    AND raw_address IS NOT NULL AND raw_address <> ''
  ```

**Why `address_standardized_at IS NULL` is deliberately dropped from the enabled
path:** a `not_confirmed` re-check sets `attempted_at` but never writes `std_*`, so
`std_at` can stay NULL indefinitely. Including `std_at IS NULL` would re-select such
a row every run despite a recent attempt — the exact churn this design eliminates.
After the migration backfill, the only enabled-path rows with `attempted_at IS NULL`
are the ~119 genuinely-unprocessed rows (`validated_at` was NULL), so nothing is
starved.

Keying on `attempted_at` means each enabled-path row is re-checked **at most once
per TTL** whether the check passes or fails — no churn. `NULLS FIRST` prioritizes
never-attempted rows. The cutoff is computed in Python with
`datetime.now(UTC) - timedelta(days=VALIDATION_TTL_DAYS)` (project convention,
keeps it testable), not raw SQL `now()`.

### 4. Daily ceiling — `backfill_addresses`

- `DAILY_VALIDATION_LIMIT = 5000` (module constant).
- `used_today = count(locations WHERE address_validation_attempted_at >=
  start-of-UTC-day)`. Because `attempted_at` is stamped on every call including
  failures, this is an **exact** daily call counter.
- `budget = max(0, DAILY_VALIDATION_LIMIT - used_today)`; if `budget == 0`, log and
  return `0` without querying further.
- Apply `LIMIT budget` to the selector.
- The ceiling is shared across both twice-daily scrape hooks, the weekly timer, and
  any manual `refresh` run the same day (all stamp `attempted_at`), since they draw
  on the same upstream quota.
- Day boundary is UTC; upstream reset TZ is unknown but the 5K-of-10K margin
  absorbs any misalignment.

### 5. Pacing

`rate_limit` default → **1.0** (1 req/s) in three places: the `scrape` command, the
`backfill-addresses` command, and the `backfill_addresses()` function default. A
full 5,000-call day is ≈83 minutes of API time.

### Scope boundaries

- The daily ceiling lives in `backfill_addresses` (the automatic/scheduled path)
  only. `refresh-addresses` / `refresh_specific_addresses` remain manual, operator-
  controlled, and un-ceilinged (they still stamp `attempted_at`, so they count
  against the same-day budget for the scheduled path).
- No change to the frozen/derived data contract; `std_*` and `validated_at`
  semantics are preserved (and made more accurate).

## Spread math (answers "does the ceiling space out the wave?")

~59K due rows ÷ 5,000/day ≈ **~12 days** to drain the initial wave, at 1 req/s,
never exceeding 5K/day — well inside the USPS 10K ceiling, so no 429, no Google
fallover.

## Testing

- Selector: picks stale (`attempted_at < cutoff`) and never-attempted
  (`attempted_at IS NULL`) rows; skips fresh (`attempted_at` within TTL).
- Daily ceiling: with `used_today` near the limit, `LIMIT` is clamped to the
  remaining budget; at/over the limit, nothing is processed.
- Non-destructive renewal: a row with a confirmed `validation_status`, populated
  `std_*`, and an old `validated_at` that re-checks to `not_confirmed` retains its
  `std_*` and `validated_at`, gets `validation_status`/`dpv` updated, and
  `attempted_at` bumped.
- `attempted_at` stamped on both the confirmed and not_confirmed branches.
- Ordering: `NULLS FIRST` so never-attempted rows are processed before stale ones.
- Mode-aware selector: validation-disabled path selects on `standardized_at IS NULL`
  (not `attempted_at`), so a `not_confirmed`-with-recent-`attempted_at` row is not
  re-selected while validation is enabled.

## Documentation

- `docs/SCHEMA.md` — new `address_validation_attempted_at` column + migration entry.
- `docs/DEPLOYMENT.md` — correct "weekly" to "every scrape (twice daily) + weekly
  timer"; document the daily ceiling and 1 req/s pacing (resolves review finding #4).
- CLI help text for `backfill-addresses` / module docstring.

## Open questions

None. Design approved 2026-07-25.
