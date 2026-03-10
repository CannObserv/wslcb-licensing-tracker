# Design: Address Validator API v2 Integration

**Date:** 2026-03-10
**Status:** Approved

## Goal

Adopt the updated Address Validator API (v2.0.0), replacing the existing `/api/v1/standardize`
call with `/api/v1/validate` to capture richer USPS DPV data (validation status, DPV match
code, lat/long, canonical validated address string). Rename `address_line_1`/`address_line_2`
columns to `std_` prefix for consistency, add five new columns, and add a `--rate-limit`
parameter to the backfill/refresh CLI commands.

## API Changes

The API introduced two new endpoints (`/api/v1/parse`, `/api/v1/validate`) and updated
`ValidateRequestV1` to mirror `StandardizeRequestV1`:

- **`ValidateRequestV1`** now accepts either a full raw address string (`address`) or
  pre-parsed `components` dict — same shape as `StandardizeRequestV1`. The server runs
  parse → standardize internally before calling the USPS DPV provider. `address` is the
  full raw address string (not just the street line).
- **`ValidateResponseV1`** returns all fields `StandardizeResponseV1` returns, plus:
  `validation` (`{status, dpv_match_code, provider}`), `latitude`, `longitude`, and
  `validated` (canonical single-line string).

This enables a **single-call flow**: send the raw address to `/validate`, receive both
standardized components and USPS confirmation in one response.

## Approved Approach

### 1. API client changes (`address_validator.py`)

Replace the `standardize()` function with a `validate()` function that calls
`POST /api/v1/validate` with `{"address": raw_address}`. The `validate_location()` function
is updated to call `validate()` and write all new columns in addition to the existing `std_*`
columns.

The old `standardize()` function is removed — it is no longer called anywhere in the hot path.

### 2. Schema migration (new migration in `schema.py`)

**Rename existing columns:**

| Old name | New name |
|---|---|
| `address_line_1` | `std_address_line_1` |
| `address_line_2` | `std_address_line_2` |

SQLite `RENAME COLUMN` (available since 3.25) is used, guarded by `_column_exists()` checks
for idempotency.

**Add five new nullable columns to `locations`:**

| Column | Type | Source field |
|---|---|---|
| `validated_address` | `TEXT` | `ValidateResponseV1.validated` |
| `validation_status` | `TEXT` | `ValidationResult.status` |
| `dpv_match_code` | `TEXT` | `ValidationResult.dpv_match_code` |
| `latitude` | `REAL` | `ValidateResponseV1.latitude` |
| `longitude` | `REAL` | `ValidateResponseV1.longitude` |

New columns default to `NULL`. No index needed on the new columns at this time.

### 3. Column update policy

When `/validate` returns non-null address components (status is `confirmed`,
`confirmed_missing_secondary`, or `confirmed_bad_secondary`):
- Overwrite `std_address_line_1`, `std_address_line_2`, `std_city`, `std_region`,
  `std_postal_code`, `std_country` with corrected USPS values.
- Store all five new columns.
- Set `address_validated_at`.

When status is `not_confirmed` or `unavailable` (address fields are null in response):
- Store `validation_status` and `dpv_match_code` (so we know validation was attempted).
- Leave `std_*` address columns unchanged.
- Do **not** set `address_validated_at` — ensures `backfill-addresses` will retry.

### 4. Regex-parsed columns — kept as offline fallback

`city`, `state`, `zip_code` columns and the `get_or_create_location()` signature are
unchanged. Regex-parsed values continue to be written at insert time and serve as a fallback
tier when the validator is offline during a scrape.

**Rationale:** Address validation is synchronous during ingestion (`IngestOptions.validate_addresses=True`,
`av_client` shared across the batch). When the validator is online, `std_*` columns are
populated in the same `ingest_record()` call, making regex values immediately redundant. When
the validator is offline, regex values prevent a display/filter gap until `backfill-addresses`
runs. The existing `COALESCE(NULLIF(std_X, ''), regex_X)` pattern in `queries.py` is correct
and unchanged.

### 5. Rate limiting

`_validate_batch()` gains a `rate_limit: float` parameter (seconds between API calls).
Default: **0.1 s (10 req/sec)**.

Both `backfill-addresses` and `refresh-addresses` CLI commands gain a `--rate-limit FLOAT`
argument (default 0.1) passed through to `_validate_batch()`.

## Out of Scope

- Exposing `validation_status`, `dpv_match_code`, `latitude`, or `longitude` in the web UI
  or CSV export (deferred — schema only for now).
- Removing the regex-parsed `city`/`state`/`zip_code` columns from the schema (SQLite column
  removal requires table rebuild; not worth the cost).
- Using the new `/api/v1/parse` endpoint (no identified use case).

## Files Affected

| File | Change |
|---|---|
| `src/wslcb_licensing_tracker/schema.py` | New migration: rename 2 columns, add 5 columns |
| `src/wslcb_licensing_tracker/address_validator.py` | Replace `standardize()` with `validate()`; update `validate_location()`; add `--rate-limit` support |
| `src/wslcb_licensing_tracker/queries.py` | Update column references: `address_line_1` → `std_address_line_1`, `address_line_2` → `std_address_line_2` |
| `src/wslcb_licensing_tracker/cli.py` | Add `--rate-limit` to `backfill-addresses` and `refresh-addresses` subcommands |
| `tests/test_database.py` | Update location helper tests for renamed columns |
| `tests/test_queries.py` | Update query tests for renamed columns |
| `tests/test_scraper.py` | Update any address column references |
| `tests/test_routes.py` | Update any address column references in fixtures |
| Templates | Update `address_line_1`/`address_line_2` references to `std_address_line_1`/`std_address_line_2` |
