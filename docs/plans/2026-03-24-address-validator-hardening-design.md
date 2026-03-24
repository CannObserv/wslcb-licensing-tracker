# Address Validator Client Hardening

**Date:** 2026-03-24

## Goal

Prepare `pg_address_validator.py` for restarting `/validate` calls by fixing
a critical field-mapping bug, adding HTTP 429 retry handling, and tuning
timeouts/rate limits to match the service's throughput.

## Findings

### 1. Standardize response field mapping is wrong (Critical)

`standardize_location()` reads `std_address_line_1`, `std_city`, etc. from
the `/api/v1/standardize` response. The API actually returns `address_line_1`,
`city`, `region`, `postal_code`, `country`, and `standardized` (not
`std_address_string`). Every `.get("std_*")` silently returns the default
empty string, so all standardized locations have blank `std_*` columns.

The `/validate` endpoint uses the correct field names in `validate_location()`,
so validation would overwrite with real data — but only when enabled.

**Impact:** Every location standardized since the post-scrape integration
(#113) has blank `std_*` columns despite `address_standardized_at` being set.

### 2. No HTTP 429 handling (High)

The `/validate` endpoint returns HTTP 429 with a `Retry-After` header when
upstream providers are rate-limited. Our code treats 429 like any non-200 —
logs a warning and returns `None`. The batch moves on, permanently skipping
that address for the run.

### 3. Timeout too short, rate limit too fast (Medium)

- `TIMEOUT = 5.0s` — too short when the service queues requests to USPS
  (5 req/s throughput). A request behind a small queue could easily exceed 5s.
- Default `rate_limit = 0.1s` (10 req/s) is 2x the service's USPS throughput,
  causing unnecessary queuing.

## Approved Approach

### Fix 1: Correct standardize field mapping

Update `standardize_location()` to read the actual API field names:

| Old key | New key |
|---|---|
| `std_address_line_1` | `address_line_1` |
| `std_address_line_2` | `address_line_2` |
| `std_city` | `city` |
| `std_region` | `region` |
| `std_postal_code` | `postal_code` |
| `std_country` | `country` |
| `std_address_string` | `standardized` |

Drop the `validation_status` write from standardize (not in the response);
hardcode `"standardized"` as before.

### Fix 2: Backfill blanked locations

Locations with `address_standardized_at IS NOT NULL` and blank `std_address_line_1`
were affected by the bug. After deploying the fix, run
`wslcb refresh-addresses` to re-process all locations. The existing
`refresh_addresses()` function already handles this — no new code needed,
just the field mapping fix.

### Fix 3: HTTP 429 retry with Retry-After

Add retry logic to `standardize()` and `validate()`:

- On HTTP 429: read `Retry-After` header (seconds), sleep that duration,
  retry once.
- If `Retry-After` is missing or unparseable, use a 2s default.
- Max 3 retries per request with exponential backoff (Retry-After, then 2x,
  then 4x).
- Log each retry at `warning` level.
- After max retries exhausted, return `None` as today.

**Implementation:** A new `_post_with_retry()` helper handles the retry
loop: on 429, it reads `Retry-After`, sleeps, and retries with a doubling
backoff multiplier. `standardize()` and `validate()` delegate to this helper
instead of calling `client.post()` directly.

**Batch-level adaptive backoff (not implemented):** The original design
proposed also adding 429 awareness to `_validate_batch()` — detecting when
individual calls hit rate limits and dynamically increasing the inter-request
sleep for the remainder of the batch. This was dropped during implementation
because: (a) the per-request retry already absorbs transient 429s before
the batch loop sees them, (b) the rate-limit default was reduced from 0.1s
to 0.2s to match the service's 5 req/s USPS throughput, making sustained
429s unlikely, and (c) adding a return signal from `standardize()`/
`validate()` to distinguish "429 after retries" from "other failure" would
complicate the API for marginal benefit. If sustained 429s become a problem
in practice, #118 (Retry-After max cap) is the right place to revisit this.

### Fix 4: Timeout and rate limit tuning

- `TIMEOUT`: 5.0s → 15.0s (accommodates queued requests at 5 req/s USPS
  throughput)
- Default `rate_limit`: 0.1s → 0.2s (matches 5 req/s service throughput,
  avoids unnecessary queuing)

Update the default in `_validate_batch()`, `backfill_addresses()`,
`refresh_addresses()`, `refresh_specific_addresses()`, and the CLI
`--rate-limit` flag.

## Key Decisions

- **Retry inside HTTP helpers, not at batch level** — keeps the batch loop
  simple; each call is self-contained.
- **No batch-level adaptive backoff** — per-request retry absorbs transient
  429s; rate-limit default already matches service throughput. See discussion
  in Fix 3 above.
- **No new CLI command for backfill** — `wslcb refresh-addresses` already
  re-processes everything.
- **`Retry-After` header is authoritative** — always prefer it over our own
  backoff calculation.
- **No `Retry-After` upper cap** — tracked in #118 for future hardening.

## Out of Scope

- Circuit breaker pattern (overkill for single-client usage)
- Concurrent/parallel requests (sequential is correct for rate-limited upstream)
- Changes to `/standardize` or `/validate` request payload format
