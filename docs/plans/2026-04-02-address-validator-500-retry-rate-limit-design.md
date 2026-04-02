# Address Validator: 500 Retry + Rate Limit Reduction

**Date:** 2026-04-02
**Issue:** #127

## Goal

Fix 1,262 address failures from the 2026-04-01 backfill run caused by:
1. `_post_with_retry` not retrying on HTTP 500 (proxy throttle responses)
2. Default rate limit too aggressive (0.2 s → 5 req/s) for bulk backfill

## Approved Approach

### 1. Retry on 500 in `_post_with_retry`

Add `HTTP_INTERNAL_SERVER_ERROR = 500` constant alongside `HTTP_TOO_MANY_REQUESTS = 429`.

Extend retry condition to `response.status_code in (HTTP_TOO_MANY_REQUESTS, HTTP_INTERNAL_SERVER_ERROR)` — shared backoff path reusing existing exponential backoff logic. Distinct log messages per code:

- 429: `"... returned 429 (rate limited by service), retrying in ..."`
- 500: `"... returned 500 (proxy throttle), retrying in ..."`

`_parse_retry_after` already falls back to `DEFAULT_RETRY_AFTER` when no `Retry-After` header is present, so 500s get `DEFAULT_RETRY_AFTER × backoff_multiplier` automatically. Update exhausted-retries log to reference both codes.

### 2. Lower all `rate_limit` defaults from 0.2 → 0.5

All four defaults updated:
- `_validate_batch` — bulk batch path
- `backfill_addresses` — CLI backfill
- `refresh_addresses` — CLI refresh
- `process_location` — single-record live scrape path

## Key Decisions

- **Shared branch, distinct logs:** 429 and 500 share backoff logic to avoid duplication, but log messages distinguish the source (service vs. proxy) for easier diagnosis.
- **All four defaults lowered:** Including `process_location` (live scrape path) per explicit instruction — throughput impact on live scraping is acceptable given the proxy throttle risk.
- `MAX_RETRIES`, `DEFAULT_RETRY_AFTER`, and backoff multiplier unchanged.

## Out of Scope

- No CLI flag changes
- No changes to `_validate_batch` batch_size or transaction logic
