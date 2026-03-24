# Unified Validate: Reduce Address API Calls by Half

**Issue:** #121
**Date:** 2026-03-24

## Goal

Cut address API request volume ~50% by using `/validate` as a single call
that covers both standardization and validation, where appropriate. Eliminates
the bursty 2-requests-per-row pattern that triggered exe.dev proxy 500s during
the 2026-03-24 backfill run (see #119).

## Background

The Address Validator's `/validate` response is a strict superset of
`/standardize` — same field names (`address_line_1`, `city`, `region`, etc.)
plus `validation`, `latitude`, `longitude`. The only difference is the
formatted-string key: `standardized` vs `validated`.

During the 2026-03-24 backfill, 783/1453 `/validate` calls got proxy-generated
500s due to burst traffic (standardize+validate back-to-back with no
inter-call pause). The Address Validator service itself saw zero errors.

## Approved Approach

### Decision table

| Scenario | ENABLE_ADDRESS_VALIDATION | Needs std? | Needs val? | API call |
|---|---|---|---|---|
| A: Validation off, not yet standardized | off | yes | no | 1× `/standardize` |
| B: Validation on, neither done | on | yes | yes | 1× `/validate` (covers both) |
| C: Validation on, already standardized | on | no | yes | 1× `/validate` |
| D: Both already done | either | no | no | 0 |

### Changes to `_validate_batch()`

Replace the current two-call pattern:

```python
# Before (2 HTTP calls per row)
ok = await standardize_location(conn, location_id, address)
await validate_location(conn, location_id, address)
```

With a single-call dispatcher:

```python
# After (1 HTTP call per row)
ok = await process_location(conn, location_id, address)
```

### New `process_location()` function

Determines which endpoint to call based on current state and config:

1. Query the location row for `address_standardized_at` and `address_validated_at`.
2. If both are set → skip (return True).
3. If `ENABLE_ADDRESS_VALIDATION` is on → call `/validate`, write all columns
   (`std_*` + `validation_status` + `dpv_match_code` + `latitude` + `longitude`
   + both timestamps).
4. If validation is off → call `/standardize`, write `std_*` columns +
   `address_standardized_at` only.

### DB write changes

`validate_location()` already writes `std_*` columns from the `/validate`
response. The unified path reuses that same mapping. The `std_address_string`
column is written from `result.get("validated")` when using `/validate`, or
`result.get("standardized")` when using `/standardize`.

### `_validate_record_location()` (single-record path)

Same optimization: when validation is enabled and neither timestamp is set,
call `/validate` once instead of standardize-then-validate. When validation
is off, call `/standardize` only.

### Existing `standardize_location()` / `validate_location()` retained

These remain as-is for callers that need a specific endpoint (e.g., future
admin "re-standardize only" action). The new `process_location()` is the
smart dispatcher used by batch and single-record flows.

### Transaction resilience (#120)

Add `conn.begin_nested()` (savepoint) around each row in `_validate_batch()`.
On exception, rollback the savepoint and continue to the next row. Add
periodic `await conn.commit()` every `batch_size` rows (default 100) to
flush progress incrementally.

This prevents a single failed DB write from poisoning the rest of the batch
(the InFailedSQLTransaction cascade seen on 2026-03-24).

## Key Decisions

- **`/validate` is the preferred endpoint when validation is enabled** — it's
  a superset, one call instead of two.
- **No change to caller-commits for single-record helpers** — `standardize_location`
  and `validate_location` keep the convention. Only `_validate_batch` manages
  its own transaction lifecycle (savepoints + periodic commits) because it's
  a long-running bulk operation.
- **Savepoint per row, commit per batch** — balances resilience (one bad row
  doesn't kill the batch) with performance (not committing every single row).

## Out of Scope

- Sending pre-parsed components to `/validate` (optimization, not needed now)
- Retrying proxy 500s (treat as transient; the request reduction should
  prevent them)
- Changes to the Address Validator service itself
