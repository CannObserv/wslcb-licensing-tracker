---
title: Address validation TTL renewal — attempted_at, daily ceiling, non-destructive re-checks
date: 2026-07-25
status: draft
---

# Address validation TTL renewal

Implements [docs/specs/2026-07-25-address-validation-ttl-renewal-design.md](../specs/2026-07-25-address-validation-ttl-renewal-design.md) (issue #150).

## Problem

Validated addresses are never re-checked, so upstream validator/USPS improvements
(and drift) are never picked up. The first fix (`validated_at < cutoff` selector,
committed in `0a79cee`) runs post-scrape twice daily, unbounded, against a ~59K-row
cohort that all crosses the 180-day TTL in a narrow window — a multi-hour,
tens-of-thousands-of-calls wave that would blow USPS's 10K/day limit and 429 into
Google's 160/day cap. It also keys scheduling off `address_validated_at`, which is
already overloaded (58,716 `standardized` rows carry a stale `validated_at` from a
past standardize-only pass), and it degrades good records on a transient failed
re-check.

## Approach

Separate scheduling from provenance: add `locations.address_validation_attempted_at`
(stamped on every `/validate` call, pass or fail); revert `address_validated_at` to
pure "last confirmation." Renewal selects on `attempted_at` (mode-aware: enabled
path keys on `attempted_at`, disabled path on `standardized_at`), ordered
`NULLS FIRST`, capped by a daily ceiling (`DAILY_VALIDATION_LIMIT = 5000`, counted
via `attempted_at >= start-of-UTC-day`) and paced at 1 req/s. Failed re-checks write
only `status`/`dpv`/`attempted_at` — never `std_*` or `validated_at` — so a re-check
is non-destructive and each row is re-checked at most once per TTL. A migration
backfills `attempted_at := validated_at` so the wave tracks the original validation
distribution (~59K ÷ 5K/day ≈ 12 days) instead of the whole table going eligible on
deploy.

## Tradeoffs / alternatives

- **Keep `validated_at < cutoff` selector, no new column** — rejected: leaves
  `validated_at` overloaded, churns `not_confirmed`-no-`std_` rows every run, and a
  transient failure can't be distinguished from a real one.
- **Trust the re-check + bump `validated_at` on failure to back off** — rejected:
  degrades good data on a transient `unavailable` and corrupts `validated_at`'s
  meaning, contradicting the #150 "never lie to validated_at" principle.
- **Per-run `LIMIT` sized to cadence instead of a true daily ceiling** — rejected:
  fragile to cadence changes and doesn't account for new-location validations
  sharing the same upstream quota; a DB-counted daily budget is exact and robust.

## Steps

1. **Migration `0005`** — `op.add_column("locations", Column("address_validation_attempted_at", DateTime(timezone=True), nullable=True))`; in the same `upgrade()`, `UPDATE locations SET address_validation_attempted_at = address_validated_at WHERE address_validated_at IS NOT NULL`. `downgrade()` drops the column. Add the column to `models.py`. Verify: `alembic upgrade head` then `alembic downgrade -1` round-trips clean on the test DB.
2. **Write path** — in `process_location` and `validate_location`, set
   `address_validation_attempted_at=datetime.now(UTC)` in **both** the confirmed and
   not_confirmed branches; leave the not_confirmed branch otherwise unchanged
   (no `std_*`, no `validated_at`). TDD: extend/adapt existing
   `test_not_confirmed_*` and `test_writes_address_validated_at_on_confirmed` to
   assert `attempted_at` is set in both cases and `validated_at`/`std_*` preserved on
   not_confirmed.
3. **Non-destructive renewal test (review finding #3)** — new test: seed a row with
   a confirmed status, populated `std_*`, and an old `validated_at`; mock `validate`
   → `not_confirmed`; run `process_location`; assert `std_*` and `validated_at`
   unchanged, `status`/`dpv` updated, `attempted_at` bumped.
4. **Selector** — rewrite `backfill_addresses` to the mode-aware selector: enabled →
   `attempted_at IS NULL OR attempted_at < cutoff`, ordered
   `attempted_at ASC NULLS FIRST`; disabled → `standardized_at IS NULL`. Drop the
   old `validated_at < cutoff` / `std_at IS NULL`-in-enabled logic. Replace the
   committed `TestBackfillTTL` with `attempted_at`-based assertions
   (stale/null selected, fresh skipped, disabled-mode keys on `standardized_at`).
5. **Daily ceiling** — add `DAILY_VALIDATION_LIMIT = 5000`; in `backfill_addresses`
   compute `used_today = count(attempted_at >= start-of-UTC-day)`,
   `budget = max(0, LIMIT - used_today)`, return 0 if budget is 0, else apply
   `LIMIT budget`. Test: near-limit clamps the `LIMIT`; at-limit processes nothing.
6. **Pacing** — change `rate_limit` default to `1.0` in the `scrape` command, the
   `backfill-addresses` command, and the `backfill_addresses()` function signature.
7. **Docs** — `docs/SCHEMA.md` (new column + `0005` migration row); `docs/DEPLOYMENT.md`
   (correct "weekly" → per-scrape + weekly; document ceiling + 1 req/s — resolves
   review finding #4); CLI help / module docstrings; `VALIDATION_TTL_DAYS` comment.
8. **Verify** — `uv run pytest tests/ -v` (env sourced) green; `ruff check` +
   `ruff format --check` clean; spot-check the selector against prod-shaped data
   (dry count) to confirm the enabled-path query returns only the ~119 null rows
   today and the projected wave after the TTL date.

## Open questions / risks

- **Daily-ceiling day boundary is UTC**, but USPS's reset TZ is unknown. Mitigated
  by the 5K-of-10K margin; noted in code. Not blocking.
- **`refresh-addresses` (manual, all rows) stays un-ceilinged** — an operator
  running it during the wave could exceed 5K/day (it stamps `attempted_at`, so it
  counts against the scheduled budget but isn't itself capped). Acceptable:
  documented as operator-controlled. Flag if you want a ceiling there too.
- **Standardize-only (validation-disabled) path** isn't ceilinged (counts only
  `attempted_at`); it's dev-only and paced by `rate_limit`. Not addressed here.
