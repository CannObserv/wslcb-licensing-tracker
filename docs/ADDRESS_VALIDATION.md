# Address Validation

The timer and the `wslcb-task@*-addresses` units are installed from
[`DEPLOYMENT.md`](DEPLOYMENT.md) § Services.

External API at `https://address-validator.exe.xyz:8000`.

- API key: `CO_WSLCB_LICENSING_TRACKER_ADDRESS_VALIDATOR_API_KEY` in `/etc/wslcb-licensing-tracker/.env` (owned `root:exedev`, mode `640`)
- `CO_WSLCB_LICENSING_TRACKER_ENABLE_ADDRESS_VALIDATION=true` enables DPV validation; otherwise only standardization runs
- Services load env via `EnvironmentFile=/etc/wslcb-licensing-tracker/.env`

## Renewal TTL + pacing

Validated addresses are not frozen forever (#150). Each location renews on a TTL
counted from `address_validation_attempted_at`, set per row at selection time
(#195, constants in `address_validator.py`):

| Row | TTL |
|---|---|
| Active: a licence record dated in the last 365 days names it as location or previous location | `VALIDATION_TTL_DAYS` = 180 |
| Inactive | `INACTIVE_TTL_DAYS` = 365 |
| USPS couldn't place it (`undetermined`/`unavailable`, or `confirmed*` with no DPV), active or not | `NO_DETERMINATION_TTL_DAYS` = 365 |

Three kinds of run spend the budget:

- **Hooks** (`backfill_addresses`): after every successful scrape (twice daily) and
  every relay import that succeeds. New locations first, newest first; then due
  rows, earliest due first. Manual `backfill-addresses` runs the same selection.
- **Daily run** (`renew-addresses`, the timer, 2:00 AM Pacific; #195): the same
  rows, then **early renewals** up to `450 − used in 24h − RENEWAL_RESERVE` (90,
  kept for the hooks). Early rows come from due days holding more than that fill
  rate (360): the excess rows, earliest due first, never one attempted under
  `EARLY_RENEWAL_FLOOR_DAYS` (35) ago, which is over address-validator's 30-day
  cache. Peaks such as the September 2026 bulk's renewals spread into the days
  before them and don't re-form one TTL later.
  `--dry-run` logs the selection; `--limit N` is a canary; `--budget-reserve N`
  overrides the reserve.
- **Refresh** (`refresh-addresses`): below.

Scheduling keys on `address_validation_attempted_at` (stamped on every *answered*
`/validate` call, confirmed or not), **not** `address_validated_at` (which stays pure
"last confirmed" provenance). So each row is re-checked at most once per TTL, and a
not_confirmed re-check is **non-destructive** — it leaves `std_*` and
`address_validated_at` intact and simply records the attempt.

Every HTTP 200 is an answer, with three special cases (#187;
CannObserv/address-validator#250, contract v2 deployed 2026-09-30):

- **No determination.** `undetermined` means neither USPS nor the Google fallback
  could place the address. Before v2 the same case arrived as `unavailable` with a
  named provider. The same address gets the same answer on every call, so the
  tracker records the attempt and keeps any prior status/dpv. A legacy
  `unavailable` (#189) or DPV-less `confirmed*` (#190) status gives way to
  `undetermined`, keeping `std_*` and `address_validated_at`.
- **Google-grade confirmation.** A `confirmed*` with no DPV code comes from Google,
  which can alter the street, suite or ZIP (address-validator#258). It never
  replaces a USPS confirmation; it only records the attempt. A row with no
  USPS confirmation still takes it. A row counts as USPS-confirmed when
  `address_validated_at` is set, even if a later `invalid`/`not_found` answer
  cleared its DPV code; only a DPV-less `confirmed*` row does not (#189).
- **Retry-later warning.** Any answer (`undetermined`, or a DPV-less
  `invalid`/`not_found`) warning "a later retry may produce a determination" means
  a fallback provider was out. The tracker writes nothing and retries next run
  (#191). A batch stops after `MAX_CONSECUTIVE_RETRY_LATER` (3) of them with no
  final answer between: USPS is out, and each row would spend a Google call.

A call with **no answer** writes nothing, so the row is retried next run, and it
counts toward the breaker (#183):
- a transport failure, or 429/5xx still failing after retries;
- `unavailable` with no provider, meaning none is configured on the validator.

A 429 whose `Retry-After` is over 60s means a daily provider quota is out
(Google's resets at midnight Pacific; address-validator#270). The run stops on the
first one, or on a 5xx asking for over 60s. It writes nothing for that row, leaves
the rest for the next run, and logs the status and wait (#187).

Outages come back as HTTP errors, never as a 200 status. Among never-attempted
rows the newest location goes first, so a fresh scrape's locations don't queue
behind old rows that keep getting no answer.

**Pacing + daily ceiling** keep us inside upstream limits (USPS 500 per rolling
24h; a 429 falls over to Google at 160/day):

- `--rate-limit` defaults to **1.0** (1 req/s).
- `DAILY_VALIDATION_LIMIT = 450` caps answered `/validate` calls (rows with
  `attempted_at` in the last 24h) across the hooks, the daily run and any
  refresh run.
- One validating run at a time: the hooks, the daily run and a budgeted
  refresh each hold a Postgres advisory lock, so two can't both spend the
  same budget. A run that finds it held logs `Another run holds the validation
  lock` and skips; the next run catches up (#195).
- Rows USPS couldn't place also ask Google (160/day, 5/min). In every run —
  hooks, daily run and both refreshes — known ones go at least
  `GOOGLE_SPACING_SECONDS` (13) apart, and after the first retry-later answer
  the run skips its remaining ones: each would spend a USPS call that leaves
  no stamp, and placeable rows between them reset the retry-later breaker.
  The hooks and the daily run also select at most
  `NO_DETERMINATION_DAILY_LIMIT` (40) per rolling 24h; a refresh runs the ids
  it is given (#195).
- Unanswered calls leave no stamp, so the ceiling can't see them. Instead a batch
  stops after `MAX_CONSECUTIVE_NO_ANSWER` (10) unanswered rows in a row, logging
  `Stopping: … (provider outage?)`; the rest wait for the next run (#183).

To renew outside the TTL, use `refresh-addresses --location-ids F` (uncapped).
For a backlog, add `--budget-reserve N`: ids run in file order up to
`450 − used in 24h − N`, keeping N for the scrape hooks (#189/#190).

## Common address commands

```bash
# Backfill un-processed locations + renew TTL-stale validations
# (runs automatically after every scrape; manual invocation for catch-up only)
uv run wslcb ingest backfill-addresses

# The daily run (#195). One-off runs need the unit's environment (a bare shell
# has no database URL or API key), so run them as transient units: same user,
# directory and env file as wslcb-task@, and a journal to check.
renew() {  # renew <unit-name> [renew-addresses flags]
  local unit=$1; shift
  sudo systemd-run --unit="$unit" --uid=exedev --wait \
    -p WorkingDirectory=/home/exedev/wslcb-licensing-tracker \
    -p EnvironmentFile=/etc/wslcb-licensing-tracker/.env -p Environment=PYTHONUNBUFFERED=1 \
    /home/exedev/wslcb-licensing-tracker/.venv/bin/python -m wslcb_licensing_tracker.cli \
    renew-addresses "$@"
  journalctl -u "$unit" --no-pager -n 50
}
renew wslcb-renew-dry-run --dry-run   # what it would select; calls nothing
renew wslcb-renew-canary --limit 25   # a small live canary
sudo systemctl start 'wslcb-task@renew-addresses.service'   # a full run
journalctl -u 'wslcb-task@renew-addresses.service' -n 50

# Re-standardize all locations
sudo systemctl start 'wslcb-task@refresh-addresses.service'
journalctl -u 'wslcb-task@refresh-addresses.service' -f
# or manually:
uv run wslcb ingest refresh-addresses
```
