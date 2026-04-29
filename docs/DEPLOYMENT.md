# Deployment

Operations reference for the exe.dev VM deployment.

## Services

| Service | Purpose |
|---|---|
| `wslcb-web.service` | uvicorn on port 8000; `ExecStartPost` polls `/api/v1/health` up to 5×(3 s) |
| `wslcb-scraper.timer` | Fires twice daily at 12:30 AM and 6:30 AM Pacific, ±5 min jitter |
| `wslcb-task@.service` | Systemd template for oneshot tasks; instance name = CLI subcommand |
| `wslcb-healthcheck.service` + `.timer` | curl `/api/v1/health` every 5 min; restarts `wslcb-web` on failure |
| `wslcb-address-validation.timer` | Weekly address backfill, Sunday 2:00 AM Pacific, ±5 min jitter |

### Task service instances

```
wslcb-task@scrape
wslcb-task@refresh-addresses
wslcb-task@backfill-addresses
wslcb-task@backfill-snapshots
wslcb-task@backfill-provenance
wslcb-task@rebuild-links
```

### Sudoers rule (install once)

```bash
sudo cp infra/sudoers.d-wslcb-healthcheck /etc/sudoers.d/wslcb-healthcheck
sudo chmod 440 /etc/sudoers.d/wslcb-healthcheck
```

Grants `exedev` passwordless `sudo /usr/bin/systemctl restart wslcb-web.service`.

## After changing service files

```bash
sudo cp infra/wslcb-web.service infra/wslcb-task@.service infra/wslcb-scraper.timer \
     infra/wslcb-address-validation.timer \
     infra/wslcb-healthcheck.service infra/wslcb-healthcheck.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-address-validation.timer
sudo systemctl enable --now wslcb-healthcheck.timer
sudo systemctl restart wslcb-web.service
```

## Logging

Under systemd (non-TTY), all output is JSON lines — `timestamp`, `level`, `name`, `message`. Captured by the journal. Uvicorn access/error logs routed through the same formatter.

```bash
journalctl -u wslcb-web.service -f
journalctl -u 'wslcb-task@scrape.service' -f
```

## Address Validation

External API at `https://address-validator.exe.xyz:8000`.

- API key: `ADDRESS_VALIDATOR_API_KEY` in `/etc/wslcb-licensing-tracker/.env` (owned `root:exedev`, mode `640`)
- `ENABLE_ADDRESS_VALIDATION=true` enables DPV validation; otherwise only standardization runs
- Services load env via `EnvironmentFile=/etc/wslcb-licensing-tracker/.env`

### Common address commands

```bash
# Backfill un-processed locations
# (runs automatically after every scrape; manual invocation for catch-up only)
uv run wslcb backfill-addresses

# Re-standardize all locations
sudo systemctl start 'wslcb-task@refresh-addresses.service'
journalctl -u 'wslcb-task@refresh-addresses.service' -f
# or manually:
uv run wslcb refresh-addresses
```

## Testing

### PostgreSQL integration tests

PG integration tests require `TEST_DATABASE_URL` pointing at a test database.

```bash
TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/testdb uv run pytest tests/ -v
```

Without `TEST_DATABASE_URL` the PG test suite is **skipped** (not failed) — safe for local dev without a DB.

**In CI**, set both env vars to make missing `TEST_DATABASE_URL` a hard failure that aborts the session:

```bash
REQUIRE_PG_TESTS=1 TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/testdb uv run pytest tests/ -v
```

With `REQUIRE_PG_TESTS=1`, if `TEST_DATABASE_URL` is missing, `pytest_sessionstart` calls `pytest.exit(returncode=1)` before any collection or test execution — catching all skip paths including `@pytest.mark.skipif` and standalone connection fixtures.

## Caching

Filter dropdowns and dashboard statistics have **no in-process cache** (#99).
Every request hits the database directly.  The underlying queries are indexed
and complete in <10 ms, so the previous TTL caches were removed to eliminate
silent inconsistency when running multiple uvicorn workers.

`invalidate_filter_cache()` and `invalidate_stats_cache()` still exist as
no-ops so admin mutation call-sites compile without changes.

Worker count has no effect on data freshness — safe to scale workers freely.

## Environment

- Virtualenv at `.venv/` (managed by `uv sync`). If project directory moves, recreate.
- All persistent data in `./data/` (gitignored).

### BUILD_ID

Set automatically by `wslcb-web.service` at startup — `ExecStartPre` writes the short git SHA to `/run/wslcb-build-id`. Used for static asset cache-busting (`?v=<build_id>`) and shown in `/api/v1/health` response and page footer. Falls back to `"dev"` if unset.
