# Deployment

Operations reference for the exe.dev VM deployment.

## Services

| Service | Purpose |
|---|---|
| `wslcb-web.service` | uvicorn on port 8000; `ExecStartPost` polls `/api/v1/health` up to 5×(3 s) |
| `wslcb-scraper.timer` | Fires twice daily at 12:30 AM and 6:30 AM Pacific, ±5 min jitter |
| `wslcb-task@.service` | Systemd template for oneshot tasks; instance name = CLI subcommand |
| `wslcb-healthcheck.service` + `.timer` | curl `/api/v1/health` every 5 min; restarts `wslcb-web` on failure |

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
sudo cp deploy/sudoers.d-wslcb-healthcheck /etc/sudoers.d/wslcb-healthcheck
sudo chmod 440 /etc/sudoers.d/wslcb-healthcheck
```

Grants `exedev` passwordless `sudo /usr/bin/systemctl restart wslcb-web.service`.

## After changing service files

```bash
sudo cp deploy/wslcb-web.service deploy/wslcb-task@.service deploy/wslcb-scraper.timer \
     deploy/wslcb-healthcheck.service deploy/wslcb-healthcheck.timer /etc/systemd/system/
sudo systemctl daemon-reload
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

- API key: `/etc/wslcb-licensing-tracker/env` (`ADDRESS_VALIDATOR_API_KEY=...`), `640 root:exedev`
- Falls back to `<project-root>/env` for local dev
- `ENABLE_ADDRESS_VALIDATION=true` in env enables DPV validation (phase 2); otherwise only standardization runs
- Services load env via `EnvironmentFile=/etc/wslcb-licensing-tracker/env`

### Common address commands

```bash
# Backfill un-processed locations
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

## Environment

- Virtualenv at `.venv/` (managed by `uv sync`). If project directory moves, recreate.
- All persistent data in `./data/` (gitignored).

### BUILD_ID

Set automatically by `wslcb-web.service` at startup — `ExecStartPre` writes the short git SHA to `/run/wslcb-build-id`. Used for static asset cache-busting (`?v=<build_id>`) and shown in `/api/v1/health` response and page footer. Falls back to `"dev"` if unset.
