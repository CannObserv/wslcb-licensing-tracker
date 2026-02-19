# AGENTS.md — WSLCB Licensing Tracker

Guidance for AI agents working on this project.

## Project Overview

This is a Python web application that scrapes Washington State Liquor and Cannabis Board (WSLCB) licensing activity data, stores it in SQLite, and serves a searchable web interface. The source page only shows a rolling 30-day window; this tracker builds a permanent historical archive.

**Live instance:** https://wslcb-licensing-tracker.exe.xyz:8000/
**Repo:** https://github.com/CannObserv/wslcb-licensing-tracker

## Architecture at a Glance

```
scraper.py  →  wslcb.db (SQLite + FTS5)  ←  app.py (FastAPI)  →  templates/ (Jinja2 + HTMX)
```

- **No build step.** The frontend uses Tailwind CSS via CDN and HTMX. No node_modules, no bundler.
- **Single-file modules.** Each `.py` file is self-contained and under 250 lines. Keep it that way.
- **SQLite is the only datastore.** No Redis, no Postgres. WAL mode is enabled for concurrent reads.

## Key Files

| File | Purpose | Notes |
|---|---|---|
| `database.py` | Schema, migrations, queries, FTS | All DB access goes through here. `init_db()` is idempotent. |
| `scraper.py` | Fetches and parses the WSLCB page | Run standalone: `python scraper.py`. Logs to `scrape_log` table. |
| `app.py` | FastAPI web app | Runs on port 8000. Mounts `/static`, uses Jinja2 templates. |
| `templates/` | Jinja2 HTML templates | `base.html` is the layout. `partials/results.html` is the HTMX target. |

## Database Schema

### `license_records` (main table)
- Uniqueness constraint: `(section_type, record_date, license_number, application_type)`
- `section_type` values: `new_application`, `approved`, `discontinued`
- Dates stored as `YYYY-MM-DD` (ISO 8601) for proper sorting
- `city`, `state`, `zip_code` are extracted from `business_location` at scrape time
- `applicants` field is semicolon-separated; only populated for `new_application` records

### `license_records_fts` (FTS5 virtual table)
- Indexes: business_name, business_location, applicants, license_type, application_type, license_number
- Kept in sync via AFTER INSERT/UPDATE/DELETE triggers — never write to it directly

### `scrape_log`
- One row per scrape run with status, record counts, timestamps, error messages

## Conventions

### Python
- Python 3.12+ with venv at `./venv/`
- Dependencies: `fastapi uvicorn jinja2 httpx beautifulsoup4 lxml python-multipart`
- No requirements.txt yet — add one if dependencies grow
- Use `datetime.now(timezone.utc)` not `datetime.utcnow()` (deprecated)
- Module docstrings on every `.py` file

### Templates
- Tailwind CSS via CDN (`<script src="https://cdn.tailwindcss.com">`)
- HTMX for partial page updates on search — the `/search` endpoint detects `HX-Request` header
- Custom Jinja2 filters registered in `app.py`: `section_label`, `phone_format`
- Color-coded badges: `.badge-new` (blue), `.badge-approved` (green), `.badge-discontinued` (red)

### Data Integrity
- The UNIQUE constraint prevents duplicate records across daily scrapes
- `insert_record()` catches `IntegrityError` and returns `False` for skipped dupes
- The source page contains duplicates within itself (especially in approved/discontinued sections); this is expected
- Never delete historical data — the whole point is accumulating beyond the 30-day window

## The Source Page

URL: `https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp`

- Classic ASP page, ~12 MB of HTML
- Three sections, each a `<table>` with a `<th>` header containing "STATEWIDE ..."
- Records are key-value pairs in `<tr>` rows with 2 `<td>` cells (label + value)
- Records are separated by blank rows (2 cells, whitespace only)
- The date field label differs per section: "Notification Date:", "Approved Date:", "Discontinued Date:"
- New applications include an "Applicant(s):" field; approved/discontinued do not
- License types in approved/discontinued sections may appear as numeric codes (e.g., "349,") rather than names — this is a known data quality issue from the source
- The page carries a banner about "known data transfer issues" — expect occasional anomalies

## Deployment

- Runs on an exe.dev VM as systemd services
- `wslcb-web.service` — uvicorn on port 8000, auto-restart
- `wslcb-scraper.timer` — fires daily at 14:00 UTC (6 AM Pacific), ±5 min jitter
- `wslcb-scraper.service` — oneshot, triggered by the timer
- After changing service files: `sudo cp *.service *.timer /etc/systemd/system/ && sudo systemctl daemon-reload`
- DB file lives at `./wslcb.db` (override with `WSLCB_DB_PATH` env var)
- Venv shebangs are absolute paths — if the project directory moves, recreate the venv

## Git Workflow

- Push access via SSH deploy key (ed25519, stored at `~/.ssh/wslcb-deploy-key`)
- Remote: `git@github.com:CannObserv/wslcb-licensing-tracker.git`
- Single `main` branch for now
- Write clear commit messages; group related changes

## Common Tasks

### Run a manual scrape
```bash
cd /home/exedev/wslcb-licensing-tracker
source venv/bin/activate
python scraper.py
```

### Check scrape history
```bash
sqlite3 wslcb.db "SELECT id, started_at, status, records_new, records_approved, records_discontinued, records_skipped FROM scrape_log ORDER BY id DESC LIMIT 10;"
```

### Restart the web app after code changes
```bash
sudo systemctl restart wslcb-web.service
```

### Add a new database column
1. Add the column to the `CREATE TABLE` in `database.py` (for fresh installs)
2. Add an `ALTER TABLE` migration in `init_db()` wrapped in a try/except (for existing installs)
3. Update `insert_record()`, `search_records()`, and templates as needed

## Known Issues & Future Work

- License types for approved/discontinued records sometimes appear as numeric codes rather than human-readable names — could be mapped with a lookup table
- No authentication — the app is fully public
- No rate limiting on search/export
- No requirements.txt or pyproject.toml yet
- The city extraction regex may miss edge cases in business_location formatting
- Consider adding: email/webhook alerts for new records matching saved searches
