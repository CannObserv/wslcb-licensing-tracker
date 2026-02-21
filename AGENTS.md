# AGENTS.md — WSLCB Licensing Tracker

Guidance for AI agents working on this project.

## Project Overview

This is a Python web application that scrapes Washington State Liquor and Cannabis Board (WSLCB) licensing activity data, stores it in SQLite, and serves a searchable web interface. The source page only shows a rolling 30-day window; this tracker builds a permanent historical archive.

**Live instance:** https://wslcb-licensing-tracker.exe.xyz:8000/
**Repo:** https://github.com/CannObserv/wslcb-licensing-tracker

## Architecture at a Glance

```
scraper.py  →  data/wslcb.db (SQLite + FTS5)  ←  app.py (FastAPI)  →  templates/ (Jinja2 + HTMX)
             ↘ data/[yyyy]/[date]-v[x]/*.html (archived snapshots)
```

- **No build step.** The frontend uses Tailwind CSS via CDN and HTMX. No node_modules, no bundler.
- **Small modules.** Each `.py` file is self-contained and ideally under 300 lines.
- **SQLite is the only datastore.** No Redis, no Postgres. WAL mode is enabled for concurrent reads.

## Key Files

| File | Purpose | Notes |
|---|---|---|
| `database.py` | Schema, migrations, queries, FTS | All DB access goes through here. `init_db()` is idempotent. Exports `DATA_DIR`. |
| `endorsements.py` | License type normalization | Seed code map, `process_record()`, `discover_code_mappings()`, query helpers. |
| `scraper.py` | Fetches and parses the WSLCB page | Run standalone: `python scraper.py`. Logs to `scrape_log` table. Archives source HTML. `--backfill-addresses` flag runs address validation backfill. |
| `address_validator.py` | Client for address validation API | Calls `https://address-validator.exe.xyz:8000`. API key in `./env` file. Graceful degradation on failure. |
| `app.py` | FastAPI web app | Runs on port 8000. Mounts `/static`, uses Jinja2 templates. Uses `@app.lifespan`. |
| `templates/` | Jinja2 HTML templates | `base.html` is the layout. `partials/results.html` is the HTMX target. |

## Database Schema

### `license_records` (main table)
- Uniqueness constraint: `(section_type, record_date, license_number, application_type)`
- `section_type` values: `new_application`, `approved`, `discontinued`
- Dates stored as `YYYY-MM-DD` (ISO 8601) for proper sorting
- `city`, `state`, `zip_code` are extracted from `business_location` at scrape time (legacy regex)
- `address_line_1` — USPS-standardized street address (e.g., `1200 WESTLAKE AVE N`)
- `address_line_2` — secondary unit designator (e.g., `STE 100`, `# A1`, `UNIT 2`); empty string if none
- `std_city` — standardized city name from the address validator
- `std_state` — standardized 2-letter state code
- `std_zip` — standardized ZIP code, may include +4 suffix (e.g., `98109-3528`)
- `address_validated_at` — ISO 8601 timestamp of when the address was validated; NULL = not yet validated
- All `std_*` / `address_line_*` columns default to empty string (not NULL) for validated records
- Queries and UI use `COALESCE(NULLIF(std_city, ''), city)` to prefer standardized data with fallback
- `applicants` field is semicolon-separated; only populated for `new_application` records
- `license_type` stores the raw value from the source page (text or numeric code); never modified

### `license_endorsements`
- One row per canonical endorsement name (e.g., "CANNABIS RETAILER")
- `name` is UNIQUE; ~96 distinct endorsement names in current data

### `endorsement_codes`
- Maps WSLCB numeric codes → `license_endorsements` (many-to-many)
- Composite PK `(code, endorsement_id)` — multiple codes can map to the same endorsement, and one code can expand to multiple endorsements
- Seeded from `SEED_CODE_MAP` in `endorsements.py` (71 codes); auto-discovered codes are added by `discover_code_mappings()`

### `record_endorsements`
- Junction table linking `license_records` ↔ `license_endorsements`
- Populated at ingest time by `process_record()`, not at display time
- `ON DELETE CASCADE` on both FKs (note: only effective on fresh DBs; see comment in `init_db()`)

### `license_records_fts` (FTS5 virtual table)
- Indexes: business_name, business_location, applicants, license_type, application_type, license_number
- Kept in sync via AFTER INSERT/UPDATE/DELETE triggers — never write to it directly
- **Known limitation:** indexes raw `license_type`, so FTS text search won't match endorsement names for records that store numeric codes. The endorsement dropdown filter works correctly (uses junction table).

### `scrape_log`
- One row per scrape run with status, record counts, timestamps, error messages
- `snapshot_path` stores the path to the archived HTML snapshot, relative to `DATA_DIR` (e.g., `2025/2025_07_09-v1/licensing info.lcb.wa.gov-2025_07_09-v1.html`); `NULL` if archiving failed

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
- `insert_record()` catches `IntegrityError` and returns `None` for skipped dupes (returns the new row id on success)
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
- License types in approved/discontinued sections appear as numeric codes (e.g., "349,") — these are resolved to text names by the endorsement normalization layer
- The page carries a banner about "known data transfer issues" — expect occasional anomalies

## Data Directory

All persistent data lives under `data/` (override with `WSLCB_DATA_DIR` env var):

```
data/
├── wslcb.db                           # SQLite database
└── [yyyy]/                            # Year directories for HTML snapshots
    └── [yyyy_mm_dd]-v[x]/             # One snapshot per scrape run (v1, v2, ... for same-day runs)
        └── licensing info.lcb.wa.gov-[yyyy_mm_dd]-v[x].html
```

- Snapshots are saved verbatim as received from the server (no transformation)
- Snapshot archiving is best-effort; failure does not abort the scrape
- The entire `data/` directory is gitignored

## Deployment

- Runs on an exe.dev VM as systemd services
- `wslcb-web.service` — uvicorn on port 8000, auto-restart
- `wslcb-scraper.timer` — fires daily at 14:00 UTC (6 AM Pacific), ±5 min jitter
- `wslcb-scraper.service` — oneshot, triggered by the timer
- After changing service files: `sudo cp *.service *.timer /etc/systemd/system/ && sudo systemctl daemon-reload`
- All persistent data lives in `./data/` (override with `WSLCB_DATA_DIR` env var)
- Venv shebangs are absolute paths — if the project directory moves, recreate the venv

## Git Workflow

- Push access via SSH deploy key (ed25519, stored at `~/.ssh/wslcb-deploy-key`)
- Remote: `git@github.com:CannObserv/wslcb-licensing-tracker.git`
- Single `main` branch for now
- Write clear commit messages; group related changes

## Address Validation

- External API at `https://address-validator.exe.xyz:8000` (FastAPI, OpenAPI docs at `/docs`)
- Authenticated via `X-API-Key` header; key stored in `./env` file (`ADDRESS_VALIDATOR_API_KEY=...`)
- `./env` file is `640 root:exedev`, gitignored
- Called at scrape time for each new record; graceful degradation if unavailable
- Systemd services load the env file via `EnvironmentFile=` directive
- Backfill: `python scraper.py --backfill-addresses` (processes all records where `address_validated_at IS NULL`)

## Common Tasks

### Run a manual scrape
```bash
cd /home/exedev/wslcb-licensing-tracker
source venv/bin/activate
python scraper.py
```

### Check scrape history
```bash
sqlite3 data/wslcb.db "SELECT id, started_at, status, records_new, records_approved, records_discontinued, records_skipped, snapshot_path FROM scrape_log ORDER BY id DESC LIMIT 10;"
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

- FTS indexes raw `license_type` values — text search for endorsement names won't find approved/discontinued records that store numeric codes (the endorsement filter works correctly)
- No authentication — the app is fully public
- No rate limiting on search/export
- No requirements.txt or pyproject.toml yet
- The city extraction regex misses ~6% of records (suite info between street and city); the address validator handles these correctly
- A handful of validator edge cases remain (parenthetical annotations in addresses, zip codes in city field)
- `ON DELETE CASCADE` on endorsement FK columns only applies to fresh databases (existing DBs retain original schema; manual cleanup in `_merge_placeholders` handles this)
- Consider adding: email/webhook alerts for new records matching saved searches
