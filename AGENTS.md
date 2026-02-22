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

license_records → locations (FK: location_id, previous_location_id)
                → record_endorsements → license_endorsements
```

- **No build step.** The frontend uses Tailwind CSS via CDN and HTMX. No node_modules, no bundler.
- **Small modules.** Each `.py` file is self-contained. Aim to keep files under 300 lines where practical; `database.py` (~550 lines) and `scraper.py` (~440 lines) are larger due to query breadth and parsing logic respectively.
- **SQLite is the only datastore.** No Redis, no Postgres. WAL mode is enabled for concurrent reads.

## Key Files

| File | Purpose | Notes |
|---|---|---|
| `database.py` | Schema, queries, FTS | All DB access goes through here. `init_db()` is idempotent. Exports `DATA_DIR`, `enrich_record()`, `get_or_create_location()`. |
| `migrate_locations.py` | One-time migration | Moves inline address columns to `locations` table. Imported lazily by `init_db()`; no-op after migration completes. |
| `endorsements.py` | License type normalization | Seed code map, `process_record()`, `discover_code_mappings()`, query helpers. |
| `scraper.py` | Fetches and parses the WSLCB page | Run standalone: `python scraper.py`. Logs to `scrape_log` table. Archives source HTML. `--backfill-addresses` validates un-validated records; `--refresh-addresses` re-validates all records; `--backfill-from-snapshots` recovers ASSUMPTION and CHANGE OF LOCATION data from snapshots (`--backfill-assumptions` still accepted). |
| `address_validator.py` | Client for address validation API | Calls `https://address-validator.exe.xyz:8000`. API key in `./env` file. Graceful degradation on failure. Exports `refresh_addresses()` for full re-validation. |
| `app.py` | FastAPI web app | Runs on port 8000. Mounts `/static`, uses Jinja2 templates. Uses `@app.lifespan`. |
| `templates/` | Jinja2 HTML templates | `base.html` is the layout. `partials/results.html` is the HTMX target. |

## Database Schema

### `locations` (address normalization table)
- One row per unique raw address string from the WSLCB source
- `raw_address` (UNIQUE) — the first-seen raw string, normalized (NBSP → space)
- `city`, `state`, `zip_code` — regex-parsed from raw address at creation time
- `address_line_1` — USPS-standardized street address (e.g., `1200 WESTLAKE AVE N`)
- `address_line_2` — secondary unit designator (e.g., `STE 100`, `# A1`, `UNIT 2`); empty string if none
- `std_city` — standardized city name from the address validator
- `std_state` — standardized 2-letter state code
- `std_zip` — standardized ZIP code, may include +4 suffix (e.g., `98109-3528`)
- `address_validated_at` — ISO 8601 timestamp of when the address was validated; NULL = not yet validated
- All `std_*` / `address_line_*` columns default to empty string (not NULL) for validated records
- New records that reference an already-known raw address reuse the existing location row (no redundant API call)
- `get_or_create_location()` in `database.py` handles the upsert logic

### `license_records` (main table)
- Uniqueness constraint: `(section_type, record_date, license_number, application_type)`
- `section_type` values: `new_application`, `approved`, `discontinued`
- Dates stored as `YYYY-MM-DD` (ISO 8601) for proper sorting
- `location_id` — FK to `locations(id)` for the primary business address; NULL if no address
- `previous_location_id` — FK to `locations(id)` for the previous address (CHANGE OF LOCATION records); NULL for other types
- Address data is accessed via JOINs; `_RECORD_SELECT` in `database.py` provides the standard joined query aliasing location columns (business_location, city, std_city, etc.) for backward compatibility with templates
- `previous_business_name` — seller's business name for ASSUMPTION records; empty string for other types
- `previous_applicants` — seller's applicants for ASSUMPTION records; empty string for other types
- `applicants` field is semicolon-separated; for ASSUMPTION records this holds the buyer's applicants ("New Applicant(s)" from source)
- For ASSUMPTION records: `business_name` = buyer ("New Business Name"), `previous_business_name` = seller ("Current Business Name")
- For CHANGE OF LOCATION records: `location_id` points to the new/destination address, `previous_location_id` points to the old/origin address
- Approved-section CHANGE OF LOCATION records only have `location_id` (the source doesn't provide the previous address)
- `license_type` stores the raw value from the source page (text or numeric code); never modified
- `enrich_record()` in `database.py` adds `display_city`, `display_zip`, `display_previous_city`, `display_previous_zip` with standardized-first fallback

### `license_endorsements`
- One row per canonical endorsement name (e.g., "CANNABIS RETAILER")
- `name` is UNIQUE

### `endorsement_codes`
- Maps WSLCB numeric codes → `license_endorsements` (many-to-many)
- Composite PK `(code, endorsement_id)` — multiple codes can map to the same endorsement, and one code can expand to multiple endorsements
- Seeded from `SEED_CODE_MAP` in `endorsements.py` (71 codes); auto-discovered codes are added by `discover_code_mappings()`

### `record_endorsements`
- Junction table linking `license_records` ↔ `license_endorsements`
- Populated at ingest time by `process_record()`, not at display time
- `ON DELETE CASCADE` on both FKs (note: only effective on fresh DBs; see comment in `init_db()`)

### `license_records_fts` (FTS5 virtual table)
- Indexes: business_name, business_location, applicants, license_type, application_type, license_number, previous_business_name, previous_applicants, previous_business_location
- Uses `license_records_fts_content` VIEW as its content source — this view JOINs `license_records` → `locations` to expose `raw_address` as `business_location` / `previous_business_location` for indexing
- Kept in sync via triggers on `license_records`: AFTER INSERT and AFTER UPDATE read new values from the content view; BEFORE DELETE reads old values from the content view (must be BEFORE because the view JOIN returns nothing after the row is deleted)
- Never write to the FTS table directly
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
- ASSUMPTION records use variant field labels: `Current Business Name:`, `New Business Name:`, `Current Applicant(s):`, `New Applicant(s):` instead of the standard `Business Name:` / `Applicant(s):`
- CHANGE OF LOCATION records use `Current Business Location:` / `New Business Location:` instead of `Business Location:` (stored via `previous_location_id` / `location_id` FKs to the `locations` table)
- CHANGE OF LOCATION records in the source have a `\Application Type:` label (with leading backslash) instead of `Application Type:`
- The page carries a banner about "known data transfer issues" — expect occasional anomalies

## Data Directory

All persistent data lives under `data/`:

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
- `wslcb-task@.service` — systemd template for oneshot tasks; instance name becomes the `scraper.py` argument
  - `wslcb-task@scrape.service` — daily scrape (triggered by the timer)
  - `wslcb-task@--refresh-addresses.service` — full address re-validation
  - `wslcb-task@--backfill-addresses.service` — backfill un-validated addresses
  - `wslcb-task@--backfill-from-snapshots.service` — recover ASSUMPTION/CHANGE OF LOCATION data from archived HTML
- After changing service files: `sudo cp wslcb-web.service wslcb-task@.service wslcb-scraper.timer /etc/systemd/system/ && sudo systemctl daemon-reload`
- All persistent data lives in `./data/`
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
- Operates on the `locations` table — each unique raw address is validated once and shared across all records that reference it
- At scrape time, `validate_record()` checks if the location is already validated; skips the API call if so
- Systemd services load the env file via `EnvironmentFile=` directive
- Backfill: `python scraper.py --backfill-addresses` (processes all locations where `address_validated_at IS NULL`)
- Refresh: `python scraper.py --refresh-addresses` (re-validates all locations; safe to interrupt)

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

### Refresh all standardized addresses
```bash
sudo systemctl start 'wslcb-task@--refresh-addresses.service'
journalctl -u 'wslcb-task@--refresh-addresses.service' -f   # tail logs
```
Re-validates every location against the address-validator API. Safe to interrupt — progress is committed in batches.

Or manually:
```bash
cd /home/exedev/wslcb-licensing-tracker
source venv/bin/activate
python -u scraper.py --refresh-addresses
```

### Backfill records from archived snapshots
```bash
cd /home/exedev/wslcb-licensing-tracker
source venv/bin/activate
python -u scraper.py --backfill-from-snapshots
```
Parses all archived HTML snapshots and fixes:
1. ASSUMPTION records with empty business names (pre-fix scrape)
2. CHANGE OF LOCATION records with empty business_location/application_type or missing previous_business_location

Safe to re-run — only updates records that still have empty fields. The old `--backfill-assumptions` flag is still accepted for compatibility.

### Add a new column to `locations`
1. Add the column to the `CREATE TABLE IF NOT EXISTS locations` in `database.py`
2. Add a try/except `ALTER TABLE locations ADD COLUMN ...` migration block in `init_db()` for existing installs
3. If the column should be searchable via FTS, add it to the `license_records_fts_content` view in `_ensure_fts()`
4. If needed in display, add it to `_RECORD_SELECT` and update templates

### Add a new column to `license_records`
1. Add the column to both `CREATE TABLE IF NOT EXISTS license_records` in `database.py` and the rebuild SQL in `migrate_locations.py`
2. Add a try/except `ALTER TABLE` migration in `init_db()` for existing installs
3. Update `insert_record()`, `_RECORD_SELECT`, `search_records()`, and templates as needed

## Known Issues & Future Work

- FTS indexes raw `license_type` values — text search for endorsement names won't find approved/discontinued records that store numeric codes (the endorsement filter works correctly)
- No authentication — the app is fully public
- No rate limiting on search/export
- No requirements.txt or pyproject.toml yet
- The city extraction regex misses ~6% of records (suite info between street and city); the address validator handles these correctly
- Two source records have malformed cities (#436924: zip in city field, #078771: street name in city field); corrected manually in the locations table but corrections are overwritten by `--refresh-addresses` — needs a durable data-override mechanism
- `ON DELETE CASCADE` on endorsement FK columns only applies to fresh databases (existing DBs retain original schema; manual cleanup in `_merge_placeholders` handles this)
- 7 ASSUMPTION records (IDs 2039–2046, all from 2026-01-21) have empty `business_name` / `previous_business_name` because they were scraped before the ASSUMPTION fix and no archived snapshot covers their date range (earliest snapshot is 2026-02-20)
- Approved-section CHANGE OF LOCATION records lack `previous_location_id` because the source page only provides `Business Location:` (the new address) for approved records
- Consider adding: email/webhook alerts for new records matching saved searches
