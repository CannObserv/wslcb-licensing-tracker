# AGENTS.md — WSLCB Licensing Tracker

Guidance for AI agents working on this project.

## Project Overview

This is a Python web application that scrapes Washington State Liquor and Cannabis Board (WSLCB) licensing activity data, stores it in SQLite, and serves a searchable web interface. The source page only shows a rolling 30-day window; this tracker builds a permanent historical archive.

**Live instance:** https://wslcb-licensing-tracker.exe.xyz:8000/
**Repo:** https://github.com/CannObserv/wslcb-licensing-tracker

## Architecture at a Glance

```
scraper.py  →  data/wslcb.db (SQLite + FTS5)  ←  app.py (FastAPI)  →  templates/ (Jinja2 + HTMX)
             ↘ data/wslcb/licensinginfo/[yyyy]/[date]/*.html (archived snapshots)

license_records → locations (FK: location_id, previous_location_id)
                → record_endorsements → license_endorsements
```

- **No build step.** The frontend uses Tailwind CSS via CDN and HTMX. No node_modules, no bundler.
- **Small modules.** The DB layer is split into `database.py` (core schema/connections), `entities.py` (applicant normalization), and `queries.py` (search/CRUD). Dependencies flow one-way: `queries → database, entities, endorsements`.
- **SQLite is the only datastore.** No Redis, no Postgres. WAL mode is enabled for concurrent reads.

## Key Files

| File | Purpose | Notes |
|---|---|---|
| `database.py` | Schema, connections, FTS | Core DB layer. `init_db()` is idempotent. Exports `DATA_DIR`, `get_or_create_location()`. |
| `entities.py` | Entity (applicant) normalization | `get_or_create_entity()`, `backfill_entities()`, `get_record_entities()`, `get_entity_by_id()`, `merge_duplicate_entities()`, `clean_applicants_string()`, `clean_record_strings()`, `parse_and_link_entities()`. |
| `queries.py` | Record queries and CRUD | `search_records()`, `export_records()`, `get_filter_options()`, `get_cities_for_state()`, `get_stats()`, `insert_record()`, `enrich_record()`, `hydrate_records()`, `get_record_by_id()`, `get_related_records()`, `get_entity_records()`. |
| `migrate_locations.py` | One-time migration | Moves inline address columns to `locations` table. Imported lazily by `init_db()`; no-op after migration completes. |
| `endorsements.py` | License type normalization | Seed code map (98 codes), `process_record()`, `discover_code_mappings()`, `repair_code_name_endorsements()`, query helpers. |
| `log_config.py` | Centralized logging setup | `setup_logging()` configures root logger; auto-detects TTY vs JSON format. Called once per entry point. |
| `scraper.py` | Fetches and parses the WSLCB page | Run standalone: `python scraper.py`. Logs to `scrape_log` table. Archives source HTML. `--backfill-addresses` validates un-validated records; `--refresh-addresses` re-validates all records; `--backfill-from-snapshots` delegates to `backfill_snapshots.py` (`--backfill-assumptions` still accepted). |
| `backfill_snapshots.py` | Ingest + repair from archived snapshots | Two-phase: (1) insert new records from all snapshots, (2) repair broken ASSUMPTION/CHANGE OF LOCATION records. Safe to re-run. Address validation deferred to `--backfill-addresses`. |
| `address_validator.py` | Client for address validation API | Calls `https://address-validator.exe.xyz:8000`. API key in `./env` file. Graceful degradation on failure. Exports `refresh_addresses()` for full re-validation. |
| `app.py` | FastAPI web app | Runs on port 8000. Mounts `/static`, uses Jinja2 templates. Uses `@app.lifespan`. |
| `templates/` | Jinja2 HTML templates | `base.html` is the layout (includes Tailwind config with brand colors). `partials/results.html` is the HTMX target. `partials/record_table.html` is the shared record table (used by results and entity pages). `404.html` handles not-found errors. |
| `link_records.py` | Application→outcome record linking | Bidirectional nearest-neighbor matching with ±7-day tolerance. `build_all_links()`, `link_new_record()`, `get_outcome_status()`, `get_reverse_link_info()`, `outcome_filter_sql()`. |
| `backfill_diffs.py` | Ingest from CO diff archives | Parses unified-diff files in `data/wslcb/licensinginfo-diffs/{notifications,approvals,discontinued}/`. Safe to re-run. |
| `backfill_provenance.py` | One-time provenance backfill | Re-processes all snapshots to populate `record_sources` junction links for existing records. Safe to re-run. |
| `templates/entity.html` | Entity detail page | Shows all records for a person or organization, with type badge and license count. |
| `static/images/` | Cannabis Observer brand assets | `cannabis_observer-icon-square.svg` (icon) and `cannabis_observer-name.svg` (wordmark). See **Style Guide** for usage. |

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
- Address data is accessed via JOINs; `_RECORD_SELECT` in `queries.py` provides the standard joined query aliasing location columns (business_location, city, std_city, etc.) for backward compatibility with templates
- `previous_business_name` — seller's business name for ASSUMPTION records; empty string for other types
- `previous_applicants` — seller's applicants for ASSUMPTION records; empty string for other types
- `applicants` field is semicolon-separated; for ASSUMPTION records this holds the buyer's applicants ("New Applicant(s)" from source)
- For ASSUMPTION records: `business_name` = buyer ("New Business Name"), `previous_business_name` = seller ("Current Business Name")
- For CHANGE OF LOCATION records: `location_id` points to the new/destination address, `previous_location_id` points to the old/origin address
- Approved-section CHANGE OF LOCATION records only have `location_id` (the source doesn't provide the previous address)
- `license_type` stores the raw value from the source page (text or numeric code); never modified
- `enrich_record()` in `queries.py` adds `display_city`, `display_zip`, `display_previous_city`, `display_previous_zip` with standardized-first fallback

### `license_endorsements`
- One row per canonical endorsement name (e.g., "CANNABIS RETAILER")
- `name` is UNIQUE

### `endorsement_codes`
- Maps WSLCB numeric codes → `license_endorsements` (many-to-many)
- Composite PK `(code, endorsement_id)` — multiple codes can map to the same endorsement, and one code can expand to multiple endorsements
- Seeded from `SEED_CODE_MAP` in `endorsements.py` (98 codes); auto-discovered codes are added by `discover_code_mappings()`

### `record_endorsements`
- Junction table linking `license_records` ↔ `license_endorsements`
- Populated at ingest time by `process_record()`, not at display time
- `ON DELETE CASCADE` on both FKs (note: only effective on fresh DBs; see comment in `init_db()`)

### `entities` (applicant normalization table)
- One row per unique applicant name (person or organization)
- `name` (UNIQUE) — the normalized/cleaned name (uppercased, stray trailing punctuation stripped)
- `entity_type` — `'person'`, `'organization'`, or `''` (unknown); classified by heuristic at creation time
- The first element of the semicolon-delimited `applicants` field (which equals `business_name`) is **excluded** — only the individual people/orgs behind the license are stored
- `get_or_create_entity()` in `entities.py` normalizes names via `clean_entity_name()`: uppercase, strip whitespace, and remove stray trailing punctuation (periods, commas) that isn't part of a recognized suffix
- The `_LEGIT_TRAILING_DOT` regex in `entities.py` defines the suffix allowlist — add new entries there when the WSLCB source uses a new legitimate abbreviation ending with a period.  Current list: `INC`, `LLC`, `L.L.C`, `L.L.P`, `LTD`, `CORP`, `CO`, `L.P`, `PTY`, `JR`, `SR`, `S.P.A`, `F.O.E`, `U.P`, `D.B.A`, `P.C`, `N.A`, `P.A`, `W. & S`
- `clean_applicants_string()` applies the same cleaning to each element of a semicolon-delimited applicants string — used at ingest time so the `applicants`/`previous_applicants` columns on `license_records` stay consistent with entity names
- `insert_record()` in `queries.py` also cleans `business_name` and `previous_business_name` via `clean_entity_name()` before storage, so all name columns are consistently uppercased and stripped of stray punctuation
- `merge_duplicate_entities()` runs at web app startup (via `backfill_entities()` in the `app.py` lifespan) — cleans `business_name`, `previous_business_name`, `applicants`, and `previous_applicants` in `license_records` via `clean_record_strings()`, then merges duplicate entities and renames dirty ones in place; all work is committed in a single transaction

### `record_entities` (junction table)
- Links `license_records` ↔ `entities` with role and position
- `role` — `'applicant'` or `'previous_applicant'` (for ASSUMPTION seller applicants)
- `position` — 0-indexed ordering from the source document (after the business name)
- Composite PK `(record_id, entity_id, role)`
- Populated at ingest time by `_parse_and_link_entities()`; backfilled for existing data on startup
- `ON DELETE CASCADE` on both FKs

### `record_links` (application→outcome linking)
- Links new_application records to their corresponding approved or discontinued outcome records
- `new_app_id` — FK to `license_records(id)`, the new_application record
- `outcome_id` — FK to `license_records(id)`, the approved or discontinued record
- `confidence` — `'high'` (mutual match) or `'medium'` (forward-only match); CHECK constraint also allows `'low'` (reserved for future use, not currently produced)
- `days_gap` — `outcome_date - new_app_date` in days (can be negative when outcome precedes notification)
- `linked_at` — ISO 8601 timestamp of when the link was created
- UNIQUE on `(new_app_id, outcome_id)` — prevents duplicate links
- Indexed on both `new_app_id` and `outcome_id` for fast lookups from either direction
- Rebuilt from scratch by `build_all_links()` in `link_records.py`; incrementally updated by `link_new_record()` during scraping
- `DATE_TOLERANCE_DAYS = 7` — the ±7-day window handles outcome-before-notification date patterns
- Approval linking: `new_application` → `approved` with same `application_type` (RENEWAL, NEW APPLICATION, ASSUMPTION, etc.)
- Discontinuance linking: `new_application/DISC. LIQUOR SALES` → `discontinued/DISCONTINUED`
- `PENDING_CUTOFF_DAYS = 180` — unlinked applications older than this are classified as "unknown" instead of "pending"
- `DATA_GAP_CUTOFF = '2025-05-12'` — post-gap NEW APPLICATION records get "data_gap" status (WSLCB stopped publishing these approvals)
- `ON DELETE CASCADE` on both FKs

### `license_records_fts` (FTS5 virtual table)
- Indexes: business_name, business_location, applicants, license_type, application_type, license_number, previous_business_name, previous_applicants, previous_business_location
- Uses `license_records_fts_content` VIEW as its content source — this view JOINs `license_records` → `locations` to expose `raw_address` as `business_location` / `previous_business_location` for indexing
- Kept in sync via triggers on `license_records`: AFTER INSERT inserts new values; updates use a BEFORE UPDATE / AFTER UPDATE pair (delete old, insert new); BEFORE DELETE removes old values. All read from the content view
- Never write to the FTS table directly
- **Known limitation:** indexes raw `license_type`, so FTS text search won't match endorsement names for records that store numeric codes. The endorsement dropdown filter works correctly (uses junction table).

### `scrape_log`
- One row per scrape run with status, record counts, timestamps, error messages
- `snapshot_path` stores the path to the archived HTML snapshot, relative to `DATA_DIR` (e.g., `wslcb/licensinginfo/2025/2025_07_09/2025_07_09-licensinginfo.lcb.wa.gov-v1.html`); `NULL` if archiving failed

### `source_types` (provenance enum)
- Fixed-ID reference table: `1=live_scrape`, `2=co_archive`, `3=internet_archive`, `4=co_diff_archive`, `5=manual`
- Python constants in `database.py`: `SOURCE_TYPE_LIVE_SCRAPE`, etc.
- Seeded by `init_db()` via `INSERT OR IGNORE`

### `sources` (provenance artifacts)
- One row per source artifact (a specific HTML snapshot file or scrape run)
- `source_type_id` — FK to `source_types`
- `snapshot_path` — repo-relative path to archived HTML file; `NULL` if archiving failed
- `url` — original URL (WSLCB page, Wayback Machine URL)
- `captured_at` — when the source was captured (distinct from ingestion)
- `ingested_at` — when we processed it
- `scrape_log_id` — FK to `scrape_log` for live scrapes (avoids duplicating operational data)
- `metadata` — JSON blob for source-specific attributes (`truncated`, `file_size_bytes`, `sections_present`, `sha256`, `wayback_timestamp`)
- UNIQUE constraint on `(source_type_id, snapshot_path)`
- `get_or_create_source()` in `database.py` handles idempotent upsert

### `record_sources` (provenance junction)
- M:M junction linking `license_records` ↔ `sources`
- `role` — `'first_seen'` (introduced by this source), `'confirmed'` (already existed, corroborated), `'repaired'` (data fixed from this source); enforced by CHECK constraint
- Composite PK `(record_id, source_id, role)` — a record can have multiple roles for the same source (e.g., `first_seen` + `repaired`)
- `link_record_source()` in `database.py` handles idempotent insert
- `ON DELETE CASCADE` on both FKs
- `get_record_sources()` in `queries.py` returns provenance for display on detail page

## Conventions

### Python
- Python 3.12+ with venv at `./venv/`
- Dependencies: `fastapi uvicorn jinja2 httpx beautifulsoup4 lxml python-multipart python-json-logger`
- No requirements.txt yet — add one if dependencies grow
- Use `datetime.now(timezone.utc)` not `datetime.utcnow()` (deprecated)
- Module docstrings on every `.py` file

### Logging
- **Never use `print()` for operational output.** All logging goes through Python's `logging` module.
- Each module declares `logger = logging.getLogger(__name__)` at the top.
- Entry points (`app.py` lifespan, `scraper.py` main, `backfill_snapshots.py` main, `database.py` main) call `setup_logging()` from `log_config.py` before doing any work.
- Log levels:
  - `logger.debug()` — progress counters, verbose operational detail ("Fetched 12,000,000 bytes", "Found 3 data sections")
  - `logger.info()` — meaningful events (records inserted, scrape complete, migrations, summaries)
  - `logger.warning()` — recoverable issues (API timeouts, failed validations, missing config)
  - `logger.error()` — unrecoverable failures (scrape crash, missing API key)
- Format auto-detects: human-readable on TTY, JSON lines under systemd/pipe (via `python-json-logger`).
- Use `%s`/`%d` style formatting in log calls (not f-strings) so messages are only formatted if the level is enabled.

### Templates
- Tailwind CSS via CDN (`<script src="https://cdn.tailwindcss.com">`) with custom `tailwind.config` in `base.html`
- HTMX for partial page updates on search — the `/search` endpoint detects `HX-Request` header
- Custom Jinja2 filters registered in `app.py`: `section_label`, `phone_format`
- See **Style Guide** below for colors, branding, and component conventions

### Style Guide

This project is branded as a **Cannabis Observer** project. The visual identity draws from the Cannabis Observer SVG assets and uses a consistent color palette across all templates.

#### Brand Assets

| File | Purpose |
|---|---|
| `static/images/cannabis_observer-icon-square.svg` | Square icon (510×510); used in navbar (32×32) and footer (16×16) |
| `static/images/cannabis_observer-name.svg` | Stylized "cannabis OBSERVER" wordmark (wide aspect ratio); available for future use |

The emoji triad 🌱🏛️🔍 (seedling + classical building + magnifying glass) represents Cannabis Observer's mission and appears in the footer.

#### Color Palette

Custom colors are defined in the Tailwind config block in `base.html`:

| Token | Hex | Usage |
|---|---|---|
| `co-green` | `#8cbe69` | Cannabis Observer brand green (from SVG). Reserved for future emphasis; not currently used as a UI accent |
| `co-purple` (DEFAULT/600) | `#6d4488` | **Primary UI accent.** Buttons, links, active pagination, detail-page highlight panels, entity links |
| `co-purple-50` | `#f5f0f8` | Highlight panel backgrounds (Assumption "Buyer", Change of Location "New Location") |
| `co-purple-100` | `#ebe1f1` | Highlight panel borders |
| `co-purple-700` | `#5a3870` | Hover states for buttons and links |
| `co-purple-800` | `#472c59` | Available for darker emphasis if needed |

The favicon uses bright green `#17de6b` (a vibrant accent distinct from the brand palette).

#### Record-Type Badges (semantic — do NOT change to brand colors)

| Class | Background | Text | Used for |
|---|---|---|---|
| `.badge-new` | `#dbeafe` (blue-100) | `#1e40af` (blue-800) | New Application records |
| `.badge-approved` | `#dcfce7` (green-100) | `#166534` (green-800) | Approved records |
| `.badge-discontinued` | `#fee2e2` (red-100) | `#991b1b` (red-800) | Discontinued records |

These use fixed hex values (not Tailwind classes) and serve a **semantic** purpose — they should remain visually distinct from the brand accent.

#### Dashboard Stat Card Colors (semantic — do NOT change to brand colors)

The stat cards on the dashboard (`index.html`) use Tailwind semantic colors matching the record-type badges:

- **New Applications**: `border-blue-200`, `text-blue-600`/`text-blue-700`
- **Approved**: `border-green-200`, `text-green-600`/`text-green-700`
- **Discontinued**: `border-red-200`, `text-red-600`/`text-red-700`
- **Neutral cards** (Total Records, Unique Businesses, etc.): `border-gray-200`, `text-gray-900`

#### Component Conventions

- **Buttons** (Search, submit): `bg-co-purple text-white hover:bg-co-purple-700`
- **Text links** (back links, Export CSV): `text-co-purple hover:text-co-purple-700`
- **Entity name links** (detail page): `text-co-purple hover:text-co-purple-700 hover:underline`
- **Focus rings** on inputs: `focus:border-co-purple focus:ring-1 focus:ring-co-purple`
- **Active pagination page**: `bg-co-purple text-white border-co-purple`
- **Detail page accent panels** ("Buyer (New) →", "New Location →"): `bg-co-purple-50 border-co-purple-100` with `text-co-purple` header
- **Entity type badges**: `bg-co-purple-50 text-co-purple` for Organization, `bg-amber-100 text-amber-800` for Person
- **Provenance source badges** (detail page, `detail.html`):
  - Live Scrape: `bg-green-50 text-green-700 border-green-200` (semantic: "live/active")
  - CO Archive / CO Diff Archive: `bg-co-purple-50 text-co-purple border-co-purple-100` (brand)
  - Internet Archive: `bg-amber-50 text-amber-700 border-amber-200`
  - Repaired: `bg-sky-50 text-sky-700 border-sky-200` (distinct from Internet Archive amber)
- **Provenance display**: collapsed summary badges with count (e.g., "⚡ Live Scrape ×13") + date range; expandable `<details>` for individual sources when >3
- **Search filter grid**: `grid-cols-1 md:grid-cols-3 lg:grid-cols-[repeat(auto-fill,minmax(9rem,1fr))]` — auto-fill at desktop so items expand when the conditional city filter is hidden; explicit 1-col and 3-col at mobile/tablet
- **Search button**: always bottom-right of the filter card (alongside "Clear filters" link at bottom-left), not inline with the text input
- **Navbar**: Cannabis Observer icon (32×32) + bold site title; nav links use `hover:text-co-purple-700`
- **Footer**: two lines — (1) "A project of [icon] Cannabis Observer 🌱🏛️🔍" linked to `https://cannabis.observer/`, (2) WSLCB data source attribution

### Data Integrity
- The UNIQUE constraint prevents duplicate records across daily scrapes
- `insert_record()` checks for duplicates before creating location rows (avoiding orphans), with `IntegrityError` as a safety net; returns `(id, True)` for new records, `(id, False)` for existing duplicates, `None` only on unexpected `IntegrityError`
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
- Historical data (pre-2025) used "CODE, NAME" format (e.g., "450, GROCERY STORE - BEER/WINE") instead of bare codes; `process_record()` handles both formats
- ASSUMPTION records use variant field labels: `Current Business Name:`, `New Business Name:`, `Current Applicant(s):`, `New Applicant(s):` instead of the standard `Business Name:` / `Applicant(s):`
- CHANGE OF LOCATION records use `Current Business Location:` / `New Business Location:` instead of `Business Location:` (stored via `previous_location_id` / `location_id` FKs to the `locations` table)
- CHANGE OF LOCATION records in the source have a `\Application Type:` label (with leading backslash) instead of `Application Type:`
- The page carries a banner about "known data transfer issues" — expect occasional anomalies

## Data Directory

All persistent data lives under `data/`:

```
data/
├── wslcb.db                           # SQLite database
└── wslcb/
    ├── licensinginfo/                 # HTML snapshots from licensinginfo.lcb.wa.gov
    │   └── [yyyy]/                    # Year directories
    │       └── [yyyy_mm_dd]/          # Date directories (multiple versions for same-day runs)
    │           └── [yyyy_mm_dd]-licensinginfo.lcb.wa.gov-v[x].html
    └── licensinginfo-diffs/           # CO diff archive files
        ├── notifications/             # Unified diffs of the notifications section
        ├── approvals/                 # Unified diffs of the approvals section
        ├── discontinued/              # Unified diffs of the discontinued section
        └── *.csv                      # CSV exports from backfill_diffs.py runs
```

- Snapshots are saved verbatim as received from the server (no transformation)
- Snapshot archiving is best-effort; failure does not abort the scrape
- Multiple same-day scrapes produce v1, v2, etc. files in the same date directory
- The entire `data/` directory is gitignored

## Deployment

- Runs on an exe.dev VM as systemd services
- `wslcb-web.service` — uvicorn on port 8000, auto-restart
- `wslcb-scraper.timer` — fires twice daily at 12:30 AM and 6:30 AM Pacific, ±5 min jitter
- `wslcb-task@.service` — systemd template for oneshot tasks; instance name becomes the `scraper.py` argument
  - `wslcb-task@scrape.service` — scrape (triggered by the timer)
  - `wslcb-task@--refresh-addresses.service` — full address re-validation
  - `wslcb-task@--backfill-addresses.service` — backfill un-validated addresses
  - `wslcb-task@--backfill-from-snapshots.service` — recover ASSUMPTION/CHANGE OF LOCATION data from archived HTML
- After changing service files: `sudo cp wslcb-web.service wslcb-task@.service wslcb-scraper.timer /etc/systemd/system/ && sudo systemctl daemon-reload`
- Under systemd (non-TTY), all log output is JSON lines — structured fields (`timestamp`, `level`, `name`, `message`) are captured by the journal. Uvicorn access/error logs are routed through the same formatter.
- All persistent data lives in `./data/`
- Venv shebangs are absolute paths — if the project directory moves, recreate the venv

## Git Workflow

- Push access via SSH deploy key (ed25519, stored at `~/.ssh/wslcb-deploy-key`)
- Remote: `git@github.com:CannObserv/wslcb-licensing-tracker.git`
- Single `main` branch for now
- Write clear commit messages; group related changes
- **When implementing a GitHub issue**, prefix every commit message with `#<number>: ` (e.g., `#1: Implement structured logging`). GitHub auto-links these to the issue. Include `(closes #<number>)` in the final commit of the series to auto-close the issue on push.

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
python -u backfill_snapshots.py
```
Two-phase process:
1. **Ingest** — insert new records from all archived HTML snapshots (duplicates skipped)
2. **Repair** — fix broken ASSUMPTION records (empty business names) and CHANGE OF LOCATION records (missing locations)

Safe to re-run. Address validation is deferred; run `--backfill-addresses` afterward.
Also available via `python scraper.py --backfill-from-snapshots` (delegates to `backfill_snapshots.py`; `--backfill-assumptions` still accepted for compatibility).

### Rebuild application→outcome links
```bash
cd /home/exedev/wslcb-licensing-tracker
source venv/bin/activate
python scraper.py --rebuild-links
```
Clears and rebuilds all `record_links` from scratch. Safe to run at any time (~85 seconds on current dataset). Links are also built incrementally during scraping and on first web app startup (if table is empty).

### Add a new column to `locations`
1. Add the column to the `CREATE TABLE IF NOT EXISTS locations` in `database.py`
2. Add a try/except `ALTER TABLE locations ADD COLUMN ...` migration block in `init_db()` for existing installs
3. If the column should be searchable via FTS, add it to the `license_records_fts_content` view in `_ensure_fts()` (in `database.py`)
4. If needed in display, add it to `RECORD_COLUMNS` in `queries.py` and update templates

### Add a new column to `license_records`
1. Add the column to both `CREATE TABLE IF NOT EXISTS license_records` in `database.py` and the rebuild SQL in `migrate_locations.py`
2. Add a try/except `ALTER TABLE` migration in `init_db()` for existing installs
3. Update `insert_record()` and `RECORD_COLUMNS` in `queries.py`, `search_records()`, and templates as needed

## Known Issues & Future Work

- Non-standard state values exist in `locations.state` from regex parsing errors (e.g., `SR WA`, `TERMINAL WA`); all validated `std_state` values resolve to valid US state codes. The state filter only shows valid US state codes (validated against `US_STATES` in `queries.py`)
- FTS indexes raw `license_type` values — text search for endorsement names won't find approved/discontinued records that store numeric codes (the endorsement filter works correctly)
- No authentication — the app is fully public
- No rate limiting on search/export
- No requirements.txt or pyproject.toml yet
- The city extraction regex misses ~6% of records (suite info between street and city); the address validator handles these correctly
- Two source records have malformed cities (#436924: zip in city field, #078771: street name in city field); corrected manually in the locations table but corrections are overwritten by `--refresh-addresses` — needs a durable data-override mechanism
- `ON DELETE CASCADE` on endorsement FK columns only applies to fresh databases (existing DBs retain original schema; manual cleanup in `_merge_placeholders` handles this)
- The `applicants` and `previous_applicants` string columns on `license_records` are retained for FTS indexing and CSV export; values are cleaned at ingest time (alongside `business_name` and `previous_business_name`) to stay consistent with entity names. Removal is deferred to a future phase
- Approved-section CHANGE OF LOCATION records lack `previous_location_id` because the source page only provides `Business Location:` (the new address) for approved records
- `search_records()` runs separate COUNT and SELECT queries with the same WHERE clause; could use `COUNT(*) OVER()` window function (fine at current scale)
- CSV export (`/export`) loads up to 100K rows into memory with no streaming; acceptable for current dataset size
- `search_records()` and `export_records()` share filter logic via `_build_where_clause()` — when adding a new filter parameter, update the shared helper
- Consider adding: email/webhook alerts for new records matching saved searches
