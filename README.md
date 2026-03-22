# WSLCB Licensing Tracker

A web application that scrapes, archives, and provides searchable access to licensing activity published by the **Washington State Liquor and Cannabis Board (WSLCB)**.

The Board publishes a [rolling 30-day report](https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp) of new license applications, approvals, and discontinuances for alcohol, cannabis, tobacco, and vapor product businesses across Washington State. This tracker scrapes that report twice daily and preserves the data in a searchable database, building a historical archive that extends beyond the 30-day window.

A project of [Cannabis Observer](https://cannabis.observer/) 🌱🏛️🔍

**Live instance:** [https://wslcb-licensing-tracker.exe.xyz:8000/](https://wslcb-licensing-tracker.exe.xyz:8000/)

## Features

- **Twice-daily automated scraping** of the WSLCB statewide licensing activity page
- **Full-text search** across business names, locations, applicants, license types, and license numbers
- **Normalized endorsements** — numeric license codes from approved/discontinued records are resolved to human-readable names via a managed code→endorsement mapping
- **Address standardization** — raw business addresses are parsed into structured components (street, suite, city, state, ZIP) via an external validation API, fixing ~6% of records with mis-parsed cities
- **Filterable results** by record type, application type, endorsement, state, city, and date range
- **Record detail pages** with related records for the same license number
- **CSV export** of any search result set
- **Historical archive** — the source only shows 30 days, but the database retains all data
- **License transfer tracking** — ASSUMPTION records capture both seller and buyer business names and applicants
- **Location change tracking** — CHANGE OF LOCATION records capture both previous and new business addresses
- **Entity normalization** — applicant names (people and organizations) are extracted into a shared `entities` table with name cleaning (uppercasing, stray punctuation removal), enabling cross-license analysis (e.g., "show all licenses for person X")
- **Deduplication** — safe to re-scrape; duplicate records are automatically skipped
- **Content hash detection** — scrapes are skipped entirely when the page hasn't changed (common on weekends), avoiding redundant parsing, snapshot files, and provenance noise

## Data

The tracker captures three categories of licensing activity:

| Section | Description |
|---|---|
| **New Applications** | Businesses that have submitted new license applications, renewals, changes of location, tradename changes, corporate officer changes, and other application types |
| **Approved** | Licenses recently approved by the Board |
| **Discontinued** | Licenses that have been discontinued |

Each record includes:

| Field | Description |
|---|---|
| Date | Notification, approval, or discontinuance date |
| Business Name | Registered business name (for ASSUMPTION records, the buyer's business name) |
| Business Location | Full street address including city, state, and ZIP (raw and standardized components) |
| Applicant(s) | Named applicants (new applications only; for ASSUMPTION records, the buyer's applicants) |
| Previous Business Name | Seller's business name (ASSUMPTION records only) |
| Previous Applicant(s) | Seller's applicants (ASSUMPTION records only) |
| Previous Location | Origin address before relocation (CHANGE OF LOCATION records only, new applications section) |
| Endorsements | One or more license/endorsement types (e.g., "CANNABIS RETAILER", "GROCERY STORE - BEER/WINE"), normalized from text names or numeric WSLCB codes |
| Application Type | RENEWAL, NEW APPLICATION, CHANGE OF LOCATION, ASSUMPTION, DISCONTINUED, etc. |
| License Number | WSLCB license number |
| Contact Phone | Business contact phone number |

## Architecture

| Component | Technology |
|---|---|
| Scraper | Python, [httpx](https://www.python-httpx.org/), [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/) + lxml |
| Address validation | External API ([address-validator](https://address-validator.exe.xyz:8000/docs)) for USPS-standardized parsing |
| Database | [PostgreSQL](https://www.postgresql.org/) with [tsvector](https://www.postgresql.org/docs/current/datatype-textsearch.html) + [pg_trgm](https://www.postgresql.org/docs/current/pgtrgm.html) full-text search |
| Web framework | [FastAPI](https://fastapi.tiangolo.com/) with [Jinja2](https://jinja.palletsprojects.com/) templates |
| Frontend | Server-rendered HTML, [HTMX](https://htmx.org/), [Tailwind CSS](https://tailwindcss.com/) (pre-built CLI, custom brand palette) |
| Scheduling | systemd timer (twice-daily) |

## Logging

All modules use Python’s `logging` module via a centralized configuration in `log_config.py`. Output format is auto-detected:

- **Interactive terminal (TTY):** human-readable with timestamps — `2026-02-23 04:18:47  INFO      scraper  Starting scrape of ...`
- **systemd / pipe (non-TTY):** JSON lines via [python-json-logger](https://github.com/madzak/python-json-logger) — `{"timestamp": "2026-02-23T04:18:47", "level": "INFO", "name": "scraper", "message": "Starting scrape of ..."}`

Uvicorn’s access and error logs are routed through the same formatter for consistent output.

## Project Structure

```
wslcb-licensing-tracker/
├── app.py                  # FastAPI web application
├── api_routes.py           # Versioned API router (/api/v1/*)
├── admin_routes.py         # Admin router (/admin/*)
├── admin_auth.py           # Admin authentication (exe.dev proxy headers)
├── admin_audit.py          # Admin audit log helpers
├── cli.py                  # Unified CLI entry point (argparse subcommands)
├── pipeline.py             # Unified ingestion pipeline (ingest_record, ingest_batch)
├── display.py              # Presentation formatting (format_outcome, summarize_provenance)
├── parser.py               # Pure HTML/diff parsing (no DB, no side effects)
├── db.py                   # Connection management, constants, location/source/provenance helpers
├── schema.py               # DDL, PRAGMA user_version migrations, FTS5, seeding
├── database.py             # Async SQLAlchemy engine factory (DATABASE_URL env var)
├── models.py               # SQLAlchemy Core Table definitions (all 20 tables)
├── pg_schema.py            # Alembic-based schema init (alembic upgrade head)
├── pg_db.py                # Async equivalents of db.py helpers
├── pg_pipeline.py          # Async ingestion pipeline (insert_record, ingest_batch)
├── pg_scraper.py           # Async scraper (WSLCB page fetch, archive, ingest)
├── pg_backfill_snapshots.py # Async backfill from archived HTML snapshots
├── pg_backfill_diffs.py    # Async backfill from CO diff archives
├── pg_integrity.py         # Async integrity checks (run_all_checks, fix_orphaned_locations)
├── pg_endorsements.py      # Async endorsement pipeline
├── pg_endorsements_seed.py # Async endorsement seeding and repair
├── pg_endorsements_admin.py # Async admin helpers for endorsement management
├── pg_entities.py          # Async entity normalization
├── pg_address_validator.py # Async address validation DB layer
├── pg_link_records.py      # Async application→outcome record linking
├── pg_queries.py           # Async search, filters, stats (tsvector + pg_trgm)
├── pg_admin_audit.py       # Async admin audit log
├── pg_substances.py        # Async regulated substance CRUD
├── data_migration.py       # Run-once data migration framework (resolves #85)
├── queries.py              # Record search, filters, stats, CRUD
├── entities.py             # Entity (applicant) normalization
├── endorsements.py         # Core endorsement pipeline (process_record, query helpers, alias management)
├── endorsements_seed.py    # Seeding, repair, and backfill (seed_endorsements, discover_code_mappings, …)
├── endorsements_admin.py   # Admin helpers for endorsement management
├── substances.py           # Regulated substance CRUD
├── link_records.py         # Application→outcome record linking
├── log_config.py           # Centralized logging configuration
├── address_validator.py    # Address validation API client
├── scraper.py              # WSLCB page scraper — fetch, archive, ingest via pipeline
├── backfill_snapshots.py   # Ingest + repair from archived HTML snapshots via pipeline
├── backfill_diffs.py       # Ingest from CO diff archives via pipeline
├── backfill_provenance.py  # One-time backfill of source provenance links
├── integrity.py            # Database integrity checks (used by cli.py check)
├── rebuild.py              # Rebuild database from archived sources (used by cli.py rebuild)
├── seed_code_map.json      # Seed data: WSLCB numeric code → endorsement name(s)
├── env                     # API keys (gitignored, 640 root:exedev)
├── templates/
│   ├── base.html           # Base layout template
│   ├── index.html          # Dashboard with stats
│   ├── search.html         # Search interface with filters
│   ├── detail.html         # Record detail page
│   ├── 404.html            # Not-found error page
│   ├── entities.html       # Entities landing page (searchable, paginated)
│   ├── entity.html         # Entity detail page
│   ├── admin/
│   │   ├── base.html       # Admin base layout
│   │   ├── dashboard.html  # System dashboard
│   │   ├── endorsements.html # Endorsement management (list, suggestions, codes)
│   │   ├── users.html      # Admin user management
│   │   └── audit_log.html  # Audit log
│   └── partials/
│       ├── results.html           # Search results partial (HTMX)
│       ├── record_table.html      # Shared record table (results + entity pages)
│       ├── entities_results.html  # Entities results partial (HTMX)
│       └── source_viewer.html     # Source viewer partial (HTMX iframe)
├── static/                 # Static assets
│   ├── css/
│   │   ├── input.css       # Tailwind source (@tailwind directives, badge classes)
│   │   └── tailwind.css    # Pre-built Tailwind output (committed)
│   ├── js/
│   │   ├── search.js       # Search page: section toggle, state→city fetch, endorsement dropdown
│   │   ├── admin-endorsements.js  # Admin endorsements page
│   │   └── detail.js       # Record detail page: source viewer toggle
│   └── images/             # Cannabis Observer brand assets (icon + wordmark SVGs)
├── data/                   # Persistent data (gitignored)
│   ├── wslcb.db            # SQLite database
│   └── wslcb/                  # Archived data by source
│       ├── licensinginfo/      # HTML snapshots from licensinginfo.lcb.wa.gov
│       │   └── [yyyy]/         # Archived HTML snapshots by year
│       └── licensinginfo-diffs/  # CO diff archive files
│           ├── notifications/  # Unified diffs of the notifications section
│           ├── approvals/      # Unified diffs of the approvals section
│           └── discontinued/   # Unified diffs of the discontinued section
├── scripts/
│   └── sqlite_to_pg.py     # One-time SQLite→PostgreSQL data migration script
├── alembic/                # Alembic schema migrations
│   ├── alembic.ini
│   └── versions/
│       ├── 0001_baseline_postgresql_schema.py
│       └── 0002_fts.py
├── requirements.txt        # Python dependencies (runtime + dev)
├── pytest.ini              # Pytest configuration
├── tests/                  # Test suite
│   ├── conftest.py              # Shared fixtures (in-memory DB, sample records)
│   ├── test_parser.py           # Parser function tests
│   ├── test_db.py               # Connection management and constant tests
│   ├── test_schema.py           # Migration framework tests
│   ├── test_database.py         # db.py helper tests (location/source/provenance)
│   ├── test_pipeline.py         # Ingestion pipeline tests
│   ├── test_display.py          # Presentation formatting tests
│   ├── test_queries.py          # Record insert/query tests
│   ├── test_link_records.py     # Record linking tests (bulk + incremental)
│   ├── test_endorsements.py     # Endorsement normalization tests
│   ├── test_integrity.py        # Integrity check tests
│   ├── test_rebuild.py          # Rebuild from sources tests
│   ├── test_scraper.py          # Scraper hash deduplication tests
│   ├── test_address_validator.py # Address validation client tests
│   ├── test_routes.py           # Public route tests
│   ├── test_api_routes.py       # Versioned API route tests
│   ├── test_entities.py         # Entity normalization tests
│   ├── test_source_viewer.py    # Source viewer route tests
│   ├── test_admin_auth.py       # Admin authentication tests
│   ├── test_admin_audit.py      # Admin audit log tests
│   ├── test_admin_users.py      # Admin user management route tests
│   ├── test_admin_endorsements.py # Admin endorsement/substance route tests
│   └── fixtures/                # Minimal HTML fixtures for parser tests
├── wslcb-web.service            # systemd service for the web app
├── wslcb-task@.service          # systemd template for oneshot tasks
├── wslcb-scraper.timer          # systemd timer (twice-daily: 12:30 AM and 6:30 AM Pacific)
├── wslcb-healthcheck.service    # systemd health check service (restarts web on failure)
└── wslcb-healthcheck.timer      # systemd health check timer (every 5 minutes)
```

## Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 14+
- systemd (for scheduling; optional if running manually)

### Installation

```bash
git clone https://github.com/CannObserv/wslcb-licensing-tracker.git
cd wslcb-licensing-tracker

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configure the database

Copy the template and set your PostgreSQL connection URL:

```bash
cp env.example env
# Edit env: set DATABASE_URL=postgresql+asyncpg://user:password@localhost/wslcb
```

Apply the schema:
```bash
alembic upgrade head
```

### Run the initial scrape

```bash
python cli.py scrape
```

This fetches the current 30-day report, populates the SQLite database (`data/wslcb.db`), and archives a copy of the source HTML under `data/wslcb/licensinginfo/`.

### Start the web application

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

Then visit [http://localhost:8000](http://localhost:8000).

### Set up automated scraping (systemd)

```bash
sudo cp wslcb-web.service wslcb-task@.service wslcb-scraper.timer \
     wslcb-healthcheck.service wslcb-healthcheck.timer /etc/systemd/system/
sudo cp sudoers.d-wslcb-healthcheck /etc/sudoers.d/wslcb-healthcheck
sudo chmod 440 /etc/sudoers.d/wslcb-healthcheck

sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-web.service
sudo systemctl enable --now wslcb-scraper.timer
sudo systemctl enable --now wslcb-healthcheck.timer
```

The healthcheck service polls `/api/v1/health` every 5 minutes and automatically restarts the web app if it is unreachable. The `sudoers.d-wslcb-healthcheck` snippet grants the necessary permission for a passwordless `systemctl restart wslcb-web.service`.

The scraper runs twice daily at 12:30 AM and 6:30 AM Pacific with up to 5 minutes of random delay.

Check the timer status:

```bash
systemctl list-timers wslcb-scraper.timer
journalctl -u wslcb-task@scrape.service   # scraper logs
journalctl -u wslcb-web.service       # web app logs
```

### Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `ADDRESS_VALIDATOR_API_KEY` | *(none)* | API key for the address validation service (also read from `./env` file) |

The SQLite database and archived HTML snapshots are stored in `./data/` relative to the project root.

## API Endpoints

### Public UI Routes

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard with summary statistics |
| `GET /search` | Search interface (HTML) or HTMX partial results |
| `GET /record/{id}` | Record detail page |
| `GET /entities` | Searchable, paginated entity list |
| `GET /entity/{id}` | Entity detail page — all license records for a person or organization |
| `GET /source/{source_id}/record/{record_id}` | HTMX partial — renders archived source HTML for a specific record |

### Versioned API (`/api/v1/`)

| Endpoint | Description |
|---|---|
| `GET /api/v1/cities` | JSON list of cities for a given state (query param `state`) |
| `GET /api/v1/stats` | JSON aggregate statistics |
| `GET /api/v1/export` | Streaming CSV export (same query params as `/search`) |
| `GET /api/v1/health` | Health check — 200 OK or 503 when the DB is unavailable |

All `/api/v1/` responses use a consistent JSON envelope: `{"ok": bool, "message": "...", "data": ...}`. The CSV export endpoint is exempt and returns a raw `StreamingResponse`.

### Admin Routes (`/admin/`)

| Endpoint | Description |
|---|---|
| `GET /admin/` | System dashboard — record counts, recent scrapes, data quality checklist |
| `GET /admin/endorsements` | Endorsement management — list, duplicate suggestions, code mappings |
| `GET /admin/users` | Admin user management — add/remove admins |
| `GET /admin/audit-log` | Audit log — paginated history of all admin mutations |

Admin routes are protected by exe.dev proxy authentication (`X-ExeDev-Email` / `X-ExeDev-UserID` headers). The first admin user must be bootstrapped via CLI:

```bash
python cli.py admin add-user you@example.com
```

## License Type Normalization

The WSLCB source page uses two different representations for license types:

- **New applications** list endorsements as semicolon-separated text (e.g., `GROCERY STORE - BEER/WINE; SNACK BAR`)
- **Approved/discontinued** records use opaque numeric codes (e.g., `450,`)

Historical data also uses a hybrid `"CODE, NAME"` format (e.g., `"450, GROCERY STORE - BEER/WINE"`). The tracker normalizes all three into a shared `license_endorsements` table, linked to records via a `record_endorsements` junction table. A seed mapping of 103 known codes is loaded at startup from `seed_code_map.json`, and new mappings are automatically discovered by cross-referencing license numbers that appear in both sections.

## Address Standardization

Business addresses are stored in a normalized `locations` table — each unique raw address string from the WSLCB source is stored once and shared across all license records that reference it. Records link to locations via `location_id` and `previous_location_id` foreign keys.

Each location is standardized via an external address validation API into structured USPS-standard components:

| Field | Example |
|---|---|
| `address_line_1` | `1200 WESTLAKE AVE N` |
| `address_line_2` | `STE 100` |
| `std_city` | `SEATTLE` |
| `std_state` | `WA` |
| `std_zip` | `98109-3528` |

Locations are validated at scrape time. When a new record references an already-known address, it reuses the existing location row and skips the API call. Un-validated locations can be backfilled:

```bash
python cli.py backfill-addresses
```

To re-validate all locations (e.g., after the validation service is updated):

```bash
python cli.py refresh-addresses
```

This is safe to interrupt — progress is committed in batches and each location's timestamp is updated individually.

## Integrity Checks

To check the database for data quality issues:

```bash
python cli.py check
```

This reports orphaned locations, broken foreign keys, un-enriched records, endorsement anomalies, and entity duplicates.

To auto-fix safe issues (e.g., remove orphaned locations):

```bash
python cli.py check --fix
```

The original raw address string is always preserved in `locations.raw_address`. If the validation service is unavailable, the scrape completes normally and standardized fields remain empty until a future backfill.

## Rebuilding from Sources

To create a fresh database from all archived diff files and HTML snapshots:

```bash
python cli.py rebuild --output data/wslcb-rebuilt.db
```

This replays all historical data through the ingestion pipeline in four phases: diff archive ingestion, snapshot ingestion, endorsement discovery, and outcome link building.

Use `--force` to overwrite an existing output file, and `--verify` to compare the rebuilt database against the production database:

```bash
python cli.py rebuild --output data/wslcb-rebuilt.db --verify --force
```

Verification compares record natural keys and reports missing/extra records with a per-section breakdown. Exits with code 1 if discrepancies are found.

**Note:** This is a long-running operation (20+ minutes on the full archive of 4400+ diff files).

## ASSUMPTION Records

ASSUMPTION records represent one business assuming (purchasing) a license from another. They contain data about both the seller and buyer, using different field labels than standard records:

| Source Field | DB Column | Description |
|---|---|---|
| Current Business Name | `previous_business_name` | Seller's business name |
| Current Applicant(s) | `previous_applicants` | Seller's applicants (often empty) |
| New Business Name | `business_name` | Buyer's business name |
| New Applicant(s) | `applicants` | Buyer's applicants |
| Business Location | `location_id` → `locations` | Single shared location |
| Contact Phone | `contact_phone` | Buyer's contact phone |

## CHANGE OF LOCATION Records

CHANGE OF LOCATION records represent a business moving to a new physical address. In the new applications section, they use different field labels to capture both the origin and destination:

| Source Field | DB Column | Description |
|---|---|---|
| Current Business Location | `previous_location_id` → `locations` | Origin address (moving from) |
| New Business Location | `location_id` → `locations` | Destination address (moving to) |

Both the previous and new addresses are stored as entries in the `locations` table, each with their own regex-parsed and USPS-standardized components.

In the approved section, CHANGE OF LOCATION records only have `location_id` (the new address) — the source page does not provide the previous address for approved records.

## Entity Normalization

Applicant names (people and organizations behind each license) are extracted from the semicolon-delimited `applicants` and `previous_applicants` fields into a shared `entities` table. This enables cross-license analysis — e.g., viewing all licenses associated with a particular person.

Names are normalized at ingestion time:

- **Uppercased** for consistency (the WSLCB source is predominantly uppercase but occasionally uses mixed case)
- **Stray trailing punctuation stripped** — the source occasionally appends errant periods or commas to names (e.g., `WOLDU ARAYA BERAKI.`). These are removed while preserving legitimate suffixes like `INC.`, `JR.`, `SR.`, `LTD.`, `CORP.`, etc.
- **Deduplicated** by exact name match — the same person appearing on multiple licenses shares a single entity row

The first element of the `applicants` string (the business name) is excluded from entity extraction — only the individual people and organizations are stored.

The same cleaning is applied to the `applicants` and `previous_applicants` string columns on `license_records`, so FTS search results and CSV exports are consistent with entity names.

Entities are classified as `person` or `organization` by a heuristic that checks for business-indicator patterns (`LLC`, `INC`, `CORP`, `TRUST`, etc.).

## Source Provenance

Every license record is linked to the source artifact(s) it was extracted from via a many-to-many `record_sources` junction table. This tracks *where* each record came from (live scrape, archived snapshot, Internet Archive, etc.), when the source was captured, and what role the source played (first introduction, corroboration, or data repair).

### Source Types

| ID | Slug | Description |
|---|---|---|
| 1 | `live_scrape` | Direct scrape of the WSLCB licensing page |
| 2 | `co_archive` | Cannabis Observer archived HTML snapshots |
| 3 | `internet_archive` | Wayback Machine snapshots |
| 4 | `co_diff_archive` | Cannabis Observer diff-detected change snapshots |
| 5 | `manual` | Manually entered or corrected records |

### Roles

| Role | Meaning |
|---|---|
| `first_seen` | This source introduced the record to the database |
| `confirmed` | Record already existed; this source corroborates it |
| `repaired` | This source was used to fix/enrich the record |

Provenance is displayed on record detail pages as collapsed summary badges (e.g., "⚡ Live Scrape ×13, 📁 CO Archive ×10") with an expandable list of individual sources.

## Backfilling from Snapshots

To ingest historical records and repair broken data from archived HTML snapshots:

```bash
python cli.py backfill-snapshots
```

This runs a two-phase process:
1. **Ingest** — insert new records from all archived snapshots (duplicates are safely skipped)
2. **Repair** — fix broken ASSUMPTION records (empty business names) and CHANGE OF LOCATION records (missing locations)

Safe to re-run at any time. Address validation is deferred; run `python cli.py backfill-addresses` afterward to validate new locations.

## Testing

The project uses pytest with in-memory SQLite databases and static HTML fixtures. Tests run in under 1 second with no network calls.

```bash
source venv/bin/activate
python -m pytest tests/ -v
```

The project follows **red/green TDD**: every new feature and bug fix starts with a failing test, then the minimum code to make it pass, then refactoring.

Test structure:

| File | Scope |
|---|---|
| `tests/test_parser.py` | Pure HTML parsing functions — all record types, edge cases |
| `tests/test_db.py` | Connection management, constants, raw address normalization |
| `tests/test_schema.py` | Migration framework, user_version, existing DB detection |
| `tests/test_database.py` | Location/source/provenance helper functions |
| `tests/test_pipeline.py` | Unified ingestion pipeline — insert, endorsements, provenance, outcome linking |
| `tests/test_display.py` | Presentation formatting — outcome statuses, provenance summaries |
| `tests/test_link_records.py` | Record linking — bulk, incremental, outcome status, reverse links |
| `tests/test_endorsements.py` | Endorsement normalization — merge helper, processing, repair, alias system |
| `tests/test_integrity.py` | Integrity checks — all check and fix functions |
| `tests/test_rebuild.py` | Rebuild from sources — empty data, snapshot ingestion, overwrite/force, DB comparison |
| `tests/test_scraper.py` | Scraper logic — content hash deduplication, redundant data cleanup |
| `tests/test_queries.py` | Record insertion, deduplication, entity creation |
| `tests/test_address_validator.py` | Address validation client — request building, response parsing, error handling |
| `tests/test_routes.py` | Public route tests — dashboard layout, search, stat cards |
| `tests/test_api_routes.py` | Versioned API routes — cities, stats, export, health check |
| `tests/test_entities.py` | Entity normalization — extraction, cleaning, type classification |
| `tests/test_source_viewer.py` | Source viewer route — snapshot dispatch, iframe rendering, not-found cases |
| `tests/test_admin_auth.py` | Admin authentication — header extraction, env-var fallback, redirect/403 |
| `tests/test_admin_audit.py` | Admin audit log — log_action, get_audit_log, filters, pagination |
| `tests/test_admin_users.py` | Admin user management routes — add, remove, list, self-removal guard |
| `tests/test_admin_endorsements.py` | Admin endorsement/substance routes — add, remove, set-endorsements |
| `tests/conftest.py` | Shared fixtures: in-memory DB, sample record dicts |
| `tests/fixtures/` | Minimal HTML files exercising each record type and section |

## Data Source

All data is sourced from the Washington State Liquor and Cannabis Board's public licensing activity page:

**[https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp](https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp)**

> *STATEWIDE New License Applications, Approvals, and Discontinuances (for past 30 days, in order of most recent date)*

This is an unofficial tracker and is not affiliated with or endorsed by the WSLCB.

## License

MIT
