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
| Scheduling | systemd timers (scraper twice-daily, address validation weekly) |

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
├── cli.py                  # Unified CLI entry point (click groups: ingest, db, admin)
├── display.py              # Presentation formatting (format_outcome, summarize_provenance)
├── parser.py               # Pure HTML/diff parsing (no DB, no side effects)
├── text_utils.py           # Pure-string text normalization (clean_entity_name, strip_duplicate_marker, …)
├── database.py             # Async SQLAlchemy engine factory (DATABASE_URL env var)
├── models.py               # SQLAlchemy Core Table definitions (all 20 tables)
├── pg_schema.py            # Alembic-based schema init (alembic upgrade head)
├── pg_db.py                # Async location/source/provenance helpers + shared constants
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
├── pg_queries_hydrate.py   # Integration layer: enrich_record, hydrate_records
├── pg_queries_search.py    # Core search + single-record lookups (imports pg_db only)
├── pg_queries_filter.py    # Filter dropdowns (no cache — always live from DB)
├── pg_queries_stats.py     # Dashboard statistics (no cache — always live from DB)
├── pg_queries_export.py    # Flat CSV export (streaming + batch)
├── pg_queries_entity.py    # Entity-centric record queries
├── pg_admin_audit.py       # Async admin audit log
├── pg_substances.py        # Async regulated substance CRUD
├── data_migration.py       # Run-once data migration framework (resolves #85)
├── log_config.py           # Centralized logging configuration
├── seed_code_map.json      # Seed data: WSLCB numeric code → endorsement name(s)
├── .env                    # Dev env vars: DATABASE_URL, GH_TOKEN, ADMIN_DEV_EMAIL (gitignored)
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
│   └── wslcb/                  # Archived data by source
│       ├── licensinginfo/      # HTML snapshots from licensinginfo.lcb.wa.gov
│       │   └── [yyyy]/         # Archived HTML snapshots by year
│       └── licensinginfo-diffs/  # CO diff archive files
│           ├── notifications/  # Unified diffs of the notifications section
│           ├── approvals/      # Unified diffs of the approvals section
│           └── discontinued/   # Unified diffs of the discontinued section
├── package.json            # JS test tooling only (devDependencies: jsdom)
├── pyproject.toml          # Python project metadata, dependencies, ruff + pytest config (uv)
├── scripts/
│   ├── build-css.sh            # Rebuild Tailwind output (run manually or via pre-commit)
│   ├── download-tailwind.sh    # Download platform-specific Tailwind CLI binary
│   ├── pre-commit-tailwind.sh  # Pre-commit hook wrapper for build-css.sh
│   └── sqlite_to_pg.py         # One-time SQLite→PostgreSQL data migration script
├── alembic/                # Alembic schema migrations
│   ├── alembic.ini
│   └── versions/
│       ├── 0001_baseline_postgresql_schema.py
│       ├── 0002_fts.py
│       ├── 0003_timestamp_columns.py
│       └── 0004_nullable_std_address_line_2.py
├── infra/                  # systemd unit and timer files (copy to /etc/systemd/system/)
│   ├── wslcb-web.service           # Web app service
│   ├── wslcb-task@.service         # Oneshot task template
│   ├── wslcb-scraper.timer         # Twice-daily scraper timer
│   ├── wslcb-healthcheck.service   # Health check service (restarts web on failure)
│   ├── wslcb-healthcheck.timer     # Health check timer (every 5 minutes)
│   ├── wslcb-address-validation.timer # Weekly address backfill timer
│   └── sudoers.d-wslcb-healthcheck # sudoers snippet for passwordless restart
├── skills/                 # Claude Code agent skills (local + symlinks to skills-vendor/)
├── skills-vendor/          # Vendored skill repos (git submodules)
│   ├── gregoryfoster-skills/   # gregoryfoster/skills submodule
│   └── obra-superpowers/       # obra/superpowers submodule
├── tests/                  # Test suite
│   ├── conftest.py              # Shared fixtures (sample record dicts)
│   ├── test_parser.py           # Parser function tests
│   ├── test_text_utils.py       # Pure-string normalization tests
│   ├── test_display.py          # Presentation formatting tests
│   ├── test_models.py           # SQLAlchemy Table definition tests
│   ├── test_app.py              # App lifespan and startup tests
│   ├── test_routes.py           # Public route tests
│   ├── test_api_routes.py       # Versioned API route tests
│   ├── test_source_viewer.py    # Source viewer route tests
│   ├── test_admin_auth.py       # Admin authentication tests
│   ├── test_admin_routes.py     # Admin route tests
│   ├── test_admin_users.py      # Admin user management route tests
│   ├── test_admin_endorsements.py # Admin endorsement/substance route tests
│   ├── test_data_migration.py   # Run-once data migration framework tests
│   ├── test_pg_database.py      # Async engine factory tests
│   ├── test_pg_db.py            # Location/source/provenance helper tests
│   ├── test_pg_schema.py        # Alembic migration tests (require TEST_DATABASE_URL)
│   ├── test_pg_pipeline.py      # Ingestion pipeline tests (require TEST_DATABASE_URL)
│   ├── test_pg_queries.py       # Search, filter, stats tests (require TEST_DATABASE_URL)
│   ├── test_pg_link_records.py  # Record linking tests (require TEST_DATABASE_URL)
│   ├── test_pg_endorsements.py  # Endorsement pipeline tests (require TEST_DATABASE_URL)
│   ├── test_pg_endorsements_seed.py # Endorsement seeding tests (require TEST_DATABASE_URL)
│   ├── test_pg_endorsements_admin.py # Admin endorsement helper tests
│   ├── test_pg_entities.py      # Entity normalization tests (require TEST_DATABASE_URL)
│   ├── test_pg_integrity.py     # Integrity check tests (require TEST_DATABASE_URL)
│   ├── test_pg_scraper.py       # Scraper hash deduplication tests
│   ├── test_pg_address_validator.py # Address validation DB layer tests
│   ├── test_pg_admin_audit.py   # Admin audit log tests
│   ├── test_pg_substances.py    # Regulated substance CRUD tests
│   ├── test_cli.py              # Click CLI group and subcommand tests
│   ├── test_cli_scrape.py       # Scrape CLI subcommand tests
│   ├── test_cache_removal.py    # Cache removal verification tests (#99)
│   ├── js/
│   │   └── test_detail.js       # Source viewer toggle JS tests (Node + jsdom)
│   └── fixtures/                # Minimal HTML fixtures for parser tests
└── docs/                   # Operations and architecture documentation
    ├── DEPLOYMENT.md        # systemd setup, service lifecycle, address validation ops
    ├── SCHEMA.md            # Full table/column reference and migration history
    ├── SOURCE_PAGE.md       # WSLCB source page structure and field label quirks
    └── STYLE.md             # Brand colors and CSS conventions
```

## Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 14+
- systemd (for scheduling; optional if running manually)

### Installation

```bash
git clone --recurse-submodules https://github.com/CannObserv/wslcb-licensing-tracker.git
cd wslcb-licensing-tracker

# Install dependencies (requires uv — https://docs.astral.sh/uv/)
uv sync --dev

# Install pre-commit hooks (ruff lint/format + Tailwind CSS rebuild)
uv run pre-commit install
```

### Configure the database

Create `.env` at the repo root and set your PostgreSQL connection URL:

```bash
# .env (gitignored)
DATABASE_URL=postgresql+asyncpg://user:password@localhost/wslcb
```

Apply the schema:
```bash
uv run alembic upgrade head
```

### Run the initial scrape

```bash
uv run wslcb scrape
```

This fetches the current 30-day report, inserts records into the PostgreSQL database, and archives a copy of the source HTML under `data/wslcb/licensinginfo/`.

### Start the web application

```bash
uv run uvicorn wslcb_licensing_tracker.app:app --host 0.0.0.0 --port 8000
```

Then visit [http://localhost:8000](http://localhost:8000).

### Set up automated scraping (systemd)

```bash
sudo cp infra/wslcb-web.service infra/wslcb-task@.service infra/wslcb-scraper.timer \
     infra/wslcb-address-validation.timer \
     infra/wslcb-healthcheck.service infra/wslcb-healthcheck.timer /etc/systemd/system/
sudo cp infra/sudoers.d-wslcb-healthcheck /etc/sudoers.d/wslcb-healthcheck
sudo chmod 440 /etc/sudoers.d/wslcb-healthcheck

sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-web.service
sudo systemctl enable --now wslcb-scraper.timer
sudo systemctl enable --now wslcb-address-validation.timer
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
| `ADDRESS_VALIDATOR_API_KEY` | *(none)* | API key for the address validation service |

Archived HTML snapshots and other persistent data are stored in `./data/` relative to the project root.

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
uv run wslcb admin add-user you@example.com
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
uv run wslcb backfill-addresses
```

To re-validate all locations (e.g., after the validation service is updated):

```bash
uv run wslcb refresh-addresses
```

This is safe to interrupt — progress is committed in batches and each location's timestamp is updated individually.

## Integrity Checks

To check the database for data quality issues:

```bash
uv run wslcb db check
```

This reports orphaned locations, broken foreign keys, un-enriched records, endorsement anomalies, and entity duplicates.

To auto-fix safe issues (e.g., remove orphaned locations):

```bash
uv run wslcb db check --fix
```

The original raw address string is always preserved in `locations.raw_address`. If the validation service is unavailable, the scrape completes normally and standardized fields remain empty until a future backfill.

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
uv run wslcb backfill-snapshots
```

This runs a two-phase process:
1. **Ingest** — insert new records from all archived snapshots (duplicates are safely skipped)
2. **Repair** — fix broken ASSUMPTION records (empty business names) and CHANGE OF LOCATION records (missing locations)

Safe to re-run at any time. Address validation is deferred; run `uv run wslcb backfill-addresses` afterward to validate new locations.

## Testing

The project uses pytest with async PostgreSQL mocks and static HTML fixtures. Tests that require a live database are skipped unless `TEST_DATABASE_URL` is set. Non-PG tests run in under 2 seconds with no network calls.

```bash
uv run pytest tests/ -v
```

The project follows **red/green TDD**: every new feature and bug fix starts with a failing test, then the minimum code to make it pass, then refactoring.

Test structure:

| File | Scope |
|---|---|
| `tests/test_parser.py` | Pure HTML parsing functions — all record types, edge cases |
| `tests/test_text_utils.py` | Pure-string normalization — clean_entity_name, strip_duplicate_marker, clean_applicants_string |
| `tests/test_display.py` | Presentation formatting — outcome statuses, provenance summaries |
| `tests/test_models.py` | SQLAlchemy Table definition smoke tests |
| `tests/test_app.py` | App lifespan and startup |
| `tests/test_routes.py` | Public route tests — dashboard layout, search, stat cards |
| `tests/test_api_routes.py` | Versioned API routes — cities, stats, export, health check |
| `tests/test_source_viewer.py` | Source viewer route — snapshot dispatch, iframe rendering, not-found cases |
| `tests/test_admin_auth.py` | Admin authentication — header extraction, env-var fallback, redirect/403 |
| `tests/test_admin_routes.py` | Admin route rendering and redirects |
| `tests/test_admin_users.py` | Admin user management routes — add, remove, list, self-removal guard |
| `tests/test_admin_endorsements.py` | Admin endorsement/substance routes — add, remove, set-endorsements |
| `tests/test_data_migration.py` | Run-once migration framework — idempotency, ordering |
| `tests/test_pg_database.py` | Async engine factory |
| `tests/test_pg_db.py` | Location/source/provenance helper functions |
| `tests/test_pg_schema.py` | Alembic migration tests (require `TEST_DATABASE_URL`) |
| `tests/test_pg_pipeline.py` | Ingestion pipeline — insert, dedup, endorsements, entities (require `TEST_DATABASE_URL`) |
| `tests/test_pg_queries.py` | Search, filters, stats, export, entity queries (require `TEST_DATABASE_URL`) |
| `tests/test_pg_link_records.py` | Record linking — bulk, incremental, outcome status (require `TEST_DATABASE_URL`) |
| `tests/test_pg_endorsements.py` | Endorsement pipeline — processing, repair, alias system (require `TEST_DATABASE_URL`) |
| `tests/test_pg_endorsements_seed.py` | Endorsement seeding and code-map repair (require `TEST_DATABASE_URL`) |
| `tests/test_pg_endorsements_admin.py` | Admin endorsement helpers — similarity, suggestions, dismissals |
| `tests/test_pg_entities.py` | Entity normalization — extraction, dedup, reprocessing (require `TEST_DATABASE_URL`) |
| `tests/test_pg_integrity.py` | Integrity checks — all check and fix functions (require `TEST_DATABASE_URL`) |
| `tests/test_pg_scraper.py` | Scraper — content hash deduplication, redundant data cleanup |
| `tests/test_pg_address_validator.py` | Address validation DB layer — standardize, validate, backfill |
| `tests/test_pg_admin_audit.py` | Admin audit log — log_action, get_audit_log, filters, pagination |
| `tests/test_pg_substances.py` | Regulated substance CRUD — get, add, remove, set-endorsements |
| `tests/conftest.py` | Shared fixtures: sample record dicts, pg_conn/pg_engine (skipped without TEST_DATABASE_URL) |
| `tests/fixtures/` | Minimal HTML files exercising each record type and section |

## Data Source

All data is sourced from the Washington State Liquor and Cannabis Board's public licensing activity page:

**[https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp](https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp)**

> *STATEWIDE New License Applications, Approvals, and Discontinuances (for past 30 days, in order of most recent date)*

This is an unofficial tracker and is not affiliated with or endorsed by the WSLCB.

## License

MIT
