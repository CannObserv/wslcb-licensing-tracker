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
| Scheduling | systemd timers (scraper twice-daily, address validation daily) |

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
├── admin_routes.py         # Admin router (/admin/*: dashboard, users, audit log)
├── admin_endorsement_routes.py # Admin endorsement-curation router (/admin/endorsements*)
├── admin_entity_routes.py  # Admin classification review router (/admin/entities/*, #208)
├── admin_auth.py           # Admin authentication (exe.dev proxy headers)
├── cli.py                  # Unified CLI entry point (click groups: ingest, db, admin)
├── display.py              # Presentation formatting (format_outcome, summarize_provenance, classification_badge)
├── parser.py               # Pure HTML/diff parsing (no DB, no side effects)
├── diff_replay.py          # Chain replay of CO diff archives (#151; no DB, no side effects)
├── text_utils.py           # Pure-string text normalization (clean_entity_name, strip_duplicate_marker, …)
├── engine.py               # Async SQLAlchemy engine factory (DATABASE_URL env var)
├── models.py               # SQLAlchemy Core Table definitions (all 20 tables)
├── schema.py               # Alembic-based schema init (alembic upgrade head)
├── db.py                   # Async location/source/provenance helpers + shared constants
├── pipeline.py             # Async ingestion pipeline (insert_record, ingest_batch)
├── scraper.py              # Async scraper (WSLCB page fetch, archive, ingest)
├── relay.py                # Import operator-relayed captures during an IP block (#201)
├── backfill_snapshots.py   # Async backfill from archived HTML snapshots
├── backfill_diffs.py       # Async backfill from CO diff archives
├── replay_extracts.py      # Replay-generated provenance extracts (#154)
├── integrity.py            # Async integrity checks (run_all_checks, fix_orphaned_locations)
├── endorsements.py         # Async endorsement pipeline
├── endorsements_seed.py    # Async endorsement seeding and repair
├── endorsements_admin.py   # Async admin helpers for endorsement management
├── entities.py             # Async entity normalization + regex classifier
├── entity_classification.py # Classification resolver (manual > Jev > regex), manual rows, Jev rollback (#206)
├── jev_client.py           # HTTP transport for TypeSafe Jev classification (#207)
├── jev_classifier.py       # Jev orchestration: candidates, batches, run rows, check-in payload (#207)
├── classification_review.py # Audited operator actions, review queue, health/spend queries (#208)
├── address_client.py       # HTTP transport for the address-validator service
├── address_validator.py    # Async address validation DB layer
├── link_records.py         # Async application→outcome record linking
├── queries_hydrate.py      # Integration layer: enrich_record, hydrate_records
├── queries_search.py       # Core search + single-record lookups (imports db only)
├── queries_filter.py       # Filter dropdowns (no cache — always live from DB)
├── queries_stats.py        # Dashboard statistics (no cache — always live from DB)
├── queries_export.py       # Flat CSV export (streaming + batch)
├── queries_entity.py       # Entity-centric record queries
├── admin_audit.py          # Async admin audit log
├── substances.py           # Async regulated substance CRUD
├── data_migration.py       # Run-once data migration framework (resolves #85)
├── disk_hygiene.py         # Weekly cache/worktree/data-straggler cleanup (#138)
├── backup.py               # Nightly pg_dump + data/ archive mirror to GCS, create-only (#185)
├── checkin.py              # co-status dead-man check-ins: backup (#185), scrape (#196), classify (#207)
├── restore.py              # Fetch/verify/restore a dump; fetch archive files (#185)
├── log_config.py           # Centralized logging configuration
├── config.py               # Runtime config helpers (get_build_id); env vars in .env.example
├── seed_code_map.json      # Seed data: WSLCB numeric code → endorsement name(s)
├── .env                    # Dev env vars: DATABASE_URL, ADMIN_DEV_EMAIL, etc. (gitignored)
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
│   │   ├── entity_review.html # Entity classification review queue (#208)
│   │   ├── users.html      # Admin user management
│   │   └── audit_log.html  # Audit log
│   └── partials/
│       ├── results.html           # Search results partial (HTMX)
│       ├── record_table.html      # Shared record table (results + entity pages)
│       ├── entities_results.html  # Entities results partial (HTMX)
│       ├── entities_filters.html  # Entities filter-card macros; swapped out of band on live search (#215)
│       ├── classification_badge.html # Entity type chip + public provenance badge macros (#209)
│       ├── entity_admin_panel.html # Entity-page classification panel, admins only (#208)
│       ├── entity_classification_forms.html # Classification action form macros (#208)
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
├── pyproject.toml          # Python project metadata, dependencies, ruff + pytest + coverage config (uv)
├── scripts/
│   ├── build-css.sh            # Rebuild Tailwind output (run manually or via pre-commit)
│   ├── download-tailwind.sh    # Download platform-specific Tailwind CLI binary
│   ├── pre-commit-tailwind.sh  # Pre-commit hook wrapper for build-css.sh
│   ├── setup-backup-role.sql   # Once per cluster: the backup's read-only `wslcb_backup` role (#185)
│   └── relay/                  # Operator's Mac: relay-capture.sh + LaunchAgent plist (#201)
├── alembic.ini             # Alembic config (repo root, not inside alembic/)
├── alembic/                # Alembic schema migrations
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       ├── 0001_baseline_postgresql_schema.py
│       ├── 0002_fts.py
│       ├── 0003_timestamp_columns.py
│       ├── 0004_nullable_std_address_line_2.py
│       ├── 0005_address_validation_attempted_at.py
│       ├── 0006_co_replay_source_type.py
│       ├── 0007_replay_extract_role.py
│       └── 0008_relayed_scrape.py
├── infra/                  # systemd unit and timer files (copy to /etc/systemd/system/)
│   ├── wslcb-web.service           # Web app service
│   ├── wslcb-task@.service         # Oneshot task template
│   ├── wslcb-scraper.timer         # Twice-daily scraper timer
│   ├── wslcb-task@scrape.service.d/ # Scrape check-in drop-in: key credential, 2 h timeout (#196; key file first)
│   ├── wslcb-task@import-relayed.service.d/ # Relay import: 30 min timeout, no check-in (#201)
│   ├── wslcb-entity-classification.timer # Weekly Jev classification, the classify dead-man (#207)
│   ├── wslcb-task@classify-entities.service.d/ # Classify check-in drop-in: key credential, 1 h timeout (#207; key file first)
│   ├── wslcb-healthcheck.service   # Health check service (restarts web on failure)
│   ├── wslcb-healthcheck.timer     # Health check timer (every 5 minutes)
│   ├── wslcb-address-validation.timer # Daily address renewal timer, renew-addresses (#195)
│   ├── wslcb-task@renew-addresses.service.d/ # Daily renewal: 90 min timeout (#195)
│   ├── wslcb-backup.service        # Nightly DB + data/ backup to GCS (#185; see docs/RECOVERY.md)
│   ├── wslcb-backup.timer          # Daily at 08:17 Pacific, after the morning scrape
│   └── sudoers.d-wslcb-healthcheck # sudoers snippet for passwordless restart
├── skills/                 # Claude Code agent skills (local + symlinks to skills-vendor/)
├── skills-vendor/          # Vendored skill repos (git submodules)
│   ├── gregoryfoster-skills/   # gregoryfoster/skills submodule
│   └── obra-superpowers/       # obra/superpowers submodule
├── tests/                  # Test suite
│   ├── conftest.py              # Shared fixtures (sample record dicts)
│   ├── _support.py              # Directly-imported helpers (engine stamping for bare TestClient)
│   ├── cli_helpers.py           # Shared helpers for the CLI test modules
│   ├── htmx_fixtures.py         # Renders the htmx harness's pages from the templates (#232)
│   ├── test_htmx_fixtures.py    # Checked-in harness fixtures match a fresh render (#232)
│   ├── test_parser.py           # Parser function tests
│   ├── test_diff_replay.py      # Chain replay of unified-diff archives (#151)
│   ├── test_text_utils.py       # Pure-string normalization tests
│   ├── test_display.py          # Presentation formatting tests
│   ├── test_models.py           # SQLAlchemy Table definition tests
│   ├── test_app.py              # App lifespan and startup tests; every template autoescaped under Starlette 1.x (#221)
│   ├── test_config.py           # Runtime config helper tests (get_build_id, Jev threshold)
│   ├── test_log_config.py       # Structured-logging regression tests (#162)
│   ├── test_routes.py           # Public route tests
│   ├── test_api_routes.py       # Versioned API route tests
│   ├── test_source_viewer.py    # Source viewer route tests
│   ├── test_admin_auth.py       # Admin authentication tests
│   ├── test_admin_routes.py     # Admin route tests
│   ├── test_admin_entity_routes.py # Admin classification review route tests
│   ├── test_admin_users.py      # Admin user management route tests
│   ├── test_admin_auth_pg.py    # Admin user CLI DB logic (require TEST_DATABASE_URL)
│   ├── test_admin_users_pg.py   # Admin user route DB logic (require TEST_DATABASE_URL)
│   ├── test_admin_endorsements.py # Admin endorsement/substance route tests
│   ├── test_data_migration.py   # Run-once data migration framework tests
│   ├── test_engine.py      # Async engine factory tests
│   ├── test_db.py            # Location/source/provenance helper tests
│   ├── test_schema.py        # Alembic migration tests (require TEST_DATABASE_URL)
│   ├── test_pipeline.py      # Ingestion pipeline tests (require TEST_DATABASE_URL)
│   ├── test_queries.py       # Search, filter, stats tests (require TEST_DATABASE_URL)
│   ├── test_link_records.py  # Record linking tests (require TEST_DATABASE_URL)
│   ├── test_endorsements.py  # Endorsement pipeline tests (require TEST_DATABASE_URL)
│   ├── test_endorsements_seed.py # Endorsement seeding tests (require TEST_DATABASE_URL)
│   ├── test_endorsements_admin.py # Admin endorsement helper tests
│   ├── test_entities.py      # Entity normalization tests (require TEST_DATABASE_URL)
│   ├── test_entity_classification.py # Classification resolver + Jev rollback (require TEST_DATABASE_URL)
│   ├── test_classification_review.py # Operator actions, review queue, health (require TEST_DATABASE_URL)
│   ├── test_integrity.py     # Integrity check tests (require TEST_DATABASE_URL)
│   ├── test_scraper.py       # Scraper hash dedup + check-in outcome/history/variables (#196), freshness (#197)
│   ├── test_backfill_snapshots.py # Two-phase snapshot ingest and repair helpers
│   ├── test_backfill_diffs.py   # Diff-archive scanning, dry-run, and live ingest
│   ├── test_replay_extracts.py  # Replay-generated provenance extracts (#154)
│   ├── test_address_validator.py # Address validation DB layer tests
│   ├── test_admin_audit.py   # Admin audit log tests
│   ├── test_substances.py    # Regulated substance CRUD tests
│   ├── test_cli.py              # Click CLI group and subcommand tests
│   ├── test_cli_scrape.py       # Scrape CLI: post-scrape backfill, co-status check-in (#196)
│   ├── test_relay.py            # Relay import: names, validation, inbox drain (#201)
│   ├── test_cli_relay.py        # `wslcb ingest import-relayed` exit codes, no check-in (#201)
│   ├── test_relay_laptop.py     # relay-capture.sh (stubbed curl/scp/ssh), plist, drop-in (#201)
│   ├── test_cache_removal.py    # Cache removal verification tests (#99)
│   ├── test_disk_hygiene.py     # Disk-hygiene selection logic + fail-open removal (#138)
│   ├── test_backup.py           # Dump verify/tiers + archive mirror rules, against GCS fakes (#185)
│   ├── test_checkin.py          # Dead-man check-in config + retry rules, per monitor (#185, #196)
│   ├── test_restore.py          # --latest across tiers, suspect names, verified fetch (#185)
│   ├── test_cli_backup.py       # `wslcb ops backup|restore|restore-archive` (#185)
│   ├── test_backup_restore_rehearsal.py  # Real pg_dump → fake bucket → pg_restore round trip (#185)
│   ├── scratch_db.py            # Rehearsal scratch DB: pid-namespaced name, drop past autovacuum, leak sweep (#202)
│   ├── test_scratch_db.py       # scratch_db drop fallback/retry/leak warning; real stray-session drop + sweep (#202)
│   ├── test_infra_backup_units.py  # wslcb-backup unit sandbox + timer pins (#185)
│   ├── test_infra_scrape_unit.py   # scrape check-in drop-in + monitor timing pins (#196)
│   ├── test_infra_classify_unit.py # classify timer + check-in drop-in + monitor window pins (#207)
│   ├── test_infra_address_validation_unit.py # daily renew-addresses timer + timeout drop-in pins (#195)
│   ├── test_jev_client.py       # Jev transport: request shape, retries, malformed answers (#207)
│   ├── test_jev_classifier.py   # Classify-once runs, lock, failures, check-in payload (require TEST_DATABASE_URL)
│   ├── gcs_fakes.py             # SDK-faithful google-cloud-storage fakes (#185)
│   ├── test_seed_code_map.py    # Structural guards for seed_code_map.json
│   ├── test_doc_sensitive_paths.py # Guards .skills/doc-sensitive-paths + doc-sections against inert entries (#172, #173)
│   ├── test_no_channel_urls.py  # No live Mayfly channel URL in any tracked file (#181)
│   ├── test_docs_layout.py      # Dated docs stay in docs/specs/ + docs/plans/, not docs/superpowers/ (#176)
│   ├── test_infra_memory_pressure.py # infra/ memory-pressure configs, SocratiCode pin agreement + loopback recreate (#175, #180, #222)
│   ├── test_infra_web_unit.py    # wslcb-web starts after Postgres, never bound to its lifetime (#184)
│   ├── test_journald_config.py # journald drop-in: persistent, 512M; on the host, the flush unmasked + journal on disk (#186)
│   ├── test_needrestart_config.py # needrestart drop-in lists restarts, never performs them (#184)
│   ├── test_patching_hosts_knob.py # .skills/patching-hosts reads clean under the vendored reader; review dates never gate; backup + Tailscale origin/pins; a quiet range covers every timer firing (#192, #193, #220, #223)
│   ├── test_certifi_bundle.py   # certifi ≥ 2026.10.19 in lock + venv, no emSign ECC Root CA - G3 (#220)
│   ├── test_dependency_floors.py # starlette, python-multipart, anyio, idna, lxml, soupsieve, mako (#221) + dev: virtualenv, filelock, pytest, pygments (#224) ≥ advisory fix in lock + venv
│   ├── test_warning_ratchet.py  # pyproject filterwarnings fail a run on a new RuntimeWarning / PytestWarning / own DeprecationWarning; one narrow starlette ignore, live only while httpx2 is absent (#216)
│   ├── js/
│   │   ├── test_detail.js       # Source viewer toggle JS tests (Node + jsdom)
│   │   ├── htmx_harness.js      # Serves rendered fixtures + static/ to jsdom running the vendored htmx (#232)
│   │   ├── test_htmx.js         # htmx behaviour: /entities OOB live search (#215), Back shows the URL's state (#231)
│   │   └── fixtures/            # Pages rendered by tests/htmx_fixtures.py (regenerate, never hand-edit)
│   └── fixtures/                # Minimal HTML fixtures for parser tests
└── docs/                   # Operations and architecture documentation
    ├── DEPLOYMENT.md        # systemd setup, service lifecycle, address validation ops
    ├── MEMORY_PRESSURE.md   # No swap + co-hosted prod: earlyoom, memory configs, SocratiCode pins (#175)
    ├── MIGRATIONS.md        # Alembic chain, revision history, adding a migration
    ├── MONITORING.md        # co-status dead-man check-ins: backup + scrape monitors, scrape drop-in install (#196)
    ├── RELAY.md             # Relayed scrape during the WSLCB block: VM + Mac install, monitoring (#201)
    ├── RECOVERY.md          # Nightly GCS backup, restore, drills (#185)
    ├── SCHEMA.md            # Full table/column reference
    ├── SKILLS.md            # AI agent skill inventory and trigger phrases
    ├── SOCRATICODE.md       # Code-exploration tool table, per-tool notes, graph health, index scope
    ├── SOURCE_PAGE.md       # WSLCB source page structure and field label quirks
    ├── STYLE.md             # Brand colors and CSS conventions
    ├── plans/               # Dated implementation plans (archive)
    ├── research/            # Dated analyses and audits (archive)
    └── specs/               # Dated design specs (archive)
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
uv run wslcb ingest scrape
```

This fetches the current 30-day report, inserts records into the PostgreSQL database, and archives a copy of the source HTML under `data/wslcb/licensinginfo/`.

### Start the web application

```bash
uv run uvicorn wslcb_licensing_tracker.app:app --host 0.0.0.0 --port 8000 --log-config src/wslcb_licensing_tracker/log_config.json
```

`--log-config` routes uvicorn's own access/error lines through the app's JSON formatter so journald never mixes plain-text and JSON records (GH #162).

Then visit [http://localhost:8000](http://localhost:8000).

### Set up automated scraping (systemd)

```bash
sudo cp infra/wslcb-web.service infra/wslcb-task@.service infra/wslcb-scraper.timer \
     infra/wslcb-address-validation.timer \
     infra/wslcb-healthcheck.service infra/wslcb-healthcheck.timer /etc/systemd/system/
sudo cp -r infra/wslcb-task@renew-addresses.service.d /etc/systemd/system/
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

Each run can check in to a co-status dead-man monitor, so a failing or stopped scraper alerts (#196). Its drop-in, `infra/wslcb-task@scrape.service.d/`, is deliberately not in the copy block above: it needs its key file first, or every scrape fails to start. Install it from [`docs/MONITORING.md`](docs/MONITORING.md).

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
| `GET /entities` | Searchable, paginated entity list (`?q=`, `?type=`, `?subtype=`, `?sort=`); each row shows its subtype and provenance badge |
| `GET /entity/{id}` | Entity detail page — type, subtype, provenance badge and all license records for a person or organization |
| `GET /source/{source_id}/record/{record_id}` | HTMX partial — renders archived source HTML for a specific record |

### Versioned API (`/api/v1/`)

| Endpoint | Description |
|---|---|
| `GET /api/v1/cities` | JSON list of cities for a given state (query param `state`) |
| `GET /api/v1/stats` | JSON aggregate statistics |
| `GET /api/v1/export` | Streaming CSV export (same query params as `/search`) |
| `GET /api/v1/health` | Liveness — 200 OK or 503 when the DB is unavailable; `data.scrape` reports scrape freshness but never fails it (#197) |
| `GET /api/v1/health/ingest` | Data freshness — 503 when the last successful scrape is over 26 h old or there has never been one, for external uptime checks |

All `/api/v1/` responses use a consistent JSON envelope: `{"ok": bool, "message": "...", "data": ...}`. The CSV export endpoint is exempt and returns a raw `StreamingResponse`.

### Admin Routes (`/admin/`)

| Endpoint | Description |
|---|---|
| `GET /admin/` | System dashboard — record counts, recent scrapes, data quality checklist, entity classification health/spend |
| `GET /admin/endorsements` | Endorsement management — list, duplicate suggestions, code mappings |
| `GET /admin/entities/review` | Entity classification review queue (`?tab=unknown` for *Subtype unknown*) |
| `POST /admin/entities/{id}/{classify,accept,not-a-name,clear-manual,flag,jev}` | Audited classification actions; each 303-redirects to its `next` path. `jev` is a paid call (409 while disabled) |
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
uv run wslcb ingest backfill-addresses
```

The daily timer runs `renew-addresses`, which also renews early from due-date peaks within the rolling 24h budget (#195; see [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md#renewal-ttl--pacing)).

To re-validate all locations (e.g., after the validation service is updated):

```bash
uv run wslcb ingest refresh-addresses
```

USPS allows about 500 calls per rolling 24h, so a full refresh stops within minutes when the quota runs out and leaves the scrape hooks no budget for a day. To work through a list of locations at a sustainable pace, pass `--location-ids FILE --budget-reserve 100` and run it daily (see [DEPLOYMENT.md](docs/DEPLOYMENT.md)).

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

Each entity is classified `person` or `organization`, with an `entity_subtype` for organizations (business, nonprofit, trust, estate, tribe, government, unknown) and a `classification_source` (regex, Jev or manual). Every source's answer is kept in `entity_classifications`; the shown values are resolved from them (#206, see [`docs/SCHEMA.md`](docs/SCHEMA.md)). Public pages show the subtype and a vendor-neutral badge: **Rules**, **AI · NN%** or **Reviewed** (#209).

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
| 6 | `co_replay` | Reconstructed from CO diff chain replay (#154) |
| 7 | `relayed_scrape` | WSLCB page captured off-host by the operator during an IP block (#201) |

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
uv run wslcb ingest backfill-snapshots
```

This runs a two-phase process:
1. **Ingest** — insert new records from all archived snapshots (duplicates are safely skipped)
2. **Repair** — fix broken ASSUMPTION records (empty business names) and CHANGE OF LOCATION records (missing locations)

Safe to re-run at any time. Address validation is deferred; run `uv run wslcb ingest backfill-addresses` afterward to validate new locations.

## Backups

Nightly, `wslcb-backup.timer` dumps the database and mirrors the frozen source
files under `data/` to two GCS buckets, create-only, with a co-status dead-man
check-in (#185). Install, restore and drills: [`docs/RECOVERY.md`](docs/RECOVERY.md).

```bash
uv run wslcb ops restore --list                                   # dumps, both tiers
uv run wslcb ops restore --latest --prefix HOST --download-only DIR
uv run wslcb ops restore-archive --prefix HOST --into DIR [--path SUBPATH]
```

## Testing

The project uses pytest with async PostgreSQL mocks and static HTML fixtures. Tests that require a live database are skipped unless `TEST_DATABASE_URL` is set. Non-PG tests run in under 2 seconds with no network calls.

```bash
uv run pytest tests/ -v              # coverage runs automatically
uv run pytest tests/ -v --no-cov    # skip coverage for quick iteration
npm test                             # JS tests (Node + jsdom), incl. the htmx harness
```

The htmx harness (`tests/js/test_htmx.js`, #232) runs the vendored htmx under jsdom (there's no browser on the host) against pages rendered from the real templates. Those pages are checked in under `tests/js/fixtures/`. After a template change, regenerate them with `uv run python -m tests.htmx_fixtures`; `tests/test_htmx_fixtures.py` fails until you do.

The project follows **red/green TDD**: every new feature and bug fix starts with a failing test, then the minimum code to make it pass, then refactoring.

Test structure:

| File | Scope |
|---|---|
| `tests/test_parser.py` | Pure HTML parsing functions — all record types, edge cases |
| `tests/test_text_utils.py` | Pure-string normalization — clean_entity_name, strip_duplicate_marker, clean_applicants_string |
| `tests/test_display.py` | Presentation formatting — outcome statuses, provenance summaries |
| `tests/test_models.py` | SQLAlchemy Table definition smoke tests |
| `tests/test_app.py` | App lifespan and startup |
| `tests/test_routes.py` | Public route tests — dashboard layout, search, stat cards, entities list + live-search filter links (#215) |
| `tests/test_api_routes.py` | Versioned API routes — cities, stats, export, health checks + scrape freshness |
| `tests/test_source_viewer.py` | Source viewer route — snapshot dispatch, iframe rendering, not-found cases |
| `tests/test_admin_auth.py` | Admin authentication — header extraction, env-var fallback, redirect/403 |
| `tests/test_admin_routes.py` | Admin route rendering and redirects |
| `tests/test_admin_entity_routes.py` | Classification review routes — queue page, actions, safe `next`, error codes, Jev re-classify |
| `tests/test_admin_users.py` | Admin user management routes — add, remove, list, self-removal guard |
| `tests/test_admin_endorsements.py` | Admin endorsement/substance routes — add, remove, set-endorsements |
| `tests/test_data_migration.py` | Run-once migration framework — idempotency, ordering |
| `tests/test_engine.py` | Async engine factory |
| `tests/test_db.py` | Location/source/provenance helper functions |
| `tests/test_schema.py` | Alembic migration tests (require `TEST_DATABASE_URL`) |
| `tests/test_pipeline.py` | Ingestion pipeline — insert, dedup, endorsements, entities (require `TEST_DATABASE_URL`) |
| `tests/test_queries.py` | Search, filters, stats, export, entity queries (require `TEST_DATABASE_URL`) |
| `tests/test_link_records.py` | Record linking — bulk, incremental, outcome status (require `TEST_DATABASE_URL`) |
| `tests/test_endorsements.py` | Endorsement pipeline — processing, repair, alias system (require `TEST_DATABASE_URL`) |
| `tests/test_endorsements_seed.py` | Endorsement seeding and code-map repair (require `TEST_DATABASE_URL`) |
| `tests/test_endorsements_admin.py` | Admin endorsement helpers — similarity, suggestions, dismissals |
| `tests/test_entities.py` | Entity normalization — extraction, dedup, reprocessing, reclassify through the resolver (require `TEST_DATABASE_URL`) |
| `tests/test_entity_classification.py` | Classification resolver, manual rows, protected/confirmed-noise sets, `ignore-jev` (require `TEST_DATABASE_URL`) |
| `tests/test_classification_review.py` | Audited operator actions, review-queue membership/order, entity detail, health/spend (require `TEST_DATABASE_URL`) |
| `tests/test_jev_client.py` | Jev transport — request shape, 429/529 retry, auth errors, malformed responses fail the batch |
| `tests/test_jev_classifier.py` | Jev runs — classify once, modes, advisory lock, failure stops, run rows, check-in payload (require `TEST_DATABASE_URL`) |
| `tests/test_integrity.py` | Integrity checks — all check and fix functions (require `TEST_DATABASE_URL`) |
| `tests/test_scraper.py` | Scraper — content hash deduplication, redundant data cleanup, run history + freshness |
| `tests/test_address_validator.py` | Address validation DB layer — standardize, validate, backfill; renewal TTLs, early renewal and a walked rolling 24h budget (#195) |
| `tests/test_admin_audit.py` | Admin audit log — log_action, get_audit_log, filters, pagination |
| `tests/test_substances.py` | Regulated substance CRUD — get, add, remove, set-endorsements |
| `tests/test_htmx_fixtures.py` | htmx harness fixtures are byte-identical to a fresh render of the templates (#232) |
| `tests/js/test_htmx.js` | Vendored htmx under jsdom: `/entities` OOB live search (#215); Back shows the URL's query in every control, never a cached snapshot (#231) |
| `tests/conftest.py` | Shared fixtures: sample record dicts, pg_conn/pg_engine (skipped without TEST_DATABASE_URL) |
| `tests/fixtures/` | Minimal HTML files exercising each record type and section |

## Data Source

All data is sourced from the Washington State Liquor and Cannabis Board's public licensing activity page:

**[https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp](https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp)**

> *STATEWIDE New License Applications, Approvals, and Discontinuances (for past 30 days, in order of most recent date)*

This is an unofficial tracker and is not affiliated with or endorsed by the WSLCB.

## License

MIT
