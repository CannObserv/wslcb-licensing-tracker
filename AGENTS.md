# AGENTS.md — WSLCB Licensing Tracker

Guidance for AI agents working on this project.

## Project Overview

This is a Python web application that scrapes Washington State Liquor and Cannabis Board (WSLCB) licensing activity data, stores it in SQLite, and serves a searchable web interface. The source page only shows a rolling 30-day window; this tracker builds a permanent historical archive.

**Live instance:** https://wslcb-licensing-tracker.exe.xyz:8000/
**Repo:** https://github.com/CannObserv/wslcb-licensing-tracker

## Architecture at a Glance

```
src/wslcb_licensing_tracker/scraper.py ─┐
src/wslcb_licensing_tracker/backfill_snapshots.py ─┼─→ pipeline.py ─→ data/wslcb.db (SQLite + FTS5) ←─ app.py (FastAPI) ─→ templates/ (Jinja2 + HTMX)
backfill_diffs.py ───┘                                                         ←─ display.py (presentation)
                       ↘ data/wslcb/licensinginfo/[yyyy]/[date]/*.html (archived snapshots)

license_records → locations (FK: location_id, previous_location_id)
                → record_endorsements → license_endorsements
```

- **No build step.** The frontend uses Tailwind CSS via CDN and HTMX. No node_modules, no bundler.
- **Source layout.** All Python source lives in `src/wslcb_licensing_tracker/`. The CLI entry point is `wslcb` (installed via `uv sync`). Run as `python -m wslcb_licensing_tracker.cli <subcommand>` or `wslcb <subcommand>`.
- **Small modules.** The DB layer is split into `db.py` (connections/constants/helpers) and `schema.py` (DDL/migrations/FTS). Entity normalization lives in `entities.py`; search/read queries in `queries.py`; record insertion and enrichment in `pipeline.py`. All ingestion flows through `pipeline.py`. Dependencies flow one-way: `pipeline → db, schema, endorsements, entities, link_records, address_validator`; `queries → db, schema, entities, endorsements, display, link_records`.
- **SQLite is the only datastore.** No Redis, no Postgres. WAL mode is enabled for concurrent reads.

## Key Files

| File | Purpose | Notes |
|---|---|---|
| `src/wslcb_licensing_tracker/db.py` | Connection management, constants, and core helpers | `get_connection()`, `get_db()`, `DATA_DIR`, `DB_PATH`, source type constants, `WSLCB_SOURCE_URL`, `_normalize_raw_address()`. `SOURCE_ROLE_PRIORITY` — shared dict `{"first_seen": 0, "repaired": 1, "confirmed": 2}` imported by both `db.py` and `display.py` to avoid circular imports. `US_STATES` — dict of US state code → full name; used by the state filter dropdown (defined here as pure reference data). Location helper: `get_or_create_location()`. Source helpers: `get_or_create_source()`, `link_record_source()`. Provenance queries: `get_primary_source(conn, record_id) → dict | None` — returns the single most-relevant source for a record by role priority (`first_seen > repaired > confirmed`), then snapshot presence, then newest `captured_at`; `get_record_sources(conn, record_id) → list[dict]` — returns all provenance sources newest-first. |
| `src/wslcb_licensing_tracker/schema.py` | DDL, migrations, FTS | All table creation, `PRAGMA user_version` migration framework, FTS5 setup, data seeding. `init_db()`, `migrate()`, `MIGRATIONS` list. `_table_exists(conn, name)`, `_column_exists(conn, table, column)` — private introspection helpers used by migration guards; exported for testability (both args must be trusted schema-name literals). Migration 002 (`enrichment_tracking`) adds `record_enrichments` table and `raw_*` shadow columns. |
| `src/wslcb_licensing_tracker/entities.py` | Entity (applicant) normalization | `get_or_create_entity()`, `backfill_entities()`, `get_record_entities()`, `get_entity_by_id()`, `merge_duplicate_entities()`, `clean_applicants_string()`, `clean_record_strings()`, `parse_and_link_entities()`, `reprocess_entities()`. `strip_duplicate_marker(name)` — strips WSLCB `DUPLICATE` annotation tokens (e.g. `(DUPLICATE)`, `DUPLICATE`, `*DUPLICATE*`, unclosed `(DUPLICATE`) from a single applicant name; called by both `clean_applicants_string()` and `parse_and_link_entities()`. `ADDITIONAL_NAMES_MARKERS` — exported `frozenset` of WSLCB meta-label strings (`"ADDITIONAL NAMES ON FILE"` and typo variant) that must be skipped during entity creation; imported by `queries.py` for ingest-time flag detection. |
| `src/wslcb_licensing_tracker/queries.py` | Record search and read queries | `search_records()`, `export_records()`, `export_records_cursor()` (streaming generator variant — yields one dict per row directly from the SQLite cursor; used by the `/export` route via `StreamingResponse`), `get_filter_options()`, `get_cities_for_state()`, `get_stats()`, `enrich_record()`, `hydrate_records()`, `get_record_by_id()`, `get_related_records()`, `get_entity_records()`, `get_entities()`. `get_entities(conn, *, q, entity_type, sort, page, per_page) → dict` — paginated, searchable entity list; returns `{"entities": [...], "total": int}`; `sort` accepts `"count"` (default, record_count DESC) or `"name"` (ASC); any non-`"name"` value defaults to count-sort; `page` is clamped to `max(1, page)`. `invalidate_filter_cache()` — clears the 5-minute in-process filter option cache; call after any admin mutation that changes endorsements or substances. `_build_where_clause()` accepts `endorsements: list[str]` for multi-select OR-semantics filtering (also handles mixed known/unknown names gracefully). `_EXPORT_SELECT` — module-level SQL constant for CSV export; bakes in `DATA_GAP_CUTOFF`, `PENDING_CUTOFF_DAYS`, and `LINKABLE_TYPES` at import time (no runtime `.format()`). Uses three correlated subqueries against `record_links` (via `idx_record_links_new`) instead of a materialised window-function CTE, so the outer WHERE filter is applied before link resolution. `outcome_status` uses `CASE (subquery) WHEN` form to evaluate the `section_type` lookup once rather than twice. Re-exports `insert_record` (from `pipeline`), `get_primary_source` / `get_record_sources` / `US_STATES` (from `db`) for backward compatibility. |
| `src/wslcb_licensing_tracker/endorsements.py` | License type normalization (core) | Loads `SEED_CODE_MAP` from `seed_code_map.json` at module init (103 codes). `process_record()`, `discover_code_mappings()`, `repair_code_name_endorsements()`, `_merge_endorsement()` (shared merge helper), `get_endorsement_options()`, `get_record_endorsements()`, `resolve_endorsement()`, `set_canonical_endorsement()`, `rename_endorsement()`, `get_endorsement_groups()`. Re-exports all public names from `endorsements_admin` and `substances` for backward compatibility. |
| `src/wslcb_licensing_tracker/seed_code_map.json` | Seed data: WSLCB numeric code → endorsement name(s) | 103-entry JSON dict loaded by `endorsements.py` at module init. Keys are WSLCB license class ID strings; values are lists of endorsement names. Edit this file (not the Python module) when adding or correcting seed mappings. |
| `src/wslcb_licensing_tracker/endorsements_admin.py` | Admin UI helpers for endorsement management | Similarity algorithm (`endorsement_similarity()`, `_sim_tokenize()`, `_sim_features()`), `get_endorsement_list()`, `suggest_duplicate_endorsements()`, `dismiss_suggestion()`, `get_code_mappings()`, `add_code_mapping()`, `remove_code_mapping()`, `create_code()`. No dependency on `admin_audit` — audit logging is the caller's responsibility. |
| `src/wslcb_licensing_tracker/substances.py` | Regulated substance CRUD | `get_regulated_substances()`, `get_substance_endorsement_ids()`, `set_substance_endorsements()`, `add_substance()`, `remove_substance()`. No dependency on `admin_audit` — audit logging is the caller's responsibility. Route handlers in `app.py` call `log_action()` directly after each mutation. |
| `src/wslcb_licensing_tracker/log_config.py` | Centralized logging setup | `setup_logging()` configures root logger; auto-detects TTY vs JSON format. Called once per entry point. |
| `src/wslcb_licensing_tracker/parser.py` | Pure HTML/diff parsing | All parsing functions, file discovery, constants. No DB access, no side effects. Only depends on stdlib + bs4/lxml + `db.DATA_DIR`. `extract_tbody_from_snapshot(path, section_type, license_number, record_date, application_type) → str | None` — locates and returns the raw outer HTML of the matching `<tbody>` in a full HTML snapshot file. `extract_tbody_from_diff(path, ...) → str | None` — reconstructs a `<tbody>` from added (then removed) lines of a unified diff file. `_match_key(tbody, ...)` — shared BeautifulSoup key-matching helper. `_extract_tbody_lines(lines) → list[list[str]]` — splits flat diff lines into per-record groups, handling both `<tbody>`-wrapped and bare-`<tr>` formats. |
| `src/wslcb_licensing_tracker/scraper.py` | Fetches and parses the WSLCB page | Exports `scrape()`, `compute_content_hash()`, `get_last_content_hash()`, `cleanup_redundant_scrapes()`. Logs to `scrape_log` table. Archives source HTML. Uses `pipeline.ingest_batch()` for record insertion. Skips parse/ingest when content hash matches last successful scrape (`status='unchanged'`). Use `cli.py scrape` to run. |
| `src/wslcb_licensing_tracker/cli.py` | Unified CLI entry point | Argparse subcommands for all operational tasks. Includes `cleanup-redundant` for removing data from zero-new-record scrapes. Replaces `python scraper.py --flag` pattern. |
| `src/wslcb_licensing_tracker/backfill_snapshots.py` | Ingest + repair from archived snapshots | Two-phase: (1) insert new records via `pipeline.ingest_batch()`, (2) repair broken ASSUMPTION/CHANGE OF LOCATION records. Safe to re-run. Address validation deferred to `cli.py backfill-addresses`. |
| `src/wslcb_licensing_tracker/address_validator.py` | Client for address validation API | Calls `POST /api/v1/validate` at `https://address-validator.exe.xyz:8000`. API key in `./env` file. Graceful degradation on failure. Exports `backfill_addresses()` and `refresh_addresses()` (both accept `rate_limit: float = 0.1` seconds). `validate_location()` uses a two-tier write: when provider confirms (`address_line_1` non-null) it writes all `std_address_line_1/2`, `std_city`, `std_region`, `std_postal_code`, `std_country`, `validated_address`, `validation_status`, `dpv_match_code`, `latitude`, `longitude`, and sets `address_validated_at`; for `not_confirmed`/`unavailable` it writes only `validation_status` and `dpv_match_code` and leaves `address_validated_at` NULL so `backfill-addresses` retries. `country` validated as ISO 3166-1 alpha-2 before storage. `warnings` list from the API is logged via `logger.warning()`; not stored in the DB. |
| `src/wslcb_licensing_tracker/app.py` | FastAPI web app | Runs on port 8000. Mounts `/static`, uses Jinja2 templates. Uses `@app.lifespan`. Public routes only — admin routes are in `admin_routes.py`, included via `app.include_router()` near the app setup block. `admin_routes.init_router(_tpl)` is called after `_tpl` is defined (must precede first request). `GET /source/{source_id}/record/{record_id}` — public HTMX partial; validates source↔record link, dispatches to `extract_tbody_from_snapshot` or `extract_tbody_from_diff` based on `source_type`, renders `partials/source_viewer.html`. `srcdoc_attr` is built server-side with `html.escape()` to correctly encode the full iframe page HTML for the attribute context. |
| `src/wslcb_licensing_tracker/api_routes.py` | FastAPI `APIRouter` for all `/api/v1/*` routes | All versioned public API handlers. `APIRouter(prefix="/api/v1", tags=["api"])`. JSON responses use `{"ok": bool, "message": str, "data": ...}` envelope via `_ok()` helper; CSV export (`/api/v1/export`) is exempt (returns `StreamingResponse`). Endpoints: `GET /api/v1/cities` (state→cities lookup, Cache-Control 5 min), `GET /api/v1/stats` (aggregate DB stats), `GET /api/v1/export` (streaming CSV), `GET /api/v1/health` (DB probe, 200/503, no auth). Tests must patch `api_routes.get_db`; `test_routes.py` `_make_client` also patches it for export tests. |
| `src/wslcb_licensing_tracker/admin_routes.py` | FastAPI `APIRouter` for all `/admin/*` routes | All admin route handlers extracted from `app.py`. Uses `init_router(tpl_fn)` to receive the shared `_tpl()` helper at startup (must be called before the first request; raises `RuntimeError` otherwise). Tests must patch `admin_routes.get_db` (not `app.get_db`) when testing admin endpoints. Public-route tests (`test_routes.py`) continue to patch `app.get_db` since those handlers were not moved. |
| `templates/` | Jinja2 HTML templates | `base.html` is the layout (includes Tailwind config with brand colors). `partials/results.html` is the HTMX target. `partials/record_table.html` is the shared record table (used by results and entity pages). `404.html` handles not-found errors. |
| `src/wslcb_licensing_tracker/pipeline.py` | Unified ingestion pipeline | `insert_record()` — canonical record-insertion function (dedup, location resolution, name cleaning, entity linking). `ingest_record()`, `ingest_batch()`, `IngestOptions`, `IngestResult`, `BatchResult` — full enrichment pipeline wrapping `insert_record()`; all ingestion paths (scraper, snapshot backfill, diff backfill) go through this module. `_applicants_have_additional_names(*strings)` — returns `True` if any semicolon-delimited token in the given applicant strings matches `ADDITIONAL_NAMES_MARKERS`; used internally by `insert_record()` to set `has_additional_names`. Tracks enrichment completion via `_record_enrichment()` after each step. Exports step name constants: `STEP_ENDORSEMENTS`, `STEP_ENTITIES`, `STEP_ADDRESS`, `STEP_OUTCOME_LINK`. |
| `src/wslcb_licensing_tracker/display.py` | Presentation formatting | `format_outcome()`, `summarize_provenance()`, `OUTCOME_STYLES`. Owns CSS classes, icons, badge text; domain layer returns semantic data only. `summarize_provenance()` emits `primary_source_id` per group — the `id` of the highest-priority source of that type (role priority from `db.SOURCE_ROLE_PRIORITY`, then snapshot presence, then newest `captured_at`). `_ROLE_PRIORITY` — module-level alias for `db.SOURCE_ROLE_PRIORITY`. |
| `src/wslcb_licensing_tracker/link_records.py` | Application→outcome record linking | Bidirectional nearest-neighbor matching with ±7-day tolerance. `build_all_links()`, `link_new_record()`, `get_outcome_status()` (semantic only — no CSS), `get_reverse_link_info()`, `outcome_filter_sql()`. Internally: `_link_section(mode)` handles bulk linking (parameterized for approval/discontinuance), `_link_incremental(direction)` handles single-record linking (parameterized for forward/backward). |
| `src/wslcb_licensing_tracker/backfill_diffs.py` | Ingest from CO diff archives | Orchestrates insertion from diff-extracted records via `pipeline.ingest_record()`. Parsing logic lives in `parser.py`. Safe to re-run. |
| `src/wslcb_licensing_tracker/backfill_provenance.py` | One-time provenance backfill | Re-processes all snapshots to populate `record_sources` junction links for existing records. Safe to re-run. |
| `src/wslcb_licensing_tracker/integrity.py` | Database integrity checks | `check_orphaned_locations()`, `check_broken_fks()`, `check_unenriched_records()`, `check_endorsement_anomalies()`, `check_entity_duplicates()`, `run_all_checks()`, `fix_orphaned_locations()`. Used by `cli.py check`. |
| `src/wslcb_licensing_tracker/rebuild.py` | Rebuild database from sources | `rebuild_from_sources()`, `compare_databases()`, `RebuildResult`, `ComparisonResult`. Four-phase rebuild: (1) replay diff archives, (2) replay HTML snapshots, (3) endorsement discovery, (4) build outcome links. Verification compares natural keys between production and rebuilt DBs. Used by `cli.py rebuild`. |
| `templates/entities.html` | Entities landing page | `GET /entities` — searchable, paginated list of all applicant entities. Search input (HTMX live, 300 ms debounce), type filter buttons (All / People / Organizations), sort toggle (Most Active / A–Z). HTMX target `#entities-results`; partial swapped from `partials/entities_results.html`. Route param `entity_type` uses `Query(alias="type")` to map the `?type=` query-string key without shadowing the Python builtin. |
| `templates/partials/entities_results.html` | Entities results partial | HTMX-swapped into `#entities-results`; renders results table (name link → `/entity/{id}`, type badge, record count), row-count line, Clear filters link, and Prev/Next pagination. |
| `templates/entity.html` | Entity detail page | Shows all records for a person or organization, with type badge and license count. |
| `templates/partials/source_viewer.html` | Source viewer HTMX partial | Rendered by `GET /source/{source_id}/record/{record_id}`; shows source metadata header bar, sandboxed `<iframe srcdoc=…>` with the matched `<tbody>` HTML, or a "not found" notice. Close button clears `#source-viewer` innerHTML. Receives `source`, `record`, `found` (bool), `srcdoc_attr` (pre-escaped attribute value built server-side). |
| `static/images/` | Cannabis Observer brand assets | `cannabis_observer-icon-square.svg` (icon) and `cannabis_observer-name.svg` (wordmark). See **Style Guide** for usage. |
| `src/wslcb_licensing_tracker/admin_auth.py` | Admin authentication middleware | `require_admin()` FastAPI dependency (redirects to exe.dev login or 403). `get_current_user()` non-enforcing variant (caches result on `request.state`). `AdminRedirectException` sentinel class. Reads `X-ExeDev-Email` / `X-ExeDev-UserID` proxy headers; falls back to `ADMIN_DEV_EMAIL` / `ADMIN_DEV_USERID` env vars for local dev. |
| `src/wslcb_licensing_tracker/admin_audit.py` | Admin audit log | `log_action(conn, email, action, target_type, target_id=None, details=None)` inserts one audit row (caller commits); serialises `details` dict to JSON. `get_audit_log(conn, page, per_page, filters)` returns `(rows, total_count)` with optional filters: `action`, `target_type`, `admin_email`, `date_from`/`date_to`; each row includes `details_parsed` (decoded dict or `None`). |
| `templates/admin/endorsements.html` | Endorsement management UI | Three-tab interface: (1) **Endorsement List** — searchable flat table of all endorsements with status badges, record counts, code associations, inline Rename, and checkbox-driven alias creation; (2) **Duplicate Suggestions** — algorithmically surfaced near-duplicate pairs with Accept/Dismiss actions; (3) **Code Mappings** — all WSLCB numeric codes with add/remove endorsement and create-new-code actions. |
| `templates/admin/users.html` | Admin user management UI | Lists all admin users (email, role, added date, added-by). Inline add-user form (email input + button) and per-row remove buttons with JS confirm. Shows "you" label for the currently signed-in admin; remove button hidden for self. Error banner driven by `?error=` query param. |
| `templates/admin/dashboard.html` | System dashboard | Record counts (total, by section type, last 24 h/7 d), last 5 scrape runs with status badges, data-quality checklist (orphaned locations, missing endorsements/entities, unresolved codes, placeholder endorsements), quick-link buttons to Endorsements / Audit Log / Users. |
| `skills/` | Agent Skills | Local override skills (`reviewing-code-claude`, `shipping-work-claude`) and symlinks to `vendor/gregoryfoster-skills/` global skills (`reviewing-architecture-claude`, `managing-skills-claude`). See **Agent Skills** section. |
| `pyproject.toml` | Project metadata and dependencies | Managed by `uv`. Install all deps with `uv sync --dev`. Lock file: `uv.lock`. |
| `pyproject.toml` `[tool.pytest.ini_options]` | Pytest configuration | Integrated into `pyproject.toml`: `testpaths`, `pythonpath = ["src"]`, `asyncio_mode`. |
| `tests/conftest.py` | Shared test fixtures | In-memory DB, sample record dicts, `FIXTURES_DIR` path constant. |
| `tests/test_parser.py` | Parser tests | Tests for all `parser.py` functions using static HTML fixtures. |
| `tests/test_db.py` | Connection/constant tests | `db.py` connections, pragmas, raw address normalization, constant values. |
| `tests/test_schema.py` | Migration framework tests | `PRAGMA user_version`, migration runner, existing DB detection, registry sanity checks. |
| `tests/test_database.py` | Database helper tests | `db.py` helpers: schema init, location/source helpers, provenance linking. |
| `tests/test_pipeline.py` | Pipeline tests | `ingest_record()` and `ingest_batch()`: insertion, dedup, endorsements, provenance, entities, outcome linking. |
| `tests/test_display.py` | Display tests | `format_outcome()` and `summarize_provenance()`: all outcome statuses, provenance grouping, date ranges. |
| `tests/test_queries.py` | Query tests | Search, filter, stats, and export functions. Record insertion helpers import `insert_record` from `pipeline` (its canonical home); provenance helpers import `get_primary_source` from `db`. |
| `tests/test_link_records.py` | Link records tests | `_link_section()`, `_link_incremental()`, `build_all_links()`, `get_outcome_status()`, `get_reverse_link_info()` — bulk and incremental linking. |
| `tests/test_endorsements.py` | Endorsement tests | `_merge_endorsement()`, `process_record()`, `merge_mixed_case_endorsements()`, `repair_code_name_endorsements()`, query helpers. Revised UI helpers: `endorsement_similarity` (similarity algorithm including +/− polar-token hard rule), `get_endorsement_list`, `suggest_duplicate_endorsements`, `dismiss_suggestion`, `get_code_mappings`, `add_code_mapping`, `remove_code_mapping`, `create_code`. Regulated substance CRUD: `get_regulated_substances`, `get_substance_endorsement_ids`, `set_substance_endorsements`, `add_substance`, `remove_substance`. Substance functions are now imported from `substances` directly; audit logging tests verify the caller-delegates pattern. |
| `tests/test_admin_endorsements.py` | Admin substance route tests | Uses TestClient with cross-thread in-memory DB fixture; patches `admin_routes.get_db`. Covers: `GET /admin/endorsements` (default tab, all section params), `POST /admin/endorsements/substances/add` (inserts row, audit-logged, blank-name error, non-admin rejected), `POST /admin/endorsements/substances/remove` (deletes row, cascades junction, audit-logged, non-admin rejected), `POST /admin/endorsements/substances/set-endorsements` (replaces associations, clears all, audit-logged, non-admin rejected). |
| `tests/test_integrity.py` | Integrity check tests | All check functions, fix functions, aggregate runner. |
| `tests/test_rebuild.py` | Rebuild tests | `rebuild_from_sources()`: empty data, DB creation, overwrite protection, force mode, snapshot ingestion, timing. `compare_databases()`: identical DBs, missing records, extra records, per-section breakdown. |
| `tests/test_scraper.py` | Scraper tests | `compute_content_hash()`, `get_last_content_hash()`, `cleanup_redundant_scrapes()`: hash computation, last-hash retrieval, redundant data cleanup. |
| `tests/test_admin_auth.py` | Auth middleware tests | `require_admin()`, `get_current_user()`: header extraction, env-var fallback, admin lookup, redirect and 403 behaviour. |
| `tests/test_admin_audit.py` | Audit log tests | `log_action`: insert, target_id, NULL details, dict round-trip. `get_audit_log`: pagination, newest-first ordering, all filter types (action, target_type, admin_email, date_from, date_to), empty-table edge case. |
| `tests/test_admin_users.py` | Admin user management route tests | Uses TestClient with cross-thread in-memory DB fixture; patches `admin_routes.get_db`. Covers: `GET /admin/users` (list, non-admin forbidden), `POST /admin/users/add` (inserts row, audit-logged, duplicate no-crash), `POST /admin/users/remove` (deletes row, audit-logged, self-removal rejected), `GET /admin/` (renders, empty scrape log, correct counts). |
| `tests/test_endorsements.py` | Endorsement tests (extended) | Alias system: `resolve_endorsement` (no-alias, aliased, canonical passthrough), `set_canonical_endorsement` (creates aliases, idempotent), `rename_endorsement` (creates named endorsement + alias, reuses existing name), alias resolution in `get_endorsement_options` (variants excluded) and `get_record_endorsements` (canonical name returned). Admin helpers imported from `endorsements_admin`; substance helpers imported from `substances`. |
| `tests/test_routes.py` | Main public route tests | Uses TestClient with cross-thread in-memory DB fixture and mocked `get_stats`. Covers: dashboard section order (HTML comment anchors as landmarks), search placeholder consistency across `/` and `/search`, stat card mobile layout and link behaviour, `TestAdditionalNamesNotice` (notice shown/hidden by `has_additional_names` flag, including fallback branch when entity list is empty). |
| `tests/test_source_viewer.py` | Source viewer route tests | Uses TestClient with cross-thread in-memory DB fixture; parser extractor functions mocked. Covers: 404 on invalid IDs, 404 when no `record_sources` link, iframe rendered when extractor returns HTML, not-found message when extractor returns `None`, diff sources dispatched to `extract_tbody_from_diff`. |
| `tests/fixtures/` | HTML test fixtures | Minimal realistic HTML for each record type and section. |
| `tests/fixtures/snapshot_two_records.html` | Snapshot parser fixture | Minimal full-page HTML snapshot containing two `new_application` records; used by `test_extract_tbody_from_snapshot_*` tests. |
| `tests/fixtures/diff_two_records.txt` | Diff parser fixture | Minimal unified diff with one added and one removed record block; used by `test_extract_tbody_from_diff_*` tests. |

## Frozen vs. Derived Data Contract

The schema is divided into two logical layers. This distinction is foundational: it determines what can be safely regenerated and what must never be altered after ingest.

### Frozen layer (immutable after ingest)

These values are captured exactly as parsed from the source HTML and must never be altered:

| Column / Table | Notes |
|---|---|
| `license_records.license_type` | Raw text or numeric code from the WSLCB source page |
| `license_records.raw_*` shadow columns | As-parsed names before any cleaning (backup of the original source values) |
| `sources` / `record_sources` | Provenance chain — which source artifact introduced or confirmed each record |

### Derived layer (regenerable from frozen data + current configuration)

These tables are *computed outputs* — they can be safely deleted and rebuilt without data loss:

| Table | Regenerated by | CLI command |
|---|---|---|
| `record_endorsements` | `endorsements.process_record()` from `license_type` + `endorsement_codes` | `cli.py reprocess-endorsements` |
| `record_entities` | `entities.parse_and_link_entities()` from `applicants` / `previous_applicants` | `cli.py reprocess-entities` |
| `record_links` | `link_records.build_all_links()` from matching rules | `cli.py rebuild-links` |

Because `record_endorsements` is derived, `process_record()` is **idempotent**: it deletes existing rows for a record before inserting fresh ones. This means code-mapping edits or alias changes can be applied retroactively without risk.

### Reprocessing endorsements

When endorsement code mappings change (new code discovered, alias set, mapping corrected), run:

```bash
wslcb reprocess-endorsements              # all records
wslcb reprocess-endorsements --code 394   # records with license_type '394,'
wslcb reprocess-endorsements --record-id 12345  # single record
wslcb reprocess-endorsements --dry-run    # preview without writing
```

The `record_enrichments.version` stamp is bumped to `'2'` for every reprocessed record, enabling queries like:

```sql
-- Records that haven't been reprocessed with the current logic:
SELECT record_id FROM record_enrichments
WHERE step = 'endorsements' AND CAST(version AS INTEGER) < 2;
```

## Database Schema

Full table and column reference, including the migration framework, lives in
[`docs/SCHEMA.md`](docs/SCHEMA.md). The summary below covers the most
frequently referenced design decisions.

- **Frozen layer** — `license_type`, `raw_*` shadow columns, and `sources`/`record_sources`
  are captured exactly as parsed and must never be altered. See *Frozen vs. Derived Data Contract*.
- **Natural-key uniqueness** — `license_records` is unique on `(section_type, record_date, license_number, application_type)`.
- **Location sharing** — `locations` holds one row per unique raw address; all records pointing to the same address share the row. `get_or_create_location()` in `db.py` handles the upsert.
- **Endorsement alias resolution** — `endorsement_aliases` maps variant endorsement rows to a canonical row; never modifies `record_endorsements`. Resolution happens at query time via LEFT JOIN.
- **FTS** — `license_records_fts` is a content-FTS5 table kept in sync by triggers; never write to it directly.
- **Migrations** — `PRAGMA user_version` + the `MIGRATIONS` list in `schema.py`; add new columns to `_m001_baseline()` *and* a new migration function. See `docs/SCHEMA.md` for the full history.


## Agent Skills

This project follows the [agentskills.io](https://agentskills.io) spec. Skills live in the `skills/` directory and are auto-discovered by the agent framework. A skill is either a local override (committed directory) or a symlink to an external skills repo vendored as a git submodule.

### External skill repos (git submodules)

| Repo | Submodule path |
|---|---|
| `gregoryfoster/skills` | `vendor/gregoryfoster-skills/` |
| `obra/superpowers` | `vendor/obra-superpowers/` |

After cloning this project, initialize submodules:

```bash
git submodule update --init --recursive
```

**Submodule freshness is enforced automatically.** A `UserPromptSubmit` hook in `.claude/settings.json` runs `git submodule update --remote --merge` once per calendar day (lock file: `/tmp/wslcb-submodule-update-YYYYMMDD`) and auto-commits any updated refs. No manual pull is needed during normal use.

If any submodule ref changed outside of a Claude Code session, commit it manually:

```bash
git add vendor/gregoryfoster-skills vendor/obra-superpowers
git commit -m "chore: update skills submodules"
```

### Claude Code skill discovery (.claude/skills/)

Claude Code discovers project skills from `.claude/skills/<name>/SKILL.md`, not from the project root `skills/` directory. This project wires both discovery systems through a two-level symlink chain:

```
.claude/skills/<name>  →  ../../skills/<name>  →  (dir or ../vendor/…)
```

This means:
- Local override directories in `skills/` automatically shadow vendor skills in Claude Code too
- No duplication of symlink targets — `skills/` is the single source of truth
- Adding a new skill to `skills/` requires a matching symlink in `.claude/skills/`

To add a new skill symlink to both discovery systems:

```bash
ln -s ../vendor/<owner>-<repo>/skills/<skill-name> skills/<skill-name>
ln -s ../../skills/<skill-name> .claude/skills/<skill-name>
git add skills/<skill-name> .claude/skills/<skill-name>
git commit -m "feat: add <skill-name> skill"
```

The `CLAUDE.md` file in the project root contains `@AGENTS.md`, which causes Claude Code to auto-load this file at the start of every conversation.

To add a new external skill repo, follow the `managing-skills-claude` skill (available at `vendor/gregoryfoster-skills/skills/managing-skills-claude/`).

### Available skills

| Skill | Source | Triggers |
|---|---|---|
| `reviewing-code-claude` | Local override | CR, code review, perform a review |
| `reviewing-architecture-claude` | Symlink → `vendor/gregoryfoster-skills/` | AR, architecture review, architectural review |
| `shipping-work-claude` | Local override | ship it, push GH, close GH, wrap up |
| `managing-skills-claude` | Symlink → `vendor/gregoryfoster-skills/` | add skill repo, add external skills, manage skills, update skills submodule |
| `brainstorming` | Local override | brainstorm, design this, let's design |
| `using-git-worktrees` | Symlink → `vendor/obra-superpowers/` | set up worktree, create worktree |
| `systematic-debugging` | Symlink → `vendor/obra-superpowers/` | (description-driven¹) |
| `verification-before-completion` | Symlink → `vendor/obra-superpowers/` | (description-driven¹) |
| `test-driven-development` | Symlink → `vendor/obra-superpowers/` | (description-driven¹) |
| `writing-plans` | Symlink → `vendor/obra-superpowers/` | write plan, implementation plan |
| `writing-skills` | Symlink → `vendor/obra-superpowers/` | write skill, new skill, author skill |
| `subagent-driven-development` | Symlink → `vendor/obra-superpowers/` | subagent dev, dispatch agents |
| `dispatching-parallel-agents` | Symlink → `vendor/obra-superpowers/` | parallel agents |

¹ These obra/superpowers skills have no explicit trigger phrases — their SKILL.md descriptions instruct the agent when to apply them. The agent must use `systematic-debugging` whenever encountering any bug, test failure, or unexpected behavior; `verification-before-completion` before any completion claim or commit; and `test-driven-development` before writing any implementation code.

### Local overrides

A committed directory in `skills/` with the same name as a symlinked global skill completely supersedes the global version (no inheritance). The local version is fully self-contained.

| Skill | Override reason |
|---|---|
| `reviewing-code-claude` | Python/SQLite/FastAPI-specific review dimensions; pytest in venv; Iron Law + rationalization-prevention table; verification gate before commit |
| `shipping-work-claude` | Concrete pytest command; Conventional Commits convention; systemd restart reminder; Iron Law + HARD-GATE on partial issue closure |
| `brainstorming` | Project conventions (docs/plans/ path, Conventional Commits for design doc); invokes using-git-worktrees after design approval; writing-plans optional not mandatory; proactive-suggestion mode instead of universal hard gate |

### Authoring new skills

Follow the `writing-skills` TDD cycle:
1. **RED** — run pressure scenarios (or mental model) without the skill; document where the agent fails
2. **GREEN** — write a minimal SKILL.md that addresses those specific failures
3. **REFACTOR** — find new rationalizations, close loopholes, re-test

Skill frontmatter must include `triggers` in `metadata` for AGENTS.md discovery. New project-specific skills go in `skills/<name>/` as committed directories. Cross-project skills belong in `gregoryfoster/skills` (add via `managing-skills-claude`).

## Conventions

### Python
- Python 3.12+ with virtualenv managed by `uv` at `./.venv/`
- Dependencies declared in `pyproject.toml`; install with `uv sync --dev`
- Use `datetime.now(timezone.utc)` not `datetime.utcnow()` (deprecated)
- Module docstrings on every `.py` file

### Logging
- **Never use `print()` for operational output** except in CLI summary lines (final human-readable output from `cli.py` subcommands). All other output goes through Python's `logging` module.
- Each module declares `logger = logging.getLogger(__name__)` at the top.
- Entry points (`app.py` lifespan, `scraper.py` main, `backfill_snapshots.py` main, `db.py` `__main__`) call `setup_logging()` from `log_config.py` before doing any work.
- Log levels:
  - `logger.debug()` — progress counters, verbose operational detail ("Fetched 12,000,000 bytes", "Found 3 data sections")
  - `logger.info()` — meaningful events (records inserted, scrape complete, migrations, summaries)
  - `logger.warning()` — recoverable issues (API timeouts, failed validations, missing config)
  - `logger.error()` — unrecoverable failures (scrape crash, missing API key)
- Format auto-detects: human-readable on TTY, JSON lines under systemd/pipe (via `python-json-logger`).
- Use `%s`/`%d` style formatting in log calls (not f-strings) so messages are only formatted if the level is enabled.

### Testing — Red/Green TDD

This project follows a **red/green TDD** discipline for all new code and bug fixes:

1. **Red** — Write a failing test that demonstrates the desired behavior (or reproduces the bug).
2. **Green** — Write the minimum code to make the test pass.
3. **Refactor** — Clean up while keeping tests green.

**Rules:**
- Every new feature, bug fix, or behavioral change **must** have a test written **before** the implementation.
- All tests must pass (`pytest`) before committing. Run `uv run pytest tests/ -v` to verify.
- Tests must be fast: no network calls, no disk-based databases. Use the in-memory `db` fixture from `conftest.py`.
- HTML parsing tests use static fixture files in `tests/fixtures/`; keep them minimal and realistic.
- Parser tests (`test_parser.py`) test pure functions — HTML in, dicts out. No database.
- Database/query tests (`test_db.py`, `test_schema.py`, `test_database.py`, `test_queries.py`) use the `db` fixture or in-memory connections.
- `test_database.py` imports from `db` and `schema` directly (the `database.py` backward-compat shim no longer exists).
- Pipeline tests (`test_pipeline.py`) verify end-to-end ingestion: insert, endorsements, provenance, entities, outcome linking.
- Display tests (`test_display.py`) test pure presentation formatting — no database.
- Link record tests (`test_link_records.py`) test bulk and incremental linking with the parameterized `_link_section()` and `_link_incremental()` functions.
- Endorsement tests (`test_endorsements.py`) test `_merge_endorsement()`, `process_record()`, and all repair functions.
- Integrity tests (`test_integrity.py`) test each check function and fix function.
- Use the sample record fixtures from `conftest.py` (`standard_new_application`, `assumption_record`, `change_of_location_record`, `approved_numeric_code`, `discontinued_code_name`) for tests that need record dicts.

**Infrastructure:**
- pytest config in `[tool.pytest.ini_options]` in `pyproject.toml`; test discovery in `tests/`
- `tests/conftest.py` — shared fixtures (in-memory DB, sample records, `FIXTURES_DIR` path)
- `tests/fixtures/` — static HTML files for parser tests
- `pyproject.toml` `[dependency-groups] dev` includes `pytest` and `pytest-asyncio`

**Test helper utilities (module-level, `tests/test_routes.py`):**
- `_html_section(html, start_comment, end_comment)` — slices the rendered HTML between two HTML comment anchors; used by all dashboard layout and link tests to isolate a section.
- `_card_tag(section, label)` — given a section slice and a visible label string, walks backwards to find the outer card wrapper element (`<a>` or `<div>`) and returns its full opening tag. Resolves card wrapper type unambiguously by comparing the last `<a ` and `<div` positions before the inner label element.

**When to add tests:**
- Fixing a bug → write a test that fails with the bug, then fix it
- Adding a feature → write tests for the expected behavior first
- Refactoring → ensure existing tests cover the behavior, add more if needed
- Modifying `parser.py` → add/update `test_parser.py` with fixture HTML
- Modifying `db.py` (connections/constants) → add/update `test_db.py`
- Modifying `db.py` (location/source/provenance helpers) → add/update `test_database.py`
- Modifying `schema.py` → add/update `test_schema.py`
- Modifying `queries.py` → add/update `test_queries.py`
- Modifying `pipeline.py` → add/update `test_pipeline.py`
- Modifying `display.py` → add/update `test_display.py`
- Modifying `link_records.py` → add/update `test_link_records.py`
- Modifying `endorsements.py` → add/update `test_endorsements.py`
- Modifying `endorsements_admin.py` → add/update `test_endorsements.py`
- Modifying `substances.py` → add/update `test_endorsements.py`
- Modifying `integrity.py` → add/update `test_integrity.py`
- Modifying `rebuild.py` → add/update `test_rebuild.py`
- Modifying `scraper.py` → add/update `test_scraper.py`

### Templates
- Tailwind CSS via CDN (`<script src="https://cdn.tailwindcss.com">`) with custom `tailwind.config` in `base.html`
- HTMX for partial page updates on search — the `/search` endpoint detects `HX-Request` header
- Custom Jinja2 filters registered in `app.py`: `section_label`, `phone_format`, `build_qs` (builds a URL query string from a dict, handling list values for multi-value params like `endorsement[]`)
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
- `wslcb-web.service` — uvicorn on port 8000, auto-restart; `ExecStartPost` polls `/api/v1/health` up to 5×(3 s) before marking start as failed
- `wslcb-scraper.timer` — fires twice daily at 12:30 AM and 6:30 AM Pacific, ±5 min jitter
- `wslcb-task@.service` — systemd template for oneshot tasks; instance name becomes the `cli.py` subcommand
  - `wslcb-task@scrape.service` — scrape (triggered by the timer)
  - `wslcb-task@refresh-addresses.service` — full address re-validation
  - `wslcb-task@backfill-addresses.service` — backfill un-validated addresses
  - `wslcb-task@backfill-snapshots.service` — recover ASSUMPTION/CHANGE OF LOCATION data from archived HTML
  - `wslcb-task@backfill-provenance.service` — populate source provenance links
  - `wslcb-task@rebuild-links.service` — rebuild application→outcome links
- `wslcb-healthcheck.service` + `wslcb-healthcheck.timer` — curl `/api/v1/health` every 5 min; restart `wslcb-web` on failure (requires sudoers rule below)
- `sudoers.d-wslcb-healthcheck` — sudoers snippet granting `exedev` passwordless `sudo /usr/bin/systemctl restart wslcb-web.service` (scoped to that single command); install once to `/etc/sudoers.d/wslcb-healthcheck`
- After changing service files:
  ```bash
  sudo cp deploy/wslcb-web.service deploy/wslcb-task@.service deploy/wslcb-scraper.timer \
       deploy/wslcb-healthcheck.service deploy/wslcb-healthcheck.timer /etc/systemd/system/
  sudo cp deploy/sudoers.d-wslcb-healthcheck /etc/sudoers.d/wslcb-healthcheck
  sudo chmod 440 /etc/sudoers.d/wslcb-healthcheck
  sudo systemctl daemon-reload
  sudo systemctl enable --now wslcb-healthcheck.timer
  sudo systemctl restart wslcb-web.service
  ```
- Under systemd (non-TTY), all log output is JSON lines — structured fields (`timestamp`, `level`, `name`, `message`) are captured by the journal. Uvicorn access/error logs are routed through the same formatter.
- All persistent data lives in `./data/`
- The virtualenv is at `.venv/` (created by `uv sync`). If the project directory moves, recreate with `uv sync --dev`.

## Git Workflow

### Credentials (two transports — use the right one for each operation)

| Operation | Transport | How it works |
|---|---|---|
| `git push` / `git pull` | SSH deploy key | `~/.ssh/config` routes `github.com` → `~/.ssh/wslcb-deploy-key`; no extra flags needed |
| `gh` CLI (issues, PRs, comments) | PAT via `gh` CLI | Pre-authenticated as `gregoryfoster`; credentials at `~/.config/gh/hosts.yml` |

- **Never use `gh` for git push/pull** and **never use the SSH key for API calls** — each transport only works for its intended layer.
- Do not copy, log, or expose the PAT value; reference only the path `~/.config/gh/hosts.yml`.
- Verify access: `gh auth status` (PAT) · `ssh -T git@github.com -i ~/.ssh/wslcb-deploy-key` (SSH key)

### Repository

- Remote: `git@github.com:CannObserv/wslcb-licensing-tracker.git`
- Single `main` branch for now
- Write clear commit messages; group related changes
- **Commit message convention** — Conventional Commits style:
  ```
  #<number> [type]: <description>       # with GH issue
  [type]: <description>                 # without GH issue
  ```
  Multiple issues: `#19, #20 [type]: <description>`  
  Common types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`  
  GitHub auto-links `#<number>` references to the issue. Put `(closes #<number>)` in the commit **body** (not the subject line) on the final commit of a series to auto-close on push:
  ```
  #39 feat: migrate PLAYBOOKS.md to Agent Skills

  (closes #39)
  ```

## Address Validation

- External API at `https://address-validator.exe.xyz:8000` (FastAPI, OpenAPI docs at `/docs`)
- Authenticated via `X-API-Key` header; key stored in `./env` file (`ADDRESS_VALIDATOR_API_KEY=...`)
- `./env` file is `640 root:exedev`, gitignored
- Operates on the `locations` table — each unique raw address is validated once and shared across all records that reference it
- At scrape time, `validate_record()` checks if the location is already validated; skips the API call if so
- Systemd services load the env file via `EnvironmentFile=` directive
- Backfill: `wslcb backfill-addresses` (processes all locations where `address_validated_at IS NULL`)
- Refresh: `wslcb refresh-addresses` (re-validates all locations; safe to interrupt)

## Common Tasks

### Run tests
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run pytest tests/ -v
```
All tests must pass before committing. Tests use in-memory SQLite and static fixtures — no network, no disk DB, runs in <1 second.

### Manage admin users
```bash
cd /home/exedev/wslcb-licensing-tracker
wslcb admin add-user you@example.com    # add an admin
wslcb admin list-users                  # show all admins
wslcb admin remove-user you@example.com # remove (blocks last user)
```
The first admin must be bootstrapped via CLI. Subsequent admins can be added via the web UI (`/admin/users`) once auth is working.

### Run a manual scrape
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb scrape
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
sudo systemctl start 'wslcb-task@refresh-addresses.service'
journalctl -u 'wslcb-task@refresh-addresses.service' -f   # tail logs
```
Re-validates every location against the address-validator API. Safe to interrupt — progress is committed in batches.

Or manually:
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb refresh-addresses
```

### Backfill records from archived snapshots
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb backfill-snapshots
```
Two-phase process:
1. **Ingest** — insert new records from all archived HTML snapshots (duplicates skipped)
2. **Repair** — fix broken ASSUMPTION records (empty business names) and CHANGE OF LOCATION records (missing locations)

Safe to re-run. Address validation is deferred; run `cli.py backfill-addresses` afterward.

### Run integrity checks
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb check           # report issues
uv run wslcb check --fix     # auto-fix safe issues (orphan cleanup)
```
Exits with code 1 when issues are found. Checks: orphaned locations, broken FKs, un-enriched records, endorsement anomalies, entity duplicates.

### Rebuild database from archived sources
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb rebuild --output data/wslcb-rebuilt.db
```
Creates a fresh database by replaying all archived diff files and HTML snapshots through the ingestion pipeline. Four phases: (1) ingest diff archives, (2) ingest HTML snapshots, (3) endorsement discovery, (4) build outcome links.

To overwrite an existing output file:
```bash
uv run wslcb rebuild --output data/wslcb-rebuilt.db --force
```

To rebuild and verify against the production database:
```bash
uv run wslcb rebuild --output data/wslcb-rebuilt.db --verify
```
Verification compares record natural keys `(section_type, record_date, license_number, application_type)` and reports missing/extra records with per-section breakdown. Exits with code 1 if discrepancies are found.

**Note:** This is a long-running operation. Diff extraction alone can take 20+ minutes on the full archive (4400+ diff files). Run via `tmux` or systemd.

### Clean up redundant scrape data
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb cleanup-redundant
```
Removes `record_sources` (confirmed) rows and `sources` rows from scrapes that inserted zero new records, deletes their duplicate snapshot files, and re-stamps their `scrape_log` entries as `status='unchanged'`. Use `--keep-files` to skip file deletion. Safe to re-run.

### Reprocess entity links
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb reprocess-entities              # all records
uv run wslcb reprocess-entities --record-id 12345  # single record
uv run wslcb reprocess-entities --dry-run    # preview without writing
```

Regenerates `record_entities` rows from `license_records.applicants` /
`previous_applicants` using the current entity-normalization logic.  Existing
links for each targeted record are deleted and rebuilt (idempotent).  The
`record_enrichments.version` stamp is bumped to `'2'` for every reprocessed
record, enabling queries like:

```sql
-- Records that haven't been reprocessed with the current entity logic:
SELECT record_id FROM record_enrichments
WHERE step = 'entities' AND CAST(version AS INTEGER) < 2;
```

Use this after:
- Adding a new suffix to `_LEGIT_TRAILING_DOT` in `entities.py`
- A `merge_duplicate_entities()` run that merges entity rows
- Any other change to name-normalization or cleaning rules

### Rebuild application→outcome links
```bash
cd /home/exedev/wslcb-licensing-tracker
uv run wslcb rebuild-links
```
Clears and rebuilds all `record_links` from scratch. Safe to run at any time (~85 seconds on current dataset). Links are also built incrementally during scraping and on first web app startup (if table is empty).

### Add a new column to `locations`
1. Add the column to `_m001_baseline()` in `schema.py` (for fresh installs)
2. Add a new migration function (e.g., `_m002_add_column()`) that runs `ALTER TABLE locations ADD COLUMN ...`
3. Append the migration tuple to `MIGRATIONS` in `schema.py`
4. If the column should be searchable via FTS, add it to the `license_records_fts_content` view in `_ensure_fts()` (in `schema.py`)
5. If needed in display, add it to `RECORD_COLUMNS` in `queries.py` and update templates

### Add a new column to `license_records`
1. Add the column to `_m001_baseline()` in `schema.py` (for fresh installs)
2. Add a new migration function that runs `ALTER TABLE license_records ADD COLUMN ...`
3. Append the migration tuple to `MIGRATIONS` in `schema.py`
4. Update `insert_record()` in `pipeline.py` and `RECORD_COLUMNS` in `queries.py`, `search_records()`, and templates as needed

## Known Issues & Future Work

- Non-standard state values exist in `locations.state` from regex parsing errors (e.g., `SR WA`, `TERMINAL WA`); all validated `std_state` values resolve to valid US state codes. The state filter only shows valid US state codes (validated against `US_STATES` in `db.py`)
- FTS indexes raw `license_type` values — text search for endorsement names won't find approved/discontinued records that store numeric codes (the endorsement filter works correctly)
- No authentication — the app is fully public
- No rate limiting on search/export
- The city extraction regex misses ~6% of records (suite info between street and city); the address validator handles these correctly
- Two source records have malformed cities (#436924: zip in city field, #078771: street name in city field); corrected manually in the locations table but corrections are overwritten by `--refresh-addresses` — needs a durable data-override mechanism
- `ON DELETE CASCADE` on endorsement FK columns only applies to fresh databases (existing DBs retain original schema; manual cleanup in `_merge_placeholders` handles this)
- The `applicants` and `previous_applicants` string columns on `license_records` are retained for FTS indexing and CSV export; values are cleaned at ingest time (alongside `business_name` and `previous_business_name`) to stay consistent with entity names. Removal is deferred to a future phase
- Approved-section CHANGE OF LOCATION records lack `previous_location_id` because the source page only provides `Business Location:` (the new address) for approved records
- `search_records()` runs separate COUNT and SELECT queries with the same WHERE clause; could use `COUNT(*) OVER()` window function (fine at current scale)
- `search_records()` and `export_records()` share filter logic via `_build_where_clause()` — when adding a new filter parameter, update the shared helper
- Consider adding: email/webhook alerts for new records matching saved searches
