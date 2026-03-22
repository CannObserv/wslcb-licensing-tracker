# AGENTS.md — WSLCB Licensing Tracker

Guidance for AI agents working on this project.

**Output style:** Be terse. Sacrifice grammar for readability. No filler, no restating what the user said. Short sentences. Lead with the answer.

**Live instance:** https://wslcb-licensing-tracker.exe.xyz:8000/
**Repo:** https://github.com/CannObserv/wslcb-licensing-tracker

## Architecture

```
pg_scraper.py ─┐
pg_backfill_snapshots.py ─┼─→ pg_pipeline.py ─→ PostgreSQL (tsvector + pg_trgm) ←─ app.py (FastAPI) ─→ templates/ (Jinja2 + HTMX)
pg_backfill_diffs.py ──────┘                                                       ←─ display.py (presentation)
                             ↘ data/wslcb/licensinginfo/[yyyy]/[date]/*.html

license_records → locations (FK: location_id, previous_location_id)
                → record_endorsements → license_endorsements
```

- No build step. Tailwind via CDN, HTMX. No node_modules.
- All Python source in `src/wslcb_licensing_tracker/`. CLI: `wslcb <subcommand>` or `python -m wslcb_licensing_tracker.cli <subcommand>`.
- PostgreSQL (asyncpg + SQLAlchemy 2.0 Core async). Schema managed by Alembic (`alembic upgrade head`).

## Key Files

| File | Purpose / Non-obvious notes |
|---|---|
| `db.py` | Connections, constants, core helpers. `get_connection()`, `get_db()`, `DATA_DIR`, `DB_PATH`. `get_or_create_location()`, `get_or_create_source()`, `link_record_source()`. `get_primary_source()` / `get_record_sources()` — provenance queries. `SOURCE_ROLE_PRIORITY` shared with `display.py` (avoids circular import). `US_STATES` dict for state filter dropdown. `_normalize_raw_address()`. Text normalization utilities: `clean_entity_name()`, `strip_duplicate_marker()`, `clean_applicants_string()` — live here so `schema.py` migrations can import them without a layering violation. |
| `schema.py` | DDL, migrations, FTS. `init_db()`, `migrate()`, `MIGRATIONS` list. `PRAGMA user_version` migration framework. `_table_exists()` / `_column_exists()` exported for testability. |
| `pipeline.py` | **All ingestion flows through here.** `insert_record()` — canonical insertion (dedup, location resolution, name cleaning, entity linking). `ingest_record()`, `ingest_batch()`, `IngestOptions`, `IngestResult`, `BatchResult`. Step constants: `STEP_ENDORSEMENTS`, `STEP_ENTITIES`, `STEP_ADDRESS`, `STEP_OUTCOME_LINK`. |
| `database.py` | *(PostgreSQL migration — Phase 1)* Async engine factory. `create_engine_from_env()` — reads `DATABASE_URL` env var. `get_db()` FastAPI dependency yielding `AsyncConnection`. |
| `models.py` | *(PostgreSQL migration — Phase 1)* SQLAlchemy Core `Table` objects for all 20 tables. Single shared `metadata`. Import table objects from here for all PG queries. |
| `pg_schema.py` | *(PostgreSQL migration — Phase 2)* Alembic-based init. `init_db(engine)` — runs all pending migrations (idempotent). `_table_exists(conn, name)` / `_column_exists(conn, table, column)` — introspection helpers. |
| `pg_db.py` | *(PostgreSQL migration — Phase 2)* Async equivalents of `db.py` helpers. `get_or_create_location()`, `get_or_create_source()`, `link_record_source()`, `get_primary_source()`, `get_record_sources()`. Re-imports pure-string utilities from `db.py`. |
| `pg_pipeline.py` | *(PostgreSQL migration — Phase 2)* Async equivalent of `pipeline.py`. `insert_record()`, `ingest_record()`, `ingest_batch()`, `IngestOptions`, `IngestResult`, `BatchResult`. Entity linking / address validation / outcome linking are STUBBED — ported in Phase 3. |
| `pg_admin_audit.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `admin_audit.py`. `log_action()` uses `pg_insert(...).returning(id)`. `get_audit_log()` uses named `text()` params; `admin_email` filter uses `lower()` instead of `COLLATE NOCASE`. |
| `pg_substances.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `substances.py`. `get_regulated_substances()` runs two queries (substances + per-substance endorsements). `remove_substance()` manually deletes junction rows before the parent (no CASCADE assumption). |
| `pg_endorsements.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `endorsements.py`. `ensure_endorsement()` uses ON CONFLICT DO NOTHING + RETURNING with fallback SELECT. `_sync_resolved_endorsements()` uses a single `text()` UPDATE with `STRING_AGG(...ORDER BY)`. Alias self-join uses aliased table objects. Numeric code detection uses `.isdigit()` on the stripped Python value (no SQL GLOB needed). |
| `pg_endorsements_seed.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `endorsements_seed.py`. Imports static `SEED_CODE_MAP` from `endorsements_seed` (no DB at import time). Placeholder detection uses `col.op('~')(r'^\d+$')` instead of `GLOB '[0-9]*'`. |
| `pg_endorsements_admin.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `endorsements_admin.py`. `endorsement_similarity()` is pure Python — copied verbatim, not async. `dismiss_suggestion()` swaps `id_a/id_b` if `id_a > id_b` to enforce the `id_a < id_b` constraint. `get_endorsement_list()` uses `STRING_AGG` for code aggregation. |
| `pg_entities.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `entities.py`. `get_or_create_entity()` uses ON CONFLICT DO NOTHING + RETURNING + fallback SELECT. `_ENTITY_REPROCESS_VERSION = 2` constant preserved. Also includes `get_entity_by_id()` and `backfill_entities()` (added Phase 4). |
| `pg_address_validator.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `address_validator.py`. Pure HTTP functions (`_load_api_key`, `standardize`, `validate`) copied verbatim. HTTP calls wrapped in `asyncio.to_thread()`. DB writes use `update(locations).where(...).values(...)`. |
| `pg_link_records.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `link_records.py`. `get_outcome_status()` and `outcome_filter_sql()` are pure Python — not async. Bidirectional linking queries use `text()` with PG date arithmetic (`record_date::date - interval 'N days'`). `build_all_links()` truncates `record_links` before rebuilding. `get_record_links_bulk()` is a NEW function (not in SQLite version) — batch SELECT JOIN returning `dict[int, dict]`. |
| `pg_queries.py` | *(PostgreSQL migration — Phase 3)* Async equivalent of `queries.py`. Text search uses `search_vector @@ plainto_tsquery('english', ...)` (tsvector GIN) OR `business_name % query` / `applicants % query` (pg_trgm similarity). `search_records` orders by `ts_rank` when a text query is present; export remains date-ordered. `export_records_cursor()` is `AsyncGenerator[dict, None]` (true server-side streaming via `conn.stream()`). `outcome_filter_sql` fragments emit `CURRENT_DATE - interval '...'` (PG-native). In-memory TTL caches (`_filter_cache`, `_city_cache`, `_stats_cache`) preserved unchanged. **API difference from SQLite:** `get_related_records(conn, record: dict)` takes a full record dict, not `(license_number, exclude_id)`. Also includes `get_record_link()` (added Phase 4). |
| `queries.py` | Search and read queries. `search_records()`, `export_records()`, `export_records_cursor()` (streaming generator for `/export`), `get_filter_options()`, `get_cities_for_state()`, `get_stats()`, `enrich_record()`, `hydrate_records()`, `get_record_by_id()`, `get_related_records()`, `get_entity_records()`, `get_entities()`. `invalidate_filter_cache()` — call after any admin mutation. Re-exports `insert_record` (from `pipeline`), `get_primary_source` / `get_record_sources` / `US_STATES` (from `db`) for backward compat. |
| `endorsements.py` | Core endorsement pipeline. `process_record()` (idempotent — deletes existing rows before inserting, then calls `_sync_resolved_endorsements()` to keep `license_records.resolved_endorsements` in sync for FTS), `reprocess_endorsements()`, `get_record_endorsements()`, `get_endorsement_options()`, alias management (`set_canonical_endorsement`, `rename_endorsement`, `resolve_endorsement`, `remove_alias`), `get_endorsement_groups()`. |
| `endorsements_seed.py` | Seeding, repair, and backfill. `seed_endorsements()` loads `SEED_CODE_MAP` from `seed_code_map.json`. `merge_mixed_case_endorsements()`, `repair_code_name_endorsements()`, `backfill()`, `discover_code_mappings()`. All follow caller-commits convention. |
| `seed_code_map.json` | 103-entry JSON dict: WSLCB numeric code → endorsement name(s). **Edit this file, not the Python module**, when adding/correcting seed mappings. |
| `endorsements_admin.py` | Admin UI helpers: `endorsement_similarity()`, `get_endorsement_list()`, `suggest_duplicate_endorsements()`, `dismiss_suggestion()`, `get_code_mappings()`, `add_code_mapping()`, `remove_code_mapping()`, `create_code()`. No `admin_audit` dep — caller logs. |
| `substances.py` | Regulated substance CRUD. No `admin_audit` dep — caller logs. |
| `entities.py` | Entity normalization. `get_or_create_entity()`, `parse_and_link_entities()`, `reprocess_entities()`, `merge_duplicate_entities()`. `ADDITIONAL_NAMES_MARKERS` exported frozenset. Text utilities (`clean_entity_name`, `strip_duplicate_marker`, `clean_applicants_string`) re-imported from `db.py`. |
| `parser.py` | Pure HTML/diff parsing — no DB, no side effects. `extract_tbody_from_snapshot()`, `extract_tbody_from_diff()`. Only depends on stdlib + bs4/lxml + `db.DATA_DIR`. |
| `display.py` | Presentation formatting. `format_outcome()`, `summarize_provenance()`, `OUTCOME_STYLES`. `_ROLE_PRIORITY` alias for `db.SOURCE_ROLE_PRIORITY`. |
| `link_records.py` | Application→outcome linking. Bidirectional nearest-neighbor, ±7-day tolerance. `build_all_links()`, `link_new_record()`, `get_outcome_status()`, `get_reverse_link_info()`, `outcome_filter_sql()`. |
| `data_migration.py` | *(PostgreSQL migration — Phase 4)* Run-once data migration framework. `run_pending_migrations(engine)` — checks `data_migrations` table, runs any pending migrations in order, marks each complete. Called from lifespan. Replaces unconditional startup repair calls (resolves #85). Registered migrations: seed endorsements, repair code-name, merge mixed-case, backfill endorsements, backfill entities, build record links. |
| `pg_integrity.py` | *(PostgreSQL migration — Phase 6)* Async integrity checks. `check_orphaned_locations()`, `check_unenriched_records()`, `check_endorsement_anomalies()`, `check_broken_fks()`, `check_entity_duplicates()`, `fix_orphaned_locations()` (caller-commits). `run_all_checks(conn, *, fix=False)` — runs all checks, auto-commits after fix. `print_report(report)` — pure Python, returns issue count. |
| `pg_scraper.py` | *(PostgreSQL migration — Phase 6)* Async port of `scraper.py`. `scrape(engine)` — fetch, hash-check, archive, ingest via `pg_pipeline.ingest_batch()`. `get_last_content_hash(conn)` — SQLAlchemy query against `scrape_log`. `cleanup_redundant_scrapes(engine)` — removes unchanged scrape rows. Pure helpers `compute_content_hash` and `save_html_snapshot` are re-imported from `scraper.py`. |
| `pg_backfill_snapshots.py` | *(PostgreSQL migration — Phase 6)* Async port of `backfill_snapshots.py`. Two-phase: ingest from snapshot files, then repair ASSUMPTION and CHANGE OF LOCATION records. `backfill_from_snapshots(engine)`. |
| `pg_backfill_diffs.py` | *(PostgreSQL migration — Phase 6)* Async port of `backfill_diffs.py`. `backfill_diffs(engine, section, single_file, limit, dry_run)`. Pure diff parsing re-imported from `parser.py`. |
| `app.py` | FastAPI app, port 8000. *(Phase 4: async PG)* Lifespan creates `AsyncEngine` on `app.state.engine`, calls `run_pending_migrations()`, disposes on shutdown. All routes use `async with get_db(request.app.state.engine) as conn:`. Admin routes via `app.include_router()`; `admin_routes.init_router(_tpl)` must be called before first request. Public routes only — admin routes in `admin_routes.py`. |
| `api_routes.py` | `APIRouter(prefix="/api/v1")`. *(Phase 4: async PG)* All routes async; `_get_db` dependency yields `AsyncConnection`. CSV export uses async generator with `export_records_cursor()`. JSON envelope `{"ok": bool, "message": str, "data": ...}`. Endpoints: `GET /api/v1/cities`, `/stats`, `/export` (StreamingResponse), `/health`. |
| `admin_routes.py` | `APIRouter` for `/admin/*`. *(Phase 4: async PG)* All routes async; `_get_db` dependency yields `AsyncConnection`; all imports from `pg_*` equivalents. `init_router(tpl_fn)` receives shared `_tpl()` at startup. |
| `admin_auth.py` | `require_admin()` FastAPI dependency. Reads `X-ExeDev-Email` / `X-ExeDev-UserID` proxy headers; falls back to `ADMIN_DEV_EMAIL` / `ADMIN_DEV_USERID` env vars for local dev. |
| `admin_audit.py` | `log_action(conn, email, action, target_type, target_id, details)` — caller commits. `get_audit_log(conn, page, per_page, filters)` → `(rows, total_count)`. |
| `address_validator.py` | Two-phase pipeline. Phase 1 (always): `standardize_location()` → `std_*` columns + `address_standardized_at`. Phase 2 (optional, `ENABLE_ADDRESS_VALIDATION=1`): `validate_location()` → DPV fields + `address_validated_at`. API key from `/etc/wslcb-licensing-tracker/env`, falls back to `<project-root>/env`. |
| `scraper.py` | Fetches WSLCB page, archives HTML, calls `pipeline.ingest_batch()`. Skips parse/ingest on content-hash match. |
| `backfill_snapshots.py` | Two-phase: (1) insert from archived HTML, (2) repair broken ASSUMPTION/CHANGE OF LOCATION records. Safe to re-run. |
| `backfill_diffs.py` | Ingest from CO diff archives via `pipeline.ingest_record()`. Safe to re-run. |
| `backfill_provenance.py` | One-time backfill: creates `sources` rows from `scrape_log` entries and CO archive snapshots, then rebuilds `record_sources` junction links. Safe to re-run. |
| `integrity.py` | `run_all_checks()`, `fix_orphaned_locations()`. CLI: `wslcb check [--fix]`. Exits 1 when issues found. |
| `rebuild.py` | `rebuild_from_sources()`, `compare_databases()`. Four phases: diff archives → HTML snapshots → endorsement discovery → outcome links. |
| `log_config.py` | `setup_logging()` — auto-detects TTY vs JSON format. Call once per entry point. |
| `cli.py` | *(Phase 6: updated)* All commands use PG modules with `asyncio.run()` wrappers. Engine from `create_engine_from_env()` (reads `DATABASE_URL`). Admin user commands use inline SQLAlchemy queries. |
| `templates/` | `base.html` (main layout — nav, footer, CSS/JS includes). `partials/results.html` (HTMX target). `partials/record_table.html` (shared record table). |
| `tailwind.config.js` | Tailwind CSS config — content paths, co-green/co-purple palette. Consumed by `scripts/build-css.sh`. |
| `static/css/input.css` | Tailwind source: `@tailwind` directives + HTMX loading states + badge classes + `.scroll-shadow-right`. |
| `static/js/search.js` | Client-side logic for search page: section-type toggle, state→city fetch, endorsement multi-select dropdown. |
| `static/js/admin-endorsements.js` | Admin endorsements page JS: filter, alias panel, rebuild-hiddens, suggestion helpers, code filter. |
| `static/js/detail.js` | Record detail page JS: source viewer toggle (`toggleSourceViewer`, `closeSourceViewer`). |

## Frozen vs. Derived Data Contract

**Frozen (never alter after ingest):**
- `license_records.license_type` — raw text/code from source
- `license_records.raw_*` shadow columns — as-parsed values
- `sources` / `record_sources` — provenance chain

**Derived (regenerable):**

| Table / Column | Regenerated by | Command |
|---|---|---|
| `record_endorsements` | `pg_endorsements.reprocess_endorsements()` | `wslcb reprocess-endorsements` |
| `license_records.resolved_endorsements` | `pg_endorsements.reprocess_endorsements()` | `wslcb reprocess-endorsements` |
| `record_entities` | `pg_entities.reprocess_entities()` | `wslcb reprocess-entities` |
| `record_links` | `pg_link_records.build_all_links()` | `wslcb rebuild-links` |

`reprocess_endorsements()` is idempotent — deletes existing rows before inserting fresh ones.

`build_all_links()` also backfills `license_records.previous_location_id` for approved CHANGE OF LOCATION records when NULL (sourced from the matched new-application record). Run `wslcb rebuild-links` to repair existing rows.

## Conventions

### Python
- Python 3.12+, `uv` at `./.venv/`. Install: `uv sync --dev`.
- Use `datetime.now(timezone.utc)` not `datetime.utcnow()` (deprecated).
- Module docstrings on every `.py` file.

### Logging
- **Never `print()`** for operational output (CLI summary lines excepted). Use `logging`.
- Each module: `logger = logging.getLogger(__name__)`.
- Entry points call `setup_logging()` from `log_config.py` before doing any work.
- Use `%s`/`%d` style in log calls (not f-strings).

### Testing
Red/Green TDD: write a failing test first, then implement.

```bash
uv run pytest tests/ -v   # must pass before committing
```

- No network calls, no disk DB. Use in-memory `db` fixture from `conftest.py`.
- HTML parser tests use static fixtures in `tests/fixtures/`.
- Match `test_<module>.py` to the module being changed.
- Sample record fixtures in `conftest.py`: `standard_new_application`, `assumption_record`, `change_of_location_record`, `approved_numeric_code`, `discontinued_code_name`.

### Templates
- Tailwind pre-built via `scripts/build-css.sh` → `static/css/tailwind.css`. Config in `tailwind.config.js`. No bundler.
- HTMX for partial updates; `/search` detects `HX-Request` header.
- Custom Jinja2 filters in `app.py`: `section_label`, `phone_format`, `build_qs`.
- See [`docs/STYLE.md`](STYLE.md) for brand colors and CSS conventions.

### Data Integrity
- `insert_record()` returns `(id, True)` for new, `(id, False)` for duplicate, `None` on unexpected `IntegrityError`.
- Never delete historical data — accumulating beyond the 30-day source window is the whole point.

## Git Workflow

### Credentials

| Operation | Transport |
|---|---|
| `git push` / `git pull` | SSH deploy key — `~/.ssh/config` routes `github.com` → `~/.ssh/wslcb-deploy-key` |
| `gh` CLI (issues, PRs) | PAT — pre-authenticated as `gregoryfoster` |

Never use `gh` for git push/pull. Never use SSH key for API calls.

### Commit convention

```
#<issue> [type]: <description>      # with GH issue
[type]: <description>               # without GH issue
```

Common types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`. Put `(closes #N)` in commit **body** (not subject) to auto-close on push.

## Dev Setup (one-time after clone)

```bash
# Install pre-commit hooks (runs ruff + auto-rebuilds Tailwind CSS before each commit)
uv run pre-commit install

# Manual CSS rebuild (if you change templates or tailwind.config.js without committing)
scripts/build-css.sh
```

The Tailwind CLI binary is auto-downloaded on first `build-css.sh` run (~26MB, platform-specific, gitignored at `scripts/bin/`).

## Common Tasks

```bash
# Run tests
uv run pytest tests/ -v

# Manual scrape
uv run wslcb scrape

# Restart web app
sudo systemctl restart wslcb-web.service

# Integrity check
uv run wslcb check
uv run wslcb check --fix

# Rebuild links
uv run wslcb rebuild-links

# Reprocess endorsements
uv run wslcb reprocess-endorsements [--code 394] [--record-id 12345] [--dry-run]

# Reprocess entities
uv run wslcb reprocess-entities [--record-id 12345] [--dry-run]

# Manage admin users
wslcb admin add-user you@example.com
wslcb admin list-users
wslcb admin remove-user you@example.com

# Backfill / repair
uv run wslcb backfill-snapshots
uv run wslcb backfill-diffs [--section notifications] [--limit 100] [--dry-run]
uv run wslcb backfill-addresses
uv run wslcb cleanup-redundant

# Rebuild DB from PostgreSQL archives (use instead of 'rebuild' which targets SQLite)
DATABASE_URL=postgresql+asyncpg://... python scripts/sqlite_to_pg.py  # one-time SQLite→PG
```

See [`docs/DEPLOYMENT.md`](DEPLOYMENT.md) for systemd services, address validation, and ops commands.
See [`docs/SOURCE_PAGE.md`](SOURCE_PAGE.md) for WSLCB source page structure and field label quirks (needed when touching `parser.py`).
See [`docs/SCHEMA.md`](SCHEMA.md) for full table/column reference and migration history.
