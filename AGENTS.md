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
| `text_utils.py` | Pure-string text normalization utilities (no DB dependency). `clean_entity_name()`, `strip_duplicate_marker()`, `clean_applicants_string()`, `_normalize_raw_address()`. Import from here; do not duplicate these functions. |
| `database.py` | Async engine factory. `create_engine_from_env()` — reads `DATABASE_URL` env var. `get_db()` async context manager yielding `AsyncConnection`. |
| `models.py` | SQLAlchemy Core `Table` objects for all 20 tables. Single shared `metadata`. Import table objects from here for all PG queries. |
| `pg_schema.py` | Alembic-based init. `init_db(engine)` — runs all pending migrations (idempotent). `_table_exists(conn, name)` / `_column_exists(conn, table, column)` — introspection helpers. |
| `pg_db.py` | Async location/source/provenance helpers + shared constants. `get_or_create_location()`, `get_or_create_source()`, `link_record_source()`, `get_primary_source()`, `get_record_sources()`. Constants: `DATA_DIR`, `WSLCB_SOURCE_URL`, `SOURCE_TYPE_*`, `SOURCE_ROLE_PRIORITY`, `US_STATES`. |
| `pg_pipeline.py` | **All ingestion flows through here.** `insert_record()` — canonical insertion (dedup, location resolution, name cleaning, entity linking). `ingest_record()`, `ingest_batch()`, `IngestOptions`, `IngestResult`, `BatchResult`. |
| `pg_admin_audit.py` | Async audit log helpers. `log_action()` uses `pg_insert(...).returning(id)`. `get_audit_log()` uses named `text()` params; `admin_email` filter uses `lower()` instead of `COLLATE NOCASE`. |
| `pg_substances.py` | Async regulated substance CRUD. `get_regulated_substances()` runs two queries (substances + per-substance endorsements). `remove_substance()` manually deletes junction rows before the parent (no CASCADE assumption). |
| `pg_endorsements.py` | Async endorsement pipeline. `ensure_endorsement()` uses ON CONFLICT DO NOTHING + RETURNING with fallback SELECT. `_sync_resolved_endorsements()` uses a single `text()` UPDATE with `STRING_AGG(...ORDER BY)`. Alias self-join uses aliased table objects. Numeric code detection uses `.isdigit()` on the stripped Python value (no SQL GLOB needed). |
| `pg_endorsements_seed.py` | Async endorsement seeding, repair, and backfill. `SEED_CODE_MAP` loaded from `seed_code_map.json` at module init. Placeholder detection uses `col.op('~')(r'^\d+$')` instead of `GLOB '[0-9]*'`. |
| `pg_endorsements_admin.py` | Async admin endorsement helpers. `endorsement_similarity()` is pure Python — not async. `dismiss_suggestion()` swaps `id_a/id_b` if `id_a > id_b` to enforce the `id_a < id_b` constraint. `get_endorsement_list()` uses `STRING_AGG` for code aggregation. |
| `pg_entities.py` | Async entity normalization. `get_or_create_entity()` uses ON CONFLICT DO NOTHING + RETURNING + fallback SELECT. `_ENTITY_REPROCESS_VERSION = 2` constant preserved. `ADDITIONAL_NAMES_MARKERS` frozenset defined here. Also includes `get_entity_by_id()` and `backfill_entities()`. |
| `pg_address_validator.py` | Async address validation DB layer. Pure HTTP functions (`_load_api_key`, `standardize`, `validate`) defined here. HTTP calls wrapped in `asyncio.to_thread()`. DB writes use `update(locations).where(...).values(...)`. `_env_candidates` list exposed for monkeypatching in tests. |
| `pg_link_records.py` | Async application→outcome linking. `get_outcome_status()` and `outcome_filter_sql()` are pure Python — not async. Bidirectional linking queries use `text()` with PG date arithmetic (`record_date::date - interval 'N days'`). `build_all_links()` truncates `record_links` before rebuilding. `get_record_links_bulk()` — batch SELECT JOIN returning `dict[int, dict]`. |
| `pg_queries.py` | Async search and read queries. Text search uses `search_vector @@ plainto_tsquery('english', ...)` (tsvector GIN) OR `business_name % query` / `applicants % query` (pg_trgm similarity). `search_records` orders by `ts_rank` when a text query is present; export remains date-ordered. `export_records_cursor()` is `AsyncGenerator[dict, None]` (true server-side streaming via `conn.stream()`). In-memory TTL caches (`_filter_cache`, `_city_cache`, `_stats_cache`) preserved. `get_related_records(conn, record: dict)` takes a full record dict. Also includes `get_record_link()`. |
| `seed_code_map.json` | 103-entry JSON dict: WSLCB numeric code → endorsement name(s). **Edit this file, not the Python module**, when adding/correcting seed mappings. |
| `parser.py` | Pure HTML/diff parsing — no DB, no side effects. `extract_tbody_from_snapshot()`, `extract_tbody_from_diff()`. Only depends on stdlib + bs4/lxml + `pg_db.DATA_DIR`. |
| `display.py` | Presentation formatting. `format_outcome()`, `summarize_provenance()`, `OUTCOME_STYLES`. `_ROLE_PRIORITY` alias for `pg_db.SOURCE_ROLE_PRIORITY`. |
| `data_migration.py` | Run-once data migration framework. `run_pending_migrations(engine)` — checks `data_migrations` table, runs any pending migrations in order, marks each complete. Called from lifespan. Registered migrations: seed endorsements, repair code-name, merge mixed-case, backfill endorsements, backfill entities, build record links. |
| `pg_integrity.py` | Async integrity checks. `check_orphaned_locations()`, `check_unenriched_records()`, `check_endorsement_anomalies()`, `check_broken_fks()`, `check_entity_duplicates()`, `fix_orphaned_locations()` (caller-commits). `run_all_checks(conn, *, fix=False)` — runs all checks, auto-commits after fix. `print_report(report)` — pure Python, returns issue count. |
| `pg_scraper.py` | Async scraper. `scrape(engine)` — fetch, hash-check, archive, ingest via `pg_pipeline.ingest_batch()`. `get_last_content_hash(conn)` — SQLAlchemy query against `scrape_log`. `cleanup_redundant_scrapes(engine)` — removes unchanged scrape rows. Pure helpers `compute_content_hash` and `save_html_snapshot` defined here. |
| `pg_backfill_snapshots.py` | Async backfill from HTML snapshots. Two-phase: ingest from snapshot files, then repair ASSUMPTION and CHANGE OF LOCATION records. `backfill_from_snapshots(engine)`. |
| `pg_backfill_diffs.py` | Async backfill from diff archives. `backfill_diffs(engine, section, single_file, limit, dry_run)`. Pure diff parsing re-imported from `parser.py`. |
| `app.py` | FastAPI app, port 8000. Lifespan creates `AsyncEngine` on `app.state.engine`, calls `run_pending_migrations()`, disposes on shutdown. All routes use `async with get_db(request.app.state.engine) as conn:`. Admin routes via `app.include_router()`; `admin_routes.init_router(_tpl)` must be called before first request. Public routes only — admin routes in `admin_routes.py`. |
| `api_routes.py` | `APIRouter(prefix="/api/v1")`. All routes async; `_get_db` dependency yields `AsyncConnection`. CSV export uses async generator with `export_records_cursor()`. JSON envelope `{"ok": bool, "message": str, "data": ...}`. Endpoints: `GET /api/v1/cities`, `/stats`, `/export` (StreamingResponse), `/health`. |
| `admin_routes.py` | `APIRouter` for `/admin/*`. All routes async; `_get_db` dependency yields `AsyncConnection`; all imports from `pg_*` equivalents. `init_router(tpl_fn)` receives shared `_tpl()` at startup. |
| `admin_auth.py` | `require_admin()` FastAPI dependency. Reads `X-ExeDev-Email` / `X-ExeDev-UserID` proxy headers; falls back to `ADMIN_DEV_EMAIL` / `ADMIN_DEV_USERID` env vars for local dev. `_lookup_admin()` is async — uses `request.app.state.engine` to query `admin_users`. |
| `log_config.py` | `setup_logging()` — auto-detects TTY vs JSON format. Call once per entry point. |
| `cli.py` | All commands use PG modules with `asyncio.run()` wrappers. Engine from `create_engine_from_env()` (reads `DATABASE_URL`). Admin user commands use inline SQLAlchemy queries. |
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

- No network calls, no disk DB. PostgreSQL tests use `pg_conn` / `pg_engine` fixtures (require `TEST_DATABASE_URL`); non-PG tests mock the async connection.
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

# One-time SQLite → PostgreSQL data migration
DATABASE_URL=postgresql+asyncpg://... python scripts/sqlite_to_pg.py
```

See [`docs/DEPLOYMENT.md`](DEPLOYMENT.md) for systemd services, address validation, and ops commands.
See [`docs/SOURCE_PAGE.md`](SOURCE_PAGE.md) for WSLCB source page structure and field label quirks (needed when touching `parser.py`).
See [`docs/SCHEMA.md`](SCHEMA.md) for full table/column reference and migration history.
