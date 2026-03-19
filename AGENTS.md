# AGENTS.md — WSLCB Licensing Tracker

Guidance for AI agents working on this project.

**Output style:** Be terse. Sacrifice grammar for readability. No filler, no restating what the user said. Short sentences. Lead with the answer.

**Live instance:** https://wslcb-licensing-tracker.exe.xyz:8000/
**Repo:** https://github.com/CannObserv/wslcb-licensing-tracker

## Architecture

```
scraper.py ─┐
backfill_snapshots.py ─┼─→ pipeline.py ─→ data/wslcb.db (SQLite + FTS5) ←─ app.py (FastAPI) ─→ templates/ (Jinja2 + HTMX)
backfill_diffs.py ──────┘                                                  ←─ display.py (presentation)
                          ↘ data/wslcb/licensinginfo/[yyyy]/[date]/*.html

license_records → locations (FK: location_id, previous_location_id)
                → record_endorsements → license_endorsements
```

- No build step. Tailwind via CDN, HTMX. No node_modules.
- All Python source in `src/wslcb_licensing_tracker/`. CLI: `wslcb <subcommand>` or `python -m wslcb_licensing_tracker.cli <subcommand>`.
- SQLite only. WAL mode for concurrent reads.

## Key Files

| File | Purpose / Non-obvious notes |
|---|---|
| `db.py` | Connections, constants, core helpers. `get_connection()`, `get_db()`, `DATA_DIR`, `DB_PATH`. `get_or_create_location()`, `get_or_create_source()`, `link_record_source()`. `get_primary_source()` / `get_record_sources()` — provenance queries. `SOURCE_ROLE_PRIORITY` shared with `display.py` (avoids circular import). `US_STATES` dict for state filter dropdown. `_normalize_raw_address()`. Text normalization utilities: `clean_entity_name()`, `strip_duplicate_marker()`, `clean_applicants_string()` — live here so `schema.py` migrations can import them without a layering violation. |
| `schema.py` | DDL, migrations, FTS. `init_db()`, `migrate()`, `MIGRATIONS` list. `PRAGMA user_version` migration framework. `_table_exists()` / `_column_exists()` exported for testability. |
| `pipeline.py` | **All ingestion flows through here.** `insert_record()` — canonical insertion (dedup, location resolution, name cleaning, entity linking). `ingest_record()`, `ingest_batch()`, `IngestOptions`, `IngestResult`, `BatchResult`. Step constants: `STEP_ENDORSEMENTS`, `STEP_ENTITIES`, `STEP_ADDRESS`, `STEP_OUTCOME_LINK`. |
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
| `app.py` | FastAPI app, port 8000. Admin routes via `app.include_router()`; `admin_routes.init_router(_tpl)` must be called before first request. Public routes only — admin routes in `admin_routes.py`. |
| `api_routes.py` | `APIRouter(prefix="/api/v1")`. JSON envelope `{"ok": bool, "message": str, "data": ...}`. Endpoints: `GET /api/v1/cities`, `/stats`, `/export` (StreamingResponse), `/health`. Tests patch `api_routes.get_db`. |
| `admin_routes.py` | `APIRouter` for `/admin/*`. `init_router(tpl_fn)` receives shared `_tpl()` at startup. **Tests must patch `admin_routes.get_db`**, not `app.get_db`. |
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
| `cli.py` | Unified CLI entry point. All operational subcommands. |
| `templates/` | `base.html` (layout + Tailwind config). `partials/results.html` (HTMX target). `partials/record_table.html` (shared record table). |

## Frozen vs. Derived Data Contract

**Frozen (never alter after ingest):**
- `license_records.license_type` — raw text/code from source
- `license_records.raw_*` shadow columns — as-parsed values
- `sources` / `record_sources` — provenance chain

**Derived (regenerable):**

| Table / Column | Regenerated by | Command |
|---|---|---|
| `record_endorsements` | `endorsements.process_record()` | `wslcb reprocess-endorsements` |
| `license_records.resolved_endorsements` | `endorsements.process_record()` | `wslcb reprocess-endorsements` |
| `record_entities` | `entities.parse_and_link_entities()` | `wslcb reprocess-entities` |
| `record_links` | `link_records.build_all_links()` | `wslcb rebuild-links` |

`process_record()` is idempotent — deletes existing rows before inserting fresh ones.

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
- Tailwind via CDN + custom `tailwind.config` in `base.html`. No bundler.
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
uv run wslcb backfill-addresses
uv run wslcb cleanup-redundant

# Rebuild DB from archived sources (long — run in tmux)
uv run wslcb rebuild --output data/wslcb-rebuilt.db [--force] [--verify]
```

See [`docs/DEPLOYMENT.md`](DEPLOYMENT.md) for systemd services, address validation, and ops commands.
See [`docs/SOURCE_PAGE.md`](SOURCE_PAGE.md) for WSLCB source page structure and field label quirks (needed when touching `parser.py`).
See [`docs/SCHEMA.md`](SCHEMA.md) for full table/column reference and migration history.
