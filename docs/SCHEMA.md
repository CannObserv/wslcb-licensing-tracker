# Database Schema Reference

Detailed reference for all SQLite tables, columns, constraints, and the migration framework.
For high-level architecture and module descriptions, see [`AGENTS.md`](../AGENTS.md).

## Tables

### `locations` (address normalization table)
- One row per unique raw address string from the WSLCB source
- `raw_address` (UNIQUE) — the first-seen raw string, normalized (NBSP → space)
- `city`, `state`, `zip_code` — regex-parsed from raw address at creation time
- `std_address_line_1` — USPS-standardized street address (e.g., `1200 WESTLAKE AVE N`); empty string if none
- `std_address_line_2` — secondary unit designator (e.g., `STE 100`, `# A1`, `UNIT 2`); NULL if none (migration 0004; older rows may have empty string)
- `std_city` — standardized city name from the address validator
- `std_state` — standardized 2-letter state code
- `std_zip` — standardized ZIP code, may include +4 suffix (e.g., `98109-3528`)
- `std_postal_code` — standardized postal code from `/api/v1/validate`
- `std_country` — ISO 3166-1 alpha-2 country code (e.g., `US`); validated before storage; empty string if invalid
- `std_region` — standardized state/region code from the address validator
- `validated_address` — full single-line validated address string from the API (e.g., `1200 WESTLAKE AVE N  SEATTLE WA 98109`); NULL if not confirmed
- `validation_status` — USPS DPV status: `confirmed`, `corrected`, `not_confirmed`, or `unavailable`; NULL if not yet validated
- `dpv_match_code` — USPS DPV match code (e.g., `Y` = confirmed, `S` = correctable, `D` = missing secondary, `N` = not confirmed); NULL if not yet validated
- `latitude` — WGS84 latitude from the address validator; NULL if not confirmed
- `longitude` — WGS84 longitude from the address validator; NULL if not confirmed
- `address_validated_at` — TIMESTAMPTZ of when the address was confirmed (i.e., provider returned `address_line_1`); NULL = not yet confirmed (includes `not_confirmed` and `unavailable` responses)
- Most `std_*` columns default to empty string; `std_address_line_2` is nullable (NULL = no second line; query layer normalises via `COALESCE`). `validated_address`, `validation_status`, `dpv_match_code`, `latitude`, `longitude` are also nullable
- New records that reference an already-known raw address reuse the existing location row (no redundant API call)
- `get_or_create_location()` in `pg_db.py` handles the upsert logic (uses `_normalize_raw_address()` from `text_utils.py`)

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
- Seeded from `SEED_CODE_MAP` in `endorsements.py` (103 codes); auto-discovered codes are added by `discover_code_mappings()`

### `endorsement_aliases` (alias resolution table)
- One row per aliased endorsement; maps a variant `license_endorsements` row to its canonical counterpart
- `endorsement_id` — FK to the variant (UNIQUE — each endorsement can have at most one canonical)
- `canonical_endorsement_id` — FK to the preferred canonical endorsement
- `created_by` — admin email of the person who created the alias
- Original rows in `license_endorsements` and `record_endorsements` are never modified; alias resolution happens at query time
- `resolve_endorsement(conn, id)` in `endorsements.py` returns the canonical ID (or the same ID if no alias exists)
- `set_canonical_endorsement(conn, canonical_id, variant_ids, created_by)` creates alias rows via `INSERT … ON CONFLICT DO UPDATE` (idempotent)
- `rename_endorsement(conn, endorsement_id, new_name, created_by)` creates a new named endorsement and aliases the bare-code row to it
- `get_endorsement_list(conn)` returns all endorsements as a flat list with `is_canonical`, `is_variant`, `canonical_id`, `canonical_name`, `record_count`, and `codes` fields — used by the revised admin UI
- `get_endorsement_groups(conn)` (legacy) returns endorsements grouped by numeric code; still available but no longer used by the admin UI
- `get_endorsement_options()` and `get_record_endorsements()` both apply alias resolution via a LEFT JOIN so the filter dropdown and record display show canonical names only

### `endorsement_dismissed_suggestions` (duplicate-suggestion suppression table)
- One row per admin-dismissed near-duplicate pair; prevents the pair from being re-surfaced by `suggest_duplicate_endorsements()`
- `endorsement_id_a`, `endorsement_id_b` — always stored with smaller ID first (enforced by `CHECK (endorsement_id_a < endorsement_id_b)`)
- `dismissed_by` — admin email; `dismissed_at` — TIMESTAMPTZ
- Dismissal is permanent but does not prevent explicit aliasing through the main alias action
- `dismiss_suggestion(conn, id_a, id_b, dismissed_by)` in `endorsements.py` handles normalisation and idempotent insert
- Added by migration 008 (`endorsement_dismissed_suggestions`)

### `regulated_substances` (regulated substance taxonomy)
- One row per regulated substance category (e.g., "Cannabis", "Alcohol")
- `name` (UNIQUE, COLLATE NOCASE) — display name shown in the search filter dropdown
- `display_order` — controls order in the dropdown (lower = first)
- Seeded by migration 009 with "Cannabis" (display_order=1) and "Alcohol" (display_order=2)
- Managed via `/admin/endorsements?section=substances`; CRUD functions in `endorsements.py`

### `regulated_substance_endorsements` (substance↔endorsement junction)
- M:M junction linking `regulated_substances` ↔ `license_endorsements`
- Composite PK `(substance_id, endorsement_id)` with `ON DELETE CASCADE` on both FKs
- `idx_rse_endorsement` index on `endorsement_id` for reverse lookups
- **Cannabis** seed rule: endorsement name `LIKE '%CANNABIS%'` OR `LIKE '%RETAIL CERT%'` OR `= 'TRIBAL COMPACT'`
- **Alcohol** seed rule: all other endorsements except `UNDEFINED` (which remains unassigned)
- Consumed by `get_filter_options()` via `get_regulated_substances()` to populate the `regulated_substance` filter key used client-side in `search.html` to pre-select License Type checkboxes

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
- `clean_applicants_string()` applies `strip_duplicate_marker()` then `clean_entity_name()` to each element of a semicolon-delimited applicants string, then deduplicates tokens (first-occurrence-wins, preserving order) — used at ingest time so the `applicants`/`previous_applicants` columns on `license_records` stay consistent with entity names; DUPLICATE-annotated duplicates of the same person are collapsed to one token
- `insert_record()` in `pipeline.py` also cleans `business_name` and `previous_business_name` via `clean_entity_name()` before storage, so all name columns are consistently uppercased and stripped of stray punctuation
- `has_additional_names` — `INTEGER NOT NULL DEFAULT 0`; set to `1` at ingest time when `applicants` or `previous_applicants` contains an `ADDITIONAL_NAMES_MARKERS` token. Used to show the notice *"+ WSLCB source signaled additional entities may be on file"* on the detail page. Backfilled by migration 010.
- `raw_business_name`, `raw_previous_business_name`, `raw_applicants`, `raw_previous_applicants` — shadow columns storing the as-parsed values from the source *before* name cleaning (uppercase, punctuation stripping); going forward, `insert_record()` saves the raw values here, then writes cleaned values to the primary columns; for existing records, these were backfilled with the already-cleaned values (originals lost)
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
- `linked_at` — TIMESTAMPTZ of when the link was created
- UNIQUE on `(new_app_id, outcome_id)` — prevents duplicate links
- Indexed on both `new_app_id` and `outcome_id` for fast lookups from either direction
- Rebuilt from scratch by `build_all_links()` in `link_records.py`; incrementally updated by `link_new_record()` during scraping
- `DATE_TOLERANCE_DAYS = 7` — the ±7-day window handles outcome-before-notification date patterns
- Approval linking: `new_application` → `approved` with same `application_type` (RENEWAL, NEW APPLICATION, ASSUMPTION, etc.)
- Discontinuance linking: `new_application/DISC. LIQUOR SALES` → `discontinued/DISCONTINUED`
- `PENDING_CUTOFF_DAYS = 180` — unlinked applications older than this are classified as "unknown" instead of "pending"
- `DATA_GAP_CUTOFF = '2025-05-12'` — post-gap NEW APPLICATION records get "data_gap" status (WSLCB stopped publishing these approvals)
- `ON DELETE CASCADE` on both FKs

### `record_enrichments` (enrichment tracking)
- Tracks which enrichment steps have been applied to each record
- `record_id` — FK to `license_records(id)`, `ON DELETE CASCADE`
- `step` — enrichment step name: `'endorsements'`, `'entities'`, `'address'`, `'outcome_link'`
- `completed_at` — TIMESTAMPTZ of when the step finished
- `version` — schema/logic version of the step (default `'1'`); allows re-processing when step logic changes
- Composite PK `(record_id, step)`
- Written by `_record_enrichment()` in `pipeline.py` after each enrichment step succeeds; uses `INSERT OR REPLACE` so re-runs update the timestamp
- Step name constants exported from `pipeline.py`: `STEP_ENDORSEMENTS`, `STEP_ENTITIES`, `STEP_ADDRESS`, `STEP_OUTCOME_LINK`
- Enables targeted re-processing queries, e.g., find records missing entity linking: `WHERE id NOT IN (SELECT record_id FROM record_enrichments WHERE step = 'entities')`

### `license_records_fts` (FTS5 virtual table)
- Indexes: business_name, business_location, applicants, license_type, resolved_endorsements, application_type, license_number, previous_business_name, previous_applicants, previous_business_location
- Uses `license_records_fts_content` VIEW as its content source — this view JOINs `license_records` → `locations` to expose `raw_address` as `business_location` / `previous_business_location` for indexing
- Kept in sync via triggers on `license_records`: AFTER INSERT inserts new values; updates use a BEFORE UPDATE / AFTER UPDATE pair (delete old, insert new); BEFORE DELETE removes old values. All read from the content view
- Never write to the FTS table directly
- `resolved_endorsements` (semicolon-joined resolved endorsement names, kept in sync by `process_record()`) enables FTS text search to match approved/discontinued records whose raw `license_type` is a numeric code.

### `admin_users`
- One row per admin user; keyed by email (`COLLATE NOCASE`)
- `role` — `'admin'` (only value currently); column exists for future RBAC extensibility
- `created_by` — `'system'` for seed rows, `'cli'` for CLI-added users, email of creating admin for UI-added users
- Managed via `python cli.py admin add-user / list-users / remove-user`; removing the last admin is blocked
- Auth middleware in `admin_auth.py` checks this table on every admin request (result cached on `request.state`)

### `admin_audit_log`
- One row per admin mutation; written by `admin_audit.py` `log_action()`
- `action` — `{domain}.{verb}` pattern (e.g., `endorsement.set_canonical`, `admin_user.add`)
- `target_type` — entity type affected (e.g., `'endorsement'`, `'admin_user'`)
- `target_id` — optional FK to the affected row
- `details` — JSON-serialized dict with action-specific context (old/new values, counts); `NULL` if not applicable

### `scrape_log`
- One row per scrape run with status, record counts, timestamps, error messages
- `status` values: `'running'`, `'success'`, `'error'`, `'unchanged'` (page content identical to last scrape)
- `content_hash` — SHA-256 hex digest of the fetched HTML; used to detect unchanged pages and skip redundant parsing/ingestion
- `snapshot_path` stores the path to the archived HTML snapshot, relative to `DATA_DIR` (e.g., `wslcb/licensinginfo/2025/2025_07_09/2025_07_09-licensinginfo.lcb.wa.gov-v1.html`); `NULL` if archiving failed or scrape was `unchanged`

### `source_types` (provenance enum)
- Fixed-ID reference table: `1=live_scrape`, `2=co_archive`, `3=internet_archive`, `4=co_diff_archive`, `5=manual`
- Python constants in `pg_db.py`: `SOURCE_TYPE_LIVE_SCRAPE`, etc.
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
- `get_or_create_source()` in `pg_db.py` handles idempotent upsert

### `record_sources` (provenance junction)
- M:M junction linking `license_records` ↔ `sources`
- `role` — `'first_seen'` (introduced by this source), `'confirmed'` (already existed, corroborated), `'repaired'` (data fixed from this source); enforced by CHECK constraint
- Composite PK `(record_id, source_id, role)` — a record can have multiple roles for the same source (e.g., `first_seen` + `repaired`)
- `link_record_source()` in `pg_db.py` handles idempotent insert
- `ON DELETE CASCADE` on both FKs
- `get_record_sources()` in `pg_db.py` returns provenance for display on detail page

## Migration Framework

- `PRAGMA user_version` tracks the current schema version; each migration bumps it
- `MIGRATIONS` list in `schema.py`: `(version, name, function)` tuples run in order
- Fresh databases run all migrations starting from 0 (baseline creates the full schema)
- Existing databases (tables present, `user_version == 0`) are stamped to `_EXISTING_DB_STAMP_VERSION = 1` — the last version subsumed by their pre-framework schema — then the migration loop runs everything above it
- Migration 001 (`baseline`): full initial schema (all tables, indexes, seed data)
- Migration 002 (`enrichment_tracking`): adds `record_enrichments` table, adds `raw_*` shadow columns to `license_records` (conditionally via `PRAGMA table_info`), backfills `raw_* = cleaned values` for existing records
- Migration 003 (`content_hash`): adds `content_hash TEXT` column to `scrape_log` for SHA-256 deduplication of fetched HTML
- Migration 004 (`address_validator_v1`): renames `std_state`→`std_region`, `std_zip`→`std_postal_code`, adds `std_country`; backfills `'US'` for validated rows
- Migration 005 (`admin_users`): adds `admin_users` table
- Migration 006 (`admin_audit_log`): adds `admin_audit_log` table
- Migration 007 (`endorsement_aliases`): adds `endorsement_aliases` table with `UNIQUE(endorsement_id)` constraint
- Migration 008 (`endorsement_dismissed_suggestions`): adds `endorsement_dismissed_suggestions` table with `CHECK (endorsement_id_a < endorsement_id_b)` constraint and `ON DELETE CASCADE` FKs
- Migration 009 (`regulated_substances`): adds `regulated_substances` and `regulated_substance_endorsements` tables
- Migration 010 (`additional_names_flag`): adds `has_additional_names INTEGER NOT NULL DEFAULT 0` to `license_records`; backfills from `applicants`/`previous_applicants`; deletes spurious `ADDITIONAL NAMES ON FILE` and `ADDTIONAL NAMES ON FILE` entity rows (cascade removes `record_entities` links) with `idx_rse_endorsement` index; seeds Cannabis and Alcohol substance rows and their endorsement associations from existing `license_endorsements` data
- Migration 011 (`clean_duplicate_markers`): strips WSLCB `DUPLICATE` annotation tokens from `applicants` and `previous_applicants` columns in `license_records` (frozen `raw_*` shadow columns are left untouched); deletes all `entities` rows whose `name LIKE '%DUPLICATE%'`; `record_entities` cascade-deletes automatically; after this migration run `cli.py reprocess-entities` to rebuild entity links from the cleaned strings
- Migration 012 (`entities_name_index`): adds `idx_entities_name` index on `entities(name)` for faster entity lookup and deduplication queries
- Migration 013 (`address_validator_v2`): renames `address_line_1`→`std_address_line_1` and `address_line_2`→`std_address_line_2` (consistent with the `std_` prefix on other standardized columns); adds five new columns to store results from `POST /api/v1/validate`: `validated_address TEXT`, `validation_status TEXT`, `dpv_match_code TEXT`, `latitude REAL`, `longitude REAL`
- Migration 014 (`address_standardize_pipeline`): renames `validated_address`→`std_address_string`; adds `address_standardized_at TEXT`; backfills `address_standardized_at` from `address_validated_at` for rows already DPV-validated
- Migration 015 (`resolved_endorsements`): adds `resolved_endorsements TEXT NOT NULL DEFAULT ''` to `license_records`; backfills from `record_endorsements` (semicolon-joined, alphabetically ordered endorsement names); also included in FTS via `_FTS_COLUMNS` and the `license_records_fts_content` view
- To add a new migration: write a function, append a `(version, name, fn)` tuple to `MIGRATIONS`; include the new columns/tables in `_m001_baseline()` as well (for fresh installs)
