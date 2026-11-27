# Database Schema Reference

Detailed reference for all PostgreSQL tables, columns and constraints. The Alembic migration chain is in [`MIGRATIONS.md`](MIGRATIONS.md).
For high-level architecture and module descriptions, see [`AGENTS.md`](../AGENTS.md).

## Tables

### `locations` (address normalization table)
- One row per unique raw address string from the WSLCB source
- `raw_address` (UNIQUE) — the first-seen raw string, normalized (NBSP → space)
- `city`, `state`, `zip_code` — regex-parsed from raw address at creation time
- `std_address_line_1` — USPS-standardized street address (e.g., `1200 WESTLAKE AVE N`); empty string if none
- `std_address_line_2` — secondary unit designator (e.g., `STE 100`, `# A1`, `UNIT 2`); NULL if none (migration 0004; older rows may have empty string)
- `std_city` — standardized city name from the address validator
- `std_postal_code` — standardized postal code, may include +4 suffix (e.g., `98109-3528`)
- `std_country` — ISO 3166-1 alpha-2 country code (e.g., `US`); validated before storage; empty string if invalid
- `std_region` — standardized state/region code from the address validator
- `std_address_string` — full single-line address string from the API (`standardized` from `/standardize`, `validated` from `/validate`; e.g., `1200 WESTLAKE AVE N  SEATTLE WA 98109`); NULL if never standardized
- `validation_status` — last `/validate` answer: `confirmed`, `confirmed_missing_secondary`, `confirmed_bad_secondary`, `not_confirmed`, `invalid`, `not_found`, or `undetermined` (`standardized` after a standardize-only run; legacy `unavailable` from before address-validator's contract v2); NULL if not yet validated. A no-determination answer (`undetermined`/`unavailable`) or a DPV-less (Google) confirmation of a USPS-confirmed row is written only when the row has no prior status; otherwise the earlier status is kept (#187) — except that a no-determination answer replaces an earlier no-determination status, so legacy `unavailable` rows take `undetermined` (#189), and so does a DPV-less `confirmed*` status, keeping its `std_*` and `address_validated_at` (#190). "USPS-confirmed" means `address_validated_at` is set and the row is not a `confirmed*` status with a NULL `dpv_match_code`: a later non-confirming answer can clear the DPV code but keeps the confirmed `std_*` (#189)
- `dpv_match_code` — USPS DPV match code (e.g., `Y` = confirmed, `S` = correctable, `D` = missing secondary, `N` = not confirmed); NULL if not yet validated
- `latitude` — WGS84 latitude from the address validator; NULL if not confirmed
- `longitude` — WGS84 longitude from the address validator; NULL if not confirmed
- `address_validated_at` — TIMESTAMPTZ of the last time a `/validate` call **confirmed** this address (provenance). NULL = never confirmed. Written only on a confirmed response; never cleared or written on failure, so a later not_confirmed re-check does not blank it (#150)
- `address_validation_attempted_at` — TIMESTAMPTZ of the last **answered** `/validate` call for this row, confirmed or not (scheduling). Every HTTP 200 counts as answered, including `unavailable` from a named provider (#187). A call with no answer (transport failure, 429/5xx after retries, or `unavailable` with no provider configured) leaves it untouched so the row is retried next run (#183). This — not `address_validated_at` — is the TTL renewal key (`backfill_addresses` re-checks rows whose attempt is older than their renewal TTL: 730 days if active, 2,190 if inactive or unplaced by USPS, #195/#245), and its count over the last 24h is the answered-call total used for the rolling `DAILY_VALIDATION_LIMIT` ceiling (#189/#190). Not set by standardize-only (validation-disabled) runs. Added by Alembic revision `0005`; backfilled from `address_validated_at` (#150)
- Most `std_*` columns default to empty string; `std_address_line_2` is nullable (NULL = no second line; query layer normalises via `COALESCE`). `std_address_string`, `validation_status`, `dpv_match_code`, `latitude`, `longitude` are also nullable
- All `std_*` text is stored uppercase whichever provider answered — Google-grade answers arrive mixed case and split the city filter (#188); rows written before it are fixed by the one-off backfill tracked on #188
- New records that reference an already-known raw address reuse the existing location row (no redundant API call)
- `get_or_create_location()` in `db.py` handles the upsert logic (uses `_normalize_raw_address()` from `text_utils.py`)

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

### Entity tables
Columns, constraints and rules: [`SCHEMA-ENTITIES.md`](SCHEMA-ENTITIES.md). One line per table:
- [`entities`](SCHEMA-ENTITIES.md#entities-applicant-normalization-table) — one row per unique applicant name (person or organization), with its resolved type and subtype
- [`entity_aliases`, `entity_alias_dismissals`](SCHEMA-ENTITIES.md#entity_aliases-entity_alias_dismissals) — alias links and dismissals (#262)
- [`entity_splits`, `entity_split_members`](SCHEMA-ENTITIES.md#entity_splits-entity_split_members) — couple verdicts and their people (#269)
- [`entity_classifications`](SCHEMA-ENTITIES.md#entity_classifications-per-source-classification-answers) — append-only classification answers, one row per source answer (#206)
- [`entity_review_flags`](SCHEMA-ENTITIES.md#entity_review_flags-operator-review-flags) — operator review flags; an open one protects an entity from the noise purge
- [`classification_runs`](SCHEMA-ENTITIES.md#classification_runs-jev-run-records) — one row per Jev classification run
- [`record_entities`](SCHEMA-ENTITIES.md#record_entities-junction-table) — `license_records` ↔ `entities` junction: role, position, `party_id`, `via_entity_id`

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
- Which enrichment steps ran on each record, and with which logic version
- `record_id` — FK to `license_records(id)`, `ON DELETE CASCADE`; composite PK `(record_id, step)`
- `step` — `'endorsements'`, `'entities'`, `'outcome_link'`; `'address'` is retired (SQLite-era rows)
- `completed_at` — TIMESTAMPTZ of when the step finished
- `version` — the step's logic version; a stamp below the current one is *behind* (`wslcb db check` counts them)
- Written only by `enrichment.stamp_enrichment()` (ingest, reprocess, backfills, admin unalias); versions, bump rule, history: `enrichment.py` (#267)

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
- `snapshot_path` stores the path to the archived HTML snapshot, relative to `CO_WSLCB_LICENSING_TRACKER_DATA_DIR` (e.g., `wslcb/licensinginfo/2025/2025_07_09/2025_07_09-licensinginfo.lcb.wa.gov-v1.html`); `NULL` if archiving failed or scrape was `unchanged`
- `origin` — `'server'` (the VM's own fetch; default) or `'relay'` (an operator capture imported by `wslcb ingest import-relayed`, #201); CHECK constraint `ck_scrape_log_origin` (Alembic `0008`)
- `captured_at` — a relay's off-host fetch time; `NULL` for server runs. `scrape_health()` dates freshness by `coalesce(captured_at, finished_at, started_at)` and counts errors from `origin = 'server'` rows only ([`RELAY.md`](RELAY.md) § Monitoring)

### `source_types` (provenance enum)
- Fixed-ID reference table: `1=live_scrape`, `2=co_archive`, `3=internet_archive`, `4=co_diff_archive`, `5=manual`, `6=co_replay`, `7=relayed_scrape`
- Python constants in `db.py`: `SOURCE_TYPE_LIVE_SCRAPE`, etc.
- Seeded by `init_db()` via `INSERT OR IGNORE`
- `co_replay` (Alembic `0006`, #154) — replay-generated provenance extracts: per-record reconstructed `<tbody>` files under `data/wslcb/licensinginfo-replay/`, written by `wslcb ingest generate-replay-extracts`
- `relayed_scrape` (Alembic `0008`, #201) — the WSLCB page captured off-host by the operator during an IP block (#198) and imported by `wslcb ingest import-relayed`; label "Relayed Capture". Its `metadata` is `{"relay": "owner-laptop", "captured_at": …}` — never an IP or hostname

### `sources` (provenance artifacts)
- One row per source artifact (a specific HTML snapshot file or scrape run)
- `source_type_id` — FK to `source_types`
- `snapshot_path` — repo-relative path to archived HTML file; `NULL` if archiving failed
- `sources.snapshot_path` stores the path as it was at ingest time. Files compressed by `wslcb ingest compress-snapshots` are renamed `.html` → `.html.gz` on disk but the DB column is not updated. `parser._read_snapshot()` transparently falls back to the `.gz` sibling, so all read paths work without a migration. Do not "fix" the DB paths — the fallback is the contract. The same contract applies to `data/wslcb/licensinginfo-diffs/` archives: `wslcb ingest compress-diffs` (#137) renames `.txt` → `.txt.gz` in place, and `parser.extract_records_from_diff()` / `glob_with_gz()` transparently tolerate either extension — no DB migration there either.
- `url` — original URL (WSLCB page, Wayback Machine URL)
- `captured_at` — when the source was captured (distinct from ingestion)
- `ingested_at` — when we processed it
- `scrape_log_id` — FK to `scrape_log` for live scrapes (avoids duplicating operational data)
- `metadata` — JSON blob for source-specific attributes (`truncated`, `file_size_bytes`, `sections_present`, `sha256`, `wayback_timestamp`)
- UNIQUE constraint on `(source_type_id, snapshot_path)`
- `get_or_create_source()` in `db.py` handles idempotent upsert

### `record_sources` (provenance junction)
- M:M junction linking `license_records` ↔ `sources`
- `role` — `'first_seen'` (introduced by this source), `'confirmed'` (already existed, corroborated), `'repaired'` (data fixed from this source), `'replay_extract'` (replay-generated provenance extract, Alembic `0007`, #154); enforced by CHECK constraint
- Composite PK `(record_id, source_id, role)` — a record can have multiple roles for the same source (e.g., `first_seen` + `repaired`)
- `link_record_source()` in `db.py` handles idempotent insert
- `ON DELETE CASCADE` on both FKs
- `get_record_sources()` in `db.py` returns provenance for display on detail page

## Migration Framework

The Alembic chain, its revision history, and how to add a migration are in
[`MIGRATIONS.md`](MIGRATIONS.md).
