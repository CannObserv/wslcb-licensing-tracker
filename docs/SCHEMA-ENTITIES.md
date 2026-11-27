# Entity Tables Schema Reference

Columns, constraints and rules of the applicant-entity tables, split out of [`SCHEMA.md`](SCHEMA.md) (#271), which keeps a one-line pointer per table. Behaviour and operator workflows: [`ENTITY_ALIASES.md`](ENTITY_ALIASES.md) (alias links, relink), [`COUPLE_SPLITS.md`](COUPLE_SPLITS.md) (couple verdicts), [`TOMBSTONES.md`](TOMBSTONES.md) (confirmed non-names).

## `entities` (applicant normalization table)
- One row per unique applicant name (person or organization)
- `name` (UNIQUE) — the normalized/cleaned name (uppercased, stray trailing punctuation stripped)
- `entity_type` (`person` / `organization`; `''` legacy), `entity_subtype` (set iff `organization`, one of seven; CHECKs `ck_entities_subtype`, `ck_entities_subtype_matches_type` #212), `classification_source` (`regex` / `jev` / `manual`; default `regex`) — **resolved** columns, seeded on insert, then written only by `entity_classification.resolve_entities()`: current manual row > non-ignored Jev row ≥ `…_JEV_CONFIDENCE_THRESHOLD` (0.7) with a real type > regex row (#206)
- Regex answer: `regex_classification()` = `entity_category()` (trust / legal suffix / truncated suffix / tribe / government / org keyword / estate; #203) via `REGEX_SUBTYPE` (suffixes, org keywords → `business`; never `nonprofit`), stored with `model = REGEX_MODEL` (own version). `get_or_create_entity()` writes it on insert only; after a classifier change run `wslcb db reclassify-entities [--dry-run]` (appends changed regex rows, re-resolves, never overrides manual)
- Non-name applicant parts (`is_noise_name()`: WAC citations, bare numbers, ownership-count annotations like `17 SHAREHOLDERS W/10% OR LESS`, bare legal suffixes, `NO PEOPLE REQUIRED`, a few observed fragments) are skipped like `ADDITIONAL_NAMES_MARKERS`; `reclassify-entities` deletes existing ones unless protected by a manual decision or open flag; a confirmed `not_a_name` stays, unlinked ([`TOMBSTONES.md`](TOMBSTONES.md)). An appended citation (`7-ELEVEN, INC. PER WAC 314-07-035`) is stripped from the entity name at extraction, not from `applicants` (#211)
- The first element of the semicolon-delimited `applicants` field (which equals `business_name`) is **excluded** — only the individual people/orgs behind the license are stored
- Extraction reads `entities.entity_source(stored, raw)`: the `raw_*` shadow when it cleans to the stored column, else the stored column (#266). `clean_applicants_string()` de-duplicates a later repeat of element 0 away, but that repeat is a listed applicant (the LLC sharing the trade name, or a sole proprietor's `X; X`), so it is linked. Ingest, `reprocess-entities`, `backfill_entities`, `db check` and the WAC-repair check all read through it
- `get_or_create_entity()` normalizes names via `clean_entity_name()`: uppercased, trimmed, stray trailing periods and commas removed unless part of a recognized suffix
- `_LEGIT_TRAILING_DOT` (`entities.py`) is the suffix allowlist: add an abbreviation there when WSLCB prints a new one ending in a period. Current list: `INC`, `LLC`, `L.L.C`, `L.L.P`, `LTD`, `CORP`, `CO`, `L.P`, `PTY`, `JR`, `SR`, `S.P.A`, `F.O.E`, `U.P`, `D.B.A`, `P.C`, `N.A`, `P.A`, `W. & S`
- `clean_applicants_string()` applies `strip_duplicate_marker()` then `clean_entity_name()` to each element, then de-duplicates (first occurrence wins, order kept), so DUPLICATE-annotated repeats collapse to one; ingest uses it to keep `applicants`/`previous_applicants` consistent with entity names
- `insert_record()` in `pipeline.py` also cleans `business_name` and `previous_business_name` via `clean_entity_name()` before storage, so all name columns are consistently uppercased and stripped of stray punctuation
- `has_additional_names` — `INTEGER NOT NULL DEFAULT 0`; set to `1` at ingest time when `applicants` or `previous_applicants` contains an `ADDITIONAL_NAMES_MARKERS` token. Used to show the notice *"+ WSLCB source signaled additional entities may be on file"* on the detail page. Backfilled by migration 010.
- `raw_business_name`, `raw_previous_business_name`, `raw_applicants`, `raw_previous_applicants` — as-parsed values before name cleaning: `insert_record()` stores them here and the cleaned values in the primary columns. Records older than these columns were backfilled with already-cleaned values (originals lost)
- `merge_duplicate_entities()` merges entities whose names differ only by case into the lowest id, moving its links. It ran once, through `backfill_entities()` in data migration `0005_backfill_entities` (`data_migration.py`), not at every startup
- `link_key` (#262) — derived alias format key; index `idx_entities_link_key`

## `entity_aliases`, `entity_alias_dismissals`
- Alias links and dismissals (#262): `rule` rows derived (`relink-entities`), the rest operator data. See [`ENTITY_ALIASES.md`](ENTITY_ALIASES.md)

## `entity_splits`, `entity_split_members`
- Couple verdicts and their people (#269): operator data. See [`COUPLE_SPLITS.md`](COUPLE_SPLITS.md)

## `entity_classifications` (per-source classification answers)
- Append-only (#206): one row per answer from one source. `source` — `regex`, `jev` or `manual` (CHECK `ck_ec_source`)
- `entity_id` — FK to `entities(id)`, `ON DELETE CASCADE`
- `entity_type` — `person`, `organization` or `not_a_name` (CHECK `ck_ec_entity_type`); `not_a_name` is stored as an answer and never resolved onto `entities`
- `entity_subtype` — set iff `organization`, one of the seven (CHECKs `ck_ec_subtype`, `ck_ec_subtype_matches_type` #212)
- `confidence`, `probabilities` (JSONB), `input_tokens` — Jev only; `input_tokens` is this entity's share of its batch
- `model` — `jev-…` for Jev, `regex-<n>` (`entities.REGEX_MODEL`) for regex, NULL for manual
- `created_at`, `created_by` (admin email on manual rows), `note`
- `ignored_at` — set by `wslcb db ignore-jev` (Jev rows) or a cleared manual override; an ignored row never takes part in resolution
- A source's *current* answer is its latest (`max(id)`) non-ignored row. Indexes `idx_ec_entity_source (entity_id, source, id)`, `idx_ec_source_model (source, model)`

## `entity_review_flags` (operator review flags)
- Columns (#206): `entity_id` (FK `ON DELETE CASCADE`), `flagged_by`, `reason`, `created_at`, `resolved_at`, `resolved_by`, `resolution`
- Open = `resolved_at IS NULL` (partial index `idx_erf_open`). An open flag protects a noise-named entity from the `reclassify-entities` purge
- Operator input: `merge_duplicate_entities()` moves them to the surviving entity; deleting an entity (noise purge) deletes them with it

## `classification_runs` (Jev run records)
- One row per Jev classification run (#206): `origin` (`post_scrape`, `post_import`, `cli`, `admin`; CHECK `ck_classification_runs_origin`), `model`, `started_at`, `finished_at`, `names_sent`, `batches_ok`, `batches_failed`, `input_tokens` (BIGINT), `error`
- Indexed on `started_at`. Written from #207 on

## `record_entities` (junction table)
- Links `license_records` ↔ `entities` with role and position
- `role` — `'applicant'` or `'previous_applicant'` (for ASSUMPTION seller applicants)
- `position` — 0-indexed ordering from the source document (after the business name)
- Composite PK `(record_id, entity_id, role)`
- `party_id` — derived alias party (#263, [`ENTITY_ALIASES.md`](ENTITY_ALIASES.md))
- `via_entity_id` — the split couple a member row is linked through; NULL on printed rows (#269, [`COUPLE_SPLITS.md`](COUPLE_SPLITS.md))
- Populated at ingest by `parse_and_link_entities()`; rebuilt by `reprocess-entities`
- `ON DELETE CASCADE` on both FKs
