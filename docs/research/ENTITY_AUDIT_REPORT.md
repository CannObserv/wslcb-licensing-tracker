# Entity Name Cleaning — Integration Audit Report

## Summary of `_clean_entity_name()` behavior

Strips whitespace, uppercases, then iteratively removes trailing `.` and `,`
unless the name ends with a recognized suffix (`INC.`, `LLC.`, `JR.`, etc.).

---

## 1. Ingestion Paths — Is cleaning applied everywhere?

### Path A: Live scraper (`scraper.py` → `queries.py:insert_record()`)
- `scraper.py` scrapes HTML, calls `insert_record(conn, rec)` (queries.py:127).
- `insert_record()` stores raw `applicants` string in `license_records.applicants`.
- Then calls `_parse_and_link_entities(conn, record_id, record["applicants"], "applicant")` (queries.py:156).
- Also calls `_parse_and_link_entities()` for `previous_applicants` (queries.py:159–160).
- `_parse_and_link_entities()` → `get_or_create_entity()` → `_clean_entity_name()`. ✅ Cleaning applied.

### Path B: Backfill from snapshots (`backfill_snapshots.py`)
- `_ingest_records()` calls `insert_record()` — **same path as A**. ✅ Cleaning applied.
- **`_repair_assumptions()`** (backfill_snapshots.py:69–93): Directly UPDATEs `license_records.applicants` and `previous_applicants` via raw SQL **without** calling `_parse_and_link_entities()`. See **Bug #1**.

### Path C: Backfill entities for existing records (`entities.py:backfill_entities()`)
- Called from `app.py` lifespan on startup (app.py:38).
- Queries records that have no `record_entities` links yet.
- Calls `_parse_and_link_entities()` → `get_or_create_entity()` → `_clean_entity_name()`. ✅ Cleaning applied.
- Also calls `merge_duplicate_entities()` to fix pre-existing dirty entities. ✅

---

## 2. FTS Consistency — Raw names indexed, cleaned names linked

| File | Line | Severity | Description |
|------|------|----------|-------------|
| database.py | 198–212 | **minor** | **FTS indexes raw `applicants` / `previous_applicants` strings** (e.g., `"WOLDU ARAYA BERAKI."`), but the `entities` table stores the cleaned version (`"WOLDU ARAYA BERAKI"`). An FTS search for `"BERAKI."` with the trailing period would match the raw field but the entity page shows the cleaned name. Conversely, a search won't match if a user copies the cleaned name and searches with quotes. In practice this is low-impact because FTS prefix matching (`BERAKI*`) works for both, and users rarely type trailing periods. **Not a bug** — cosmetic only. |

---

## 3. CSV Export Consistency

| File | Line | Severity | Description |
|------|------|----------|-------------|
| app.py | 145–162 | **minor** | CSV export uses `r.get("applicants")` — the **raw** semicolon-delimited string from `license_records`. It does NOT use entity names from the `entities` table. A CSV consumer will see `"BUSINESS NAME; WOLDU ARAYA BERAKI."` (with trailing period) even though the entity table has the cleaned version. The export does not include an `entities` column. This is a **data inconsistency between the CSV and the web UI entity links**, but the raw field preserves provenance, so it's arguably correct for export. |

---

## 4. Display — Are both raw and cleaned names visible?

| File | Line | Severity | Description |
|------|------|----------|-------------|
| templates/detail.html | ~various | **OK** | The detail page shows entity names from the `entities` table (cleaned) via the `entity_links()` macro. It does **not** also display the raw `applicants` string alongside. The raw `applicants` field is never rendered on the detail page — **no dual display issue**. ✅ |
| templates/partials/record_table.html | — | **OK** | The search results table does not display applicant names at all (only business name, location, etc.). ✅ |
| templates/entity.html | — | **OK** | Shows `entity.name` (cleaned) from the `entities` table. ✅ |

---

## 5. `previous_applicants` Path

| File | Line | Severity | Description |
|------|------|----------|-------------|
| queries.py | 159–160 | **OK** | `insert_record()` calls `_parse_and_link_entities(conn, record_id, record["previous_applicants"], "previous_applicant")` when `previous_applicants` is truthy. ✅ Cleaning applied via `get_or_create_entity()`. |
| entities.py | 117–120 | **OK** | `backfill_entities()` also processes `previous_applicants` with role `"previous_applicant"`. ✅ |

---

## 6. Backfill Path

| File | Line | Severity | Description |
|------|------|----------|-------------|
| entities.py | 102–127 | **OK** | `backfill_entities()` uses `_parse_and_link_entities()` → `get_or_create_entity()` → `_clean_entity_name()`. ✅ Same cleaning pipeline. Also calls `merge_duplicate_entities()` as a safety net. |

---

## Bugs & Issues Found

### Bug #1 (medium): `_repair_assumptions()` bypasses entity creation

- **File:** `backfill_snapshots.py`, lines 69–93
- **Severity:** **bug**
- **Description:** `_repair_assumptions()` directly UPDATEs `license_records.applicants` and `previous_applicants` columns via raw SQL. It does **not** call `_parse_and_link_entities()` after updating, so:
  1. Records repaired by this function will have updated applicant strings but **no corresponding entity links** in `record_entities`.
  2. These orphaned records would only get entity links on the next `backfill_entities()` run (which only processes records with zero entity links — but repaired records may already have stale links from the original broken insert).
  3. Actually, looking more carefully: the original broken insert had `business_name = ''` and `applicants` could also be empty. `insert_record()` would have called `_parse_and_link_entities()` with the empty string, creating no links. Then `_repair_assumptions()` updates the applicant text. So the record now has applicant text but no entity links. `backfill_entities()` queries `WHERE re.record_id IS NULL` — if no links were ever created, this record **will** be picked up. ✅ 
  4. **However**, if the original insert DID have some applicants (only `business_name` was empty), then entity links already exist, and the repair updates `applicants`/`previous_applicants` to new values — but the old entity links remain. The new names are never linked. **This is the actual bug.**
- **Fix:** After the UPDATE in `_repair_assumptions()`, call `_parse_and_link_entities()` for the affected record, or delete existing entity links first and re-create them.

### Minor #2: `_repair_assumptions` has no entity link cleanup

- **File:** `backfill_snapshots.py`, lines 69–93  
- **Severity:** **minor** (subset of Bug #1)
- **Description:** When `_repair_assumptions()` overwrites `applicants` and `previous_applicants` on existing records, any old `record_entities` rows pointing to the pre-repair (possibly empty) applicant data are left orphaned. No `DELETE FROM record_entities WHERE record_id = ?` precedes the re-linking.

### Minor #3: Raw `applicants` field in `license_records` is never updated to cleaned form

- **File:** `queries.py`, line 143 / `database.py` schema
- **Severity:** **minor / by-design**
- **Description:** The `license_records.applicants` column always stores the original raw string from the WSLCB source (e.g., `"BUSINESS; WOLDU ARAYA BERAKI."`). This is never retroactively cleaned. The `entities` table has the cleaned version. This is arguably correct (preserving source fidelity) but means the FTS index and CSV export contain the un-cleaned text. Searching for an exact cleaned name in FTS may not match if the dirty suffix changes tokenization.

### Perf #4: `merge_duplicate_entities()` does a full table scan

- **File:** `entities.py`, lines 175–221
- **Severity:** **perf**
- **Description:** `merge_duplicate_entities()` fetches ALL entities and re-cleans each name. For large entity tables, this is O(n). It's called on every `backfill_entities()` invocation (and thus every app startup). After the initial cleanup, most runs will find zero entities to merge, but the full scan still occurs.
- **Fix:** Could add a `WHERE name LIKE '%.%' OR name LIKE '%,%'` filter to the initial query to only check entities that might have trailing punctuation.

### Style #5: Comment says "uppercased by get_or_create_entity" but cleaning is in `_clean_entity_name`

- **File:** `entities.py`, line 14
- **Severity:** **style**
- **Description:** Comment says `# Input is always uppercased by get_or_create_entity()` but the actual uppercasing now happens inside `_clean_entity_name()`. The comment is slightly misleading (though not wrong since `get_or_create_entity` calls `_clean_entity_name`).

---

## Complete Ingestion Path Map

```
WSLCB Website HTML
  │
  ├─ scraper.py:scrape()
  │   └─ queries.py:insert_record()
  │       ├─ INSERT INTO license_records (raw applicants string)
  │       ├─ _parse_and_link_entities(applicants, "applicant")      ✅ cleaned
  │       └─ _parse_and_link_entities(previous_applicants, "prev")  ✅ cleaned
  │
  ├─ backfill_snapshots.py:backfill_from_snapshots()
  │   ├─ _ingest_records() → insert_record()                        ✅ same as above
  │   ├─ _repair_assumptions() → raw SQL UPDATE                     ⚠️  NO entity re-link
  │   └─ _repair_change_of_location() → location only, no entity    (OK, no applicant change)
  │
  └─ app.py lifespan startup
      └─ entities.py:backfill_entities()
          ├─ _parse_and_link_entities(applicants)                    ✅ cleaned
          ├─ _parse_and_link_entities(previous_applicants)           ✅ cleaned
          └─ merge_duplicate_entities()                              ✅ cleanup pass
```

## Consolidated Issue Table

| # | File | Line(s) | Severity | Description |
|---|------|---------|----------|-------------|
| 1 | backfill_snapshots.py | 69–93 | **bug** | `_repair_assumptions()` updates `applicants`/`previous_applicants` without re-linking entities; records with pre-existing (stale) entity links will never be re-processed by `backfill_entities()` |
| 2 | app.py | 145–162 | **minor** | CSV export uses raw `applicants` field (un-cleaned); entity data not exported |
| 3 | database.py | 198–212 | **minor** | FTS indexes raw applicant strings; entity table has cleaned names; minor search inconsistency possible |
| 4 | entities.py | 175–221 | **perf** | `merge_duplicate_entities()` full-scans entities table on every startup; could filter to only dirty-looking names |
| 5 | entities.py | 14 | **style** | Comment attributes uppercasing to `get_or_create_entity` but it's done in `_clean_entity_name` |
