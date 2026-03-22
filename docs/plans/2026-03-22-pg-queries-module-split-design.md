# Design: Split pg_queries.py into focused sub-modules

**Issue:** #101
**Date:** 2026-03-22

## Goal

Break the 926-line `pg_queries.py` integration hub into focused sub-modules, each with a narrow import surface. Remove all imports from `pg_endorsements`, `pg_entities`, `pg_link_records`, and `pg_substances` in the resulting `pg_queries_*.py` files (except where explicitly documented as an integration layer).

## Approved approach: 6-module split

`pg_queries.py` is deleted. No backwards-compat shim. All callers updated directly.

### New modules

| Module | Contents | Domain imports |
|---|---|---|
| `pg_queries_hydrate.py` | `enrich_record`, `_hydrate_records` | `pg_endorsements`, `pg_entities`, `pg_link_records` — acknowledged integration layer |
| `pg_queries_filter.py` | `get_filter_options`, `get_cities_for_state`, `invalidate_filter_cache`, caches | `pg_endorsements`, `pg_substances` |
| `pg_queries_stats.py` | `get_stats`, `_get_pipeline_stats` | `pg_db` constants only |
| `pg_queries_export.py` | `export_records`, `export_records_cursor`, `_EXPORT_SELECT` | `pg_db` constants only |
| `pg_queries_search.py` | `RECORD_COLUMNS`, `RECORD_JOINS`, `_build_where_clause`, `search_records`, `get_record_by_id`, `get_related_records`, `get_record_source_link`, `get_source_by_id`, `get_record_link` | `pg_db` only |
| `pg_queries_entity.py` | `get_entity_records`, `get_entities` | `pg_queries_search` (shared SQL constants), `pg_queries_hydrate` |

### Constants migration

Move from `pg_link_records.py` to `pg_db.py`:
- `DATA_GAP_CUTOFF`
- `LINKABLE_TYPES`
- `PENDING_CUTOFF_DAYS`
- `outcome_filter_sql()`

`pg_link_records.py` then imports them from `pg_db` (no circular dependency).
`pg_queries_search.py`, `pg_queries_stats.py`, and `pg_queries_export.py` import them from `pg_db`.

### Callers to update

- `app.py` — update imports from `pg_queries` to new sub-modules
- `api_routes.py` — `export_records_cursor`, `get_cities_for_state`, `get_stats`
- `admin_routes.py` — `invalidate_filter_cache`
- `tests/test_pg_queries.py` — update all import paths

## Key decisions

- **`pg_queries_hydrate.py` is the one acknowledged integration module.** It calls `get_record_endorsements`, `get_record_entities`, and `get_record_links_bulk` — cross-domain by design. Its docstring documents this explicitly.
- **No re-export shim.** Callers updated to import directly from sub-modules. Avoids hiding the coupling.
- **Constants move to `pg_db.py`** so `pg_queries_search.py`, `_stats.py`, and `_export.py` have no imports from domain modules at all.

## Out of scope

- Changing function signatures or behavior
- Splitting `tests/test_pg_queries.py` into per-module test files (update imports in-place)
- Any changes to `pg_link_records.py` beyond the constants/function migration
