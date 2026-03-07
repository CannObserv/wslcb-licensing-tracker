# Design: `/entities` Landing Page

**Issue:** #50  
**Date:** 2026-03-07  
**Status:** Approved

## Goal

Add a `GET /entities` page listing all known applicant entities (persons and organizations) with search, type filter, sort, and pagination. Supports both discovery (find a known person/org) and exploration (browse by activity level). Updates the Dashboard "Unique Entities" stat card to link here.

## Approved Approach: Simple SQL with COLLATE NOCASE + index

A single `GROUP BY` query joining `entities` → `record_entities`, with optional `LIKE` name search, `WHERE entity_type` filter, `ORDER BY` on either `record_count DESC` or `name ASC`, and LIMIT/OFFSET pagination. No FTS, no denormalized count column.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Name search | `LIKE '%term%' COLLATE NOCASE` | 60k clean/uppercase rows; FTS overkill |
| Index | `idx_entities_name ON entities (name COLLATE NOCASE)` | Added via new `schema.py` migration (no data change) |
| Sort options | `count` (default, desc) / `name` (asc) | Supports both exploration and discovery |
| HTMX pattern | Same as `/search`: `HX-Request` header → partial only | Consistent with existing UI |
| Type filter | `<a>` links rebuild full URL | No JS needed; browser-native |
| Per page | 50 rows | Balances scan-ability with load time |
| Back link on `entity.html` | Update to `referrer`-aware fallback to `/entities` | Currently hardcodes `/search` |

## Data Layer

New function `get_entities(conn, *, q, entity_type, sort, page, per_page) → dict` in `queries.py`:

```sql
SELECT e.id, e.name, e.entity_type, COUNT(re.record_id) AS record_count
FROM entities e
LEFT JOIN record_entities re ON re.entity_id = e.id
WHERE  (e.name LIKE :q COLLATE NOCASE OR :q IS NULL)
  AND  (e.entity_type = :type OR :type IS NULL)
GROUP BY e.id
ORDER BY [record_count DESC | e.name ASC]
LIMIT :per_page OFFSET :offset
```

Paired `SELECT COUNT(*) FROM (above without ORDER/LIMIT)` for pagination total. `q` param becomes `%term%` server-side.

## Route

`GET /entities` in `app.py`. Reads `q`, `type`, `sort`, `page` from query params. `HX-Request` → returns `partials/entities_results.html` partial only; full request → renders `entities.html`.

## Templates

- `templates/entities.html` — extends `base.html`; page header, search input, type filter buttons (All / Person / Organization), sort toggle (Most Active / A–Z), `#entities-results` div
- `templates/partials/entities_results.html` — results table (name link → `/entity/{id}`, type badge, record count), row count line, pagination; HTMX-swapped into `#entities-results`
- Search input: `hx-get="/entities"`, `hx-trigger="input changed delay:300ms"`, `hx-target="#entities-results"`, `hx-include` of filter/sort params

## Dashboard Update

`templates/index.html`: wrap the Unique Entities stat card in `<a href="/entities">` (currently a plain `<div>`). Remove `#50` TODO comment. Update corresponding assertion in `tests/test_routes.py`.

## Testing

- `tests/test_queries.py` — `get_entities()`: no filter, name search (`q`), type filter, sort by name, pagination, empty result
- `tests/test_routes.py` — `GET /entities` renders (full + HTMX partial), dashboard card links to `/entities`

## Out of Scope (v1)

- Sort within type subgroup
- Entity merge UI
- Denormalized record count column
- FTS on entity names
