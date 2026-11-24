---
title: "#263 entity alias read paths: entity page union + redirect, /entities, record chips"
date: 2026-11-24
status: draft
---

# #263 entity alias read paths

Design: [#247 spec](../specs/2026-11-23-247-entity-name-linking-design.md)
§4 "The public pages". The layer it reads: [ENTITY_ALIASES.md](../ENTITY_ALIASES.md)
(#262, live since 2026-11-24). The spec left one choice to the builder: how
the union query is built, a CTE over `entity_aliases` or a materialised map.
The decision is made here, from measurements.

## Problem

Prod has 8,946 alias rows, but every page still shows one entity per spelling:
- BP's 5 spellings are 5 pages;
- `/entities` lists every variant;
- a record that lists `RANDAL ANDERSON; RANDAL JUAN ANDERSON` shows two chips.

## Approach

**The per-link party is stored.** A new derived column,
`record_entities.party_id`, holds `eff(entity_id, record's licence)`, with an
index on `(party_id, record_id)`. Every read path becomes an indexed lookup on
it:

| Page | Query |
|---|---|
| `/entity/{C}` | records with `party_id = C`, plus C's own links. A link of C's whose party is another entity carries a note: "On licence N: <party> →" (the scoped-variant case, e.g. `TY`). |
| `/entity/{V}` when V has a global alias row | 302 to `/entity/{eff(V)}?from={V}`. The canonical's page then shows "Listed as: V". |
| "Also listed as" on C's page | the entities that reach C: "everywhere" if `eff(m, ∅) = C`, else the licences where m's links resolve to C |
| `/entities` | hides entities with a global alias row. The count is `COUNT(DISTINCT record_id)` grouped by `party_id`. `q` also matches the members' names. |
| record chips (`get_record_entities`) | group a role's links by `party_id`. One chip per party, linked to the party, showing every printed spelling. |

**How `party_id` is kept current:**
- **Insert:** a `BEFORE INSERT OR UPDATE OF entity_id` trigger defaults it to `entity_id`. That makes it NOT NULL with no change to the insert sites or the test helpers. A merge's re-pointed links follow the new `entity_id`.
- **Alias changes:** `relink` (#262) refreshes it in the same transaction, for the links of every entity whose `eff` can have changed, i.e. the reverse closure of the entities it looked at. A full `relink-entities` refreshes all links, compares the computed and stored values, and writes only the differences.
- Every alias write, ingest, reprocess, merge and purge already ends in a `relink`, so no new hook is needed. `RelinkResult` gains `parties_updated`.
- `record_entities` is already in AGENTS.md's Derived table, rebuilt by `reprocess-entities`, which now relinks. **AGENTS.md doesn't change**; it is at 5,996 of its 6,000 budget.

**Migration `0012`:**
- add `record_entities.party_id`, the trigger and the index;
- backfill `party_id = entity_id`.
- With `lock_timeout`, as in 0010 and 0011.
- Deploy: `upgrade` right after the push, then restart, then `relink-entities` (which fills the real parties).

## Measured (scratch copy of prod, 2026-11-24, 8,946 alias rows)

| `/entities` query (sort by count, first page) | ms |
|---|---|
| today: `COUNT(re.record_id)` per entity | 268 |
| party resolved at read time (map table + join to `license_records`) | 519 |
| **stored `party_id` + `(party_id, record_id)` index** | **95** (162 when sorted by name) |

Today on prod: `/entities` 355 ms; `/entity/18623` (7-ELEVEN) 18 ms;
`/entity/27626` (BP) 32 ms; `/record/{id}` 17 ms.

## Tradeoffs / alternatives

- **Resolve the party at read time** (a CTE or map table joined through the record's licence). Rejected: `/entities` would roughly double, to 519 ms, even with the map table indexed.
- **A `(entity, scope) → party` map table instead of a per-link column.** Rejected: it still needs the join to `license_records` and the distinct count, which is the expensive part. The per-link column makes that count presorted.
- **Compute `party_id` in `parse_and_link_entities`.** Rejected: it would add an `eff` lookup per link at ingest and reprocess, for a value the `relink` that follows in the same transaction refreshes anyway.
- **A precomputed per-party counts table.** Rejected: counts change on every ingest, so it would need a third maintenance path.

## Steps

1. **Migration 0012, the model and schema tests.**
   - The trigger defaults `party_id` on insert and follows an `entity_id` update.
   - The index exists.
   - Round trip on `wslcb_test`.
2. **`refresh_parties` in `entity_aliases`, called from `relink`.** PG tests:
   - the format-key group, the scoped Pendergraft chain, a type flip, a merge and a purge each leave every link's `party_id = eff(entity, licence)`;
   - a second full relink updates 0.
   - The CLI prints `parties_updated`.
3. **Entity page.**
   - `get_entity_page(conn, id)` returns the records, members, notes and the redirect target.
   - Route: the 302 with `?from`; the union; the distinct licence count; "Also listed as"; "Listed as"; the scoped-variant note. Template changes in `entity.html` / `record_table.html`.
   - Route tests on the BP, 7-ELEVEN and Pendergraft fixtures.
4. **`/entities`.** Party counts, variants hidden, member-name search. Route tests for the counts, the hiding and the search.
5. **Record chips.** `get_record_entities` dedupes by party; the `detail.html` macro shows every spelling and links to the party. Route test: one chip for two spellings on one record.
6. **Docs.**
   - ENTITY_ALIASES.md: a "Read paths" section and `party_id`.
   - A short `party_id` line in SCHEMA.md's `record_entities` section (budget: 9,970 of 10,000).
   - MIGRATIONS.md: 0012.
   - README: the test table if a test file is added.
7. **Rehearse on `wslcb_scratch_263`.**
   - Upgrade, `relink-entities`, then a second run (0 changes).
   - A dev server on :8001 against scratch: time `/entity/18623`, `/entity/27626`, `/entities` and `/entities?sort=name` against the prod numbers above, and look at BP, 7-ELEVEN and a double-listed record.
8. **Ship.** Pause before. Then `upgrade` right after the push, restart, `relink-entities` twice, and check the pages on prod.

## Open questions / risks

- **Admins are redirected too.** #264's Links panel needs a variant's own page. It can add `?stay=1` (or skip the redirect when signed in) when it builds the panel.
- **The "Records" column changes meaning.** It now counts distinct records, not link rows. An entity listed in both roles on one record used to count twice; now it counts once.
- **Targeted refresh cost at ingest.** It adds the reverse closure (at most 4 small queries) and a fetch of the affected entities' links. I'll time it in step 7 against #262's 7.1 ms per record.
- **The Pendergraft worked case needs scoped rows.** Prod has none until #264 or #265, so the case is covered by tests, not on prod.
