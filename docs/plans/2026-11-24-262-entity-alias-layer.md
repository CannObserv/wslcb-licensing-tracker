---
title: "#262 entity alias layer: schema, eff(), format-key rule, relink-entities"
date: 2026-11-24
status: draft
---

# #262 entity alias layer

Design: [#247 spec](../specs/2026-11-23-247-entity-name-linking-design.md)
§1 Model, §2 format key, §3 scope, §5 (tombstones in the alias layer), §6.
This plan covers only *how*; the spec's decisions are not reopened.

## Problem

An entity is one cleaned string, so `ZHAO, QIAN` and `QIAN ZHAO`, or BP's 5
spellings, are unlinked parties. The only join today is `merge_entity_into`,
which deletes the variant and its Jev rows, and a reprocess undoes it. #263
(read paths), #264 (admin UI), #265 (middle names) and #269 (couples) all
build on an alias layer that doesn't exist yet.

## Approach

A new module, `entity_aliases.py`, owns the layer. It has four parts.

- **Pure functions:**
  - `link_key(name, type)`, the spec §2 regexes;
  - `elect_canonical`, the spec §1 table plus stickiness;
  - `plan_rule_rows(groups, …) → desired rows`.
- **Write API.**
  - `add_manual_alias` refuses: cycles (naming the closing link), a tombstone target, and self-links. A cross-type link opens a `members disagree on type` flag.
  - `remove_manual_alias`, `dismiss_pair` and `undismiss_pair`.
  - No audit rows here; #264's routes write those.
- **Resolution.**
  - `eff_many(conn, [(entity_id, licence)]) → {…: party_id}`: one query loads the alias rows for the involved ids, then the walk runs in Python (scoped row, else global; manual over rule; at most 4 steps).
  - #263 decides whether to materialise a party map, after measuring the entity page.
- **Relink.**
  - `relink(conn, entity_ids=None)` computes the desired `rule='format_key'` rows for the affected groups, or for all groups when `entity_ids` is None, and diffs them against the stored rows. It deletes what's stale, inserts what's missing and leaves the rest. A second run therefore changes 0 rows, and `created_at` survives.
  - It refreshes `entities.link_key` first, for every entity it touches.
  - Rule-row writes are `ON CONFLICT (variant_id, scope_license, source) DO UPDATE`. No advisory lock: ingest commits every 200 records, so a lock taken per record would be held for a whole batch and stall admin requests. A rare race leaves at most one stale rule row, which the next relink of that group, or a full one, corrects. *(Revised during step 1.)*

Rules are written so #265 can add `rule='middle_names'` as a second planner
without touching the diff or write code.

**Hooks**, all in the caller's transaction:

| Where | What |
|---|---|
| `pipeline._enrich_new_record` | after entity linking, in its own savepoint: `relink(entity_ids=<the record's>)` |
| `resolve_entities` | when a change alters `entity_type`: relink those ids (old and new groups), and flag manual rows that now cross types |
| `record_manual` / `clear_manual` | always relink the entity, so marking it not-a-name drops its rule rows at once; refuse `not_a_name` while it is in a manual alias row |
| `merge_entity_into` | re-point its alias and dismissal rows to the survivor (drop self-loops and duplicates, re-order pairs a<b), then relink the survivor |
| `_purge_entities` | relink the groups of the purged ids (CASCADE has dropped their rows; the group may need a new canonical) |
| `protected_ids` | adds every entity in a manual alias row or a dismissal |

**Tombstone predicate.** The current manual answer is `not_a_name`. A new
`tombstone_ids()` in `entity_classification.py` returns those ids. Today
`reclassify` still purges them; #261 makes them durable and reuses the same
predicate.

**Migration `0011`.**
- Creates `entity_aliases` and `entity_alias_dismissals` exactly as spec §1 gives them: `NULLS NOT DISTINCT`, both CHECKs, `ON DELETE CASCADE`.
- Adds index `(canonical_id)` and index `(variant_id, scope_license)`.
- Adds `entities.link_key text NULL` plus an index on `(entity_type, link_key)`, with `lock_timeout` as in 0010.
- The migration does not backfill. `link_key` depends on the resolved type, so `relink-entities` fills it, and the deploy order is upgrade → dry-run → run.

**CLI.** `wslcb db relink-entities [--dry-run]` prints:
- per rule and type: groups, rows kept, inserted and deleted;
- rows skipped for a cycle or a dismissal;
- link keys updated.

**Calls made while building** (each is pinned by a test):
- **Only a *global* manual variant is barred from being a rule group's canonical.** Spec §1 says "a manual variant is never elected", but read literally that breaks its own Pendergraft worked case: `TY PENDERGRAFT`'s scoped manual row would re-elect `PENDERGRAFT, TY` and cut its chain on 355693.
- **A dismissal in any scope blocks the pair's global format-key row.** A global row would otherwise link the pair on the dismissed licence as well.
- **Cycle checks are one breadth-first search over (entity, licence committed to) states.** A per-licence pass would cost (licences × rows) once #265 adds its scoped rows.

## Tradeoffs / alternatives

- **Materialised `(entity, scope) → party` map now.** Rejected for #262: nothing reads it yet. #263 owns the entity-page latency budget and can add it with a measurement.
- **Delete-and-reinsert all rule rows in relink.** Rejected: it churns ids and `created_at`, and the dry-run couldn't report a real diff. The diff costs the same and makes "a second run changes 0" a property, not a coincidence.
- **Backfill `link_key` in the migration.** Rejected: the migration would have to import the app's key code, which can then drift from the migration. Relink is the single writer.
- **Recursive SQL CTE for `eff`.** Deferred: the Python walk over one batched fetch is simpler to test against the spec's worked cases. #263 can swap in a CTE if the page needs it.

## Steps

1. **Pure layer, TDD.**
   - `link_key` against the spec's cases: `ZHAO, QIAN`, `H.S. WRIGHT III`, a JR comma tail, FRANCHISOR, the person/org `VUE, SUNLIGHT` gate.
   - `elect_canonical`: BP → 27626, 7-ELEVEN → 18623, stickiness, a manual target preferred, a manual variant never elected.
   - `eff` over in-memory rows: the Pendergraft 4-step chain, scoped over global, manual over rule, the 4-step cap.
2. **Migration 0011 + `models.py`.**
   - A PG test of the round trip on `wslcb_test` (never bare alembic: see memory).
   - The constraints reject a self-link, a rule row with `rule` NULL, and a duplicate `(variant, NULL scope, source)`.
3. **Write API + `tombstone_ids` + `protected_ids`.** PG tests:
   - a cycle refused, with the closing link named;
   - a tombstone target refused;
   - a cross-type manual link opens the flag;
   - a dismissal blocks the rule;
   - a global dismissal blocks scoped rows.
4. **`relink` + CLI.** PG tests:
   - groups form across `,`/`.` and FRANCHISOR, and never across types;
   - a second run makes 0 changes;
   - manual rows and dismissals are untouched;
   - a rule row that would cycle with a manual row is skipped;
   - tombstones are excluded.
5. **Hooks.** PG tests, one per row of the hooks table:
   - an ingest record joins its group;
   - a type flip drops the rule row in the same transaction;
   - a not-a-name mark drops the entity's rule rows;
   - a merge re-points its rows;
   - a purge re-elects the canonical.
   - The no-Jev-row-deleted assertion runs across the suite's fixtures.
6. **Docs.**
   - SCHEMA.md: the two tables and the column.
   - MIGRATIONS.md: 0011.
   - AGENTS.md: a Derived table row for `entity_aliases` `source='rule'` rows + `entities.link_key` → `relink-entities`, and that manual rows and dismissals are operator data.
7. **Rehearse on a scratch copy of prod** (read-only `pg_dump` recipe):
   - upgrade, then `relink-entities --dry-run`, then the run, then a second run (0 changes);
   - then a full `reprocess-entities`: every alias row is intact, and the Jev row count is unchanged.
8. **Ship. Pause before it.** The post-ship prod steps, with you:
   - `alembic upgrade head`, a restart, `relink-entities --dry-run` checked against 8,946 rows / 8,918 groups (measured today ±ingest), the real run, then a second run that changes 0.

## Open questions / risks

- **Expected counts.** Re-measured today with the spec's exact rules: 8,914 person + 32 org = 8,946 rows in 8,918 groups (sizes: 8,897×2, 19×3, BP×5, 7-ELEVEN×8). That is 11-23's numbers +1. Canonicals: BP 27626, 7-ELEVEN 18623, `ZHAO, QIAN` → `QIAN ZHAO` (80); `ZHAO QIAN` (81) is untouched.
- **Ingest cost.** The hook adds one indexed lookup and a small diff per record. The relayed scrape ingests at most a few hundred records a day, so it's negligible.
- **Nothing reads the aliases until #263.** After this ships, prod has the rows but the pages don't change. That is intended: the spec orders the plumbing before the read paths.
- **Couple names (#246/#269).** The format key can group `HAROUN, WASSEF & RACHA`-style strings with their natural-order twin, which is the same printed couple. #269's §5a exclusion of split entities is added there, not here.
