---
title: "#269 couple-name splitting: verdicts, member links, seeded pass"
date: 2026-11-26
status: approved
---

# #269 couple-name splitting

Design: [#246 spec](../specs/2026-11-24-246-couple-name-splitting-design.md)
(approved, merged `9504a3f`). This plan says how it lands on the code as it
stands after #262–#265, and where it departs from the spec's letter.

## Problem

Fourteen person entities name two people in one string (`TREVOR & JAYNE
BLACKWELL`). Neither person is linked to those records, so their entity pages
miss them. The operator chose operator-confirmed splits (spec approach A) and
approved the 14-case table as their decision, to be applied by a seeded pass.

## Re-measured on prod (read-only, 2026-11-26)

- **The 14 are unchanged:** same ids, names and `person`/Jev resolution; no
  manual rows, flags, alias rows or dismissals on any of them. No new
  couple-shaped person since 04-07.
- **BLACKWELL gained record 2814665** (licence 427023, 11-25). Both members are
  printed on it, so it adds no member link: **21 member links still expected.**
- 8 of the 18 member names already exist (TREVOR EUGENE / JAYNE MARIE
  BLACKWELL, both RIVAS, both OCHS, both HAROUN); the other **10 are new**.
- Prod is at `0012`; Jev rows 67,718.
- **#265 already excludes `AND`/`&` names** from the middle-name rule
  (`_COUPLE_TOKENS`), so the spec's *For #265* fix is done.
- **#261 is still open, but its predicate exists:** `tombstone_ids` (#262) is
  "current manual answer `not_a_name`", exactly what the spec asks for. The
  tombstone checks therefore go in, not out.

## Approach

**Schema (migration 0013).** As spec §1:
- `entity_splits` (`combined_id` PK/FK cascade, `verdict` split|keep,
  `created_by`, `created_at`, `note`);
- `entity_split_members` (`combined_id`, `ordinal`, `member_name`; PK
  `(combined_id, ordinal)`, UNIQUE `(combined_id, member_name)`);
- `record_entities.via_entity_id` (nullable FK cascade, `CHECK <> entity_id`,
  partial index where not null).

New module **`entity_splits.py`** holds the pure shape and prefill functions,
the verdict reads, and the audited set / keep / clear writes.

**Extraction (§2).** `parse_and_link_entities` gains `split_map=None`. After
the printed pass it looks up the printed entities' members: `reprocess_entities`
loads the map once and passes it in; ingest and backfill leave it `None`, and
the function runs one indexed query per `(record, role)`. Members go in with
`via_entity_id`, positions after the printed rows and `ON CONFLICT DO NOTHING`.
A tombstoned member is skipped. `reprocess_entities` gains `record_ids`, and
the CLI's `--record-id` becomes repeatable. `_unmatched_records` reads printed
rows only.

**Reads (§4):**
- `get_record_entities` builds chips from printed rows only. A chip whose
  entity has a `split` verdict gains `members`, each `{id, name}`: the id is
  the member's `party_id` on that record, from its printed or member row.
- `get_entity_records` sets `listed_as` on a record the entity reaches only
  through member rows.
- The combined entity's page shows the banner. `/entities` hides `split`
  entities.

**Guards (§5, §5a):**
- `protected_ids` adds every verdict.
- `record_manual` refuses `not_a_name` or a non-person type while a verdict
  exists.
- `_on_type_change` opens the `split entity no longer resolves to person`
  flag.
- `merge_entity_into` refuses a verdict entity on either side;
  `merge_duplicate_entities` and the WAC repair skip one with a warning.
- `relink` leaves `split` entities out of the format-key and middle-name
  planners.
- `add_manual_alias` refuses a `split` end. Saving a split refuses an entity
  in a manual alias row; its rule rows simply drop on the relink.

**Admin (§3).**
- A **Couples** tab in the review queue. Its filter: resolved person, a
  standalone `AND`/`&` token, no verdict, not a tombstone. It shows 14 today.
- A **split panel** on a person entity's admin panel, and in each Couples row.
  It shows the records, licences and roles; the prefill; the picks; whether
  each prefilled name exists, and on which licences; and the truncation badge,
  judged on the raw part.
- **Actions:** Split into… (four name fields, blanks ignored), Keep whole and
  Clear. Each writes the verdict and the audit row, then runs
  `reprocess_entities(record_ids=printed records)`, which relinks, all in one
  transaction.

**Seeded pass.** `wslcb db seed-couple-splits [--dry-run]` writes the approved
table:
- 9 split and 3 keep verdicts, as the spec's table gives them;
- manual `organization`/`trust` rows for 17210 and 45444;
- 14 audit rows;
- then it reprocesses and relinks the records.

It refuses to run if any id has been renamed, deleted or already decided.
`--dry-run` runs everything inside a savepoint and rolls back, printing the
counts. `created_by` is the one admin account; with more than one admin,
`--admin-email` is required. Neither the output nor the logs print the email.

## Tradeoffs / alternatives

- **The seeded pass as a startup data migration**: rejected. It would run on
  restart, before the backup is taken, and it would have no dry run. A CLI
  command gives both.
- **Loading the split map once per ingest batch**: rejected. It means
  threading the map through the pipeline. A per-call query on an empty or
  14-row table costs well under 1 ms. Only a full reprocess (260k calls) needs
  the preloaded map.
- **One-click picks in JavaScript**: rejected. A `<datalist>` on each name
  field gives a one-click choice with no script, matching the vendored-only
  asset rule.
- **Members as chips in the printed list**: rejected (spec anti-goal 3). They
  sit inside the couple chip.

## Steps

1. **Migration 0013 and the model.** Upgrade/downgrade round-trip against
   `wslcb_test` (alembic pinned), and `alembic check` clean.
2. **Pure functions.**
   - `couple_shaped`, `suggest_split` (shape, truncation completion,
     fuller-form preference) and `truncation_suspect`.
   - Tested on the 14 fixtures against the spec's *Prefill* column.
3. **Extraction.** `split_map`, the member pass, `record_ids` and the
   repeatable `--record-id`; `_unmatched_records` reads printed rows.
   - PG test, as the spec says: write a verdict, run a full reprocess, assert
     the printed rows are unchanged and the member rows re-created, then clear
     and assert they are gone.
   - Plus ingest and a printed member that wins.
4. **Guards.** `protected_ids`, `record_manual`, the type-change flag, the
   merge refusal and skips, `relink`'s exclusion, and `add_manual_alias`. Each
   gets a red/green PG test.
5. **Writes and audit.** `set_split`, `keep_whole` and `clear_verdict`, with
   validation (§3 step 1), one transaction and the three audit actions with
   names.
6. **Display.** Couple chips with members, the `listed as` label, the banner
   and the `/entities` filter, with route tests.
7. **Admin UI.** The Couples tab, the split panel and its three POST routes.
   Checked by eye on a dev server (8001) against a scratch copy.
8. **The seeded pass.** The CLI command, refusal tests, and a dry run in a
   savepoint.
9. **Docs.** AGENTS.md's Frozen/Derived note, kept within budget;
   `SCHEMA.md`; `MIGRATIONS.md`; `ENTITY_ALIASES.md` (§5a); `DEPLOYMENT.md`
   (the seed command).
10. **Rehearsal on a scratch copy of prod.** Migrate, seed (dry run, then
    real, then a second run that refuses), and check the done-when table below.
    Then CR, then ship.

## Done when (verified on prod after the ship)

| Check | Expected |
|---|---|
| Verdicts | 9 `split`, 3 `keep` |
| Reclassified | 17210, 45444 → `organization`/`trust` |
| New person entities | 10 |
| Member links | 21, each matching the table |
| Printed links on reprocessed records | unchanged (diff vs backup) |
| `/entities` | the 9 split entities hidden |
| Jev rows | 67,718 + 0 deleted |
| Full-reprocess PG test | passes; `pytest tests/` green |

**Ship order:**
1. Push.
2. `alembic upgrade head` right away, because migrations don't run at startup.
3. Restart.
4. Back up under `data/remediation-backups/2026-11-26-246/`: the 14 entities'
   `record_entities` and `entity_classifications` rows, plus the dry run's
   output.
5. Run the seed: dry run, then real.
6. `VACUUM (ANALYZE) record_entities`.
7. Verify.

## Open questions / risks

- **`created_by` = the sole admin account** (my call). The spec says "the
  approving operator's admin account", and there is exactly one.
- **The 10 new members get Jev answers at the next scheduled
  `classify-entities`** (about 10 calls). Until then they resolve by regex,
  which is `person` for all 10.
- **New member rows feed #265's rule.** For example, `JASON MINAMI` could form
  a middle-name row with another JASON … MINAMI on 089038. That is intended
  (spec §5a). The rehearsal reports any such rows.
- **Merge edge:** a member being merged into an entity that is printed on the
  same record. The printed row wins, and the member row is dropped by the
  existing conflict branch. Tested in step 4.
- **AGENTS.md budget:** one line in Frozen/Derived. Detail goes to
  `SCHEMA.md` / `ENTITY_ALIASES.md`.
