# Couple names: operator-confirmed splits

Some applicant parts name two people in one string (`TREVOR & JAYNE
BLACKWELL`). Each such string is one `person` entity, so neither person was
linked to those records. An operator can now say that a combined name stands
for several people, and which people (#269). Design:
[`specs/2026-11-24-246-couple-name-splitting-design.md`](specs/2026-11-24-246-couple-name-splitting-design.md).
Code: `entity_splits.py` (pure prefill, verdict reads), `split_review.py`
(audited writes, panel, queue), `couple_seed.py` (the #246 seeded pass).

Nothing splits automatically. A combined name stays combined until an
operator saves a verdict.

## Data

| Table / column | What | Kind |
|---|---|---|
| `entity_splits` | one verdict per combined entity: `split` (names its people) or `keep` (not a split; off the queue); `created_by`, `created_at`, `note` | operator data |
| `entity_split_members` | a `split`'s people as cleaned names, in order (`ordinal`); unique per verdict | operator data |
| `record_entities.via_entity_id` | set on a **member row**: the record prints the combined entity, and this row links one of its people. NULL on every printed row | derived |

- Members are names, not ids: extraction resolves them through
  `get_or_create_entity`, so a purged or merged member comes back on the next
  reprocess.
- Deleting the combined entity cascades its verdict and member rows away; a
  verdict also protects it from every automated purge.

## Extraction

`parse_and_link_entities` runs two passes per `(record, role)`:

1. **Printed:** as before. Positions 0…n−1, `via_entity_id` NULL.
2. **Members:** for each printed entity with a `split` verdict, each member
   at the next positions, `via_entity_id` = the combined entity.
   - `ON CONFLICT DO NOTHING`: a member already printed in that role keeps its
     printed row (`RENEE AND TIM OCHS; …; RENEE ELIZABETH OCHS`).
   - A tombstoned member (current manual `not_a_name`) is skipped.

Ingest and `backfill_entities` look up the verdicts per call (one query; under
1 ms a record on a prod copy). `reprocess-entities` loads them once. A full
reprocess rebuilds every member row. Checks comparing links with a fresh
extraction (the WAC repair's) read printed rows only.

No `STEP_VERSIONS["entities"]` bump: with no verdict, output is unchanged,
and every verdict write reprocesses its own records.

## Display

| Where | What |
|---|---|
| record page | chips are printed rows only; a split couple's chip keeps the printed name and links each person inside it (`→ TREVOR EUGENE BLACKWELL · JAYNE MARIE BLACKWELL`), each to its party on that record |
| a member's entity page | lists the records it reaches through the couple, labelled `listed as <couple>` |
| the couple's entity page | banner `WSLCB lists N people under this name:` with links; no redirect |
| `/entities` | hides `split` couples; members are listed and searchable |

`keep` entities display as before.

## Operator actions

**The Couples tab** (`/admin/entities/review?tab=couples`) lists persons
whose name has a standalone `AND` or `&` token (a glued `B&P` is not one),
with no verdict, that are not tombstones. Each row carries the split panel.
The **entity page** carries the same panel for any person or decided entity.
It is open for a couple-shaped or decided name and folded otherwise.

The panel shows:

- the records printing the name, with the printed string;
- the **prefill**, a suggestion only:
  1. shape: `F (AND|&) G L` → `F L`, `G L`, or `F [M] L (AND|&) G [N] K` →
     the two halves; anything else gets none;
  2. with the **cut-off badge** (the raw part is 28–30 characters, WSLCB's
     30-character cut), a last token that starts exactly one longer printed
     token is completed (`SALLY A CLA` → `SALLY A CLARK`);
  3. each part becomes the unique longest middle-compatible person printed in
     the same role (`TREVOR BLACKWELL` → `TREVOR EUGENE BLACKWELL`);
- **picks**: persons printed on those records in the same role, then persons
  on the same licences in the other role, never a couple string;
- for each name, whether it already exists and on which licences.

| Action | Route | Audit action |
|---|---|---|
| Split into… (2–4 names) | `POST /admin/entities/{id}/split` | `entity.split_set` |
| Keep whole | `POST /admin/entities/{id}/split-keep` | `entity.split_keep` |
| Clear verdict | `POST /admin/entities/{id}/split-clear` | `entity.split_clear` |

Each runs in one transaction: validate, replace the verdict, audit (the
combined entity's id and name, the members, the previous verdict, the note),
then `reprocess_entities(record_ids=the printed records)`, which relinks.

**A split is refused for:**

- a non-person;
- an entity in an operator alias link (unlink it first);
- fewer than 2 names;
- a name that cleans to empty, repeats another, equals the combined name, has
  an `AND`/`&` token, or is marked not-a-name.

## Guards

| Path | With a verdict |
|---|---|
| automated purges (`protected_ids`) | the entity is kept, with its Jev rows |
| manual classification | only `person` is accepted; clear the verdict first |
| automated type flip (Jev re-ask, `ignore-jev`) | the verdict stays in force, and a flag opens: `split entity no longer resolves to person` |
| `merge_entity_into` | refused on either side; the case-dupe and WAC merges skip it |
| alias rules (`relink`) | a `split` entity takes no part in the format-key or middle-name rule |
| manual alias link | refused on a `split` entity; link a member instead |

Member rows are ordinary licence links, so members take part in both alias
rules like any other entity.

## The #246 seeded pass

`wslcb db seed-couple-splits [--dry-run] [--admin-email E]` writes the table
the operator approved on 2026-11-24:

- 9 `split` and 3 `keep` verdicts;
- manual `organization`/`trust` rows for 17210 and 45444;

all through the audited paths above. It refuses, writing nothing, if any of
the 14 ids is missing, renamed or already decided. Once applied, a rerun
reports `Already applied`. `--dry-run` rolls back a savepoint. It reports the
records reprocessed, the member links, the new entities, and whether every
printed link is unchanged; a changed printed link rolls the run back.
Attribution is the only admin account, or `--admin-email`; the address is
never printed.

**Back up first** (spec: `data/remediation-backups/<date>-246/`). Run this
from the deploy checkout, with the `PG*` variables set from the production
`.env`, never a URL on the command line:

```bash
D=data/remediation-backups/$(date -u +%F)-246; mkdir -p "$D"
IDS=13439,15477,16331,17210,21867,30292,45444,45609,49087,49945,51098,57311,57314,66317
export PGOPTIONS='-c default_transaction_read_only=on'
psql -X -c "\copy (SELECT * FROM record_entities WHERE record_id IN (SELECT record_id FROM record_entities WHERE entity_id IN ($IDS)) ORDER BY 1, 3, 4) TO '$D/record_entities.csv' CSV HEADER"
psql -X -c "\copy (SELECT * FROM entity_classifications WHERE entity_id IN ($IDS) ORDER BY id) TO '$D/entity_classifications.csv' CSV HEADER"
unset PGOPTIONS
.venv/bin/wslcb db seed-couple-splits --dry-run | tee "$D/dry-run.txt"
```

Rehearsed on a prod copy (2026-11-27): 29 records reprocessed, 21 member
links, 10 new persons, printed links unchanged. A full relink afterwards made
0 changes.
