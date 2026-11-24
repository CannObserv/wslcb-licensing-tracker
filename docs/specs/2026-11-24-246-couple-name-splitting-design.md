# Couple names: operator-confirmed splitting — design

2026-11-24 · #246 · builds on #247 ([spec](2026-11-23-247-entity-name-linking-design.md)) and #206 (classification and merge semantics) · implementation waits for #262 and #265 (#247 Q7) · measured on prod, read-only, 2026-11-24, against `origin/main` `8280441`

## Intent

Some applicant parts name two people in one string: `TREVOR & JAYNE BLACKWELL`,
`JASON AND WENDI MINAMI`. Each such string becomes **one** `person` entity.
Neither individual is linked, so a person's entity page misses those records.

This design lets an operator say that a combined name stands for two (or more)
people and which people they are. The decision is persisted against the
combined entity's id. Extraction then links the named people to every record
that prints the combined string. A reprocess re-applies the decision, and the
record page still shows what WSLCB printed.

## What the data looks like (measured)

The test set is the 14 `person` entities whose name contains an `AND` or `&`
token. They have 29 links on 28 records, and 16 Jev rows. Counts are as of
2026-11-24 ≈17:00Z. Record 2804134, a ninth BLACKWELL record, was ingested
that day, after the issue's 28-link count. All 14 resolve through Jev, since
Jev's `person` criteria explicitly include couples (`jev_client.KIND_CRITERIA`).

| id | Name | Role · licence(s) | Records | Same-record people (same role) |
|---|---|---|---|---|
| 13439 | TREVOR & JAYNE BLACKWELL | applicant · 6 licences | 9 | TREVOR EUGENE BLACKWELL (9/9); JAYNE MARIE BLACKWELL (4/9) |
| 15477 | JASON AND WENDI MINAMI | applicant · 089038 | 2 | none |
| 16331 | CHRISTINA & RAYMUND AND RIVAS | applicant · 436348 | 2 | CHRISTINA RIVAS; RAYMUNDO PALOMAREZ RIVAS; RAMUNDO RIVAS JR |
| 17210 | HEATHER TWIST PHILLI THE ROBERT PHILLIPS AND | applicant · 437973 | 1 | HEATHER T PHILLIPS; ROBERT B PHILLIPS |
| 21867 | GARTH GEHLEN AND NATHAN DOIDGE | previous_applicant · 414115 | 2 | none |
| 30292 | JAMES AND WENDY HULL | previous_applicant · 412869 | 1 | none |
| 45444 | KEVIN AND ANNA LANGE CHILDREN | applicant · 441649 | 2 | none (`JPD 2019 GIFT TRUST` co-listed) |
| 45609 | RENEE AND TIM OCHS | applicant · 077675 | 2 | RENEE OCHS; RENEE ELIZABETH OCHS; TIMOTHY FLOYD OCHS |
| 49087 | LOPEZ & LOPEZ | applicant · 368806 | 2 | JOSE RAMON LOPEZ; STEPHANY VERONICA LOPEZ; ARMANDO G LOPEZ CARMONA |
| 49945 | DONALD P CLARK AND SALLY A CLA | applicant · 429234 | 2 | none (`SALLY A CLARK, TTEE OF THE CLA` is an org) |
| 51098 | WASSEF & RACHA HAROUN | applicant · 424811 | 1 | WASSEF HAROUN; RACHA BARAZI HAROUN |
| 57311 | COLIN & MARTHA MOSELEY | applicant · 423367 | 1 | none |
| 57314 | MERRITT KEVIN & PATR AULD | applicant · 423367 | 1 | none |
| 66317 | FRANCISCO & VILLA VILLA | applicant · 072487 | 1 | none (`FRANCISCO VILLA` 9454 exists on 077940/445627 only) |

**Other facts the design rests on:**

- **No new couple strings are arriving.** All 14 entities were created
  between 2026-02-22 and 04-07. None of the 4,628 entities created since is
  couple-shaped.
- **The obvious shapes also match organizations.** `F & F L` matches 8
  organizations (`TANK AND BEAR LLC`, `GLEASON & KOATZ LLP`) and
  `F [M] L & F [M] L` matches 7 more. The regex alone already types all 15 as
  organizations.
- **No couple of individuals among the organizations.** None of the 89
  organizations with `AND`/`&` is two individuals. Seven are trusts named after
  a couple, such as `ANDREW AND SUSAN HERMRECK FAMILY TRUST`.
- **No other separator marks a couple:** not `/`, `+`, `OR` or `ET UX`.
- **The #247 middle-name rule reaches one split part, not the Hulls.** The
  Hull and Gehlen couples are `previous_applicant`s, while `JAMES RICHARD HULL`
  and `GARTH PATRICK GEHLEN` are `applicant`s on the same licence, so the role
  gate blocks them. `WENDY KIM HULL` is on another licence. Only
  `RACHA HAROUN` → `RACHA BARAZI HAROUN` forms. (These corrections are
  [posted on #246](https://github.com/CannObserv/wslcb-licensing-tracker/issues/246#issuecomment-7134146412).)

## Goals

- **Validate:** an operator decides, case by case, whether a combined name is
  several people, and which people.
- **Durable:** the decision survives `reprocess-entities`, `reclassify-entities`
  and new ingests. That holds because the decision is persisted and nothing
  automated deletes it.
- **Findable:** each named person's entity page lists every record that prints
  the combined string.
- **Faithful:** the record page keeps WSLCB's printed list as it is.

## Non-goals

- Splitting automatically. The operator chose operator confirmation
  (2026-11-24). See *Approaches*.
- Changing `applicants`, `previous_applicants`, any `raw_*` column, or
  `extract_entity_names`'s string-level behaviour.
- Asking Jev a new question, or any change to classification sources.
- Identity across records beyond what the operator names. Linking a split
  person to other spellings is #247's alias layer (#262, #265, #264).

## Anti-goals

The design has failed, even if it works, if:

1. **A name is split without an operator choosing to.**
2. **An operator decision is silently undone,** by a reprocess, reclassify,
   purge or merge.
3. **The record page stops showing what WSLCB printed.**
4. **Jev rows are deleted.** The combined entity is kept, with its rows.
5. **An organization is split.** The queue lists persons only, and the split
   action is refused on an organization.

## Constraints

- `record_entities` is derived and rebuilt from `entity_source(stored, raw)`
  (#266). A split must therefore come from the raw string plus a persisted
  decision that extraction consults.
- Never bulk-delete Jev rows. Manual rows and open flags survive (#206).
- `record_entities` has primary key `(record_id, entity_id, role)`: one entity
  appears at most once per record and role.
- `enrichment.STEP_VERSIONS` is the single source for the `entities` version
  (#267). See §6.

## Approaches

### Who decides (Q1)

| Approach | Trade-offs | Outcome |
|---|---|---|
| **A. Operator confirms every split.** The review queue lists couple-shaped person names, each with a prefilled suggestion. Nothing splits until the operator saves. | No false splits. Fits the volume: 14 names, and none new since April. A new couple stays combined until it is reviewed. | **Chosen** by the operator, 2026-11-24 |
| B. A strict shape auto-splits; the operator handles the rest. `F & F L` splits automatically when the regex says person (6 of 14). | New couples split with no one acting. It needs a re-split whenever a type changes (regex at ingest, Jev later), a "don't split" table, and shape tuning (`FRANCISCO & VILLA VILLA` matches). That is a lot of machinery for a shape with no arrivals in 7 months. | Rejected |
| C. Read-time membership only. The combined name stays linked; read paths union in its members. | `record_entities` is untouched. But #265's rule reads who is on a licence and would never see the members, and every read path needs the union. | Rejected |
| D. A Jev sub-question ("couple? members?"). | It costs money, and Jev already calls couples persons. An operator still has to check the members. | Rejected |

### What a split produces (Q2)

| Option | Trade-offs | Outcome |
|---|---|---|
| **Printed link kept, member links added and marked.** The combined entity keeps its link. Each member gets a link marked `via_entity_id = combined`. | The printed list is unchanged. Member pages list the record. Members are real licence links, so #265 and the alias layer see them. Reads separate printed rows from member rows by `via_entity_id`. | **Chosen** *(my call)* |
| Replace. Members are linked, and the combined entity is unlinked but kept. | Nothing is double-counted. But when every member is already printed on the record (HAROUN, OCHS, RIVAS), every member insert hits the primary key, nothing is left to mark where the couple was, and the printed string vanishes from the record page (anti-goal 3). | Rejected |
| Members only through a relation, with no record links. | Same as C above. | Rejected |

## Design

### 1. Model (Q4: persistence)

```
entity_splits                         -- one operator verdict per combined entity
  combined_id   PK, FK entities.id ON DELETE CASCADE
  verdict       text CHECK (verdict IN ('split', 'keep'))
  created_by, created_at, note

entity_split_members                  -- the people a 'split' verdict names
  combined_id   FK entity_splits.combined_id ON DELETE CASCADE
  ordinal       smallint              -- display order
  member_name   text NOT NULL         -- stored cleaned (clean_entity_name)
  PRIMARY KEY (combined_id, ordinal)
  UNIQUE (combined_id, member_name)

record_entities
  + via_entity_id  integer NULL, FK entities.id ON DELETE CASCADE
                   CHECK (via_entity_id IS NULL OR via_entity_id <> entity_id)
                   indexed
```

- **Keyed by the combined entity's id,** as #247 asked. The combined row is
  never deleted while a verdict exists (§5), so its Jev rows stay too.
- **Members are stored as names, not ids.** Extraction emits them through
  `get_or_create_entity`, as #247 recommended. A member entity that is purged
  or merged away comes back on the next reprocess. The operator can pick an
  existing entity in the UI; the form then copies its name. *(My call: an id
  FK would cascade away the decision when a member entity is deleted.)*
- **`keep`** records "this is not a split; take it off the queue". It has no
  effect on extraction.
- **A `split` names at least 2 members.** A one-member "split" is an alias, and
  aliases belong to #247 (`Same as…`).
- **Operator data, not derived.** `entity_splits` and `entity_split_members`
  are kept like manual classification rows. Nothing automated writes or deletes
  them.
  - Member rows (`via_entity_id` set) are derived: `reprocess-entities`
    rebuilds them.
  - Add both facts to AGENTS.md's Frozen/Derived section and to SCHEMA.md.

### 2. Extraction

`extract_entity_names` stays a pure string function. `parse_and_link_entities`
gains a split map, `dict[combined_id, tuple[member_name, ...]]`. It is loaded
once per reprocess and once per ingest batch, from verdict `split` rows. For
one `(record, role)`:

1. **Printed pass,** unchanged. Each extracted name is linked at positions
   0…n−1 with `via_entity_id` NULL.
2. **Member pass.** For each printed entity in the split map, in printed
   order, `get_or_create_entity` is called for each member. The member is
   inserted with `via_entity_id = combined_id` and positions n, n+1, …
   - `ON CONFLICT DO NOTHING`. A member that is already printed on the record
     in that role keeps its printed row, and gets no member row.
   - Running the printed pass first makes the printed row win. Without that,
     `RENEE AND TIM OCHS; …; RENEE OCHS` would turn the printed `RENEE OCHS`
     into a member row.
   - A member that is a tombstone (#261: current manual answer `not_a_name`)
     is skipped, as printed tombstones are.

Every caller goes through this function: ingest (`pipeline`), `reprocess_entities`
and `backfill_entities`. So all three agree.

**Checks that compare links with a fresh extraction ignore member rows**
(`via_entity_id IS NOT NULL`). That covers `entity_wac_repair._unmatched_records`
and any #266-style link check; positions are checked over printed rows only.
Without the filter, every split record would be reported as drift.

### 3. Validation: the review queue and the split panel (Q1, Q3)

**The queue.** `/admin/entities/review` gains a **Couples** tab. It lists
entities that:

- resolve to `person`;
- have a name containing a standalone `AND` or `&` token;
- have no `entity_splits` row;
- are not tombstones.

That is 14 today. Organizations never appear (anti-goal 5). Reclassifying an
entry as a trust through the existing classification panel also takes it off
the queue, because it is then an organization.

**The split panel** sits on the entity page's admin panel, for any
person-typed entity, and in each queue row. It shows:

- the records and licences the name is printed on, and the role;
- the people printed on those records in the same role (one-click picks), and
  the people on the same licences in the other role (picks, labelled with the
  role);
- for each prefilled member: whether an entity of that name already exists,
  and on which licences. For `FRANCISCO VILLA` it shows 9454, on 077940 and
  445627, not this licence;
- a **truncation-suspect** badge when the raw part is at least 28 characters
  (`_TRUNCATION_MIN_LEN`). Truncation is judged on the raw part, before
  `strip_wac_annotation`: `GARTH GEHLEN AND NATHAN DOIDGE` is 30 characters
  cleaned but 45 raw, so it is not suspect.

Its actions are:

- **Split into…:** two or more editable name fields, prefilled, with a note;
- **Keep whole:** with a note;
- **Clear:** deletes the verdict.

**The prefill** is a suggestion only. The operator edits it before saving.

1. **Shape.** Strip nothing; tokenize the cleaned name.
   - `F (AND|&) F′ L` → `F L`, `F′ L`: the shared surname is inherited.
   - `F [M] L (AND|&) F′ [M′] L′` → the two halves as printed.
   - Anything else gets no prefill: one or more than two conjunctions, a
     leading or trailing conjunction, or two bare tokens.
2. **Truncation completion.** If the badge applies and the last token of the
   last part is a strict prefix of exactly one token printed on the same
   record, complete it. `SALLY A CLA` → `SALLY A CLARK`, from
   `SALLY A CLARK, TTEE OF THE CLA`.
3. **Prefer a printed fuller form.** For each part, find the persons printed
   in the same role on any record of the combined entity whose name is
   middle-name-compatible with the part. That means the same first and last
   tokens and suffix, and middles compatible as in #247 §3. If exactly one of
   them is the longest, prefill it. `TREVOR BLACKWELL` →
   `TREVOR EUGENE BLACKWELL`. Nicknames are not matched: `TIM` stays `TIM`.

**Why step 3:** picking a fuller form names a person who already exists, so
the split creates no new entity. A plain name would need an alias (#262,
#265) to reach that person, and for previous-role parts no rule ever forms
one. The operator owns that identity call, so the prefill only proposes it.
*(My call.)*

**Writes, in one transaction:**

1. Validate. Refuse:
   - a non-person combined entity;
   - a member name that cleans to empty, equals the combined name, contains an
     `AND`/`&` token, or names a tombstone;
   - fewer than 2 members.
2. Upsert `entity_splits` and replace `entity_split_members`.
3. Write the audit row (§7).
4. Reprocess the combined entity's printed records:
   `reprocess_entities(record_ids=…)`, where the set is
   `record_entities.entity_id = combined AND via_entity_id IS NULL`.
   `reprocess_entities` gains a record-id-list parameter. This stamps those
   records at the current `entities` version. **Clear** reprocesses the same
   set.

### 4. Display (Q5)

**Record page.** The chips are the printed rows (`via_entity_id` NULL), in
position order, as today. A chip whose entity has a `split` verdict adds its
members as links. Each member resolves through #263's `eff(e, lic)` when that
exists.

```
Applicants
  TREVOR & JAYNE BLACKWELL  → Trevor Eugene Blackwell · Jayne Marie Blackwell
  JEFFREY EUGENE BLACKWELL
  KAREN MARIE BLACKWELL
  TREVOR EUGENE BLACKWELL
```

The members come from `entity_split_members`, not from member rows. A member
that is also printed on the record (`TREVOR EUGENE BLACKWELL` above) has no
member row, but it still appears inside the couple chip.

**A member's entity page** lists the records through member rows too. In the
records list, a record reached through a member row is labelled
`listed as TREVOR & JAYNE BLACKWELL`.

**The combined entity's page** stays at its URL, with its records and
classification. A banner reads `WSLCB lists 2 people under this name:` and
links each member. It does not redirect, because there are two targets.

**`/entities`** hides entities with a `split` verdict: they are not a person.
A search for a member's name finds the member itself. `record_count` for a
member counts its member rows, which is correct: it is on those records.

**Kept-whole entities** (`keep`) display exactly as today.

### 5. Durability

- **`protected_ids`** (`entity_classification.py`) also returns every
  `entity_splits.combined_id`. No automated purge removes a decided combined
  entity, and with it its Jev rows. That covers `reclassify_entities`'
  `is_noise_name` purge and #261's tombstone path.
- **Marking a combined entity not-a-name is refused** while it has a verdict;
  the panel says to clear the verdict first. This mirrors #247 §5 for manual
  alias rows.
- **`merge_entity_into`** (#206) refuses a combined entity with a verdict as
  either side. A member entity may still be merged. A #206 merge only joins
  strings that clean identically, so the stored `member_name` still resolves
  to the surviving entity on the next reprocess.
- **A reprocess** reloads the split map, so it re-creates every member row.
- **A full reprocess deletes and rebuilds** `record_entities` per
  `(record, role)`, member rows included.
- **A later type change doesn't undo a split.** *(My call, from the builder
  check.)*
  - The classification panel refuses a non-person type while a verdict
    exists, and says to clear the verdict first.
  - An automated flip (`resolve_entities` after a Jev re-ask, or
    `ignore-jev`) leaves the verdict in force. It opens an
    `entity_review_flags` row on the combined entity with reason
    `split entity no longer resolves to person`.
  - Extraction applies the verdict either way. Undoing an operator decision
    automatically would break anti-goal 2.

### 5a. Aliases (#262–#265)

*(My calls, from the builder check.)*

- **A combined entity with a `split` verdict is not a party.**
  - No alias rule takes it as a variant or a canonical.
  - The format-key and middle-name rules skip it, and a manual "Same as…" on
    it is refused.
  - Saving a split on an entity that is already an alias variant or target is
    refused until it is unlinked. This is #247 §5's pattern for not-a-name.
  - A couple chip therefore never resolves the combined entity through
    `eff()`; only its members do.
- **Saving or clearing a split relinks.** It runs #262's per-record relink
  (format key, plus #265's middle-name rule) for the reprocessed records, in
  the same transaction. New members such as `GARTH GEHLEN` then join their
  licence groups at once, with no wait for a manual `relink-entities`.

### 6. Version stamp and ordering

**No `STEP_VERSIONS["entities"]` bump is required.** *(My call.)*

- With no verdict rows, extraction produces exactly what it does today for
  every record.
- Output changes only for the records of an entity with a verdict. Every
  verdict write reprocesses and stamps those records itself: the operator's
  save (§3) and the seeded pass alike.

So #246 needs no ordering against the full `reprocess-entities` that waits on
#261: either can run first. If the implementation changes output without a
verdict (positions, for example), it bumps to the next free number (7, unless
#261, #262 or #265 took it).

### 7. Audit

There are three new `admin_audit_log` actions:

- `entity.split_set`
- `entity.split_keep`
- `entity.split_clear`

`details` carries the combined entity's id **and name**, the member names in
order, the previous verdict and members (for a change or a clear), and the
note. That follows #247's rule: an audit row must name what it decided.

### 8. Classification and Jev

- **The combined entity keeps every classification row.** Nothing is
  re-asked.
- **New member entities** get their regex row at creation, and are picked up
  by the next `classify-entities` pass like any new entity. Expected cost: 10
  entities (*Test set*).
- **`classify-entities` may skip entities with a `split` verdict** if they
  ever lose their Jev answer. Today all 14 already have one, so nothing is
  needed.

## Test set: expected outcomes (the 14 cases)

**Decision (operator, 2026-11-24): approving this table is the operator's
decision on these 14 entities.** The implementation applies it in one seeded
pass (*The seeded pass*, below). It does not wait for each to be entered through the queue. This
meets anti-goal 1: an operator chose every split.

**Columns:**

- *Prefill*: what §3 proposes.
- *Expected*: what the operator saves.
- *New links*: member rows created. A member already printed on a record
  creates none there.

| id | Prefill | Expected verdict and members | New entities | New links |
|---|---|---|---|---|
| 13439 TREVOR & JAYNE BLACKWELL | TREVOR EUGENE BLACKWELL, JAYNE MARIE BLACKWELL (step 3 on both) | **split**, as prefilled | 0 | 5: JAYNE MARIE on 14176, 14198, 14200, 14206, 2804134 (licences 089058, 400889, 402383, 406920) |
| 15477 JASON AND WENDI MINAMI | JASON MINAMI, WENDI MINAMI | **split**, as prefilled | 2 | 4 (2 records × 2) |
| 16331 CHRISTINA & RAYMUND AND RIVAS | none (two conjunctions) | **split**: CHRISTINA RIVAS, RAYMUNDO PALOMAREZ RIVAS (operator picks; `RAYMUND` is the cut-off form) | 0 | 0: both are printed on both records; the chip links them |
| 17210 HEATHER TWIST PHILLI THE ROBERT PHILLIPS AND | none (trailing conjunction) | **no verdict**: reclassify as organization/trust (trust wording; truncated) | 0 | 0 |
| 21867 GARTH GEHLEN AND NATHAN DOIDGE | GARTH GEHLEN, NATHAN DOIDGE | **split**, as prefilled. Linking `GARTH GEHLEN` to `GARTH PATRICK GEHLEN` across the hand-over stays an optional #264 link | 2 | 4 (previous_applicant) |
| 30292 JAMES AND WENDY HULL | JAMES HULL, WENDY HULL | **split**, as prefilled. `JAMES RICHARD HULL` (same licence, applicant) is offered as a pick; choosing it is the operator's identity call | 2 | 2 (previous_applicant) |
| 45444 KEVIN AND ANNA LANGE CHILDREN | none; truncation badge (29 chars) | **no verdict**: reclassify as organization/trust (beneficiary wording, `JPD 2019 GIFT TRUST` co-listed) | 0 | 0 |
| 45609 RENEE AND TIM OCHS | RENEE ELIZABETH OCHS (step 3), TIM OCHS | **split**: RENEE ELIZABETH OCHS, TIMOTHY FLOYD OCHS (operator resolves the nickname from the record) | 0 | 0: both are printed on both records |
| 49087 LOPEZ & LOPEZ | none (two bare tokens) | **keep**: three LOPEZes are printed, and nothing says which two | 0 | 0 |
| 49945 DONALD P CLARK AND SALLY A CLA | DONALD P CLARK, SALLY A CLARK (step 2 completion); truncation badge | **split**, as prefilled. Sally is also listed as trustee of `THE CLA…`, so the trust has its own name and this string reads as the individuals | 2 | 4 |
| 51098 WASSEF & RACHA HAROUN | WASSEF HAROUN, RACHA BARAZI HAROUN (step 3) | **split**, as prefilled | 0 | 0: both are printed |
| 57311 COLIN & MARTHA MOSELEY | COLIN MOSELEY, MARTHA MOSELEY | **split**, as prefilled | 2 | 2 |
| 57314 MERRITT KEVIN & PATR AULD | MERRITT KEVIN, PATR AULD (shape 2) | **keep**: `PATR` is cut off and the token order is unclear; no AULD is printed elsewhere | 0 | 0 |
| 66317 FRANCISCO & VILLA VILLA | FRANCISCO VILLA, VILLA VILLA; the panel shows `FRANCISCO VILLA` (9454) exists only on other licences | **keep**: `VILLA VILLA` is not a credible given name, and the existing match is a string collision | 0 | 0 |

**Totals:**

| Outcome | Count |
|---|---|
| Split | 9 |
| Keep | 3 |
| Reclassified as trust | 2 |
| New member entities | 10 |
| New links | 21 |

The three fully covered couples (RIVAS, OCHS, HAROUN) add no links. Their
records gain only the member links inside the couple chip. This is the
"suppress the combined string" outcome #247 suggested, reached without a
separate suppression mode.

### The seeded pass

The pass is a one-off, idempotent data migration or CLI command, with a
`--dry-run`. It runs after the schema migration, on the deploy:

1. **Verdicts.** Write the 9 `split` and 3 `keep` rows exactly as the table
   gives them. `created_by` is the approving operator's admin account. The
   note cites #246 and this spec.
2. **Trusts.** Write manual `organization`/`trust` classification rows for
   17210 and 45444, through the existing manual-classification path, with the
   same attribution and note.
3. **Audit.** Write one audit row per entity (§7).
4. **Links.** Reprocess the 14 entities' printed records (28 at the snapshot; the pass recomputes the set when it runs), then
   relink them (§5a), in the same transaction.
5. **Backup.** Back up the pass first under
   `data/remediation-backups/<date>-246/`: the touched `record_entities` rows
   and the 14 entities' `entity_classifications` rows.

The pass refuses to run if any of the 14 ids has been renamed, deleted or
already given a verdict. The table was approved against these names.

**Done means verified on prod** after the pass:

| Check | Expected |
|---|---|
| Verdicts | 9 `split`, 3 `keep` |
| Reclassified | 17210 and 45444 resolve `organization`/`trust` |
| New entities | 10, all persons |
| New member links | 21 at the snapshot, plus one per member not printed on any couple record ingested since; each matches the table's members |
| Printed links | unchanged on every reprocessed record |
| `/entities` | the 9 split entities are hidden |

The 10 new entities are classified by the next scheduled `classify-entities`
pass. That Jev spend is about 10 calls.

**Test fixtures.** Use the 14 names and their printed applicant strings
(above) as the unit fixture for the shape parser, the truncation badge, the
completion and the step-3 preference. Add a PG test:

1. Write a verdict.
2. Run a full `reprocess_entities`.
3. Assert that the printed rows are unchanged and the member rows are
   re-created.
4. Clear the verdict; assert the member rows are gone.

## Left to the builder

- **Where the split map is loaded at ingest:** once per scrape batch, or a
  cheap per-record query. There are 14 candidates at most.
- **The queue tab's layout,** and whether its prefill is computed when the
  page loads.
- **The exact token regex for `AND`/`&`.** `&` may be glued (`B&P`); only a
  standalone token counts, and organizations never reach the queue anyway.
- **The banner and label wording.**
- **The `reprocess_entities` record-list parameter:** its signature, and
  whether `--record-id` on the CLI becomes repeatable.

## Follow-up

The implementation issue (to be filed when this design ships) covers:

- the schema migration;
- extraction;
- the panel and the queue tab;
- the display;
- the audit actions;
- the guards (`protected_ids`, merge, not-a-name, type change, alias);
- the docs: AGENTS.md's Derived table and SCHEMA.md.

**Depends on #262 and #265** (#247 Q7). #263 lands before #265, so the plan is
written against the real alias, `eff()` and chip interfaces after they merge,
with no stubs. *(My call, from the builder check.)*

- **#261.** The tombstone checks (queue filter, validation, member pass) use
  #261's predicate. #261 runs in parallel with #262 and is expected first. If
  it has not merged, those checks are left out: until then, non-names are
  purged, not kept, so there is nothing to skip.
- **#265's conjunction fix** (*For #265* below). If #265 ships without it,
  this implementation adds it. Without the fix, the wrong HAROUN and RIVAS
  canonicals survive the split.

## For later issues (not #246)

### For #265:

- **Couple strings break the middle-name rule as written.** `&` and `AND`
  survive `link_key`, so they parse as middle tokens. #247 §3 says a form with
  no middles is compatible with any middles. Applied to the 14 (traced against
  the definition, not run):
  - **Two wrong canonicals.** `WASSEF HAROUN` → `WASSEF & RACHA HAROUN` on
    424811, and `CHRISTINA RIVAS` → `CHRISTINA & RAYMUND AND RIVAS` on 436348.
    The couple string is the longest form, so it is elected.
  - **One lost link.** On 077675 the group (RENEE, OCHS) holds `RENEE OCHS`,
    `RENEE ELIZABETH OCHS` and `RENEE AND TIM OCHS`. `[ELIZABETH]` versus
    `[AND, TIM]` conflicts, so the whole group writes nothing, and
    `RENEE OCHS` → `RENEE ELIZABETH OCHS` is never made.
  - **Six spurious conflict groups.** (TREVOR, BLACKWELL) on each of the 6
    Blackwell licences.

  **Fix:** a person name containing a standalone `AND` or `&` token takes no
  part in the rule, as variant or as canonical. Once #246 ships, its member
  rows take part like any other licence link. The 4,908 rows / 404 conflicts
  in #265's body include these cases.
- **The body's worked example.** `JAMES HULL` → `JAMES RICHARD HULL` does not
  form: the role gate and the licence scope block it. `RACHA HAROUN` →
  `RACHA BARAZI HAROUN` on 424811 is a working one.

## Appendix: how the numbers were measured

- **Read-only queries** on prod, with `default_transaction_read_only=on`,
  2026-11-24, against `origin/main` `8280441`. Nothing under `src/` or
  `alembic/` has changed since `a4cc807`.
- **The candidate set.** `entities` where `name ~ '\mAND\M' OR name LIKE '%&%'`,
  split by resolved type: 14 persons, 89 organizations.
- **Records, roles and licences.** `record_entities` joined to
  `license_records`, reading `coalesce(raw_*, *)` for the printed string.
- **Existing parts:** an exact `entities.name` match, and a first…last pattern
  match among persons, with licences and roles for each hit.
- **Shapes:** the regexes in §3, run over every entity. The regex type of the
  matching organizations comes from their `entity_classifications` `regex`
  row.
- **Arrival rate:** `entities.created_at` of the 14, and the count of entities
  created after 2026-04-07.
