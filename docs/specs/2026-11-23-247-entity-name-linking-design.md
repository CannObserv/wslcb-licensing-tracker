# Entity name linking: aliases, durable not-a-name — design

2026-11-23 · #247 · feeds #246 · builds on #206 (merge and purge semantics) and #211/#214 (the cleanup it makes durable) · all measurements are from prod, read-only, on 2026-11-23 ≈20:15Z, against `origin/main` `db2d99f`

## Intent

An entity is one cleaned name string. When one real party appears under
several spellings (`PENDERGRAFT, TY` / `TY PENDERGRAFT`,
`BP WEST COAST PRODUCTS LLC` / `…, LLC` / `…,LLC`), it becomes several
unlinked entities. Each entity page then shows only part of that party's
records.

Today an operator can fold one entity into another (#206 `merge_entity_into`)
or purge a non-name (#214). Both **delete the entity row**. `record_entities`
is derived: `reprocess-entities` rebuilds it from the frozen `applicants`
through `get_or_create_entity(name)`. So the next full reprocess recreates
every merged-away or purged string as a fresh, unclassified entity.

This design keeps every entity row and records "same party as" and
"not a name" as persisted data attached to those rows. A reprocess then has
nothing to undo. The repeatable cases are linked by deterministic rules; the
rest by operator decisions.

## What a reprocess would lose today (measured)

I ran today's `extract_entity_names` over all 128,294 records from a
read-only export and compared the result with the live `record_entities`:

| Finding | Count | Consequence |
|---|---|---|
| Purged strings that would be recreated | **2**: `IL` (7 links), `IL IL` (4 links) | The #214 not-a-name purges come back. This is the only present-day loss. |
| #211 merges undone | **0 of 11** | All 11 were WAC-suffix merges. Extraction now strips the suffix itself (`strip_wac_annotation`, `_ENTITY_REPROCESS_VERSION = 5`), so it reproduces every one. |
| Current links a reprocess would drop | 32 links on 30 entities | Unrelated drift: the record's own business name is linked as an applicant. See *For later issues*. |

The audit rows for `IL` and `IL IL` (`admin_audit_log` 38 and 40) hold
`target_id` and `manual_id` but no name. The strings and the manual rows
survive only in `data/remediation-backups/2026-11-06-211/entities.jsonl` and
`…/2026-11-06-214/entity_classifications.jsonl`.

## Goals

- **Durability.** Every link and not-a-name decision survives
  `reprocess-entities`, `reclassify-entities` and new ingests, because
  nothing that carries a decision is deleted.
- **Reproducibility.** The linked view comes from the frozen raw strings
  plus persisted rows: rule rows that can be regenerated, and operator rows
  that are kept.
- **One party, one page.** An entity page shows every record of the party:
  global spellings everywhere, and licence-scoped spellings on that licence.
- **Undoable.** Every link can be removed. Removing a rule's link persists
  as a dismissal, so the rule does not put it back.
- **#246 can build on it.** Split parts that #246 creates resolve to
  existing people through the same layer.

## Non-goals

- Re-asking Jev, or any other change to classification sources.
- Changing `applicants`, `previous_applicants` or any `raw_*` column. WSLCB's
  own typos are not parse failures, so the #152 carve-out does not apply.
- Splitting couples (#246), or repairing WSLCB's `;` inside a name, as
  extraction logic.
- Fuzzy or phonetic matching (Soundex, edit distance) as an automatic rule.

## Anti-goals

The design has failed, even if it works technically, if:

1. **Two different people are fused without an operator choosing to.** This
   is why middle-name linking is licence-scoped, and why no rule crosses
   person/organization.
2. **An operator decision is silently undone.** This covers a rebuild, a
   reprocess, a reclassify or a purge.
3. **Jev rows are deleted.** Linking never deletes an entity, so it never
   cascades away classification rows.
4. **The record page stops showing what WSLCB printed.** Record chips keep
   the as-printed string and only point at the linked party.

## Constraints

- Frozen: `applicants`, `previous_applicants`, `raw_*`, `sources`,
  `record_sources`.
- Never bulk-delete Jev rows. Manual rows and open flags survive (#206).
- Two people can share a name. A rule must not join strings that are
  different names.
- `record_entities` stays derived and is rebuilt as it is today.

## Approaches considered

| Approach | Description | Trade-offs | Outcome |
|---|---|---|---|
| **A. Alias layer over entities** | Keep one entity per string. A new `entity_aliases` table maps a variant entity id to a canonical entity id, with an optional licence scope and a rule-or-manual source. Reads resolve through it. | Nothing is deleted, so it is reprocess-safe by construction. Undo is a row delete. Jev rows stay. Cost: four read paths have to resolve aliases. It follows the `endorsement_aliases` precedent. | **Chosen** by the operator, 2026-11-23 |
| B. Alias at extraction | `get_or_create_entity` maps a variant string to the canonical id, so `record_entities` point straight at the canonical and the variant entity is merged away (#206). | Read paths are unchanged. But a merge deletes the variant's Jev rows, an undo needs a reprocess plus a Jev re-ask, scope needs extraction context, and the record no longer shows the printed string. | Rejected |
| C. Party/cluster table | A `parties` table; entities join through membership rows derived from "same-as" edges (union-find). | The display name is easy to re-choose. But transitive closure fuses people (`DAVID HILL` ~ `DAVID A HILL` ~ `DAVID CLIFTON HILL`), and it adds a new URL space. | Rejected |

**Why not reuse `endorsement_aliases` as it stands:** the shape carries over
(id-keyed variant → canonical, unique per variant, plus a dismissed-pairs
table), and so does the reason it survives reprocess (the variant row is never
deleted). Entities need three things it lacks:

- a licence scope;
- the source (which rule, or an operator), so rule rows can be regenerated
  without touching operator rows;
- flatness enforced when the row is written. `resolve_endorsement` follows
  chains to depth 5 instead.

The entity tables are therefore separate, but modelled on it.

## Design

### 1. Model (Q1)

```
entity_aliases
  id                 identity PK
  variant_id         FK entities.id ON DELETE CASCADE
  canonical_id       FK entities.id ON DELETE CASCADE
  scope_license      text NULL            -- NULL = everywhere; else a license_number
  source             text  CHECK (source IN ('rule', 'manual'))
  rule               text NULL            -- 'format_key' | 'middle_names'; NULL for manual
  created_at, created_by, note
  UNIQUE NULLS NOT DISTINCT (variant_id, scope_license, source)
  CHECK (variant_id <> canonical_id)
  CHECK ((source = 'rule') = (rule IS NOT NULL))

entity_alias_dismissals                 -- an unordered pair, like endorsement_dismissed_suggestions
  entity_id_a, entity_id_b  FK entities.id ON DELETE CASCADE, CHECK (entity_id_a < entity_id_b)
  scope_license      text NULL
  dismissed_by, dismissed_at, note
  UNIQUE NULLS NOT DISTINCT (entity_id_a, entity_id_b, scope_license)
```

Prod runs PostgreSQL 16.15, so `NULLS NOT DISTINCT` is available.

**Rows store what was decided; reads resolve.** A manual row stores exactly
the pair the operator chose, and no automated step ever rewrites it. *(My
call, from the builder check: rewriting operator rows to keep them flat would
silently change a decision when an unrelated rule row moved, which is
anti-goal 2.)*

**Resolution.** For entity `e` on a record with licence `lic`, start at
`x = e` and repeat:
1. if `x` has a row scoped to `lic`, move to its canonical;
2. otherwise, if `x` has a global row, move to its canonical;
3. otherwise stop.

At each step a `manual` row wins over a `rule` row. The result is
`eff(e, lic)`. The loop is capped at 4 steps, which is enough for every
measured case; the writer guarantees there are no cycles.

**Writes.**
- A manual row that would create a cycle is refused, and the panel says
  which link closes it.
- A rule row that would create a cycle with a manual row is not written.
- A scoped manual row may target an entity that is itself a scoped variant
  on the same licence. Resolution follows the chain.

Reads may go through a derived map `(entity, scope) → party`. Rebuild it on
every alias write and in `relink-entities`, so that the entity page needs a
single lookup. This is left to the builder.

**Rows are derived or kept, the same split as `entity_classifications`.**
- `source='rule'` rows are **derived**. `wslcb db relink-entities` deletes and
  rebuilds them; it is idempotent and has `--dry-run`. Add it to AGENTS.md's
  Derived table.
- `source='manual'` rows and all dismissals are **operator data**. Nothing
  automated deletes them.
- A dismissal is an unordered pair plus a scope, so it still holds if the
  group's canonical flips. It stops any rule from linking the two entities in
  that scope. A global dismissal also stops every scoped row for the pair.
  *(My call, from the builder check.)*

**A canonical is still an ordinary entity.** It keeps its own name, links and
classification rows. The canonical of a rule group is chosen
deterministically:

| Kind of group | Canonical, in order of preference |
|---|---|
| Person, format key | the natural-order spelling (no `LAST, FIRST` comma), then the most links, then the lowest id |
| Organization, format key | a spelling without a role annotation, then the most links, then the lowest id |
| Middle names | the longest form (most middle tokens, then the most characters), then the person order above |
| Manual | the operator's choice |

**A group's canonical sticks.** `relink-entities` keeps a rule group's
current canonical for as long as it is still eligible: same resolved type,
not a tombstone, and not a manual variant. Otherwise it elects a new one by
the table above. Two exceptions to the table: an entity that is the target
of a manual row is preferred, and an entity that is a manual variant is never
elected. This keeps `/entity/{id}` URLs and redirects stable as link counts
change. *(My call, from the builder check.)*

### 2. Automatic rules (Q2): the format key

**Decision (operator, 2026-11-23): automatic and global.**
`link_key(name, resolved_type)` is a pure function. Entities with the same
`(resolved_type, link_key)` form one group:

- **person:** if the name has exactly one comma and the part after it is not
  a generational suffix (`JR SR II III IV`, with or without `.`), reorder
  `LAST, FIRST [MIDDLE…]` to `FIRST [MIDDLE…] LAST`. Then turn every `,` and
  `.` into a space and collapse whitespace.
- **organization:** strip a trailing role annotation
  (`[\s,(\-]*\(?\s*FRANCHIS(OR|EE)\s*\)?$`). Then turn every `,` and `.` into
  a space and collapse whitespace.
- **The key never crosses types:** person and organization keys never match.

The key never drops a `LAST, FIRST` comma without also reordering. That
removes the `ZHAO QIAN` / `ZHAO, QIAN` trap by construction: `ZHAO, QIAN`
keys as `QIAN ZHAO`, and that entity exists.

**Measured.** The format key folds **8,945 entities into 8,917 groups**
(13.2% of the 67,604 entities):

| Class | Groups | Variant entities |
|---|---|---|
| Person | 8,895 | 8,913 |
| Organization | 22 | 32 |

- Most person variants (8,833) are exact `LAST, FIRST` reorders.
- The rest are period or suffix-comma differences, such as
  `H.S. WRIGHT III` / `H S WRIGHT III`.
- Group sizes: 8,896 groups of 2, 19 of 3, one of 5 (BP) and one of 8
  (7-ELEVEN).

**False merges (measured).** No ground truth exists, so I measured every
signal that is available:

- **Structural errors: 0.**
  - No comma form whose tail is a generational suffix has a reorder twin.
  - One key is shared by a person and an organization (`VUE, SUNLIGHT` /
    `SUNLIGHT VUE`); the type gate excludes it.
  - I checked all 22 organization groups by hand: every one is one company
    (Appendix B).
- **Evidence tiers for the 8,833 reorder pairs:**

  | Tier | Pairs | Share |
  |---|---|---|
  | Share a licence | 7,843 | 88.8% |
  | Share a co-applicant | 4 | 0.0% |
  | Same city only | 547 | 6.2% |
  | No shared context | 439 | 5.0% |

- **Baseline.** Today the system already treats an identical string as one
  entity. Among the 14,205 single-string persons on two or more licences,
  **19.0%** have licences that no shared co-applicant connects. The reorder
  pairs with no licence or co-applicant in common come to 11.2%. The rule
  therefore adds no class of error beyond same-name strings, which the system
  already accepts.

**Not automatic. These go to the review queue (§4):**
- **77 comma-drop traps:** `LAST, FIRST`, `LAST FIRST` and `FIRST LAST` all
  exist (e.g. `THOMAS, DANIEL` / `THOMAS DANIEL` / `DANIEL THOMAS`). The key
  links the first and last; it suggests the middle one.
- **22 comma forms** whose only twin is the comma-dropped string.
- **11 legal-designator differences** (`SUGAR MOUNTAIN CAPITAL` /
  `… LLC`, `REGAL CINEMAS, INC.` / `… CORPORATION`). 10 of the 11 share a
  licence or a co-applicant.

**Keeping rule rows current, with no timer.** *(My call, from the builder
check.)*
- **Storage.** Store `entities.link_key` as a derived column with an index.
  `get_or_create_entity` writes it.
- **At ingest,** after a record's entities are linked, run both rules for
  just that record: the format key for its entities, and the middle-name rule
  for its licence.
- **When a resolved type changes,** relink the groups involved in the same
  transaction. That happens in `resolve_entities` (classify, reclassify, a
  manual set or clear). A rule row can then never cross types, even for a
  moment.
- **`wslcb db relink-entities`** remains the full rebuild. Run it after a
  migration, a rule change or a `reprocess-entities`.

**Types.** The gate uses the *resolved* type. When a member moves to the
other type, its rule row is dropped by the relink above. A manual row stays,
and opens a flag (see §6).

### 3. Scope (Q3)

| Link | Scope | Why |
|---|---|---|
| Format key | Global | It is the same name, spelled differently. Its risk equals that of an identical string. |
| Middle names | **Licence** (operator, 2026-11-23) | Globally this would fuse people: 1,078 first-and-last groups have conflicting middles, e.g. `JOHN B DIMMER` / `JOHN C DIMMER`. |
| First-name-only fragments (`TY`), abbreviations (`SMC`), nicknames (`TY` ~ `TYLER`) | Licence, set by the operator | Too few to justify a rule. Wrong outside the licence. |
| Manual link | The operator chooses this licence or everywhere | Default: this licence. |

**The scope unit is the licence number**, not a set of records. A scoped
link then covers new records on the same licence without being decided again.
A licence is also how the source groups one business's filings.

**Middle-name rule (`rule='middle_names'`), automatic and licence-scoped.**
Within each licence, take its person entities and group them by the first
and last token of their `link_key`.

**The exact definition.** *(My call, from the builder check; the measured
numbers use it.)*
- **Parsing.** Parse each `link_key` into first token, last token, middle
  tokens, and a trailing generational suffix (`JR SR II III IV`). The suffix
  is split off before the last token is taken.
- **Grouping.** Within a licence, group by `(first, last, suffix)`. A
  missing suffix only matches a missing suffix, so `JOHN SMITH` and
  `JOHN SMITH JR` never link.
- **Compatible middles:**
  - a form with no middles is compatible with any middles: `TYLER` ~
    `TYLER DAVID`, `JAMES` ~ `JAMES RICHARD`;
  - otherwise both forms have the same number of middle tokens, and each
    pair of tokens is equal or one is a single letter that starts the other:
    `A` ~ `ALLEN`;
  - `B` ~ `B C` is *not* compatible.
- **Roles.** The variant and the canonical must share a role on that licence
  (`applicant` or `previous_applicant`). On ASSUMPTION records the seller and
  the buyer share a licence number, and this keeps a family hand-over such as
  `JOHN SMITH` → `JOHN A SMITH` from being fused. It costs nothing today:
  0 compatible pairs are cross-role only, and prod has just 158
  `previous_applicant` links.
- **Outcome.** If every pair in a group is compatible, link each form to the
  longest form, scoped to that licence. If any pair conflicts, write nothing
  and send the group to review.

Measured: **4,908 scoped rows, 3,724 distinct variant entities, 3,638
licences.** 404 conflicting groups within a licence are left for review.

Evidence for the global middle-name pairs:

| Group | Pairs | Same record | Same licence | Co-applicant | City only | None |
|---|---|---|---|---|---|---|
| Compatible middles | 5,137 | 2,227 | 1,942 | 143 | 249 | 576 |
| Conflicting middles | 2,453 | 448 | 348 | 32 | 136 | 1,489 |

Licence scoping removes the "none" and "city only" pairs by construction.

**Two names on one record support a merge.** WSLCB often prints one person
twice on a record, in a short and a long form:

- `MARY ANDERSON; MARY ANGEL ANDERSON; RANDAL ANDERSON; RANDAL JUAN ANDERSON`
- `LEONARD E ROSE; LEONARD E. ROSE`
- `RENEE AND TIM OCHS; RENEE ELIZABETH OCHS; RENEE OCHS`

Of the 75 period-difference person pairs, 57 appear on the same record. A
rule must therefore not treat "both names on one record" as evidence of two
people.

### 4. Operator UI (Q4)

**The entity page's admin panel gains a "Links" section** (beside #208's
classification panel):

- It lists the members of the party: name, source (rule name or the
  operator's email), scope, date, and an **Unlink** button.
  - Unlinking a manual row deletes it.
  - Unlinking a rule row writes a dismissal with the same scope.
- A **Same as…** form takes the target entity, by id or name search, and a
  scope radio: "this licence" (choose one of the entity's licences) or
  "everywhere". It also takes a note.
- A **Not linked to…** action writes a dismissal on its own. Use it to
  pre-empt a rule.

**The review queue** (`/admin/entities/review`) **gains a "Links" tab** of
suggestions. Each suggestion offers *Link* (writes a manual row; the scope is
pre-filled) or *Dismiss* (writes a dismissal).

| Suggestion | Measured count | Default scope |
|---|---|---|
| Comma-drop traps and comma-dropped-only twins | 77 + 22 | everywhere |
| Legal-designator differences (organizations) | 11 variants in 9 groups | everywhere |
| Middle-name conflicts within a licence | 404 groups | licence |
| Single-token person with exactly one same-licence party (`TY`, `HALEY`, `KIRANDEEP`, `SCHAFER`) | 4 of 14 | licence |

Suggestions are computed when the tab loads (or cached by `relink-entities`).
They are never stored as links.

**The public pages:**
- **A global variant's page** (`/entity/{variant}`) redirects (302) to the
  canonical's page, with `?from={variant}`. The canonical's page then shows
  "Listed as: …".
- **The canonical's page** lists the union of records: its own links, its
  global variants' links, and its scoped variants' links on matching
  licences, with distinct licences counted over that union. Under the name it
  lists "Also listed as", each spelling with its scope.
- **A scoped variant's page** (`TY`) stays its own page. On records under the
  scoped licence it shows "On licence 355693: TY PENDERGRAFT →".
- **`/entities`** hides global variants and counts records over the union.
  A search on a variant's spelling still finds the party, because the search
  also matches the names of a canonical's members.
- **Record chips** (`get_record_entities`) keep the printed name, link to
  `eff(e, lic)`'s page, and drop duplicates by party within a role. One
  record listing `RANDAL ANDERSON` and `RANDAL JUAN ANDERSON` shows one chip
  with both spellings.

**Audit.** Three new `admin_audit_log` actions:
- `entity.alias_set`
- `entity.alias_remove`
- `entity.alias_dismiss`

`details` carries **both names**, both ids, the scope, the source and the note.
This is the lesson of `IL`: an audit row with no name cannot be replayed.
Every not-a-name audit row from now on also carries the name.

### 5. Not-a-name (Q5): tombstones

**Decision (operator, 2026-11-23): keep the entity as a tombstone.** This is
the same principle as linking, held in a different table. The decision
already has its #206 home, a manual `entity_classifications` row with
`entity_type='not_a_name'`. It stopped being durable only because the row
holding it was deleted.

**Change 1: `reclassify_entities` no longer deletes confirmed non-names.**
- Entities in `confirmed_noise_ids` lose their `record_entities` links, and
  positions are renumbered.
- The entity row stays, with its manual row.
- Regex noise (`is_noise_name`) is still purged. Extraction never recreates
  it, so nothing is lost.

**Change 2: `parse_and_link_entities` does not link a tombstone.** A
tombstone is an entity whose current manual answer is `not_a_name`. The id
set is loaded once per reprocess and once per ingest batch.

**Change 3: `protected_ids` also covers entities in any manual alias or
dismissal row,** so no automated purge removes an operator decision.

**Tombstones in the alias layer.** *(My calls, from the builder check.)*
`choose_classification` ignores a manual `not_a_name`, so a tombstone keeps
the resolved type `person` or `organization`. These rules therefore key off
"the current manual answer is `not_a_name`":

- **Rules and suggestions skip tombstones.** A tombstone can't be a manual
  link target either.
- **Marking an entity not-a-name drops its rule rows at once,** and its group
  re-elects a canonical. The mark is refused while the entity is the target
  or variant of a manual row; the panel says to unlink it first.
- **`/entities` excludes tombstones.** `/entity/{tombstone}` returns 404 to
  the public. An admin sees the page with its panel, so the decision can be
  cleared.
- **Clearing a tombstone's not-a-name** (`clear_manual`) reprocesses the
  records whose `applicants` or `previous_applicants` contain the name: a
  substring prefilter, after which extraction decides. Its links then come
  back.

**Change 4: backfill `IL` and `IL IL`.** This is a one-off data migration
from the #214 backup:
- recreate each entity under its original name;
- re-insert its manual `not_a_name` row with the original `created_by` and
  `created_at`, plus a note citing #247 and the backup;
- write one audit row each.

No `raw_*` or frozen data is touched. The pass is backed up under
`data/remediation-backups/`.

A scoped not-a-name ("not a name on this licence") is not needed by any
measured case, so it is left out.

### 6. Classification (Q6)

- **Each entity keeps its own classification rows.** Linking moves nothing
  and deletes nothing, so there are no #206 merge semantics here and no Jev
  re-ask.
- **A party shows its canonical's resolved classification.** A member's own
  answer stays visible in its row of the Links list.
- **Rules join only entities of the same resolved type** (§2). A manual link
  across types is allowed. It opens an `entity_review_flags` row on the
  canonical with reason `members disagree on type`. That row surfaces in the
  existing main queue.
- **If a manual answer on a variant differs from the canonical's resolution,
  the variant's answer is not copied.** The canonical keeps its resolution;
  the operator decides through the classification panel. Copying would
  silently override a Jev or regex answer on the canonical with the variant's
  answer.
- **Jev cost.** `classify-entities` can skip global variants from now on,
  since a party's displayed type comes from its canonical. That is an
  optimisation, listed as a follow-up, not a requirement.

### 7. #246 (Q7): after this design's foundation, designed alongside

**Answer.** #246's *design* can proceed now; it depends only on the model
chosen here. Its *implementation* goes after follow-ups 2 and 5 below (#262 and #265).

**The measured reason.** #246's split parts link to existing people only
through the **licence-scoped middle-name rule** or an operator nickname link,
not through `LAST, FIRST` reordering. Of the 18 parts that #246's named cases
split into:

- 2 match an entity exactly (`RENEE OCHS`, `WASSEF HAROUN`);
- 0 match through reordering;
- `HULL, JAMES` does not exist;
- `JAMES HULL` would link to `JAMES RICHARD HULL` and `WENDY HULL` to
  `WENDY KIM HULL`, through the middle-name rule;
- `TIM OCHS` would link to `TIMOTHY FLOYD OCHS` only through an operator
  nickname link.

**For #246:**
- **In 5 of the 7 cases sampled, the couple string repeats people who are
  already listed separately on the same record.** For example
  `RENEE AND TIM OCHS; RENEE ELIZABETH OCHS; RENEE OCHS …` and
  `ANAR; RACHA BARAZI HAROUN; WASSEF & RACHA HAROUN; WASSEF HAROUN`. For
  those records the right outcome may be to suppress the combined string
  (it is covered by its members) rather than to create new people.
- **An alias is one-to-one, but a couple maps to two people.** #246 needs its
  own one-to-many relation. Key it by the combined entity's **id**, which is
  kept, as tombstones are here, not by a string.
- **Split parts should be emitted as plain name strings** through
  `get_or_create_entity`. The format key and the middle-name rule then link
  them with no #246-specific code.

## Worked cases (expected outcomes)

Each case gives the live entity ids and the outcome once follow-ups 1–5 have
shipped.

### Tyler Pendergraft, licence 355693

| Step | Rows | How it is created |
|---|---|---|
| 1 | `PENDERGRAFT, TY` (24234) → `TY PENDERGRAFT` (38707), global | format key, automatic |
| 2 | `TYLER PENDERGRAFT` (83154) → `TYLER DAVID PENDERGRAFT` (38708), scope 355693 | middle-name rule, automatic |
| 3 | `TY PENDERGRAFT` (38707) → `TYLER DAVID PENDERGRAFT` (38708), scope 355693 | operator nickname link |
| 4 | `TY` (24142) → `TY PENDERGRAFT` (38707), scope 355693 | operator accepts a single-token suggestion |

- **Step 3's evidence:** `TY PENDERGRAFT` and `TYLER [DAVID] PENDERGRAFT` are
  printed side by side on all 4 of their records, the double-listing pattern
  from §3.
- **Step 4:** resolution follows `TY` → 38707 → 38708 through the two rows
  scoped to 355693, so `eff(TY, 355693)` = 38708. The order the steps
  happen in doesn't matter: no stored row is rewritten.
- **Result:** on 355693, all 5 entities resolve to 38708, which shows 7
  records. Outside 355693 (none today), `TY` stays `TY`.

### Haley Marie Pendergraft, licence 355693

- `PENDERGRAFT, HALEY` (24235) → `HALEY PENDERGRAFT` (38706), global, by the
  format key.
- `HALEY` (24143) → 38706, scope 355693, when the operator accepts the
  suggestion.
- The full name `HALEY MARIE PENDERGRAFT` from the body appears in no entity
  in prod.

### Sugar Mountain Capital

- `SUGAR MOUNTAIN CAPITAL` (5964) and `SUGAR MOUNTAIN CAPITAL LLC` (41265)
  appear as a legal-designator suggestion. They have no licence in common
  (401176/401200 versus 421522/427697) but share the co-applicants
  `KURT BEECHER DAMMEIER` and `LESLIE DAMMEIER`. The operator links them
  globally; the canonical is 41265.
- `SMC` (655020, manual org) is the only short org name whose initials match
  within its co-applicant network. The operator links it to 41265 with scope
  450267. Its manual classification stays on 655020.
- No abbreviation rule is built: there are 2 short all-letter org names in
  all.

### `ASSOCIATES, LLC` (20239)

- It is not an alias case. The source string is
  `ASPEN LODGING GROUP, LLC & VANCE HOTEL; ASSOCIATES, LLC (PER WAC 314-07-035)`
  (records 17941 and 19491, licence 087556, role `previous_applicant`).
- WSLCB's `;` split the licensee element, which extraction skips as the first
  part. No entity `VANCE HOTEL ASSOCIATES, LLC` exists to link to.
- **Expected:** the operator marks it not-a-name; it becomes a tombstone; its
  2 links go, durably.
- **Alternative, the operator's call:** create `VANCE HOTEL ASSOCIATES, LLC`
  and link 20239 to it with scope 087556.

### BP West Coast Products: 5 entities, not 3

- All 5 form one format-key group: 19027, 19360, 27626, 27630, 28073.
- The canonical is `BP WEST COAST PRODUCTS, LLC` (27626, 18 links): it has no
  role annotation and the most links.
- The other 4 become global rule rows. The canonical's page shows the union,
  40 links.

### 7-ELEVEN: 8 entities

- One format-key group: 18334, 18338, 18623, 19771, 20242, 25508, 29097,
  29325.
- The canonical is `7-ELEVEN,INC` (18623, 14 links).

### `IL` and `IL IL` (the #214 purges)

- They are recreated as tombstones by the backfill.
- Their 11 links stay absent through any later reprocess.

### `ZHAO, QIAN` / `ZHAO QIAN`

- The format key links `ZHAO, QIAN` to `QIAN ZHAO`, globally.
- `ZHAO QIAN` stays separate. It is 1 of the 77 trap suggestions, and the
  operator decides.

### `H.S. WRIGHT III` / `H S WRIGHT III`

- One format-key group, global.
- The evidence is weak (same city only): it is 1 of the 4 period-class pairs
  with no licence or co-applicant in common.

## Follow-ups (ordered)

The operator accepted the design on 2026-11-23, filed 1–5 and deferred 6.

| # | Issue | Answers | Depends on | Size | Decision |
|---|---|---|---|---|---|
| 1 | **Durable not-a-name.** Tombstones in `reclassify`; extraction skips them; `protected_ids` widened; audit details carry the name; backfill `IL` / `IL IL` from the #214 backup. | Q5 | none | S | filed: #261 |
| 2 | **Alias schema and the format-key rule.** Migration for `entity_aliases`, `entity_alias_dismissals` and `entities.link_key`; cycle-safe write API; resolution; `wslcb db relink-entities` (`--dry-run`); relink on ingest and on type change; tombstones excluded; `merge_entity_into` re-points alias rows before it deletes; AGENTS.md Derived table and SCHEMA.md. | Q1, Q2, Q3, Q6 | none | M | filed: #262 |
| 3 | **Read paths.** Entity page union and redirect; `/entities` hides variants and counts the union; record chips. | Q1 | #262 | M | filed: #263 |
| 4 | **Admin UI.** Links panel, Links review tab, suggestion generators, audit actions. | Q4 | #262, #263 | M | filed: #264 |
| 5 | **Licence-scoped middle-name rule.** Lands after 2–3 so its 4,908 rows arrive on proven plumbing. | Q2, Q3 | #262, #263 | S | filed: #265 |
| 6 | **Jev spend.** `classify-entities` skips global variants. | Q6 | #262 | XS (optional) | deferred, not filed |
| #246 | Couple splitting. Design now; implement after #262 and #265. | Q7 | #262, #265 | — | existing issue |

#261 and #262 can run in parallel.

**Rule kept from #206:** destructive `merge_entity_into` stays only for
merges extraction itself reproduces, where the cleaned string becomes
identical (case, WAC suffix). Every judgement merge is an alias.

## Left to the builder

- **Where suggestions live:** computed when the tab loads, or cached in a
  derived table by `relink-entities`. Measure the load time first.
- **How the union query is built** (a CTE over `entity_aliases`, or a
  materialised member map), and the index choice. Keep `/entity/{id}` under
  today's latency for the largest party (the 7-ELEVEN group).
- **The exact regex** for generational suffixes and role annotations. Keep the
  measured behaviour in Appendix A as the test fixture.
- **The redirect code** (302 or 301) and the `?from=` wording.
- **Whether the ingest hook** runs per record or per batch.
- **The FK on-delete behaviour** for `canonical_id`. CASCADE plus the
  `protected_ids` guard is the proposal. RESTRICT is acceptable if the purge
  paths re-point first.

## For later issues (not #247)

- **For #N (link drift):** a full reprocess would drop 32 current links on 30
  entities. In each, the record's own business name is linked as an applicant
  (e.g. `7 CLOUDS SMOKE SHOP LLC` on records 2030818 and 2075091, ingested
  between August and November 2026). Find the path that wrote them, then
  decide whether to reprocess those records.
- **For #N (enrichment version):** `pipeline._record_enrichment` stamps
  `entities` as version `"1"` on ingest, while `reprocess_entities` stamps
  `_ENTITY_REPROCESS_VERSION` (5). Prod has 16,153 records at v1 and 112,141
  at v2 (last run 2026-07-30), and none at 5. The stamp can't tell a fresh
  record from a stale one.
- **For #N (orphans):** 142 entities have no links today, and nothing
  collects them. Any future orphan GC must keep alias members, alias targets
  and tombstones.

## Appendix A: how the numbers were measured

- **Data.** Read-only `\copy` of the following, all from 2026-11-23 ≈20:15Z,
  with `default_transaction_read_only=on`:
  - `license_records(id, applicants, previous_applicants, license_number)`;
  - `entities(id, name, entity_type, entity_subtype, classification_source)`;
  - `record_entities`;
  - record → `locations.std_city` (falling back to `city`).

  The analysis ran offline on the export.
- **Reprocess simulation.** `extract_entity_names` from `db2d99f` over every
  record's two strings, compared by `(record, role, entity)` with the live
  links.
- **Evidence tiers**, for a pair (variant, canonical), first match wins:
  1. linked to the same record;
  2. share a `license_number`;
  3. share a co-applicant entity (excluding each other);
  4. share a city only;
  5. none.
- **Baseline.** Person entities with links on two or more licences. A licence
  pair is joined when the two licences' records share a co-applicant
  (union-find). Reported: the share whose licences do not all join.
- **Compatible middles.** As defined in §3. The *global* middle-name table
  in §3 came from an earlier pass that treated the generational suffix as the
  last token. It is shown only to support the scoping decision; the
  licence-scoped numbers use the exact definition.
- **Counts quoted:**

  | Rule or class | Measured |
  |---|---|
  | `LAST, FIRST` persons | 12,536 (8,834 with a reorder twin of any type, 8,833 same type) |
  | Format key | 8,945 variants / 8,917 groups |
  | Comma-drop traps | 77 (plus 22 comma-dropped-only) |
  | Legal designators | 11 / 9 |
  | Role annotations | 20 names contain `FRANCHIS`, 18 end in a role annotation |
  | Middle names, global | 5,137 compatible / 2,453 conflicting variants in 1,078 groups |
  | Middle names, licence-scoped (exact definition, §3) | 4,908 rows / 3,724 entities / 3,638 licences / 404 conflicts; 0 cross-role-only pairs |
  | Single-token persons | 14; 4 have exactly one same-licence party once the format key is applied (`TY`, `HALEY`, `KIRANDEEP`, `SCHAFER`); `CASTELLANOS` has 2; 9 have none |
  | Short org names | 2 |
  | Resolved types | person 66,120 / organization 1,484 |

## Appendix B: all 22 organization format-key groups (checked by hand)

Each entry gives the entity id and its link count, as `id:links`.

| Group | Members |
|---|---|
| 13 COINS HOLDINGS LLC | 5561:3 · 52504:2 |
| 7-ELEVEN INC | 18334:1 · 18338:3 · 18623:14 · 19771:1 · 20242:1 · 25508:1 · 29097:4 · 29325:1 |
| A FRENCH CORP GRAND VINS | 34127:4 · 34128:4 |
| A-1 VISTA LLC | 645771:1 · 654162:1 |
| AMF BOWLING CENTERS INC | 5151:5 · 11000:2 |
| ANCIENT LAKE WINE COMPANY LLC | 16398:2 · 70609:4 |
| ARENA SPORTS INC | 14860:1 · 33806:3 · 44415:2 |
| ARLINGTON ROLLING FARMS LLC | 9927:1 · 10192:2 |
| BOSTON PIZZA RESTAURANTS LP | 20226:1 · 29460:1 |
| BP WEST COAST PRODUCTS LLC | 19027:6 · 19360:13 · 27626:18 · 27630:2 · 28073:1 |
| CASTAV LLC | 13282:1 · 53640:9 |
| CURTIS MYERS LLC | 3056:1 · 41882:2 |
| ELWOOD INVESTMENTS L L C | 7881:4 · 660985:1 |
| ES RESTAURANT GROUP INC | 6736:18 · 37648:8 |
| FOLEY FAMILY WINES INC | 11199:3 · 11240:1 |
| GOOD CHEER HOSPITALITY INC | 61273:1 · 61474:1 |
| GOODWIN GROUP INC | 46582:2 · 62274:1 |
| LACHSELIAN LLC | 13413:3 · 77641:2 |
| OJD LLC | 32301:5 · 44111:3 |
| SEA CREATURES LLC | 7125:10 · 57293:2 |
| TOMCAL INC | 46827:6 · 46828:6 |
| TRAIN WRECK INC | 55866:2 · 61999:1 |
