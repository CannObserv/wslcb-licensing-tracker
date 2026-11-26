---
title: "#265 licence-scoped middle-name linking rule"
date: 2026-11-26
status: draft
---

# #265 licence-scoped middle-name rule

Design: [#247 spec](../specs/2026-11-23-247-entity-name-linking-design.md) §3
"Middle-name rule". The plumbing it writes through:
[ENTITY_ALIASES.md](../ENTITY_ALIASES.md) (`relink`, #262; `party_id`, #263;
the Links tab, #264).

## Problem

Within a licence, WSLCB often prints one person in a short and a long form
(`TYLER PENDERGRAFT` / `TYLER DAVID PENDERGRAFT`). The format key links only
spellings of the same name, so these stay two parties: the record chips split,
and the entity pages each show half the records. The spec's rule links them
automatically, only on that licence. Groups that conflict go to the Links tab
for the operator.

## Approach

**A second planner in `relink`, after the format key.**

**For each licence, the members are its person parties.**
- A party is `eff(entity, licence)` under the format-key and manual rows: the
  stored `party_id` before this rule exists.
- Every spelling the format key already links folds into its canonical.
- An operator's scoped link on the licence folds the variant into its target.

**Keys and grouping:**
- Parse each party's `link_key` into first, last, middles and a trailing
  generational suffix (JR/SR/II/III/IV), split off first.
- Leave a party out when it has fewer than two tokens, has a standalone
  `AND`/`&` token (couples, #246's note), or is marked not-a-name.
- Group by `(licence, first, last, suffix)`.

**Compatibility is as in the issue:**
- no middles is compatible with any middles;
- otherwise the same number of middles, each equal or a single letter that
  starts the other.

**A compatible group:** each party links to the longest form, scoped to the
licence. "Longest" means:
1. the most middles;
2. then the most characters;
3. then the person order (natural order, most links, lowest id).

**Rows a group skips:**
- a party that shares no role with the canonical;
- a pair dismissed everywhere or on that licence. A dismissal between two
  entities counts against their parties.

**A group with a conflict writes nothing.**

**Why this is safe by construction:** each row runs from a party to a party,
and a party has no applicable row on that licence. A middle-name row
therefore never closes a loop and never adds more than one resolution step.

**Rebuilt alongside the format key:**
- **The full rebuild** (`relink-entities`) covers every licence.
- **A targeted relink** covers:
  - the licences of the person links of every entity touched, or of any
    entity resolving through one (`reverse_closure`);
  - plus every scope of an alias row or dismissal touching those entities.

  That covers ingest (the record's licence), a type change, a manual link,
  Unlink, Undo, a dismissal and a merge.
- **Rows are diffed** per `(variant, licence)` and written with the existing
  `ON CONFLICT`. `refresh_parties` gets both ends of every changed row.

**Middle-name rows are left out of two cycle checks:** the format-key
planner's and `add_manual_alias`'s. Both checks run in a transaction that
replans the middle-name rows, so an operator's link wins over one. This is
the #262 treatment of format-key rows.

**Conflicts on the Links tab.** `alias_suggestions` gains the generator
`_middle_names`, kind `middle_names`, scope = the licence.
- It suggests every pair in a conflicting group, shorter → longer, labelled
  with both middles: `J / K`, `B / B C`. That is 484 pairs in 397 groups on
  prod.
- Linking or dismissing a pair drops it, as with the other kinds.

**The dry run's report adds per-rule counts:** rows, variant entities,
licences and conflicting groups.

## Measured on prod (read-only export, 2026-11-26)

| Definition | Rows | Variant entities | Licences | Conflicts |
|---|---|---|---|---|
| The issue's (11-23, one row per entity) | 4,908 | 3,724 | 3,638 | 404 |
| Same definition, today | 4,924 | 3,731 | 3,652 | 405 |
| **This plan: one row per party, couples excluded** | **4,706** | **3,506** | **3,651** | **397** |

- **The definition reproduces.** 11-23 to today is new ingests: +16 rows.
- **Per party instead of per entity: 218 fewer rows, with identical
  resolution.** A format-key variant such as `PENDERGRAFT, TYLER` already
  resolves to `TYLER PENDERGRAFT`. It reaches 38708 through that party's one
  row and needs no row of its own.
- **Excluding couples:**
  - 8 fewer conflicts: BLACKWELL ×6, OCHS and one more;
  - **`RENEE OCHS` → `RENEE ELIZABETH OCHS`** (077675) now forms;
  - `WASSEF HAROUN` no longer goes to the couple string.
- **Cross-role pairs: 0**, as the issue says.
- **Pendergraft:** `TYLER PENDERGRAFT` (83154) → 38708, scope 355693, the spec's
  step 2. The four manual rows from #264 are inputs, not touched.
- **Conflict shapes (397 groups):**

  | Shape | Groups |
  |---|---|
  | One middle each, differing (`MATTHEW J` / `MATTHEW K`) | 149 |
  | A compound surname, one form with an initial (`FERNANDO CUELLAR VAZQUEZ` / `FERNANDO G CUELLAR VAZQUEZ`) | 78 |
  | `B` / `B C` | 67 |
  | 3 or more members, or other | 103 |

## Tradeoffs / alternatives

- **One row per entity, as the issue counted it.** About 4,924 rows. A format-key
  variant then carries a scoped row beside its global one. That is redundant,
  and it loops when the two rules elect different canonicals (the format key's
  canonical sticks, while this rule's is the longest). Rejected.
- **One suggestion per conflicting group.** A suggestion is a pair with a
  Link and a Dismiss, and a group has no single pair to offer. Rejected for
  pairs.
- **Caching conflicts in a table filled by `relink`.** That needs a migration.
  Rejected unless the Links tab measures slow (budget: keep it under ~0.5 s).
- **Widening compatibility for compound surnames (78 groups).** That is a
  change to the approved definition. Left for a later issue (see open
  questions).

## Steps

1. **Pure functions in `entity_aliases.py`:**
   - `parse_person_key`, `middles_compatible` and `plan_licence`. Given a
     licence's parties, `plan_licence` returns the rows and the conflicts.
   - The unit fixture covers suffix, initials, `B` vs `B C`, no middles,
     cross-role, `AND`/`&`, dismissed, and the tie-breaks.
2. **Database side of `relink`:**
   - pick the licences and load the parties under the post-plan format-key
     rows and the manual rows;
   - diff, write, and refresh parties;
   - leave middle-name rows out of the two cycle checks;
   - add the counts to `RelinkResult` and to the CLI output.

   PG tests cover the Pendergraft case on a fixture; ingest of a new long
   form; a type change dropping rows; a manual link, Unlink then Undo; and a
   second run with 0 changes.
3. **The Links tab:**
   - `_middle_names` in `GENERATORS` and `KIND_ORDER`, with its label in the
     template;
   - route and template tests.
4. **Docs:**
   - ENTITY_ALIASES.md: the rule, the per-party call, the cycle reasoning;
   - the CLI help;
   - README if the module tree changes.
5. **Scratch rehearsal on a full prod copy (pinned scratch URL):**
   - `--dry-run` ≈ the bold row above;
   - a real run, then a second run with 0 changes;
   - `VACUUM (ANALYZE) record_entities`;
   - Pendergraft and Ochs resolve as expected;
   - the four manual rows are unchanged;
   - time the full relink, the ingest hook per record and the Links tab.
6. **Gate, CR, then pause before shipping.**
7. **Ship.** No migration:
   - merge, `push.sh`, restart `wslcb-web`;
   - prod: `relink-entities --dry-run`, the real run, a second run with
     0 changes, `VACUUM (ANALYZE) record_entities`;
   - verify 83154 on 355693 and the Links tab;
   - comment and close.

## Open questions / risks

- **D1 (yours): accept the per-party counts as the done-when.** The issue's
  numbers (4,908 / 3,724 / 3,638 / 404) count one row per entity and include
  couples. Under this plan the dry run should show about
  4,706 / 3,506 / 3,651 / 397, give or take new ingests, with the same
  resolution.
- **Compound surnames.** 78 of the 397 conflicts are a compound surname where
  one form carries a middle initial. They land on the Links tab under the
  approved definition. A follow-up could compare the last two tokens. I won't
  file it unless you want it.
- **The ingest hook's cost** is about 7.9 ms per record today. Loading a
  licence's parties adds a query or two; I'll measure it in step 5.
- **Purges:** a noise purge relinks only its key-mates. A conflict that only a
  purged person caused clears at the next full relink. Purged entities are
  almost all non-names, so I'll note this rather than build for it.
- **#269:** its split-entity exclusion goes into `plan_licence` beside the
  `AND`/`&` exclusion.
