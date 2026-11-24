# Entity aliases

How one party's several spellings are linked without merging their entity
rows (#262). Design: [`specs/2026-11-23-247-entity-name-linking-design.md`](specs/2026-11-23-247-entity-name-linking-design.md)
§1–§3. Code: `src/wslcb_licensing_tracker/entity_aliases.py`.

An entity stays one cleaned string. `entity_aliases` points a **variant**
entity at a **canonical** one, everywhere or on one licence. Nothing is
deleted, so a `reprocess-entities` leaves every alias row in place and no Jev
row is lost.

## Tables

### `entity_aliases`

| Column | Meaning |
|---|---|
| `variant_id`, `canonical_id` | FKs to `entities(id)`, `ON DELETE CASCADE`; index on `canonical_id` |
| `scope_license` | NULL = everywhere; else a `license_number` |
| `source` | `rule`: **derived**, rebuilt by `relink`. `manual`: **operator data**, never rewritten or deleted by anything automated |
| `rule` | the rule that wrote a `rule` row: `format_key` (#262), `middle_names` (#265); NULL for `manual` |
| `created_at`, `created_by`, `note` | |

Constraints: `UNIQUE NULLS NOT DISTINCT (variant_id, scope_license, source)`
(`uq_entity_aliases_variant_scope_source`); CHECKs `ck_entity_aliases_not_self`,
`ck_entity_aliases_source`, `ck_entity_aliases_rule_iff_rule_source`.

### `entity_alias_dismissals`

An unordered pair an operator said are **not** one party: `entity_id_a` <
`entity_id_b` (CHECK `ck_entity_alias_dismissals_ordered`), FKs `ON DELETE
CASCADE`, `scope_license` (NULL = everywhere), `dismissed_by`,
`dismissed_at`, `note`. `UNIQUE NULLS NOT DISTINCT (entity_id_a, entity_id_b,
scope_license)`; index on `entity_id_b`. **Operator data.**

### `entities.link_key`

The **derived** format key, `link_key(name, resolved type)`; index
`idx_entities_link_key`. NULL for any type but person and organization.
`get_or_create_entity()` writes it on insert; `relink` refreshes it.

### `record_entities.party_id` (#263)

Each link's party: `eff(entity_id, the record's licence)`. **Derived.**
Index `idx_re_party_record (party_id, record_id)`. A trigger
(`record_entities_party_default`, migration 0012) defaults it to `entity_id`
on insert, and when an update re-points `entity_id` without setting it, so
it is never NULL and insert sites need not know about it. `relink` writes the
real party (`refresh_parties`) for the links of every entity whose
resolution passes through the entities it looked at; a full
`relink-entities` checks every link and writes only differences.

## Resolution

`eff(e, licence)`: starting at `e`, follow the row scoped to the licence,
else the global row; at each step `manual` beats `rule`; at most 4 steps.
`eff_many(conn, [(entity, licence), …])` resolves a batch with one fetch per
step. With no licence context only global rows apply.

The writers keep the graph acyclic:
- `add_manual_alias` refuses a link that would close a loop among manual and
  other non-format-key rows, and names the rows that close it.
- `relink` skips a rule row that would close a loop (counted as
  `skipped_cycle`). A manual link therefore beats a format-key row: it is
  accepted, and the group is replanned in the same transaction.
- A merge drops a re-pointed manual row that would close a loop: the rows left
  already state the same equivalence.

## The format-key rule (global)

- **person:** `LAST, FIRST [MIDDLE…]` → `FIRST [MIDDLE…] LAST`, unless the
  part after the one comma is `JR SR II III IV`; then `,` and `.` → space.
- **organization:** strip a trailing FRANCHISOR/FRANCHISEE annotation; then
  `,` and `.` → space.
- Entities with the same `(resolved type, link_key)` form a group; every
  other member gets a global `rule` row to the group's canonical. Keys never
  cross types.

**Canonical election**, in order:
1. the current canonical, while it is still an eligible member, unless an
   operator pointed it at another member;
2. a manual target;
3. the natural-order (person) or annotation-free (organization) spelling;
4. the most record links;
5. the lowest id.

A *global* manual variant is never elected; a scoped one can be, because it
is still its group's party everywhere else (the spec's Pendergraft case).

**No rule row** for a tombstone (current manual answer `not_a_name`), or for
a pair with a dismissal in any scope: the rule row is global, so it would
link the pair on the dismissed licence too.

## Keeping rule rows current

`relink(conn, entity_ids)` rebuilds the groups those entities are in now or
were in before; `relink(conn)` rebuilds all of them (`wslcb db
relink-entities [--dry-run]`, ≈7 s on prod). Rows are diffed, not
rewritten, so a second run changes nothing. It runs, in the caller's
transaction:

| When | Where |
|---|---|
| a record is ingested | `pipeline._enrich_new_record` (own savepoint) |
| a resolved type changes | `resolve_entities` (classify, reclassify, Jev); manual rows now across types get a flag |
| a manual classification is set or cleared | `record_manual` / `clear_manual` (so a not-a-name mark drops the rule rows) |
| entities are merged, purged or renamed | `merge_entity_into` (operator rows re-pointed first), `_purge_entities`, the WAC repair |
| `reprocess-entities` | every group, or the record's entities with `--record-id` |

Each of these also refreshes `record_entities.party_id` for the affected links.

## Read paths (#263)

All read `record_entities.party_id` (`queries_entity.py`, `entities.get_record_entities`):

| Page | Behaviour |
|---|---|
| `/entity/{id}` of a global variant | 302 to `/entity/{party}?from={id}`; the party's page shows "Listed as: …" |
| `/entity/{id}` | records = links with `party_id = id` plus the entity's own links (distinct); licences counted over that union; "Also listed as" lists the members: everywhere, or the licences where their links resolve here |
| a scoped variant's page (e.g. `TY`) | stays its own page; its records on the scoped licence carry "On licence N: <party> →" |
| `/entities` | global variants hidden; `record_count` = distinct records per party (two spellings on one record count once); `q` also matches spellings linked under a party |
| record chips | one chip per party within a role, linked to the party, every printed spelling shown (`A / B`) |

Measured on a prod copy (2026-11-24): the per-party count with the
`(party_id, record_id)` index is 95 ms, against 268 ms for the old per-entity
count and 519 ms when the party is resolved at read time.

## Operator writes

`add_manual_alias`, `remove_manual_alias`, `dismiss_pair` and
`undismiss_pair` (the #264 UI calls them; audit rows are written there):
- a self-link, a not-a-name entity at either end, or a second manual row for
  the variant in the same scope is refused (`AliasError`);
- a manual link across resolved types is allowed and opens an
  `entity_review_flags` row `members disagree on type` on the canonical, as
  does a later type flip of either end.

Entities in a manual row or a dismissal are in `protected_ids()`. Marking an
entity in a manual row not-a-name is refused until it is unlinked.
