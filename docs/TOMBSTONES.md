# Tombstones: operator-confirmed non-names

An operator can mark an entity **Not a name** (the classification panel, or
accepting a Jev `not_a_name` answer). Since #261 the entity becomes a
**tombstone**: its row and its classification rows stay, and no record links
to it. Before #261, `reclassify-entities` deleted the entity together with the
decision, and the next reprocess recreated the string as a fresh entity. That
is how `IL` and `IL IL` were lost in #214. Design: #247 spec §5
([`specs/2026-11-23-247-entity-name-linking-design.md`](specs/2026-11-23-247-entity-name-linking-design.md)).
Code: `entity_classification.tombstone_ids` / `tombstone_names`,
`entities.unlink_tombstones` / `records_naming`, `classification_review`,
`tombstone_restore.py`.

**Definition.** A tombstone is an entity whose current manual answer (the
latest non-ignored manual row) is `not_a_name`, whatever its open flags. The
resolver ignores that answer, so `entities.entity_type` still reads `person`
or `organization`. Code that cares asks `tombstone_ids`, never the type.

## What a tombstone does

| Where | Effect |
|---|---|
| Marking it | its `record_entities` rows go in the same transaction, with positions compacted and its alias groups re-elected; the audit row records `tombstone: entered`, `links_removed` |
| Extraction (ingest, `reprocess-entities`, `backfill_entities`) | the name is skipped before `get_or_create_entity`, so it is never linked or recreated. The lowercased name set loads once per reprocess, ingest batch or backfill |
| `reclassify-entities` | kept, never purged, even when the name is regex noise; any links it still has are removed (marks made before #261) |
| `db check`, the WAC verify | its name is not an expected link |
| Alias rules, suggestions, manual links, Jev | excluded, as before (#262, [`ENTITY_ALIASES.md`](ENTITY_ALIASES.md)) |
| `/entities` | not listed |
| `/entity/{id}` | 404 to the public. An admin sees the page with a banner giving the number of records that print the name |

## Leaving tombstone state

*Clear manual override*, or a manual row of a real type over the tombstone,
reprocesses the records that print the name. The prefilter is
`records_naming`: a case-insensitive match of the whole name over
`applicants`, `previous_applicants` and both `raw_*` columns, bounded by a
non-alphanumeric character or either end. `IL` finds 101 prod records; a
plain substring finds 15,661. Extraction then decides which links return.
The audit row records `tombstone: left`, `records_reprocessed`.

## Restoring purged non-names

`wslcb db restore-tombstones --backup DIR [--dry-run] [--admin-email E]`
reads `entities.jsonl` and `entity_classifications.jsonl` from a remediation
backup. It recreates each entity whose latest manual answer there is
`not_a_name`, as follows:

- the entity, under its original id, name and `created_at`, so the audit rows
  already written about it point at it again;
- its manual row, with the original `created_by` and `created_at` and a note
  citing #261;
- a fresh regex row;
- one `entity.tombstone_restore` audit row, attributed to the only admin
  account or to `--admin-email`.

It links nothing, and it refuses (writing nothing) when an id now holds
another name or the name is held by another entity. A rerun lists the
entities as already restored. The #214 backup is
`data/remediation-backups/2026-11-06-214/` (`IL` 30693, `IL IL` 30694).
