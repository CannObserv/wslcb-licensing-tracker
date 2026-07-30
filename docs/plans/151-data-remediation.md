# #151 Data Remediation — replay-driven cleanup of diff-era records

Status: APPROVED 2026-07-30 — decisions: (1) corrections add a `manual` source row; (2) transient date-variants are kept as separate records; (3) diff archive is FROZEN after this pass (no periodic backfill-diffs re-runs)
Prereq: chain-replay ingestion merged (17bfd36). Follow-on: #154 (provenance extracts) runs after this.

## Problem

The DB still carries the old parser's diff-era damage, quantified against the
replayed corpus (78,651 records; 43,677/43,677 names validated, 0 bleeds):

| Class | Count | Evidence |
|---|---|---|
| Missing legit records | ~369 | replay keys absent from DB |
| Shared-key wrong names | 46 + 17 | DB holds address/blank/other-business name where replay (anchor-validated) has the true name |
| Diff-only suspects, license never on page | 98 | phantoms incl. license-field chimeras (`4/17/2024`, `MERLIN PROPERTY GROUP II LLC`, street addresses) |
| Diff-only suspects, license known, near-date/same-name | ~878 | likely legit transients (in-place date corrections) — replay mutation extraction now validates these |
| Diff-only suspects, license known, no nearby date | ~711 | per-license review against replay |

"Diff-only" = no full-page source (snapshot/IA/live) corroborates the record.
Prior lists (52 unverifiable + 15 weak + ~6 chimeras from #151) are subsumed
by these classes.

## Approach

One scripted pass driven by a fresh replay of the corpus (deterministic;
~50 min). Every mutation is backed up first (TSV, same pattern as
`hybrid_records_backup.tsv` / `bleed_correction_backup.tsv`). No `raw_*`
column or provenance row is ever modified — corrections touch
`business_name` only; deletions remove whole phantom records and their
dependent rows.

1. **Backup** — export all candidate rows (all classes) + their
   record_sources to a timestamped TSV before any write.
2. **Insert missing** — live `wslcb ingest backfill-diffs` run; duplicates
   skip via the natural-key constraint. Address validation deferred
   (`backfill-addresses` after).
3. **Correct names** — for the 46+17 shared-key disagreements: set
   `business_name` to the replay value (anchor-validated). Record ids +
   old/new values in the backup TSV.
4. **Delete phantoms** — the 98 never-on-page records and license-field
   chimeras, after re-verifying each against the fresh replay by-license
   index. Clean orphaned locations afterward (`wslcb check --fix`).
5. **Classify residual suspects** — near-date/same-name records whose
   (license, date) now matches a replay mutation/transient → mark resolved
   (keep). Remainder → per-license report for manual review; no automated
   deletion.
6. **Rebuild derived data** — `reprocess-endorsements`, `reprocess-entities`,
   `rebuild-links` (regenerable per the frozen/derived contract).
7. **Verify** — re-run the anchor validation + DB key-set comparison;
   post numbers to #151; close #151 if items 2–5 are fully resolved.

## Tradeoffs

- **Delete vs mark**: phantoms are deleted (with backup) rather than
  soft-flagged — they never existed on the source page, and keeping them
  contradicts the tracker's purpose. The "never delete historical data"
  rule protects *observed* history; these were never observed.
- **Automated vs manual for the 711**: automated deletion risks killing
  legit records the replay missed (mid-2025 damaged era); manual review of
  a generated report is slower but safe. Chosen: report + manual.
- **scraped_at on inserted records** is the evidencing capture ts (entry) or
  an upper bound (exit) — documented; good enough for provenance.

## Steps (execution artifacts)

- `scratch/remediation_151.py` (session scratch, not committed): replay →
  classify → backup → mutate, each phase gated by `--phase` and `--dry-run`.
- Backup TSVs kept alongside prior ones; paths posted to #151.

## Decisions (were open questions)

1. **Yes** — name corrections add a `manual` source row for auditability.
2. **Yes** — both date-variants of transient records are kept; both were
   genuinely on the page.
3. **Frozen** — the diff archive is fully ingested after this pass;
   `backfill-diffs` is not re-run periodically.
