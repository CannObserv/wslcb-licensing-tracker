# Investigation: Approved Section Data Gap

**Date:** 2026-02-27  
**Status:** Root cause identified — WSLCB source page restructure, not a scraper bug

## Summary

The approved section of the WSLCB source page underwent a **major restructure around March 30, 2025**. Several application types — most notably **NEW APPLICATION** — were removed from the approved section entirely, while **RENEWAL** (previously absent) was added to both sections. The WSLCB added an error banner acknowledging "a known data transfer issue" that remains on the page today.

This is a **source-side change**, not a scraper bug. Our scraper correctly captures what the WSLCB publishes.

## Timeline

| Date | Event |
|---|---|
| **Jan 26, 2025** | Wayback snapshot: no error banner, no RENEWAL type, NEW APPLICATION present in approved (normal state) |
| **~Mar 30, 2025** | RENEWAL first appears in both new_application and approved sections (diff: `2025_03_30-09_15_00`). Error banner added to page. |
| **Apr–May 2025** | Several minor types disappear from approved section (ADDED FEES, SE PLAN REFUND, IN LIEU, etc.) |
| **May 12, 2025** | Last approved NEW APPLICATION record date |
| **Jun 10, 2025** | Last diff showing NEW APPLICATION additions to approved section |
| **Jun 13, 2025** | Last NEW APPLICATION records age out of the rolling 30-day approved window (removals only) |
| **Jun 14, 2025 → present** | Zero NEW APPLICATION records in approved section |

## What Changed

### Types removed from the approved section

| Application Type | Last Approved Record | Historical Count | Impact |
|---|---|---|---|
| **NEW APPLICATION** | 2025-05-12 | 4,026 | **High** — was #1 type by volume, 46% match rate |
| IN LIEU | 2025-03-27 | 404 | Low — likely folded into ADDED/CHANGE OF CLASS/IN LIEU |
| ADDED FEES | 2025-03-26 | 258 | Low |
| ALCOHOL PERMITS | 2025-02-14 | 176 | Low |
| SE PLAN REFUND | 2025-03-12 | 123 | Low |
| CHANGE OF CORPORATE NAME | 2025-03-25 | 17 | Negligible |
| DUPLICATE LICENSE/PERMIT | 2024-10-31 | 14 | Negligible |
| ADD FINANCIER | 2025-03-03 | 11 | Negligible |
| DISC. LIQUOR SALES | 2023-08-14 | 3 | Negligible (was already gone) |

### Types added to the new_application section

| Application Type | First Record | Count (to date) | Previously |
|---|---|---|---|
| **RENEWAL** | 2025-03-30 | 28,677 | Never existed in either section |
| DISC. LIQUOR SALES | 2025-04-30 | 825 | Only in new_application, never in approved |
| ADDED/CHANGE OF TRADENAME | 2025-04-18 | 703 | Was in both sections (rare in approved) |
| CHANGE OF CORPORATE OFFICER | 2025-05-06 | 233 | Was in both sections (still in approved ✓) |
| RESUME BUSINESS | 2025-04-30 | 28 | Was in both sections (still in approved ✓) |

### Types unaffected (still flowing in both sections)

- RENEWAL *(new, present in both)*
- ASSUMPTION
- CHANGE OF LOCATION
- CHANGE OF CORPORATE OFFICER
- ADDED/CHANGE OF CLASS/IN LIEU
- RESUME BUSINESS

## The Error Banner

The source page now carries a prominent red banner:

> *"This report contains errors due to a known data transfer issue. Our team is working to fix the problem as soon as possible. When the issue is fixed this message will be removed."*

This banner was **not present** on Jan 26, 2025 (Wayback snapshot) but **was present** by May 24, 2025 (Wayback snapshot). It appeared concurrently with the RENEWAL introduction on ~March 30, 2025, and remains on the page as of Feb 27, 2026 — **nearly 11 months later**.

## Interpretation

The most likely explanation is that the WSLCB migrated or restructured their backend data feed around March 2025. The migration:

1. **Introduced RENEWAL** as a new application type (previously, renewals may have been categorized differently or not published)
2. **Broke the data pipeline** for NEW APPLICATION approvals and several minor types
3. The WSLCB is aware of the issue (hence the banner) but has not fixed it in ~11 months

Note the **RENEWAL explosion**: from zero to 9,718 records/month in September 2025 — renewals now dominate the approved section. Meanwhile, the total volume of other approved types (ASSUMPTION, CHANGE OF LOCATION, etc.) remained roughly stable, confirming they were unaffected.

## Impact on Record Linking (#14)

### What's linkable

| Category | Records | % of new_application |
|---|---|---|
| Fully linkable types (RENEWAL, ASSUMPTION, etc.) | 33,676 | 80.0% |
| NEW APPLICATION (pre-gap, ≤ 2025-05-12) | 5,486 | 13.0% |
| **Subtotal: linkable** | **39,162** | **93.0%** |
| NEW APPLICATION (post-gap, > 2025-05-12) | 1,401 | 3.3% |
| Never-linkable types (DISC. LIQUOR SALES, etc.) | 1,529 | 3.6% |
| **Total** | **42,092** | **100%** |

**93% of all new_application records can be linked to approved records.** The gap only affects 1,401 NEW APPLICATION records filed after May 12, 2025.

### Implications for implementation

1. **Proceed with linking** — the data gap does not block the feature. The vast majority of records are linkable.
2. **Show the gap clearly in the UI** — for post-May-2025 NEW APPLICATION records with no match, display "Approval data unavailable" (distinct from "Pending" or "Unknown") so users understand this is a source limitation, not a missing approval.
3. **RENEWAL is the new dominant type** — with 28,677 new_application records and a 95.5% match rate, RENEWAL alone makes the linking feature highly valuable.
4. **Monitor for fix** — if WSLCB resolves the "data transfer issue", NEW APPLICATION will presumably reappear in the approved section. The linking engine should automatically pick up new records.
5. **Consider filing a public records request** — the approval data likely exists in the WSLCB's internal systems; only the public web report is broken.

## Investigated: Did RENEWAL Replace NEW APPLICATION?

**No.** RENEWAL is a new data category, not a relabeling of NEW APPLICATION.

### Evidence

**1. Volume mismatch (21x):** Pre-restructure approved/NEW APPLICATION averaged 123 records/month. Post-restructure approved/RENEWAL averages 2,620/month — a 21x increase. If RENEWAL replaced NEW APPLICATION, the volumes should be comparable.

**2. NEW APPLICATION volume unchanged in new_application section:** The new_application/NEW APPLICATION rate held steady at ~80-170/month both before and after RENEWAL appeared. If they were the same category, the old one would have dropped.

| Month | new_app/NEW APPLICATION | new_app/RENEWAL |
|---|---|---|
| 2025-01 | 142 | 0 |
| 2025-02 | 164 | 0 |
| 2025-03 | 173 | 44 ← RENEWAL first appears |
| 2025-04 | 99 | 1,003 |
| 2025-05 | 108 | 1,356 |
| ... | ~80-150/month | ~1,200-7,600/month |

**3. Total approved volume 10x:** Pre-restructure: 286 approved records/month. Post-restructure: 2,768/month. RENEWAL is a massive new data feed, not a reshuffling.

**4. License-level inspection (17 cross-type candidates, 6 distinct licenses outside bulk dump):** Every case shows NEW APPLICATION and RENEWAL as **separate sequential events** on the same license, not the same event under different names:
- Different business names on the same license (e.g., license 080610: "TAQUERIA EL PATRON AJ LLC" filed NEW APPLICATION, then "DON TACO" — the existing holder — filed RENEWAL)
- RENEWAL filed days after NEW APPLICATION as an independent new_application record
- NEW APPLICATION has no approved match; the RENEWAL is the approval of the *RENEWAL filing*, not the NEW APPLICATION

**5. Pre-restructure RENEWAL count: zero.** No RENEWAL records exist before March 30, 2025 in either section. Renewals were simply not published on the WSLCB licensing activity page prior to the restructure.

### Conclusion

RENEWAL represents license renewals — a routine process for existing licensees that was not publicly reported before the March 2025 restructure. NEW APPLICATION represents initial applications for new licenses (or re-applications after discontinuance). They are fundamentally different licensing actions and should **not** be cross-matched.

The approved/NEW APPLICATION gap remains a genuine loss of data visibility: the WSLCB is no longer publishing when NEW APPLICATION filings are approved. The linking engine should match new_application/NEW APPLICATION → approved/NEW APPLICATION (same type) and treat post-May-2025 NEW APPLICATION records as having no approval data available.

## DISC. LIQUOR SALES → Discontinued Linking

Investigated whether new_application/DISC. LIQUOR SALES records can be linked to corresponding discontinued/DISCONTINUED records — the discontinuance equivalent of linking new_application → approved.

### Finding: Yes — near-perfect 1:1 correspondence

DISC. LIQUOR SALES is a **notification of intent to discontinue** that appears in the new_application section, paralleling how NEW APPLICATION is a notification of intent to obtain a new license. The discontinued/DISCONTINUED record is the **official outcome**.

| Metric | Value |
|---|---|
| Total DISC. LIQUOR SALES records | 825 |
| Forward matches (has a discontinued record within ±7d) | 815 (98.8%) |
| Mutual matches (bidirectional) | 814 (98.7%) |
| Forward-only (ambiguous) | 1 |
| No match (too recent or withdrawn) | 10 |

### Date gap pattern

Nearly identical to the approved date pattern:

| Gap (discontinued_date − filing_date) | Count |
|---|---|
| −2 days | 1 |
| −1 day | 533 (65%) |
| 0 (same day) | 245 (30%) |
| +10 to +41 days | 36 (4%) |

The discontinued date **precedes** the DISC. LIQUOR SALES notification date by 1 day in 65% of cases — the same weekday-offset pattern seen with approved dates preceding notification dates.

### Key observations

1. **DISC. LIQUOR SALES only exists post-restructure** (first record: 2025-04-30). Before the March 2025 restructure, discontinuances appeared directly in the discontinued section with no prior notification in the new_application section.

2. **Misnomer: covers all license types**, not just liquor. Cannabis licenses (CANNABIS PROCESSOR, CANNABIS RETAILER) also appear with this application type.

3. **10 unmatched records**: 4 are from 2026-02-27 (too recent — discontinued record hasn't appeared yet), 5 from October 2025 (likely withdrawn requests), 1 is a duplicate from the Sept 11 bulk dump.

4. **Not all discontinued records have a DISC. LIQUOR SALES notification**: of 2,084 post-restructure discontinued records, only 809 (39%) have a matching DISC. LIQUOR SALES filing. The remaining 1,275 appear directly in the discontinued section with no prior notification.

5. **Different from lifecycle linking**: the DISC. LIQUOR SALES → discontinued link tracks a specific discontinuance request and its outcome. Separately, 1,170 discontinued records have a prior NEW APPLICATION filing for the same license — but that's lifecycle tracking (application → eventual discontinuance), not a request→outcome pair.

### Implementation impact

This is a natural extension of the record_links table:
- Same bidirectional matching algorithm, same ±7-day tolerance
- `new_app_id` → DISC. LIQUOR SALES record, `approved_id` column repurposed (or renamed to `outcome_id`) → discontinued record
- Status display: "DISCONTINUED on DATE (N days after filing)" paralleling "APPROVED on DATE"
- 98.7% match rate means almost every DISC. LIQUOR SALES record gets a link

## Verification

### Live source page (fetched 2026-02-27)

Approved section application types:
```
RENEWAL: 3,046
ASSUMPTION: 129
CHANGE OF CORPORATE OFFICER: 62
CHANGE OF LOCATION: 25
ADDED/CHANGE OF CLASS/IN LIEU: 21
RESUME BUSINESS: 3
```

No NEW APPLICATION, no IN LIEU, no ADDED FEES, no SE PLAN REFUND.

### Wayback Machine corroboration

| Snapshot Date | NEW APPLICATION in Approved | Error Banner | RENEWAL in Approved |
|---|---|---|---|
| Jan 26, 2025 | Yes (normal) | No | No |
| May 24, 2025 | Yes (86 records, aging out) | Yes | Yes (2,467) |
| Oct 2, 2025 | No | Yes | Yes (10,722) |
| Feb 13, 2026 | No | Yes | Yes |
