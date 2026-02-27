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
