# New Application → Approved Record Linking: Analysis & Plan

*Date: 2026-02-27 · Updated 2026-02-27 (date-tolerance correction)*

## Executive Summary

We analyzed 42,092 "New Application" records and 42,373 "Approved" records in the WSLCB licensing tracker to determine how many new applications can be reliably linked to a corresponding approval. Using a **bidirectional nearest-neighbor matching** algorithm with a **±7-day tolerance window**, we can confidently link **3,170 NEW APPLICATION → Approved pairs** (46% of NEW APPLICATION records). Across all application types, **33,606 pairs** (80% of new_app records) can be linked with high confidence.

The remaining ~20% are either genuinely pending, predate our data collection, or represent applications that were denied/withdrawn/abandoned.

---

## 1. Data Landscape

### Dataset Size

| Section | Records | Date Range |
|---|---|---|
| New Application | 42,092 | 2022-08-08 – 2026-02-27 |
| Approved | 42,373 | 2022-08-08 – 2026-02-26 |
| Discontinued | 5,194 | 2022-08-08 – 2026-02-26 |

### New Application Records by Application Type

| Application Type | New App Records | Approved Records |
|---|---|---|
| RENEWAL | 28,677 | 31,440 |
| NEW APPLICATION | 6,887 | 4,026 |
| ASSUMPTION | 2,998 | 2,725 |
| ADDED/CHANGE OF CLASS/IN LIEU | 1,059 | 1,133 |
| DISC. LIQUOR SALES | 825 | 3 |
| ADDED/CHANGE OF TRADENAME | 703 | 220 |
| CHANGE OF LOCATION | 681 | 572 |
| CHANGE OF CORPORATE OFFICER | 233 | 1,218 |
| RESUME BUSINESS | 28 | 31 |

### Key Data Limitation

The latest **approved "NEW APPLICATION"** record is from **2025-05-12** — over 9 months ago. This means any new application filed after roughly February 2025 has no chance of matching an approved record in our data, regardless of whether it was actually approved. This significantly inflates the "unmatched" count.

---

## 2. Matching Strategy

### Why Simple Matching Fails

A license number can appear in the new_application section **multiple times** — for example, a new license application that was denied, then re-applied for; or a license that goes through multiple application types over its lifetime. This means:

- **Closest-approved-after-new-app** (forward matching) creates contention: 655 approved records are "claimed" by multiple new applications.
- **Latest-new-app-before-approval** (backward matching) creates the reverse: 151 new applications are claimed by multiple approvals.
- **Ordinal pairing** (1st new → 1st approved, 2nd → 2nd, etc.) breaks when cycles span the edges of our data window.

### Bidirectional Nearest-Neighbor Matching (Recommended)

The algorithm that produces the cleanest results:

1. **Forward pass**: For each new_application record, find the **earliest** approved record for the same license_number + application_type with `approved_date >= new_app_date - 7 days`.
2. **Backward pass**: For each approved record, find the **latest** new_application record for the same license_number + application_type with `new_app_date <= approved_date + 7 days`.
3. **Mutual match**: Only link pairs where **both passes agree** — the new_app's best approved match points back to that same new_app.

This eliminates contested matches and produces **clean 1:1 pairs** with high confidence.

#### Why ±7 days, not strict date ordering?

The WSLCB source page uses different date semantics per section: `Notification Date` for new applications vs `Approved Date` for approvals. These are different events — a license can be approved *before* its notification date is published. In practice this manifests as:

- **−1 day** (the most common negative gap): approval on a weekday, notification published the next business day. Example: FRED MEYER #351 approved 2025-12-01, notification 2025-12-02.
- **−3 days**: Friday approval → Monday notification (weekend offset). Example: SUMMIT INN approved 2025-09-06 (Sat), notification 2025-09-09 (Tue).
- **−7 to −12 days**: less common, but same business name and license number confirm same event.

Beyond ±30 days, pairs are overwhelmingly different application cycles (often years apart, sometimes with different business names). The 7-day window captures 99%+ of the genuine same-event negative-gap pairs while avoiding false matches from different cycles.

A 30-day tolerance was also tested; it gains only ~225 additional RENEWAL matches (27,388 → 27,613) with marginal improvement elsewhere, so the 7-day window is the recommended conservative default.

### Results by Application Type

| Application Type | Total New Apps | Mutual Matches | Forward-Only (Ambiguous) | No Match | Match Rate |
|---|---|---|---|---|---|
| RENEWAL | 28,677 | 27,388 | 1,113 | 176 | 95.5% |
| NEW APPLICATION | 6,887 | 3,170 | 944 | 2,773 | 46.0% |
| ASSUMPTION | 2,998 | 2,159 | 346 | 493 | 72.0% |
| ADDED/CHANGE OF CLASS | 1,059 | 489 | 54 | 516 | 46.2% |
| CHANGE OF LOCATION | 681 | 157 | 45 | 479 | 23.1% |
| CHANGE OF CORP OFFICER | 233 | 214 | 8 | 11 | 91.8% |
| RESUME BUSINESS | 28 | 25 | 1 | 2 | 89.3% |
| **Total** | **42,092** | **33,606** | **2,512** | **4,453** | **79.8%** |

*(DISC. LIQUOR SALES and ADDED/CHANGE OF TRADENAME excluded — these types almost never appear as matched pairs in the approved section.)*

The ±7-day tolerance dramatically improves match rates for types where same-day or next-day approval-before-notification is common: RENEWAL jumped from 83.3% to 95.5%, CHANGE OF CORPORATE OFFICER from 26.6% to 91.8%, and ASSUMPTION from 59.3% to 72.0%.

### Time-to-Approval Distribution (Mutual Matches, NEW APPLICATION Only)

| Days to Approval | Count | Percentage |
|---|---|---|
| −3 to −1 days (approved before notification) | 41 | 1.3% |
| Same day | 9 | 0.3% |
| 1–30 days | 730 | 23.0% |
| 31–60 days | 1,307 | 41.2% |
| 61–90 days | 692 | 21.8% |
| 91–180 days | 367 | 11.6% |
| 181–365 days | 22 | 0.7% |
| 365+ days | 2 | 0.1% |

**Median time to approval: ~50 days.** 87% of approvals occur within 90 days. 1.3% of matched pairs have the approval date *before* the notification date (weekday offsets).

### Full Outcome Breakdown (NEW APPLICATION Only)

| Outcome | Count | Percentage |
|---|---|---|
| ✅ Approved (mutual match) | 3,170 | 46.0% |
| ⚠️ Approved (ambiguous/forward-only match) | 944 | 13.7% |
| ⏳ Pending (filed after Feb 2025) | 1,712 | 24.9% |
| ❓ No outcome known (old) | 926 | 13.4% |
| 🚫 Not approved + later discontinued | 135 | 2.0% |

The 13.4% "no outcome known" likely represents:
- Applications denied or withdrawn (WSLCB doesn't publish denials)
- Applications that predate our data window (we started collecting in Aug 2022) where the approval was before our first scrape
- Approvals that fell in a gap between scrapes (the 30-day rolling window)

---

## 3. Implementation Plan: Record Linking

### Schema Changes

#### New junction table: `record_links`

```sql
CREATE TABLE IF NOT EXISTS record_links (
    id INTEGER PRIMARY KEY,
    new_app_id INTEGER NOT NULL REFERENCES license_records(id) ON DELETE CASCADE,
    approved_id INTEGER NOT NULL REFERENCES license_records(id) ON DELETE CASCADE,
    confidence TEXT NOT NULL CHECK (confidence IN ('high', 'medium', 'low')),
    days_gap INTEGER,  -- approved_date - new_app_date in days
    linked_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(new_app_id, approved_id)
);
CREATE INDEX idx_record_links_new ON record_links(new_app_id);
CREATE INDEX idx_record_links_approved ON record_links(approved_id);
```

**Confidence levels:**
- `high` — Bidirectional mutual match (both sides agree)
- `medium` — Forward-only match (new_app → approved, but the approved record has a closer new_app). Used when the contested new_app is the only one without an alternative match
- `low` — Heuristic match (e.g., same license_number, different application_type, within time window)

#### Why a junction table instead of a direct FK?

- A new_application might eventually match multiple approved records (e.g., partial approvals, or if the license goes through multiple approval cycles)
- Allows storing metadata (confidence, days_gap) about the link
- Can be rebuilt from scratch without modifying the core `license_records` table
- Supports future linking of discontinued records too

### Linking Algorithm (Python)

```python
# Tolerance window in days — accounts for approval dates that precede
# notification dates within the same 30-day source window. The WSLCB
# source uses "Notification Date" for new applications and "Approved Date"
# for approvals; these are different events and the approval can occur
# before the notification is published (weekday offsets, batch processing).
DATE_TOLERANCE_DAYS = 7

def link_records(conn):
    """Build new_application → approved links using bidirectional matching."""
    # Phase 1: Mutual matches (high confidence)
    conn.execute("""
        INSERT OR IGNORE INTO record_links (new_app_id, approved_id, confidence, days_gap)
        SELECT f.new_id, f.approved_id, 'high',
               CAST(julianday(a.record_date) - julianday(n.record_date) AS INTEGER)
        FROM (
            -- Forward: each new_app → earliest approved within tolerance
            SELECT n.id AS new_id,
                   (SELECT a.id FROM license_records a
                    WHERE a.section_type='approved'
                      AND a.license_number = n.license_number
                      AND a.application_type = n.application_type
                      AND julianday(a.record_date) >= julianday(n.record_date) - :tol
                    ORDER BY a.record_date LIMIT 1) AS approved_id
            FROM license_records n
            WHERE n.section_type='new_application'
        ) f
        JOIN (
            -- Backward: each approved → latest new_app within tolerance
            SELECT a.id AS approved_id,
                   (SELECT n.id FROM license_records n
                    WHERE n.section_type='new_application'
                      AND n.license_number = a.license_number
                      AND n.application_type = a.application_type
                      AND julianday(n.record_date) <= julianday(a.record_date) + :tol
                    ORDER BY n.record_date DESC LIMIT 1) AS new_id
            FROM license_records a
            WHERE a.section_type='approved'
        ) b ON f.new_id = b.new_id AND f.approved_id = b.approved_id
        JOIN license_records n ON n.id = f.new_id
        JOIN license_records a ON a.id = f.approved_id
        WHERE f.approved_id IS NOT NULL
    """, {"tol": DATE_TOLERANCE_DAYS})
    
    # Phase 2: Remaining forward-only matches (medium confidence)
    # Only for new_apps that don't yet have a high-confidence link
    # and the approved record doesn't have a high-confidence link either
    ...
```

### When to Run Linking

- **On scrape**: After inserting new records, run incremental linking for any new/updated records
- **On startup**: `backfill_links()` runs the full algorithm once (idempotent, skips existing links)
- **CLI**: `python scraper.py --rebuild-links` for full rebuild

### Migration Path

1. Add `record_links` table in `database.py` `init_db()`
2. Add `link_records.py` module with the matching algorithm
3. Add backfill call in `app.py` lifespan (after `backfill_entities()`)
4. Add incremental linking in `scraper.py` after record insertion
5. Add `get_record_links()` to `queries.py`
6. Update templates

---

## 4. UI/UX Plan: Communicating Application Status

### Design Principle

New Application records are the most interesting to users because they represent *pending activity* — a business is trying to get licensed. The key question a user has when looking at a new application is: **"Was this approved?"**

We should answer that question immediately and prominently.

### 4.1 Record Table (Search Results, Entity Pages)

#### Status Indicator Column

Add an **"Outcome" column** to the record table that shows the disposition of each new_application record:

| Icon | Meaning | Style |
|---|---|---|
| ✅ Approved | Linked to an approved record | Green text, clickable link to the approved record |
| ⏳ Pending | Filed recently (< 180 days), no approved record yet | Amber/yellow text |
| ❓ Unknown | Old record with no matched approval or discontinuation | Gray text |
| 🚫 Discontinued | License was discontinued (linked to discontinued record) | Red text |

This column only appears for `new_application` records. For `approved` and `discontinued` records, the column is blank or hidden.

**Implementation:**
- Add `approval_status` and `approved_record_id` fields to the hydrated record dict in `queries.py`
- Compute from `record_links` table with a LEFT JOIN
- The ✅ is a link: clicking it navigates to the approved record's detail page

#### Subtle Row Styling

For new_application records in the table:
- **Approved**: Very faint green left border (`border-l-2 border-green-300`)
- **Pending**: Very faint amber left border (`border-l-2 border-amber-300`)
- **Discontinued**: Very faint red left border (`border-l-2 border-red-300`)
- **Unknown**: No special styling

This provides a quick visual scan without overwhelming the existing design.

### 4.2 Record Detail Page

#### Approval Status Banner

At the top of a new_application detail page, between the header and the detail fields, show a status banner:

**If approved:**
```
┌─────────────────────────────────────────────────────┐
│ ✅ APPROVED on 2025-03-15 (47 days after application)│
│    → View approved record                           │
└─────────────────────────────────────────────────────┘
```
Style: `bg-green-50 border border-green-200 text-green-800`

**If pending (recent):**
```
┌──────────────────────────────────────────────────────┐
│ ⏳ PENDING — Filed 45 days ago, no approval yet      │
│    Typical approval takes 50–90 days                 │
└──────────────────────────────────────────────────────┘
```
Style: `bg-amber-50 border border-amber-200 text-amber-800`

**If old with no outcome:**
```
┌──────────────────────────────────────────────────────┐
│ ❓ NO OUTCOME RECORDED — Filed 2+ years ago           │
│    May have been denied, withdrawn, or approved       │
│    before our tracking began                          │
└──────────────────────────────────────────────────────┘
```
Style: `bg-gray-50 border border-gray-200 text-gray-600`

#### Bidirectional Cross-Links in Related Records

In the "Other records for license #XXXXXX" section:
- If the current record is a new_application and has a linked approved record, highlight that row with a green indicator: `→ This is the approval for this application`
- If the current record is an approved record and has a linked new_application, highlight that row: `→ This is the original application`

### 4.3 Dashboard

#### Application Pipeline Stats

Add a new card row to the dashboard showing the pipeline:

```
┌────────────────┐ ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
│  ✅ Approved    │ │  ⏳ Pending     │ │  ❓ Unknown    │ │  🚫 Discontinued│
│     3,170      │ │     1,712      │ │       926      │ │       135      │
│   (46% of      │ │  (25% of       │ │   (13%)        │ │    (2%)        │
│  new apps)     │ │  new apps)     │ │                │ │                │
└────────────────┘ └────────────────┘ └────────────────┘ └────────────────┘
```

These numbers are for `application_type = 'NEW APPLICATION'` only — the most meaningful subset.

### 4.4 Search Filters

Add an **"Approval Status"** filter dropdown to the search page:

```
Approval Status: [All ▾]
  - All
  - ✅ Approved
  - ⏳ Pending  
  - ❓ No Outcome
  - 🚫 Discontinued
```

This filter only applies when `section_type = 'new_application'`. If another section type is selected, the filter is hidden (or grayed out).

**Implementation:**
- Add `approval_status` query parameter to `search_records()`
- Filter via LEFT JOIN on `record_links` + age calculation

### 4.5 CSV Export

Add columns to the CSV export:
- `approval_status` — `approved`, `pending`, `unknown`, `discontinued`
- `approved_date` — date of the linked approved record (if any)
- `days_to_approval` — integer days between application and approval

---

## 5. Implementation Phases

### Phase 1: Schema + Linking Engine
- Add `record_links` table to `database.py`
- Create `link_records.py` with the bidirectional matching algorithm
- Backfill all existing records
- Add incremental linking to `scraper.py`

### Phase 2: Query Layer + Detail Page
- Add `get_record_links()` and `get_approval_status()` to `queries.py`
- Update `hydrate_records()` to include approval status
- Add status banner to `detail.html`
- Highlight linked records in the "Related records" section

### Phase 3: Record Table + Search
- Add Outcome column to `record_table.html`
- Add left-border row styling
- Add "Approval Status" filter to search page
- Update `search_records()` to support the filter

### Phase 4: Dashboard + Export
- Add pipeline stats to dashboard
- Add approval columns to CSV export

---

## 6. Risks & Open Questions

1. **Data gap**: No approved "NEW APPLICATION" records after 2025-05-12. Until we understand why (WSLCB page change? Scraper bug?), many recent applications will show as "Pending" indefinitely. **→ Investigate the approved data gap as a prerequisite.**

2. **False matches**: The bidirectional algorithm is conservative, but a license that is applied for, denied, then re-applied could still produce a match between application #1 and approval of application #2. The `days_gap` field helps flag these (>180 days is suspicious).

3. **DISC. LIQUOR SALES**: These appear in new_application but almost never in approved (only 3 records). They seem to be a different workflow. Exclude from matching.

4. **CHANGE OF CORPORATE OFFICER**: More approved records (1,218) than new_application records (233). The approved section likely captures these even when they don't go through the notification period. With the ±7-day tolerance, match rate is 91.8% (up from 26.6% with strict date ordering — these are overwhelmingly same-day or next-day pairs).

5. **Forward-only matches (2,512 records)**: These are ambiguous — the new_app points to an approved record, but that approved record has a closer new_app. Consider:
   - Display with "medium" confidence badge
   - Or exclude from display entirely (only show high-confidence links)

6. **"Pending" cutoff**: What age threshold separates "pending" from "probably denied"? Analysis shows 87% of approvals occur within 90 days and 99% within 180 days. A 180-day cutoff for "pending" seems reasonable.
