# Data Quality Audit — 2026-02-26

Post-historical-backfill audit of the WSLCB Licensing Tracker database.

**Database (at time of audit):** 89,157 records | 57,137 locations | 59,159 entities  
**Database (post-fixes):** 89,156 records | 57,137 locations | 59,044 entities  
**Date range:** 2022-08-08 to 2026-02-25  
**Data sources:** Live scrapes (2025-11 to present) + unified-diff backfill (2022-08 to 2025-12)

---

## 1. Unvalidated Addresses (14 locations)

These 14 locations failed the address validation API. All have correct regex-parsed fields (except #32366) and can be used as-is with the parsed city/state/zip; they just lack USPS-standardized components.

| ID | raw_address | parsed_city | parsed_zip | Notes |
|---|---|---|---|---|
| 32364 | 200 QUEEN ANNE AVE N, SEATTLE, WA 98109 | SEATTLE | 98109 | |
| 32365 | 600 COOPER POINT RD SW, OLYMPIA, WA 98502 | OLYMPIA | 98502 | |
| 32366 | 125 W CHANCE A LA MER NW, WHALERS PLAZA OCEAN SHORES, WA 98569 | *(empty)* | *(empty)* | Regex also failed — "WHALERS PLAZA" confused the parser |
| 32367 | 1 FRONT ST, FRIDAY HARBOR, WA 98250 | FRIDAY HARBOR | 98250 | |
| 32368 | 4741 N BALTIMORE ST, TACOMA, WA 98407 | TACOMA | 98407 | |
| 32369 | 1 FRONT N ST STE E3, FRIDAY HARBOR, WA 98250 | FRIDAY HARBOR | 98250 | |
| 32370 | 25207 104TH AVE SE, KENT, WA 98031 | KENT | 98031 | |
| 32371 | 101 W UMPTANUM RD, ELLENSBURG, WA 98926 | ELLENSBURG | 98926 | |
| 32372 | 57662 US-155, ELECTRIC CITY, WA 99123 | ELECTRIC CITY | 99123 | Highway address |
| 32373 | 8402 EVERGREEN WAY, EVERETT, WA 98208 | EVERETT | 98208 | |
| 32374 | 57662 HWY 155 N, ELECTRIC CITY, WA 98123 | ELECTRIC CITY | 98123 | Wrong ZIP? (99123 vs 98123) |
| 32375 | 740 228TH AVE NE, SAMMAMISH, WA 98074 | SAMMAMISH | 98074 | |
| 32376 | 352 GRIFFIN AVE, ENUMCLAW, WA 98022 | ENUMCLAW | 98022 | |
| 32377 | 18010 E VALLEY HWY, KENT, WA 98032 | KENT | 98032 | |

**Recommendation:** These are likely API-side limitations (unfamiliar street names, highway addresses). The parsed fields are usable for 13 of 14. No action needed unless the API is updated.

---

## 2. City Parsing Failures (3,009 locations)

**3,009 locations** (5.3%) have empty `city` from the regex parser but correct `std_city` from the API. The dominant pattern is addresses where suite/unit/building designators appear between the street and city *without* a comma separator:

- `9401 AURORA AVE N, BLDG 1, LOWR LEVEL, UNIT  SEATTLE, WA 98103` → parsed city: empty, std_city: SEATTLE
- `530 RONLEE LN NW STE B, STE B OLYMPIA, WA 98502` → parsed city: empty, std_city: OLYMPIA
- `7430 164TH AVE NE, B225 REDMOND, WA 98052` → parsed city: empty, std_city: REDMOND

Additional **318 locations** have a non-empty but *wrong* parsed city (suite info absorbed into the city field), e.g.:
- `STE A TACOMA` instead of `TACOMA`
- `STE B MOUNT VERNON` instead of `MOUNT VERNON`
- `PUGET PARK SHOP. CTR. EVERETT` instead of `EVERETT`

**Impact:** The `display_city` template helper falls back correctly: `std_city || city`, so the web UI shows the right city for validated records. The issue only affects the 14 unvalidated locations above and any future records before validation.

**Recommendation:** Known limitation per AGENTS.md (~6% miss rate). The address validator handles these correctly. No code change needed.

---

## 3. Endorsement Inconsistencies

### 3a. Unmapped Numeric Codes (7 codes, 102 records)

These endorsement codes have no text description and couldn't be resolved by cross-referencing:

| Code | Records | Notes |
|---|---|---|
| 0 | 58 | Likely a null/placeholder code in the source |
| 346 | 12 | |
| 331 | 10 | Distinct from `331, MANUFACTURER - LIQUOR` (2 records) |
| 386 | 9 | Distinct from `386, SE CANNABIS RETAILER` (12 records) |
| 376 | 6 | |
| 358 | 5 | |
| 99 | 2 | |

### 3b. Naming Inconsistencies (same code, different name text)

The WSLCB has renamed endorsements over time, creating multiple `license_endorsements` rows for the same semantic endorsement:

| Code | Variant Names | Records |
|---|---|---|
| 379 | `TAKE OUT/DELIVERY ENDORSEMENT`, `TAKEOUT/DELIVERY`, `Curbside/Delivery Endorsement`, `CURBSIDE/DELIVERY ENDORSEMENT` | 369 + 165 + 282 + 8 |
| 380 | `PREMIXED COCKTAILS/WINE TO-GO`, `Cocktails/Wine To-Go` | 122 + 61 |
| 381 | `GROWLERS TAKE OUT/DELIVERY`, `GROWLERS TAKEOUT/DELIVERY`, `Growlers Curbside/Delivery` | 21 + 4 + 17 |
| 386 | `SE CANNABIS RETAILER`, `SE Cannabis Retailer`, *(bare code)* | 12 + 1 + 9 |
| 327 | `DOMESTIC WINERY`, `DOMESTIC WINERY < 250,000 LITERS` | 370 + 50 |

**Recommendation:** Consider an endorsement alias/merge table that maps variant names to a canonical name for filtering and display. This would consolidate the 4 variants of code 379 into a single filter option. The bare numeric codes (3a) could potentially be resolved by searching for the same license numbers in the new_application section where text names are used.

---

## 4. Entity Classification Issues

### 4a. Type Distribution

| Type | Count |
|---|---|
| person | 58,277 (98.5%) |
| organization | 882 (1.5%) |

### 4b. Misclassified Entities

**Organizations classified as `person`** (~30 clear cases found in sampling):
- `FOSTER GARVEY` (law firm, 129 records)
- `ALASKAN BREWING & BOTTLING CO.` 
- `GROTH VINEYARDS & WINERY`
- `E. & J. GALLO WINERY`
- `HERBS BAR AND GRILL`
- `ARAMARK SPORTS AND ENTERTAINME`
- `LOS PINOS MEXICAN  RESTAURANT`
- `SIMPLY SOULFUL CAFE AND ESPRES`
- `HAAN TANDOOR & BAR`
- Plus ~20 more business names that lack the heuristic triggers (LLC, INC, etc.)

**Persons classified as `organization`** (fewer issues — the org heuristic has high precision):
- All 882 organizations checked out as legitimate; the heuristic catches `LLC`, `INC`, `CORP`, `TRUST`, `LP`, `LTD`, etc.

### 4c. Junk/Fragment Entities (11 entries)

| ID | Name | Records | Root Cause |
|---|---|---|---|
| 21868 | `-035` | 2 | WAC reference fragment (`PER WAC 314-55-035`) split on semicolon |
| 26884 | `035` | 6 | Same — WAC fragment (`314.07; 035`) split on semicolon |
| 23353 | `(PER WAC 314-07-035)` | 6 | WAC parenthetical stored as entity |
| 18792 | `(ESTATE PROJECT)` | 1 | Parenthetical note stored as entity |
| 26476 | `........COMMERCIAL KITCHEN, & GALLERY L.L` | 1 | Garbled source data with leading dots |
| 56684 | `AMANDA MCLEES\`` | 1 | Trailing backtick character |
| 27632 | `5` | 1 | Truncation artifact (from `THORNTONS LLC` applicants) |
| 58824 | `A` | 1 | Truncation artifact (`CASTLE OAKS VINEYARDS, INC.; A`) |
| 30693 | `IL` | 5 | Source data issue (appears in NEW LUCK TOY applicants as literal `IL`) |
| 24142 | `TY` | 2 | Truncated first name (`TY; HALEY` → later corrected to `PENDERGRAFT, TY`) |
| 18922 | `LLC` | 2 | Bare `LLC` fragment from malformed applicants string |

**Root cause:** The WSLCB source sometimes includes WAC regulatory references, parenthetical notes, or garbled text within the semicolon-delimited applicants field. The entity extraction splits on semicolons and stores each segment as an entity without validation.

**Recommendation:** Add a filter in `parse_and_link_entities()` to skip segments that:
- Match a WAC reference pattern (`PER WAC ...`, bare numeric fragments like `035`)
- Are enclosed in parentheses (`(...)`) 
- Are ≤ 2 characters long
- Start with punctuation (`.`, `-`)
- Are known non-name tokens (`LLC`, `INC`, etc. when standalone)

### 4d. Duplicate Entities from Double Spaces (107 pairs) — ✅ RESOLVED

The WSLCB source uses inconsistent spacing — the same person appears as both `SMITH, JOHN  MICHAEL` (double space) and `SMITH, JOHN MICHAEL` (single space), creating two separate entity rows. **107 confirmed duplicate pairs** existed at the time of audit.

Additionally, **4,821 entities** had double spaces in their names (most didn't have a single-space twin, but the inconsistency made searching harder).

Examples:
- `ACKLEY, BRANDON  J` (id 27038) ↔ `ACKLEY, BRANDON J` (id 19645)
- `BLEDSOE, DREW  MCQUEEN` (id 21106) ↔ `BLEDSOE, DREW MCQUEEN` (id 19633)
- `CASTRO,  ROXANA` (id 17984) ↔ `CASTRO, ROXANA` (id 17982)

**Fix applied** (`09d026a`): Added `re.sub(r'\s+', ' ', cleaned)` to `_clean_entity_name()` in `entities.py` to collapse runs of whitespace to a single space. On service restart, the existing `merge_duplicate_entities()` startup hook automatically:
- Renamed 4,713 entities with double spaces
- Merged 115 duplicate pairs into their canonical single-space form
- Cleaned all double spaces in `business_name`, `applicants`, `previous_business_name`, `previous_applicants` columns in `license_records`
- Net result: 59,159 → 59,044 entities. Zero double-space names remain.

Future records are also protected — any new names with extra whitespace are normalized at ingest time.

---

## 5. Source Data Truncation (WSLCB limitation)

The WSLCB source HTML truncates field values at fixed character limits:

- **Applicant names:** Truncated at **30 characters** (sharp cliff: 297 entities at 30 chars, only 22 at 31) and **45 characters** (21 entities at 45 chars, 0 at 46+)
- **Business names:** Not truncated (max 78 chars)

Truncated examples:
- `SOUTHERN WINE & SPIRITS PACIFIC NORTHWEST LLC` (45) — likely complete
- `C NORTHWEST HOLDINGS LLC SOUTHERN WINE & SPIR` (45) — truncated
- `TRUSTE DATED MAY 23 2006 THE DEBRA MATHY REVO` (45) — truncated
- `ARAMARK SPORTS AND ENTERTAINME` (30) — truncated

The 30-char limit affects individual applicant segments within the semicolon-delimited `applicants` field. The 45-char limit affects the applicants field after joining (some entries combine multiple people).

**Recommendation:** Not fixable — this is a source data limitation. Document in AGENTS.md as a known issue.

---

## 6. Missing / Empty Fields

| Field | Empty/Null Count | % of Total | Notes |
|---|---|---|---|
| `business_name` | 125 | 0.14% | 64 discontinued, 38 approved renewals, 23 other |
| `license_type` | 497 | 0.56% | 360 approved, 137 new_application; scattered dates |
| `location_id` | 92 | 0.10% | 87 approved, 1 discontinued, 4 new_application |
| `applicants` (new_application only) | 730 | 1.7% of new_apps | Expected for some record types |
| `contact_phone` | 35,302 | 39.6% | Normal — approved/discontinued sections often omit phone |
| `previous_business_name` (ASSUMPTION) | 2,838 | 49.7% of ASSUMPTIONs | Expected: 2,718 are approved (source never provides seller info for approved), 120 are new_application (source data issue) |

**Recommendation:** The 125 empty business names and 497 empty license types are minor source data gaps. The 120 new_application ASSUMPTIONs missing `previous_business_name` may be recoverable from archived snapshots if available for those dates.

---

## 7. Record with Malformed application_type — ✅ RESOLVED

One record had a license number (`434776`) in the `application_type` field:

| ID | Date | Business | License# | application_type | license_type |
|---|---|---|---|---|---|
| 32020 | 2024-09-25 | G & T ADULT DAYCARE BAR & GRILL | 433344 | 434776 | 424, SPIRITS/BR/WN REST LOUNGE + |

**Root cause:** Diff boundary chimera from the supplemental (with-context) parsing pass. The diff file `2024_10_04-07_16_42-approvals-diff.txt` splits the G & T ADULT DAYCARE record across two hunks. When the supplemental pass reassembles using context lines, it creates a chimera that combines G & T's business name and license type with JACQUOT FARM AND VINEYARD's license number (`433344`), phone, and application type.

The correct G & T record already existed as **#32016** (parsed from the earlier `2024_09_26` diff). Record #32020 was a corrupt duplicate with shifted fields.

**Fix applied** (`125906a`): Deleted record #32020 and its `record_endorsements` link. The correct record #32016 remains. This was the only chimera in the database (confirmed by scanning for 6-digit numeric `application_type` values). This is a known limitation of the supplemental context-line pass — it can produce valid-looking records from fields belonging to adjacent records at hunk boundaries.

---

## 8. Non-ASSUMPTION Records with Previous Applicant Data (43 records)

43 `NEW APPLICATION` and `CHANGE OF LOCATION` records have `previous_applicants` and/or `previous_business_name` populated. These are records where a new applicant is taking over a previously-licensed location — the WSLCB source provides the prior licensee's info even though the application type isn't `ASSUMPTION`.

This appears to be legitimate source data, not a parsing error. No action needed.

---

## 9. Phone Number Quality

| Category | Count | Notes |
|---|---|---|
| Standard format (`NNN-NNN-NNNN`) | ~708 | Minority of records |
| 10 digits, no separators | ~53,583 | Most common format |
| All zeros (`0000000000`) | 229 | Placeholder |
| Partial/truncated | 34 | e.g., `(206) 349-` |
| Empty | 35,302 | Normal for approved/discontinued |

**Recommendation:** Low priority. The `phone_format` Jinja2 filter already handles display formatting. The 229 all-zeros entries could be filtered in display (show as empty instead of `000-000-0000`).

---

## 10. Volume Spike (2025-04 onward)

Monthly record volumes jump dramatically starting April 2025:

| Period | Monthly avg (new_app + approved) | Dominant type |
|---|---|---|
| 2022-08 to 2025-03 | ~600 | Mixed |
| 2025-04 to 2025-07 | ~3,400 | RENEWAL (70-80%) |
| 2025-08 to 2025-09 | ~16,000 | RENEWAL (90%+) |
| 2025-10 to 2026-02 | ~4,500 | RENEWAL (80%) |

Peak: September 2025 with 17,356 renewals alone. This is a bulk renewal cycle — the WSLCB appears to have shifted to mass-processing renewals starting mid-2025. **Not a data quality issue.**

---

## Summary of Recommendations

| # | Issue | Severity | Effort | Status |
|---|---|---|---|---|
| 1 | **Double-space duplicate entities** (107 pairs) | Medium | Low | ✅ Fixed (`09d026a`) — whitespace normalization added to `_clean_entity_name()`; 115 entities merged, 4,713 renamed on restart |
| 2 | **Junk/fragment entities** (11 entries) | Low | Low | Open — add entity name validation filter (skip WAC refs, parens, ≤2 chars) |
| 3 | **Endorsement naming variants** (4 codes with multiple names) | Medium | Medium | [Issue #7](https://github.com/CannObserv/wslcb-licensing-tracker/issues/7) — admin interface with endorsement alias table |
| 4 | **Unmapped endorsement codes** (7 codes, 102 records) | Low | Low | [Issue #7](https://github.com/CannObserv/wslcb-licensing-tracker/issues/7) — admin interface to assign text descriptions |
| 5 | **Misclassified org-as-person entities** (~30) | Low | Medium | Open — expand heuristic to check for business-name patterns (BAR, GRILL, WINERY, BREWERY, RESTAURANT, CAFE, etc.) |
| 6 | **Malformed record #32020** | Low | Trivial | ✅ Fixed (`125906a`) — chimera record deleted; root cause: diff boundary artifact in supplemental context-line pass |
| 7 | **Source truncation at 30/45 chars** | Info | N/A | Open — document in AGENTS.md; not fixable |
| 8 | **All-zeros phone numbers** | Low | Trivial | Open — filter in display template |
| 9 | **14 unvalidated addresses** | Info | N/A | Open — API limitation; parsed fields are usable |
