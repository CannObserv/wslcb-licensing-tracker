# Person vs. Organization Name Classification — Library Comparison

Initial research: 2025-06. Updated: 2026-11. Context: ~67K uppercased name
strings from WSLCB licensing data (66,165 persons, 1,062 organizations).

Current approach: regex checking for org suffixes (LLC, INC, CORP, etc.),
defaulting everything else to "person". This misclassifies orgs that lack
legal suffixes — at least 26 entities currently labeled "person" are clearly
organizations (restaurants, wineries, clubs, etc.).

The 2025-06 evaluation recommended `probablepeople` as the primary classifier
with the regex as a fallback layer. That recommendation was never implemented.
The 2026-11 update tested `probablepeople` against **live WSLCB data** and
uncovered a significant demographic-bias problem not visible in the original
synthetic test set.

---

## 1. `probablepeople` — ⚠️ DEMOTED (was ⭐ RECOMMENDED)

**Verdict: Functional but has a systematic bias against non-Western names.**

| Attribute | Detail |
|---|---|
| Classification? | **Yes** — returns `"Person"` or `"Corporation"` directly |
| ALL-CAPS? | Handles well for Western names; degrades on non-Western |
| Install size | **~2.3 MB** total (probablepeople 905 KB + python-crfsuite 1.2 MB + doublemetaphone 172 KB + probableparsing 3 KB) |
| Model download? | **No** — CRF model is bundled in the wheel |
| License | MIT |
| Maintenance | Semi-maintained — v0.5.6 (Oct 2024) after a 7-year gap; 63 open issues, 5 stale PRs, CI broken (Travis), README links wrong repo |
| Speed | **~11,000 names/sec** |
| Python | ≥3.9 (tested OK on 3.12) |

### Tested against live WSLCB data (2026-11)

**500 persons sampled randomly:**
- 462 (92.4%) correctly classified as Person
- **31 (6.2%) misclassified as Corporation** — false positives
- 7 (1.4%) threw `RepeatedLabelError`

**All 1,062 known organizations:**
- 988 (93.1%) correctly classified as Corporation
- 25 (2.4%) misclassified as Person — trusts, short LLCs, edge cases
- 49 (4.6%) threw `RepeatedLabelError` — mostly names with parenthetical annotations (WAC refs, "FRANCHISOR", etc.)

### The bias problem

The 6.2% false-positive rate on persons is **not random** — it disproportionately
affects non-Western names. Tested manually:

```
Misclassified as Corporation (ALL are real people):
  SUNA HONG                       => Corporation  ✗
  HEEN AH LEE                     => Corporation  ✗
  PO AN LIM                       => Corporation  ✗
  ZHONG LUN LIU                   => Corporation  ✗
  PATIL, DEEPANJALI SURESH        => Corporation  ✗
  EYOEL TEBEBE YOHANNES           => Corporation  ✗
  POLASUB, THISUN                 => Corporation  ✗
  NGUYEN THI                      => Corporation  ✗
  BOUNXONE MAITRICHITH            => Corporation  ✗
  NANTAKORN DACHALERS             => Corporation  ✗
  PHENSIRI CHAICHITTANANOND       => Corporation  ✗
  SREESUNGNOEN, SAWAT             => Corporation  ✗
  JATINDER SINGH JEMMU            => Corporation  ✗
  NABEELA AWAWDED                 => Corporation  ✗

Correctly classified:
  CHANG WEI                       => Person       ✓
  KWON YOO                        => Person       ✓
  PARK SUNG HO                    => Person       ✓
  MOHAMMAD HASSAN ALI             => Person       ✓
  AHMED MOHAMED                   => Person       ✓
  GARCIA MARIA ELENA              => Person       ✓
```

The CRF model was trained primarily on Western name patterns. Names with
unfamiliar phonetics, unusual syllable structure, or fewer recognizable
given-name tokens get classified as Corporation. At WSLCB scale (~66K persons),
this would misclassify an estimated **~4,100 people** — many of them from
Asian, African, and South Asian backgrounds. This is worse than the current
regex, which at least treats all non-suffix-bearing names equally.

### What it does well

- Orgs **without** legal suffixes: FORGE CELLARS, TOTAL WINE & MORE,
  COSTCO WHOLESALE, HILLS RESTAURANT & BAR — correctly classified
- DBA patterns parsed correctly
- "Household" type for couple patterns (TREVOR & JAYNE BLACKWELL)

### Maintenance concerns

- **63 open issues**, 5 PRs unmerged for 6+ years
- Self-classified as "Alpha" since 2014
- README issues link incorrectly points to usaddress repo
- No CHANGELOG, no versioning discipline
- 7-year release gap (2017 → 2024) could recur

---

## 2. `nameparser` (python-nameparser)

**Verdict: Not useful for classification. Parser only, not a classifier.**

| Attribute | Detail |
|---|---|
| Version | 2.5.0 (Oct 2026) — actively maintained |
| Classification? | **No** — always assumes input is a person name |
| ALL-CAPS? | Handles fine |
| Install size | **~138 KB** — pure Python, zero dependencies |
| License | LGPL |
| Python | ≥3.11 |

Parses everything as a human name. No way to distinguish "FORGE CELLARS" from
"JOHN SMITH" in the parsed output. Skip for classification.

---

## 3. `usaddress`

**Verdict: Irrelevant — parses US street addresses, not names.**

Same team as probablepeople (datamade), same CRF approach. Not applicable.

---

## 4. `name-cleaver`

**Verdict: Dead. Last PyPI release 2013, last GitHub push 2017.**

| Attribute | Detail |
|---|---|
| Version | 0.6.0 (2013) |
| Stars | 127, archived: No |
| License | BSD |

Sunlight Foundation project. Has `OrganizationNameCleaver` / `IndividualNameCleaver`
but you must already know the type. Won't import on Python 3.12. **Dead project.**

---

## 5. spaCy NER

**Verdict: Actively bad for this task. 60% accuracy — worse than regex.**

| Attribute | Detail |
|---|---|
| Version | 3.8.16 (Nov 2025) |
| Classification? | Yes — NER labels include PERSON and ORG |
| ALL-CAPS? | **Inconsistent** — unpredictable behavior |
| Install size | **~335 MB** (42 packages: spacy 129 MB + numpy 43 MB + thinc 16 MB + model 15 MB + …) |
| Model download? | Yes — `python -m spacy download en_core_web_sm` (12 MB) |
| License | MIT |
| Python | ≥3.9 |

### Tested against WSLCB-like data (2026-11)

50 representative uppercased names (30 person, 20 org):
- **Person names: 19/30 correct (63%)** — 8 missed entirely, 3 misclassified as ORG
- **Org names: 11/20 correct (55%)** — 7 missed entirely, 2 wrong labels

**Root cause:** NER models are trained on running text ("The CEO of Acme Corp
announced…") and rely on sentence context for classification. Isolated name
strings provide no context. ALL-CAPS input further confuses capitalization-dependent
features. No improvements to this limitation in any 3.8.x release or v4.0 dev.

Not just "overkill" — actively worse than heuristics at 20× the install size.

---

## 6. Hugging Face / GLiNER / flair

**Verdict: Massively overkill. Same isolated-string problem.**

| Library | Install Size | Status |
|---|---|---|
| HuggingFace NER (dslim/bert-base-NER) | **~1+ GB** (torch + model) | Active |
| GLiNER (zero-shot NER) | **~3–5 GB** (torch + transformers + model) | Active (v0.2.29, Sep 2026) |
| flair | **~500+ MB** | Stale on PyPI (last release 2021) |

All share the fundamental problem: designed for entity extraction from prose,
not classification of isolated strings. The install footprint is absurd for a
task solvable with a regex.

---

## 7. `names-dataset`

**Verdict: Possible supplementary signal, heavy RAM cost.**

| Attribute | Detail |
|---|---|
| Version | 3.3.1 (Apr 2025) |
| Classification? | **Indirect** — lookup whether tokens are known first/last names |
| Install size | Wheel small, but **requires 3.2 GB RAM** to load name database |
| License | MIT |
| Stars | 1,025 |

730K first names + 983K last names from Facebook data. Could check "is the first
word a known given name?" but at 3.2 GB resident memory, impractical for this
host (co-hosts wslcb-web.service, constrained on RAM per #175).

---

## 8. `gender-guesser`

**Verdict: Stale supplementary signal only.**

| Attribute | Detail |
|---|---|
| Version | 0.4.0 (last release 2016) |
| License | **GPLv3** — restrictive |
| Stars | 144, zero dependencies |

If first token has a recognized gender → likely person. But "LIBERTY" and "GRACE"
are both given names and business words. Weak signal, stale, GPLv3.

---

## 9. `cleanco` (NEW — 2026-11)

**Verdict: Marginal improvement over our regex. Same approach, wider dictionary.**

| Attribute | Detail |
|---|---|
| Version | 2.3 (May 2024) |
| Classification? | **Suffix-only** — detects and strips legal entity suffixes |
| Install size | **11 KB**, zero dependencies |
| License | MIT |

Contains 912 legal-entity suffixes across 90 entity type categories. But most
are international (GmbH, S.A., Oy, N.V., etc.) — irrelevant for WA State data.
The US-relevant terms largely overlap with our existing `_ORG_PATTERNS` regex.
Cannot classify names lacking a legal suffix (the actual hard problem).

---

## 10. LLM API batch classification (NEW — 2026-11)

**Verdict: Highest accuracy option for a one-time bulk reclassification.**

| Model | Input Rate | Output Rate | Est. Cost (17K names) |
|---|---|---|---|
| GPT-4.1-nano | $0.10/MTok | $0.40/MTok | **$0.05** |
| GPT-4.1-nano batch (50% off) | $0.05/MTok | $0.20/MTok | **$0.03** |
| GPT-4o-mini | $0.15/MTok | $0.60/MTok | **$0.08** |
| Claude Haiku 3.5 | $0.80/MTok | $4.00/MTok | **$0.49** |
| Claude Haiku 3.5 batch (50% off) | $0.40/MTok | $2.00/MTok | **$0.24** |

Approach: batch 100 names per request with a system prompt asking for
person/organization classification. LLMs understand contextual patterns
("GREEN THUMB GARDENS" = org, "GREEN JOHN" = person) that no regex or CRF
can reliably detect. Handles non-Western names without bias.

**Tradeoffs:**
- Non-deterministic — same name may classify differently across runs
- External API dependency (not suitable for real-time ingestion path)
- Requires API key and network access
- Results should be stored and used as a lookup, with regex for new names

---

## 11. Cloud NER APIs (NEW — 2026-11)

**Verdict: Poor fit. Same isolated-string problem as spaCy/HuggingFace.**

| Service | 17K names cost | Problem |
|---|---|---|
| Google Cloud NL | ~$17 | Trained on sentences, not isolated strings |
| AWS Comprehend (real-time) | ~$1.70 | Same |
| AWS Comprehend (batch) | ~$50 minimum | 10M char minimum charge |
| Azure Text Analytics | ~$17 | Same |

---

## Recommendation (updated 2026-11)

### Do NOT adopt `probablepeople` as primary classifier

The 2025-06 recommendation is **withdrawn**. While probablepeople correctly
identifies orgs without legal suffixes (the gap in our regex), its 6.2%
false-positive rate on person names — concentrated on non-Western names —
makes it unsuitable as a primary classifier for WSLCB data, which includes
a diverse applicant population.

### Recommended: layered approach

1. **Keep regex as the primary real-time classifier** — it's fast,
   deterministic, zero-dependency, and equally (in)accurate across
   demographics. Expand `_ORG_PATTERNS` to catch more suffixes already
   recognized in `text_utils.py` (P.C., N.A., P.A., D.B.A., etc.).

2. **One-time LLM batch reclassification** (~$0.03–$0.50) to fix the ~26
   known misclassified entities and discover others. Store results as the
   canonical classification. Use regex only for new names going forward.

3. **Add business-word heuristics** to the regex layer — names containing
   RESTAURANT, WINERY, BREWING, DISTILLERY, CLUB, CAFE, HOTEL, etc.
   without person-name patterns are almost certainly organizations.

4. **Optional: `probablepeople` as a third-pass signal** — only where
   regex says "person" AND the name doesn't match known person-name
   patterns (given-name first token, LAST FIRST format, etc.). Never use
   it as the sole classifier.

### Quick wins (zero new dependencies)

- Sync `_ORG_PATTERNS` with suffixes already in `text_utils._LEGIT_TRAILING_DOT`
  (P.C., N.A., P.A., S.P.A., D.B.A., L.L.P., PTY)
- Add TRUST, DBA, FRANCHISOR patterns (common in WSLCB data)
- Add business-category keywords (BREWING, WINERY, RESTAURANT, etc.)
  as a secondary heuristic layer
