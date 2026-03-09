# Person vs. Organization Name Classification — Library Comparison

Researched 2025-06. Context: ~17K uppercased name strings from WSLCB licensing data.
Current approach: regex checking for org suffixes (LLC, INC, CORP, etc.), defaulting
everything else to "person". This misclassifies ~30-50 orgs that lack legal suffixes.

---

## 1. `probablepeople` ⭐ RECOMMENDED

**Verdict: Best fit for this task. Purpose-built for exactly this problem.**

| Attribute | Detail |
|---|---|
| Classification? | **Yes** — returns `"Person"` or `"Corporation"` directly |
| ALL-CAPS? | **Handles perfectly** — tested extensively |
| Install size | **~14.5 MB** total (probablepeople 9.3 MB + python-crfsuite 4.7 MB + doublemetaphone 0.5 MB) |
| Model download? | **No** — CRF model is bundled in the wheel |
| License | MIT |
| Maintenance | Active — last push May 2025, 616 GitHub stars, by datamade.us |
| Speed | **11,000+ names/sec** — processes all 17K records in ~1.5s |

**How it works:** Uses a Conditional Random Field (CRF) model trained on labeled
name data. Considers character patterns, word shapes, phonetics (double metaphone),
and positional features to probabilistically parse and classify.

**Test results on WSLCB-like data:**
```
Person names (all correct):
  JOHN SMITH              => Person       ✓
  MARY JANE DOE           => Person       ✓
  KIM LEE                 => Person       ✓
  ROBERT JAMES WILSON III => Person       ✓
  JOSE GARCIA-LOPEZ       => Person       ✓
  DIANA ROSS              => Person       ✓
  CHANG WEI               => Person       ✓
  MARIA ELENA GUTIERREZ   => Person       ✓
  VAN DER BERG            => Person       ✓
  DE LA CRUZ              => Person       ✓
  O BRIEN                 => Person       ✓
  MC DONALD               => Person       ✓
  J SMITH                 => Person       ✓
  MICHAEL JORDAN          => Person       ✓

Org names — WITH legal suffixes (easy, regex catches these too):
  BUTTERNUT HOLDINGS LLC  => Corporation  ✓

Org names — WITHOUT legal suffixes (the ones regex MISSES):
  FORGE CELLARS                 => Corporation  ✓
  THE CHICKEN SHACK WASHINGTON  => Corporation  ✓
  ARTICULATED SOLUTIONS         => Corporation  ✓
  PACIFIC NORTHWEST BREWING     => Corporation  ✓
  SMITH & JONES                 => Corporation  ✓
  A B C DISTRIBUTING            => Corporation  ✓
  COSTCO WHOLESALE              => Corporation  ✓
  TOTAL WINE & MORE             => Corporation  ✓
  EMERALD CITY CANNABIS         => Corporation  ✓
  GREEN LEAF SOLUTIONS          => Corporation  ✓
  THUNDER ISLAND BREWING        => Corporation  ✓
  LIBERTY BOTTLEWORKS           => Corporation  ✓
  HERITAGE DISTILLING           => Corporation  ✓
  JOHNSON FAMILY VINEYARDS      => Corporation  ✓
  SMITH TOWER BREWING           => Corporation  ✓
  LEE ENTERPRISES               => Corporation  ✓
  PARK PLACE WINE MERCHANTS     => Corporation  ✓
  BLUE STAR CAFE                => Corporation  ✓
  FIVE GUYS                     => Corporation  ✓
  THREE BEARS TRADING           => Corporation  ✓
  JACK IN THE BOX               => Corporation  ✓
  PAPA JOHNS                    => Corporation  ✓
  WENDYS OLD FASHIONED HAMBURGERS => Corporation ✓
  BJ'S RESTAURANT               => Corporation  ✓
  7 ELEVEN                      => Corporation  ✓
  24 HOUR FITNESS               => Corporation  ✓
  TRADER JOES                   => Corporation  ✓

DBA patterns (bonus!):
  JOHN SMITH DBA JOES BAR      => Corporation  (parses both parts)
  ABC LLC DBA THE PUB           => Corporation  (parses both parts)
```

**Known weaknesses (edge cases that go wrong):**
```
  WASHINGTON WINE CO   => Person    ✗  ("CO" parsed as surname, not "Company")
  BEST BUY             => Person    ✗  (two common English words)
  BAKER STREET         => Person    ✗  (Baker is a common surname)
  YOUNG LIFE           => Person    ✗  (Young is a common surname)
  DR PEPPER            => Person    ✗  (DR parsed as title prefix)
  SAFEWAY              => Person    ✗  (single-word orgs default to Person)
  WALMART              => Person    ✗  (single-word orgs default to Person)
  MCDONALDS            => Person    ✗  (Mc prefix = Irish surname pattern)
```

Single-word names are the main weakness — the model defaults to Person for those.
This is actually fine for our use case since single-word entries in WSLCB data are
rare and the current regex fallback can handle "CO", "SAFEWAY" etc. as special cases.

**Usage:**
```python
import probablepeople as pp

def classify_name(name: str) -> str:
    try:
        parsed, name_type = pp.tag(name)
        return name_type  # "Person" or "Corporation"
    except pp.RepeatedLabelError:
        return "Unknown"
```

---

## 2. `nameparser` (python-nameparser)

**Verdict: Not useful for classification. It's a parser, not a classifier.**

| Attribute | Detail |
|---|---|
| Classification? | **No** — always assumes input is a person name |
| ALL-CAPS? | Handles fine (has capitalization helpers) |
| Install size | **138 KB** — pure Python, zero dependencies |
| Model download? | No |
| License | LGPL |
| Maintenance | Last push May 2024, 702 GitHub stars |

Parses everything as a human name. "BUTTERNUT HOLDINGS LLC" becomes
`first="BUTTERNUT", last="LLC"`. You could theoretically heuristic on "did it find
a plausible first+last combo" but this is fragile — "FORGE CELLARS" also parses
as `first="FORGE", last="CELLARS"` with no way to distinguish from a real name.

**Skip this for classification. Useful only if you already know something is a person
name and want to extract first/middle/last/suffix.**

---

## 3. `usaddress`

**Verdict: Irrelevant. Parses US street addresses, not names.**

Same team as probablepeople (datamade), same CRF approach, but trained on address
strings. 1,617 GitHub stars, actively maintained. Not applicable to our task.

---

## 4. `name-cleaver`

**Verdict: Broken on modern Python. Even if working, doesn't classify.**

By the Sunlight Foundation, designed for political campaign finance data. Has
`IndividualNameCleaver` and `OrganizationNameCleaver` but you must already know
the type before calling. Fails to import on Python 3.12 due to old-style relative
imports (`from cleaver import ...`). Last release 2019. **Dead project.**

---

## 5. spaCy NER

**Verdict: Overkill. Heavy dependency for marginal benefit over probablepeople.**

| Attribute | Detail |
|---|---|
| Classification? | **Yes** — NER labels include PERSON and ORG |
| ALL-CAPS? | **Poor with small model**, better with large/transformer models |
| Install size | **~200+ MB** (spaCy ~30MB + model 12-560MB + numpy + thinc + etc.) |
| Model download? | **Yes** — separate `python -m spacy download en_core_web_sm` |
| License | MIT |
| Maintenance | Very active, industry standard |

spaCy NER is designed for **running text** ("John Smith is the CEO of Acme Corp"),
not **isolated name strings**. When you feed it just "JOHN SMITH" with no sentence
context, accuracy drops significantly. ALL-CAPS input further hurts the small models
since they rely on capitalization patterns. The transformer model (`en_core_web_trf`)
handles ALL-CAPS better but requires PyTorch (~873 MB) — absurd for this task.

**Would require wrapping names in synthetic sentences** ("The entity called JOHN SMITH
applied for a license") to get reasonable results, which is hacky.

---

## 6. Hugging Face Transformers NER (e.g., `dslim/bert-base-NER`)

**Verdict: Massively overkill. Same problems as spaCy NER but heavier.**

| Attribute | Detail |
|---|---|
| Classification? | Yes — PERSON, ORG, LOC, MISC labels |
| ALL-CAPS? | Depends on model training data; generally poor on ALL-CAPS |
| Install size | **~1+ GB** (transformers 10MB + torch 873MB + model ~400MB) |
| Model download? | Yes — ~400MB model download on first use |
| License | Varies per model |
| Maintenance | Active |

Same fundamental problem as spaCy: NER models expect running text, not isolated
name strings. Requires a GPU for reasonable speed on 17K records. Completely
disproportionate to the problem.

---

## 7. `names-dataset`

**Verdict: Possible supplementary signal, but huge and indirect.**

| Attribute | Detail |
|---|---|
| Classification? | **Indirect** — lookup whether tokens are known first/last names |
| Install size | **53 MB** — ships a large names database |
| License | MIT |

Contains millions of first/last names from various countries. You could check
"is the first word a known given name?" as a heuristic. But at 53 MB for an
indirect signal, probablepeople (14.5 MB) gives you a direct answer.

---

## 8. `gender-guesser`

**Verdict: Indirect supplementary signal only.**

Can check if the first token is a known given name (and its gender). If the first
word matches a given name, it's more likely a person. But it's a weak signal —
"LIBERTY" and "GRACE" are both given names and business-name words. GPLv3 license
is also restrictive.

---

## Recommendation

### Primary: `probablepeople` — use as the main classifier

```
pip install probablepeople
```

- 14.5 MB total install, MIT license, no model download, 11K names/sec
- Correctly classifies the vast majority of cases including the hard ones
  (orgs without legal suffixes) that the current regex misses
- Handles ALL-CAPS perfectly

### Supplementary: Keep the regex as a fallback/override layer

For the few edge cases where probablepeople fails (single-word orgs like WALMART,
SAFEWAY; and "CO" suffix), the existing regex suffix check should run as a
second pass:

```python
import probablepeople as pp
import re

ORG_SUFFIXES = re.compile(
    r'\b(LLC|INC|CORP|CORPORATION|COMPANY|CO|LTD|LP|LLP|PLLC|PC|PLC)\b'
)

def classify_name(name: str) -> str:
    # Layer 1: regex catches obvious legal suffixes
    if ORG_SUFFIXES.search(name):
        return "Corporation"
    # Layer 2: probablepeople ML classification
    try:
        _, name_type = pp.tag(name)
        return name_type  # "Person" or "Corporation"
    except pp.RepeatedLabelError:
        return "Person"  # safe default for WSLCB data
```

This two-layer approach covers:
- ✅ Orgs with legal suffixes (regex)
- ✅ Orgs without legal suffixes like "FORGE CELLARS" (probablepeople)
- ✅ Person names (probablepeople)
- ✅ Edge cases like "WASHINGTON WINE CO" (regex catches "CO")
- ⚠️  Single-word orgs (WALMART, SAFEWAY) still need manual review or a known-org list
