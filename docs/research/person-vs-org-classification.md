# Person vs. Organization Name Classification

| | |
|---|---|
| Initial survey | 2025-06 (desk research; recommended `probablepeople`, never implemented) |
| Revision 1 | 2026-10-22 (live-data spot checks; withdrew `probablepeople`) |
| Revision 2 | 2026-10-22 (self-review; hand-labeled gold set; added TypeSafe Jev) — **this document** |
| Code under study | `entities._classify_entity_type()` — one regex, default `person` |
| Artifacts | [`person-vs-org/`](person-vs-org/): `gold.csv`, `predictions.csv`, `evaluate.py` |

## TL;DR

1. **The regex has bugs and a blind spot far larger than previously reported.**
   `CO\.` can never match (see §2), `LLP` is absent, and 30-char truncation
   defeats suffix matching. In the 438-name org-ish pool, it labels **198
   organizations, 39 trusts, 87 estates and 18 non-name strings as "person"**.
   Revision 1 said "~26".
2. **TypeSafe Jev (`jev-1.14.0`) is the best automated option measured.** It
   flagged 0/300 random persons, caught 198/198 hidden orgs and 86/87 estates,
   and returns a calibrated confidence. It costs **~$1.50 to classify all
   67K entities**, with p50 latency of 88 ms.
3. **`probablepeople` stays rejected.** It wrongly flags 8% of random persons,
   more often for non-Western names (15% vs 6%, p≈0.03).
4. **Recommended:** fix the regex now (zero deps), then add Jev as an offline,
   version-pinned enrichment pass gated on confidence (§7). First settle the
   taxonomy question: what estates, trusts and noise strings *are* (§8).

---

## 1. Self-review of Revision 1

Revision 1 had methodological errors. They are corrected here:

| # | Revision 1 claim / practice | Problem | Correction |
|---|---|---|---|
| 1 | Scored tools against `entities.entity_type` | That column *is* the regex output, so it isn't ground truth. Tools were penalised for correctly catching regex misses. | Hand-labeled gold set (§3) |
| 2 | "At least 26 orgs mislabeled person" | Undercount from one ad-hoc keyword query | Census of the org-ish pool: 324 non-persons + 18 noise (§4) |
| 3 | `probablepeople` bias "disproportionately non-Western" | Cherry-picked examples with no base rate. About half the false positives in its own list were Anglo names. | Measured on a random sample: 15% vs 6%, p≈0.03. Direction holds, but small n and subjective origin tags (§5.2). |
| 4 | "Regex treats demographics equally" | True, but it missed that the regex itself is buggy | §2 |
| 5 | LLM price table (GPT-4.1-nano, Haiku 3.5) for "17K names" | Unverified subagent figures, stale model names, wrong population (67K entities) | Removed. Haiku 5.5 measured directly (§5.3). |
| 6 | flair "stale on PyPI since 2021" | False — 0.15.1 shipped 2025-02-05 | Fixed (§6) |
| 7 | spaCy "tested against WSLCB-like data" | Subagent-run on synthetic names; never re-verified | Labeled as such (§6) |
| 8 | Doc dated "2026-11" | Wrong date | Fixed |

---

## 2. Defects in the current regex

```
\b(LLC|L\.?L\.?C\.?|INC\.?|CORP\.?|CORPORATION|TRUST|LTD\.?|LIMITED|PARTNERS|PARTNERSHIP
  |HOLDINGS|GROUP|ENTERPRISE|ENTERPRISES|ASSOCIATION|FOUNDATION|COMPANY|CO\.|L\.?P\.?)\b
```

| Defect | Example misclassified as person |
|---|---|
| `CO\.\b` needs a word char after the `.`, so it never matches before a space or at end of string. Bare `CO` is absent. | `ALASKAN BREWING & BOTTLING CO.`, `KINGS & CONVICTS HOLDING CO.`, `SEABROOK MANAGEMENT CO` |
| `LLP` absent | `GLEASON & KOATZ LLP`, `RED RIVER CATTLE LLP` |
| WSLCB truncates entries at 30 chars (212 entities are exactly 30), so suffixes get cut | `FAVIA ERICKSON WINEGROWERS, LL`, `TABLE & CO HOSPITALITY CORPORA` |
| No estate pattern (WSLCB format: `GIVEN ESTATE SURNAME`) | `DARRYL R ESTATE WILCOX` (87 in pool) |
| Only `TRUST` for trusts | `… REVOCABLE TRU`, `… TRST`, `… TTEE OF THE`, `… IRREVOCABL` |
| No suffix-less orgs | `FORGE CELLARS`, `LOWER ELWHA KLALLAM TRIBE`, `CITY OF LONGVIEW` |
| Non-name noise becomes entities | `PER WAC 314-07-035`, `17 SHAREHOLDERS W/10% OR LESS`, `035`, `TRUSTEE` |

---

## 3. Method

**Gold set** (`person-vs-org/gold.csv`, 838 names, deterministic `md5` sampling
from the production `entities` table on 2026-10-22):

| Stratum | Population | Sampled | Purpose |
|---|---|---|---|
| A | regex-`person`, all | 300 random | Person false-positive rate in the bulk |
| B | regex-`person` with org-ish signals (keyword net, `&`, digits, length = 30) | **all 438** (census) | The hard pool, where regex misses live |
| C | regex-`organization` | 100 random | Org recall and precision |

Labels: `P` person, `H` household of persons (`TREVOR & JAYNE BLACKWELL`),
`O` organization (incl. tribes, governments, lodges), `T` trust, `E` estate,
`N` not-a-name, `A` ambiguous (4, excluded). Labeled by the agent and **open to
audit**. A few truncated-trust labels are debatable (e.g. `THE ANDREW DAVID
STEPHEN BUSCH`, assumed to be `…TRUST`).

Binary scoring: P/H → person; O/T/E → non-person. N is excluded from the
binary metrics.

**Gold distribution:** A = 300 P. B = 198 O, 87 E, 39 T, 83 P, 9 H, 18 N, 4 A.
C = 91 O, 9 T. Stratum A has **0/300 non-persons**, so the miss rate outside
the B pool is <1% (rule of three, ≲650 entities). The regex's misses are
concentrated in pool B.

---

## 4. Results

`uv run python docs/research/person-vs-org/evaluate.py score`

| Metric | regex (current) | regex (fixed†) | probablepeople | **Jev 5-way** | Jev binary | Haiku 5.5 |
|---|---|---|---|---|---|---|
| A: persons wrongly flagged | 0/300 | 0/300 | **24/300** | **0/300** | 1/300 | 0/300 |
| B: persons wrongly flagged | 0/92 | 0/92 | 36/92 | 5/92 | 4/92 | 2/92 |
| B: orgs caught | 0/198 | 148/198 | 185/198 | **198/198** | 198/198 | 197/198‡ |
| B: trusts caught | 0/39 | 30/39 | 33/39 | 34/39 | 19/39 | 36/39 |
| B: estates caught | 0/87 | 87/87 | 38/87 | 86/87 | 8/87 | 87/87 |
| C: orgs/trusts caught | 100/100 | 100/100 | 97/100 | 100/100 | 100/100 | 100/100 |
| 5-way accuracy (excl. A) | — | — | — | 813/834 | — | 824/834 |
| Not-a-name caught | — | — | — | 15/18 | — | 16/18 |
| Confidence returned | — | — | — | **calibrated** | yes | no |

† Prototype only, not committed. Its patterns were written while looking at
pool B, so these figures are **in-sample and optimistic**.
‡ Plus 1 trust: Haiku silently dropped 2 of 838 rows from its output.

---

## 5. Candidates in depth

### 5.1 TypeSafe Jev — ⭐ RECOMMENDED (as offline enrichment)

A "System One" *decision* model. You send `state` plus typed questions
(Choice / Score / Noul) to `POST https://api.typesafe.ai/v1/systemone`, and get
back typed answers with per-option probabilities and a confidence score. It
does not generate text, so there is nothing to parse and rows can't be dropped.

| Attribute | Measured / documented |
|---|---|
| Model | `jev-latest` resolved to **`jev-1.14.0`**, even though the Models page (2026-10-22) still says `jev-latest` → 1.13. **Pin the versioned ID.** |
| Price | $0.042 / Mtok input; output free (docs) |
| Measured tokens | ~526 input tokens per name (5-way prompt + context) → **$0.000022/name, ≈ $1.48 for all 67,227 entities** |
| Latency | p50 **88 ms**, p95 150 ms (concurrency 8, from this VM) |
| Throughput | 838 names in ~10 s at concurrency 8, with no 429s observed |
| Order sensitivity | Options reversed: 834/838 identical choices |
| Determinism | **Not deterministic.** A full re-run changed 7/838 labels (0.8%); a 120-name re-run gave max probability delta 0.37. Store results; don't recompute on read. |
| Language | Docs: English best; other scripts "handled but not equally well". No person-name bias observed here (0/59 non-Western persons flagged). |
| Access | Direct API (key in `.env` as `TYPESAFE_API_KEY`), or via OpenRouter. Python SDK `typesafe-sdk`; plain `httpx` is enough. |
| Vendor risk | Young vendor; new signups were reportedly paused in Sept 2026. Our key works, but plan for the API to disappear: the regex must remain a working fallback. |
| Data | Sends applicant names, which are already public WSLCB records. Docs say customer data is not used for training. |

**Prompt used** (see `evaluate.py`): `state = {name, context}`, where context
says the entry is a WSLCB applicant-list entry, uppercase, possibly
`LAST, FIRST`, possibly truncated at 30 chars. One Choice question with five
options, each with a one-line criterion and examples: person / organization /
trust / estate / not_a_name.

**The taxonomy matters more than the model.** The bare binary prompt ("person
or organization?") caught only 8/87 estates and 19/39 trusts. The same model
with explicit trust/estate options caught 86/87 and 34/39.

**Confidence gating** (5-way, 834 gold names, excl. ambiguous):

| Threshold | Auto-accepted | Errors among auto | Routed to fallback/review |
|---|---|---|---|
| ≥ 0.5 | 821 | 13 | 13 |
| ≥ 0.7 | 802 | 7 | 32 |
| ≥ 0.8 | 783 | 6 | 51 |
| ≥ 0.9 | 716 | 4 | 118 |

**Residual errors** are mostly truncated trusts with no trust token left
(`CHRISTOPHER BANKE JACKSON 2013`, `BROTT AND CARRIE RITZEL FAMILY`), surnames
that look like words (`TRUSTY` → trust at conf 0.37–0.50; `WANPEN POST` → org at
0.55), and noise. Most are low-confidence, which is what the gate exists for.

### 5.2 `probablepeople` — ❌ REJECTED as primary classifier

CRF parser by datamade. v0.5.6 (2024-10-29), MIT, ~2.3 MB of wheels, no model
download, works on Python 3.12. Semi-maintained: 63 open issues, PRs unmerged
since 2018, defunct Travis CI.

- **8% of random persons wrongly flagged** (18 "Corporation" + 6
  `RepeatedLabelError` out of 300). Extrapolated, that is ~5K of 66K persons.
- **Bias, measured:** origin tags were applied subjectively from the names
  (East/SE/South Asian, African, Middle Eastern). The rate was 9/59 (15%) for
  those names vs 15/241 (6%) for the rest; Fisher exact p = 0.031. Small n,
  but consistent with a model trained mainly on Western names.
- Good at suffix-less orgs (185/198) but poor at estates (38/87).
  `RepeatedLabelError` on 4–5-token names and names with parenthetical
  annotations.

### 5.3 General-purpose LLM (measured: `claude-haiku-5.5`)

The whole gold set went in one prompt, with the same five labels, asking for
`id,label` per line.

- **Highest raw 5-way accuracy** (824/834), 0/300 persons wrongly flagged.
- **Silently dropped 2/838 rows.** Batched generative output needs
  reconciliation logic.
- No calibrated confidence, so there is no principled review gate.
- Generative output tokens are billed: 23.5K output vs 16.7K input for 838
  names. Current per-token pricing was not verified here.
- Viable as a **one-off audit / second opinion** (e.g. adjudicating Jev's
  low-confidence tail). Less suited than Jev for a repeatable pipeline step.

---

## 6. Other candidates (unchanged verdicts; facts re-checked)

| Candidate | Verdict | Verified facts (2026-10-22) |
|---|---|---|
| `nameparser` | Parser, not a classifier; treats every input as a human name | 2.5.0 |
| `usaddress` | Irrelevant (addresses) | — |
| `name-cleaver` | Dead | 0.6.0 (2013); **`import name_cleaver` fails on 3.12** (`No module named 'cleaver'`), verified |
| `cleanco` | Suffix detection only (912 terms, mostly non-US). Doesn't address suffix-less orgs. | 2.3 (2024-05), 11 KB wheel, verified |
| `names-dataset` | Indirect signal; impractical on this RAM-constrained host (#175) | 3.3.1 (2025-04); README: needs **3.2 GB RAM**, verified |
| `gender-guesser` | Weak signal, GPLv3, stale | 0.4.0 (2016) |
| spaCy NER | Bad fit: NER expects running text, not isolated strings | 3.8.16; ~335 MB installed. Subagent test on 50 **synthetic** names: ~60%. Not re-verified on gold set. |
| HF NER / GLiNER / flair | Overkill (torch, GB-scale); same isolated-string problem | flair 0.15.1 (2025-02); GLiNER 0.2.29 |
| Cloud NER (Google NL, AWS Comprehend, Azure) | Same isolated-string problem | Prices not verified; excluded |

---

## 7. Recommendation

**Step 1 — fix the regex (no new dependencies, do first).** Add `CO`, `LLP`,
truncated-suffix tails (`, LL`, `CORPORA`, `INCORPOR`), trust variants (`TR`,
`TRST`, `TRU`, `TTEE`, `REVOC*`, `IRREV*`), an estate pattern, and a curated
set of unambiguous org words (`WINERY`, `BREWING`, `TRIBE`, `CITY OF`, …).
Avoid words that are common surnames (`CHURCH`, `POST`, `INN`, `PIZZA`, `YOUNG`).
The in-sample prototype caught 148/198 orgs, 30/39 trusts and 87/87 estates
with 0/392 persons wrongly flagged. Validate it on a fresh sample before
trusting those numbers. Bump `_ENTITY_REPROCESS_VERSION`.

**Step 2 — Jev as an offline enrichment pass.** It follows the same shape as
address validation: never on the ingest hot path.
- Pin `jev-1.14.0` and store `label`, `confidence` and `model` per entity.
  This is derived data, regenerable on a version bump.
- Auto-accept at confidence ≥ 0.7; below that, keep the regex label and queue
  the entity for admin review. Population-weighted, that is ~0.7% of entities
  (~470): 2/300 in stratum A plus 32/438 in pool B. The gold set's 32/834 is
  inflated by its deliberate hard-case enrichment.
- Only send names that are new, or where the regex and the stored Jev label
  disagree. One-off backfill of all 67K costs ≈ $1.50 and takes ~15 minutes
  at concurrency 8.
- If the API is unavailable, everything degrades to the regex (Step 1).

**Step 3 — filter noise at ingest.** `PER WAC 314-07-035`,
`17 SHAREHOLDERS W/10% OR LESS`, `035`, etc. should not become entities. This
belongs with the existing `ADDITIONAL_NAMES_MARKERS` skip-list logic.

**Not recommended:** `probablepeople` (bias, false positives), spaCy and other
NER stacks (wrong task shape), and a general LLM as the pipeline classifier
(dropped rows, no confidence). A general LLM is fine for one-off audits.

## 8. Open questions (product decisions before Step 2)

1. **Taxonomy.** `entity_type` is binary. Should estates (`JOHN ESTATE SMITH`)
   be `person` (linked to the individual), `organization`, or a new `estate`
   type? The same question applies to trusts (often named for a person) and
   tribes/governments (currently grouped as `organization`).
2. **Review queue.** Is ~470 entities (~0.7%) going to manual review acceptable, or
   should low-confidence results silently keep the regex label?
3. **Vendor dependency.** Is a third-party API acceptable for a derived
   column, given the regex fallback?
