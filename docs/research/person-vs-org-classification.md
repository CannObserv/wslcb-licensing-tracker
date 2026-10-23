# Person vs. Organization Name Classification

| | |
|---|---|
| Initial survey | 2025-06 (desk research; recommended `probablepeople`, never implemented) |
| Revision 1 | 2026-10-22 (live-data spot checks; withdrew `probablepeople`) |
| Revision 2 | 2026-10-22 (self-review; hand-labeled gold set; added TypeSafe Jev) — **this document** |
| Update | 2026-10-22 — Step 1 + Step 3 shipped in #203 (§9) |
| Update | 2026-10-23 — org-subtype validation and go-live trust set, #210 (§10) |
| Code under study | `entities._classify_entity_type()` — one regex, default `person` |
| Artifacts | [`person-vs-org/`](person-vs-org/): `gold.csv`, `predictions.csv`, `evaluate.py`; §10 adds `subtype_eval.py`, `subtype_supplement*.csv`, `subtype_label_review.csv`, `subtype_wordings.csv`, `subtype_spend.csv` |

## TL;DR

1. **The regex has bugs and a blind spot far larger than previously reported.**
   `CO\.` can never match (see §2), `LLP` is absent, and 30-char truncation
   defeats suffix matching. In the 438-name org-ish pool, it labels **198
   organizations, 39 trusts, 87 estates and 18 non-name strings as "person"**.
   Revision 1 said "~26".
2. **TypeSafe Jev (`jev-1.14.0`) is the best automated option measured.** It
   flagged 0/300 random persons, caught 198/198 hidden orgs and 86/87 estates,
   and returns a calibrated confidence. Batched 25 names per request, it costs
   **~$0.65 to classify all 67K entities** with no accuracy loss (unbatched:
   ~$1.50).
3. **`probablepeople` stays rejected.** It wrongly flags 8% of random persons,
   more often for non-Western names (15% vs 6%, p≈0.03).
4. **Recommended:** fix the regex now (zero deps), then add Jev as an offline,
   version-pinned enrichment pass gated on confidence (§7). First settle the
   taxonomy question: what estates, trusts and noise strings *are* (§8).
   *Update:* the regex fix and noise filter shipped in #203 (§9).

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

| Metric | regex (pre-#203) | regex (#203, shipped§) | regex (fixed†) | probablepeople | **Jev 5-way** | Jev binary | Haiku 5.5 |
|---|---|---|---|---|---|---|---|
| A: persons wrongly flagged | 0/300 | 0/300 | 0/300 | **24/300** | **0/300** | 1/300 | 0/300 |
| B: persons wrongly flagged | 0/92 | 0/92 | 0/92 | 36/92 | 5/92 | 4/92 | 2/92 |
| B: orgs caught | 0/198 | 182/198 | 148/198 | 185/198 | **198/198** | 198/198 | 197/198‡ |
| B: trusts caught | 0/39 | 36/39 | 30/39 | 33/39 | 34/39 | 19/39 | 36/39 |
| B: estates caught | 0/87 | 87/87 | 87/87 | 38/87 | 86/87 | 8/87 | 87/87 |
| C: orgs/trusts caught | 100/100 | 100/100 | 100/100 | 97/100 | 100/100 | 100/100 | 100/100 |
| 5-way accuracy (excl. A) | — | — | — | — | 813/834 | — | 824/834 |
| Not-a-name caught | — | 18/18 | — | — | 15/18 | — | 16/18 |
| Confidence returned | — | — | — | — | **calibrated** | yes | no |

† Prototype only, not committed. Its patterns were written while looking at
pool B, so these figures are **in-sample and optimistic**.
‡ Plus 1 trust: Haiku silently dropped 2 of 838 rows from its output.
§ Column `regex203` in `predictions.csv`; regenerate with `evaluate.py regex`.
Also tuned with pool B in view, so its gold-set recall is in-sample too; the
out-of-sample check is the production review in §9. 0/816 real names called noise.

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
| Measured tokens | Unbatched: ~526 input tokens per name (5-way prompt + context) → $0.000022/name, ≈ $1.48 for all 67,227 entities. **Batched 25/request: ~238 → ≈ $0.65** (see Batching below) |
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

**Batching (measured 2026-10-22, `person-vs-org/batch_experiment.py`).** The
results above sent one name per request. Jev's batching shares one `state`
across many questions; each question is scored independently, with no limit on
question count beyond the context limits (64k per request, 32k per question).
The jaggedness notes warn that unrelated state content (here, the *other*
names) is a distractor, so batch size was measured directly. The test used the
834 non-ambiguous gold names, one Choice question per name (`names.n<i>`):

| Names / request | 5-way correct | Persons wrongly flagged (A) | conf < 0.7 | Input tok / name | p50 per request |
|---|---|---|---|---|---|
| 1 (baseline) | 820 | 0/300 | 17 | 535 | 86 ms |
| 10 | 823 | 0/300 | 17 | 256 | 97 ms |
| 25, shuffled | 821–822 | 0/300 | 17–19 | 238 | 128–143 ms |
| 50, shuffled | 825 | 0/300 | 18 | 233 | 155 ms |
| 100 | 822 | 0/300 | 13 | 230 | 220 ms |
| 25, defs in state, shuffled | 820 | 0/300 | 22 | 138 | 97 ms |

- **No accuracy cost.** Every variant is within run-to-run drift: an unbatched
  re-run alone disagreed with the stored `jev5` labels on 8 names. Mixing the
  strata (shuffled) also made no difference.
- **Cost falls ~55%.** At K ≥ 25 the shared context is amortised. Most of the
  remaining ~230 tokens is the five label definitions, repeated in each
  question's `criteria`. That gives ≈ $0.65 for all 67K entities, vs ≈ $1.50
  unbatched.
- Moving the definitions into the shared state cuts tokens to ~130/name
  (≈ $0.37), with slightly more low-confidence answers. It departs from the
  documented criteria-per-question pattern and saves ~$0.30 per full pass, so
  it isn't worth it.
- **Recommended: 25 names per request, criteria per question.** That is ~6K
  tokens per request, far under both limits. Larger batches only shave a few
  tokens per name while a single failure re-queues more names. The full 67K
  backfill is ~2,700 requests (~1 min at concurrency 8).

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
  disagree. Batch 25 names per request. One-off backfill of all 67K costs
  ≈ $0.65 and takes ~1 minute at concurrency 8.
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

## 9. Step 1 + Step 3 shipped (#203, 2026-10-22)

`entities.entity_category()` holds one regex per category (trust, legal suffix,
truncated suffix, government/tribe, org keyword, estate). Only the binary
`entity_type` is stored. `is_noise_name()` skips non-name parts at ingest, and
`wslcb db reclassify-entities [--dry-run]` applies both rules to existing rows.

**Gold set:** 0/300 + 0/92 persons wrongly flagged; 182/198 orgs, 36/39 trusts,
87/87 estates, 18/18 noise; 0/816 real names called noise. The remaining misses
are orgs with no keyword (`FAMILY DOLLAR`, `LIBERTY BAR`,
`INDIANOLA COUNTRY STORE`) and truncated trusts with no trust token left
(`CHRISTOPHER BANKE JACKSON 2013`).

**Production review:** before applying, the classifier was run offline over all
67,227 entities. That gave 331 person→organization flips, and every one was
eyeballed. The review removed four rules: `CASINO` (gold person
`ERIC WILLIAM CASINO`), `MARINA` (14 given names), `PARISH` (6 surnames) and
mid-name `P.C.` (`ANDREW P.C. WRIGHT`). After those fixes, all 26 flips outside
the gold set were correct, as was a seeded random 50 of the 331. Few flips fall
outside the gold set because pool B was a census of the org-ish names, so most
regex misses had already been labeled.

**Production apply:** organization 1,062 → 1,391; person 66,165 → 65,811. 25
noise entities (62 links across 61 records) were removed. Backup:
`data/remediation-backups/2026-10-22-203/`.

**Deliberately left alone:**
- Real names with an appended citation (`7-ELEVEN, INC. PER WAC 314-07-035`,
  `JAMES AND WENDY HULL AS PER WAC 314-55-035`) are kept verbatim. Stripping the
  suffix would rename and merge entities, which needs its own cleanup.
- Short fragments (`IL`, `TY`, `CCFW`, `SMC`, `1744FOE`) are kept because they
  may be initials or abbreviations.

## 10. Org subtype validation (#210, 2026-10-23)

**Question.** Production asks Jev two questions per name: the 5-way *kind*
question (§5.1) and an *org* question over `business` / `nonprofit` / `tribe` /
`government` (`jev_client.ORG_CRITERIA`). The go-live bar: a subtype is
trusted only at **≥ 90 % precision at the 0.7 threshold, on ≥ 20 answers**.
A subtype below the bar, or with too little evidence, is never assigned by Jev
(the resolver falls back to the regex's subtype, else `unknown`).

**What counts.** A Jev subtype reaches `entities.entity_subtype` only when the
kind answer is `organization` at ≥ 0.7 (the resolver's gate) **and** the org
answer is at ≥ 0.7 (`map_answer`). Precision for subtype S = correct ÷ all
rows passing both gates with answer S, including rows whose gold label is not
`O` (a person Jev calls a business is a business error).

### Method

- **Labels.** The 289 `O` gold rows got a `subtype` column, drafted from the
  criteria definitions **before any live call** (commit `629246d`). The
  operator reviewed the 11 unsure rows plus a seeded random 30 against public
  sources (`subtype_label_review.csv`). Three flipped nonprofit → business on
  evidence: THE ROSE THEATRE, ELLENSBURG GOLF CLUB, SKALITUDE POLLINATOR
  SANCTUARY. Labels follow what the entity *is*, not the criteria's keyword
  hints. Final: business 243, nonprofit 24, tribe 13, government 9.
- **Harness.** `subtype_eval.py` sends names through the shipped
  `jev_client.classify_batch` (25 per request, `entity_id` order as production
  batches them) and scores through `map_answer`. It refuses to send while a
  label file has uncommitted changes, caps total spend at $0.10 through a
  ledger, and pins each wording to a `--tag` (`subtype_wordings.csv`).
- **Supplement (held-out).** A read-only `md5(name)` sample from production,
  outside the gold set: regex tribe / government plus keyword pools the regex
  may miss, and a nonprofit keyword pool. Only **20 names** exist: production
  holds 1,392 org entities (of 67,221), 16 regex tribes and 7 regex
  governments, and gold stratum B was already a census of the org-ish names.
  Labeled and committed before Jev saw them (tribe 4, business 10,
  nonprofit 4, person 2; no government).
- **Two wordings.** `prod` is the wording #207 shipped. `prod2` is the
  operator-approved rewording (business adds cinemas/theaters, golf courses,
  investment funds; nonprofit becomes lodges, posts, churches, museums,
  foundations, civic associations and members' clubs, with "a theater, golf
  club or venue is a business unless the name says it is a nonprofit";
  government adds community colleges). The `prod` examples included five
  gold names (FAMILY DOLLAR, LIBERTY BAR, LOWER ELWHA KLALLAM TRIBE,
  CITY OF LONGVIEW, and CONFEDERATED TRIBES OF THE COLVILLE RESERVATION by
  30-char prefix), which slightly flatters `prod`'s gold score; `prod2` uses
  none (`test_org_criteria_examples_are_not_scored_names`).

### Runs and spend

| Run | Names | Requests | Input tokens | Tok / name | Spend |
|---|---|---|---|---|---|
| gold, `prod` | 834 | 34 | 414,512 | 497.0 (496.8 full batches) | $0.0174 |
| supplement, `prod` | 20 | 1 | 10,012 | 500.6 | $0.0004 |
| gold, `prod2` | 834 | 34 | 454,544 | **545.0** (544.8 full batches) | $0.0191 |
| supplement, `prod2` | 20 | 1 | 10,972 | 548.6 | $0.0005 |
| **total** | | | 890,040 | | **$0.0374** |

Both questions cost ~2.3× the kind-only 238 tokens/name; `jev_classifier.EST_INPUT_TOKENS_PER_NAME` is now **545**.

### Kind regression gate

| | Persons wrongly flagged (A) | 5-way correct |
|---|---|---|
| `jev5` (unbatched, §5.1) | 0/300 | 813/834 |
| K=25 kind-only (§5.1 batching) | 0/300 | 821–822 |
| `prod` | 0/300 | 824/834 |
| `prod2` | 0/300 | 822/834 |

Gate: A = 0/300 and 5-way ≥ 813. Both pass: the org question does not hurt the
kind answer. `prod2`'s two kind changes vs `prod` (CAKEBREAD CELLARS SD FBO
DENNI T→O, TAYLOR, ALEXANDER CHURCH P→O) are both below 0.5 confidence, so the
resolver never uses them.

### Org subtypes at 0.7 (gold + supplement)

| Subtype | Wording | Answered ≥ 0.7 | Correct | Precision | 95 % Wilson low | Held-out | Verdict |
|---|---|---|---|---|---|---|---|
| business | `prod` | 243 | 242 | 99.6 % | 97.7 % | 8/8 | pass |
| nonprofit | `prod` | 31 | 26 | 83.9 % | 67.4 % | 4/4 | **fail** |
| tribe | `prod` | 16 | 16 | 100 % | 80.6 % | 3/3 | insufficient |
| government | `prod` | 7 | 7 | 100 % | 64.6 % | 0/0 | insufficient |
| business | **`prod2`** | 248 | 245 | 98.8 % | 96.5 % | 9/9 | **pass** |
| nonprofit | **`prod2`** | 26 | 25 | 96.2 % | 81.1 % | 4/4 | **pass** |
| tribe | **`prod2`** | 16 | 16 | 100 % | 80.6 % | 3/3 | insufficient |
| government | **`prod2`** | 7 | 7 | 100 % | 64.6 % | 0/0 | insufficient |

Verdict rule (operator, #210): gold + supplement decide; < 20 answers is
"insufficient evidence" and fails closed; ≥ 10 held-out answers below 90 %
would veto (never triggered: held-out had no errors). At 0.8 / 0.9, `prod2`
business is 98.8 % / 99.6 % and nonprofit 100 % / 100 %.

**Errors, `prod`:** THE ROSE THEATRE and ELLENSBURG GOLF CLUB → nonprofit (the
criteria's "theater"/"club" hints), SOUTH PUGET SOUND COMMUNITY CO → nonprofit,
BCP SPECIAL OPPORTUNITIES FUND → nonprofit, SOCIETY (not a name) → nonprofit,
ALEXA SO INN (person) → business.

**Errors, `prod2`:** CHEWELAH GOLF & COUNTRY CLUB and SEATTLE GOLF CLUB
(real nonprofits) → business, the cost of the golf-club default, accepted when
the wording was approved; SOCIETY → nonprofit and ALEXA SO INN → business,
both kind-question errors the org wording cannot reach. SOUTH PUGET SOUND
COMMUNITY CO (0.61) and BCP (0.48) drop below 0.7, so they store `unknown`
and fall back to the regex subtype.

**What trust changes.** A trusted subtype also *overrides* the regex's. Under
`prod2` at 0.7, Jev nonprofit replaces a regex `business` on 23 names (19 gold,
4 supplement), all correct but SOCIETY.

### Decision (pinned in #210 task 5.2)

- **`JEV_TRUSTED_ORG_SUBTYPES = {"business", "nonprofit"}`**, with
  `ORG_CRITERIA` = the `prod2` wording (`test_org_criteria_is_the_wording_section_10_scored`).
- **Tribe and government: insufficient evidence, not trusted.** Production
  cannot supply 20 answers today (≤ 17 tribe, ≤ 9 government names exist).
  The cost is small: every gold tribe and 7 of 9 gold governments are already
  regex-labeled, and an untrusted Jev answer falls back to that.
- The trust set is valid at `…_JEV_CONFIDENCE_THRESHOLD` ≥ 0.7; lowering it
  voids this result. Any `ORG_CRITERIA` change means re-scoring under a new tag.

### Limits

- **Nonprofit is marginal and partly tuned.** The rewording was drafted
  against `prod`'s gold errors, so `prod2`'s gold score is a development
  number; the held-out check is only 4 nonprofit answers. 25/26 leaves one
  error of headroom before 23/26 = 88.5 % would fail; Jev drifts ~0.8 % per
  run (§5.1).
- **The supplement only measures inside keyword pools**, and its batches were
  homogeneous (one 20-name request). §5.1 found no composition effect.
- **One run per wording.**
- **Golf clubs and theaters are name-ambiguous**: the `prod2` default sends
  member-owned clubs to business.

### For the backfill (#210 task 5.3)

At the 67,221 entities awaiting classification, the dry run should show
≈ 2,689 requests, ≈ 36.6 M input tokens, **≈ $1.54** (545 tokens/name at
$0.042/M), inside the plan's ≤ $2.

### Follow-ups (not done here)

- A `cooperative` subtype (operator question): needs a migration, an
  `ORG_CRITERIA` change and a re-score; 1 co-op in the 289 gold orgs.
- Re-check tribe / government if the entity population grows enough to give
  20 answers.
