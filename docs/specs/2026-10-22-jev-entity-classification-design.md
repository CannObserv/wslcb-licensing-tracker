# Optional Jev entity classification, `entity_subtype`, and operator review — design

2026-10-22 · follows #203 · evidence: [`docs/research/person-vs-org-classification.md`](../research/person-vs-org-classification.md)

## Intent

Entities are labeled `person` or `organization` by a regex
(`entities.entity_category()`, fixed in #203). It still misses organizations
that have no telltale word (`FAMILY DOLLAR`, `LIBERTY BAR`), and trusts whose
name was cut off at 30 characters before the word TRUST. Nobody is harmed by
this yet, but when an operator spots a business listed as a person, it
undercuts their confidence in the rest of the data. The goal is to remove
visible mistakes and give operators a way to correct the ones that remain.

Three parts:

1. **Finer-grained labels.** Organizations get an `entity_subtype` (business,
   nonprofit, trust, estate, tribe, government, unknown). It is visible and
   filterable on the public site. `entity_type` stays binary: estates,
   trusts, tribes and governments are all `organization`.
2. **Optional TypeSafe Jev classification.** An offline enrichment pass that
   classifies each entity once. It is turned on with an environment variable,
   and the regex remains the fallback.
3. **Operator control.** Operators can fix a classification in place, flag
   it, and work a review queue. Every label shows where it came from.

## Goals

- An operator browsing or filtering entities rarely meets an obvious
  misclassification. When they do, they can fix it right there, and the fix
  sticks.
- Every displayed classification shows its source: rules, Jev (with
  confidence) or an operator.
- With Jev off, unreachable, or the vendor gone, nothing breaks. Entities with
  no stored Jev answer keep regex labels; stored Jev answers keep being used
  (see *The enable flag*).

## Anti-goals (owner's ranking, most important first)

This can technically work and still fail the owner if any of these happen:

1. **A manual correction is overwritten.** No automated pass (Jev backfill,
   refresh, model upgrade, regex reclassify, `reprocess-entities`, noise
   purge, duplicate merge) may change or delete an operator's classification.
   An entity with a manual classification is never sent to Jev by an
   automatic pass or bulk refresh (see Operator actions for the one
   exception).
2. **Jev stops working and nobody notices.** A missing key, a vendor outage
   or a disabled timer must surface on the dashboard and through co-status
   check-ins.
3. **Unclear provenance.** An operator can't tell where a label came from.
4. **The review queue grows into a backlog nobody trusts.** It has to stay
   small and quick to clear.
5. **Cost or dependency creep.** Spend must be bounded and visible, and Jev
   must never be on the ingestion critical path.

## Hard requirement: classify once

Once an entity has a stored Jev result, later scrapes, timers and backfills
**never send it to Jev again**. A Jev re-query happens only through:

- an explicit CLI refresh (all entities, entities last classified by a given
  model version, or a list of entity IDs);
- an operator's "re-classify with Jev" action on one entity in the admin UI;
- a scheduled refresh cycle, if one is added later. There is **none by
  default**.

Pinning a new model version does not by itself re-query anything. Calls that
failed and stored no result are retried; that isn't a re-query. Entity names
are immutable (`uq_entities_name`), so a stored answer never goes stale
because the input changed.

## Non-goals

- Changing `entity_type` beyond `person` / `organization`.
- Public flagging or public corrections. Flags and fixes are admin-only.
- Stripping WAC text from real names, or merging the resulting duplicates
  (deferred in #203).
- A general LLM classifier. The research rejects one as a pipeline step (it
  drops rows and gives no confidence).
- Renaming existing environment variables. That is #204; this work only
  follows its prefix rule.

## Decided

The owner made these decisions:

- Column name `entity_subtype`.
- Subtypes are visible and filterable publicly.
- Operators can both fix in place and flag for review.
- Jev mirrors the address-validation pattern: an enable flag, a post-scrape
  pass, a weekly timer, a one-off backfill command, and an admin queue for
  low-confidence results and flags.
- Requests are batched: **25 names per request**, label definitions in each
  question's `criteria` (measured in the research doc §5.1, *Batching*: no
  accuracy loss, ~238 input tokens/name, ≈ $0.65 for all 67K entities).
  *(2026-10-23, #209: superseded. With the org-subtype question added it is
  ~545 tokens/name, ≈ $1.54 for 67K; see research §10 and `jev_client.ORG_CRITERIA`.)*
- Classify once (above).
- New environment variables use the prefix `CO_WSLCB_LICENSING_TRACKER_` (#204).

## Design

### Data model

**`entities`** gains two columns:

| Column | Meaning |
|---|---|
| `entity_subtype` | `NULL` for persons; otherwise one of `business`, `nonprofit`, `trust`, `estate`, `tribe`, `government`, `unknown`. A CHECK constraint or lookup enforces the set. |
| `classification_source` | `regex` / `jev` / `manual`: which source produced the shown `entity_type` + `entity_subtype`. The provenance badge reads this, and it can be filtered without a join. |

`entity_type` and `entity_subtype` on `entities` stay the **resolved** values,
so every existing query, filter and template keeps working.

**`entity_classifications`** (new) keeps every source's answer:

| Column | Meaning |
|---|---|
| `id`, `entity_id` (FK) | |
| `source` | `regex` / `jev` / `manual` |
| `entity_type`, `entity_subtype` | The source's answer. Jev's `not_a_name` is stored as an answer, not mapped (see Resolution). |
| `confidence` | Jev's confidence for its type answer. NULL for regex/manual. |
| `probabilities` | Jev's full per-option probabilities (JSONB), for later re-thresholding without re-querying. |
| `model` | `jev-1.14.0`, the regex version (`regex-1`, `entities.REGEX_MODEL`), or NULL for manual |
| `input_tokens` | Jev only: this entity's share of the batch's tokens (batch total ÷ names), for spend reporting |
| `created_at`, `created_by` | `created_by` is the admin's email for manual rows |
| `note` | Optional operator note on manual rows |

Rows are append-only per source. The *current* answer for a source is its
latest row. History is kept: a model refresh adds new `jev` rows, and a
changed manual decision adds a new `manual` row. The audit log also records
manual actions (`admin_audit.log_action`).

**`entity_review_flags`** (new): `entity_id`, `flagged_by`, `reason`,
`created_at`, `resolved_at`, `resolved_by`, `resolution`. Flags are admin-only.

**Data contract.** Add these to AGENTS.md's table:
- `regex` rows, and the resolved columns on `entities`, are **derived**.
- `jev` rows are derived but *not freely regenerable*. Re-creating them costs
  money and is non-deterministic (~1% label drift run to run), so treat them
  like `co_replay` source rows and don't bulk-delete them.
- `manual` rows and flags are **operator input**. They are never deleted by
  any automated pass and must survive the nightly backup/restore (they live in
  the DB, so they do).

### The enable flag

`…_ENABLE_JEV_CLASSIFICATION` controls **calls only** (owner's decision).
Turning it off stops new Jev requests; answers already stored keep winning
resolution, so no label flips in bulk because of a config change. Resolution
depends only on DB state, never on config.

**Rollback** is a separate, explicit, reversible command that marks `jev` rows
as ignored (all of them, or one model version) and re-resolves. Un-ignoring
restores them. Nothing is deleted. The builder chooses the mechanism (e.g. an
`ignored_at` column on `entity_classifications`).

### Resolution

One function computes the shown `entity_type`, `entity_subtype` and
`classification_source` from an entity's current rows:

1. A **manual** row, if any.
2. Otherwise a non-ignored **jev** row with `confidence ≥ threshold`
   (default 0.7) whose answer is a real type (not `not_a_name`).
3. Otherwise the **regex** row.

Resolution re-runs whenever any row for the entity is written, and as a bulk
pass. #203's `wslcb db reclassify-entities` must write `regex` rows and
re-resolve. It must never set `entities.entity_type` directly, and its noise
purge must skip any entity with a manual row or an open flag.
`merge_duplicate_entities()` must move manual rows and flags to the surviving
entity; it currently deletes duplicates outright.

### Regex subtypes

The regex row carries a subtype from `entity_category()`:

| Category | Subtype |
|---|---|
| `trust` | `trust` |
| `estate` | `estate` |
| `government_tribe` | `tribe` or `government`. Split the pattern so each alternation knows which. |
| `legal_suffix`, `truncated_suffix`, `org_keyword` | `business` |
| none (person) | NULL |

The regex never claims `nonprofit` (an `INC` may be either). Only Jev or an
operator can.

### Jev request shape

Each request contains up to 25 names in `state.names`, plus the shared
context string from the research. Each name gets two questions (speculative
fan-out, as recommended by TypeSafe):

- **`kind_<i>`**: the validated five-way Choice (person / organization / trust /
  estate / not_a_name), with the criteria wording from
  `docs/research/person-vs-org/evaluate.py`. Changing the wording requires
  re-scoring the gold set.
- **`org_<i>`**: a Choice among business / nonprofit / tribe / government.
  Its answer is used only when `kind` is `organization`.

Mapping:
- `kind`=trust → (organization, trust); estate → (organization, estate).
- organization → (organization, `org` answer). If `org` confidence is below
  the threshold, the subtype is `unknown`. At resolution, an `unknown` (or an
  untrusted subtype, see the go-live bar) falls back to the regex's org
  subtype when it has one.
- person → (person, NULL).
- not_a_name → stored as-is. Not resolved; goes to the review queue.

The `org` question is unvalidated: the gold set labels organizations as
`O` only. Before go-live, the builder hand-labels the gold set's ~289 `O`
rows by subtype (business / nonprofit / tribe / government) and records the
score in the research doc. **Go-live bar (owner-approved), per subtype:**
when Jev's `org` confidence clears the threshold, its answer must be right
≥ 90% of the time. A subtype that misses the bar is never assigned by Jev:
those answers fall back to the regex's subtype, or `unknown`. The five-way
type question ships regardless, since it is already validated. Small
subtypes (tribe, government) may have too few gold rows to judge; label a
targeted supplementary sample from production if needed. Adding a second question means re-measuring
tokens per name.

The model is pinned (default `jev-1.14.0`) and stored per row. HTTP 429/529
responses are retried with backoff. Any other failure stores nothing for that
batch, so its names are picked up by the next run.

### When Jev runs

- **After each scrape, and after each relayed-capture import.** Classify
  entities that have no `jev` row and no `manual` row. This hooks in next to
  the post-scrape address backfill in `cli.py`. Exceptions are logged and
  swallowed; ingestion never fails because of Jev.
- **Weekly timer** `wslcb-entity-classification.timer`, via the existing
  `wslcb-task@` template. It catches anything missed, including entities
  created by backfill paths, and checks in to co-status.
- **CLI** `wslcb db classify-entities`:
  - default: unclassified only;
  - `--refresh-all`, `--refresh-model <id>`, `--ids-file <path>`: explicit
    re-queries. All of these **skip entities with a manual row**; only the
    single-entity admin action may query one;
  - `--limit N`, `--dry-run` (counts and estimated cost; no calls).
- **Admin "re-classify with Jev"** on one entity: a single synchronous call.

Each automatic run is capped by `..._JEV_MAX_NAMES_PER_RUN`. The CLI can
override the cap for the one-off backfill (~2,700 requests, about a minute).

### Configuration

All new variables use the `CO_WSLCB_LICENSING_TRACKER_` prefix and are added
to `.env.example`:

| Variable | Default |
|---|---|
| `…_ENABLE_JEV_CLASSIFICATION` | off |
| `…_TYPESAFE_API_KEY` | — (the app and timer need it, like the address-validator key) |
| `…_JEV_MODEL` | `jev-1.14.0` |
| `…_JEV_CONFIDENCE_THRESHOLD` | `0.7` |
| `…_JEV_MAX_NAMES_PER_RUN` | `5000` (builder may tune) |
| `…_CLASSIFY_CHECKIN_BASE_URL`, `…_CLASSIFY_MONITOR_ID` | a new `checkin.Monitor` |

If the flag is on but the key is missing, that is a visible health failure,
not a silent no-op.

### Health and spend (anti-goals 2 and 5)

- Record each run's outcome: when it ran, names sent, batches OK/failed,
  tokens, model. A small `classification_runs` table is the simplest option.
- **Admin dashboard panel:**
  - enabled or disabled;
  - last successful call;
  - entities awaiting classification;
  - failures in the last 7 days;
  - tokens and estimated spend, this month and all-time (price constant in
    config).
- **co-status check-in.** One new monitor. **The weekly timer is the
  dead-man signal**, so only the weekly timer ever sends `ok`.
  - The weekly timer sends `ok` on success, and `alert` when calls are
    failing, the key is missing while the flag is on, or the backlog grows
    past a threshold.
  - Post-scrape passes send only `alert` (on failure), never `ok`. Twice-daily
    `ok`s would mask a disabled weekly timer.
  - With the flag deliberately off, the weekly timer still runs and sends
    `ok` with an `enabled=false` variable. The dead-man stays armed, and the
    dashboard shows "disabled".
  - `wslcb-task@import-relayed` has no `checkin-key` credential, so its
    post-import pass doesn't check in. Its failures show on the dashboard, and
    the weekly timer catches the backlog. Don't add a credential to the relay
    unit for this.

  Follow `checkin.py`'s contract: check-ins never raise and never change the
  exit status. Document the monitor in `docs/MONITORING.md`.

### Provenance badge (anti-goal 3)

- **Public site:** next to the type/subtype on entity pages and entity lists:
  - **Rules** (regex);
  - **AI · 92%** (Jev, with confidence);
  - **Reviewed** (manual).
- **Admin view:** also shows the model ID and each source's answer side by
  side.
- Reuse the existing provenance-chip styling (#201). Keep the public wording
  vendor-neutral.

### Operator actions and the review queue (anti-goal 4)

**On an entity page (admin signed in):**
- **Set classification:** type + subtype + optional note. Writes a `manual`
  row, re-resolves, and writes to the audit log.
- **Flag for review:** reason. Writes an `entity_review_flags` row.
- **Re-classify with Jev:** a single explicit call. It is allowed on an
  entity with a manual override, as a second opinion. The new `jev` row is
  stored and shown side by side, but manual still wins resolution, so nothing
  is overwritten.
- **Clear manual override:** returns the entity to automatic resolution.
  Audited.

**Admin review queue** (`/admin/entities/review`) lists, without duplicates:
- open flags;
- Jev answers below the threshold;
- Jev `not_a_name` answers.

It is sorted with flags first, then by ascending confidence, and the count is
shown on the dashboard.

Each item has one-click actions, and each writes a `manual` row (or a
resolution) that removes the item from the queue:
- **Accept Jev's answer**
- **Accept the regex's answer**
- **Set other…**
- **Not a name:** for an entity that isn't a real name, record it as noise
  so the noise purge may remove it. The builder decides how a confirmed
  noise decision is stored and purged; it must stay audited.

A Jev answer where Jev and the regex disagree but Jev is above the threshold
is auto-accepted, not queued. Organizations whose subtype resolves to
`unknown` are **not** in the main queue or its dashboard count. They get a
separate "Subtype unknown" tab with the same one-click actions, so they can
be worked when convenient without swamping the queue. Report how many there
are after the backfill. Expected volume: about 470 items after the
backfill (0.7% of entities), then a trickle per scrape.

### Public UI

- `/entities`: a subtype filter (`?subtype=`) next to the existing type
  filter, and a subtype label plus badge in results.
- `/entity/{id}`: the type, subtype and badge.
- API responses that expose `entity_type` gain `entity_subtype` and
  `classification_source`.

### Rollout

1. Migration: new tables and columns. Backfill `regex` rows plus resolution
   for all 67K entities (no Jev).
2. Ship the UI, review queue, health panel, and the flag set to **off**.
3. Validate the subtype question on the gold set (above). Run the gold set
   end to end through the production code path; expect 0/300 persons flagged.
4. Turn the flag on. Run `classify-entities --dry-run`, then the backfill.
   Report before/after counts by type and subtype, the queue size, and
   tokens/spend.
5. Enable the weekly timer and the co-status monitor.

## Left to the builder

- Exact schema details: lookup table vs CHECK, index choices, whether
  `classification_runs` is a table or reuses an existing run-history
  mechanism.
- HTTP client details. Plain `httpx` matches the research; the
  `typesafe-sdk` package is optional, but mind the host's RAM limits (#175).
- UI layout, badge styling within `docs/STYLE.md`, and HTMX interactions.
- How "confirmed noise" is stored and purged, as long as it is audited and
  never automatic for a manual decision.
- Issue split. One epic with children (schema + resolution; Jev client +
  CLI + timer; admin actions + queue; public UI; monitoring) is reasonable.

## Calls made by the designer, not the owner

These are mine; the owner should check them:

- The `classification_source` column on `entities` (for the badge and
  filtering).
- Regex categories map to `business` by default; the regex never says
  `nonprofit`.
- A second Jev question for the org subtype, rather than one wider choice.
  It keeps the validated five-way question unchanged.
- Jev's `not_a_name` answers go to the review queue and are never
  auto-purged.
- Public badge wording: "Rules" / "AI · NN%" / "Reviewed".
- A default cap of 5,000 names per automatic run.
- Answers are kept as append-only history, with the latest row per source
  winning.
- The weekly timer is the only `ok` sender. Post-scrape passes alert only;
  the relay import doesn't check in; with the flag off, the timer still checks
  in `ok` with `enabled=false`.
- Bulk refreshes skip manual entities. The single-entity admin re-classify
  may query one, as a second opinion that can't win.
- `unknown` subtypes get their own tab, not the main queue.
