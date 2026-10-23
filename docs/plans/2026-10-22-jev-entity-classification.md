# Jev Entity Classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `entity_subtype`, per-source classification history with a resolver, an optional batched TypeSafe Jev enrichment pass, operator corrections/flags/review queue, provenance badges, and the health/spend surface. The regex stays the fallback.

**Architecture:** `entities.entity_type` / `entity_subtype` / `classification_source` become *resolved* columns. They are computed by one resolver (`entity_classification.py`) from append-only rows in `entity_classifications` (manual > Jev ≥ threshold > regex). `jev_client.py` is the pure HTTP transport. `jev_classifier.py` picks candidates, batches 25 names, stores answers and records `classification_runs`. It is called from the post-scrape/post-import hooks, the `wslcb db classify-entities` CLI (the weekly timer runs it), and one admin action. `classification_review.py` holds the audited operator actions, the review queue and the health stats behind the admin routes.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0 Core async + asyncpg, Alembic, Jinja2/HTMX/Tailwind, httpx (no `typesafe-sdk`: RAM limits, #175), click, systemd, co-status check-ins.

**Spec:** [`docs/specs/2026-10-22-jev-entity-classification-design.md`](../specs/2026-10-22-jev-entity-classification-design.md). Evidence: [`docs/research/person-vs-org-classification.md`](../research/person-vs-org-classification.md) §5.1.

## Global Constraints

- New env vars use the prefix `CO_WSLCB_LICENSING_TRACKER_`. Never rename existing vars (#204). All of them go in `.env.example`.
- `…_ENABLE_JEV_CLASSIFICATION` default off · `…_TYPESAFE_API_KEY` no default · `…_JEV_MODEL` `jev-1.14.0` · `…_JEV_CONFIDENCE_THRESHOLD` `0.7` · `…_JEV_MAX_NAMES_PER_RUN` `5000` · `…_CLASSIFY_CHECKIN_BASE_URL`, `…_CLASSIFY_MONITOR_ID`.
- Jev: `POST https://api.typesafe.ai/v1/systemone` with `Authorization: Bearer <key>`. The body is `{model, state, questions}` and the response is `{model, usage:{input_tokens}, answers:{<key>:{choice, confidence, …}}}`. Send **25 names per request**, with label definitions in each question's `criteria`.
- The five-way `kind` wording and the context string are copied verbatim from `docs/research/person-vs-org/evaluate.py` (`FIVE`, `CTX`). Changing them requires re-scoring the gold set.
- `entity_type` ∈ {`person`, `organization`}. `entity_subtype` is NULL for persons, otherwise one of `business`, `nonprofit`, `trust`, `estate`, `tribe`, `government`, `unknown`. `classification_source` ∈ {`regex`, `jev`, `manual`}.
- Resolution: (1) the current manual row; (2) otherwise a non-ignored Jev row with `confidence ≥ threshold` and a real type (not `not_a_name`); (3) otherwise the regex row.
- **Classify once.** An entity with any stored Jev row is never re-sent, except by `--refresh-all`, `--refresh-model`, `--ids-file`, or the admin single-entity action. There is no scheduled refresh.
- **No automated pass changes or deletes a manual row.** Bulk paths skip entities with a manual row. Only the admin single-entity action may query Jev for one.
- The enable flag gates **calls only**. Resolution reads DB rows (plus the threshold), never the flag.
- Jev is never on the ingestion critical path. Hook exceptions are logged and swallowed.
- HTTP 429/529 are retried with backoff. Any other failure stores nothing for that batch.
- Check-ins: only the weekly timer (default-mode CLI) sends `ok`. Post-scrape sends `alert` only. Post-import never checks in. `checkin.post_checkin` never raises and never changes the exit status.
- Public badge copy, vendor-neutral: `Rules` / `AI · NN%` / `Reviewed`.
- Go-live bar: per org subtype, Jev's `org` answer at or above the threshold must be right ≥ 90% of the time, or Jev never assigns that subtype.
- Price constant: $0.042 per million input tokens.
- Repo conventions: red/green TDD; `logging` with `%s`-style calls, never `print`; a module docstring on every `.py`; `datetime.now(UTC)`. Tests make no network calls (use `httpx.MockTransport`) and use `pg_conn`/`pg_engine`. Bare `TestClient(app)` route tests import `_stamp_engine` from `tests/_support.py`. Run `uv run pytest tests/ -v` before every commit. Commit format: `#<phase issue> [type]: …`.

## Review Focus

1. **A malformed Jev response** (missing `kind_<i>`/`org_<i>`, unknown choice, confidence outside 0..1) fails the whole batch: nothing is stored and the names are retried next run. It is never a partial write or a crash. Pinned in Task 2.1.
2. **A wrong or revoked API key (HTTP 401/403) with the flag on** stops the run after the first auth failure instead of firing ~2,700 doomed requests. It is recorded as `error="auth_failed"` and alerts. Pinned in Task 2.2.
3. **Overlapping runs** (a manual backfill during a post-scrape pass) must not both send the same unclassified names, which would break classify-once. A Postgres advisory lock makes the second run skip. Pinned in Task 2.2.
4. **An entity merged or purged between candidate selection and the write** is skipped. It must not abort the batch with an FK violation. Pinned in Task 2.2.
5. **Bad admin input:** person + subtype, organization without a subtype, a bogus subtype, a missing entity, or an external `next=` URL. These return 400/404 or fall back to a safe redirect, and nothing is written. Pinned in Task 3.3. (A public bogus `?subtype=` returns 200 with the filter ignored; pinned in Task 4.1.)

## File Structure

| File | Responsibility | Phase |
|---|---|---|
| `alembic/versions/0009_entity_classification.py` | columns on `entities`; `entity_classifications`, `entity_review_flags`, `classification_runs` | 1 |
| `src/…/models.py` | matching `Table` objects | 1 |
| `src/…/entities.py` | regex subtypes, regex row on create, `reclassify_entities` via the resolver, merge moves manual rows/flags | 1 |
| `src/…/entity_classification.py` (new) | constants, pure resolution rule, DB resolver, manual rows, Jev ignore/unignore | 1 |
| `src/…/config.py` | `jev_confidence_threshold()`, price constant | 1 |
| `src/…/jev_client.py` (new) | Jev transport + env getters; no DB | 2 |
| `src/…/jev_classifier.py` (new) | candidates, batching, storing, run rows, check-in payload | 2 |
| `src/…/checkin.py` | `CLASSIFY` monitor | 2 |
| `src/…/cli.py` | `db classify-entities`, `db ignore-jev`, hooks, alias | 1–2 |
| `infra/wslcb-entity-classification.timer`, `infra/wslcb-task@classify-entities.service.d/checkin.conf` | weekly timer + check-in drop-in | 2 |
| `src/…/classification_review.py` (new) | audited operator actions, review queue, detail, health | 3 |
| `src/…/admin_entity_routes.py` (new) | `/admin/entities/*` routes | 3 |
| `templates/admin/entity_review.html`, `templates/partials/entity_admin_panel.html` (new) | queue page, admin panel on entity page | 3 |
| `src/…/queries_entity.py`, `src/…/display.py`, `templates/{entities,entity}.html`, `templates/partials/{entities_results,classification_badge}.html` | subtype filter, badge | 4 |
| `docs/research/person-vs-org/{gold.csv,subtype_eval.py}` | subtype gold labels, production-path eval | 5 |

## Phases → child issues

| Phase | Tasks | Depends on |
|---|---|---|
| 1 Schema + resolution (#206) | 1.1–1.6 | — |
| 2 Jev client + CLI + timer + monitoring (#207) | 2.1–2.5 | Phase 1 merged; 1.6 deployed before Phase 2 ships |
| 3 Admin actions + review queue + health panel (#208) | 3.1–3.4 | Phases 1, 2 |
| 4 Public UI + API fields (#209) | 4.1–4.2 | Phase 1 only (can run parallel to 2–3) |
| 5 Rollout + validation (#210) | 5.1–5.4 | 5.1 needs 2.1; 5.2 needs 5.1; 5.3–5.4 need everything deployed |

Settled after the plan was drafted (designer calls; spec updated where noted):
- **Subtype fallback is uniform.** A Jev org subtype that is `unknown` (low `org` confidence) *or* not yet trusted falls back to the regex's org subtype when it has one, else `unknown` (spec updated).
- **API fields:** no `/api/v1` response exposes `entity_type` today, so the fields go into the query-layer dicts only. No new endpoint.
- **Threshold:** an env var read at resolve time. A change takes effect at the next re-resolve (`reclassify-entities`), not instantly.
- **Flag off blocks every Jev call**, including explicit CLI refreshes and the admin single-entity action (409). `--dry-run` still works.

Tasks marked **⚠ OPERATOR-GATED** touch production. Do not run them without the operator's explicit go-ahead.

---

## Phase 1 — Schema + resolution

### Task 1.1: Migration 0009 and table models

**Files:**
- Create: `alembic/versions/0009_entity_classification.py` (`revision = "0009"`, `down_revision = "0008"`)
- Modify: `src/wslcb_licensing_tracker/models.py` (`entities` at ~L198; new tables after `record_entities`)
- Modify: `tests/conftest.py` `_PG_SESSION_TABLES` (prepend `"classification_runs"`, `"entity_review_flags"`, `"entity_classifications"`)
- Modify: `docs/SCHEMA.md` (`entities` section, three new sections, migration-history row `0009_entity_classification`)
- Test: `tests/test_models.py`, `tests/test_schema.py`

**Interfaces:**
- Produces (models): `entities.c.entity_subtype` (Text NULL), `entities.c.classification_source` (Text NOT NULL, server default `'regex'`).
- `entity_classifications`: `id`, `entity_id` (FK `entities.id` ON DELETE CASCADE), `source`, `entity_type`, `entity_subtype`, `confidence` (Float), `probabilities` (JSONB), `model`, `input_tokens` (Float), `created_at`, `created_by`, `note`, `ignored_at` (timestamptz NULL).
- `entity_review_flags`: `id`, `entity_id` (FK CASCADE), `flagged_by`, `reason`, `created_at`, `resolved_at`, `resolved_by`, `resolution`.
- `classification_runs`: `id`, `origin`, `model`, `started_at`, `finished_at`, `names_sent`, `batches_ok`, `batches_failed` (Integer default 0), `input_tokens` (BigInteger default 0), `error` (Text NULL).
- CHECK constraints (literal values frozen in the migration):
  - `ck_entities_subtype`: `entity_subtype IS NULL OR IN (7 subtypes)`.
  - `ck_entities_classification_source`: `IN ('regex','jev','manual')`.
  - `ck_ec_source`: same set.
  - `ck_ec_entity_type`: `IN ('person','organization','not_a_name')`.
  - `ck_ec_subtype`: as for `entities`.
  - `ck_classification_runs_origin`: `IN ('post_scrape','post_import','cli','admin')`.
- Indexes:
  - `idx_entities_subtype`.
  - `idx_ec_entity_source (entity_id, source, id)`.
  - `idx_ec_source_model (source, model)`.
  - `idx_erf_open` on `entity_id` with `postgresql_where=resolved_at IS NULL`.
  - `idx_classification_runs_started (started_at)`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_models.py
def test_classification_tables_present():
    for name in ("entity_classifications", "entity_review_flags", "classification_runs"):
        assert name in metadata.tables
    assert {"entity_subtype", "classification_source"} <= set(entities.c.keys())

# tests/test_schema.py (pg_conn)
async def test_entity_subtype_check_rejects_unknown_value(pg_conn):  # IntegrityError on entity_subtype='bogus'
async def test_classification_source_defaults_to_regex(pg_conn):     # bare insert -> 'regex'
async def test_entity_delete_cascades_classifications_and_flags(pg_conn)
async def test_runs_origin_check(pg_conn):                           # origin='weekly' -> IntegrityError
```

- [ ] **Step 2: Run** `uv run pytest tests/test_models.py tests/test_schema.py -v`. Expected: FAIL (missing tables/columns).
- [ ] **Step 3: Write the migration and models.** Follow the docstring style of `0008_relayed_scrape.py`. `downgrade()` drops the three tables, the indexes, the constraints and the two columns.
- [ ] **Step 4: Run** the same tests, then `uv run alembic upgrade head && uv run alembic downgrade -1 && uv run alembic upgrade head` against the test DB. Expected: PASS; the round-trip is clean.
- [ ] **Step 5: Commit**: `#206 feat: schema for entity classification sources, review flags, runs`

### Task 1.2: Regex subtypes and a regex row on create

**Files:**
- Modify: `src/wslcb_licensing_tracker/entities.py`:
  - `_ENTITY_REPROCESS_VERSION` → `4`.
  - Split `_GOVERNMENT_TRIBE` (~L218) into `_TRIBE` and `_GOVERNMENT`.
  - In `_CATEGORY_CHECKS` (~L402), replace `government_tribe` with `("tribe", …)` then `("government", …)` at the same position.
  - Rewrite `get_or_create_entity` (~L428).
- Test: `tests/test_entities_classify.py` (update the `test_category` params), `tests/test_entities.py`

**Interfaces:**
- Produces:
  - `REGEX_SUBTYPE: dict[str, str]` = `{trust: trust, estate: estate, tribe: tribe, government: government, legal_suffix: business, truncated_suffix: business, org_keyword: business}`.
  - `REGEX_MODEL: str = str(_ENTITY_REPROCESS_VERSION)`.
  - `regex_classification(name: str) -> tuple[str, str | None]`.
  - `get_or_create_entity(conn, name: str) -> int`. The unused `entity_type` parameter is removed (no callers pass it). On a real insert, it writes `entity_type`/`entity_subtype`/`classification_source='regex'` plus one `entity_classifications` row (`source='regex'`, `model=REGEX_MODEL`).
- `_TRIBE` tokens: `TRIBES?`, `TRIBAL`, `INDIANS`, `CONFEDERATED`, `RESERVATION`, `INDIAN NATION`, `NATION OF`, `BAND OF`. `NATION OF` and `BAND OF` are taken out of the shared `(?:CITY|TOWN|…|NATION|BAND|CHAMBER) OF` group. Every other existing `_GOVERNMENT_TRIBE` alternative goes to `_GOVERNMENT`, including `CHAMBER OF`. Precedence is tribe before government, so `CONFEDERATED TRIBES … COUNTY` is a tribe.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.parametrize(("name", "expected"), [
    ("SMITH FAMILY TRUST", ("organization", "trust")),
    ("DARRYL R ESTATE WILCOX", ("organization", "estate")),
    ("GLEASON & KOATZ LLP", ("organization", "business")),
    ("FAVIA ERICKSON WINEGROWERS, LL", ("organization", "business")),
    ("FORGE CELLARS", ("organization", "business")),
    ("CITY OF LONGVIEW", ("organization", "government")),
    ("LOWER ELWHA KLALLAM TRIBE", ("organization", "tribe")),
    ("CONFEDERATED TRIBES OF THE COLVILLE RESERVATION", ("organization", "tribe")),
    ("JOHN DOE", ("person", None)),
])
def test_regex_classification(name, expected):
    assert regex_classification(name) == expected

def test_regex_never_claims_nonprofit():
    assert "nonprofit" not in REGEX_SUBTYPE.values()

# test_entities.py (pg_conn)
async def test_create_writes_regex_row_and_resolved_columns(pg_conn):
    eid = await get_or_create_entity(pg_conn, "LOWER ELWHA KLALLAM TRIBE")
    # entities row == ("organization", "tribe", "regex")
    # exactly one entity_classifications row: source regex, model "4", confidence None
    await get_or_create_entity(pg_conn, "lower elwha klallam tribe")
    # still exactly one row
```

In `test_category`, change the expected values to `government` / `tribe` for the two `government_tribe` cases.

- [ ] **Step 2: Run** `uv run pytest tests/test_entities_classify.py tests/test_entities.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.** Write the regex row only when the insert's `RETURNING` produced an id, not on the fallback-SELECT path.
- [ ] **Step 4: Run** the same tests plus `uv run python docs/research/person-vs-org/evaluate.py regex`. Expected: PASS. The gold-set binary scores are unchanged from §9 (0/300, 0/92, 182/198, 36/39, 87/87). Revert the `predictions.csv` rewrite if only the column was touched with identical values.
- [ ] **Step 5: Commit**: `#206 feat: regex entity subtypes; regex classification row on create`

### Task 1.3: The resolver

**Files:**
- Create: `src/wslcb_licensing_tracker/entity_classification.py`
- Modify: `src/wslcb_licensing_tracker/config.py`; `.env.example` (new `# ── Entity classification (Jev, optional) ──` section with the threshold line)
- Test: `tests/test_entity_classification.py` (new), `tests/test_config.py`

**Interfaces:**
- Produces in `config.py`:
  - `ENV_PREFIX = "CO_WSLCB_LICENSING_TRACKER_"`.
  - `JEV_USD_PER_MILLION_INPUT_TOKENS = 0.042`.
  - `jev_confidence_threshold() -> float`: read at call time. Default `0.7`. A non-numeric value, or one outside `[0, 1]`, logs a WARNING and returns `0.7`.
- Produces in `entity_classification.py`:
  - Constants: `SOURCES = ("regex", "jev", "manual")`, `ENTITY_SUBTYPES` (the 7), `JEV_ORG_SUBTYPES = ("business", "nonprofit", "tribe", "government")`, `NOT_A_NAME = "not_a_name"`, `JEV_TRUSTED_ORG_SUBTYPES: frozenset[str] = frozenset()` (set in Task 5.2).
  - `@dataclass(frozen=True) SourceAnswer(entity_type: str, entity_subtype: str | None, confidence: float | None = None, model: str | None = None)`.
  - `@dataclass(frozen=True) ResolutionChange(entity_id: int, old: tuple[str, str | None, str], new: tuple[str, str | None, str])` and `ResolveResult(examined: int, changes: list[ResolutionChange])`.
  - `choose_classification(manual: SourceAnswer | None, jev: SourceAnswer | None, regex: SourceAnswer | None, *, threshold: float, trusted_org_subtypes: frozenset[str] = JEV_TRUSTED_ORG_SUBTYPES) -> tuple[str, str | None, str] | None`.
  - `async current_answers(conn, entity_ids: Sequence[int] | None = None) -> dict[int, dict[str, SourceAnswer]]`: the latest (`max(id)`) row per source with `ignored_at IS NULL`.
  - `async resolve_entities(conn, entity_ids: Sequence[int] | None = None) -> ResolveResult`: updates only the rows whose triple changed; when `entity_ids` is given, chunks of 1000.
  - `async record_manual(conn, entity_id: int, *, entity_type: str, entity_subtype: str | None, created_by: str, note: str | None = None) -> int`: validates, inserts, resolves, returns the row id. Raises `ValueError` for: person with a subtype; organization with a subtype missing or not in `ENTITY_SUBTYPES`; `not_a_name` with a subtype; any other type. Raises `LookupError` when the entity doesn't exist.
  - `async clear_manual(conn, entity_id: int) -> int`: sets `ignored_at` on the non-ignored manual rows, resolves, returns the count.
  - `async protected_ids(conn) -> set[int]`: a current manual row of type ≠ `not_a_name`, **or** an open flag.
  - `async confirmed_noise_ids(conn) -> set[int]`: a current manual row of type `not_a_name` and no open flag.
- Rule for `choose_classification` (a body is given because the subtype fallback is not implied by the signature):

```python
if manual and manual.entity_type != NOT_A_NAME:
    return manual.entity_type, manual.entity_subtype, "manual"
if jev and jev.entity_type in ("person", "organization") and (jev.confidence or 0) >= threshold:
    sub = jev.entity_subtype
    if jev.entity_type == "organization" and (
        sub == "unknown" or (sub in JEV_ORG_SUBTYPES and sub not in trusted_org_subtypes)
    ):
        sub = regex.entity_subtype if regex and regex.entity_type == "organization" and regex.entity_subtype else "unknown"
    return jev.entity_type, sub, "jev"
if regex:
    return regex.entity_type, regex.entity_subtype, "regex"
return None
```

- [ ] **Step 1: Write the failing tests**

```python
R_PERSON = SourceAnswer("person", None); R_BIZ = SourceAnswer("organization", "business")
@pytest.mark.parametrize(("manual", "jev", "regex", "trusted", "expected"), [
    (SourceAnswer("person", None), SourceAnswer("organization", "business", .99), R_BIZ, frozenset(), ("person", None, "manual")),
    (SourceAnswer("not_a_name", None), None, R_PERSON, frozenset(), ("person", None, "regex")),
    (None, SourceAnswer("organization", "trust", .70), R_PERSON, frozenset(), ("organization", "trust", "jev")),
    (None, SourceAnswer("organization", "business", .69), R_PERSON, frozenset(), ("person", None, "regex")),
    (None, SourceAnswer("not_a_name", None, .99), R_PERSON, frozenset(), ("person", None, "regex")),
    (None, SourceAnswer("organization", "nonprofit", .95), R_BIZ, frozenset(), ("organization", "business", "jev")),
    (None, SourceAnswer("organization", "nonprofit", .95), R_PERSON, frozenset(), ("organization", "unknown", "jev")),
    (None, SourceAnswer("organization", "nonprofit", .95), R_BIZ, frozenset({"nonprofit"}), ("organization", "nonprofit", "jev")),
    (None, SourceAnswer("organization", "unknown", .95), R_BIZ, frozenset(), ("organization", "business", "jev")),
    (None, SourceAnswer("organization", "unknown", .95), R_PERSON, frozenset(), ("organization", "unknown", "jev")),
    (None, SourceAnswer("person", None, .9), R_BIZ, frozenset(), ("person", None, "jev")),
    (None, None, None, frozenset(), None),
])
def test_choose_classification(manual, jev, regex, trusted, expected): ...

# PG (pg_conn); seed jev rows by inserting into entity_classifications directly
async def test_resolve_entities_applies_jev_and_reports_changes(pg_conn)
async def test_ignored_jev_row_is_skipped(pg_conn)
async def test_latest_manual_row_wins_and_history_kept(pg_conn)
async def test_clear_manual_returns_to_automatic_and_keeps_rows(pg_conn)  # rows keep ignored_at set
async def test_record_manual_rejects_bad_combinations(pg_conn)          # ValueError x4, LookupError for missing id; no row written
async def test_threshold_env_changes_resolution(pg_conn, monkeypatch)    # 0.9 -> jev .8 falls back to regex
async def test_protected_and_confirmed_noise_ids(pg_conn)
# tests/test_config.py
def test_jev_confidence_threshold_default_valid_invalid(monkeypatch, caplog)  # unset 0.7; "0.85"; "abc"->0.7+WARNING; "1.5"->0.7
```

- [ ] **Step 2: Run** `uv run pytest tests/test_entity_classification.py tests/test_config.py -v`. Expected: FAIL (ImportError).
- [ ] **Step 3: Implement** `entity_classification.py`. It must not import `entities.py`, which imports it in Task 1.4. Add the config helpers.
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#206 feat: classification resolver (manual > jev >= threshold > regex)`

### Task 1.4: Reclassify, purge and merge through the resolver

**Files:**
- Modify: `src/wslcb_licensing_tracker/entities.py`: `reclassify_entities` (~L773), `_plan_reclassification`, `_apply_type_changes` (delete it), `merge_duplicate_entities` (~L586).
- Modify: `src/wslcb_licensing_tracker/cli.py` `reclassify_entities` echo (~L528).
- Modify: `AGENTS.md` data-contract table.
- Modify: `docs/SCHEMA.md` `entities` bullets.
- Test: `tests/test_entities.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: `regex_classification`, `REGEX_MODEL` (1.2); `current_answers`, `resolve_entities`, `protected_ids`, `confirmed_noise_ids` (1.3).
- Produces: `reclassify_entities(conn, *, dry_run=False, sample_limit=25) -> dict`. The existing keys stay (`entities_examined`, `reclassified` = number of resolved `entity_type` changes, `transitions`, `reclassified_sample` items `{id, name, old, new, category}`, `noise_entities`, `noise_links`, `noise_sample`). New keys: `regex_rows_written`, `subtype_changes`, `protected_noise_skipped`, `confirmed_noise`.
- Behaviour:
  1. For each entity whose name is not noise, insert a regex row when the current regex row is missing or differs in `(entity_type, entity_subtype, model)` (`SourceAnswer.model`).
  2. `resolve_entities(conn)` over all entities. It never writes `entities.entity_type` directly.
  3. Purge set = (`is_noise_name` ids − `protected_ids`) ∪ `confirmed_noise_ids`, via `_purge_entities`.
  4. **Dry-run** runs the same code inside `conn.begin_nested()` and rolls the savepoint back.
- `merge_duplicate_entities`: before deleting a duplicate, `UPDATE` its `source='manual'` rows and all its flags to the canonical id. After the loop, `resolve_entities(conn, canonical_ids)`.
- AGENTS.md table: replace the `entities.entity_type` row with ``entity_classifications` regex rows + resolved `entities.entity_type/entity_subtype/classification_source` | `entities.reclassify_entities()` | `wslcb db reclassify-entities``. Add one sentence below the table: "`entity_classifications` jev rows are derived but not freely regenerable (cost, ~1% drift); treat them like `co_replay` sources: never bulk-delete, roll back with `wslcb db ignore-jev`. Manual rows and `entity_review_flags` are operator input; no automated pass changes or deletes them."

- [ ] **Step 1: Write the failing tests** (pg_conn; reuse `_seed_entity`)

```python
async def test_reclassify_writes_regex_rows_and_resolves(pg_conn)        # seeded 'person' LLP -> organization/business/regex; regex_rows_written>=1; 2nd run: regex_rows_written==0, reclassified==0
async def test_reclassify_never_overrides_manual(pg_conn)                # manual person on 'GLEASON & KOATZ LLP' stays person/manual after run
async def test_noise_purge_skips_manual_and_flagged(pg_conn)             # 'TRUSTEE' with manual person row, '035' with open flag: both survive; protected_noise_skipped>=2
async def test_confirmed_noise_is_purged(pg_conn)                        # manual not_a_name on 'JOHN DOE XYZ' -> deleted, links renumbered; confirmed_noise>=1
async def test_reclassify_dry_run_writes_nothing(pg_conn)                # counts reported; no new rows; types unchanged
async def test_reprocess_entities_leaves_classification_untouched(pg_conn)  # manual row + resolved columns identical after reprocess_entities(record_id=…)
async def test_merge_moves_manual_rows_and_flags(pg_conn)                # dup with manual org/nonprofit + open flag -> canonical has both; canonical resolved manual
```

Existing `TestReclassifyEntities` tests keep passing. In `test_cli.py`, extend `test_reclassify_entities_dry_run` so the output contains `regex row(s)` and `protected`.

- [ ] **Step 2: Run** `uv run pytest tests/test_entities.py tests/test_cli.py -v`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement.** CLI echo adds: `{regex_rows_written:,} regex row(s) written; {subtype_changes:,} subtype change(s).` and `Kept {protected_noise_skipped:,} noise-named entities with a manual decision or open flag; {confirmed_noise:,} operator-confirmed non-names removed.`
- [ ] **Step 4: Run** `uv run pytest tests/ -v`. Expected: PASS.
- [ ] **Step 5: Commit**: `#206 feat: reclassify/merge/purge go through the resolver and protect manual decisions`

### Task 1.5: Jev rollback command

**Files:**
- Modify: `src/wslcb_licensing_tracker/entity_classification.py`, `src/wslcb_licensing_tracker/cli.py` (db group; module docstring list)
- Test: `tests/test_entity_classification.py`, `tests/test_cli.py`

**Interfaces:**
- Produces: `async set_jev_ignored(conn, *, ignored: bool, model: str | None = None) -> dict` returning `{"rows": int, "entities": int, "changed": int}`. It sets `ignored_at = now()` (or NULL when un-ignoring) on `source='jev'` rows only, optionally filtered by `model`, then resolves the affected entities.
- CLI: `wslcb db ignore-jev [--model ID] [--undo] [--dry-run]` (dry-run uses a savepoint). Output: `Ignored|Restored N jev row(s) across M entities; K resolved classification(s) changed.`

- [ ] **Step 1: Write the failing tests**: `test_ignore_and_restore_jev_rows` (jev-resolved entity flips to regex, then back; manual and regex rows' `ignored_at` untouched); `test_ignore_by_model_only_touches_that_model`; CLI `test_ignore_jev_passes_options` (mock; `--undo` → `ignored=False`).
- [ ] **Step 2: Run** `uv run pytest tests/test_entity_classification.py tests/test_cli.py -k jev -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#206 feat: wslcb db ignore-jev rollback (reversible, deletes nothing)`

### Task 1.6: ⚠ OPERATOR-GATED: production migration + regex backfill

Run this right after Phase 1 merges, and before deploying any later phase. Rollout step 1.

- [ ] **Step 1: Back up.** `sudo systemctl start wslcb-backup.service; journalctl -u wslcb-backup -n 20 -o cat --no-pager`. Expected: the run succeeds. Then `mkdir -p data/remediation-backups/<date>-206` and `\copy` `entities` and `record_entities` as JSON lines into it, as for `2026-10-22-203/`.
- [ ] **Step 2: Counts before.** Save `SELECT entity_type, count(*) FROM entities GROUP BY 1` to `counts_before.txt`.
- [ ] **Step 3: Migrate.** `uv run alembic upgrade head`. Expected: `alembic current` = `0009`.
- [ ] **Step 4: Dry-run.** `uv run wslcb db reclassify-entities --dry-run | tee …/dry-run.txt`. Expected: `regex_rows_written` ≈ the entity count (~67K); `reclassified` 0 (the #203 rules are unchanged); the noise counts match #203's residue (≈0). Stop and report if `reclassified` is not 0.
- [ ] **Step 5: Apply.** `uv run wslcb db reclassify-entities | tee …/apply.txt`, then `sudo systemctl restart wslcb-web.service`.
- [ ] **Step 6: Verify.** `SELECT count(*) FROM entities e WHERE NOT EXISTS (SELECT 1 FROM entity_classifications c WHERE c.entity_id=e.id AND c.source='regex')` = 0. Save the type × subtype × source counts to `counts_after.txt`. A second dry-run reports 0 everywhere. `/api/v1/health` returns 200.
- [ ] **Step 7: Report** the before/after counts on the Phase 1 child issue.

---

## Phase 2 — Jev client, CLI, timer, monitoring (depends on Phase 1)

### Task 2.1: `jev_client.py` transport

**Files:**
- Create: `src/wslcb_licensing_tracker/jev_client.py`
- Modify: `.env.example` (the remaining Jev vars and the CLASSIFY check-in vars, under the section from 1.3)
- Test: `tests/test_jev_client.py` (new)

**Interfaces:**
- Produces:
  - Constants: `URL`, `DEFAULT_MODEL = "jev-1.14.0"`, `BATCH_SIZE = 25`, `CONCURRENCY = 8`, `TIMEOUT = 60.0`, `MAX_ATTEMPTS = 6`, `RETRY_STATUSES = frozenset({429, 529})` (sleep `2**attempt` s), `CONTEXT` (= `evaluate.CTX`), `KIND_CRITERIA` (= `evaluate.FIVE`), `ORG_CRITERIA` (below).
  - Env getters, all read at call time with `ENV_PREFIX`: `is_classification_enabled() -> bool` (`1/true/yes`), `get_api_key() -> str`, `get_model() -> str`, `max_names_per_run() -> int` (default 5000; an invalid or < 1 value → WARNING + 5000).
  - `build_request(names: Sequence[str], model: str) -> dict`: `state = {"context": CONTEXT, "names": {"n0": …}}`, and per name two questions:
    - `kind_<i>`: `{"type": "choice", "instructions": "What kind of entity does \`names.n<i>\` refer to?", "criteria": KIND_CRITERIA}`;
    - `org_<i>`: `"If \`names.n<i>\` is an organization, which kind of organization is it?"` with `ORG_CRITERIA`.
  - `@dataclass(frozen=True) JevAnswer(kind: str, kind_confidence: float, org: str, org_confidence: float, raw: dict)`. `raw = {"kind": <answer obj>, "org": <answer obj>}`.
  - `@dataclass(frozen=True) JevBatch(model: str, input_tokens: int, answers: list[JevAnswer])`, with answers in input order.
  - `class JevError(Exception)` with `status: int | None` and the property `auth: bool` (status 401/403).
  - `async classify_batch(names: Sequence[str], *, api_key: str, model: str, client: httpx.AsyncClient, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> JevBatch`. Raises `ValueError` when `len(names)` is 0 or > 25. Raises `JevError` for: a non-retryable status; retries exhausted; `httpx.TransportError`; a malformed response (missing key, choice outside the criteria, confidence not in [0, 1]). It never logs the key.
- `ORG_CRITERIA` (new; Task 5.1 validates it; changing it later means re-scoring):
  *(2026-10-23, #209: the block below is the original draft. It was reworded on the operator's instruction (#210); the shipped text is `jev_client.ORG_CRITERIA`, validated in research §10.)*

```python
ORG_CRITERIA = {
    "business": "A for-profit company or business of any size, including stores, bars, restaurants, "
    "wineries, breweries and holding companies (e.g. FAMILY DOLLAR, LIBERTY BAR, ACME HOLDINGS LLC)",
    "nonprofit": "A nonprofit, charity, club, fraternal lodge, veterans post, church, museum, theater "
    "or civic association (e.g. ELKS LODGE 1234, AMERICAN LEGION POST 12)",
    "tribe": "A Native American tribe, band or nation, or a tribal government "
    "(e.g. LOWER ELWHA KLALLAM TRIBE, CONFEDERATED TRIBES OF THE COLVILLE RESERVATION)",
    "government": "A city, town, county, state, port, public school or university, public utility, "
    "or other government agency (e.g. CITY OF LONGVIEW, PORT OF SEATTLE)",
}
```

- [ ] **Step 1: Write the failing tests** (`httpx.MockTransport`; recorded `sleep`)

```python
def test_wording_matches_research():  # importlib-load docs/research/person-vs-org/evaluate.py
    assert jev_client.KIND_CRITERIA == ev.FIVE and jev_client.CONTEXT == ev.CTX
def test_build_request_shape():       # 2 names -> keys {kind_0, org_0, kind_1, org_1}; state.names == {"n0":…, "n1":…}; "names.n1" in kind_1 instructions; model echoed
async def test_classify_batch_parses_in_order():  # asserts Authorization == "Bearer k"; tokens; answers[1].kind == "trust"
async def test_retries_429_and_529():  # 429, 529, 200 -> sleeps == [1, 2]
async def test_gives_up_after_max_attempts():  # always 429 -> JevError, 6 calls
async def test_other_status_not_retried():  # 500 -> JevError(status=500), 1 call
async def test_401_is_auth_error():  # .auth is True
async def test_malformed_response_raises():  # parametrize: missing org_0; choice "robot"; confidence 1.7
async def test_transport_error_raises():
async def test_batch_size_bounds():  # [] and 26 names -> ValueError, no call
def test_env_getters(monkeypatch):  # enabled false by default; "1" true; model default; max_names "x" -> 5000
```

- [ ] **Step 2: Run** `uv run pytest tests/test_jev_client.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.** Mirror `address_client.py`'s module layout and its "pure transport, no DB" docstring.
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#207 feat: jev_client transport (25-name batches, kind + org questions)`

### Task 2.2: `jev_classifier.py` orchestration

**Files:**
- Create: `src/wslcb_licensing_tracker/jev_classifier.py`
- Test: `tests/test_jev_classifier.py` (new). Use **`pg_engine`**: `classify_entities` commits per batch. Seed entities with a unique `JEVTEST …` prefix and delete them, plus their `classification_runs`, in teardown.

**Interfaces:**
- Consumes: `jev_client.*` (2.1); `resolve_entities` (1.3); `config.jev_confidence_threshold`, `JEV_USD_PER_MILLION_INPUT_TOKENS`.
- Produces:
  - Types: `Mode = Literal["unclassified", "refresh_all", "refresh_model", "ids"]`; `Origin = Literal["post_scrape", "post_import", "cli", "admin"]`.
  - Constants: `EST_INPUT_TOKENS_PER_NAME = 238` (kind-only measurement; Task 5.2 replaces it); `MAX_CONSECUTIVE_FAILURES = 3`; `ADVISORY_LOCK_KEY = 0x6A6576` (any fixed bigint).
  - `@dataclass RunSummary(origin: str, enabled: bool, key_present: bool, model: str, candidates: int = 0, names_sent: int = 0, batches_ok: int = 0, batches_failed: int = 0, input_tokens: int = 0, dry_run: bool = False, skipped_locked: bool = False, error: str | None = None, run_id: int | None = None)` with the property `estimated_cost_usd -> float`. The property uses `input_tokens`, or `candidates × EST_INPUT_TOKENS_PER_NAME` on a dry-run.
  - `map_answer(answer: JevAnswer, *, threshold: float) -> tuple[str, str | None]`:
    - trust → `("organization", "trust")`; estate → `("organization", "estate")`;
    - organization → `("organization", org)` if `org_confidence ≥ threshold`, else `("organization", "unknown")`;
    - person → `("person", None)`; not_a_name → `("not_a_name", None)`.
  - `async select_candidates(conn, *, mode: Mode, model: str | None = None, ids: Sequence[int] | None = None, include_manual: bool = False, limit: int | None = None) -> list[tuple[int, str]]`:
    - `unclassified`: no `jev` row at all (ignored rows count as classified);
    - `refresh_all`: every entity;
    - `refresh_model`: has a jev row with that `model`;
    - `ids`: those ids.
    - Every mode excludes entities with a current (non-ignored) manual row unless `include_manual`. Ordered by id.
  - `async awaiting_count(conn) -> int`: the `unclassified` candidates without the limit.
  - `async store_batch(conn, entities: Sequence[tuple[int, str]], batch: JevBatch, *, threshold: float) -> list[int]`. It inserts jev rows with `INSERT … SELECT … WHERE EXISTS (entity)` and returns the stored ids. Per row: `confidence = kind_confidence`, `probabilities = answer.raw`, `model = batch.model`, `input_tokens = batch.input_tokens / len(entities)`.
  - `async classify_entities(conn, *, origin: Origin, mode: Mode = "unclassified", model_filter: str | None = None, ids: Sequence[int] | None = None, include_manual: bool = False, limit: int | None = None, dry_run: bool = False, client: httpx.AsyncClient | None = None, concurrency: int = CONCURRENCY) -> RunSummary`. Order of checks:
    1. **dry-run**: count only. It runs whatever the flag or key says, makes no calls and writes no run row.
    2. **flag off**: return `enabled=False`. No calls, no run row.
    3. **key empty**: write a finished run row with `error="missing_api_key"`.
    4. **advisory lock not acquired** (`pg_try_advisory_lock`): `skipped_locked=True`.
    5. **Otherwise**: insert a run row, commit, chunk by 25, and post with a semaphore of `concurrency`. Results are handled in completion order on the one connection. Each OK batch gets `store_batch` + `resolve_entities` + commit. On `JevError`, `batches_failed += 1` and a WARNING is logged.
    6. **Stopping**: an `.auth` error stops at once (`error="auth_failed"`). `MAX_CONSECUTIVE_FAILURES` stops with `error="consecutive_failures"`. Pending tasks are cancelled.
    7. **Finally**: update the run row and unlock.
    - `limit=None` means `max_names_per_run()`.

- [ ] **Step 1: Write the failing tests.** A fake transport answers by name: names containing `LLC` → organization .95 / business .9; `LOWCONF` → person .4; `NOISE` → not_a_name .9; otherwise person .97.

```python
def test_map_answer_table(): ...  # the five mappings, incl. org_confidence .5 -> "unknown"
async def test_dry_run_counts_without_calls_even_when_disabled(pg_engine)  # transport never called; estimated_cost_usd == n*238*0.042/1e6
async def test_disabled_makes_no_calls_and_no_run_row(pg_engine)
async def test_missing_key_records_failed_run(pg_engine)                  # run row error == "missing_api_key"; 0 calls
async def test_classifies_once(pg_engine)                                 # 2nd run names_sent == 0; rows carry confidence, probabilities.kind, model, input_tokens
async def test_bulk_modes_skip_manual_but_admin_may_query(pg_engine)      # unclassified/refresh_all/refresh_model/ids skip it; include_manual=True sends it; manual still wins resolution
async def test_refresh_model_selects_only_that_model(pg_engine)
async def test_ignored_jev_rows_are_not_resent(pg_engine)
async def test_failed_batch_stores_nothing_and_is_retried(pg_engine)      # 500 then 200 across two runs
async def test_auth_failure_stops_run(pg_engine)                          # concurrency=1, 3 batches, 401 -> exactly 1 call; error "auth_failed"
async def test_consecutive_failures_stop_run(pg_engine)                   # concurrency=1, 5 batches, all 500 -> 3 calls
async def test_overlapping_run_is_skipped(pg_engine)                      # hold the lock on a 2nd connection -> skipped_locked, 0 calls
async def test_store_batch_skips_deleted_entity(pg_engine)                # delete one id first -> no error, it is absent from the result
async def test_resolution_and_not_a_name(pg_engine)                       # LLC -> source jev; NOISE -> jev row stored, entity stays regex
async def test_run_row_totals(pg_engine)                                  # names_sent, batches_ok, input_tokens, finished_at set
```

- [ ] **Step 2: Run** `uv run pytest tests/test_jev_classifier.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#207 feat: jev_classifier: classify-once batching, run records, advisory lock`

### Task 2.3: `wslcb db classify-entities`

**Files:**
- Modify: `src/wslcb_licensing_tracker/cli.py`: the command, an `_ALIASES` entry `(classify_entities, "classify-entities")`, and the module docstring.
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: `classify_entities`, `RunSummary` (2.2).
- Produces the CLI `wslcb db classify-entities [--refresh-all | --refresh-model ID | --ids-file PATH] [--limit N] [--dry-run]`.
  - The three refresh options are mutually exclusive (`click.UsageError`).
  - `--ids-file`: newline-separated ints → `mode="ids"`.
  - `origin="cli"`. `--limit` overrides the cap; for the one-off backfill pass a number above the entity count.
  - Output:
    - `[dry-run] Would send N name(s) in B request(s); est. $X.XX` with 4 decimals;
    - `Jev classification disabled (…ENABLE_JEV_CLASSIFICATION); nothing sent.`;
    - `Sent N name(s): B ok, F failed batch(es); T tokens (~$X.XXXX); model M.`;
    - `Skipped: another classification run holds the lock.`
  - Exit 1 when `summary.error` is set or `batches_failed > 0`.

- [ ] **Step 1: Write the failing tests** (patch `wslcb_licensing_tracker.cli.run_classify_entities`):
  - `test_classify_entities_default` (kwargs `mode="unclassified", origin="cli"`);
  - `test_refresh_options_exclusive` (exit 2);
  - `test_ids_file` (`ids=[3, 5]`);
  - `test_dry_run_output`;
  - `test_disabled_exit_0`;
  - `test_failed_batches_exit_1`;
  - `test_alias_registered` (`"classify-entities" in main.commands`).
- [ ] **Step 2: Run** `uv run pytest tests/test_cli.py -k classify -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#207 feat: wslcb db classify-entities (default, refresh, dry-run, limit)`

### Task 2.4: CLASSIFY monitor, weekly check-in, post-ingest hooks

**Files:**
- Modify: `src/wslcb_licensing_tracker/checkin.py` (the `CLASSIFY` monitor; docstring "Three jobs").
- Modify: `tests/conftest.py` `_no_configured_checkin` (add `CLASSIFY`).
- Modify: `src/wslcb_licensing_tracker/jev_classifier.py` (`checkin_payload`).
- Modify: `src/wslcb_licensing_tracker/cli.py`: the `scrape` `_run` (~L160) and the `import_relayed` `_run` (~L206) call `_post_ingest_classification` **before** the address backfill. `classify-entities` checks in when run in default mode.
- Test: `tests/test_jev_classifier.py`, `tests/test_cli.py`, `tests/test_cli_scrape.py`, `tests/test_cli_relay.py`, `tests/test_checkin.py`

**Interfaces:**
- Produces:
  - `checkin.CLASSIFY = Monitor("classify", "CO_WSLCB_LICENSING_TRACKER_CLASSIFY_CHECKIN_BASE_URL", "CO_WSLCB_LICENSING_TRACKER_CLASSIFY_MONITOR_ID")`.
  - `jev_classifier.BACKLOG_ALERT_THRESHOLD = 1000`.
  - `checkin_payload(summary: RunSummary, *, backlog: int, host: str) -> tuple[Literal["ok", "alert"], dict, dict | None]`. Every payload carries `outcome` (`ok`/`failed`), `source_host`, `enabled` (`"true"`/`"false"`), `model`, `names_sent`, `batches_ok`, `batches_failed`, `backlog`, `error` (`""` when none). Rules, first match wins:
    1. flag off → `ok`, metadata None;
    2. `error` set → `alert`, `{"fault": error}`;
    3. `batches_failed > 0` → `alert`, `{"fault": "batches_failed"}`;
    4. `backlog > BACKLOG_ALERT_THRESHOLD` → `alert`, `{"fault": "backlog"}`;
    5. else `ok`. A `skipped_locked` run is `ok`.
  - `cli._post_ingest_classification(engine, *, origin: Origin, alert: bool) -> None`. It never raises. On an exception it logs a WARNING with `exc_info`; if `alert`, it posts `alert` with fault `type(exc).__name__`. Otherwise it builds `checkin_payload(summary, backlog=0, host=…)` and posts **only** when the status is `alert` and `alert=True`. Scrape passes `alert=True`; import-relayed passes `alert=False`.
  - Default-mode `classify-entities` (no refresh option, not dry-run) posts the payload with `backlog=await awaiting_count(conn)` via `asyncio.to_thread(post_checkin, …, monitor=CLASSIFY, environ=os.environ)`. If `classify_entities` raises, it posts `alert` (fault = exception type) and re-raises.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.parametrize("case", ["disabled_ok", "missing_key", "auth_failed", "batches_failed", "backlog", "clean_ok", "locked_ok"])
def test_checkin_payload(case): ...  # status, metadata.fault, and every variable key present
# test_cli.py
def test_default_run_checks_in_ok_once(...)          # post_checkin called once: ("ok", monitor=CLASSIFY)
def test_dry_run_and_refresh_never_check_in(...)
def test_default_run_exception_alerts_and_fails(...)
# test_cli_scrape.py
def test_scrape_runs_classification_and_survives_its_failure(...)  # classify raises -> exit 0; CLASSIFY alert posted
def test_scrape_classification_success_sends_no_classify_checkin(...)
# test_cli_relay.py
def test_import_relayed_classifies_without_any_checkin(...)  # failure swallowed; post_checkin never called with CLASSIFY
```

- [ ] **Step 2: Run** `uv run pytest tests/test_jev_classifier.py tests/test_cli.py tests/test_cli_scrape.py tests/test_cli_relay.py tests/test_checkin.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `uv run pytest tests/ -v`. Expected: PASS.
- [ ] **Step 5: Commit**: `#207 feat: classify check-ins (weekly ok/alert, post-scrape alert-only) and post-ingest hooks`

### Task 2.5: Weekly timer, drop-in, docs

**Files:**
- Create: `infra/wslcb-entity-classification.timer`: `Unit=wslcb-task@classify-entities.service`, `OnCalendar=Sun *-*-* 04:00:00 America/Los_Angeles`, `Persistent=true`, `RandomizedDelaySec=300`, `WantedBy=timers.target`.
- Create: `infra/wslcb-task@classify-entities.service.d/checkin.conf`: `[Unit] After=tailscaled.service`; `[Service] LoadCredential=checkin-key:/etc/wslcb-licensing-tracker/classify-checkin.key`, `TimeoutStartSec=1h`. Comment it like the scrape drop-in.
- Create: `tests/test_infra_classify_unit.py`, modelled on `tests/test_infra_scrape_unit.py`.
- Modify: `docs/MONITORING.md`: a table row, plus a section `## Entity classification check-in` covering the monitor `co-wslcb-classify` and its fields:
  - `interval_seconds` `604800`; `grace_seconds` `10800` (jitter + DST hour + run);
  - `renotify_seconds` `86400`;
  - `title_template` `wslcb classify {{ outcome }} on {{ source_host }}`;
  - `body_template` `{{ error }} — enabled={{ enabled }}, {{ batches_failed }} failed batch(es), backlog {{ backlog }}`.
  - Also the variables, the alert-only post-scrape rule, the relay's lack of a check-in, and the install order.
  - Note: post-scrape alerts reuse the scrape unit's `checkin-key`, so they need tenant-scoped keys (status#46).
- Modify: `docs/DEPLOYMENT.md`: a section `## Entity classification (Jev)` with the env vars, `classify-entities` / `ignore-jev` / `reclassify-entities` usage, the timer, and the backfill via `systemd-run` (Task 5.3).
- Modify: `AGENTS.md` Infrastructure row for `/etc/…/.env` (append "Jev classification vars") and Common Tasks (`wslcb db classify-entities [--dry-run]`).

- [ ] **Step 1: Write the failing tests**: `test_key_is_a_credential`, `test_after_tailnet_without_binding`, `test_bounded` (`TimeoutStartSec` ≤ 2h), `test_timer_targets_registered_alias` (the instance name `classify-entities` ∈ `cli.main.commands`), `test_weekly_and_monitor_window` (interval == 604800; grace ≥ `RandomizedDelaySec` + 3600 + 1800, read from MONITORING.md).
- [ ] **Step 2: Run** `uv run pytest tests/test_infra_classify_unit.py -v`. Expected: FAIL.
- [ ] **Step 3: Create** the unit files and docs.
- [ ] **Step 4: Run** `uv run pytest tests/ -v` and `systemd-analyze verify infra/wslcb-entity-classification.timer` (if available). Expected: PASS.
- [ ] **Step 5: Commit**: `#207 feat: weekly entity-classification timer, check-in drop-in, monitoring docs`

---

## Phase 3 — Admin actions, review queue, health panel (depends on Phases 1, 2)

### Task 3.1: Audited operator actions

**Files:**
- Create: `src/wslcb_licensing_tracker/classification_review.py`
- Test: `tests/test_classification_review.py` (new; pg_conn)

**Interfaces:**
- Consumes: `record_manual`, `clear_manual`, `current_answers`, `NOT_A_NAME` (1.3); `admin_audit.log_action`.
- Produces. Every function is caller-commits and calls `log_action(conn, admin_email, <action>, "entity", entity_id, details)`, with details carrying the old/new resolved triple:
  - `async set_classification(conn, entity_id, *, entity_type: str, entity_subtype: str | None, note: str | None, admin_email: str) -> int`, action `entity.classify_set`;
  - `async accept_answer(conn, entity_id, *, source: Literal["jev", "regex"], admin_email: str) -> int`, actions `entity.classify_accept_jev` / `entity.classify_accept_regex`. It copies the current answer; a Jev `not_a_name` answer delegates to `mark_not_a_name`; `LookupError` when there is no current answer for that source;
  - `async mark_not_a_name(conn, entity_id, *, admin_email: str, note: str | None = None) -> int`, action `entity.classify_not_a_name`;
  - `async clear_override(conn, entity_id, *, admin_email: str) -> int`, action `entity.classify_clear`;
  - `async flag_entity(conn, entity_id, *, reason: str, admin_email: str) -> int`, action `entity.flag`. `ValueError` on a blank reason; `LookupError` on a missing entity.
  - The first three resolve the entity's open flags: `resolved_at=now()`, `resolved_by=admin_email`, `resolution=<action>`.

- [ ] **Step 1: Write the failing tests**:
  - `test_set_classification_writes_manual_audit_and_resolves_flags`;
  - `test_accept_jev_copies_answer`;
  - `test_accept_jev_not_a_name_marks_noise`;
  - `test_accept_missing_source_raises`;
  - `test_mark_not_a_name_then_reclassify_purges` (calls `reclassify_entities`);
  - `test_clear_override_audited_and_flags_untouched`;
  - `test_flag_entity_validation`.
- [ ] **Step 2: Run** `uv run pytest tests/test_classification_review.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#208 feat: audited operator classification actions and review flags`

### Task 3.2: Queue, detail and health queries

**Files:**
- Modify: `src/wslcb_licensing_tracker/classification_review.py`
- Test: `tests/test_classification_review.py`

**Interfaces:**
- Consumes: `awaiting_count` (2.2); `jev_client.is_classification_enabled`, `get_api_key`, `get_model`; `config.jev_confidence_threshold`, `JEV_USD_PER_MILLION_INPUT_TOKENS`.
- Produces:
  - `async review_queue(conn, *, tab: Literal["main", "unknown"], limit: int = 200) -> list[dict]`. Item keys: `entity_id`, `name`, `entity_type`, `entity_subtype`, `classification_source`, `reasons` (subset of `flag`, `low_confidence`, `not_a_name`, `unknown_subtype`), `flags` (`[{id, reason, flagged_by, created_at}]`), `jev` (`{entity_type, entity_subtype, confidence, model}` or None), `regex` (`{entity_type, entity_subtype}` or None).
    - **main** = open flag (even with a manual row), **or** (no current manual row **and** current Jev row with `confidence < threshold` or type `not_a_name`).
    - **unknown** = `entities.entity_subtype='unknown'`, no current manual row, not in main.
    - One row per entity. Sorted by open flag first, then Jev confidence ascending (NULLs last), then id.
  - `async review_counts(conn) -> dict[str, int]` → `{"main": n, "unknown": n}`.
  - `async classification_detail(conn, entity_id) -> dict` → `{"current": {source: {entity_type, entity_subtype, confidence, model, created_at, created_by, note}}, "history": [rows newest first, incl. ignored, with ignored_at], "open_flags": [...]}`.
  - `async classification_health(conn, *, now: datetime) -> dict`. Keys:
    - `enabled`, `key_present`, `model`;
    - `last_success_at` (max `finished_at` where `batches_ok > 0`);
    - `awaiting`;
    - `failures_7d` (Σ `batches_failed` + count of runs with `error`, started ≥ now−7d);
    - `tokens_month` (calendar month of `now`, UTC), `tokens_all`, `spend_month_usd`, `spend_all_usd`;
    - `queue_main`, `queue_unknown`;
    - `problems: list[str]` (`"missing_api_key"` when enabled and no key; `"recent_failures"` when `failures_7d > 0`).

- [ ] **Step 1: Write the failing tests**:
  - `test_queue_membership_matrix`, which covers: flag; flag+manual; jev .5; jev not_a_name; jev .9 disagreeing with regex → absent; manual only → absent; unknown → unknown tab only;
  - `test_queue_dedups_with_both_reasons`;
  - `test_queue_order`;
  - `test_review_counts_match_queue`;
  - `test_detail_shows_each_source_and_history`;
  - `test_health_numbers` (seeded runs: tokens, spend = tokens × 0.042 / 1e6, month boundary);
  - `test_health_missing_key_problem`.
- [ ] **Step 2: Run** `uv run pytest tests/test_classification_review.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#208 feat: review queue, classification detail, health/spend queries`

### Task 3.3: Admin routes and the review page

**Files:**
- Create: `src/wslcb_licensing_tracker/admin_entity_routes.py` (`APIRouter(prefix="/admin")`, render via `admin_routes.render_admin`)
- Create: `templates/admin/entity_review.html`
- Modify: `src/wslcb_licensing_tracker/app.py` (`include_router`); `templates/admin/base.html` (nav link "Entity review", `active_section == 'entity_review'`)
- Test: `tests/test_admin_entity_routes.py` (new; mocked conn and patched functions, as in `tests/test_admin_routes.py`, with `_stamp_engine`)

**Interfaces:**
- Consumes: 3.1, 3.2; `classify_entities` (2.2); `is_classification_enabled` (2.1).
- Produces routes. All use `require_admin`. The POST forms take a `next` field; `_safe_next(next: str | None, entity_id: int) -> str` accepts only paths that start with `/` and not `//`, else `/entity/{id}`. On success: commit, then 303 to `next`. `ValueError` → 400, `LookupError` → 404, nothing committed.
  - `GET /admin/entities/review?tab=main|unknown`: tabs show counts; each item gets the action forms. Accept Jev (only when `jev`), Accept rules (only when `regex`), Set other… (type, subtype `<select>` incl. blank, note), Not a name.
  - `POST /admin/entities/{id}/classify` (fields `entity_type`, `entity_subtype`, `note`)
  - `POST /admin/entities/{id}/accept` (field `source`)
  - `POST /admin/entities/{id}/not-a-name` (field `note`)
  - `POST /admin/entities/{id}/clear-manual`
  - `POST /admin/entities/{id}/flag` (field `reason`)
  - `POST /admin/entities/{id}/jev`: 409 "Jev classification is disabled" when the flag is off. Otherwise `classify_entities(conn, origin="admin", mode="ids", ids=[id], include_manual=True, limit=1)` plus `log_action(…, "entity.reclassify_jev", …, details={summary counts, error})`. Then 303, or 502 with the error text when `summary.error` or `batches_failed`.
  - An empty `entity_subtype` form value becomes `None`.

- [ ] **Step 1: Write the failing tests**:
  - `test_review_page_renders_items`;
  - `test_classify_posts_and_redirects_to_next`;
  - `test_classify_invalid_combination_400_no_commit`;
  - `test_missing_entity_404`;
  - `test_external_next_falls_back` (`next=https://evil.example` and `//evil` → `/entity/7`);
  - `test_jev_disabled_409`;
  - `test_jev_enabled_calls_single_entity_with_include_manual`;
  - `test_jev_failure_502`;
  - `test_requires_admin` (no headers → redirect).
- [ ] **Step 2: Run** `uv run pytest tests/test_admin_entity_routes.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.** Plain forms with a redirect. HTMX is optional and must degrade to the redirect.
- [ ] **Step 4: Run** `uv run pytest tests/ -v`. Expected: PASS.
- [ ] **Step 5: Commit**: `#208 feat: admin entity classification routes and review queue page`

### Task 3.4: Admin panel on the entity page and the dashboard panel

**Files:**
- Create: `templates/partials/entity_admin_panel.html`
- Modify:
  - `src/wslcb_licensing_tracker/app.py` `entity_detail` (~L465): when `await get_current_user(request)`, add `classification` (`classification_detail`) and `jev_enabled` to ctx.
  - `templates/entity.html`: include the panel under `{% if current_user and classification %}`.
  - `src/wslcb_licensing_tracker/admin_routes.py` `admin_dashboard`: add `classification_health` and `review_counts` to ctx.
  - `templates/admin/dashboard.html`: a section "Entity classification".
- Test: `tests/test_routes.py`, `tests/test_admin_routes.py`

**Interfaces:**
- Consumes: 3.2, 3.3 routes.
- The panel shows each source side by side: Source | Type | Subtype | Confidence | Model | When | By/Note. It also shows the open flags and the forms: Set classification, Flag for review, Re-classify with Jev (only if `jev_enabled`), Clear manual override (only if a manual row is current).
- The dashboard section shows:
  - Enabled/Disabled;
  - last successful call;
  - awaiting;
  - failures (7 d);
  - tokens and spend (month / all-time);
  - review queue count linking to `/admin/entities/review`;
  - "Subtype unknown" count linking to `?tab=unknown`;
  - `problems` in red.

- [ ] **Step 1: Write the failing tests**:
  - `test_entity_page_admin_panel_for_admin_only` (admin sees "Set classification" and the model id; anonymous does not);
  - `test_dashboard_shows_classification_health` ("Disabled"; "missing_api_key" problem rendered as "API key missing"; the queue count).
- [ ] **Step 2: Run** `uv run pytest tests/test_routes.py tests/test_admin_routes.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement**, then `scripts/build-css.sh`.
- [ ] **Step 4: Run** `uv run pytest tests/ -v`; then check visually on port 8001 (`--log-config src/wslcb_licensing_tracker/log_config.json`). Expected: PASS; the panel renders.
- [ ] **Step 5: Commit**: `#208 feat: entity-page admin classification panel and dashboard health panel`

---

## Phase 4 — Public UI + API fields (depends on Phase 1 only)

### Task 4.1: Query layer

**Files:**
- Modify: `src/wslcb_licensing_tracker/queries_entity.py` `get_entities`; `src/wslcb_licensing_tracker/entities.py` `get_entity_by_id`, `get_record_entities`
- Test: `tests/test_queries.py`, `tests/test_entities.py`

**Interfaces:**
- Produces:
  - `get_entities(conn, *, q=None, entity_type=None, entity_subtype: str | None = None, sort="count", page=1, per_page=50)`. A subtype not in `ENTITY_SUBTYPES` is ignored. Rows gain `entity_subtype`, `classification_source`, `jev_confidence` (a correlated subquery on the latest non-ignored jev row).
  - `get_entity_by_id` gains the same three keys.
  - `get_record_entities` entity dicts gain `entity_subtype` and `classification_source`. These dicts are the data layer behind every page and any API that exposes `entity_type`; today no `/api/v1` response does (see the owner questions).

- [ ] **Step 1: Write the failing tests**:
  - `test_get_entities_subtype_filter`;
  - `test_get_entities_bogus_subtype_ignored` (same total as no filter);
  - `test_person_with_business_subtype_is_empty`;
  - `test_rows_carry_source_and_jev_confidence`;
  - `test_get_entity_by_id_fields`;
  - `test_get_record_entities_fields`.
- [ ] **Step 2: Run** `uv run pytest tests/test_queries.py tests/test_entities.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same tests. Expected: PASS.
- [ ] **Step 5: Commit**: `#209 feat: entity queries expose subtype, source, confidence; subtype filter`

### Task 4.2: Badge, subtype filter and pages

**Files:**
- Create: `templates/partials/classification_badge.html` (macro `badge(entity)`)
- Modify:
  - `src/wslcb_licensing_tracker/display.py`;
  - `src/wslcb_licensing_tracker/app.py`: filters `classification_badge` and `subtype_label`; `entities_list` gets `subtype: str = ""` → `entity_subtype`, and `subtype` in ctx;
  - `templates/entities.html`: a subtype filter next to the type buttons, with a hidden `subtype` input; every `build_qs` includes `subtype`;
  - `templates/partials/entities_results.html`: subtype label + badge; `base_qs` includes `subtype`;
  - `templates/entity.html`: subtype + badge.
- Test: `tests/test_display.py`, `tests/test_routes.py`

**Interfaces:**
- Produces:
  - `display.SUBTYPE_LABELS = {"business": "Business", "nonprofit": "Nonprofit", "trust": "Trust", "estate": "Estate", "tribe": "Tribe", "government": "Government", "unknown": "Unspecified"}`.
  - `display.classification_badge(source: str | None, confidence: float | None) -> dict` with keys `label`, `title`, `css_bg`, `css_text`, `css_border`. Styles come from `_SOURCE_TYPE_DISPLAY`'s palette (the #201 chips).
  - Labels: `regex` → `Rules`; `jev` → `AI · {round(confidence*100)}%` (`AI` when the confidence is None); `manual` → `Reviewed`.
  - Titles: "Classified by name-pattern rules" / "Classified automatically (NN% confidence)" / "Reviewed by an operator".

- [ ] **Step 1: Write the failing tests**:
  - `test_badge_labels` (`("jev", .92)` → `"AI · 92%"`; `("jev", 1.0)` → `"AI · 100%"`; `("regex", None)` → `"Rules"`; `("manual", None)` → `"Reviewed"`);
  - `test_entities_route_passes_subtype` (`?subtype=trust` → `entity_subtype="trust"`; `?subtype=bogus` → 200);
  - `test_entity_page_shows_subtype_and_badge` ("Trust", "Reviewed");
  - `test_public_pages_are_vendor_neutral` (`"Jev"` and `"TypeSafe"` absent from `/entities` and `/entity/{id}` HTML for an anonymous user).
- [ ] **Step 2: Run** `uv run pytest tests/test_display.py tests/test_routes.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement**, then `scripts/build-css.sh`.
- [ ] **Step 4: Run** `uv run pytest tests/ -v`; check `/entities?type=organization&subtype=tribe` on port 8001. Expected: PASS.
- [ ] **Step 5: Commit**: `#209 feat: public subtype filter and provenance badges`

---

## Phase 5 — Validation + rollout (5.1 needs 2.1; 5.3–5.4 need Phases 1–4 deployed)

### Task 5.1: Gold-set subtype labels and a production-path evaluation (live API: needs the operator's OK, ≈ $0.05)

**Files:**
- Modify: `docs/research/person-vs-org/gold.csv` (new column `subtype`, filled for the ~289 `O` rows with business/nonprofit/tribe/government, labeled by the `ORG_CRITERIA` definitions; blank otherwise)
- Create: `docs/research/person-vs-org/subtype_eval.py`, with the research-script header like `evaluate.py`
- Modify: `docs/research/person-vs-org/predictions.csv` (columns `prod_kind`, `prod_kind_conf`, `prod_org`, `prod_org_conf`); `docs/research/person-vs-org-classification.md` (new §10 "Org subtype validation")

**Interfaces:**
- Consumes: `jev_client.build_request`, `classify_batch`, `BATCH_SIZE` (2.1); `jev_classifier.map_answer` (2.2).
- Subcommands:
  - `run`: live. 834 non-ambiguous names, 25 per request, through `classify_batch`. The key comes from `CO_WSLCB_LICENSING_TRACKER_TYPESAFE_API_KEY`, else `evaluate._key()`. It prints tokens/name.
  - `score`: offline. Prints the A-stratum persons wrongly flagged, 5-way accuracy, and per org subtype: `n_answered≥0.7`, `correct`, `precision`, `passes (≥0.90)`.
  - `supplement --n 60`: live. It samples regex-`tribe` and regex-`government` entities from production by `md5(name)` order and runs them through Jev. It writes `subtype_supplement.csv` for hand labels. Use it for any subtype with < 20 above-threshold gold answers.

- [ ] **Step 1: Hand-label** the `subtype` column and commit the labels alone: `#210 docs: gold-set org subtype labels`.
- [ ] **Step 2: Run** `uv run python docs/research/person-vs-org/subtype_eval.py run && … score`. Expected: A-stratum 0/300 persons flagged and 5-way accuracy within drift of 813/834. If not, stop and report.
- [ ] **Step 3: Run** `supplement` for each thin subtype; label it; re-run `score`.
- [ ] **Step 4: Write §10**: the per-subtype table, the pass/fail decisions, and tokens/name with both questions.
- [ ] **Step 5: Commit**: `#210 docs: validate Jev org-subtype question on the gold set`

### Task 5.2: Apply the go-live result

**Files:**
- Modify: `src/wslcb_licensing_tracker/entity_classification.py` `JEV_TRUSTED_ORG_SUBTYPES`; `src/wslcb_licensing_tracker/jev_classifier.py` `EST_INPUT_TOKENS_PER_NAME`
- Test: `tests/test_entity_classification.py`, `tests/test_jev_classifier.py`

- [ ] **Step 1: Write failing tests that pin §10's decision**: `assert JEV_TRUSTED_ORG_SUBTYPES == frozenset({<passing subtypes>})` (⊆ `JEV_ORG_SUBTYPES`) and `assert EST_INPUT_TOKENS_PER_NAME == <measured>`.
- [ ] **Step 2: Run** them. Expected: FAIL.
- [ ] **Step 3: Set** the two constants. Cite §10 in a comment.
- [ ] **Step 4: Run** `uv run pytest tests/ -v`. Expected: PASS.
- [ ] **Step 5: Commit**: `#210 feat: trust Jev org subtypes that passed the 90% bar`

### Task 5.3: ⚠ OPERATOR-GATED: enable Jev and run the backfill (rollout step 4)

- [ ] **Step 1: Check preconditions.** Phases 1–4 and 5.2 are deployed (`git log` on the host = main; `sudo systemctl restart wslcb-web.service`). The dashboard shows "Disabled".
- [ ] **Step 2: Back up.** `sudo systemctl start wslcb-backup.service` must succeed. Save the type × subtype × source counts to `data/remediation-backups/<date>-210-jev/counts_before.txt`.
- [ ] **Step 3: Configure.** Append `CO_WSLCB_LICENSING_TRACKER_ENABLE_JEV_CLASSIFICATION=1` and `CO_WSLCB_LICENSING_TRACKER_TYPESAFE_API_KEY=…` to `/etc/wslcb-licensing-tracker/.env`. Type the key at a prompt and never echo it. Restart web. The dashboard shows "Enabled" with no problems.
- [ ] **Step 4: Dry-run.** `sudo systemd-run --uid=exedev --working-directory=/home/exedev/wslcb-licensing-tracker -p EnvironmentFile=/etc/wslcb-licensing-tracker/.env --wait --pipe -- .venv/bin/python -m wslcb_licensing_tracker.cli db classify-entities --dry-run --limit 100000 | tee …/dry-run.txt`. Expected: ≈ 67K names, ≈ 2.7K requests, est. ≤ $2. Stop if it is far off.
- [ ] **Step 5: Backfill.** The same command without `--dry-run`, teed to `apply.txt`. Expected: exit 0, 0 failed batches, ~1–3 min. Re-run to drain if needed; a second run sends 0.
- [ ] **Step 6: Verify.**
  - `counts_after.txt`: type × subtype × source.
  - The queue size (`review_counts`; expect ≈ 470) and the unknown-tab count.
  - Tokens and spend from `classification_runs`.
  - Spot-check 30 entities whose resolved type changed; save to `spot-check.tsv`.
  - `uv run wslcb db check` is unchanged.
- [ ] **Step 7: Report** the before/after counts, queue size, unknown count, and tokens/spend on the epic.
  - Rollback, if needed: `wslcb db ignore-jev` and unset the flag. Both are reversible.

### Task 5.4: ⚠ OPERATOR-GATED: weekly timer and co-status monitor (rollout step 5)

- [ ] **Step 1: Request the monitor.** Ask for `co-wslcb-classify` on tenant `co-wslcb` (a CannObserv/status issue) with the MONITORING.md fields.
- [ ] **Step 2: Install, key file first.** `sudo install -m 0400 -o root -g root /dev/null /etc/wslcb-licensing-tracker/classify-checkin.key`, then `sudo cp -r infra/wslcb-task@classify-entities.service.d /etc/systemd/system/ && sudo cp infra/wslcb-entity-classification.timer /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now wslcb-entity-classification.timer`.
- [ ] **Step 3: Configure** once the id is posted. Copy the tenant key (as MONITORING.md § Scrape check-in does) and append `CO_WSLCB_LICENSING_TRACKER_CLASSIFY_CHECKIN_BASE_URL=http://status:9000` and `…_CLASSIFY_MONITOR_ID=<id>`.
- [ ] **Step 4: Verify.** `sudo systemctl start wslcb-task@classify-entities.service; journalctl -u wslcb-task@classify-entities -n 30 -o cat --no-pager`. Expected: exit 0, `202 Accepted`, and a fresh last check-in on the monitor. `systemctl list-timers wslcb-entity-classification.timer` shows the next Sunday 04:00 PT.
- [ ] **Step 5: Close out.** Report on the epic and close the Phase 5 child.
