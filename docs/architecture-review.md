# Architecture Review & Evolution Plan

**Date:** 2026-02-28  
**Status:** Proposal  
**Scope:** Full codebase review + layered architecture plan for the WSLCB Licensing Tracker

---

## Executive Summary

The WSLCB Licensing Tracker has grown organically from a simple scraper+dashboard into a data platform with 90K records, 60K entities, 280K provenance links, entity normalization, address validation, application→outcome linking, and multiple ingestion pipelines. The code works reliably, but it was designed one feature at a time. We're now at the point where the complexity exceeds what the current flat architecture can comfortably support.

This document diagnoses the structural problems, then proposes a practical refactoring plan focused on what we can do now: unifying the ingestion pipeline, separating concerns into layers, establishing data integrity guarantees, and making the codebase testable. Future capabilities like entity resolution services and network graph analysis are acknowledged as directions but don't drive the architecture — we design for them by keeping things clean and extensible, not by building frameworks for services that don't exist yet.

---

## Part 1: Current State Diagnosis

### 1.1 What's Working Well

- **Leaf modules are clean.** `entities.py`, `endorsements.py`, `link_records.py`, and `address_validator.py` have zero or minimal project imports. They're self-contained domain logic.
- **Provenance tracking exists.** The `sources` / `record_sources` tables give us a foundation for chain-of-custody. Few projects at this scale bother.
- **SQLite is the right choice.** 112 MB database, ~300 records/scrape, single-writer workload. No reason to add infrastructure complexity.
- **HTML archival is thorough.** 3.8 GB of raw snapshots means we can always rebuild from source truth.
- **The data model is sound.** Normalized locations, entities, endorsements with junction tables — the relational design is good.

### 1.2 Structural Problems

#### Problem 1: No Layer Boundaries

The codebase has a flat structure where any module can reach into any other:

```
app.py ───→ queries.py ───→ endorsements.py
    │          │───→ entities.py
    │          │───→ link_records.py
    │          │───→ database.py
    │───→ entities.py
    │───→ endorsements.py
    │───→ link_records.py
    │───→ database.py
    │───→ log_config.py

scraper.py ───→ queries.py
    │───→ database.py
    │───→ endorsements.py
    │───→ address_validator.py
    │───→ link_records.py
    │───→ backfill_snapshots.py

backfill_snapshots.py ───→ scraper.py (parse_records_from_table)
backfill_diffs.py ───→ scraper.py (parse_records_from_table)
backfill_provenance.py ───→ scraper.py, backfill_snapshots.py, backfill_diffs.py
```

`queries.py` is a hub that imports from 4 other modules and uses 3 deferred imports to avoid circular dependencies — the classic symptom of missing layer boundaries. `app.py` imports 25+ symbols from 6 modules.

#### Problem 2: Triplicated Ingestion Pipeline

Three scripts independently implement the same "insert record → process endorsements → tag provenance → validate address → link entities → link outcomes" pipeline:

| Step | `scraper.py` | `backfill_snapshots.py` | `backfill_diffs.py` |
|------|:---:|:---:|:---:|
| Insert record | ✓ | ✓ | ✓ |
| Process endorsements | ✓ | ✓ | ✓ |
| Link provenance | ✓ | ✓ | ✓ |
| Validate address | ✓ | ✗ | ✗ |
| Link entities | ✓ | partial | ✗ |
| Link outcomes | ✓ | ✗ | ✗ |

Each copy has slightly different post-insert steps. Adding a new enrichment step requires editing 3 files and remembering which steps each one skips.

#### Problem 3: Presentation Logic in Data Layer

`get_outcome_status()` in `link_records.py` returns CSS class names (`bg-green-50 border-green-200`), emoji (`✅`, `🚫`), and display strings. This is presentation logic embedded in a data module.

Similarly, `detail.html` contains an 8-variable provenance-aggregation loop that is really controller/service logic — untestable in Jinja2.

#### Problem 4: Schema Management is Ad-Hoc

`init_db()` is 220 lines that mixes table creation, data seeding, index creation, and inline migrations (`try: ALTER TABLE ... except: pass`). Migrations in `migrate_locations.py` duplicate the `CREATE TABLE` DDL. There's no migration framework, no version tracking, no way to know which migrations have run.

#### Problem 5: No Separation of Raw vs. Enriched Data

Raw source data and derived/enriched data live in the same columns with no distinction. `business_name`, `previous_business_name`, `applicants`, and `previous_applicants` are cleaned in-place at ingest time (uppercased, punctuation stripped). If we fix a bug in the cleaning logic, we can't re-derive without re-scraping — or more precisely, without replaying from archived snapshots, which is possible but requires running the full backfill pipeline.

The archived HTML snapshots are the true source of truth. But the path from snapshot → current DB state runs through ingestion code that has changed over time. There's no way to verify that the current DB state matches what a clean replay would produce.

#### Problem 6: No Test Infrastructure

Zero tests. The module-level globals (`DATA_DIR`, `DB_PATH`), inline SQL, and interleaved side effects make testing difficult. There's no way to run the ingestion pipeline against a test database without environment manipulation.

#### Problem 7: God Functions

| Function | Lines | Responsibilities |
|----------|------:|------------------|
| `init_db()` | 220 | Schema + migrations + seeding + indexes + FTS |
| `scrape()` | 150 | HTTP + archive + parse + insert + enrich + log |
| `backfill_diffs()` | 175 | Extract + insert + CSV export |
| `backfill_provenance()` | 190 | 3 distinct phases in one function |
| `_repair_change_of_location()` | 118 | 3 code paths at 4+ nesting levels |
| `detail.html` | 311 | 5+ UI sections + controller logic |

---

## Part 2: Target Architecture

### 2.1 Design Principles

1. **Source truth is immutable.** Raw HTML snapshots are preserved. Parsed-but-untransformed field values are recoverable.
2. **Transformations are replayable.** Every enrichment (address standardization, entity normalization, endorsement resolution, outcome linking) is a discrete step that can be re-run.
3. **External services are behind clean interfaces.** `address_validator.py` already does this well. Future services should follow the same pattern: cacheable results, graceful degradation, no DB coupling in the adapter itself.
4. **Layers have direction.** Data flows down (source → raw → enriched → presentation). Dependencies point inward. Presentation never writes to the DB.
5. **Tests are possible.** Every module can be tested with an in-memory SQLite database and no network calls.

### 2.2 Three-Layer Architecture

The current codebase has two implicit layers ("stuff that writes" and "stuff that reads/displays") with no boundary between them. We introduce three explicit layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                      PRESENTATION LAYER                        │
│   app.py (routes)  │  templates/  │  display.py (formatting)   │
│   Reads enriched data. No writes. No business logic.           │
│   Owns: CSS mappings, badge rendering, provenance summaries.   │
└──────────────────────────────┬──────────────────────────────────┘
                               │ reads
┌──────────────────────────────▼──────────────────────────────────┐
│                    DOMAIN LAYER                                 │
│   queries.py (search/filter/stats)                              │
│   endorsements.py │ entities.py │ link_records.py               │
│   pipeline.py (unified ingest orchestration)                    │
│   address_validator.py (external service client)                │
│   Owns: all writes, all business logic, all enrichment.         │
│   Returns semantic data (status strings, not CSS classes).      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ reads/writes
┌──────────────────────────────▼──────────────────────────────────┐
│                      STORAGE LAYER                              │
│   db.py (connections, WAL, pragmas)                             │
│   schema.py (DDL, migrations, version tracking)                 │
│   parser.py (HTML → dicts; pure, no DB)                         │
│   SQLite: raw tables, enriched junction tables, FTS, indexes.   │
└─────────────────────────────────────────────────────────────────┘
```

This is deliberately simple. Three layers, not five. No `adapters/` package with a base class hierarchy for one concrete implementation. No `analysis/` package for code that doesn't exist yet. The package structure should reflect what the code actually does today, with room to split further when there's a real reason.

### 2.3 Concrete Changes

#### A. Extract a Parser Module

**Problem:** `parse_records_from_table()` lives in `scraper.py` but is imported by 3 other modules. `backfill_provenance.py` imports underscore-prefixed "private" functions from `backfill_snapshots.py` and `backfill_diffs.py`.

**Solution:** Move all HTML/diff parsing into `parser.py`. Pure functions: HTML in, dicts out. No DB, no side effects.

```python
# parser.py — Pure parsing, no side effects
def parse_section_table(table_element, section_type: str) -> list[dict]:
    """Parse a single WSLCB section <table> into record dicts."""

def parse_snapshot(html: str) -> dict[str, list[dict]]:
    """Parse a full WSLCB page into {section_type: [records]}."""

def parse_diff_records(diff_text: str, section_type: str) -> list[dict]:
    """Extract records from a unified diff of a WSLCB section."""

def discover_snapshots(data_dir: Path) -> list[SnapshotInfo]:
    """Find all archived HTML snapshots, sorted by date."""

def discover_diff_files(data_dir: Path) -> list[DiffFileInfo]:
    """Find all archived diff files with parsed metadata."""
```

This eliminates the circular `backfill_* → scraper → backfill_*` dependency and promotes private functions to a proper public API.

**Effort:** Small. Mechanical extraction and rename.

#### B. Unified Ingestion Pipeline

**Problem:** Three scripts independently implement insert→enrich→link with different subsets of steps.

**Solution:** A single `ingest_record()` function that all ingestion paths call:

```python
# pipeline.py — The one true ingestion path

@dataclass
class IngestOptions:
    validate_addresses: bool = True
    link_entities: bool = True
    link_outcomes: bool = True
    source_id: int | None = None
    source_role: str = 'first_seen'

def ingest_record(db, record: dict, options: IngestOptions) -> IngestResult:
    """Insert a raw record and run all enrichment steps.
    
    Steps (each idempotent, each optional):
    1. Insert raw record (dedup, create locations)
    2. Process endorsements
    3. Link provenance (if source_id provided)
    4. Validate addresses (if enabled and API available)
    5. Link entities (if enabled)
    6. Link outcomes (if enabled)
    
    Failures in steps 3-6 are logged but don't abort.
    Returns the record ID and which steps succeeded.
    """

def ingest_batch(db, records: list[dict], options: IngestOptions) -> BatchResult:
    """Ingest multiple records with progress logging and batch commits."""
```

After this, each ingestion script becomes a thin shell:
- **`scraper.py`:** Fetch HTML → archive → parse → `ingest_batch(options=IngestOptions())`
- **`backfill_snapshots.py`:** Discover files → parse → `ingest_batch(options=IngestOptions(validate_addresses=False))`  
- **`backfill_diffs.py`:** Parse diffs → `ingest_batch(options=IngestOptions(validate_addresses=False, link_entities=False))`

The repair logic in `backfill_snapshots.py` (fixing broken ASSUMPTION and CHANGE OF LOCATION records) stays in that module — it's genuinely source-specific, not part of the general pipeline.

**Effort:** Medium. The individual steps already exist as callable functions. The work is wiring them into a single orchestrator and updating callers.

#### C. Presentation Separation

**Problem:** `get_outcome_status()` returns CSS classes and emoji. `detail.html` aggregates provenance data in Jinja2.

**Solution:** Two concrete changes:

1. **`get_outcome_status()` returns semantic data:**

```python
# link_records.py — returns data, not CSS
def get_outcome_status(db, record_id, section_type, ...) -> dict:
    return {
        'status': 'approved',        # semantic enum
        'detail': 'Approved on 2025-06-15 (12 days)',
        'link_id': 42,
        'confidence': 'high',
    }

# display.py — maps semantic status → presentation
OUTCOME_STYLES = {
    'approved':     {'icon': '✅', 'bg': 'bg-green-50',  'border': 'border-green-200',  'text': 'text-green-800'},
    'discontinued': {'icon': '🚫', 'bg': 'bg-red-50',    'border': 'border-red-200',    'text': 'text-red-800'},
    'pending':      {'icon': '⏳', 'bg': 'bg-yellow-50', 'border': 'border-yellow-200', 'text': 'text-yellow-800'},
    'unknown':      {'icon': '❓', 'bg': 'bg-gray-50',   'border': 'border-gray-200',   'text': 'text-gray-600'},
    'data_gap':     {'icon': '⚠️',  'bg': 'bg-amber-50',  'border': 'border-amber-200',  'text': 'text-amber-700'},
}

def format_outcome(outcome: dict) -> dict:
    """Add display properties to a semantic outcome dict."""
    style = OUTCOME_STYLES[outcome['status']]
    return {**outcome, **style}
```

2. **Provenance aggregation moves to a Python function:**

```python
# In queries.py or a helper
def summarize_provenance(sources: list[dict]) -> list[dict]:
    """Group sources by type, compute counts and date ranges."""
```

The template receives pre-computed data and just renders it.

**Effort:** Small. The logic already exists; it just needs to move.

#### D. Schema Migration Framework

**Problem:** Migrations are inline `try/except ALTER TABLE` blocks in `init_db()`. No version tracking.

**Solution:** Use SQLite's built-in `PRAGMA user_version`:

```python
# schema.py
MIGRATIONS = [
    (1, "initial_schema",       _create_initial_tables),
    (2, "add_locations",         _migrate_to_locations),
    (3, "add_provenance",        _add_provenance_tables),
    (4, "add_record_links",      _add_record_links),
    (5, "add_enrichment_tracking", _add_enrichment_tracking),
]

def migrate(db):
    """Run all pending migrations."""
    current = db.execute("PRAGMA user_version").fetchone()[0]
    for version, name, fn in MIGRATIONS:
        if version > current:
            logger.info("Running migration %d: %s", version, name)
            fn(db)
            db.execute(f"PRAGMA user_version = {version}")
            db.commit()
```

For the existing database, we set `user_version` to the current state (say, 4) and only new migrations run going forward. The existing inline `try/except` blocks become unnecessary.

**Effort:** Medium. Retrofitting means declaring "version 4 = current state" and adding future migrations as functions. The existing code in `init_db()` can be gradually absorbed.

#### E. Split `database.py`

**Problem:** `database.py` (525 lines) mixes connection management, all DDL, FTS setup, data seeding, helper functions (`get_or_create_location`, `get_or_create_source`, `link_record_source`), and imports a migration module.

**Solution:** Split into two files:
- **`db.py`** — Connection management, `DATA_DIR`/`DB_PATH`, WAL mode, pragmas. Small.
- **`schema.py`** — All DDL, migrations, FTS setup, index creation, seeding. Where `init_db()` lives (cleaned up).

The helper functions (`get_or_create_location`, `get_or_create_source`, `link_record_source`) move to whichever domain module uses them — `get_or_create_location` is called by `insert_record`, so it belongs near the pipeline. `get_or_create_source` and `link_record_source` are provenance operations.

**Effort:** Small-medium. Mostly file reorganization.

#### F. Extract CLI from `scraper.py`

**Problem:** `scraper.py`'s `__main__` block dispatches to `--refresh-addresses`, `--backfill-addresses`, `--backfill-from-snapshots`, `--rebuild-links` — none of which are scraping. It's a de-facto CLI entry point for the whole project.

**Solution:** Either:
- A `cli.py` with `argparse` subcommands (`scrape`, `backfill-snapshots`, `backfill-diffs`, `refresh-addresses`, `rebuild-links`), or
- Keep each script as its own entry point but remove the dispatch from `scraper.py`

The first option is cleaner. The systemd service files would change from `python scraper.py` to `python cli.py scrape`.

**Effort:** Small.

#### G. Test Infrastructure

**Problem:** Zero tests. Untestable architecture (globals, side effects, no DI).

**Solution:** Start with the highest-value, lowest-effort tests:

```
tests/
├── conftest.py           # In-memory DB fixture, sample record dicts
├── test_parser.py        # Pure functions, no DB needed
├── test_pipeline.py      # Ingest into test DB, verify enrichments
├── test_endorsements.py  # Seed + resolve codes
├── test_entities.py      # Parse, clean, classify
└── test_links.py         # Link matching algorithm
```

The key enabler is making `get_db()` / `init_db()` accept a path parameter (or use a factory) so tests can use `:memory:` or a temp file. This is a small change with outsized impact.

**Effort:** Medium for initial setup + first batch of tests. Ongoing.

---

## Part 3: Implementation Roadmap

Three phases. Each delivers standalone value. No phase depends on hypothetical future services.

### Phase 1: Untangle (foundation)
**Goal:** Break the circular dependencies, enable testing, establish conventions.

| # | Task | Effort | Why |
|---|------|--------|-----|
| 1.1 | Add `requirements.txt` | S | Reproducible installs |
| 1.2 | Extract `parser.py` from `scraper.py` | S | Breaks circular deps, enables pure-function tests |
| 1.3 | Extract CLI dispatch from `scraper.py` into `cli.py` | S | `scraper.py` becomes single-purpose |
| 1.4 | Make `get_db()`/`init_db()` accept a path parameter | S | Enables test fixtures |
| 1.5 | Add `pytest` + `conftest.py` with in-memory DB fixture | S | First tests possible |
| 1.6 | Write tests for parser (pure functions, no DB) | S | Highest-value first tests |

**Outcome:** No behavioral changes. Cleaner imports. Tests exist.

### Phase 2: Unify (pipeline + layers)
**Goal:** One ingestion path. Clear layer boundaries. Data integrity tracking.

| # | Task | Effort | Why |
|---|------|--------|-----|
| 2.1 | Create `pipeline.py` with `ingest_record()` / `ingest_batch()` | M | Eliminates triplicated insert→enrich→link |
| 2.2 | Refactor `scraper.py` to use pipeline | M | First consumer |
| 2.3 | Refactor `backfill_snapshots.py` to use pipeline | M | Second consumer |
| 2.4 | Refactor `backfill_diffs.py` to use pipeline | M | Third consumer |
| 2.5 | Move presentation out of `get_outcome_status()` into `display.py` | S | Data layer returns data, not CSS |
| 2.6 | Move provenance aggregation out of `detail.html` | S | Controller logic in Python, not Jinja2 |
| 2.7 | Split `database.py` into `db.py` + `schema.py` | M | Separate concerns |
| 2.8 | Implement migration framework (`PRAGMA user_version`) | M | Tracked, versioned migrations |
| 2.9 | Write tests for pipeline and enrichment modules | M | Confidence in the unified path |

**Outcome:** Adding a new enrichment step means editing one function in `pipeline.py`. Schema changes are versioned. Templates are dumb renderers.

### Phase 3: Harden (data integrity + replay)
**Goal:** Confidence that DB state is correct and reproducible.

| # | Task | Effort | Why |
|---|------|--------|-----|
| 3.1 | Add `record_enrichments` tracking table | S | Know which enrichments ran on which records |
| 3.2 | Preserve raw field values (before cleaning) | M | Distinguish source truth from derived data |
| 3.3 | Add `rebuild` CLI command (replay all archived HTML) | M | Prove we can reproduce from source truth |
| 3.4 | Data integrity checks (orphan detection, FK validation) | M | Ongoing health monitoring |
| 3.5 | Deduplicate `_link_approvals` / `_link_discontinuances` in `link_records.py` | S | 145 lines of structural duplication |
| 3.6 | Clean up `endorsements.py` find-migrate-delete duplication | S | 4 near-identical patterns |

**Outcome:** We can verify and rebuild the database from archived sources. Data quality is monitored.

---

## Part 4: Target File Structure

This is what the codebase looks like after Phase 2. No speculative packages.

```
wslcb-licensing-tracker/
├── app.py                      # FastAPI routes (thin, reads only)
├── display.py                  # Presentation formatting (badges, CSS, icons)
├── cli.py                      # CLI entry point (subcommands)
│
├── db.py                       # Connection management, DATA_DIR, pragmas
├── schema.py                   # DDL, migrations, FTS, seeding
│
├── parser.py                   # HTML/diff → dicts (pure, no DB)
├── pipeline.py                 # Unified ingest_record / ingest_batch
├── scraper.py                  # HTTP fetch + archive → pipeline
├── backfill_snapshots.py       # Replay archived HTML → pipeline (+ repairs)
├── backfill_diffs.py           # Replay diff archives → pipeline
├── backfill_provenance.py      # One-time provenance linking
│
├── endorsements.py             # License type normalization
├── entities.py                 # Applicant extraction & normalization
├── link_records.py             # Application → outcome linking
├── address_validator.py        # External address standardization client
│
├── queries.py                  # Search, filter, stats, record hydration
├── log_config.py               # Logging setup
│
├── templates/                  # Jinja2 (dumb renderers, no business logic)
├── static/
├── tests/
│   ├── conftest.py             # In-memory DB fixture, sample records
│   ├── test_parser.py
│   ├── test_pipeline.py
│   ├── test_endorsements.py
│   ├── test_entities.py
│   └── test_links.py
│
├── data/                       # Persistent data (gitignored)
├── docs/
├── requirements.txt
└── pyproject.toml
```

Notice: still flat. No `core/`, `ingest/`, `enrich/`, `adapters/` packages. With ~15 Python files, a flat layout with clear naming is simpler than a package hierarchy. If the project grows to 25+ modules, we can introduce packages then — and the clean layer boundaries will make that reorganization straightforward.

---

## Part 5: What NOT to Change

1. **SQLite as the sole datastore.** 112 MB, single-writer. Perfect fit.
2. **Server-rendered HTML + HTMX.** Right for this project's audience and complexity.
3. **Twice-daily scraping via systemd timer.** Simple, reliable, observable.
4. **The relational data model.** Normalized locations, entities, endorsements, junctions — sound.
5. **HTML snapshot archival.** 3.8 GB of source truth. Keep archiving.
6. **Provenance tracking.** The `sources` / `record_sources` system is a differentiator.
7. **`address_validator.py`'s isolation pattern.** It's the best-structured module. Future external service clients should follow its example: stdlib + HTTP client, no project imports, cacheable results, graceful degradation.

---

## Part 6: Future Directions (Not Driving Current Work)

These are capabilities we expect to build eventually. The architecture above doesn't block any of them, but we're not pre-building abstractions for them either.

- **Entity resolution service.** When an external service exists for matching entity names to canonical identities, it'll plug in the same way `address_validator.py` does: a standalone client module, results cached in the DB, called from the pipeline. The `entities` table and `record_entities` junction are the right foundation.

- **Entity network analysis.** Co-occurrence analysis (which entities appear on the same records), ownership graphs, license transfer chains. This needs the unified pipeline (so entity links are reliably populated) and clean query interfaces. It doesn't need new architecture — it needs SQL queries and maybe a `license_profiles` materialized view.

- **Graph visualization.** D3.js, Cytoscape, etc. This is a frontend concern that will be informed by the analysis queries we build. Premature to choose a library.

- **API endpoints for data consumers.** The current HTMX approach serves HTML. A JSON API layer would be additive, not a replacement. FastAPI makes this trivial to add alongside existing routes.

- **Package reorganization.** If the module count doubles, introduce `core/`, `ingest/`, `enrich/` packages. The layer boundaries from Phase 2 make this a mechanical rename.

The right time to design for these is when we're building them. The refactoring plan above ensures we won't have to undo anything when that time comes.

---

## Part 7: Decision Points (Resolve During Implementation)

1. **Raw value preservation strategy (Phase 3.2):** Add `raw_*` shadow columns to `license_records` (simpler, self-contained) or add a `record_raw_values` satellite table (cleaner separation, but JOIN cost on every query)? Recommend: shadow columns for the 4 cleaned fields (`business_name`, `previous_business_name`, `applicants`, `previous_applicants`), since these are small strings and the alternative is a separate table for 4 values.

2. **CLI tool choice (Phase 1.3):** `argparse` subcommands or `click`? `argparse` is stdlib and sufficient. Only consider `click` if the CLI grows complex.

3. **Pipeline commit strategy (Phase 2.1):** Commit per-record (safest, slowest), per-batch (balanced), or per-source (fastest, riskiest)? Recommend: per-batch with configurable batch size, same as current `_validate_batch()` pattern.

---

## Appendix: Dependency Graph (Current → Target)

### Current
```
                    ┌─────────┐
              ┌────→│ app.py  │←── presentation
              │     └────┬────┘
              │          │ imports from 6 modules
              │          ▼
              │     ┌──────────┐     ┌──────────────┐
              │     │queries.py│────→│endorsements.py│  ← leaf
              │     │  (hub)   │────→│entities.py    │  ← leaf
              │     │          │────→│link_records.py│  ← leaf
              │     │          │────→│database.py    │
              │     └──────────┘     └──────────────┘
              │
    ┌─────────┴──────────┐
    │    scraper.py       │──→ queries, database, endorsements,
    │    (CLI dispatcher) │    address_validator, link_records
    └─────────┬──────────┘
              │ imports parse functions
              ▼
    ┌──────────────────┐
    │backfill_snapshots│──→ scraper (parser), database, queries,
    │backfill_diffs    │    entities, endorsements
    │backfill_provenance│──→ scraper, backfill_snapshots, backfill_diffs
    └──────────────────┘
```

### Target (after Phase 2)
```
    ┌────────────────┐
    │ app.py         │──→ queries, display     PRESENTATION
    │ display.py     │    (no domain imports)  (reads only)
    └───────┬────────┘
            │ reads
    ┌───────▼────────────────────────────────┐
    │ queries.py                             │
    │ endorsements.py  entities.py           │  DOMAIN
    │ link_records.py  address_validator.py   │  (all writes,
    │ pipeline.py (orchestrates all ingest)  │   all logic)
    └───────┬────────────────────────────────┘
            │ uses
    ┌───────▼────────────────────────────────┐
    │ db.py    schema.py    parser.py        │  STORAGE +
    │ scraper.py  backfill_*.py (thin)       │  INGESTION
    └────────────────────────────────────────┘

    Dependencies flow strictly downward. No circular imports.
    No deferred imports. No presentation in domain layer.
```
