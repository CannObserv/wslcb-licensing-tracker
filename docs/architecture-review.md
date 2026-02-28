# Architecture Review & Evolution Plan

**Date:** 2026-02-28  
**Status:** Proposal  
**Scope:** Full codebase review + layered architecture plan for the WSLCB Licensing Tracker

---

## Executive Summary

The WSLCB Licensing Tracker has grown organically from a simple scraper+dashboard into a data platform with 90K records, 60K entities, 280K provenance links, entity normalization, address validation, application→outcome linking, and multiple ingestion pipelines. The code works reliably, but it was designed one feature at a time. We're now at the point where the complexity exceeds what the current flat architecture can comfortably support.

This document diagnoses the structural problems, then proposes a layered architecture that separates **source data preservation**, **replicable transformation**, **external service integration**, and **analysis/presentation** into distinct, independently testable concerns.

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
app.py ──→ queries.py ──→ endorsements.py
    │          │──→ entities.py
    │          │──→ link_records.py
    │          │──→ database.py
    │──→ entities.py
    │──→ endorsements.py
    │──→ link_records.py
    │──→ database.py
    │──→ log_config.py

scraper.py ──→ queries.py
    │──→ database.py
    │──→ endorsements.py
    │──→ address_validator.py
    │──→ link_records.py
    │──→ backfill_snapshots.py

backfill_snapshots.py ──→ scraper.py (parse_records_from_table)
backfill_diffs.py ──→ scraper.py (parse_records_from_table)
backfill_provenance.py ──→ backfill_snapshots.py (_parse_snapshot)
    │──→ backfill_diffs.py (_discover_diff_files)
    │──→ scraper.py (URL)
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

Each copy has slightly different post-insert steps. Adding a new step (e.g., entity resolution via external service) requires editing 3 files and remembering which steps each one skips.

#### Problem 3: Presentation Logic in Data Layer

`get_outcome_status()` in `link_records.py` returns CSS class names (`bg-green-50 border-green-200`), emoji (`✅`, `🚫`), and display strings. This is presentation logic embedded in a data module, making it impossible to reuse the outcome logic for an API or different frontend.

Similarly, `detail.html` contains an 8-variable provenance-aggregation loop that is really controller/service logic — untestable in Jinja2.

#### Problem 4: Schema Management is Ad-Hoc

`init_db()` is 220 lines that mixes table creation, data seeding, index creation, and inline migrations (`try: ALTER TABLE ... except: pass`). Migrations in `migrate_locations.py` duplicate the `CREATE TABLE` DDL. There's no migration framework, no version tracking, no way to know which migrations have run.

#### Problem 5: No Transformation Layer

Raw source data and derived/enriched data live in the same tables with no distinction. The `license_records` table contains both raw scraped fields and cleaned/normalized values. There's no way to:
- Re-derive enrichments without re-scraping
- Audit what the original source said vs. what we transformed it into
- Replay transformations after fixing a bug in the cleaning logic

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

1. **Source truth is immutable.** Raw HTML snapshots and the parsed-but-untransformed records are never modified.
2. **Transformations are replayable.** Every enrichment (address standardization, entity normalization, endorsement resolution, outcome linking) is a discrete step that can be re-run from source data.
3. **External services are adapters.** Address validation, future entity resolution, etc. are behind clean interfaces that can be swapped, mocked, or rate-limited independently.
4. **Layers enforce direction.** Data flows down (source → staging → enriched → presentation). Dependencies point inward. Presentation never writes to the DB.
5. **Tests are possible.** Every module can be tested with an in-memory SQLite database and no network calls.

### 2.2 Layered Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      PRESENTATION LAYER                        │
│   app.py (FastAPI routes)  │  templates/  │  future: REST API  │
│   Reads from enriched views. No writes. No business logic.     │
└──────────────────────────────┬──────────────────────────────────┘
                               │ reads
┌──────────────────────────────▼──────────────────────────────────┐
│                      ANALYSIS / QUERY LAYER                    │
│   queries.py    │  analysis.py (future)  │  export.py (future) │
│   Search, filter, stats, network exploration.                  │
│   Reads enriched data. Produces display-ready structures.      │
│   Owns display formatting (outcome badges, provenance summary).│
└──────────────────────────────┬──────────────────────────────────┘
                               │ reads
┌──────────────────────────────▼──────────────────────────────────┐
│                     ENRICHMENT / TRANSFORM LAYER               │
│   pipeline.py  │  endorsements.py  │  entities.py              │
│   link_records.py  │  transformations are replayable           │
│   Orchestrates: parse → insert_raw → enrich → link.            │
│   Each enrichment step is idempotent.                          │
└──────────────────────────────┬──────────────────────────────────┘
                               │ reads raw, writes enriched
┌──────────────────────────────▼──────────────────────────────────┐
│                    EXTERNAL SERVICE ADAPTERS                    │
│   address_validator.py  │  entity_resolver.py (future)         │
│   Clean interfaces. Cacheable. Mockable. Rate-limited.         │
└──────────────────────────────┬──────────────────────────────────┘
                               │ called by enrichment layer
┌──────────────────────────────▼──────────────────────────────────┐
│                      INGESTION LAYER                           │
│   parser.py (HTML→dicts)  │  ingest.py (dicts→raw DB rows)    │
│   scraper.py (fetch+archive) │  backfill_*.py (replay sources) │
│   Writes raw data + provenance. Never enriches.                │
└──────────────────────────────┬──────────────────────────────────┘
                               │ writes
┌──────────────────────────────▼──────────────────────────────────┐
│                      STORAGE LAYER                             │
│   schema.py (DDL + migrations)  │  db.py (connections)         │
│   SQLite tables, indexes, FTS, views.                          │
│   Raw tables vs. enriched tables clearly separated.            │
└────────────────────────────────────────────────────────────────┘
```

### 2.3 Key Architectural Changes

#### A. Extract a Parser Module (`parser.py`)

**Problem:** `parse_records_from_table()` lives in `scraper.py` but is imported by `backfill_snapshots.py`, `backfill_diffs.py`, and `backfill_provenance.py`.

**Solution:** Move all HTML-to-dict parsing into `parser.py`. This module takes raw HTML and returns structured Python dicts. No DB access, no side effects.

```python
# parser.py — Pure parsing, no side effects
def parse_records_from_html(html: str) -> list[dict]:
    """Parse WSLCB HTML into a list of record dicts."""
    ...

def parse_records_from_table(table_element, section_type: str) -> list[dict]:
    """Parse a single section table into record dicts."""
    ...
```

**Effort:** Small. Mechanical move. Unblocks all other refactoring.

#### B. Unified Ingestion Pipeline (`pipeline.py`)

**Problem:** Three scripts duplicate the insert→enrich→link chain with different subsets of steps.

**Solution:** A single pipeline function that all ingestion paths call:

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
    
    Steps:
    1. insert_raw_record (dedup, create locations)
    2. process_endorsements
    3. link_provenance (if source_id provided)
    4. validate_address (if enabled)
    5. link_entities (if enabled) 
    6. link_outcome (if enabled)
    
    Each step is idempotent. Failures in optional steps 
    are logged but don't abort the pipeline.
    """
    ...

def ingest_batch(db, records: list[dict], options: IngestOptions) -> BatchResult:
    """Ingest multiple records with progress logging and batch commits."""
    ...
```

Then `scraper.py`, `backfill_snapshots.py`, and `backfill_diffs.py` each become thin shells:
- **scraper.py:** Fetch HTML → archive → parse → `ingest_batch(options=IngestOptions())`
- **backfill_snapshots.py:** Discover files → parse → `ingest_batch(options=IngestOptions(validate_addresses=False))`
- **backfill_diffs.py:** Parse diffs → `ingest_batch(options=IngestOptions(validate_addresses=False, link_entities=False))`

**Effort:** Medium. Requires untangling the three scripts, but each step is already a callable function.

#### C. Separate Raw vs. Enriched Data

**Problem:** Raw source data and derived enrichments are intermingled in `license_records`. If we fix a bug in entity cleaning or endorsement resolution, we can't replay without re-scraping.

**Solution:** Introduce a conceptual (and eventually physical) separation:

**Phase 1 — Logical separation (no schema change):**
- Document which columns are "raw" (from source) vs. "enriched" (derived)
- Ensure all enrichment is done via the pipeline, never inline during parsing
- Add a `record_enrichments` metadata table tracking which enrichment steps have been applied to each record and when

**Phase 2 — Physical separation (future, optional):**
- Raw columns stay on `license_records`: `section_type`, `record_date`, `business_name` (raw), `license_type` (raw), `application_type`, `license_number`, `contact_phone`, `applicants` (raw), etc.
- Enriched data lives in junction/satellite tables (already partially true): `record_endorsements`, `record_entities`, `record_links`, `locations` (with standardized fields)
- The raw `applicants` text column and the normalized `record_entities` junction table coexist — the text is source truth, the junction is derived

This is already ~80% of the current design. The main gap is that `business_name`, `previous_business_name`, `applicants`, and `previous_applicants` are cleaned in-place at ingest time (uppercased, punctuation stripped). Instead:
- Store the raw value as-is from the source
- Apply cleaning as an enrichment step that writes to the entity junction table
- For display/search, prefer the enriched entity names; fall back to raw

**Effort:** Phase 1 is small (documentation + metadata table). Phase 2 is medium (requires careful migration).

#### D. Schema Migration Framework

**Problem:** Migrations are inline `try/except ALTER TABLE` blocks in `init_db()`. No version tracking. DDL duplicated in `migrate_locations.py`.

**Solution:** A simple, pragmatic migration system (not Alembic — overkill for SQLite):

```python
# schema.py

MIGRATIONS = [
    (1, "initial_schema", _create_initial_tables),
    (2, "add_locations", _migrate_to_locations),
    (3, "add_provenance", _add_provenance_tables),
    (4, "add_record_links", _add_record_links),
    # ...
]

def migrate(db):
    """Run all pending migrations in order."""
    current = _get_schema_version(db)
    for version, name, fn in MIGRATIONS:
        if version > current:
            fn(db)
            _set_schema_version(db, version)
            db.commit()
```

Using SQLite's built-in `PRAGMA user_version` for version tracking. Each migration is a function. They run in order, once, tracked.

**Effort:** Medium. Retrofitting requires declaring current state as version N and adding new migrations going forward. Existing inline migrations become no-ops once the current version is set.

#### E. Presentation Separation

**Problem:** `get_outcome_status()` returns CSS classes. `detail.html` aggregates provenance data. The query layer is coupled to the specific frontend.

**Solution:**

1. **`get_outcome_status()` returns semantic data, not CSS:**
```python
# Returns:
@dataclass
class OutcomeStatus:
    status: str  # 'approved', 'discontinued', 'pending', 'unknown', 'data_gap'
    detail: str  # 'Approved on 2025-06-15 (12 days)'
    link_id: int | None
    confidence: str | None

# Presentation layer maps status → CSS/emoji:
OUTCOME_DISPLAY = {
    'approved': {'icon': '✅', 'bg': 'bg-green-50', 'border': 'border-green-200', ...},
    ...
}
```

2. **Provenance aggregation moves to a query/service function:**
```python
def get_provenance_summary(sources: list[dict]) -> dict:
    """Aggregate source records into display-ready summary."""
    # Returns: {type_slug: {label, count, date_range, icon, sources: [...]}}
```

3. **Templates become dumb renderers** — they receive pre-computed display data, no business logic.

**Effort:** Small-to-medium. Most of the logic already exists, just needs to move.

#### F. External Service Adapter Pattern

**Problem:** `address_validator.py` is well-isolated, but there's no consistent pattern for adding future services (entity resolution, geocoding, etc.).

**Solution:** Establish a consistent adapter interface:

```python
# adapters/base.py
class ServiceAdapter:
    """Base for external service integrations."""
    def __init__(self, api_key: str | None = None, base_url: str | None = None):
        ...
    
    def is_available(self) -> bool:
        """Check if the service is configured and reachable."""
        ...
    
    def health_check(self) -> dict:
        """Return service status for monitoring."""
        ...

# adapters/address_validator.py  (refactored from current)
class AddressValidator(ServiceAdapter):
    def validate(self, raw_address: str) -> StandardizedAddress | None:
        ...
    
    def validate_batch(self, addresses: list[str]) -> list[StandardizedAddress | None]:
        ...

# adapters/entity_resolver.py  (future)
class EntityResolver(ServiceAdapter):
    def resolve(self, name: str, entity_type: str) -> ResolvedEntity | None:
        """Match a name to a canonical entity via external service."""
        ...
    
    def find_connections(self, entity_id: int) -> list[EntityConnection]:
        """Discover relationships between entities."""
        ...
```

Key properties:
- **Cacheable:** Results are stored in the DB (like `locations.address_validated_at`). The adapter is only called for cache misses.
- **Mockable:** Tests inject a fake adapter. No network calls in tests.
- **Graceful degradation:** If the service is down, the pipeline continues and marks records for later enrichment.
- **Rate-limited:** Adapters own their own rate limiting (sleep, backoff, concurrency).

**Effort:** Small for the pattern. Individual service implementations are separate work items.

#### G. Entity Network Analysis Foundation

This is the primary analytical capability we're building toward. The current entity system stores names and links them to records. The next level is understanding **relationships between entities** — who works with whom, which entities appear across multiple businesses, how ownership networks form.

**Data model extensions:**

```sql
-- Entity aliases / canonical resolution
CREATE TABLE entity_aliases (
    id INTEGER PRIMARY KEY,
    entity_id INTEGER NOT NULL REFERENCES entities(id),
    alias_name TEXT NOT NULL,
    source TEXT NOT NULL,  -- 'name_variant', 'external_resolution', 'manual'
    confidence REAL DEFAULT 1.0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Entity-to-entity relationships (derived)
CREATE TABLE entity_relationships (
    id INTEGER PRIMARY KEY,
    entity_a_id INTEGER NOT NULL REFERENCES entities(id),
    entity_b_id INTEGER NOT NULL REFERENCES entities(id),
    relationship_type TEXT NOT NULL,  -- 'co_applicant', 'successor', 'shared_address'
    strength REAL DEFAULT 1.0,  -- number of shared records, normalized
    first_seen_date TEXT,
    last_seen_date TEXT,
    metadata TEXT DEFAULT '{}',
    UNIQUE(entity_a_id, entity_b_id, relationship_type)
);

-- License lifecycle (one row per license_number, derived)
CREATE TABLE license_profiles (
    license_number TEXT PRIMARY KEY,
    first_seen_date TEXT,
    last_seen_date TEXT,
    current_business_name TEXT,
    current_location_id INTEGER REFERENCES locations(id),
    application_count INTEGER DEFAULT 0,
    assumption_count INTEGER DEFAULT 0,
    endorsement_ids TEXT DEFAULT '[]',  -- JSON array
    status TEXT,  -- 'active', 'discontinued', 'pending'
    updated_at TEXT
);
```

**Derived analysis queries (future `analysis.py`):**

```python
def build_entity_graph(db) -> None:
    """Derive entity_relationships from co-occurrence in records.
    
    Two entities are 'co_applicant' if they appear on the same record.
    Strength = number of shared records.
    """

def get_entity_network(db, entity_id: int, depth: int = 2) -> NetworkGraph:
    """Get the N-hop neighborhood of an entity.
    
    Returns nodes (entities) and edges (relationships) for visualization.
    """

def get_license_timeline(db, license_number: str) -> list[TimelineEvent]:
    """Complete lifecycle of a license: applications, approvals, 
    transfers, location changes, discontinuances."""

def find_entity_clusters(db) -> list[EntityCluster]:
    """Identify groups of entities that frequently co-occur.
    Connected components in the co-applicant graph."""
```

**Effort:** Medium-large. The schema changes are straightforward. The graph derivation requires careful SQL. The visualization is a separate frontend effort.

---

## Part 3: Implementation Roadmap

Phased plan. Each phase delivers standalone value and can be paused.

### Phase 0: Foundation (prep work)
**Goal:** Enable all subsequent refactoring without breaking the running system.

| Task | Description | Effort |
|------|-------------|--------|
| 0.1 | Add `requirements.txt` / `pyproject.toml` | S |
| 0.2 | Add basic test infrastructure (`pytest`, test DB fixture, `conftest.py`) | S |
| 0.3 | Extract `parser.py` from `scraper.py` | S |
| 0.4 | Extract CLI dispatch from `scraper.py` into `cli.py` or use proper arg parsing | S |

### Phase 1: Pipeline Unification
**Goal:** One ingestion path. All enrichments are discrete, replayable steps.

| Task | Description | Effort |
|------|-------------|--------|
| 1.1 | Create `pipeline.py` with `ingest_record()` / `ingest_batch()` | M |
| 1.2 | Refactor `scraper.py` to use pipeline | M |
| 1.3 | Refactor `backfill_snapshots.py` to use pipeline | M |
| 1.4 | Refactor `backfill_diffs.py` to use pipeline | M |
| 1.5 | Add `record_enrichments` tracking table | S |
| 1.6 | Write tests for the pipeline | M |

### Phase 2: Layer Separation  
**Goal:** Clean dependency graph. Presentation can't reach into storage.

| Task | Description | Effort |
|------|-------------|--------|
| 2.1 | Split `database.py` into `db.py` (connections) + `schema.py` (DDL/migrations) | M |
| 2.2 | Implement migration framework with `PRAGMA user_version` | M |
| 2.3 | Move presentation logic out of `get_outcome_status()` | S |
| 2.4 | Move provenance aggregation out of `detail.html` into a service function | S |
| 2.5 | Create `display.py` for all presentation formatting (badges, icons, CSS mappings) | S |
| 2.6 | Deduplicate link_records.py (`_link_approvals` / `_link_discontinuances`) | S |

### Phase 3: External Service Framework
**Goal:** Clean adapter pattern for current and future external services.

| Task | Description | Effort |
|------|-------------|--------|
| 3.1 | Create `adapters/` package with base class | S |
| 3.2 | Refactor `address_validator.py` into adapter pattern | M |
| 3.3 | Add adapter health checks to `/api/stats` | S |
| 3.4 | Design entity resolver adapter interface | S |
| 3.5 | Implement entity resolver (when external service is ready) | M-L |

### Phase 4: Analysis Layer
**Goal:** Entity network exploration, license lifecycle views.

| Task | Description | Effort |
|------|-------------|--------|
| 4.1 | Add `entity_relationships` and `entity_aliases` tables | S |
| 4.2 | Build `analysis.py` with entity graph derivation | M |
| 4.3 | Add `license_profiles` derived table | M |
| 4.4 | Entity network API endpoint (`/api/entity/{id}/network`) | M |
| 4.5 | Entity network visualization (D3.js or similar) | M-L |
| 4.6 | License timeline view | M |
| 4.7 | Entity cluster detection and reporting | M |

### Phase 5: Data Integrity Hardening
**Goal:** Rebuild-from-scratch capability, audit trail.

| Task | Description | Effort |
|------|-------------|--------|
| 5.1 | Preserve raw field values separately from cleaned values | M |
| 5.2 | Add `rebuild_from_sources` command that replays all archived HTML | M |
| 5.3 | Checksums for source artifacts (some already in `sources.metadata`) | S |
| 5.4 | Data integrity checks (orphan detection, FK validation, enrichment gaps) | M |
| 5.5 | Scheduled integrity reports | S |

---

## Part 4: Target File Structure

```
wslcb-licensing-tracker/
├── app.py                      # FastAPI routes only (thin)
├── display.py                  # Presentation formatting (badges, CSS, icons)
├── cli.py                      # CLI entry point (replaces scraper.py __main__)
│
├── core/                       # Storage layer
│   ├── db.py                   # Connection management, WAL, pragmas
│   ├── schema.py               # Migrations, DDL, version tracking
│   └── models.py               # Dataclasses/TypedDicts for record shapes
│
├── ingest/                     # Ingestion layer
│   ├── parser.py               # HTML → dicts (pure, no DB)
│   ├── pipeline.py             # Unified ingest_record / ingest_batch
│   ├── scraper.py              # HTTP fetch + archive (thin)
│   ├── backfill_snapshots.py   # Replay archived HTML (thin)
│   └── backfill_diffs.py       # Replay diff archives (thin)
│
├── enrich/                     # Enrichment / transformation layer
│   ├── endorsements.py         # License type normalization
│   ├── entities.py             # Applicant extraction & normalization
│   ├── links.py                # Application → outcome linking
│   └── provenance.py           # Source tracking
│
├── adapters/                   # External service adapters
│   ├── base.py                 # ServiceAdapter base class
│   ├── address_validator.py    # USPS address standardization
│   └── entity_resolver.py      # Future: entity resolution service
│
├── analysis/                   # Analysis layer (future)
│   ├── queries.py              # Search, filter, stats
│   ├── entity_network.py       # Graph derivation, cluster detection
│   ├── license_lifecycle.py    # License timeline construction
│   └── export.py               # CSV/data export
│
├── templates/                  # Jinja2 templates (dumb renderers)
├── static/
├── tests/
│   ├── conftest.py             # Shared fixtures (in-memory DB, sample records)
│   ├── test_parser.py
│   ├── test_pipeline.py
│   ├── test_endorsements.py
│   ├── test_entities.py
│   ├── test_links.py
│   └── test_queries.py
│
├── data/                       # Persistent data (gitignored)
├── docs/                       # Architecture docs
├── requirements.txt
└── pyproject.toml
```

---

## Part 5: What NOT to Change

Some things are working well and should be preserved:

1. **SQLite as the sole datastore.** The dataset is small. No need for Postgres, Redis, or any other infrastructure.
2. **Server-rendered HTML + HTMX.** The frontend approach is right for this project. No SPA framework needed.
3. **Twice-daily scraping via systemd timer.** Simple, reliable, observable.
4. **The relational data model.** The normalized schema (locations, entities, endorsements as separate tables with junctions) is sound.
5. **HTML snapshot archival.** The 3.8 GB archive is the ultimate source of truth. Keep archiving.
6. **Provenance tracking.** The `sources` / `record_sources` system is a differentiator. Expand it, don't remove it.

---

## Part 6: Decision Points

Questions that should be answered before or during implementation:

1. **Raw value preservation (Phase 5.1):** Should we add `raw_*` columns to `license_records` and copy current values, or add a `record_raw_values` satellite table? The column approach is simpler; the satellite approach is cleaner.

2. **Entity resolution service:** Build or buy? What's the external service? This determines the adapter interface design.

3. **Graph visualization library:** D3.js force-directed graph? Cytoscape.js? Sigma.js? Depends on the interaction model we want.

4. **API-first or template-first for analysis views?** The entity network and license timeline could be JSON API endpoints consumed by a JS frontend, or server-rendered. The current HTMX approach works but may not scale to interactive graph exploration.

5. **Package structure timing:** Moving to `core/`, `ingest/`, `enrich/`, etc. packages is a large diff that touches every import. It could be done incrementally (extract one package at a time) or in a big-bang reorganization. The former is safer; the latter is cleaner.

---

## Appendix: Current Dependency Graph

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

    ┌──────────────────┐
    │address_validator │  ← isolated (only httpx + stdlib)
    │log_config        │  ← isolated (only stdlib)
    │migrate_locations │  ← isolated (only stdlib)
    └──────────────────┘
```

After refactoring, dependencies should flow strictly downward through layers:

```
    presentation  →  analysis/query  →  enrichment  →  ingestion  →  storage
                                         ↑
                                    adapters (called by enrichment)
```

No upward or lateral dependencies. No circular imports. No deferred imports.
