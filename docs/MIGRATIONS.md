# Migration Framework

Schema is managed by **Alembic** against PostgreSQL. Revisions live in
`alembic/versions/`; `alembic.ini` is at the **repo root**, not inside
`alembic/`.

- `alembic_version` (one row) tracks the applied head. Alembic skips revisions
  already recorded there, so `upgrade head` is idempotent.
- `schema.init_db()` runs `command.upgrade(cfg, "head")` programmatically on an
  existing connection. It is safe to call on every startup. There is no
  `create_all` anywhere — the migration chain is the only path that creates
  schema.
- Fresh databases replay the chain from `0001`, which is a full baseline: it
  creates every table, index, and seed row in one revision.

| Revision | Adds |
|---|---|
| `0001_baseline_postgresql_schema` | Full initial PostgreSQL schema — all tables, indexes, seed data. Subsumes the 15 migrations of the retired SQLite `PRAGMA user_version` framework, so their individual numbering no longer exists anywhere in the tree. |
| `0002_fts` | `pg_trgm` extension, `search_vector` column, FTS trigger |
| `0003_timestamp_columns` | Timestamp columns `TEXT` → `TIMESTAMPTZ` |
| `0004_nullable_std_address_line_2` | Makes `locations.std_address_line_2` nullable |
| `0005_address_validation_attempted_at` | `locations.address_validation_attempted_at`, backfilled from `address_validated_at` (#150) |
| `0006_co_replay_source_type` | Seeds the `co_replay` source type (#154) |
| `0007_replay_extract_role` | Allows the `replay_extract` `record_sources` role (#154) |
| `0008_relayed_scrape` | Seeds the `relayed_scrape` source type; `scrape_log.origin` + `scrape_log.captured_at` (#201) |
| `0009_entity_classification` | `entities.entity_subtype` + `entities.classification_source`; `entity_classifications`, `entity_review_flags`, `classification_runs` (#206) |

Each revision's `down_revision` points at the previous number, so the chain is
strictly linear — no branches or merge points.

**To add a migration:** change `models.py`, then
`uv run alembic revision --autogenerate -m "<description>"`, review the
generated file (autogenerate misses server defaults, CHECK constraints, and
data backfills), and verify with `uv run alembic upgrade head` against an empty
database. Do **not** hand-edit `0001` to carry new columns — that was the
SQLite-era pattern and it breaks every database already past the baseline.
Set `DATABASE_URL` for that verify run: unset, `alembic/env.py` falls back to
`alembic.ini`'s URL, the shared prod/dev `wslcb`.

**Not to be confused with** `data_migration.py`, the run-once *data* migration
framework (#85). That one reshapes rows, tracks its own completion, and is
independent of the Alembic schema chain.
