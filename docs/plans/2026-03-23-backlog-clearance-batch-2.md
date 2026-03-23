# Backlog Clearance — Batch 2

**Date:** 2026-03-23
**Context:** Pre-production, runway to build right

## Goal

Clear 6 independent issues from the open backlog in a single parallel batch. All issues have disjoint file coverage — no merge conflicts, no sequencing constraints.

## Approved Approach

One batch, 6 parallel agents with worktree isolation. Each agent implements with TDD, runs full test suite, self-reviews, and signals completion. Orchestrator merges all into a `batch/a` branch for human review before merging to `main`.

## Prioritization Rubrics

**Score = (Foundation × 2) + (Correctness × 2) + Scope** (max 15)

| Dimension | 1 | 2 | 3 |
|---|---|---|---|
| **Foundation Leverage** | Standalone improvement | 1–2 other issues benefit | Multiple issues depend on this |
| **Correctness Risk** | Cosmetic / organizational | Edge-case incorrect behavior | Data loss, race conditions, silent failures |
| **Scope Clarity** | Requires design discovery | Clear direction, minor decisions | Mechanical — obvious from issue |

Blast radius drives sequencing, not score. (Moot this round — all disjoint.)

## Scored Backlog

| # | Title | Found. | Correct. | Scope | Score | Blast | Files |
|---|---|---|---|---|---|---|---|
| **#112** | Pipeline enrichment stubbed | 3 | 3 | 2 | **14** | High | `pg_pipeline.py` |
| **#100** | Replace `init_router()` injection | 2 | 2 | 2 | **10** | Med | `admin_routes.py`, `app.py` |
| **#99** | Cache consistency under multiple workers | 1 | 2 | 2 | **8** | Med | `pg_queries_filter.py`, `pg_queries_stats.py` |
| **#93** | JS test harness for source viewer | 2 | 1 | 2 | **8** | Low | `static/js/`, new test files, `package.json` |
| **#88** | CLI migration argparse → click | 2 | 1 | 2 | **8** | Med | `cli.py`, `pyproject.toml` |
| **#111** | Extend ruff to tests/ | 1 | 1 | 3 | **7** | Low | `.pre-commit-config.yaml` |

## Conflict Zones

None. All 6 issues have fully disjoint file coverage.

## Batch Execution Plan

| Batch | Issues | Agents | Gate |
|---|---|---|---|
| A | #112, #100, #99, #93, #88, #111 | 6 (parallel) | Start immediately |

**Merge strategy:** Regular merge commit to `main` (preserves per-agent history).

**Branch pattern:**
1. Sync local `main` (`git pull --ff-only`)
2. `git checkout -b batch/a`
3. Spawn 6 agents with `isolation: "worktree"` — output accumulates on `batch/a`
4. Run full test suite on `batch/a`
5. Human review → merge `batch/a` → `main`

## Key Decisions

- **#88 uses `click`** (not `typer`). `click` is already a transitive dependency; `typer` would add a new dep for no gain on a 15+ command CLI.
- **#99 approach TBD by agent** — issue lists 4 options. Agent should pick the simplest that satisfies acceptance criteria (likely option 4: remove caches entirely, or option 2: short TTL + accept staleness).
- **#93 test runner TBD by agent** — Vitest or Node built-in test runner. No `package.json` exists yet; agent creates one.
- **#112 wires existing functions** — `process_record()`, `parse_and_link_entities()`, `link_new_record()` already exist; pipeline stubs need replacement with actual calls.

## Deferred Items

| # | Title | Reason |
|---|---|---|
| #8 | Internet Archive ingest | Large feature effort — deferred until foundation is solid |
| #2 | Entity classification improvement | Large feature effort — needs design discussion |
| #79 | Malformed-city override mechanism | Deferred by user |
| #82 | Rate limiting | Low priority, low traffic, single instance |

## Out of Scope

- Schema migrations (none of these 6 issues require Alembic changes)
- UI changes beyond what #93 tests require
- Multi-worker deployment (documented as constraint in #99, not implemented)
