# Skills

Project skills available via the Skill tool or `/skill-name` in Claude Code. Implementations live in `skills/`; vendor skill repos are git submodules in `skills-vendor/`.

Invoke a skill by name when the trigger phrase matches — the skill provides structured prompts, anti-patterns, and step sequences that guide the work.

## Quick reference

| Skill | When to use |
|---|---|
| `auditing-ci-cost` | "audit CI", "CI cost", "Actions spend" — no GitHub Actions here yet, so it is vendored ahead of need |
| `brainstorming` | New feature without a prior design discussion |
| `curating-context` | Say "curate context", "context budget", or "trim AGENTS.md" — also the weekly maintenance run |
| `dispatching-parallel-agents` | 2+ independent tasks with no shared state |
| `enforcing-architecture` | "add a fitness function", "enforce this contract", "lock this rule" (also invoked by `reviewing-architecture` on a `fitness` directive) |
| `init-socraticode` | "init socraticode", "set up code search", "index this project" — installs/enables the SocratiCode plugin + Qdrant and runs a full index |
| `managing-skills` | Add/update/remove skill repos (submodule + symlink) |
| `orchestrating-issue-backlog` | Prioritize backlog, design parallel execution plan |
| `reviewing-architecture` | Say "AR" or "architecture review" |
| `reviewing-code-python-fastapi` | Say "CR" or "code review" |
| `shipping-work-python-fastapi` | Say "ship it", "push GH", or "wrap up" |
| `subagent-driven-development` | Execute plans with independent tasks in current session |
| `systematic-debugging` | Any bug, test failure, or unexpected behavior |
| `test-driven-development` | Before writing implementation code |
| `using-git-worktrees` | Feature isolation or before executing plans |
| `verification-before-completion` | Before claiming work complete or committing |
| `writing-plans` | Have spec/requirements, before touching code |
| `writing-skills` | Create, edit, or verify skills |

## auditing-ci-cost

Audits a repository's GitHub Actions spend by measuring its cost shape first: a per-job billed-minute census computed from job timestamps, platform-incident days separated from the structural baseline, then a prescription that branches on whether the spend is job **count** or job **duration** — the levers for one are actively wrong for the other. Emits severity-tiered findings with a measured-or-estimated confidence label per finding, a path-filter replay gated on zero false skips, and a required non-levers section, then hands an accepted filter to `enforcing-architecture`. Needs an authenticated `gh` and `jq`.

This project has no hosted CI at all — no `.github/` tree; the scheduled work in `infra/` is host-side systemd timers (scrape, healthcheck, disk hygiene), which Actions billing never sees. The skill is linked so it is available the day that changes; until then it has no subject to measure.

**Trigger:** "audit CI", "CI cost", "Actions spend", "optimize CI", "why is CI so expensive".

## brainstorming

Explores user intent, requirements, and design before any implementation. Produces structured questions, trade-off analysis, and a design sketch before writing code.

**Trigger:** User says "brainstorm", "design this", "let's design", or proposes a new feature without a prior design discussion.

## curating-context

Curates the agent-context surface — `AGENTS.md` plus the reference docs it links — against a 6,000-token budget. Measures exactly via the Anthropic `count_tokens` endpoint (needs an Anthropic API credential in the repo-root `.env`; the skill's Phase 0 preflight names the variable), verifies falsifiable claims before removing them, classifies each section keep/demote/tighten/delete, relocates rather than deletes, then appends a telemetry row to `.skills/context-metrics.jsonl`.

**Trigger:** "curate context", "context budget", "hone AGENTS.md", "trim AGENTS.md", "prune context"; also the weekly maintenance run.

## dispatching-parallel-agents

Routes 2+ independent tasks to parallel subagents using the Agent tool. Handles task decomposition, isolation verification, and result synthesis.

**Trigger:** 2+ clearly independent tasks that can proceed without shared state or sequential dependencies.

## enforcing-architecture

Graduates an accepted architecture-review finding into an executable fitness function — picks the stack's tool (import-linter, dependency-cruiser, deptrac, module-size gate, or an OpenAPI drift guard), encodes the specific rule the finding fixed, adds the dev dependency, documents the contract in AGENTS.md, and wires it into the project's check surface as a reviewable diff. `reviewing-architecture` delegates here when a finding is accepted with a `fix + fitness` or bare `fitness` directive.

**Trigger:** "add a fitness function", "enforce this contract", "lock this rule".

## init-socraticode

Installs, configures, and indexes SocratiCode semantic code search on the project: Docker/Node preflight, plugin enablement, a project-adapted Code Exploration Policy + [`docs/SOCRATICODE.md`](SOCRATICODE.md), **two** SessionStart hooks (prefetch reminder and a once-per-day health check), a context-artifacts manifest, and a full blocking index verified by graph **edge yield** rather than by graph status — `READY` is reachable over a graph that resolved almost nothing. Its Phase 4 also prescribes the re-index after `.socraticodeignore` changes.

Re-running it on a project that already has SocratiCode **is** the audit: every phase is idempotent, and Phase 6 re-measures the yield.

**Trigger:** "init socraticode", "set up code search", "index this project", "socraticode setup".

## managing-skills

Manages external skill repos using the git submodule + symlink pattern. Adds skill repos as submodules under `skills-vendor/`, symlinks individual skills into `skills/` and `.claude/skills/` for discovery, handles updates and removal.

**Trigger:** "add skill repo", "add external skills", "manage skills", "update vendor skills".

## orchestrating-issue-backlog

Prioritizes an open issue backlog using agreed rubrics, analyzes conflict zones and dependencies, designs a parallel-safe batch execution plan using git worktrees, produces a design doc and GitHub issue, then hands off to an agent team.

**Trigger:** Need to work through a backlog of issues systematically, in parallel.

## reviewing-architecture

High-level architectural review evaluating structural health, design principles, and long-term maintainability. Produces a numbered findings report, waits for terse directives (`fix` / `stet` / `GH`), then implements approved refactors. Distinct from line-level code review.

**Trigger:** "AR", "architecture review", "architectural review".

## reviewing-code-python-fastapi

Structured code and documentation review using a severity-tiered findings format. Tuned for Python FastAPI projects (uv + ruff + pytest). Produces a numbered findings report, waits for terse directives (`fix` / `stet` / `GH`), then implements and commits approved changes.

**Trigger:** "CR", "code review", "perform a review".

## shipping-work-python-fastapi

Finalizes work by ensuring everything is committed, pushed to the remote, and reflected on GitHub: closes issues, posts summary comments, and presents a completion table. Tuned for Python FastAPI projects (uv + ruff + pytest).

Vendored except for `scripts/pre-ship.sh`, which is this project's one real fork: it **parses** `$PROJECT_ROOT/.env` line by line before delegating to the vendored gate, so the PostgreSQL tests see `TEST_DATABASE_URL` instead of silently skipping (168 of them). It never *sources* that file — `source` executes it, and `.env` holds live PATs.

**Trigger:** "ship it", "push GH", "close GH", "wrap up".

## subagent-driven-development

Executes implementation plans with independent tasks in the current session using subagents. Uses an implementer → spec-reviewer → code-quality-reviewer pipeline.

**Trigger:** Executing a multi-task implementation plan where tasks are independent.

## systematic-debugging

Structured debugging protocol: reproduce first, isolate, trace root cause, fix, verify. Resists pressure to skip steps or guess. Load before proposing any fix.

**Trigger:** Any bug, test failure, or unexpected behavior.

## test-driven-development

Red/Green TDD: write a failing test, then implement to pass it. Covers anti-patterns, fixture design, and isolation rules for this codebase.

**Trigger:** Implementing any feature or bugfix, before writing implementation code.

## using-git-worktrees

Creates isolated git worktrees for feature work, with smart directory selection and safety verification to avoid contaminating the main workspace.

**Trigger:** Starting feature work that needs isolation, or before executing implementation plans.

## verification-before-completion

Requires running verification commands and confirming output before claiming work is complete, fixed, or passing. Evidence before assertions always.

**Trigger:** About to claim work is complete, fixed, or passing; before committing or creating PRs.

## writing-plans

Produces a structured implementation plan (numbered tasks, acceptance criteria, dependencies) before touching code. Saves plans to `docs/plans/`.

**Trigger:** Have spec or requirements for a multi-step task, before touching code.

## writing-skills

Creates, edits, and verifies skills using best practices. Includes testing with subagents before deployment.

**Trigger:** Creating, editing, or verifying skills.
