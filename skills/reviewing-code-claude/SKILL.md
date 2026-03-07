---
name: reviewing-code-claude
description: Performs a structured code and documentation review using a severity-tiered findings format. Use when the user says "CR", "code review", or "perform a review". Produces a numbered findings report, waits for terse directives (fix/stet/GH), then implements and commits approved changes.
compatibility: Designed for Claude. Requires git and gh CLI. Python/SQLite web app (FastAPI + Jinja2/HTMX frontend, FTS5) using venv, pytest, systemd.
metadata:
  author: gregoryfoster
  version: "1.0"
  triggers: CR, code review, perform a review
  overrides: reviewing-code-claude
  override-reason: Python/SQLite/FastAPI-specific review dimensions (migration safety, FTS sync, frozen/derived data contract, Tailwind/HTMX style guide); runs pytest in venv during gather-context; systemd deployment awareness
  version: "1.1"
---

# Code & Documentation Review — wslcb-licensing-tracker

A systematic review workflow for this Python/SQLite/FastAPI/HTMX project. Produces a numbered findings report, waits for directives, then implements approved changes.

## Scope detection

Determine what to review (priority order):
1. **Explicit scope** — files, branch, commit range, or issue number specified by the user
2. **Conversation context** — changes implemented in this conversation
3. **Uncommitted work** — `git diff` and `git diff --staged`
4. **Ask** — if scope is ambiguous, ask before proceeding

## The Iron Law

```
NO FINDINGS REPORT WITHOUT RUNNING THE TEST SUITE FIRST
NO CHANGES WITHOUT A FINDINGS REPORT AND EXPLICIT USER DIRECTIVES
```

If you haven't run `gather-context.sh` and confirmed tests pass, you have not completed Phase 1.
If the user hasn't responded with directives, you cannot implement anything.

## Rationalization prevention

| Thought | Reality |
|---|---|
| "It's a small change, no need for a full review" | Size doesn't determine risk. Run the review. |
| "I just implemented this, I know it's correct" | Familiarity bias. Fresh pass finds what implementation blindness missed. |
| "Tests are passing, that's the review" | Tests verify behavior, not convention compliance, migration safety, or docs. |
| "The user seems in a hurry" | A fast broken change is slower than a thorough correct one. |
| "I'll fix things as I find them" | Phase 4 exists. Present first, implement after directives. |
| "This file wasn't in the diff" | Related files need review too. Check call sites, tests, AGENTS.md. |

## Procedure

### Phase 1 — Gather context

```bash
bash skills/reviewing-code-claude/scripts/gather-context.sh
```

Also:
- Read AGENTS.md conventions relevant to the changed files
- Identify all files touched and their roles in the architecture (see AGENTS.md Key Files)
- Check the live app if template/UI changes are involved (browser screenshot)

### Phase 2 — Analyze

Evaluate against these dimensions:

- **Correctness** — bugs, logic errors, edge cases, off-by-ones
- **Data integrity** — schema constraints, migration safety, FTS trigger sync; respect the frozen/derived data contract (AGENTS.md)
- **Migration safety** — new columns/tables must appear in BOTH `_m001_baseline()` (fresh installs) AND a new numbered migration function; bump `MIGRATIONS` list
- **Convention compliance** — AGENTS.md patterns: no `print()` for operational output, `logger = logging.getLogger(__name__)`, `%s`-style log formatting, SQL naming, `insert_record()` dedup contract
- **Idempotency** — backfill, reprocess, and pipeline operations must be safe to re-run
- **Documentation** — do AGENTS.md schema tables, Key Files, and Common Tasks reflect the changes?
- **Robustness** — error handling, graceful degradation, `IntegrityError` handling in DB helpers
- **UX consistency** — if templates changed: co-purple accent, semantic badge colors, style guide (AGENTS.md Style Guide section)
- **Test coverage** — TDD discipline; every behavioral change needs a test; no network/disk-DB in tests

### Phase 3 — Present findings

Title: `## Code & Documentation Review — [scope]`

1. **What's solid** — genuine positives, not filler
2. **Numbered findings** — sequential across ALL severity groups, never reset between them
   - Top-level: `1.`, `2.`, `3.` — Sub-items: `2a.`, `2b.`
   - Each finding: **What** (file:line) · **Why it matters** · **Suggested fix** (code snippet when useful)
   - Groups: 🔴 Bugs → 🟡 Issues to fix → 💭 Minor/observations
3. **Summary** — 1–2 sentences on overall assessment and top priorities

### Phase 3.5 — Verify before reporting

Before presenting findings:

```
NO COMPLETION CLAIMS WITHOUT FRESH VERIFICATION
```

- Re-run tests if any implementation happened in this conversation
- If tests fail: report the failure as a 🔴 finding regardless of cause
- Do NOT claim "tests pass" unless you have output from this session confirming it

### Phase 4 — Wait for feedback

**Stop. Do not make changes until the user responds.**

Accepted directives (reference by item number):

| Directive | Meaning |
|---|---|
| `1: fix` | Implement the suggested fix |
| `3: stet` | Leave as-is |
| `5: fix, but use X approach` | Fix with user's preferred approach |
| `2: document as TODO` | Add a code comment or AGENTS.md note |
| `7: investigate further` | Gather more information first |
| `10: GH` | Create or update a GitHub issue |

After directives, implement all requested changes. Before committing:
- Run `python -m pytest tests/ -v` and confirm all pass
- If anything fails, report before committing

Then commit and present a summary table:

| Item | Action | Result |
|---|---|---|
| 1 | Fixed | `queries.py:42 — added bounds check` |
| 3 | Stet | — |

## Second review rounds

Continue numbering from where the previous round ended. Never reset.

## Documentation sweep

Flag missing documentation updates as numbered findings when changes affect:

- **Database schema** → AGENTS.md schema section (table descriptions, column notes)
- **New files or public APIs** → AGENTS.md Key Files table and relevant sections
- **User-facing behaviour** → README.md
- **Deployment or CLI** → AGENTS.md Common Tasks

## Parameterized invocation

Triggers may include scope inline — e.g., `CR #14`, `code review queries.py`. Apply the appended context as the explicit scope (step 1 of scope detection).
