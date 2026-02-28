# Playbooks

Shorthand commands that map to well-defined, repeatable workflows. When the user references a playbook by name or trigger phrase, execute the matching procedure.

Playbooks are parameterized — the user may append context (e.g., `review entities.py`, `review #14`). When no scope is specified, infer from conversation context.

---

## `review` — Code & Documentation Review

**Triggers:** "review", "code review", "perform a review", "review following our usual pattern"

**Purpose:** Systematic review of code and documentation changes, structured for efficient async feedback.

### Scope Detection

Determine what to review, in priority order:
1. **Explicit scope** — user specifies files, a branch, a commit range, or an issue number
2. **Conversation context** — if this conversation implemented changes, review those changes
3. **Uncommitted work** — `git diff` and `git diff --staged`
4. **Ask** — if scope is ambiguous, ask before proceeding

### Procedure

#### Phase 1: Gather Context
- Read the diff (branch comparison, commit range, or working tree)
- Read AGENTS.md conventions relevant to the changed files
- Identify all files touched and their roles in the architecture
- Check the live app if UI changes are involved (browser screenshots)
- Run the app/imports to catch syntax errors

#### Phase 2: Analyze
Evaluate against these dimensions:
- **Correctness** — bugs, logic errors, edge cases, off-by-ones
- **Data integrity** — schema constraints, migration safety, FTS sync
- **Convention compliance** — AGENTS.md patterns (logging, naming, SQL style, template conventions)
- **Documentation** — do AGENTS.md, README.md, and code comments reflect the changes?
- **Robustness** — error handling, graceful degradation, idempotency
- **UX consistency** — if templates changed, do they follow the style guide?

#### Phase 3: Present Findings

Format the review as a structured report:

1. **Title** — `## Code & Documentation Review — [scope description]`

2. **What's solid** — brief list of things done well (reinforces good patterns; not filler)

3. **Numbered findings** — every actionable item gets a unique number for easy reference
   - **Top-level items:** `1.`, `2.`, `3.` (ever-incrementing, never reset)
   - **Sub-items:** `2a.`, `2b.` (for related points under one finding)
   - Each item includes:
     - **What:** precise description of the issue with file/line references
     - **Why it matters:** impact (bug? style? future maintenance?)
     - **Suggested fix:** concrete, not vague (code snippets when helpful)
   - Group by severity:
     - 🔴 **Bugs** — incorrect behavior, crashes, data corruption risk
     - 🟡 **Issues to fix** — not broken but should be addressed before shipping
     - 💭 **Minor / observations** — style, optional improvements, things noted but not blocking

4. **Summary** — 1–2 sentences on overall assessment and which items are highest priority

#### Phase 4: Wait for Feedback

**Stop and wait.** Do not make any changes until the user responds.

The user will respond with terse directives referencing item numbers:
- `1: fix` — implement the suggested fix
- `3: stet` — leave as-is (acknowledged, no action)
- `5: fix, but use X approach instead` — fix with user's preferred approach
- `2: document as TODO` — don't fix now, add a code comment or AGENTS.md note
- `7: investigate further` — gather more information before deciding

After receiving directives, implement all requested changes, commit, and present a summary table of what was done.

### If a Second Review Round is Requested

Continue numbering from where the previous round left off (e.g., if the first round ended at item 18, the second starts at 19). This maintains unambiguous references across the full review conversation.

### Documentation Sweep

If the reviewed changes affect:
- Database schema → update AGENTS.md schema section
- New files or public APIs → update AGENTS.md Key Files table and relevant sections
- User-facing behavior → update README.md
- Deployment or CLI → update AGENTS.md Common Tasks

The review should flag missing documentation updates as numbered items.
