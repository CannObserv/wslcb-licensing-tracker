# Playbooks

Shorthand commands that map to well-defined, repeatable workflows. When the user references a playbook by name or trigger phrase, execute the matching procedure.

Playbooks are parameterized — the user may append context (e.g., `code review entities.py`, `CR #14`). When no scope is specified, infer from conversation context.

---

## `review` — Code & Documentation Review

**Triggers:** "CR", "code review", "perform a review"

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
- `10: GH` - create or update a corresponding GitHub issue

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

---

## `ship` — Commit, Push, and Close GitHub Issues

**Triggers:** "ship it", "push GH", "close GH", "wrap up"

**Purpose:** Finalize work by ensuring everything is committed, pushed, and reflected on GitHub.

### Scope Detection

Determine which GitHub issue(s) to close, in priority order:
1. **Explicit scope** — user specifies issue number(s) (e.g., `wrap up #19 #20`)
2. **Conversation context** — infer from issues referenced in recent commit messages or discussion
3. **Ask** — if ambiguous, confirm before closing anything

### Procedure

#### Step 1: Ensure Clean Working Tree
- Check `git status` for uncommitted changes
- If changes exist, commit them with the `#<number>: ` message prefix convention
- If multiple issues are in scope, prefix with all (e.g., `#19, #20: ...`)

#### Step 2: Ensure on `main`
- If on a feature branch, merge to `main` first
- If already on `main`, continue

#### Step 3: Push to Origin
- `git push origin main`
- Confirm push succeeded

#### Step 4: Comment on GitHub Issues
- For each issue in scope, post a summary comment via `gh issue comment`
- Comment should include:
  - What was implemented (brief, 2–4 bullets)
  - Commit range or key commit SHAs
  - Any follow-up items or known limitations noted during implementation

#### Step 5: Close GitHub Issues
- `gh issue close <number>` for each issue in scope
- Confirm closure succeeded

#### Step 6: Report
- Present a summary table:

| Issue | Title | Status | Comment |
|---|---|---|---|
| #19 | ... | ✅ Closed | Summary posted |

### Notes
- If `gh` CLI hits errors (e.g., Projects Classic deprecation), use `--json` flag workarounds as needed
- Never close an issue that wasn't fully implemented — ask first if uncertain
- If tests haven't been run this session, run them before pushing
