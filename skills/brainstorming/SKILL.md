---
name: brainstorming
description: Explores user intent, requirements, and design before any implementation. Use when the user says "brainstorm", "design this", "let's design", or proposes a new feature without a prior design discussion.
compatibility: Designed for Claude. Requires git and gh CLI. Python/SQLite web app (FastAPI + Jinja2/HTMX frontend, FTS5) using venv, pytest, systemd.
metadata:
  author: gregoryfoster
  version: "1.0"
  triggers: brainstorm, design this, let's design
  overrides: brainstorming
  override-reason: "Project-specific conventions: docs/plans/ path, Conventional Commits for design doc commit, writing-plans is optional not mandatory; invokes using-git-worktrees after design approval for any multi-step implementation"
---

# Brainstorming Ideas Into Designs — wslcb-licensing-tracker

Help turn ideas into fully formed designs and specs through natural collaborative dialogue. Understand the project context, ask clarifying questions, propose approaches, present a design for approval, and save it.

<HARD-GATE>
Do NOT write any code, create any files (other than the design doc), run any migrations, make schema changes, or take any implementation action until you have presented a design and the user has explicitly approved it. This applies regardless of perceived simplicity.
</HARD-GATE>

## Anti-Pattern: "This Is Too Simple To Need A Design"

Every non-trivial change goes through this process. Simple things become complex once unexamined assumptions surface. The design can be a few sentences — but you MUST present it and get approval before touching implementation.

## Scope detection

Brainstorming applies when (priority order):
1. **Explicit trigger** — user says "brainstorm", "design this", "let's design"
2. **New feature request** — user proposes functionality not yet discussed or designed in this conversation
3. **Ambiguous scope** — a request could be interpreted multiple ways; design discussion prevents wasted work

Brainstorming is **not** required for:
- Bug fixes with a clear, agreed-upon cause and solution
- Explicit directed tasks ("add this column", "fix this test") with no design ambiguity
- Continuation of previously approved design in the same conversation

## Checklist

Create a task for each item and complete them in order:

1. **Explore project context** — read AGENTS.md, check recent commits, review relevant files
2. **Ask clarifying questions** — one at a time; understand purpose, constraints, success criteria
3. **Propose 2–3 approaches** — with trade-offs and a recommendation
4. **Present design** — in sections scaled to complexity; get approval after each section
5. **Write design doc** — save to `docs/plans/YYYY-MM-DD-<topic>-design.md` and commit
6. **Set up worktree** — invoke `using-git-worktrees` for any multi-step implementation
7. **Hand off** — move to implementation, or invoke `writing-plans` if a formal plan is needed

## Process

### Exploring the idea

- Read AGENTS.md and relevant Key Files before asking questions
- Ask **one question at a time** — multiple questions overwhelm and get partial answers
- Prefer multiple-choice questions when options are bounded
- Focus on: purpose, constraints, success criteria, what failure looks like

### Proposing approaches

- Always propose 2–3 alternatives with explicit trade-offs
- Lead with your recommended option and explain why
- Apply YAGNI ruthlessly — remove scope creep from all options
- For DB changes: flag migration strategy, frozen/derived contract impact
- For UI changes: flag style guide compliance (co-purple palette, HTMX patterns)

### Presenting the design

- Scale each section to its complexity: a few sentences if simple, up to ~250 words if nuanced
- Ask after each major section whether it looks right before continuing
- Cover relevant dimensions: architecture, data model, migration safety, error handling, testing strategy, UI consistency
- Be ready to revise — go back if something doesn't land

### After design approval

**Write the design doc:**
- Path: `docs/plans/YYYY-MM-DD-<topic>-design.md`
- Include: goal, approved approach, key decisions and their rationale, out-of-scope items

**Open GitHub issue:**

Create a GitHub issue to track the work:

```bash
gh issue create \
  --title "<topic — concise imperative phrase>" \
  --body "$(cat <<'EOF'
## Summary
<1–3 sentence summary of what was designed>

## Design doc
`docs/plans/YYYY-MM-DD-<topic>-design.md`

## Scope
<bullet list of the key decisions / in-scope items from the design>
EOF
)"
```

- Title: short imperative phrase matching the design topic (e.g. "Add rate-limit header to validate endpoint")
- Report the issue number to the user (e.g. "Opened #42")

**Commit the design doc:**

- Use `#<n> docs:` prefix on the commit message citing the issue number
  ```
  #<n> docs: add design doc for <topic>
  ```

**Set up a worktree (multi-step implementation):**
- Invoke `using-git-worktrees` to create an isolated workspace on a feature branch
- Use `.worktrees/` as the local directory (verify it is gitignored first)
- Skip for single-commit or directed fixes where isolation adds no value

**Hand off:**
- For small changes: proceed directly to implementation
- For multi-step work: invoke `writing-plans` to create a task-by-task plan
- Do NOT invoke any other skill without asking

## Key Principles

- **One question at a time** — never stack multiple questions in one message
- **Multiple choice preferred** — easier to answer than open-ended when options are clear
- **YAGNI** — remove scope creep from every proposed approach
- **Project conventions first** — check AGENTS.md before proposing any architectural change
- **Incremental approval** — present design in sections, get buy-in as you go
- **Flexibility** — go back and clarify whenever something doesn't make sense

## Proactive suggestion

When a user makes a feature request without explicit design context (e.g., "add X to the UI"), suggest brainstorming before diving in:

> "Before I start, this looks like a good candidate for a quick design discussion to make sure we're aligned on approach. Want me to run brainstorming, or do you have a specific implementation in mind?"

This is a suggestion, not a HARD-GATE — if the user confirms they have a clear intent, proceed.
