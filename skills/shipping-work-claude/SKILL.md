---
name: shipping-work-claude
description: "Finalizes work by ensuring everything is committed, pushed to the remote, and reflected on GitHub: closes issues, posts summary comments, and presents a completion table. Use when the user says 'ship it', 'push GH', 'close GH', or 'wrap up'."
compatibility: Designed for Claude. Requires git and gh CLI. Python project using venv, pytest, systemd.
metadata:
  author: gregoryfoster
  version: "1.0"
  triggers: ship it, push GH, close GH, wrap up
  overrides: shipping-work-claude
  override-reason: "Concrete test commands (source venv/bin/activate && python -m pytest); encodes #<n> [type]: desc Conventional Commits convention; systemd restart reminder after web app changes"
---

# Shipping Work — wslcb-licensing-tracker

Finalizes work: tests, clean commit, push, GitHub issue comments and closure.

## Scope detection

Determine which GitHub issue(s) to close (priority order):
1. **Explicit scope** — user specifies issue number(s) (e.g., `wrap up #19 #20`)
2. **Conversation context** — issues referenced in recent commit messages or discussion
3. **Ask** — if ambiguous, confirm before closing anything

## Procedure

### Step 1 — Run tests

```bash
bash skills/shipping-work-claude/scripts/pre-ship.sh
```

Do not proceed if tests fail.

### Step 2 — Ensure a clean working tree

```bash
bash skills/shipping-work-claude/scripts/check-status.sh
```

If uncommitted changes exist, commit them using this project's convention (Conventional Commits):
```
#<number> [type]: <description>       # with GH issue
[type]: <description>                 # without GH issue
```
Multiple issues: `#19, #20 [type]: <description>`  
Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`

### Step 3 — Ensure on main

If on a feature branch, merge to `main` first. Then continue.

### Step 4 — Push

```bash
bash skills/shipping-work-claude/scripts/push.sh
```

### Step 5 — Comment on GitHub issues

For each issue in scope:

```bash
bash skills/shipping-work-claude/scripts/comment-issue.sh <number> "<summary>"
```

Comment must include:
- What was implemented (2–4 bullets)
- Key commit SHAs or commit range
- Any follow-up items or known limitations

### Step 6 — Close GitHub issues

```bash
bash skills/shipping-work-claude/scripts/close-issue.sh <number>
```

Never close an issue that wasn't fully implemented — ask first if uncertain.

### Step 7 — Report

Present a summary table:

| Issue | Title | Status | Comment |
|---|---|---|---|
| #19 | ... | ✅ Closed | Summary posted |

## Notes

- If web app files changed (`app.py`, `templates/`, `static/`), remind the user to `sudo systemctl restart wslcb-web.service` if the app is running in production
- If `gh` CLI hits errors, use `--json` flag workarounds as needed
- The project's AGENTS.md is authoritative for commit conventions — read it before committing
- Load GH token for `gh` commands: `export GH_TOKEN=$(grep GITHUB_TOKEN env | cut -d= -f2)`
