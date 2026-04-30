# Skills

Project skills available via the Skill tool or `/skill-name` in Claude Code. Implementations live in `skills/`; vendor skill repos are git submodules in `skills-vendor/`.

Invoke a skill by name when the trigger phrase matches — the skill provides structured prompts, anti-patterns, and step sequences that guide the work.

## brainstorming

Explores user intent, requirements, and design before any implementation. Produces structured questions, trade-off analysis, and a design sketch before writing code.

**Trigger:** User says "brainstorm", "design this", "let's design", or proposes a new feature without a prior design discussion.

## dispatching-parallel-agents

Routes 2+ independent tasks to parallel subagents using the Agent tool. Handles task decomposition, isolation verification, and result synthesis.

**Trigger:** 2+ clearly independent tasks that can proceed without shared state or sequential dependencies.

## managing-skills-claude

Manages external skill repos using the git submodule + symlink pattern. Adds skill repos as submodules under `skills-vendor/`, symlinks individual skills into `skills/` and `.claude/skills/` for discovery, handles updates and removal.

**Trigger:** "add skill repo", "add external skills", "manage skills", "update vendor skills".

## orchestrating-issue-backlog-claude

Prioritizes an open issue backlog using agreed rubrics, analyzes conflict zones and dependencies, designs a parallel-safe batch execution plan using git worktrees, produces a design doc and GitHub issue, then hands off to an agent team.

**Trigger:** Need to work through a backlog of issues systematically, in parallel.

## reviewing-architecture-claude

High-level architectural review evaluating structural health, design principles, and long-term maintainability. Produces a numbered findings report, waits for terse directives (`fix` / `stet` / `GH`), then implements approved refactors. Distinct from line-level code review.

**Trigger:** "AR", "architecture review", "architectural review".

## reviewing-code-claude

Structured code and documentation review using a severity-tiered findings format. Produces a numbered findings report, waits for terse directives (`fix` / `stet` / `GH`), then implements and commits approved changes.

**Trigger:** "CR", "code review", "perform a review".

## shipping-work-claude

Finalizes work by ensuring everything is committed, pushed to the remote, and reflected on GitHub: closes issues, posts summary comments, and presents a completion table.

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
