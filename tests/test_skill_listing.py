"""Guards for the session skill listing (docs/SKILLS.md → Listing overrides, #268).

Ported from CannObserv/address-validator#442, which holds the method and the
measurement recipe.

- Every ``skillOverrides`` key in ``.claude/settings.json`` names a skill under
  ``.claude/skills/`` or a Claude Code built-in listed in ``BUILTIN``: a renamed
  skill would otherwise drop back into the listing at full cost, and the stale
  key would hide nothing. Built-ins are keyed by skill name, not slash alias
  (``/ship`` is ``ship-local``).
- ``BUILTIN`` holds no name without an override, so the allowlist cannot
  quietly widen what the stale-key check accepts.
- A skill that a skill or tool hands off to through the Skill tool
  (``DELEGATED_TO``) must stay callable by Claude, so it may be ``name-only``
  but never ``user-invocable-only`` or ``off``.
- Skills in ``KEEP_ON`` stay fully listed: Claude must trigger them from the
  user's wording, or they run weekly / on cadence.

Unlike address-validator, there is no in-repo strict-YAML frontmatter check:
every skill here is a vendored symlink, so frontmatter fixes go upstream.
"""

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SETTINGS = REPO_ROOT / ".claude" / "settings.json"
CLAUDE_SKILLS = REPO_ROOT / ".claude" / "skills"

ALLOWED = {"on", "name-only", "user-invocable-only", "off"}

# Claude Code built-in skills this repo overrides. Not on disk, so listed
# here; add a name only with the override that uses it.
BUILTIN = {
    "claude-api",
    "code-review",
    "dataviz",
    "fewer-permission-prompts",
    "keybindings-help",
    "run",
    "ship-local",
    "simplify",
    "steward",
    "workflow-authoring",
}

# Kept fully listed on purpose (#268): built-ins Claude must trigger from the
# user's wording ("every 5 minutes", "from now on when X"), plus repo skills
# in regular use, on a cadence, or with a trigger the name doesn't carry.
KEEP_ON = {
    "loop",
    "schedule",
    "update-config",
    "curating-context",
    "patching-hosts",
    "reviewing-architecture",
    "reviewing-code-python-fastapi",
    "shipping-work",
    "using-git-worktrees",
}

# skill -> what invokes it via the Skill tool, so it must stay model-callable.
DELEGATED_TO = {
    "enforcing-architecture": "reviewing-architecture and auditing-ci-cost",
    "writing-plans": "brainstorming",
    "test-driven-development": "systematic-debugging",
    "verification-before-completion": "systematic-debugging",
    "workflow-authoring": "the Workflow tool's instructions",
}


def _overrides() -> dict[str, str]:
    return json.loads(SETTINGS.read_text()).get("skillOverrides", {})


def test_overrides_present():
    """An empty block would pass every check below while trimming nothing."""
    assert _overrides(), "skillOverrides missing from .claude/settings.json (#268)"


def test_override_keys_name_existing_skills():
    known = {p.name for p in CLAUDE_SKILLS.iterdir()} | BUILTIN
    stale = sorted(set(_overrides()) - known)
    assert not stale, f"skillOverrides keys naming no repo skill or BUILTIN: {stale}"


def test_builtin_list_has_no_unused_names():
    unused = sorted(BUILTIN - set(_overrides()))
    assert not unused, f"BUILTIN names with no override: {unused}"


def test_override_values_are_valid():
    bad = {k: v for k, v in _overrides().items() if v not in ALLOWED}
    assert not bad, f"invalid skillOverrides values: {bad}"


@pytest.mark.parametrize("skill", sorted(KEEP_ON))
def test_kept_skills_stay_listed(skill):
    assert _overrides().get(skill, "on") == "on", f"{skill} is kept fully listed (#268)"


@pytest.mark.parametrize("skill", sorted(DELEGATED_TO))
def test_delegated_skills_stay_model_callable(skill):
    mode = _overrides().get(skill, "on")
    assert mode in {"on", "name-only"}, (
        f"{skill} is invoked by {DELEGATED_TO[skill]} via the Skill tool; "
        f"{mode!r} would block that hand-off"
    )
