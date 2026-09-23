"""Tests for the dated-docs layout — specs in docs/specs/, plans in docs/plans/ (#176).

Vendored obra-superpowers skills (`brainstorming`, and example text in
`subagent-driven-development`) default to `docs/superpowers/{specs,plans}/`.
Their own "user preferences override this default" clause defers to the
project, and AGENTS.md supplies that preference. These tests keep the stated
override and the tree in step: a stray `docs/superpowers/` fails here instead
of surfacing in a PR, and the AGENTS.md line can't be trimmed away silently.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS = REPO_ROOT / "docs"


def test_no_superpowers_docs_dir():
    """The vendored-skill default path is overridden and must not exist."""
    stray = DOCS / "superpowers"
    assert not stray.exists(), (
        f"{stray.relative_to(REPO_ROOT)} exists — move specs to docs/specs/ and "
        "plans to docs/plans/ (see AGENTS.md, #176)"
    )


def test_project_docs_dirs_exist():
    """The override targets are real directories."""
    for name in ("specs", "plans"):
        assert (DOCS / name).is_dir(), f"docs/{name}/ missing"


def test_agents_md_states_override():
    """AGENTS.md is the point-of-use statement the vendored skills defer to."""
    text = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    assert "docs/superpowers/" in text, (
        "AGENTS.md no longer states the docs/superpowers/ override (#176)"
    )
