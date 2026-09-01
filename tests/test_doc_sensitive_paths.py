"""Tests for .skills/doc-sensitive-paths — the doc-check.sh sensitive-path list.

`doc-check.sh` (vendored, shipping-work-python-fastapi) flags branch changes
that touch paths whose inventory is mirrored in project docs. The list is
tailored here rather than in a fork of the script (#172). Upstream exits 2 when
*no* entry matches any tracked file, because a list that cannot hit anything
prints the same clean green as a doc-neutral branch (gregoryfoster/skills#252).

These tests are the tighter, per-entry version of that check: a single entry
going inert as the tree evolves is caught here, long before the whole list dies.
"""

import functools
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
LIST_PATH = REPO_ROOT / ".skills" / "doc-sensitive-paths"

# The two `case` patterns in the vendored script that _path_matches ports. Kept
# verbatim so an upstream edit to the matcher fails the canary below instead of
# silently leaving this port asserting stale rules.
VENDORED_CASE_LINES = (
    'case "$file" in "$entry"*|*"/$entry"*) return 0 ;; esac',
    '"$entry"|*"/$entry"|"$entry"/*|*"/$entry"/*) return 0 ;;',
)


def _entries() -> list[str]:
    """Parse the list the way doc-check.sh does: strip comments and blanks."""
    lines = LIST_PATH.read_text(encoding="utf-8").splitlines()
    return [
        stripped for line in lines if (stripped := line.strip()) and not stripped.startswith("#")
    ]


@functools.cache
def _tracked_files() -> tuple[str, ...]:
    # Absolute path, not a bare "git": the test shells out, and resolving the
    # binary off $PATH is what ruff's S607 flags.
    git = shutil.which("git")
    if git is None:
        pytest.skip("git not on PATH")
    try:
        result = subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
            [git, "-c", "core.quotePath=false", "ls-files"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        # An exported tarball has the files but no .git. That is a checkout this
        # structural test cannot speak to, not a failing list.
        pytest.skip("not a git checkout")
    return tuple(result.stdout.splitlines())


def _vendored_doc_check() -> Path:
    """The vendored doc-check.sh, globbed the way pre-ship.sh resolves its delegate."""
    matches = sorted(
        REPO_ROOT.glob("skills-vendor/*/skills/shipping-work-python-fastapi/scripts/doc-check.sh")
    )
    if not matches:
        pytest.skip("vendored skills submodule not initialized")
    return matches[0]


def _path_matches(file: str, entry: str) -> bool:
    """Port of doc-check.sh's path_matches(): whole-segment, any depth.

    Source of truth is the vendored script; test_port_matches_vendored_matcher
    fails if the patterns this was derived from change upstream.
    """
    if entry.endswith("/"):
        return file.startswith(entry) or f"/{entry}" in file
    segments = file.split("/")
    return entry in segments


def test_list_exists_and_is_not_empty():
    assert LIST_PATH.is_file(), f"{LIST_PATH} is missing"
    assert _entries(), "doc-sensitive-paths lists no paths (doc-check.sh exits 2)"


def test_no_duplicate_entries():
    # Normalize the trailing slash first: `templates` and `templates/` match the
    # same files, so listing both is a duplicate even though the strings differ.
    normalized = [entry.rstrip("/") for entry in _entries()]
    assert len(normalized) == len(set(normalized)), (
        "duplicate entries in doc-sensitive-paths (compared ignoring trailing slashes)"
    )


def test_port_matches_vendored_matcher():
    """Canary: the vendored matcher still uses the patterns _path_matches ports."""
    source = _vendored_doc_check().read_text(encoding="utf-8")
    for line in VENDORED_CASE_LINES:
        assert line in source, (
            f"vendored doc-check.sh no longer contains {line!r} — "
            "re-read its path_matches() and update _path_matches to match"
        )


@pytest.mark.parametrize("entry", _entries() if LIST_PATH.is_file() else [])
def test_entry_matches_at_least_one_tracked_file(entry):
    """Every entry must be live. An inert entry is a gate that cannot fire."""
    tracked = _tracked_files()
    assert any(_path_matches(f, entry) for f in tracked), (
        f"{entry!r} matches no tracked file — remove it or fix the path"
    )
