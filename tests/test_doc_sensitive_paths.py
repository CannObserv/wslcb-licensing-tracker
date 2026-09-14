"""Tests for .skills/doc-sensitive-paths and .skills/doc-sections — doc-check.sh's tailoring.

`doc-check.sh` (vendored, shipping-work-python-fastapi) flags branch changes
that touch paths whose inventory is mirrored in project docs. The list is
tailored here rather than in a fork of the script (#172). Upstream exits 2 when
*no* entry matches any tracked file, because a list that cannot hit anything
prints the same clean green as a doc-neutral branch (gregoryfoster/skills#252).

These tests are the tighter, per-entry version of that check: a single entry
going inert as the tree evolves is caught here, long before the whole list dies.

`.skills/doc-sections` is the advice printed on a hit (#173). Upstream runs no
dead-entry check on it because advice is prose, so a renamed doc would go on
being named silently; the per-line check below closes that locally.
"""

import functools
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
LIST_PATH = REPO_ROOT / ".skills" / "doc-sensitive-paths"
SECTIONS_PATH = REPO_ROOT / ".skills" / "doc-sections"

# The two `case` patterns in the vendored script that _path_matches ports. Kept
# verbatim so an upstream edit to the matcher fails the canary below instead of
# silently leaving this port asserting stale rules.
VENDORED_CASE_LINES = (
    'case "$file" in "$entry"*|*"/$entry"*) return 0 ;; esac',
    '"$entry"|*"/$entry"|"$entry"/*|*"/$entry"/*) return 0 ;;',
)


def _entries(path: Path = LIST_PATH) -> list[str]:
    """Parse a .skills/ list the way doc-check.sh does: strip comments and blanks."""
    lines = path.read_text(encoding="utf-8").splitlines()
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


def test_sections_exist_and_are_not_empty():
    assert SECTIONS_PATH.is_file(), f"{SECTIONS_PATH} is missing"
    assert _entries(SECTIONS_PATH), "doc-sections lists no sections (doc-check.sh exits 2)"


@pytest.mark.parametrize("section", _entries(SECTIONS_PATH) if SECTIONS_PATH.is_file() else [])
def test_section_names_a_tracked_doc(section):
    """Each advice line is `<doc>: <sections>`, and the doc must still exist.

    This is the dead-entry check upstream deliberately skips for prose: a doc
    renamed out from under this file would otherwise be named on every hit.
    """
    doc, sep, what = section.partition(":")
    assert sep, f"{section!r} is not `<doc>: <sections>`"
    assert what.strip(), f"{section!r} names a doc but no sections in it"
    assert doc.strip() in _tracked_files(), f"{doc.strip()!r} is not a tracked file"


def _scratch_env() -> dict[str, str]:
    """Environment for every process that touches a scratch repo.

    Drops all inherited GIT_* variables: under a git hook or `rebase -x`,
    GIT_DIR / GIT_INDEX_FILE / GIT_WORK_TREE name the OUTER repo, and would
    redirect scratch-repo writes into it. Then isolates from the developer's
    git config, so a global hooksPath or signing setting cannot leak in either.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return {**env, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}


def _git(cwd: Path, *args: str) -> None:
    git = shutil.which("git")
    if git is None:
        pytest.skip("git not on PATH")
    result = subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
        [git, "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=cwd,
        env=_scratch_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"git {' '.join(args)} failed: {result.stderr.strip()}"


def _run_doc_check_in_scratch(scratch: Path) -> subprocess.CompletedProcess[str]:
    """Commit a sensitive-path change in a scratch repo and run doc-check.sh on it.

    The scratch repo carries both tailored files, so the verdict depends on this
    repo's .skills/, not on its branch history.
    """
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not on PATH")
    script = _vendored_doc_check()

    (scratch / ".skills").mkdir()
    for src in (LIST_PATH, SECTIONS_PATH):
        shutil.copy(src, scratch / ".skills" / src.name)
    (scratch / "AGENTS.md").write_text("base\n", encoding="utf-8")
    _git(scratch, "init", "-q", "-b", "main")
    _git(scratch, "add", "-A")
    _git(scratch, "commit", "-q", "-m", "base")
    _git(scratch, "checkout", "-q", "-b", "work")
    (scratch / "AGENTS.md").write_text("changed\n", encoding="utf-8")
    _git(scratch, "commit", "-q", "-am", "touch a sensitive path")

    return subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
        [bash, str(script), "--base", "main"],
        cwd=scratch,
        env=_scratch_env(),
        capture_output=True,
        text=True,
        check=False,
    )


def test_scratch_git_failure_reports_stderr(tmp_path):
    """A failed scratch-repo step must say why, not just 'exit status 128'."""
    with pytest.raises(AssertionError, match="is not a git command"):
        _git(tmp_path, "no-such-subcommand")


def test_scratch_repo_ignores_inherited_git_env(tmp_path, monkeypatch):
    """Under a git hook or `rebase -x`, GIT_DIR/GIT_INDEX_FILE name the OUTER repo.

    Inherited, they redirected every scratch-repo write there: the e2e test
    passed while committing into, and moving HEAD of, whatever repo they named.
    """
    decoy = tmp_path / "decoy"
    decoy.mkdir()
    _git(decoy, "init", "-q", "-b", "main")
    _git(decoy, "commit", "-q", "--allow-empty", "-m", "decoy")
    before = (decoy / ".git" / "HEAD").read_text(encoding="utf-8")
    refs_before = sorted(p.name for p in (decoy / ".git" / "refs" / "heads").iterdir())

    monkeypatch.setenv("GIT_DIR", str(decoy / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(decoy / ".git" / "index"))
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    result = _run_doc_check_in_scratch(scratch)

    assert (decoy / ".git" / "HEAD").read_text(encoding="utf-8") == before
    assert sorted(p.name for p in (decoy / ".git" / "refs" / "heads").iterdir()) == refs_before
    assert (scratch / ".git").is_dir(), "scratch repo was never created"
    assert result.returncode == 1, result.stdout + result.stderr


def test_doc_check_hit_routes_to_tailored_sections(tmp_path):
    """End to end: a hit prints this repo's advice, not upstream's defaults (#173)."""
    result = _run_doc_check_in_scratch(tmp_path)

    assert result.returncode == 1, result.stdout + result.stderr
    assert "(advice: .skills/doc-sections)" in result.stdout
    # Neither half may fall back to the skill's defaults, and the half-tailoring
    # note (gregoryfoster/skills#284) must not fire.
    assert "built-in defaults" not in result.stdout
    assert "Note: this project tailors" not in result.stdout
    for section in _entries(SECTIONS_PATH):
        assert f"  - {section}" in result.stdout
