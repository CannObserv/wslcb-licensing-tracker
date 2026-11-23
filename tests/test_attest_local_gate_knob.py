"""Drift guard: the `.skills/` knobs the local-gate attestation depends on (#260).

`.skills/attest-local-gate` turns on shipping-work's local-gate attestation
(gregoryfoster/skills#688): `push.sh` refuses a dirty or failing tree, then
posts `gate/local` = `success` on the SHA it pushed. The vendored reader takes
it from the commit being shipped, never the work tree, and anything but one
word (`on` or `off`) in a regular file is exit 2 before the push. So this
module asks the vendored `attest_read_knob` itself, against a scratch commit
of the file, rather than re-deriving its grammar.

`.skills/worktree_venv` stays untracked, as using-git-worktrees recommends for
a machine-local knob (here `none`: the main checkout is prod's
`WorkingDirectory=`). But the attestation's clean rule counts untracked files,
so an unignored copy made every main-checkout `push.sh` refuse. It must be
ignored, not committed.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ATTEST_KNOB = REPO_ROOT / ".skills" / "attest-local-gate"
VENV_KNOB = REPO_ROOT / ".skills" / "worktree_venv"
ATTEST_GATE = REPO_ROOT / "skills" / "shipping-work" / "scripts" / "_attest-gate.sh"


def _clean_env(*, isolated: bool = False) -> dict[str, str]:
    """The environment minus GIT_* (a hook's GIT_DIR would redirect git).

    `isolated` also drops the user's and the system's git config, so a scratch
    repository's commit can't trip on `commit.gpgsign` or a global hooks path.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    if isolated:
        env |= {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    return env


def _git(*args: str, cwd: Path = REPO_ROOT, isolated: bool = False) -> subprocess.CompletedProcess:
    """Run git under `_clean_env()`."""
    git = shutil.which("git")
    assert git
    return subprocess.run(  # noqa: S603 - fixed argv, repo-owned paths
        [git, *args],
        cwd=cwd,
        env=_clean_env(isolated=isolated),
        capture_output=True,
        text=True,
        check=False,
    )


def _settings(path: Path) -> list[str]:
    """The knob's words: blank lines and whole-line `#` comments dropped."""
    words: list[str] = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            words.extend(stripped.split())
    return words


@pytest.fixture(scope="module")
def tracked_modes() -> dict[str, str]:
    """Index mode of each tracked `.skills/` path, keyed by repo-relative path."""
    result = _git("ls-files", "-s", "--", ".skills")
    if result.returncode != 0:
        pytest.skip(f"not a git checkout: {result.stderr.strip()}")
    modes = {}
    for line in result.stdout.splitlines():
        meta, path = line.split("\t", 1)
        modes[path] = meta.split()[0]
    return modes


def test_attest_knob_is_a_tracked_regular_file(tracked_modes: dict[str, str]) -> None:
    """Read from the commit: untracked, or not a regular file, it attests nothing."""
    rel = ATTEST_KNOB.relative_to(REPO_ROOT).as_posix()
    assert rel in tracked_modes, f"{rel} is not tracked"
    assert tracked_modes[rel] in {"100644", "100755"}, (
        f"{rel} is mode {tracked_modes[rel]}; the reader takes only a regular file"
    )
    assert not ATTEST_KNOB.is_symlink()


def test_attest_knob_is_on() -> None:
    """One word, `on`: `off` or a typo would silently stop attesting."""
    assert _settings(ATTEST_KNOB) == ["on"]


def test_worktree_venv_knob_is_untracked(tracked_modes: dict[str, str]) -> None:
    """Machine-local: committing it would impose this host's `none` on every clone."""
    assert VENV_KNOB.relative_to(REPO_ROOT).as_posix() not in tracked_modes


def test_worktree_venv_knob_is_ignored() -> None:
    """Unignored, the main checkout's copy makes every attesting `push.sh` refuse."""
    rel = VENV_KNOB.relative_to(REPO_ROOT).as_posix()
    result = _git("check-ignore", "--no-index", "-q", rel)
    assert result.returncode == 0, f"{rel} is not ignored ({result.returncode}): {result.stderr}"


@pytest.mark.skipif(not ATTEST_GATE.is_file(), reason="skills-vendor submodule not checked out")
def test_vendored_reader_says_on(tmp_path: Path) -> None:
    """The vendored `attest_read_knob` reads the committed file as `on` (exit 0).

    It reads HEAD, so the file is committed to a scratch repository first,
    isolated from the user's git config. Exit 3 is `off`; exit 2 is an unusable
    knob, which refuses every push.
    """
    knob = tmp_path / ".skills" / "attest-local-gate"
    knob.parent.mkdir()
    shutil.copyfile(ATTEST_KNOB, knob)
    for args in (
        ("init", "-q"),
        ("add", ".skills/attest-local-gate"),
        ("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "knob"),
    ):
        step = _git(*args, cwd=tmp_path, isolated=True)
        assert step.returncode == 0, step.stderr

    bash = shutil.which("bash")
    assert bash
    result = subprocess.run(  # noqa: S603 - fixed argv, repo-owned paths
        [bash, "-c", '. "$1"; rc=0; attest_read_knob || rc=$?; exit $rc', "_", str(ATTEST_GATE)],
        cwd=tmp_path,
        env=_clean_env(isolated=True),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"exit {result.returncode}: {result.stderr}"
