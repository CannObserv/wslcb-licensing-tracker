"""The ship-gate wrapper loads the env in a linked worktree too (#235).

`skills/shipping-work-python-fastapi/scripts/pre-ship.sh` is project-local: it
loads the repo-root `.env` (TEST_DATABASE_URL), then execs the vendored gate.
A gitignored `.env` lives only in the primary checkout, so in a linked worktree
it loaded nothing, the PG tests skipped, the gate passed, and its per-SHA stamp
blessed the commit (gregoryfoster/skills#497).

The wrapper now falls back to the primary checkout's `.env`, warns when there
is none, and exports the names it loaded as `PRE_SHIP_ENV_NAMES`, which the
vendored gate folds into its stamp. These tests run the real wrapper against a
stand-in delegate. No test prints a value from an env file.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
WRAPPER_REL = Path("skills/shipping-work-python-fastapi/scripts/pre-ship.sh")
WRAPPER = REPO_ROOT / WRAPPER_REL
DELEGATE_REL = Path("skills-vendor/stand-in") / WRAPPER_REL

# Any output holding this means the wrapper printed a secret.
SECRET = "sk-do-not-print-235"  # noqa: S105 — a fixture, never a real value

# The stand-in delegate reports names, never values.
PROBE = """
for n in PRIMARY_ONLY WT_ONLY; do
  if [ -n "${!n+x}" ]; then echo "HAS_$n"; fi
done
if [ -n "${PRE_SHIP_ENV_NAMES+x}" ]; then
  echo "NAMES=[$PRE_SHIP_ENV_NAMES]"
else
  echo "NAMES_UNSET"
fi
echo "ARGS=[$*]"
"""


def _clean_env() -> dict[str, str]:
    """The caller's env without GIT_* and without names a test asserts on."""
    drop = {"PRE_SHIP_ENV_NAMES", "PRIMARY_ONLY", "WT_ONLY"}
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_") and k not in drop}


def _git(*args: str) -> None:
    subprocess.run(  # noqa: S603 - fixed argv
        ["git", *args],  # noqa: S607 - git on PATH, as the wrapper uses it
        check=True,
        capture_output=True,
        env=_clean_env(),
    )


def _stage(checkout: Path) -> None:
    """The real wrapper, and a stand-in for the vendored gate it globs for."""
    wrapper = checkout / WRAPPER_REL
    wrapper.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(WRAPPER, wrapper)
    delegate = checkout / DELEGATE_REL
    delegate.parent.mkdir(parents=True, exist_ok=True)
    delegate.write_text(PROBE)


@pytest.fixture
def checkouts(tmp_path: Path) -> tuple[Path, Path]:
    """A primary checkout with one commit, and a linked worktree of it, both staged."""
    primary = tmp_path / "primary"
    primary.mkdir()
    _git("-C", str(primary), "init", "-q")
    _git("-C", str(primary), "config", "user.email", "t@example.com")
    _git("-C", str(primary), "config", "user.name", "t")
    (primary / ".gitignore").write_text(".env\nskills/\nskills-vendor/\n")
    _git("-C", str(primary), "add", ".gitignore")
    _git("-C", str(primary), "commit", "-q", "-m", "init")
    worktree = tmp_path / "wt"
    _git("-C", str(primary), "worktree", "add", "-q", "-b", "wt", str(worktree))
    for checkout in (primary, worktree):
        _stage(checkout)
    return primary, worktree


def _run(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    r = subprocess.run(  # noqa: S603 - fixed argv, test-owned paths
        ["bash", str(cwd / WRAPPER_REL), *args],  # noqa: S607 - bash on PATH
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=30,
        env=_clean_env(),
        check=False,
    )
    assert SECRET not in r.stdout + r.stderr, "a value from an env file was printed"
    assert r.returncode == 0, r.stderr
    return r


def _warns(r: subprocess.CompletedProcess) -> list[str]:
    return [ln for ln in r.stderr.splitlines() if ln.startswith("WARN")]


def test_a_linked_worktree_loads_the_primary_checkouts_env(checkouts) -> None:
    primary, worktree = checkouts
    (primary / ".env").write_text(f"PRIMARY_ONLY={SECRET}\n")
    r = _run(worktree)
    assert "HAS_PRIMARY_ONLY" in r.stdout, r.stdout
    assert "NAMES=[PRIMARY_ONLY]" in r.stdout, r.stdout
    assert _warns(r) == []


def test_the_worktrees_own_env_wins(checkouts) -> None:
    """The primary's `.env` is a fallback, not a second layer."""
    primary, worktree = checkouts
    (primary / ".env").write_text(f"PRIMARY_ONLY={SECRET}\n")
    (worktree / ".env").write_text(f"WT_ONLY={SECRET}\n")
    r = _run(worktree)
    assert "HAS_WT_ONLY" in r.stdout, r.stdout
    assert "HAS_PRIMARY_ONLY" not in r.stdout, r.stdout
    assert "NAMES=[WT_ONLY]" in r.stdout, r.stdout


def test_the_primary_checkout_loads_its_own_env(checkouts) -> None:
    primary, _ = checkouts
    (primary / ".env").write_text(f"# a comment\nexport PRIMARY_ONLY='{SECRET}'\n")
    r = _run(primary)
    assert "NAMES=[PRIMARY_ONLY]" in r.stdout, r.stdout
    assert _warns(r) == []


def test_no_env_anywhere_warns_and_exports_no_names(checkouts) -> None:
    """An empty, exported list is what tells the gate to write no stamp."""
    primary, worktree = checkouts
    r = _run(worktree)
    assert "NAMES=[]" in r.stdout, r.stdout
    (warn,) = _warns(r)
    assert str(worktree / ".env") in warn
    assert str(primary / ".env") in warn


def test_arguments_reach_the_delegate(checkouts) -> None:
    _, worktree = checkouts
    assert "ARGS=[--help]" in _run(worktree, "--help").stdout


def test_the_wrapper_never_loads_the_production_env() -> None:
    """`/etc/wslcb-licensing-tracker/.env` holds prod DATABASE_URL (#235).

    The vendored recipe loads `/etc/<project>/.env` first. Here that file is
    group-readable by the gate's user, so porting that line would hand the
    production DSN to pytest.
    """
    code = [ln for ln in WRAPPER.read_text().splitlines() if not ln.lstrip().startswith("#")]
    assert not [ln for ln in code if "/etc/" in ln]
