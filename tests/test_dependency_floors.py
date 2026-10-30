"""Guard: locked and installed deps stay at or above their advisory fixes (#221, #224, #229).

``uv audit --frozen --no-dev`` at bff24d3 reported 44 advisories across seven
packages. These tests fail if ``uv.lock`` or an installed tree goes back below
the first version that clears them all. The audit is still the source of truth:
this file only stops a known-bad version from coming back without anyone noticing.

starlette and python-multipart sit on the internet-facing request path, so
they matter most. mako is reached only through alembic at migration time.

``uv audit --frozen`` with the dev group at 6fe28db added 30 more, in dev
tooling (#224). The prod ``.venv`` installs the dev group, so the installed-tree
check covers the host too. virtualenv and filelock run when pre-commit builds a
hook env; pytest and pygments run in the gate.

``npm audit`` flagged ws and form-data under jsdom, the only devDependency (#229).
They run only in ``node --test tests/js/``, so only ``package-lock.json`` is checked.
"""

import json
import tomllib
from importlib.metadata import version as installed_version
from pathlib import Path

import pytest
from packaging.version import Version

REPO_ROOT = Path(__file__).resolve().parents[1]

# package -> first version clearing every advisory uv audit listed for it (#221).
FIXED = {
    "starlette": "1.3.1",
    "python-multipart": "0.0.31",
    "anyio": "4.14.2",
    "idna": "3.15",
    "lxml": "6.1.0",
    "soupsieve": "2.9.0",
    "mako": "1.4.2",
}

# Same, for the dev group (#224, #229). filelock's fixes exist only in 4.x.
DEV_FIXED = {
    "virtualenv": "21.14.13",
    "filelock": "4.2.1",
    "pytest": "9.0.3",
    "pygments": "2.20.0",
}

FLOORS = sorted({**FIXED, **DEV_FIXED}.items())

# npm package -> floor (#229). ws 8.21.0 fixed GHSA-96hv-2xvq-fx4p, but its limit
# skipped empty fragments until 8.21.1.
NPM_FIXED = {
    "ws": "8.21.1",
    "form-data": "4.0.6",
}


def _locked() -> dict[str, str]:
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    return {p["name"]: p["version"] for p in lock["package"]}


@pytest.mark.parametrize(("package", "floor"), FLOORS)
def test_lock_pins_fixed_version(package: str, floor: str) -> None:
    locked = _locked()[package]
    assert Version(locked) >= Version(floor), f"{package} {locked} < {floor}"


@pytest.mark.parametrize(("package", "floor"), FLOORS)
def test_installed_version_is_fixed(package: str, floor: str) -> None:
    got = installed_version(package)
    assert Version(got) >= Version(floor), f"{package} {got} < {floor}"


def test_floor_tables_are_disjoint() -> None:
    # FLOORS merges the two tables, so a shared key would drop one floor silently.
    assert not FIXED.keys() & DEV_FIXED.keys()


@pytest.mark.parametrize(("package", "floor"), sorted(NPM_FIXED.items()))
def test_npm_lock_pins_fixed_version(package: str, floor: str) -> None:
    lock = json.loads((REPO_ROOT / "package-lock.json").read_text())
    copies = {
        path: entry["version"]
        for path, entry in lock["packages"].items()
        if path.rsplit("node_modules/", 1)[-1] == package
    }
    assert copies, f"{package} not in package-lock.json"
    for path, locked in copies.items():
        assert Version(locked) >= Version(floor), f"{path} {locked} < {floor}"
