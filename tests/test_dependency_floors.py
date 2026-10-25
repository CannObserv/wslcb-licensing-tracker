"""Guard: locked and installed runtime deps stay at or above their advisory fixes (#221).

``uv audit --frozen --no-dev`` at bff24d3 reported 44 advisories across seven
packages. These tests fail if ``uv.lock`` or an installed tree goes back below
the first version that clears them all. The audit is still the source of truth:
this file only stops a known-bad version from coming back without anyone noticing.

starlette and python-multipart sit on the internet-facing request path, so
they matter most. mako is reached only through alembic at migration time.
"""

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


def _locked() -> dict[str, str]:
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    return {p["name"]: p["version"] for p in lock["package"]}


@pytest.mark.parametrize(("package", "floor"), sorted(FIXED.items()))
def test_lock_pins_fixed_version(package: str, floor: str) -> None:
    locked = _locked()[package]
    assert Version(locked) >= Version(floor), f"{package} {locked} < {floor}"


@pytest.mark.parametrize(("package", "floor"), sorted(FIXED.items()))
def test_installed_version_is_fixed(package: str, floor: str) -> None:
    got = installed_version(package)
    assert Version(got) >= Version(floor), f"{package} {got} < {floor}"
