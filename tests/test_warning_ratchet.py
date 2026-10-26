"""Guard: the pytest warning ratchet fails a run on a new warning (#216).

The suite went from 1 to 3 warnings over #205 without anyone noticing, since a
warning never fails a run. ``filterwarnings`` in ``pyproject.toml`` now makes
the kinds that hid real bugs errors. These tests copy that list into a scratch
pytest project, run one probe test per case in a subprocess, and check each
probe's outcome, so the ratchet can't be loosened or deleted unnoticed.

The orphan-coroutine probe is the case that motivated #216: an ``AsyncMock``
called without ``await``, whose warning fires at garbage collection.
"""

import os
import subprocess
import sys
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

PROBES = """
import gc
import warnings
from unittest.mock import AsyncMock

import pytest
from starlette.exceptions import StarletteDeprecationWarning

HTTPX_MSG = "Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead."


def _warn_from(module, category):
    code = compile(f"import warnings\\nwarnings.warn('probe', {category})", "<probe>", "exec")
    exec(code, {"__name__": module})


def test_runtime_warning():
    warnings.warn("probe", RuntimeWarning)


def test_pytest_warning():
    warnings.warn(pytest.PytestWarning("probe"))


def test_orphan_asyncmock_coroutine():
    AsyncMock()()
    gc.collect()


def test_own_deprecation():
    _warn_from("wslcb_licensing_tracker.probe", "DeprecationWarning")


def test_third_party_deprecation():
    _warn_from("somelib.probe", "DeprecationWarning")


def test_starlette_httpx_deprecation_is_ignored():
    with warnings.catch_warnings(record=True) as rec:
        warnings.warn(HTTPX_MSG, StarletteDeprecationWarning)
    assert not rec


def test_other_starlette_deprecation_still_warns():
    with warnings.catch_warnings(record=True) as rec:
        warnings.warn("some other starlette deprecation", StarletteDeprecationWarning)
    assert rec
"""


def _filters() -> list[str]:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    return pyproject["tool"]["pytest"]["ini_options"].get("filterwarnings", [])


@pytest.fixture(scope="module")
def outcomes(tmp_path_factory) -> dict[str, str]:
    """Run the probes under the project's filters; map test name -> outcome."""
    root = tmp_path_factory.mktemp("ratchet")
    ini = "[pytest]\nfilterwarnings =\n" + "".join(f"    {f}\n" for f in _filters())
    (root / "pytest.ini").write_text(ini)
    (root / "test_probes.py").write_text(PROBES)
    report = root / "report.xml"
    # Without COV_CORE_*, pytest-cov's subprocess hook would measure the child
    # without branch data, and the parent's combine then fails.
    env = {
        k: v
        for k, v in os.environ.items()
        if k != "PYTEST_ADDOPTS" and not k.startswith(("COV_CORE_", "COVERAGE_"))
    }
    subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", f"--junitxml={report}"],
        cwd=root,
        env=env,
        capture_output=True,
        timeout=120,
        check=False,
    )
    result = {}
    for case in ET.parse(report).iter("testcase"):  # noqa: S314 — our own subprocess's report
        kinds = {child.tag for child in case} & {"failure", "error", "skipped"}
        result[case.get("name")] = kinds.pop() if kinds else "passed"
    return result


@pytest.mark.parametrize(
    ("probe", "expected"),
    [
        ("test_runtime_warning", "failure"),
        ("test_pytest_warning", "failure"),
        ("test_own_deprecation", "failure"),
        ("test_third_party_deprecation", "passed"),
        ("test_starlette_httpx_deprecation_is_ignored", "passed"),
        ("test_other_starlette_deprecation_still_warns", "passed"),
    ],
)
def test_probe_outcome(outcomes, probe, expected):
    assert outcomes.get(probe) == expected


def test_orphan_asyncmock_coroutine_fails_the_run(outcomes):
    # The warning fires in the coroutine's finalizer, so pytest reports it as an
    # unraisable exception: a failure or an error, depending on the phase it lands in.
    assert outcomes.get("test_orphan_asyncmock_coroutine") in {"failure", "error"}
