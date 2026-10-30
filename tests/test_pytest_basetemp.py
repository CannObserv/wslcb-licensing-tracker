"""Guard: pytest keeps one basetemp, and only a failed run's (#200).

The host's disk GC owns tooling garbage, not a repo's test residue
(maintaining-host-disk ``references/repo-knobs.md``). Without these knobs
pytest keeps its last three ``/tmp/pytest-of-<user>/pytest-N`` trees, passed
or not. Read from the live config, so a rename or a typo in ``pyproject.toml``
fails here rather than silently restoring the default.
"""

import pytest


def test_keeps_one_basetemp(pytestconfig: pytest.Config) -> None:
    assert int(pytestconfig.getini("tmp_path_retention_count")) == 1


def test_keeps_only_a_failed_runs_tmp_paths(pytestconfig: pytest.Config) -> None:
    assert pytestconfig.getini("tmp_path_retention_policy") == "failed"
