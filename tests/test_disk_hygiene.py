"""Tests for disk_hygiene.py: the data-straggler compression, all that's left (#200).

The tooling tiers (VS Code, extensions, npm/_npx, uv, pre-commit, orphaned
worktrees) retired to the cohort's disk GC; see the module docstring.
"""

import re
from pathlib import Path
from unittest.mock import patch

from wslcb_licensing_tracker import disk_hygiene
from wslcb_licensing_tracker.disk_hygiene import compress_data_stragglers


class TestRetired:
    """The host must never run two GCs: this module touches data/ only."""

    def test_it_runs_nothing_and_walks_no_home(self):
        """The knob's not-restarter exception says the job runs no subprocess."""
        source = Path(disk_hygiene.__file__).read_text()
        for pattern in (r"^\s*(import|from) (subprocess|shutil)\b", r"Path\.home", r"rmtree"):
            assert not re.search(pattern, source, re.MULTILINE), pattern

    def test_the_old_entry_point_is_gone(self):
        assert not hasattr(disk_hygiene, "run_disk_hygiene")


class TestCompressDataStragglers:
    """Tests for `compress_data_stragglers` — thin wrapper over compress_files, no new logic."""

    def _make_snapshot(self, tmp_path):
        d = tmp_path / "wslcb" / "licensinginfo" / "2026" / "2026_01_01"
        d.mkdir(parents=True)
        f = d / "2026_01_01-test-v1.html"
        f.write_text("<html/>")
        return f

    def _make_diff(self, tmp_path):
        d = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
        d.mkdir(parents=True)
        f = d / "2025_06_15-notifications-diff.txt"
        f.write_text("diff content")
        return f

    def test_no_stragglers_returns_none_for_both(self, tmp_path):
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = compress_data_stragglers(dry_run=True)
        assert result["snapshots"] is None
        assert result["diffs"] is None

    def test_snapshot_straggler_compressed(self, tmp_path):
        f = self._make_snapshot(tmp_path)
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = compress_data_stragglers(dry_run=False)
        assert result["snapshots"].compressed == 1
        assert not f.exists()
        assert (f.parent / (f.name + ".gz")).exists()

    def test_diff_straggler_compressed(self, tmp_path):
        f = self._make_diff(tmp_path)
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = compress_data_stragglers(dry_run=False)
        assert result["diffs"].compressed == 1
        assert not f.exists()
        assert (f.parent / (f.name + ".gz")).exists()

    def test_dry_run_does_not_write(self, tmp_path):
        f = self._make_snapshot(tmp_path)
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = compress_data_stragglers(dry_run=True)
        assert result["snapshots"].compressed == 1
        assert f.exists()
