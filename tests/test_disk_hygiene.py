"""Tests for disk_hygiene.py — pure selection logic + fail-open removal helpers."""

import json
import os
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from wslcb_licensing_tracker.disk_hygiene import (
    _remove_path,
    _run_subprocess_step,
    check_usage_threshold,
    compress_data_stragglers,
    find_orphaned_worktrees,
    run_disk_hygiene,
    select_aged_paths,
    select_extensions_to_prune,
    select_vscode_servers_to_prune,
)


class TestSelectVscodeServersToPrune:
    """Tests for `select_vscode_servers_to_prune`."""

    def _make_server_dirs(self, tmp_path, names):
        d = tmp_path / "cli" / "servers"
        d.mkdir(parents=True)
        for name in names:
            (d / name).mkdir()
        return d

    def test_missing_dir_returns_nothing(self, tmp_path):
        to_remove, warnings = select_vscode_servers_to_prune(tmp_path / "nope")
        assert to_remove == []
        assert warnings == []

    def test_missing_lru_json_skips_section(self, tmp_path):
        d = self._make_server_dirs(tmp_path, ["Stable-a", "Stable-b", "Stable-c"])
        to_remove, warnings = select_vscode_servers_to_prune(d, is_running=lambda _name: False)
        assert to_remove == []
        assert len(warnings) == 1
        assert "lru.json" in warnings[0]

    def test_empty_lru_json_skips_section(self, tmp_path):
        d = self._make_server_dirs(tmp_path, ["Stable-a", "Stable-b"])
        (d / "lru.json").write_text("[]")
        to_remove, warnings = select_vscode_servers_to_prune(d, is_running=lambda _name: False)
        assert to_remove == []
        assert len(warnings) == 1

    def test_corrupt_lru_json_skips_section(self, tmp_path):
        d = self._make_server_dirs(tmp_path, ["Stable-a", "Stable-b"])
        (d / "lru.json").write_text("{not valid json")
        to_remove, warnings = select_vscode_servers_to_prune(d, is_running=lambda _name: False)
        assert to_remove == []
        assert len(warnings) == 1

    def test_keeps_top_n_from_lru_order(self, tmp_path):
        d = self._make_server_dirs(tmp_path, ["Stable-a", "Stable-b", "Stable-c"])
        (d / "lru.json").write_text(json.dumps(["Stable-a", "Stable-b", "Stable-c"]))
        to_remove, warnings = select_vscode_servers_to_prune(
            d, keep_count=2, is_running=lambda _name: False
        )
        assert warnings == []
        assert [p.name for p in to_remove] == ["Stable-c"]

    def test_running_build_outside_keep_n_is_preserved(self, tmp_path):
        d = self._make_server_dirs(tmp_path, ["Stable-a", "Stable-b", "Stable-c"])
        (d / "lru.json").write_text(json.dumps(["Stable-a", "Stable-b", "Stable-c"]))
        to_remove, warnings = select_vscode_servers_to_prune(
            d, keep_count=2, is_running=lambda name: name == "Stable-c"
        )
        assert warnings == []
        assert to_remove == []


class TestSelectExtensionsToPrune:
    """Tests for `select_extensions_to_prune`."""

    def _touch(self, path: Path, mtime_offset: float = 0) -> None:
        path.mkdir()
        if mtime_offset:
            t = time.time() + mtime_offset
            os.utime(path, (t, t))

    def test_missing_dir_returns_nothing(self, tmp_path):
        assert select_extensions_to_prune(tmp_path / "nope") == []

    def test_single_version_kept(self, tmp_path):
        self._touch(tmp_path / "anthropic.claude-code-2.1.204-linux-x64")
        assert select_extensions_to_prune(tmp_path) == []

    def test_older_version_pruned(self, tmp_path):
        old = tmp_path / "anthropic.claude-code-2.1.100-linux-x64"
        new = tmp_path / "anthropic.claude-code-2.1.204-linux-x64"
        self._touch(old)
        self._touch(new)
        result = select_extensions_to_prune(tmp_path)
        assert result == [old]

    def test_version_comparison_not_lexical(self, tmp_path):
        """2.9.0 must be kept over 2.10.0 by *numeric*, not lexical, comparison."""
        older = tmp_path / "pub.ext-2.9.0"
        newer = tmp_path / "pub.ext-2.10.0"
        self._touch(older)
        self._touch(newer)
        result = select_extensions_to_prune(tmp_path)
        assert result == [older]

    def test_different_extensions_independent(self, tmp_path):
        self._touch(tmp_path / "pub.one-1.0.0")
        self._touch(tmp_path / "pub.two-1.0.0")
        assert select_extensions_to_prune(tmp_path) == []

    def test_malformed_dir_name_skipped(self, tmp_path):
        self._touch(tmp_path / "not-a-version-dir")
        assert select_extensions_to_prune(tmp_path) == []


class TestSelectAgedPaths:
    """Tests for `select_aged_paths`."""

    def _touch_aged(self, path: Path, age_days: float) -> None:
        path.mkdir()
        t = time.time() - age_days * 86400
        os.utime(path, (t, t))

    def test_missing_dir_returns_nothing(self, tmp_path):
        assert select_aged_paths(tmp_path / "nope", max_age_days=30) == []

    def test_old_entry_selected(self, tmp_path):
        old = tmp_path / "old"
        self._touch_aged(old, age_days=45)
        assert select_aged_paths(tmp_path, max_age_days=30) == [old]

    def test_recent_entry_not_selected(self, tmp_path):
        recent = tmp_path / "recent"
        self._touch_aged(recent, age_days=1)
        assert select_aged_paths(tmp_path, max_age_days=30) == []

    def test_deterministic_now_injection(self, tmp_path):
        """An explicit `now` makes age comparisons reproducible in tests."""
        entry = tmp_path / "entry"
        entry.mkdir()
        # entry.mtime is "now" at creation time; five days later it's stale
        # for a 3-day threshold, evaluated against an injected reference time.
        future = datetime.now(UTC) + timedelta(days=5)
        assert select_aged_paths(tmp_path, max_age_days=3, now=future) == [entry]


class TestFindOrphanedWorktrees:
    """Tests for `find_orphaned_worktrees`."""

    def _make_worktree_dir(self, base: Path, name: str, age_minutes: float = 60) -> Path:
        d = base / name
        d.mkdir(parents=True)
        t = time.time() - age_minutes * 60
        os.utime(d, (t, t))
        return d

    def test_orphaned_old_dir_is_selected(self, tmp_path):
        base = tmp_path / ".worktrees"
        orphan = self._make_worktree_dir(base, "stale-branch")
        with patch("wslcb_licensing_tracker.disk_hygiene._parse_worktree_list", return_value=set()):
            result = find_orphaned_worktrees(tmp_path, [base])
        assert result == [orphan]

    def test_registered_dir_is_kept(self, tmp_path):
        base = tmp_path / ".worktrees"
        registered = self._make_worktree_dir(base, "active-branch")
        with patch(
            "wslcb_licensing_tracker.disk_hygiene._parse_worktree_list",
            return_value={registered},
        ):
            result = find_orphaned_worktrees(tmp_path, [base])
        assert result == []

    def test_dir_with_spaces_in_name_matched_correctly(self, tmp_path):
        base = tmp_path / ".worktrees"
        registered = self._make_worktree_dir(base, "branch with spaces")
        with patch(
            "wslcb_licensing_tracker.disk_hygiene._parse_worktree_list",
            return_value={registered},
        ):
            result = find_orphaned_worktrees(tmp_path, [base])
        assert result == []

    def test_young_orphan_kept_within_grace_window(self, tmp_path):
        base = tmp_path / ".worktrees"
        young_orphan = self._make_worktree_dir(base, "brand-new", age_minutes=5)
        with patch("wslcb_licensing_tracker.disk_hygiene._parse_worktree_list", return_value=set()):
            result = find_orphaned_worktrees(tmp_path, [base], grace_minutes=30)
        assert young_orphan not in result

    def test_old_orphan_removed_past_grace_window(self, tmp_path):
        base = tmp_path / ".worktrees"
        old_orphan = self._make_worktree_dir(base, "old-orphan", age_minutes=60)
        with patch("wslcb_licensing_tracker.disk_hygiene._parse_worktree_list", return_value=set()):
            result = find_orphaned_worktrees(tmp_path, [base], grace_minutes=30)
        assert old_orphan in result

    def test_missing_base_dir_skipped(self, tmp_path):
        with patch("wslcb_licensing_tracker.disk_hygiene._parse_worktree_list", return_value=set()):
            result = find_orphaned_worktrees(tmp_path, [tmp_path / "nope"])
        assert result == []


class TestRemovePath:
    """Tests for `_remove_path` — fail-open removal."""

    def test_nonexistent_path_returns_zero(self, tmp_path):
        assert _remove_path(tmp_path / "nope", dry_run=False) == 0

    def test_dry_run_does_not_delete(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("hello")
        freed = _remove_path(f, dry_run=True)
        assert freed == 0
        assert f.exists()

    def test_removes_file_and_returns_size(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("hello world")
        size = f.stat().st_size
        freed = _remove_path(f, dry_run=False)
        assert freed == size
        assert not f.exists()

    def test_removes_dir_and_returns_total_size(self, tmp_path):
        d = tmp_path / "dir"
        d.mkdir()
        (d / "a.txt").write_text("aaaa")
        (d / "b.txt").write_text("bb")
        freed = _remove_path(d, dry_run=False)
        assert freed == 6
        assert not d.exists()

    def test_removal_failure_logs_warning_and_returns_zero(self, tmp_path, caplog):
        f = tmp_path / "file.txt"
        f.write_text("hello")
        with patch("pathlib.Path.unlink", side_effect=OSError("permission denied")):
            with caplog.at_level("WARNING"):
                freed = _remove_path(f, dry_run=False)
        assert freed == 0
        assert any("permission denied" in r.message for r in caplog.records)


class TestRunSubprocessStep:
    """Tests for `_run_subprocess_step` — fail-open shell-outs."""

    def test_dry_run_does_not_execute(self):
        with patch("subprocess.run") as mock_run:
            _run_subprocess_step(["true"], dry_run=True, label="test step")
        mock_run.assert_not_called()

    def test_success_runs_command(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(["true"], 0)
            _run_subprocess_step(["true"], dry_run=False, label="test step")
        mock_run.assert_called_once()

    def test_failure_logs_warning_and_does_not_raise(self, caplog):
        with patch(
            "subprocess.run",
            side_effect=subprocess.CalledProcessError(1, ["false"]),
        ):
            with caplog.at_level("WARNING"):
                _run_subprocess_step(["false"], dry_run=False, label="test step")
        assert any("test step" in r.message for r in caplog.records)


class TestCheckUsageThreshold:
    """Tests for `check_usage_threshold`."""

    def test_below_threshold_no_warning(self, caplog):
        with patch(
            "shutil.disk_usage",
            return_value=type("Usage", (), {"total": 100, "used": 50, "free": 50})(),
        ):
            with caplog.at_level("WARNING"):
                check_usage_threshold(threshold_pct=75)
        assert caplog.records == []

    def test_at_threshold_warns(self, caplog):
        with patch(
            "shutil.disk_usage",
            return_value=type("Usage", (), {"total": 100, "used": 80, "free": 20})(),
        ):
            with caplog.at_level("WARNING"):
                check_usage_threshold(threshold_pct=75)
        assert any("75" in r.message or "80" in r.message for r in caplog.records)


class TestCompressDataStragglers:
    """Tests for `compress_data_stragglers` — reuses cli._compress_files, no new logic."""

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


class TestRunDiskHygiene:
    """Tests for the `run_disk_hygiene` orchestrator."""

    def _fake_env(self, tmp_path):
        home = tmp_path / "home"
        repo_root = tmp_path / "repo"
        (home / ".vscode-server" / "cli" / "servers").mkdir(parents=True)
        (repo_root / ".worktrees").mkdir(parents=True)
        (repo_root / "data").mkdir(parents=True)
        return home, repo_root

    def test_aggregates_freed_bytes_across_sections(self, tmp_path):
        home, repo_root = self._fake_env(tmp_path)

        # A stale extension version to prune.
        ext_dir = home / ".vscode-server" / "extensions"
        ext_dir.mkdir(parents=True)
        old_ext = ext_dir / "pub.ext-1.0.0"
        old_ext.mkdir()
        (old_ext / "payload.bin").write_bytes(b"x" * 1000)
        (ext_dir / "pub.ext-2.0.0").mkdir()

        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", repo_root / "data"),
        ):
            result = run_disk_hygiene(dry_run=False, home=home, repo_root=repo_root)

        assert result["freed_bytes"] >= 1000
        assert not old_ext.exists()

    def test_dry_run_removes_nothing(self, tmp_path):
        home, repo_root = self._fake_env(tmp_path)
        ext_dir = home / ".vscode-server" / "extensions"
        ext_dir.mkdir(parents=True)
        old_ext = ext_dir / "pub.ext-1.0.0"
        old_ext.mkdir()
        (ext_dir / "pub.ext-2.0.0").mkdir()

        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", repo_root / "data"),
        ):
            result = run_disk_hygiene(dry_run=True, home=home, repo_root=repo_root)

        assert result["freed_bytes"] == 0
        assert old_ext.exists()

    def test_vscode_server_warnings_propagate(self, tmp_path):
        home, repo_root = self._fake_env(tmp_path)
        # No lru.json written -> server-prune section should warn.
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", repo_root / "data"),
        ):
            result = run_disk_hygiene(dry_run=True, home=home, repo_root=repo_root)

        assert any("lru.json" in w for w in result["warnings"])

    def test_includes_compress_result(self, tmp_path):
        home, repo_root = self._fake_env(tmp_path)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", repo_root / "data"),
        ):
            result = run_disk_hygiene(dry_run=True, home=home, repo_root=repo_root)

        assert "compress" in result
        assert result["compress"]["snapshots"] is None
        assert result["compress"]["diffs"] is None
