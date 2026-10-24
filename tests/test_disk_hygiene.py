"""Tests for disk_hygiene.py — pure selection logic + fail-open removal helpers."""

import json
import os
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from wslcb_licensing_tracker.disk_hygiene import (
    NPX_GRACE_MINUTES,
    _parse_worktree_list,
    _prune_dev_caches,
    _remove_path,
    _run_subprocess_step,
    check_usage_threshold,
    compress_data_stragglers,
    find_orphaned_worktrees,
    in_use,
    launch_specs,
    live_paths,
    npx_dir_name,
    pinned_specs,
    run_disk_hygiene,
    select_aged_paths,
    select_extensions_to_prune,
    select_npx_to_prune,
    select_vscode_servers_to_prune,
    withhold_reason,
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

    def test_running_older_version_is_preserved(self, tmp_path):
        """A process actively running from an older version dir must not be pruned.

        Regression: a real dry-run on this VM found the running Claude Code
        session's own extension version (2.1.204, superseded on disk by
        2.1.206 but not yet reloaded) flagged for removal.
        """
        old = tmp_path / "pub.ext-1.0.0"
        new = tmp_path / "pub.ext-2.0.0"
        self._touch(old)
        self._touch(new)
        result = select_extensions_to_prune(tmp_path, is_running=lambda path: path == old)
        assert result == []


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

    def _patch_registered(self, registered):
        return patch(
            "wslcb_licensing_tracker.disk_hygiene._parse_worktree_list",
            return_value=(registered, None),
        )

    def test_orphaned_old_dir_is_selected(self, tmp_path):
        base = tmp_path / ".worktrees"
        orphan = self._make_worktree_dir(base, "stale-branch")
        with self._patch_registered(set()):
            result, warnings = find_orphaned_worktrees(tmp_path, [base])
        assert result == [orphan]
        assert warnings == []

    def test_registered_dir_is_kept(self, tmp_path):
        base = tmp_path / ".worktrees"
        registered = self._make_worktree_dir(base, "active-branch")
        with self._patch_registered({registered}):
            result, warnings = find_orphaned_worktrees(tmp_path, [base])
        assert result == []
        assert warnings == []

    def test_dir_with_spaces_in_name_matched_correctly(self, tmp_path):
        base = tmp_path / ".worktrees"
        registered = self._make_worktree_dir(base, "branch with spaces")
        with self._patch_registered({registered}):
            result, warnings = find_orphaned_worktrees(tmp_path, [base])
        assert result == []
        assert warnings == []

    def test_young_orphan_kept_within_grace_window(self, tmp_path):
        base = tmp_path / ".worktrees"
        young_orphan = self._make_worktree_dir(base, "brand-new", age_minutes=5)
        with self._patch_registered(set()):
            result, warnings = find_orphaned_worktrees(tmp_path, [base], grace_minutes=30)
        assert young_orphan not in result
        assert warnings == []

    def test_old_orphan_removed_past_grace_window(self, tmp_path):
        base = tmp_path / ".worktrees"
        old_orphan = self._make_worktree_dir(base, "old-orphan", age_minutes=60)
        with self._patch_registered(set()):
            result, warnings = find_orphaned_worktrees(tmp_path, [base], grace_minutes=30)
        assert old_orphan in result
        assert warnings == []

    def test_missing_base_dir_skipped(self, tmp_path):
        with self._patch_registered(set()):
            result, warnings = find_orphaned_worktrees(tmp_path, [tmp_path / "nope"])
        assert result == []
        assert warnings == []

    def test_git_failure_skips_sweep_entirely(self, tmp_path):
        """A failed `git worktree list` must not be treated as 'nothing registered'.

        Regression: silently defaulting to an empty registered set on git
        failure would make every subdirectory look orphaned and delete
        active worktrees, including ones with uncommitted work.
        """
        base = tmp_path / ".worktrees"
        self._make_worktree_dir(base, "should-survive-git-failure")
        with patch(
            "wslcb_licensing_tracker.disk_hygiene._parse_worktree_list",
            return_value=(set(), "git worktree list failed (exit 128) — skipping orphan sweep"),
        ):
            result, warnings = find_orphaned_worktrees(tmp_path, [base])
        assert result == []
        assert len(warnings) == 1
        assert "git worktree list" in warnings[0]


class TestParseWorktreeList:
    """Tests for `_parse_worktree_list` — the git-failure guard itself."""

    def test_non_git_dir_returns_warning_not_empty_success(self, tmp_path):
        """Running against a directory that isn't a git repo must signal failure."""
        registered, warning = _parse_worktree_list(tmp_path)
        assert registered == set()
        assert warning is not None
        assert "git worktree list" in warning

    def test_real_repo_returns_registered_paths_no_warning(self):
        """Sanity check against this actual repo — should succeed with no warning."""
        repo_root = Path(__file__).resolve().parents[1]
        registered, warning = _parse_worktree_list(repo_root)
        assert warning is None
        assert len(registered) >= 1


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


# --- _npx pin / liveness / grace (#217) -------------------------------------------
# Ported from CannObserv/power-map tests/scripts/test_prune_disk.py @ 2addf1d.
# Unlike power-map, the 30-day age gate stays: an _npx entry goes only when it
# is old AND unpinned AND not in use AND settled past the grace window.

PINNED_SPEC = "socraticode@1.14.0"
PINNED_HASH = "2d2576fc184c3d4d"  # measured on this host, 2026-10-24


def _fake_proc(root: Path, pid: str, *, links=None, fds=(), maps="") -> None:
    """A fake ``/proc/<pid>``; *links* maps ``exe``/``cwd`` to their targets."""
    p = root / pid
    (p / "fd").mkdir(parents=True)
    for name, target in (links or {}).items():
        (p / name).symlink_to(target)
    for i, target in enumerate(fds):
        (p / "fd" / str(i)).symlink_to(target)
    (p / "maps").write_text(maps)


def _tree(root: Path) -> Path:
    (root / "a" / "b" / "c").mkdir(parents=True)
    (root / "a" / "b" / "c" / "node").write_bytes(b"x")
    return root


def _settled() -> float:
    """A cutoff after everything made so far: the whole tree reads as settled."""
    return time.time() + 60


def _age(path: Path, days: int) -> None:
    ts = (datetime.now(UTC) - timedelta(days=days)).timestamp()
    os.utime(path, (ts, ts))


def _after_grace() -> datetime:
    """A ``now`` far enough ahead that every ctime made so far is past the grace window."""
    return datetime.now(UTC) + timedelta(hours=2)


class TestLivePaths:
    """`live_paths` / `in_use` read /proc exe, cwd, fds and maps — never argv."""

    def test_reads_exe_cwd_fds_and_maps(self, tmp_path):
        proc = tmp_path / "proc"
        _fake_proc(
            proc,
            "101",
            links={"exe": "/srv/a/node", "cwd": "/srv/b"},
            fds=["/srv/c/log", "socket:[123]"],
            maps=(
                "7f00-7f01 r-xp 00000000 08:01 42   /srv/d/lib.so\n"
                "7f01-7f02 rw-p 00000000 00:00 0 \n"
                "7f02-7f03 r--p 00000000 08:01 43   /srv/e/file with space (deleted)\n"
                "7ffd-7ffe rw-p 00000000 00:00 0    [stack]\n"
            ),
        )
        (proc / "self").mkdir()  # non-numeric entries are skipped
        live = live_paths(proc)
        assert {"/srv/a/node", "/srv/b", "/srv/c/log", "/srv/d/lib.so"} <= live
        assert "/srv/e/file with space" in live
        assert not any(p.startswith(("socket:", "[")) for p in live)

    def test_skips_a_process_it_cannot_read(self, tmp_path):
        proc = tmp_path / "proc"
        (proc / "202").mkdir(parents=True)  # no exe, cwd, fd or maps: a root process
        _fake_proc(proc, "303", links={"exe": "/srv/ok"})
        assert live_paths(proc) == {"/srv/ok"}

    def test_in_use_matches_the_path_and_anything_under_it(self):
        live = {f"/home/x/.npm/_npx/{PINNED_HASH}/node_modules/x/addon.node"}
        assert in_use(Path(f"/home/x/.npm/_npx/{PINNED_HASH}"), live)
        assert not in_use(Path(f"/home/x/.npm/_npx/{PINNED_HASH[:8]}"), live)
        assert in_use(Path("/srv/a"), {"/srv/a"})


class TestNpxSpecs:
    """`npx_dir_name`, `pinned_specs`, `launch_specs`."""

    def test_npx_dir_name_is_npms_hash_of_the_spec(self):
        assert npx_dir_name(PINNED_SPEC) == PINNED_HASH

    def test_pinned_specs_reads_the_repo_settings(self, tmp_path):
        settings = tmp_path / "settings.json"
        settings.write_text(json.dumps({"env": {"SOCRATICODE_SPEC": PINNED_SPEC}}))
        assert pinned_specs(settings) == {PINNED_SPEC}

    def test_pinned_specs_tolerates_a_missing_or_bad_file(self, tmp_path):
        assert pinned_specs(tmp_path / "missing.json") == set()
        bad = tmp_path / "bad.json"
        bad.write_text("[")
        assert pinned_specs(bad) == set()

    def test_launch_specs_reads_the_vscode_machine_setting(self, tmp_path):
        env = [{"name": "OTHER", "value": "x"}, {"name": "SOCRATICODE_SPEC", "value": "s@9"}]
        path = tmp_path / "settings.json"
        path.write_text(json.dumps({"claudeCode.environmentVariables": env}))
        assert launch_specs(path) == {"s@9"}

    def test_launch_specs_tolerates_malformed_or_missing(self, tmp_path):
        path = tmp_path / "settings.json"
        for body in [
            "{",
            "[]",
            "{}",
            '{"claudeCode.environmentVariables": {"SOCRATICODE_SPEC": "x"}}',
        ]:
            path.write_text(body)
            assert launch_specs(path) == set(), body
        assert launch_specs(tmp_path / "missing.json") == set()

    def test_the_repo_pins_a_socraticode_spec(self):
        """The pin only protects the live server while the repo states its spec."""
        repo_settings = Path(__file__).resolve().parents[1] / ".claude" / "settings.json"
        assert {s for s in pinned_specs(repo_settings) if s.startswith("socraticode@")}


class TestWithholdReason:
    """`withhold_reason` — the grace window over the deep max(mtime, ctime)."""

    def test_the_default_grace_is_disk_gcs_hour(self):
        assert NPX_GRACE_MINUTES == 60

    def test_a_settled_tree_is_not_withheld(self, tmp_path):
        assert withhold_reason(_tree(tmp_path / "t"), _settled()) is None

    def test_an_extracted_tree_with_archive_mtimes_is_recently_touched(self, tmp_path):
        root = _tree(tmp_path / "t")
        for dirpath, dirnames, filenames in os.walk(root, topdown=False):
            for name in filenames + dirnames:
                os.utime(os.path.join(dirpath, name), (1, 1))
            os.utime(dirpath, (1, 1))
        assert withhold_reason(root, time.time() - 3600) == "recently touched"

    def test_a_change_deep_in_a_settled_tree_withholds_it(self, tmp_path):
        root = _tree(tmp_path / "t")
        cutoff = _settled()
        os.utime(root / "a" / "b" / "c", (cutoff + 3600, cutoff + 3600))
        assert withhold_reason(root, cutoff) == "recently touched"

    def test_an_unreadable_path_is_withheld(self, tmp_path):
        assert withhold_reason(tmp_path / "missing", _settled()).startswith("unreadable")

    def test_a_tree_past_the_walk_bound_is_withheld(self, tmp_path):
        reason = withhold_reason(_tree(tmp_path / "t"), _settled(), bound=2)
        assert reason is not None
        assert "bound" in reason


class TestSelectNpxToPrune:
    """`select_npx_to_prune` — age gate, then pin, liveness and grace."""

    def _npx(self, tmp_path, names):
        npx = tmp_path / "_npx"
        for name in names:
            (npx / name / "node_modules").mkdir(parents=True)
        return npx

    def test_missing_dir_returns_nothing(self, tmp_path):
        assert select_npx_to_prune(tmp_path / "nope", 30, set(), set()) == ([], [])

    def test_pinned_hash_survives_at_any_age(self, tmp_path):
        npx = self._npx(tmp_path, [PINNED_HASH])
        _age(npx / PINNED_HASH, 3650)
        remove, kept = select_npx_to_prune(npx, 30, {PINNED_SPEC}, set(), now=_after_grace())
        assert remove == []
        assert kept == [(npx / PINNED_HASH, f"pinned ({PINNED_SPEC})")]

    def test_unpinned_old_entry_is_pruned(self, tmp_path):
        npx = self._npx(tmp_path, [PINNED_HASH, "dead000000000000"])
        _age(npx / "dead000000000000", 40)
        remove, _ = select_npx_to_prune(npx, 30, {PINNED_SPEC}, set(), now=_after_grace())
        assert remove == [npx / "dead000000000000"]

    def test_unpinned_young_entry_is_kept(self, tmp_path):
        npx = self._npx(tmp_path, ["young00000000000"])
        _age(npx / "young00000000000", 5)
        remove, kept = select_npx_to_prune(npx, 30, set(), set(), now=_after_grace())
        assert remove == []
        assert kept == [(npx / "young00000000000", "younger than 30d")]

    def test_old_entry_in_use_is_kept(self, tmp_path):
        npx = self._npx(tmp_path, ["live000000000000"])
        _age(npx / "live000000000000", 40)
        live = {str(npx / "live000000000000" / "node_modules" / "x.node")}
        remove, kept = select_npx_to_prune(npx, 30, set(), live, now=_after_grace())
        assert remove == []
        assert kept == [(npx / "live000000000000", "in use")]

    def test_old_entry_touched_within_grace_is_kept(self, tmp_path):
        """mtime 40 days old, but ctime (which can't be backdated) is minutes old."""
        npx = self._npx(tmp_path, ["fresh00000000000"])
        _age(npx / "fresh00000000000", 40)
        remove, kept = select_npx_to_prune(npx, 30, set(), set())
        assert remove == []
        assert kept == [(npx / "fresh00000000000", "recently touched")]

    def test_grace_zero_disables_the_window(self, tmp_path):
        npx = self._npx(tmp_path, ["fresh00000000000"])
        _age(npx / "fresh00000000000", 40)
        remove, _ = select_npx_to_prune(npx, 30, set(), set(), grace_minutes=0)
        assert remove == [npx / "fresh00000000000"]

    def test_symlinks_and_files_are_left_alone(self, tmp_path):
        npx = self._npx(tmp_path, ["real000000000000"])
        (npx / "link").symlink_to(npx / "real000000000000")
        (npx / "stray").write_text("x")
        for p in (npx / "real000000000000", npx / "stray"):
            _age(p, 40)
        remove, kept = select_npx_to_prune(npx, 30, set(), set(), now=_after_grace())
        assert remove == [npx / "real000000000000"]
        assert kept == []


class TestPruneDevCaches:
    """`_prune_dev_caches` — wiring of the _npx rules and the cache shell-outs."""

    def _env(self, tmp_path, *, repo_spec=None, machine_spec=None):
        home = tmp_path / "home"
        repo_root = tmp_path / "repo"
        (home / ".npm" / "_npx").mkdir(parents=True)
        (repo_root / ".claude").mkdir(parents=True)
        if repo_spec:
            (repo_root / ".claude" / "settings.json").write_text(
                json.dumps({"env": {"SOCRATICODE_SPEC": repo_spec}})
            )
        if machine_spec:
            machine = home / ".vscode-server" / "data" / "Machine" / "settings.json"
            machine.parent.mkdir(parents=True)
            env = [{"name": "SOCRATICODE_SPEC", "value": machine_spec}]
            machine.write_text(json.dumps({"claudeCode.environmentVariables": env}))
        return home, repo_root

    def _npx_entry(self, home, name, days):
        path = home / ".npm" / "_npx" / name
        (path / "node_modules").mkdir(parents=True)
        _age(path, days)
        return path

    def test_uv_cache_prune_is_forced(self, tmp_path):
        home, repo_root = self._env(tmp_path)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step") as step,
            patch("shutil.which", return_value="/usr/bin/x"),
        ):
            _prune_dev_caches(home, repo_root, dry_run=False)
        cmds = [c.args[0] for c in step.call_args_list]
        assert ["uv", "cache", "prune", "--force"] in cmds

    def test_both_pins_are_kept_and_logged_on_dry_run(self, tmp_path, caplog):
        home, repo_root = self._env(tmp_path, repo_spec=PINNED_SPEC, machine_spec="s@2")
        declared = self._npx_entry(home, PINNED_HASH, 365)
        launched = self._npx_entry(home, npx_dir_name("s@2"), 365)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.live_paths", return_value=set()),
            caplog.at_level("INFO"),
        ):
            freed, warnings = _prune_dev_caches(home, repo_root, dry_run=True)
        assert (freed, warnings) == (0, [])
        msgs = [r.getMessage() for r in caplog.records]
        assert any(str(declared) in m and f"pinned ({PINNED_SPEC})" in m for m in msgs)
        assert any(str(launched) in m and "pinned (s@2)" in m for m in msgs)

    def test_removes_unpinned_old_entry_and_keeps_pinned(self, tmp_path):
        home, repo_root = self._env(tmp_path, repo_spec=PINNED_SPEC)
        pinned = self._npx_entry(home, PINNED_HASH, 365)
        dead = self._npx_entry(home, "dead000000000000", 365)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.live_paths", return_value=set()),
            patch("wslcb_licensing_tracker.disk_hygiene.NPX_GRACE_MINUTES", 0),
        ):
            _prune_dev_caches(home, repo_root, dry_run=False)
        assert pinned.exists()
        assert not dead.exists()

    def test_unchecked_entry_becomes_a_warning(self, tmp_path, caplog):
        home, repo_root = self._env(tmp_path)
        locked = self._npx_entry(home, "lock000000000000", 365)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.live_paths", return_value=set()),
            patch(
                "wslcb_licensing_tracker.disk_hygiene.withhold_reason",
                return_value="unreadable (Permission denied)",
            ),
            caplog.at_level("WARNING"),
        ):
            _, warnings = _prune_dev_caches(home, repo_root, dry_run=False)
        assert locked.exists()
        assert any(str(locked) in w and "unreadable" in w for w in warnings)
        assert any(
            r.levelname == "WARNING" and str(locked) in r.getMessage() for r in caplog.records
        )

    def test_npx_section_failure_does_not_abort_the_run(self, tmp_path, caplog):
        """CR 1: an OSError in the _npx step is a warning; the rest of the run continues."""
        home, repo_root = self._env(tmp_path)
        dead = self._npx_entry(home, "dead000000000000", 365)
        (repo_root / ".worktrees").mkdir()
        (repo_root / "data").mkdir()
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step") as step,
            patch("wslcb_licensing_tracker.disk_hygiene.live_paths", side_effect=OSError("boom")),
            patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", repo_root / "data"),
            patch("shutil.which", return_value="/usr/bin/x"),
            caplog.at_level("WARNING"),
        ):
            result = run_disk_hygiene(dry_run=False, home=home, repo_root=repo_root)
        assert dead.exists()
        assert any("_npx" in w and "boom" in w for w in result["warnings"])
        assert step.called  # cache shell-outs still ran

    def test_no_pinned_spec_with_npx_entries_warns(self, tmp_path, caplog):
        """CR 2: with no SOCRATICODE_SPEC found, the pin is off — say so loudly."""
        home, repo_root = self._env(tmp_path)
        self._npx_entry(home, PINNED_HASH, 1)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.live_paths", return_value=set()),
            caplog.at_level("WARNING"),
        ):
            _, warnings = _prune_dev_caches(home, repo_root, dry_run=True)
        assert any("SOCRATICODE_SPEC" in w for w in warnings)
        assert any(r.levelname == "WARNING" for r in caplog.records)

    def test_no_pinned_spec_with_empty_npx_is_quiet(self, tmp_path):
        home, repo_root = self._env(tmp_path)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.live_paths", return_value=set()),
        ):
            _, warnings = _prune_dev_caches(home, repo_root, dry_run=True)
        assert warnings == []

    def test_an_unknown_withhold_reason_is_a_warning(self, tmp_path):
        """CR 3: as power-map, anything but a known rule firing is loud, not quiet."""
        home, repo_root = self._env(tmp_path, repo_spec=PINNED_SPEC)
        odd = self._npx_entry(home, "odd0000000000000", 365)
        with (
            patch("wslcb_licensing_tracker.disk_hygiene._run_subprocess_step"),
            patch("wslcb_licensing_tracker.disk_hygiene.live_paths", return_value=set()),
            patch(
                "wslcb_licensing_tracker.disk_hygiene.withhold_reason",
                return_value="some future reason",
            ),
        ):
            _, warnings = _prune_dev_caches(home, repo_root, dry_run=True)
        assert any(str(odd) in w and "some future reason" in w for w in warnings)
