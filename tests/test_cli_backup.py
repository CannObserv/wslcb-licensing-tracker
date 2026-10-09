"""Tests for `wslcb ops backup | restore | restore-archive` (#185)."""

from unittest.mock import patch

import pytest
from click.testing import CliRunner
from gcs_fakes import FakeBucket, FakeClient
from test_backup import FakePg
from test_restore import BUCKET, HOST, T0, _dump

from wslcb_licensing_tracker.cli import main


@pytest.fixture
def dumps():
    return FakeBucket(BUCKET)


@pytest.fixture
def client(dumps):
    archive = FakeBucket("co-gcs-wslcb-archive")
    archive.put(f"{HOST}/remediation-backups/x.json", b"x")
    return FakeClient(dumps, archive)


@pytest.fixture(autouse=True)
def _env(monkeypatch, client):
    monkeypatch.setenv("WSLCB_BACKUP_BUCKET", BUCKET)
    monkeypatch.setenv("WSLCB_ARCHIVE_BUCKET", "co-gcs-wslcb-archive")
    monkeypatch.setattr("wslcb_licensing_tracker.restore.make_client", lambda: client)
    monkeypatch.setattr("wslcb_licensing_tracker.cli.subprocess.run", FakePg())


class TestBackup:
    def test_exit_code_is_the_jobs(self):
        with patch("wslcb_licensing_tracker.cli.run_backup_job", return_value=1) as job:
            result = CliRunner().invoke(main, ["ops", "backup"])
        assert result.exit_code == 1
        assert job.call_args.kwargs["database"] == "wslcb"


class TestRestore:
    def test_list(self, dumps):
        key = _dump(dumps, "daily", HOST, T0)
        result = CliRunner().invoke(main, ["ops", "restore", "--list"])
        assert result.exit_code == 0, result.output
        assert key in result.output
        assert f"source_host={HOST}" in result.output

    def test_latest_requires_prefix(self):
        """Never this host's by default: say whose."""
        result = CliRunner().invoke(main, ["ops", "restore", "--latest", "--into", "x"])
        assert result.exit_code == 2
        assert "--prefix" in result.output

    def test_requires_a_destination(self, dumps):
        _dump(dumps, "daily", HOST, T0)
        result = CliRunner().invoke(main, ["ops", "restore", "--latest", "--prefix", HOST])
        assert result.exit_code == 2

    def test_requires_exactly_one_selector(self):
        result = CliRunner().invoke(main, ["ops", "restore", "--list", "--latest"])
        assert result.exit_code == 2

    def test_download_only(self, dumps, tmp_path):
        _dump(dumps, "daily", HOST, T0)
        out = tmp_path / "out"
        result = CliRunner().invoke(
            main, ["ops", "restore", "--latest", "--prefix", HOST, "--download-only", str(out)]
        )
        assert result.exit_code == 0, result.output
        assert "verified" in result.output
        assert len(list(out.iterdir())) == 1

    def test_into_restores(self, dumps):
        _dump(dumps, "daily", HOST, T0)
        result = CliRunner().invoke(
            main,
            ["ops", "restore", "--latest", "--prefix", HOST, "--into", "wslcb_drill"],
        )
        assert result.exit_code == 0, result.output
        assert "restored" in result.output

    def test_failure_is_one_sentence_exit_1(self):
        result = CliRunner().invoke(
            main, ["ops", "restore", "--latest", "--prefix", HOST, "--into", "x"]
        )
        assert result.exit_code == 1
        assert "restore failed: no dumps" in result.output
        # Exited deliberately, not by an escaping exception.
        assert isinstance(result.exception, SystemExit)


class TestRestoreArchive:
    def test_downloads(self, tmp_path):
        out = tmp_path / "data"
        result = CliRunner().invoke(
            main, ["ops", "restore-archive", "--prefix", HOST, "--into", str(out)]
        )
        assert result.exit_code == 0, result.output
        assert (out / "remediation-backups/x.json").read_bytes() == b"x"
        assert "1 file" in result.output

    def test_prefix_is_required(self, tmp_path):
        result = CliRunner().invoke(main, ["ops", "restore-archive", "--into", str(tmp_path)])
        assert result.exit_code == 2
