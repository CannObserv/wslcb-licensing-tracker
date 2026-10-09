"""Tests for restore.py — bringing a dump or archive files back (#185)."""

import hashlib
import stat
from datetime import UTC, datetime, timedelta

import pytest
from gcs_fakes import FakeBucket, FakeClient
from test_backup import DUMP_BYTES, FakePg

BUCKET = "co-gcs-wslcb-backup"
ARCHIVE = "co-gcs-wslcb-archive"
HOST = "wslcb-licensing-tracker"
T0 = datetime(2026, 10, 9, 15, 17, 2, tzinfo=UTC)


def _stamp(at):
    return at.strftime("%Y%m%dT%H%M%SZ")


def _dump(  # noqa: PLR0913
    bucket, tier, host, at, data=DUMP_BYTES, *, created=None, sha=None
):
    key = f"{tier}/{host}/{_stamp(at)}.dump"
    bucket.put(
        key,
        data,
        {"sha256": sha or hashlib.sha256(data).hexdigest(), "source_host": host},
        created=created or at + timedelta(seconds=10),
    )
    return key


@pytest.fixture
def dumps():
    return FakeBucket(BUCKET)


@pytest.fixture
def archive():
    return FakeBucket(ARCHIVE)


@pytest.fixture
def client(dumps, archive):
    return FakeClient(dumps, archive)


class TestLatest:
    def test_newest_daily_for_the_named_host(self, client, dumps):
        from wslcb_licensing_tracker.restore import latest_key

        _dump(dumps, "daily", HOST, T0 - timedelta(days=1))
        newest = _dump(dumps, "daily", HOST, T0)
        _dump(dumps, "daily", "other-host", T0 + timedelta(hours=1))
        assert latest_key(client, BUCKET, HOST) == newest

    def test_falls_back_to_monthly_when_dailies_have_aged_out(self, client, dumps):
        """A host dead past the daily window still has its monthlies."""
        from wslcb_licensing_tracker.restore import latest_key

        older = _dump(dumps, "monthly", HOST, T0 - timedelta(days=70))
        newer = _dump(dumps, "monthly", HOST, T0 - timedelta(days=40))
        assert latest_key(client, BUCKET, HOST) == newer != older

    def test_newest_across_tiers_by_time_not_name(self, client, dumps):
        """'monthly/' sorts after 'daily/'; the stamp, not the key, decides."""
        from wslcb_licensing_tracker.restore import latest_key

        _dump(dumps, "monthly", HOST, T0 - timedelta(days=8))
        daily = _dump(dumps, "daily", HOST, T0)
        assert latest_key(client, BUCKET, HOST) == daily

    def test_passes_over_a_name_later_than_its_creation(self, client, dumps):
        """A compromised writer planting 2099… must not own --latest."""
        from wslcb_licensing_tracker.restore import latest_key

        honest = _dump(dumps, "daily", HOST, T0)
        _dump(dumps, "daily", HOST, datetime(2099, 1, 1, tzinfo=UTC), created=T0)
        assert latest_key(client, BUCKET, HOST) == honest

    def test_none_is_an_error(self, client):
        from wslcb_licensing_tracker.restore import RestoreError, latest_key

        with pytest.raises(RestoreError, match=r"no dumps"):
            latest_key(client, BUCKET, HOST)


class TestFetch:
    def test_verifies_and_writes_private(self, client, dumps, tmp_path):
        from wslcb_licensing_tracker.restore import fetch

        key = _dump(dumps, "daily", HOST, T0)
        path = fetch(client, BUCKET, key, tmp_path / "out", runner=FakePg())
        assert path.read_bytes() == DUMP_BYTES
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert stat.S_IMODE((tmp_path / "out").stat().st_mode) == 0o700

    def test_sha_mismatch_is_refused_and_removed(self, client, dumps, tmp_path):
        from wslcb_licensing_tracker.restore import RestoreError, fetch

        key = _dump(dumps, "daily", HOST, T0, sha="0" * 64)
        with pytest.raises(RestoreError, match=r"does not match"):
            fetch(client, BUCKET, key, tmp_path / "out", runner=FakePg())
        assert list((tmp_path / "out").iterdir()) == []

    def test_wrong_database_is_refused(self, client, dumps, tmp_path):
        from test_backup import TOC

        from wslcb_licensing_tracker.restore import RestoreError, fetch

        key = _dump(dumps, "daily", HOST, T0)
        toc = TOC.replace("TABLE DATA public sources", "TABLE DATA public nope")
        with pytest.raises(RestoreError, match=r"public\.sources"):
            fetch(client, BUCKET, key, tmp_path / "out", runner=FakePg(toc=toc))

    def test_refuses_an_open_directory(self, client, dumps, tmp_path):
        from wslcb_licensing_tracker.restore import RestoreError, fetch

        key = _dump(dumps, "daily", HOST, T0)
        out = tmp_path / "out"
        out.mkdir(mode=0o755)
        out.chmod(0o755)
        with pytest.raises(RestoreError, match=r"not private"):
            fetch(client, BUCKET, key, out, runner=FakePg())

    def test_never_writes_over_an_existing_file(self, client, dumps, tmp_path):
        from wslcb_licensing_tracker.restore import RestoreError, fetch

        key = _dump(dumps, "daily", HOST, T0)
        out = tmp_path / "out"
        out.mkdir(mode=0o700)
        (out / f"{_stamp(T0)}.dump").write_bytes(b"keep me")
        with pytest.raises(RestoreError, match=r"already exists"):
            fetch(client, BUCKET, key, out, runner=FakePg())
        assert (out / f"{_stamp(T0)}.dump").read_bytes() == b"keep me"


class TestRestoreInto:
    def test_single_transaction_exit_on_error_as_user(self, tmp_path):
        from wslcb_licensing_tracker.restore import restore_into

        path = tmp_path / "x.dump"
        path.write_bytes(DUMP_BYTES)
        pg = FakePg()
        restore_into(path, "wslcb_drill", run_as="postgres", runner=pg)
        argv = pg.calls[0]
        assert argv[:2] == ["setpriv", "--reuid=postgres"]
        assert "--single-transaction" in argv
        assert "--exit-on-error" in argv
        assert "--dbname=wslcb_drill" in argv

    def test_failure_is_a_restore_error(self, tmp_path):
        from wslcb_licensing_tracker.restore import RestoreError, restore_into

        path = tmp_path / "x.dump"
        path.write_bytes(DUMP_BYTES)
        with pytest.raises(RestoreError, match=r"pg_restore exited 1"):
            restore_into(path, "wslcb_drill", run_as=None, runner=FakePg(fail="pg_restore"))


class TestFetchArchive:
    def test_downloads_the_matching_subtree_verified(self, client, archive, tmp_path):
        from wslcb_licensing_tracker.restore import fetch_archive

        archive.put(f"{HOST}/wslcb/licensinginfo/2026/2026_10_09/a.html.gz", b"a")
        archive.put(f"{HOST}/wslcb/licensinginfo/2026/2026_10_08/b.html.gz", b"b")
        archive.put("other-host/wslcb/licensinginfo/2026/2026_10_09/c.html.gz", b"c")
        out = tmp_path / "data"
        count = fetch_archive(
            client, ARCHIVE, HOST, out, path="wslcb/licensinginfo/2026/2026_10_09"
        )
        assert count == 1
        restored = out / "wslcb/licensinginfo/2026/2026_10_09/a.html.gz"
        assert restored.read_bytes() == b"a"
        assert stat.S_IMODE(restored.stat().st_mode) == 0o600

    def test_path_matches_whole_segments(self, client, archive, tmp_path):
        """'remediation' must not pull in 'remediation-backups'."""
        from wslcb_licensing_tracker.restore import fetch_archive

        archive.put(f"{HOST}/remediation-backups/x.json", b"x")
        assert fetch_archive(client, ARCHIVE, HOST, tmp_path / "d", path="remediation") == 0

    def test_never_writes_over_an_existing_file(self, client, archive, tmp_path):
        from wslcb_licensing_tracker.restore import RestoreError, fetch_archive

        archive.put(f"{HOST}/remediation-backups/x.json", b"new")
        out = tmp_path / "d"
        (out / "remediation-backups").mkdir(parents=True)
        out.chmod(0o700)
        (out / "remediation-backups/x.json").write_bytes(b"old")
        with pytest.raises(RestoreError, match=r"already exists"):
            fetch_archive(client, ARCHIVE, HOST, out)
        assert (out / "remediation-backups/x.json").read_bytes() == b"old"

    def test_refuses_a_key_escaping_the_destination(self, client, archive, tmp_path):
        from wslcb_licensing_tracker.restore import RestoreError, fetch_archive

        archive.put(f"{HOST}/../../etc/evil", b"x")
        with pytest.raises(RestoreError, match=r"escapes"):
            fetch_archive(client, ARCHIVE, HOST, tmp_path / "d")

    def test_md5_mismatch_is_refused(self, client, archive, tmp_path, monkeypatch):
        from wslcb_licensing_tracker import restore

        archive.put(f"{HOST}/remediation-backups/x.json", b"x")
        monkeypatch.setattr(restore, "b64_md5_file", lambda _p: "AAAA")
        with pytest.raises(restore.RestoreError, match=r"md5"):
            restore.fetch_archive(client, ARCHIVE, HOST, tmp_path / "d")
        assert not (tmp_path / "d/remediation-backups/x.json").exists()
