"""Tests for backup.py — the nightly dump and the ``./data/`` archive mirror (#185).

Every decision runs against fakes: which key, which precondition, what the job
refuses to ship, when it checks in. Whether a real ``pg_dump`` round-trips is
``test_backup_restore_rehearsal.py``'s question, against a real server.
"""

import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from gcs_fakes import FakeBucket, FakeClient, b64_md5

BUCKET = "co-gcs-wslcb-backup"
ARCHIVE = "co-gcs-wslcb-archive"
HOST = "wslcb-licensing-tracker"
NOW = datetime(2026, 10, 9, 15, 17, 2, 512000, tzinfo=UTC)
DUMP_BYTES = b"PGDMP\x01\x0f\x00" + b"x" * 64

#: ``pg_restore --list`` on a real custom-format dump, header verbatim (the
#: entries trimmed to the ones the check reads).
TOC = """\
;
; Archive created at 2026-10-09 15:17:02 UTC
;     dbname: wslcb
;     TOC Entries: 412
;     Compression: gzip
;     Dump Version: 1.15-0
;     Format: CUSTOM
;     Dumped from database version: 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1)
;     Dumped by pg_dump version: 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1)
;
;
; Selected TOC Entries:
;
6; 2615 2200 SCHEMA - public pg_database_owner
3601; 0 16390 TABLE DATA public alembic_version wslcb
3602; 0 16400 TABLE DATA public license_records wslcb
3603; 0 16410 TABLE DATA public sources wslcb
3604; 0 16420 TABLE DATA public record_sources wslcb
3605; 0 16430 TABLE DATA public locations wslcb
"""


class FakePg:
    """Answers pg_dump, psql and pg_restore the way the real ones do — behind
    ``setpriv`` too, as the restore runs them."""

    def __init__(self, *, toc=TOC, head="0123abcd", fail="", truncated=False):
        self.toc = toc
        self.head = head
        self.fail = fail
        # A custom-format archive cut short still lists: its table of contents
        # precedes the data. Only reading the data through finds the cut.
        self.truncated = truncated
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        program = argv[argv.index("--") + 1] if argv[0] == "setpriv" else argv[0]
        if program == self.fail:
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr=f"{program}: boom\n")
        if program == "pg_dump":
            out = next(a for a in argv if a.startswith("--file="))
            Path(out.removeprefix("--file=")).write_bytes(DUMP_BYTES)
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        if program == "psql":
            return subprocess.CompletedProcess(argv, 0, stdout=f"{self.head}\n", stderr="")
        if program == "pg_restore" and "--file=/dev/null" in argv:
            if self.truncated:
                error = "pg_restore: error: could not read from input file: end of file\n"
                return subprocess.CompletedProcess(argv, 1, stdout="", stderr=error)
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        if program == "pg_restore":
            return subprocess.CompletedProcess(argv, 0, stdout=self.toc, stderr="")
        message = f"unexpected program {program}"
        raise AssertionError(message)


def _file(root: Path, rel: str, data: bytes, *, age: timedelta = timedelta(days=1)) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    stamp = (NOW - age).timestamp()
    os.utime(path, (stamp, stamp))
    return path


@pytest.fixture
def buckets():
    return FakeBucket(BUCKET), FakeBucket(ARCHIVE)


@pytest.fixture
def client(buckets):
    return FakeClient(*buckets)


@pytest.fixture
def data_dir(tmp_path):
    root = tmp_path / "data"
    _file(root, "wslcb/licensinginfo/2026/2026_10_09/a.html.gz", b"snapshot")
    _file(root, "wslcb/licensinginfo-diffs/notifications/2026_10_09.txt.gz", b"diff")
    _file(root, "remediation-backups/151.json", b"backup")
    return root


def _run(client, tmp_path, data_dir, **overrides):
    from wslcb_licensing_tracker import backup

    kwargs = {
        "database": "wslcb",
        "bucket": BUCKET,
        "archive_bucket": ARCHIVE,
        "prefix": HOST,
        "client": client,
        "workdir": tmp_path / "work",
        "data_dir": data_dir,
        "runner": FakePg(),
        "host": HOST,
        "now": lambda: NOW,
    }
    kwargs.update(overrides)
    return backup.run_backup(**kwargs)


class TestParseToc:
    def test_reads_versions_entries_and_tables(self):
        from wslcb_licensing_tracker.backup import parse_toc

        toc = parse_toc(TOC)
        assert toc.server_version.startswith("16.13")
        assert toc.pg_dump_version.startswith("16.13")
        assert toc.entries == 412
        assert "public.license_records" in toc.tables_with_data
        assert "public.record_sources" in toc.tables_with_data


class TestKeys:
    def test_dump_key_is_tier_then_host_then_stamp(self):
        """Tier first, so a lifecycle rule's matchesPrefix needs no hostname."""
        from wslcb_licensing_tracker.backup import dump_key

        assert dump_key("daily", HOST, NOW) == f"daily/{HOST}/20261009T151702Z.dump"

    def test_month_prefix(self):
        from wslcb_licensing_tracker.backup import month_prefix

        assert month_prefix(HOST, NOW) == f"monthly/{HOST}/202610"


class TestDump:
    def test_ships_a_daily_and_first_monthly(self, client, buckets, tmp_path, data_dir):
        """The first run of a month writes both tiers, create-only."""
        summary = _run(client, tmp_path, data_dir)
        dumps, _ = buckets
        assert sorted(dumps.objects) == [
            f"daily/{HOST}/20261009T151702Z.dump",
            f"monthly/{HOST}/20261009T151702Z.dump",
        ]
        assert set(dumps.preconditions) == {0}
        meta = dumps.metadata[f"daily/{HOST}/20261009T151702Z.dump"]
        assert meta["alembic_head"] == "0123abcd"
        assert meta["source_host"] == HOST
        assert meta["toc_entries"] == "412"
        assert summary["database"]["outcome"] == "uploaded"
        assert (
            summary["database"]["monthly"] == f"gs://{BUCKET}/monthly/{HOST}/20261009T151702Z.dump"
        )

    def test_no_second_monthly_in_a_month(self, client, buckets, tmp_path, data_dir):
        dumps, _ = buckets
        dumps.put(f"monthly/{HOST}/20261001T151702Z.dump", b"earlier", {"sha256": "x"})
        summary = _run(client, tmp_path, data_dir)
        assert f"monthly/{HOST}/20261009T151702Z.dump" not in dumps.objects
        assert summary["database"]["monthly"] == ""

    def test_another_hosts_monthly_does_not_count(self, client, buckets, tmp_path, data_dir):
        dumps, _ = buckets
        dumps.put("monthly/old-host/20261001T000000Z.dump", b"other", {"sha256": "x"})
        _run(client, tmp_path, data_dir)
        assert f"monthly/{HOST}/20261009T151702Z.dump" in dumps.objects

    def test_refuses_a_dump_of_the_wrong_database(self, client, buckets, tmp_path, data_dir):
        from wslcb_licensing_tracker.backup import BackupError

        toc = TOC.replace("TABLE DATA public license_records", "TABLE DATA public other")
        with pytest.raises(BackupError, match=r"license_records"):
            _run(client, tmp_path, data_dir, runner=FakePg(toc=toc))
        dumps, _ = buckets
        assert dumps.objects == {}

    def test_refuses_a_truncated_dump(self, client, buckets, tmp_path, data_dir):
        from wslcb_licensing_tracker.backup import BackupError

        with pytest.raises(BackupError, match=r"could not read"):
            _run(client, tmp_path, data_dir, runner=FakePg(truncated=True))
        assert buckets[0].objects == {}

    def test_pg_dump_failure_is_a_backup_error(self, client, tmp_path, data_dir):
        from wslcb_licensing_tracker.backup import BackupError

        with pytest.raises(BackupError, match=r"pg_dump exited 1"):
            _run(client, tmp_path, data_dir, runner=FakePg(fail="pg_dump"))

    def test_missing_bucket_fails_before_the_dump(self, buckets, tmp_path, data_dir):
        from wslcb_licensing_tracker.backup import BackupError

        pg = FakePg()
        client = FakeClient(*buckets, missing=frozenset({BUCKET}))
        with pytest.raises(BackupError, match=r"not found"):
            _run(client, tmp_path, data_dir, runner=pg)
        assert pg.calls == []

    def test_same_dump_already_there_is_unchanged(self, client, buckets, tmp_path, data_dir):
        import hashlib

        dumps, _ = buckets
        key = f"daily/{HOST}/20261009T151702Z.dump"
        dumps.put(key, DUMP_BYTES, {"sha256": hashlib.sha256(DUMP_BYTES).hexdigest()})
        summary = _run(client, tmp_path, data_dir)
        assert summary["database"]["outcome"] == "unchanged"

    def test_name_collision_with_other_bytes_fails(self, client, buckets, tmp_path, data_dir):
        from wslcb_licensing_tracker.backup import BackupError

        dumps, _ = buckets
        dumps.put(f"daily/{HOST}/20261009T151702Z.dump", b"other", {"sha256": "nope"})
        with pytest.raises(BackupError, match=r"different contents"):
            _run(client, tmp_path, data_dir)


class TestArchive:
    def test_mirrors_in_scope_files_under_the_host(self, client, buckets, tmp_path, data_dir):
        _, archive = buckets
        summary = _run(client, tmp_path, data_dir)
        assert sorted(archive.objects) == [
            f"{HOST}/remediation-backups/151.json",
            f"{HOST}/wslcb/licensinginfo-diffs/notifications/2026_10_09.txt.gz",
            f"{HOST}/wslcb/licensinginfo/2026/2026_10_09/a.html.gz",
        ]
        assert set(archive.preconditions) == {0}
        assert summary["archive"]["uploaded"] == 3

    def test_derived_and_loose_files_are_not_mirrored(self, client, buckets, tmp_path, data_dir):
        """Replay extracts are regenerable; data/*.md are analyses, not sources."""
        _file(data_dir, "wslcb/licensinginfo-replay/x/1.html", b"derived")
        _file(data_dir, "audit-report.md", b"notes")
        _run(client, tmp_path, data_dir)
        _, archive = buckets
        assert not any("replay" in k or k.endswith(".md") for k in archive.objects)

    def test_unchanged_files_are_skipped(self, client, buckets, tmp_path, data_dir):
        _run(client, tmp_path, data_dir)
        _, archive = buckets
        before = len(archive.preconditions)
        summary = _run(client, tmp_path, data_dir)
        assert len(archive.preconditions) == before
        assert summary["archive"] == {"uploaded": 0, "unchanged": 3, "unsettled": 0, "failed": 0}

    def test_new_file_is_uploaded_next_run(self, client, buckets, tmp_path, data_dir):
        _run(client, tmp_path, data_dir)
        _file(data_dir, "wslcb/licensinginfo/2026/2026_10_10/b.html.gz", b"next")
        summary = _run(client, tmp_path, data_dir)
        assert summary["archive"]["uploaded"] == 1

    def test_compression_rename_uploads_the_new_name_and_keeps_the_old(
        self, client, buckets, tmp_path, data_dir
    ):
        """``compress-snapshots`` renames in place; nothing is ever deleted."""
        old = _file(data_dir, "wslcb/licensinginfo/2026/2026_10_08/c.html", b"raw")
        _run(client, tmp_path, data_dir)
        old.unlink()
        _file(data_dir, "wslcb/licensinginfo/2026/2026_10_08/c.html.gz", b"gz")
        _run(client, tmp_path, data_dir)
        _, archive = buckets
        assert f"{HOST}/wslcb/licensinginfo/2026/2026_10_08/c.html" in archive.objects
        assert f"{HOST}/wslcb/licensinginfo/2026/2026_10_08/c.html.gz" in archive.objects

    def test_changed_local_file_is_a_failure_not_an_overwrite(
        self, client, buckets, tmp_path, data_dir
    ):
        """Archive files are frozen: a local change is reported, never shipped over."""
        from wslcb_licensing_tracker.backup import BackupError

        _run(client, tmp_path, data_dir)
        _file(data_dir, "remediation-backups/151.json", b"edited")
        with pytest.raises(BackupError, match=r"151\.json.*differs"):
            _run(client, tmp_path, data_dir)
        _, archive = buckets
        assert archive.objects[f"{HOST}/remediation-backups/151.json"] == b"backup"

    def test_unsettled_files_wait_for_the_next_run(self, client, buckets, tmp_path, data_dir):
        """A file still being written could ship half-written, then mismatch forever."""
        _file(data_dir, "wslcb/licensinginfo/2026/2026_10_09/new.html", b"..", age=timedelta())
        summary = _run(client, tmp_path, data_dir)
        assert summary["archive"]["unsettled"] == 1
        _, archive = buckets
        assert not any(k.endswith("new.html") for k in archive.objects)

    def test_symlinks_are_not_followed(self, client, buckets, tmp_path, data_dir):
        target = tmp_path / "outside.txt"
        target.write_text("secret")
        link = data_dir / "remediation-backups" / "link.json"
        link.symlink_to(target)
        _run(client, tmp_path, data_dir)
        _, archive = buckets
        assert f"{HOST}/remediation-backups/link.json" not in archive.objects

    def test_race_on_create_with_same_bytes_is_unchanged(self, client, buckets, tmp_path, data_dir):
        """A 412 after the listing is fine when the object holds these bytes."""
        from wslcb_licensing_tracker import backup

        _, archive = buckets
        result = backup.ArchiveResult()
        path = data_dir / "remediation-backups/151.json"
        archive.put(f"{HOST}/remediation-backups/151.json", b"backup")
        backup._ship_archive_file(
            client, ARCHIVE, f"{HOST}/remediation-backups/151.json", path, HOST, result
        )
        assert result.unchanged == 1
        assert result.failures == []

    def test_dump_failure_does_not_stop_the_mirror(self, client, buckets, tmp_path, data_dir):
        """Both phases always run; the failure still fails the run."""
        from wslcb_licensing_tracker.backup import BackupError

        with pytest.raises(BackupError, match=r"pg_dump"):
            _run(client, tmp_path, data_dir, runner=FakePg(fail="pg_dump"))
        _, archive = buckets
        assert len(archive.objects) == 3

    def test_missing_archive_bucket_fails_the_mirror(self, buckets, tmp_path, data_dir):
        from wslcb_licensing_tracker.backup import BackupError

        client = FakeClient(*buckets, missing=frozenset({ARCHIVE}))
        with pytest.raises(BackupError, match=r"co-gcs-wslcb-archive.*not found"):
            _run(client, tmp_path, data_dir)
        assert buckets[0].objects  # the dump still shipped


class TestMd5:
    def test_matches_what_gcs_reports(self, tmp_path):
        from wslcb_licensing_tracker.backup import b64_md5_file

        path = tmp_path / "f"
        path.write_bytes(b"hello")
        assert b64_md5_file(path) == b64_md5(b"hello")


class TestMain:
    @pytest.fixture
    def env(self, tmp_path):
        creds = tmp_path / "creds"
        creds.mkdir()
        (creds / "gcs").write_text("{}")
        (creds / "checkin-key").write_text("")
        return {
            "WSLCB_BACKUP_BUCKET": BUCKET,
            "WSLCB_ARCHIVE_BUCKET": ARCHIVE,
            "CREDENTIALS_DIRECTORY": str(creds),
            "GOOGLE_APPLICATION_CREDENTIALS": str(creds / "gcs"),
        }

    @pytest.fixture
    def checkins(self, monkeypatch):
        from wslcb_licensing_tracker import backup

        calls = []
        monkeypatch.setattr(
            backup, "post_checkin", lambda status, variables, **_: calls.append((status, variables))
        )
        return calls

    def _main(self, env, client, data_dir, runner=None):
        from wslcb_licensing_tracker import backup

        return backup.main(
            database="wslcb",
            environ=env,
            client_factory=lambda: client,
            data_dir=data_dir,
            runner=runner or FakePg(),
            host=HOST,
            now=lambda: NOW,
        )

    def test_success_checks_in_ok(self, env, client, data_dir, checkins):
        assert self._main(env, client, data_dir) == 0
        status, variables = checkins[0]
        assert status == "ok"
        assert variables["outcome"] == "ok"
        assert variables["source_host"] == HOST
        assert variables["archive_uploaded"] == 3
        assert variables["object"].startswith(f"gs://{BUCKET}/daily/")

    def test_failure_checks_in_alert_and_exits_1(self, env, client, data_dir, checkins):
        assert self._main(env, client, data_dir, runner=FakePg(fail="pg_dump")) == 1
        status, variables = checkins[0]
        assert status == "alert"
        assert variables["outcome"] == "failed"
        assert "pg_dump" in variables["error"]

    @pytest.mark.parametrize("name", ["WSLCB_BACKUP_BUCKET", "WSLCB_ARCHIVE_BUCKET"])
    def test_missing_bucket_config_exits_2(self, env, client, data_dir, checkins, name):
        """No default bucket: guessing one is how bytes land where nobody reads."""
        del env[name]
        assert self._main(env, client, data_dir) == 2
        assert checkins[0][0] == "alert"
        assert name in checkins[0][1]["error"]

    def test_misplaced_key_exits_2_before_any_client(self, env, data_dir, checkins):
        """backup.env overriding the key path would aim the job at the root-only original."""
        from wslcb_licensing_tracker import backup

        env["GOOGLE_APPLICATION_CREDENTIALS"] = "/etc/wslcb-licensing-tracker/co-wslcb-backup.json"

        def no_client():
            raise AssertionError("client built")

        rc = backup.main(
            database="wslcb", environ=env, client_factory=no_client, data_dir=data_dir, host=HOST
        )
        assert rc == 2
        assert "backup.env" in checkins[0][1]["error"]

    def test_client_construction_failure_alerts(self, env, data_dir, checkins):
        from wslcb_licensing_tracker import backup

        def broken():
            raise RuntimeError("DefaultCredentialsError: no key")

        rc = backup.main(
            database="wslcb", environ=env, client_factory=broken, data_dir=data_dir, host=HOST
        )
        assert rc == 1
        assert "no key" in checkins[0][1]["error"]
