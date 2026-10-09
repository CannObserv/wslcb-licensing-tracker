"""Backup → restore rehearsal against a real PostgreSQL (#185).

What the fakes in ``test_backup.py`` cannot answer: whether a real ``pg_dump``
of the migrated schema passes the backup's own verification, survives the
(fake) bucket, and restores into an empty database with the same tables, rows
and schema version — and whether a truncated real archive is refused. Ported
from watcher's ``tests/ops/test_backup_restore_rehearsal.py``.

Needs ``TEST_DATABASE_URL`` (skips without it, like every PG test here) and a
role that may CREATE DATABASE, for the scratch restore target.
"""

import os
import subprocess
from datetime import UTC, datetime

import pytest
from gcs_fakes import FakeBucket, FakeClient

HOST = "rehearsal-host"


def _libpq(url: str) -> str:
    """SQLAlchemy's ``postgresql+asyncpg://`` as a libpq DSN."""
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _with_db(dsn: str, name: str) -> str:
    return dsn.rsplit("/", 1)[0] + f"/{name}"


def _psql(dsn: str, sql: str) -> str:
    # Fixed argv, the SQL the test's own; psql from PATH, as the job's pg_dump is.
    argv = ["psql", "--no-psqlrc", "-qAt", "-v", "ON_ERROR_STOP=1", f"--dbname={dsn}", "-c", sql]
    result = subprocess.run(  # noqa: S603
        argv,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _counts(dsn: str, tables: list[str]) -> dict[str, str]:
    return {t: _psql(dsn, f"SELECT count(*) FROM {t}") for t in tables}


@pytest.fixture
def source_dsn(pg_engine, pg_url):
    """The migrated test database, as libpq sees it."""
    return _libpq(pg_url)


@pytest.fixture
def scratch_dsn(source_dsn):
    """An empty database to restore into, dropped afterwards."""
    name = f"wslcb_test_restore_{os.getpid()}"
    _psql(source_dsn, f"DROP DATABASE IF EXISTS {name}")
    _psql(source_dsn, f"CREATE DATABASE {name}")
    yield _with_db(source_dsn, name)
    _psql(source_dsn, f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def client():
    return FakeClient(FakeBucket("dumps"), FakeBucket("archive"))


def _ship(client, source_dsn, tmp_path):
    from wslcb_licensing_tracker import backup

    return backup.run_backup(
        database=source_dsn,
        bucket="dumps",
        archive_bucket="archive",
        prefix=HOST,
        client=client,
        workdir=tmp_path / "work",
        data_dir=tmp_path / "empty-data",
        host=HOST,
        now=lambda: datetime.now(UTC),
    )


def test_round_trip(client, source_dsn, scratch_dsn, tmp_path):
    """Dump, ship, fetch by --latest, restore: same schema version, same rows."""
    from wslcb_licensing_tracker import restore

    _psql(
        source_dsn,
        "INSERT INTO scrape_log (started_at, status) VALUES (now(), 'rehearsal')",
    )
    summary = _ship(client, source_dsn, tmp_path)
    assert summary["database"]["outcome"] == "uploaded"

    key = restore.latest_key(client, "dumps", HOST)
    path = restore.fetch(client, "dumps", key, tmp_path / "fetched", runner=subprocess.run)
    restore.restore_into(path, scratch_dsn, run_as=None, runner=subprocess.run)

    tables = ["alembic_version", "license_records", "sources", "record_sources", "scrape_log"]
    assert _counts(scratch_dsn, tables) == _counts(source_dsn, tables)
    head = _psql(scratch_dsn, "SELECT version_num FROM alembic_version")
    assert head == summary["database"]["alembic_head"]


def test_truncated_dump_is_refused(client, source_dsn, tmp_path):
    """A cut archive still lists; the read-through must catch it."""
    from wslcb_licensing_tracker import backup

    path = tmp_path / "full.dump"
    backup.run_pg_dump(source_dsn, path, runner=subprocess.run)
    data = path.read_bytes()
    cut = tmp_path / "cut.dump"
    cut.write_bytes(data[: len(data) * 3 // 5])
    with pytest.raises(backup.BackupError):
        backup.verify_dump(cut, runner=subprocess.run)


def test_failed_restore_leaves_target_untouched(client, source_dsn, scratch_dsn, tmp_path):
    """--single-transaction: a restore that fails part-way loads nothing."""
    from wslcb_licensing_tracker import restore

    _ship(client, source_dsn, tmp_path)
    key = restore.latest_key(client, "dumps", HOST)
    path = restore.fetch(client, "dumps", key, tmp_path / "fetched", runner=subprocess.run)
    _psql(scratch_dsn, "CREATE TABLE sources (blocker int)")  # collides mid-restore
    with pytest.raises(restore.RestoreError):
        restore.restore_into(path, scratch_dsn, run_as=None, runner=subprocess.run)
    tables = _psql(
        scratch_dsn,
        "SELECT string_agg(tablename, ',') FROM pg_tables WHERE schemaname = 'public'",
    )
    assert tables == "sources"
