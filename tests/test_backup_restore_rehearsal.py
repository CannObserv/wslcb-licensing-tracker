"""Backup → restore rehearsal against a real PostgreSQL (#185).

What the fakes in ``test_backup.py`` cannot answer: whether a real ``pg_dump``
of the migrated schema passes the backup's own verification, survives the
(fake) bucket, and restores into an empty database with the same tables, rows
and schema version — and whether a truncated real archive is refused. Ported
from watcher's ``tests/ops/test_backup_restore_rehearsal.py``.

Needs ``TEST_DATABASE_URL`` (skips without it, like every PG test here) and a
role that may CREATE DATABASE, for the scratch restore target. That target's
lifecycle — named, dropped past autovacuum, swept when leaked — is
``scratch_db``'s (#202).
"""

import subprocess
from datetime import UTC, datetime
from functools import partial

import pytest
import scratch_db
from gcs_fakes import FakeBucket, FakeClient
from scratch_db import psql

HOST = "rehearsal-host"


def _libpq(url: str) -> str:
    """SQLAlchemy's ``postgresql+asyncpg://`` as a libpq DSN."""
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _counts(dsn: str, tables: list[str]) -> dict[str, str]:
    return {t: psql(dsn, f"SELECT count(*) FROM {t}") for t in tables}


@pytest.fixture
def source_dsn(pg_engine, pg_url):
    """The migrated test database, as libpq sees it."""
    return _libpq(pg_url)


@pytest.fixture
def scratch_dsn(source_dsn):
    """An empty database to restore into, always dropped afterwards.

    Named per pid namespace and pid (``scratch_db.name``). Setup first sweeps
    what dead runs leaked; a teardown drop that loses to a backend it cannot
    end warns ``ScratchDatabaseLeaked`` rather than erroring a test that
    passed, and the next run's sweep reclaims it (#202).
    """
    run = partial(psql, source_dsn)
    name = scratch_db.name()
    scratch_db.sweep(run)
    scratch_db.drop(run, name)
    run(f"CREATE DATABASE {name}")
    yield scratch_db.with_database(source_dsn, name)
    scratch_db.drop(run, name)


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

    psql(
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
    head = psql(scratch_dsn, "SELECT version_num FROM alembic_version")
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
    psql(scratch_dsn, "CREATE TABLE sources (blocker int)")  # collides mid-restore
    with pytest.raises(restore.RestoreError):
        restore.restore_into(path, scratch_dsn, run_as=None, runner=subprocess.run)
    tables = psql(
        scratch_dsn,
        "SELECT string_agg(tablename, ',') FROM pg_tables WHERE schemaname = 'public'",
    )
    assert tables == "sources"


@pytest.fixture
def leaked(source_dsn):
    """A dead run's scratch database, already there when ``scratch_dsn`` sets up."""
    run = partial(psql, source_dsn)
    name = scratch_db.name(pid=scratch_db.dead_pid())
    run(f"CREATE DATABASE {name}")
    yield name
    scratch_db.drop(run, name)  # already swept, unless the test failed


def test_setup_names_this_runs_database_and_sweeps_a_dead_runs(leaked, source_dsn, scratch_dsn):
    """#202: a teardown that loses to autovacuum leaks; the next setup reclaims.

    ``leaked`` is listed first so pytest creates it before ``scratch_dsn`` sweeps.
    """
    assert scratch_dsn == scratch_db.with_database(source_dsn, scratch_db.name())
    found = psql(source_dsn, f"SELECT count(*) FROM pg_database WHERE datname = '{leaked}'")
    assert found == "0"
