"""Ship the database and the ``./data/`` archive off the node, nightly (#185).

Two phases, one run, one check-in. Both always run; either failing fails the
run. The pattern is the cohort's (CannObserv/broker#4, watcher#296 D8/D9,
watcher#297), ported from watcher's ``src/ops/backup.py`` wherever Postgres
allows, and built to be dumb in the ways that keep a backup honest:

- **It holds no database credential, and no privilege.** The unit runs as its
  own dynamic user, ``wslcb_backup``, with no capabilities; ``pg_dump`` and
  ``psql`` connect as that user over the local socket, by peer auth, to the
  role of the same name, which holds ``pg_read_all_data`` and nothing else
  (``scripts/setup-backup-role.sql``). Its keys arrive as systemd credentials.
- **It verifies before it ships.** ``pg_restore --list`` must find the data
  sections of the frozen tables (:data:`REQUIRED_TABLES`), so a readable dump of
  the wrong database is refused; then every data block is read through, since
  a truncated archive still lists.
- **It creates, and never overwrites or deletes.** ``if_generation_match=0`` in
  code; ``objectCreator`` + ``objectViewer`` and no ``delete`` at IAM.
  Retention is each bucket's lifecycle rule, so a compromised host cannot erase
  its own history.

**Dumps** go to ``WSLCB_BACKUP_BUCKET`` as ``daily/<host>/<stamp>.dump``, plus a
copy at ``monthly/<host>/<stamp>.dump`` on the month's first run. The tier
leads the key so each lifecycle rule (30 days, 365 days) is a plain
``matchesPrefix`` that no hostname change can slip past. The longer monthly
tier departs from the cohort's flat 30 days on purpose: damage to frozen data
has gone unnoticed for weeks here (#151/#152), and a flat window could leave
only damaged dumps.

**The archive** goes to ``WSLCB_ARCHIVE_BUCKET`` — a separate bucket with *no*
lifecycle rule, since ``./data/`` holds the only copy of pages the upstream
source keeps for 30 days. Each in-scope file (:data:`ARCHIVE_ROOTS`) becomes
``<host>/<path under data/>``: uploaded when absent, skipped when the bucket
already holds the same md5, and a **failure** when it holds different bytes —
these files are frozen, so a local change is reported, never shipped over. A
file younger than :data:`SETTLE_SECONDS` waits for the next run, so a scrape
mid-write never ships half a page. ``compress-*`` renames upload as new names;
the old objects stay.

Restore is ``restore.py``; docs/RECOVERY.md is the runbook around both.
"""

import base64
import hashlib
import logging
import os
import re
import socket
import subprocess
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

from .backup_checkin import CREDENTIALS_DIRECTORY_ENV, post_checkin
from .db import DATA_DIR

logger = logging.getLogger(__name__)

BUCKET_ENV = "WSLCB_BACKUP_BUCKET"
ARCHIVE_BUCKET_ENV = "WSLCB_ARCHIVE_BUCKET"
PREFIX_ENV = "WSLCB_BACKUP_PREFIX"
#: Where the GCS SDK finds its key. The unit sets it to its credential copy.
KEY_PATH_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
DAILY, MONTHLY = "daily", "monthly"
OBJECT_SUFFIX = ".dump"
# Basic-format ISO 8601, UTC. Sorts as it reads, and no ':' for a shell to trip on.
KEY_TIME_FORMAT = "%Y%m%dT%H%M%SZ"
MONTH_FORMAT = "%Y%m"
CONTENT_TYPE = "application/octet-stream"

#: A dump lacking any of these data sections is not a backup of this database.
#: The frozen tables (AGENTS.md), and the schema version a restore checks.
REQUIRED_TABLES = (
    "public.alembic_version",
    "public.license_records",
    "public.sources",
    "public.record_sources",
)

#: What the archive mirrors, relative to ``data/``. Not ``licensinginfo-replay/``
#: (derived: ``wslcb ingest generate-replay-extracts``), nor loose ``data/*.md``.
ARCHIVE_ROOTS = (
    "wslcb/licensinginfo",
    "wslcb/licensinginfo-diffs",
    "wslcb/licensinginfo-internet_archive",
    "remediation-backups",
)
#: A file modified this recently may still be being written.
SETTLE_SECONDS = 600
#: How many per-file archive failures the error message names.
_FAILURES_NAMED = 5

# Bounds on the slow calls. The dump measured 33.4 MB in 4.8 s (2026-09-29);
# these are generous for that and short enough that a wedged call is a failed
# unit rather than a hang.
PG_DUMP_TIMEOUT_SECONDS = 1800
QUERY_TIMEOUT_SECONDS = 60
UPLOAD_TIMEOUT_SECONDS = 600.0
LIST_TIMEOUT_SECONDS = 30.0

_HEADER_RE = re.compile(r"^;\s+(Dumped from database version|Dumped by pg_dump version): (.+)$")
_ENTRIES_RE = re.compile(r"^;\s+TOC Entries: (\d+)$")
_TABLE_DATA_RE = re.compile(r"^\d+; \d+ \d+ TABLE DATA (\S+) (\S+) ")

Runner = Callable[..., subprocess.CompletedProcess]


class BackupError(Exception):
    """Anything that means the run did not ship everything it should have."""


@dataclass(frozen=True)
class Toc:
    """What ``pg_restore --list`` says about an archive."""

    server_version: str | None = None
    pg_dump_version: str | None = None
    entries: int | None = None
    tables_with_data: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class Dump:
    """A verified dump on local disk, and what it says about itself."""

    path: Path
    size_bytes: int
    sha256: str
    dumped_at: datetime
    alembic_head: str | None
    toc: Toc


@dataclass
class ArchiveResult:
    """Per-file outcomes of one archive mirror pass."""

    uploaded: int = 0
    unchanged: int = 0
    unsettled: int = 0
    failures: list[str] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        """The summary the run logs and checks in."""
        return {
            "uploaded": self.uploaded,
            "unchanged": self.unchanged,
            "unsettled": self.unsettled,
            "failed": len(self.failures),
        }


# --- pure ---


def parse_toc(text: str) -> Toc:
    """The header fields and the tables with a data section."""
    versions: dict[str, str] = {}
    entries: int | None = None
    tables: set[str] = set()
    for line in text.splitlines():
        if match := _HEADER_RE.match(line):
            versions[match.group(1)] = match.group(2).strip()
        elif match := _ENTRIES_RE.match(line):
            entries = int(match.group(1))
        elif match := _TABLE_DATA_RE.match(line):
            tables.add(f"{match.group(1)}.{match.group(2)}")
    return Toc(
        server_version=versions.get("Dumped from database version"),
        pg_dump_version=versions.get("Dumped by pg_dump version"),
        entries=entries,
        tables_with_data=frozenset(tables),
    )


def dump_key(tier: str, prefix: str, dumped_at: datetime) -> str:
    """``<tier>/<host>/<stamp>.dump``."""
    stamp = dumped_at.astimezone(UTC).strftime(KEY_TIME_FORMAT)
    return f"{tier}/{prefix.strip('/')}/{stamp}{OBJECT_SUFFIX}"


def month_prefix(prefix: str, at: datetime) -> str:
    """The key prefix every monthly dump of ``at``'s UTC month shares."""
    return f"{MONTHLY}/{prefix.strip('/')}/{at.astimezone(UTC).strftime(MONTH_FORMAT)}"


def iso(at: datetime) -> str:
    """ISO 8601, UTC, second precision, ``Z``."""
    return at.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def misplaced_key(environ: Mapping[str, str]) -> str | None:
    """Why the GCS key path is not this run's credential copy; None when it is.

    Under the unit the path is ``%d/gcs``, the private copy systemd made — but
    an ``EnvironmentFile=`` beats ``Environment=``, so a stray line in
    ``backup.env`` aims the job at the root-only original. The SDK then fails
    "Permission denied", which reads as a reason to loosen the key's mode.
    Outside a unit with credentials there is no copy to compare against.
    """
    directory = environ.get(CREDENTIALS_DIRECTORY_ENV, "").strip()
    if not directory:
        return None
    key = environ.get(KEY_PATH_ENV, "").strip()
    if key and Path(key).is_relative_to(directory):
        return None
    return (
        f"{KEY_PATH_ENV} is {key!r}, not this run's credential copy under {directory}: "
        "remove it from /etc/wslcb-licensing-tracker/backup.env, which overrides the unit's own"
    )


def sha256_file(path: Path) -> str:
    """Hex sha256 of a file, streamed."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def b64_md5_file(path: Path) -> str:
    """A file's md5 as GCS reports ``md5Hash``: base64 of the raw digest."""
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode()


def _tail(text: str | bytes | None) -> str:
    if isinstance(text, bytes):
        text = text.decode(errors="replace")
    return " | ".join((text or "").strip().splitlines()[-3:])


def _describe(exc: Exception) -> str:
    return str(exc) if isinstance(exc, BackupError) else f"{type(exc).__name__}: {exc}"


# --- effects on the database and the dump file ---


def read_alembic_head(database: str, *, runner: Runner) -> str | None:
    """The schema version, recorded so a restore can be checked against it."""
    argv = [
        "psql",
        "--no-password",
        "--no-psqlrc",
        "--quiet",
        "--tuples-only",
        "--no-align",
        f"--dbname={database}",
        "--command=SELECT version_num FROM alembic_version",
    ]
    result = runner(
        argv, capture_output=True, text=True, check=False, timeout=QUERY_TIMEOUT_SECONDS
    )
    if result.returncode != 0:
        msg = f"psql could not read alembic_version: {_tail(result.stderr)}"
        raise BackupError(msg)
    return result.stdout.strip() or None


def run_pg_dump(database: str, out: Path, *, runner: Runner) -> None:
    """Custom format to ``out``, written by ``pg_dump`` itself.

    ``--file``, not watcher's stdout fd (which served its old root unit's
    privilege drop, gone here): on a seekable file ``pg_dump`` records each
    data block's offset in the table of contents, so ``pg_restore -j`` can
    work, and a cut archive fails :func:`verify_dump` wherever it is cut.
    Through stdout it appended a second table of contents (measured on the
    test database, 2026-10-09: 123,728 bytes against 62,474), and a cut that
    removed only that copy passed both reads — harmlessly, as it restored
    identically, but no longer a clean signal.
    """
    argv = [
        "pg_dump",
        "--format=custom",
        "--no-password",
        f"--file={out}",
        f"--dbname={database}",
    ]
    result = runner(
        argv, capture_output=True, text=True, check=False, timeout=PG_DUMP_TIMEOUT_SECONDS
    )
    if result.returncode != 0:
        msg = f"pg_dump exited {result.returncode}: {_tail(result.stderr)}"
        raise BackupError(msg)


def verify_dump(path: Path, *, runner: Runner) -> Toc:
    """Refuse an archive ``pg_restore`` cannot read, or one of the wrong database.

    Two reads. ``--list`` reads the table of contents, which says whose
    database it is. But custom format writes that before the data, so a
    truncated archive lists cleanly; ``--file=/dev/null`` decompresses every
    data block through to the end.
    """
    result = runner(
        ["pg_restore", "--list", str(path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=QUERY_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        msg = f"pg_restore --list rejected {path.name}: {_tail(result.stderr)}"
        raise BackupError(msg)
    toc = parse_toc(result.stdout)
    missing = [table for table in REQUIRED_TABLES if table not in toc.tables_with_data]
    if missing:
        msg = f"dump has no data for {', '.join(missing)}; refusing to ship it"
        raise BackupError(msg)
    full = runner(
        ["pg_restore", "--file=/dev/null", str(path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=PG_DUMP_TIMEOUT_SECONDS,
    )
    if full.returncode != 0:
        msg = f"pg_restore could not read {path.name} through: {_tail(full.stderr)}"
        raise BackupError(msg)
    return toc


def take_dump(database: str, workdir: Path, *, runner: Runner, now: Callable[[], datetime]) -> Dump:
    """Dump, verify, describe. The start time names it: ``pg_dump`` snapshots as it begins."""
    dumped_at = now().astimezone(UTC).replace(microsecond=0)
    alembic_head = read_alembic_head(database, runner=runner)
    workdir.mkdir(parents=True, exist_ok=True)
    path = workdir / "wslcb.dump"
    run_pg_dump(database, path, runner=runner)
    toc = verify_dump(path, runner=runner)
    return Dump(
        path=path,
        size_bytes=path.stat().st_size,
        sha256=sha256_file(path),
        dumped_at=dumped_at,
        alembic_head=alembic_head,
        toc=toc,
    )


# --- effects on the buckets ---


def preflight(client: storage.Client, bucket: str, prefix: str) -> None:
    """Prove the bucket is there and listable by this identity.

    A one-object listing, not ``exists()``: the SDK swallows a missing bucket's
    404 and returns ``False``, exactly the misconfiguration this check exists to
    catch (replicator#7 CR #1). The listing is lazy, so it is advanced.
    """
    listing = client.list_blobs(bucket, max_results=1, prefix=prefix, timeout=LIST_TIMEOUT_SECONDS)
    try:
        next(iter(listing), None)
    except NotFound as exc:
        msg = f"bucket {bucket!r} not found, or not listable: {exc}"
        raise BackupError(msg) from exc


def upload_dump(client: storage.Client, bucket: str, key: str, dump: Dump, *, host: str) -> str:
    """Create the object; ``unchanged`` only if this very dump is already there."""
    blob = client.bucket(bucket).blob(key)
    blob.metadata = {
        "dumped_at": iso(dump.dumped_at),
        "sha256": dump.sha256,
        "size_bytes": str(dump.size_bytes),
        "alembic_head": dump.alembic_head or "",
        "server_version": dump.toc.server_version or "",
        "pg_dump_version": dump.toc.pg_dump_version or "",
        "toc_entries": "" if dump.toc.entries is None else str(dump.toc.entries),
        "source_host": host,
    }
    try:
        blob.upload_from_filename(
            str(dump.path),
            content_type=CONTENT_TYPE,
            if_generation_match=0,
            timeout=UPLOAD_TIMEOUT_SECONDS,
        )
    except PreconditionFailed:
        existing = client.bucket(bucket).get_blob(key, timeout=LIST_TIMEOUT_SECONDS)
        if existing is None:
            msg = f"{key} already exists, and could not be read back"
            raise BackupError(msg) from None
        if (existing.metadata or {}).get("sha256") == dump.sha256:
            return "unchanged"
        msg = f"{key} already exists with different contents"
        raise BackupError(msg) from None
    return "uploaded"


def _month_has_dump(client: storage.Client, bucket: str, prefix: str, at: datetime) -> bool:
    listing = client.list_blobs(
        bucket, max_results=1, prefix=month_prefix(prefix, at), timeout=LIST_TIMEOUT_SECONDS
    )
    return next(iter(listing), None) is not None


def backup_database(  # noqa: PLR0913
    *,
    database: str,
    bucket: str,
    prefix: str,
    client: storage.Client,
    workdir: Path,
    runner: Runner,
    host: str,
    now: Callable[[], datetime],
) -> dict:
    """Phase 1: preflight, dump, verify, create the daily (and the month's first)."""
    preflight(client, bucket, f"{DAILY}/{prefix.strip('/')}/")
    dump = take_dump(database, workdir, runner=runner, now=now)
    key = dump_key(DAILY, prefix, dump.dumped_at)
    outcome = upload_dump(client, bucket, key, dump, host=host)
    monthly = ""
    if not _month_has_dump(client, bucket, prefix, dump.dumped_at):
        monthly_key = dump_key(MONTHLY, prefix, dump.dumped_at)
        upload_dump(client, bucket, monthly_key, dump, host=host)
        monthly = f"gs://{bucket}/{monthly_key}"
    return {
        "outcome": outcome,
        "object": f"gs://{bucket}/{key}",
        "monthly": monthly,
        "dumped_at": iso(dump.dumped_at),
        "size_bytes": dump.size_bytes,
        "sha256": dump.sha256,
        "alembic_head": dump.alembic_head,
    }


def archive_candidates(data_dir: Path) -> list[tuple[str, Path]]:
    """Every regular file under :data:`ARCHIVE_ROOTS`, as ``(relpath, path)``.

    Symlinks are neither followed nor shipped: the archive is what scrapes
    wrote, and a link could reach anything the unit can read.
    """
    found: list[tuple[str, Path]] = []
    for root in ARCHIVE_ROOTS:
        base = data_dir / root
        if not base.is_dir() or base.is_symlink():
            continue
        for dirpath, _dirnames, filenames in os.walk(base):  # never follows dir links
            for name in filenames:
                path = Path(dirpath) / name
                if path.is_symlink() or not path.is_file():
                    continue
                found.append((path.relative_to(data_dir).as_posix(), path))
    return sorted(found)


def _ship_archive_file(  # noqa: PLR0913
    client: storage.Client, bucket: str, key: str, path: Path, host: str, result: ArchiveResult
) -> None:
    """Create one archive object; a 412 is fine only when it holds these bytes."""
    blob = client.bucket(bucket).blob(key)
    blob.metadata = {"source_host": host}
    try:
        blob.upload_from_filename(
            str(path),
            content_type=CONTENT_TYPE,
            if_generation_match=0,
            timeout=UPLOAD_TIMEOUT_SECONDS,
        )
    except PreconditionFailed:
        existing = client.bucket(bucket).get_blob(key, timeout=LIST_TIMEOUT_SECONDS)
        if existing is not None and existing.md5_hash == b64_md5_file(path):
            result.unchanged += 1
        else:
            result.failures.append(f"{key}: created by another writer with different bytes")
        return
    result.uploaded += 1


def mirror_archive(  # noqa: PLR0913
    *,
    bucket: str,
    prefix: str,
    client: storage.Client,
    data_dir: Path,
    host: str,
    now: Callable[[], datetime],
) -> ArchiveResult:
    """Phase 2: create what the bucket lacks, report what it holds differently."""
    under = f"{prefix.strip('/')}/"
    preflight(client, bucket, under)
    remote = {
        blob.name: blob.md5_hash
        for blob in client.list_blobs(bucket, prefix=under, timeout=LIST_TIMEOUT_SECONDS)
    }
    settled_before = now().timestamp() - SETTLE_SECONDS
    result = ArchiveResult()
    for relpath, path in archive_candidates(data_dir):
        key = f"{under}{relpath}"
        try:
            if path.stat().st_mtime > settled_before:
                result.unsettled += 1
            elif key in remote:
                if remote[key] == b64_md5_file(path):
                    result.unchanged += 1
                else:
                    result.failures.append(f"{relpath} differs from gs://{bucket}/{key}")
            else:
                _ship_archive_file(client, bucket, key, path, host, result)
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            result.failures.append(f"{relpath}: {_describe(exc)}")
    return result


# --- orchestration ---


def run_backup(  # noqa: PLR0913 — every collaborator is injectable for the tests
    *,
    database: str,
    bucket: str,
    archive_bucket: str,
    prefix: str,
    client: storage.Client,
    workdir: Path,
    data_dir: Path,
    runner: Runner | None = None,
    host: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict:
    """One run: the dump, then the archive. Returns the summary it logged.

    Both phases run whatever the other did. Raises ``BackupError`` naming every
    failure, whatever its type — a revoked key surfaces from google.auth as a
    ``RefreshError``, a transport fault as a ``TransportError``, neither a
    ``GoogleAPICallError`` (broker#4's code review).

    ``runner`` defaults to ``subprocess.run`` looked up at call time, so a test
    that replaces it can never fall through to the real binaries.
    """
    host = host or socket.gethostname()
    runner = runner if runner is not None else subprocess.run
    errors: list[str] = []
    summary: dict = {"source_host": host}
    try:
        summary["database"] = backup_database(
            database=database,
            bucket=bucket,
            prefix=prefix,
            client=client,
            workdir=workdir,
            runner=runner,
            host=host,
            now=now,
        )
    except Exception as exc:  # noqa: BLE001 — reported below, after the archive runs
        errors.append(f"database: {_describe(exc)}")
    try:
        archive = mirror_archive(
            bucket=archive_bucket,
            prefix=prefix,
            client=client,
            data_dir=data_dir,
            host=host,
            now=now,
        )
    except Exception as exc:  # noqa: BLE001 — reported below
        errors.append(f"archive: {_describe(exc)}")
    else:
        summary["archive"] = archive.counts()
        if archive.failures:
            named = "; ".join(archive.failures[:_FAILURES_NAMED])
            more = len(archive.failures) - _FAILURES_NAMED
            errors.append(
                f"archive: {len(archive.failures)} file(s) failed: {named}"
                + (f"; and {more} more" if more > 0 else "")
            )
    if errors:
        error = " || ".join(errors)
        logger.error("Backup failed: %s", error, extra={"summary": summary})
        raise BackupError(error)
    logger.info("Backup ok: %s", summary["database"]["object"], extra={"summary": summary})
    return summary


def _ok_variables(summary: dict) -> dict:
    """The flat variables an ``ok`` check-in carries, for the monitor's templates."""
    database, archive = summary["database"], summary["archive"]
    return {
        "outcome": "ok",
        "source_host": summary["source_host"],
        "db_outcome": database["outcome"],
        "object": database["object"],
        "monthly": database["monthly"],
        "dumped_at": database["dumped_at"],
        "size_bytes": database["size_bytes"],
        "sha256": database["sha256"],
        "alembic_head": database["alembic_head"],
        **{f"archive_{name}": count for name, count in archive.items()},
    }


def main(  # noqa: PLR0913 — every collaborator is injectable for the tests
    *,
    database: str = "wslcb",
    environ: Mapping[str, str] = os.environ,
    client_factory: Callable[[], storage.Client] = storage.Client,
    data_dir: Path = DATA_DIR,
    runner: Runner | None = None,
    host: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> int:
    """Timer entrypoint. Exit 0 only when everything in scope is in a bucket."""
    host = host or socket.gethostname()

    def fail(code: int, error: str) -> int:
        # The variables a monitor's alert template can use; RECOVERY.md lists them.
        post_checkin(
            "alert", {"source_host": host, "outcome": "failed", "error": error}, environ=environ
        )
        return code

    for name in (BUCKET_ENV, ARCHIVE_BUCKET_ENV):
        if not environ.get(name):
            # No default bucket: guessing one is how bytes land where nobody reads.
            logger.error("%s not set — nowhere to ship", name)
            return fail(2, f"{name} not set")
    if misplaced := misplaced_key(environ):
        logger.error("%s — not starting", misplaced)
        return fail(2, misplaced)
    prefix = environ.get(PREFIX_ENV) or host

    try:
        # Built first, so a missing or unreadable key fails before the dump.
        client = client_factory()
    except Exception as exc:  # noqa: BLE001 — google.auth raises its own hierarchy
        error = f"{type(exc).__name__}: {exc}"
        logger.error("Backup failed before it could start: %s", error)  # noqa: TRY400
        return fail(1, error)
    with tempfile.TemporaryDirectory(prefix="wslcb-backup-") as work:
        try:
            summary = run_backup(
                database=database,
                bucket=environ[BUCKET_ENV],
                archive_bucket=environ[ARCHIVE_BUCKET_ENV],
                prefix=prefix,
                client=client,
                workdir=Path(work),
                data_dir=data_dir,
                runner=runner,
                host=host,
                now=now,
            )
        except BackupError as exc:
            return fail(1, str(exc))
    post_checkin("ok", _ok_variables(summary), environ=environ)
    return 0
