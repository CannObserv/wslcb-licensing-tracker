"""Bring a shipped dump, or archive files, back (#185).

Ported from CannObserv/watcher's ``src/ops/restore.py`` (watcher#296 D8). Three
steps, each refusing rather than guessing: **find** a dump (by key, or the
newest under the named host across both tiers); **fetch** it and prove it is
what the backup recorded (sha256 against the object's metadata, then the same
``pg_restore`` checks the backup ran); **restore** it into an existing, empty
database, in one transaction, so a failure leaves nothing half-loaded.

**The source host is always named.** A restore usually runs on a different
host from the one that shipped the dump, so this host's name is the one prefix
never wanted: once its own timer has run it would find a real, verifiable dump
of the wrong database (watcher's two-host cutover). ``--latest`` takes
``--prefix``.

The dump carries table ACLs but not roles, so on a fresh cluster the runbook
creates ``wslcb`` first. docs/RECOVERY.md is that runbook.

:func:`fetch_archive` brings ``./data/`` files back from the archive bucket,
md5-checked, never over an existing file.
"""

import logging
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import NamedTuple

from google.api_core.exceptions import NotFound
from google.cloud import storage

from .backup import (
    DAILY,
    KEY_TIME_FORMAT,
    LIST_TIMEOUT_SECONDS,
    MONTHLY,
    OBJECT_SUFFIX,
    PG_DUMP_TIMEOUT_SECONDS,
    UPLOAD_TIMEOUT_SECONDS,
    BackupError,
    Runner,
    b64_md5_file,
    sha256_file,
    verify_dump,
)

logger = logging.getLogger(__name__)

#: How far a dump's name may run ahead of the bucket's creation time for it:
#: the dumping host's clock is not the bucket's.
CLOCK_SKEW_TOLERANCE = timedelta(minutes=10)

#: What ``--list`` prints beside each name, in this order.
LISTED_METADATA = ("dumped_at", "alembic_head", "size_bytes", "source_host", "sha256")


class RestoreError(Exception):
    """Anything that means nothing was restored."""


def make_client() -> storage.Client:
    """The GCS client, from ``GOOGLE_APPLICATION_CREDENTIALS``. The seam tests replace."""
    return storage.Client()


def as_user(argv: list[str], run_as: str | None) -> list[str]:
    """Prefix ``argv`` to run as ``run_as`` via ``setpriv``; unchanged when None.

    The restore runs by hand as root — it reads the root-only key — and loads
    as ``postgres``. ``setpriv`` opens no PAM session, and ``--reset-env``
    hands the child only its passwd entry's basics, so nothing of the
    operator's root shell reaches a process any ``postgres`` uid can read.
    """
    if run_as is None:
        return argv
    return [
        "setpriv",
        f"--reuid={run_as}",
        f"--regid={run_as}",
        "--init-groups",
        "--reset-env",
        "--",
        *argv,
    ]


class Snapshot(NamedTuple):
    """One shipped dump: its name, its metadata, and the bucket's creation time."""

    name: str
    metadata: dict
    created: datetime | None

    @property
    def named_at(self) -> datetime | None:
        """The time the name claims; None for a name the backup never writes."""
        stamp = PurePosixPath(self.name).name.removesuffix(OBJECT_SUFFIX)
        try:
            return datetime.strptime(stamp, KEY_TIME_FORMAT).replace(tzinfo=UTC)
        except ValueError:
            return None

    @property
    def suspect(self) -> bool:
        """Whether the name claims a time the bucket's own clock contradicts.

        An honest dump is named when ``pg_dump`` starts and created when the
        upload lands, so its name is never later than its creation, beyond
        clock skew. A later name is a skewed writer or a forgery, and a
        planted ``2099…`` would otherwise own ``--latest``.
        """
        named_at = self.named_at
        if named_at is None or self.created is None:
            return True
        return named_at > self.created + CLOCK_SKEW_TOLERANCE


def list_snapshots(client: storage.Client, bucket: str, prefix: str | None) -> list[Snapshot]:
    """Every dump of ``prefix``'s host in both tiers (every host's, if None)."""
    if prefix:
        under = [f"{tier}/{prefix.strip('/')}/" for tier in (DAILY, MONTHLY)]
    else:
        under = [f"{DAILY}/", f"{MONTHLY}/"]
    snapshots = [
        Snapshot(blob.name, dict(blob.metadata or {}), blob.time_created)
        for listing_prefix in under
        for blob in client.list_blobs(bucket, prefix=listing_prefix, timeout=LIST_TIMEOUT_SECONDS)
        if blob.name.endswith(OBJECT_SUFFIX)
    ]
    return sorted(snapshots, key=lambda snapshot: snapshot.name)


def latest_key(client: storage.Client, bucket: str, prefix: str) -> str:
    """The newest dump the named host shipped, by stamp, passing over suspects.

    Across both tiers: a monthly is a copy of a daily, so it only wins once
    the dailies have aged out. On a tie the daily is taken.
    """
    snapshots = list_snapshots(client, bucket, prefix)
    suspect = [snapshot.name for snapshot in snapshots if snapshot.suspect]
    if suspect:
        logger.warning(
            "Passing over dumps named later than the bucket created them — a skewed "
            "clock or a forgery: %s",
            ", ".join(suspect),
        )
    candidates = [snapshot for snapshot in snapshots if not snapshot.suspect]
    if not candidates:
        passed = f" (passed over {len(suspect)} suspect)" if suspect else ""
        msg = f"no dumps for {prefix} in gs://{bucket}{passed}"
        raise RestoreError(msg)
    newest = max(candidates, key=lambda s: (s.named_at, s.name.startswith(f"{DAILY}/")))
    return newest.name


def _private_dir(dest_dir: Path) -> None:
    """Create ``dest_dir`` 0700, or accept only a private one of this user's own.

    An existing directory must be a real directory, owned by this user, and
    closed to everyone else.
    The fetch runs as root and writes the whole production database: a
    directory another user made first would let them read it, or plant a link
    for root to write through.
    """
    dest_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    st = dest_dir.lstat()
    if not stat.S_ISDIR(st.st_mode):
        msg = f"{dest_dir} is not a directory; refusing it"
        raise RestoreError(msg)
    if st.st_uid != os.geteuid():
        msg = f"{dest_dir} is owned by uid {st.st_uid}, not this user; refusing it"
        raise RestoreError(msg)
    if st.st_mode & 0o077:
        msg = (
            f"{dest_dir} is not private (mode {stat.S_IMODE(st.st_mode):o}); "
            "use a new directory, or chmod 700 it"
        )
        raise RestoreError(msg)


def _download_new(blob: storage.Blob, path: Path) -> None:
    """Download into a file created ``O_EXCL | O_NOFOLLOW`` at 0600."""
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError as exc:
        msg = f"{path} already exists; refusing to write over it"
        raise RestoreError(msg) from exc
    with os.fdopen(fd, "wb") as handle:
        try:
            blob.download_to_file(handle, timeout=UPLOAD_TIMEOUT_SECONDS)
        except NotFound as exc:
            msg = f"{blob.name} not found"
            raise RestoreError(msg) from exc


def fetch(client: storage.Client, bucket: str, key: str, dest_dir: Path, *, runner: Runner) -> Path:
    """Download ``key`` into ``dest_dir`` and prove it is the dump that was shipped.

    A failed fetch removes what it wrote.
    """
    blob = client.bucket(bucket).get_blob(key, timeout=LIST_TIMEOUT_SECONDS)
    if blob is None:
        msg = f"gs://{bucket}/{key} not found"
        raise RestoreError(msg)
    expected = (blob.metadata or {}).get("sha256")
    if not expected:
        msg = f"gs://{bucket}/{key} carries no recorded sha256; refusing it"
        raise RestoreError(msg)
    _private_dir(dest_dir)
    path = dest_dir / PurePosixPath(key).name
    _download_new(blob, path)
    try:
        _verify_fetched(path, key, expected, runner=runner)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path


def _verify_fetched(path: Path, key: str, expected: str, *, runner: Runner) -> None:
    """The recorded sha256, then the backup's own ``pg_restore`` checks."""
    actual = sha256_file(path)
    if actual != expected:
        msg = f"{key}: sha256 {actual} does not match the recorded {expected}"
        raise RestoreError(msg)
    try:
        verify_dump(path, runner=runner)
    except BackupError as exc:
        raise RestoreError(str(exc)) from exc


def restore_into(path: Path, database: str, *, run_as: str | None, runner: Runner) -> None:
    """Load ``path`` into an existing, empty ``database``, all or nothing.

    The archive goes in on stdin, so ``postgres`` never needs to read a file
    root wrote.
    """
    argv = as_user(
        [
            "pg_restore",
            "--no-password",
            "--exit-on-error",
            "--single-transaction",
            f"--dbname={database}",
        ],
        run_as,
    )
    with path.open("rb") as handle:
        result = runner(
            argv,
            stdin=handle,
            capture_output=True,
            text=True,
            check=False,
            timeout=PG_DUMP_TIMEOUT_SECONDS,
        )
    if result.returncode != 0:
        tail = " | ".join((result.stderr or "").strip().splitlines()[-3:])
        msg = f"pg_restore exited {result.returncode}: {tail}"
        raise RestoreError(msg)


def fetch_archive(
    client: storage.Client, bucket: str, prefix: str, dest_dir: Path, *, path: str = ""
) -> int:
    """Download the named host's archive files under ``path`` into ``dest_dir``.

    ``path`` is relative to ``data/`` and matches whole segments. Each file
    lands at its ``data/``-relative path, 0600, md5-checked against the bucket,
    and never over an existing file. Returns the number of files written.
    """
    host_prefix = f"{prefix.strip('/')}/"
    subtree = path.strip("/")
    listing_prefix = host_prefix + (f"{subtree}/" if subtree else "")
    _private_dir(dest_dir)
    root = dest_dir.resolve()
    count = 0
    for blob in client.list_blobs(bucket, prefix=listing_prefix, timeout=LIST_TIMEOUT_SECONDS):
        relpath = blob.name.removeprefix(host_prefix)
        target = (root / relpath).resolve()
        if not target.is_relative_to(root) or ".." in PurePosixPath(relpath).parts:
            msg = f"gs://{bucket}/{blob.name} escapes {dest_dir}; refusing it"
            raise RestoreError(msg)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _download_new(blob, target)
        if b64_md5_file(target) != blob.md5_hash:
            target.unlink(missing_ok=True)
            msg = f"gs://{bucket}/{blob.name}: md5 does not match the bucket's"
            raise RestoreError(msg)
        count += 1
    return count
