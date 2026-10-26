"""The backup/restore rehearsal's scratch database: named, dropped, swept (#202).

Ported from watcher's ``tests/ops/scratch_db.py`` (CannObserv/watcher#383) —
the logic, not the transport. watcher drives asyncpg; this module, like the
rehearsal, drives ``psql``, which reports the server's SQLSTATE on stderr under
``-v VERBOSITY=verbose``. :class:`PsqlError` carries it.

**Why a plain ``DROP`` backs up ``WITH (FORCE)``.** The autovacuum launcher
visits a freshly restored database within a minute, and on PostgreSQL 16 a
non-superuser's ``DROP DATABASE … WITH (FORCE)`` refuses an autovacuum worker:
``TerminateOtherDBBackends`` checks ``has_privs_of_role(caller, roleId)``, and
a worker's ``roleId`` is ``InvalidOid``, which no role has the privileges of —
``42501 permission denied to terminate process``. A plain ``DROP`` SIGTERMs
the worker itself and waits up to ~5 s. FORCE still goes first because it is
what ends this role's own stray connections, which a plain ``DROP`` cannot.
Granting ``pg_signal_backend`` would not help: it covers role-owned backends,
not roleless ones.

**Why the name carries the pid namespace.** ``os.kill(pid, 0)`` answers only
for this namespace, so the sweep drops a dead run's database only when the
name says it was made here.
"""

import os
import re
import subprocess
import time
import warnings
from collections.abc import Callable, Iterable
from pathlib import Path

SHAPE = re.compile(r"wslcb_test_restore_(?P<ns>\d+)_(?P<pid>\d+)")

INSUFFICIENT_PRIVILEGE = "42501"
OBJECT_IN_USE = "55006"
ATTEMPTS = 3
BACKOFF_SECONDS = 0.5

# NULLs spelled out: psql -A prints them as empty strings.
HOLDERS = (
    "SELECT pid, coalesce(usename, 'NULL'), coalesce(backend_type, 'NULL'), query "
    "FROM pg_stat_activity WHERE datname = '{name}'"
)
OWN_SCRATCH = (
    "SELECT datname FROM pg_database "
    r"WHERE datname LIKE 'wslcb\_test\_restore\_%' AND pg_has_role(datdba, 'USAGE')"
)

_SQLSTATE = re.compile(r"^ERROR:\s+([0-9A-Z]{5}):", re.MULTILINE)


class ScratchDatabaseLeaked(UserWarning):
    """A scratch database outlived its run; the next run's sweep retries it."""


class PsqlError(Exception):
    """psql exited non-zero: the SQL, the server's SQLSTATE if it sent one, and stderr.

    The message leaves out the DSN, which carries the role's password.
    """

    def __init__(self, sql: str, returncode: int, stderr: str) -> None:
        match = _SQLSTATE.search(stderr)
        self.sqlstate = match[1] if match else None
        self.stderr = stderr
        super().__init__(f"psql exited {returncode} on {sql!r}:\n{stderr.strip()}")


def psql(dsn: str, sql: str) -> str:
    """Run one statement through ``psql``; its stdout, or :class:`PsqlError`."""
    # Fixed argv, the SQL the tests' own; psql from PATH, as the job's pg_dump is.
    argv = [
        "psql",
        "--no-psqlrc",
        "-qAt",
        "-v",
        "ON_ERROR_STOP=1",
        "-v",
        "VERBOSITY=verbose",
        f"--dbname={dsn}",
        "-c",
        sql,
    ]
    result = subprocess.run(argv, capture_output=True, text=True, check=False)  # noqa: S603
    if result.returncode:
        raise PsqlError(sql, result.returncode, result.stderr)
    return result.stdout.strip()


def with_database(dsn: str, name: str) -> str:
    """``dsn`` pointed at database ``name``, query string kept."""
    return re.sub(r"/[^/?]+(\?|$)", f"/{name}\\1", dsn, count=1)


def pid_namespace() -> int:
    """This process's pid-namespace inode: the scope in which a pid means anything."""
    return Path("/proc/self/ns/pid").stat().st_ino


def name(*, pid: int | None = None, ns: int | None = None) -> str:
    """The scratch database name for one run."""
    pid = os.getpid() if pid is None else pid
    ns = pid_namespace() if ns is None else ns
    return f"wslcb_test_restore_{ns}_{pid}"


def is_alive(pid: int) -> bool:
    """Whether ``pid`` names a live process in this pid namespace."""
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, OverflowError):  # gone, or no pid could be this
        return False
    except PermissionError:
        return True  # someone else's process, but alive
    return True


def dead_pid() -> int:
    """A pid that was alive a moment ago and is not now (a reaped child), for tests."""
    child = subprocess.Popen(["true"])  # noqa: S607
    child.wait()
    return child.pid


def stale(
    candidates: Iterable[str],
    *,
    ns: int,
    own_pid: int,
    alive: Callable[[int], bool] = is_alive,
) -> list[str]:
    """The candidates a dead run in this pid namespace left behind, in order."""
    found = []
    for candidate in candidates:
        match = SHAPE.fullmatch(candidate)
        if match is None or int(match["ns"]) != ns:
            continue
        pid = int(match["pid"])
        if pid != own_pid and not alive(pid):
            found.append(candidate)
    return found


def _attempt(run: Callable[[str], str], sql: str) -> PsqlError | None:
    try:
        run(sql)
    except PsqlError as exc:
        return exc
    return None


def drop(
    run: Callable[[str], str],
    scratch: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Drop scratch database ``scratch`` through ``run``; ``True`` once gone.

    FORCE first; on ``42501`` (a backend this role cannot signal) a plain
    ``DROP``; on ``55006`` from either (still in use) both again, with backoff.
    When every attempt ends in use, warn :class:`ScratchDatabaseLeaked` naming
    the database and what ``pg_stat_activity`` shows on it, and return
    ``False``. Any other error is raised: that is a broken drop, not a race.
    """
    if not SHAPE.fullmatch(scratch):
        msg = f"not a scratch database name: {scratch!r}"
        raise ValueError(msg)
    last: PsqlError | None = None
    for attempt in range(ATTEMPTS):
        if attempt:
            sleep(BACKOFF_SECONDS * 2 ** (attempt - 1))
        error = _attempt(run, f"DROP DATABASE IF EXISTS {scratch} WITH (FORCE)")
        if error is not None and error.sqlstate == INSUFFICIENT_PRIVILEGE:
            error = _attempt(run, f"DROP DATABASE IF EXISTS {scratch}")
        if error is None:
            return True
        # From FORCE: signalled, but one outlived the ~5 s wait. From the plain
        # DROP: still held — by the worker, or a connection only FORCE ends.
        if error.sqlstate != OBJECT_IN_USE:
            raise error
        last = error
    holders = run(HOLDERS.format(name=scratch))
    warnings.warn(
        ScratchDatabaseLeaked(
            f"{scratch} not dropped after {ATTEMPTS} attempts ({last}); "
            "on it (pid|usename|backend_type|query) — a NULL usename is a roleless "
            f"backend such as autovacuum:\n{holders}"
        ),
        stacklevel=2,
    )
    return False


def sweep(
    run: Callable[[str], str],
    *,
    ns: int | None = None,
    own_pid: int | None = None,
    alive: Callable[[int], bool] = is_alive,
) -> list[str]:
    """Drop the scratch databases dead runs in this pid namespace leaked.

    Only databases this role could drop are considered. Returns the names it
    tried; a drop that still fails warns, as at teardown.
    """
    targets = stale(
        run(OWN_SCRATCH).splitlines(),
        ns=pid_namespace() if ns is None else ns,
        own_pid=os.getpid() if own_pid is None else own_pid,
        alive=alive,
    )
    for target in targets:
        drop(run, target)
    return targets
