"""The rehearsal's scratch database: named, dropped, swept (#202).

Ported from watcher's ``tests/ops/test_scratch_db.py`` (CannObserv/watcher#383).
The failure this guards was reproduced on demand here against PostgreSQL
16.15: with an autovacuum worker on the scratch database, the ``wslcb`` role's
``DROP DATABASE … WITH (FORCE)`` fails with exactly the ``42501`` below, while
a plain ``DROP`` SIGTERMs the worker and succeeds. Autovacuum's arrival can't
be timed in the suite, so these fakes cover the fallback; the PG tests at the
bottom cover what the real server always does.
"""

import os
import subprocess
import time
import warnings
from functools import partial
from pathlib import Path

import pytest
import scratch_db
from scratch_db import PsqlError, ScratchDatabaseLeaked, with_database

NS = 4026531836
PID = 4242
NAME = f"wslcb_test_restore_{NS}_{PID}"

# What psql -v VERBOSITY=verbose printed for each, captured from the server.
TERMINATE_REFUSED = (
    "ERROR:  42501: permission denied to terminate process\n"
    "DETAIL:  Only roles with privileges of the role whose process is being terminated or with "
    'privileges of the "pg_signal_backend" role may terminate this process.\n'
    "LOCATION:  TerminateOtherDBBackends, procarray.c:3849\n"
)


def refused(sql: str) -> PsqlError:
    """The 16.15 server's answer to FORCE with an autovacuum worker on the database."""
    return PsqlError(sql, 1, TERMINATE_REFUSED)


def in_use(sql: str) -> PsqlError:
    """A plain DROP that waited ~5 s and still found another backend."""
    name = sql.split()[4]
    return PsqlError(
        sql,
        1,
        f'ERROR:  55006: database "{name}" is being accessed by other users\n'
        "DETAIL:  There is 1 other session using the database.\n",
    )


def syntax(sql: str) -> PsqlError:
    return PsqlError(sql, 1, 'ERROR:  42601: syntax error at or near "x"\n')


def not_owner(sql: str) -> PsqlError:
    return PsqlError(sql, 1, f"ERROR:  42501: must be owner of database {sql.split()[4]}\n")


class FakeRun:
    """Answers each statement from a script, in order; records what it was sent.

    A script entry is an error factory (called with the SQL, then raised), a
    string (returned as psql's stdout) or ``None`` (no output).
    """

    def __init__(self, *script) -> None:
        self.script = list(script)
        self.sent: list[str] = []

    def __call__(self, sql: str) -> str:
        self.sent.append(sql)
        answer = self.script.pop(0) if self.script else None
        if callable(answer):
            raise answer(sql)
        return answer or ""


class Sleeps(list):
    def __call__(self, seconds: float) -> None:
        self.append(seconds)


def forced(name: str) -> str:
    return f"DROP DATABASE IF EXISTS {name} WITH (FORCE)"


def plain(name: str) -> str:
    return f"DROP DATABASE IF EXISTS {name}"


class TestPsql:
    def test_returns_stdout_stripped(self, monkeypatch) -> None:
        def run(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 0, stdout="42\n", stderr="")

        monkeypatch.setattr(scratch_db.subprocess, "run", run)
        assert scratch_db.psql("postgresql://u@h/db", "SELECT 42") == "42"

    def test_asks_for_the_sqlstate_and_stops_on_error(self, monkeypatch) -> None:
        seen = []

        def run(argv, **kwargs):
            seen.append(argv)
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        monkeypatch.setattr(scratch_db.subprocess, "run", run)
        scratch_db.psql("postgresql://u@h/db", "SELECT 1")
        argv = " ".join(seen[0])
        assert "VERBOSITY=verbose" in argv
        assert "ON_ERROR_STOP=1" in argv

    def test_a_failure_carries_sql_sqlstate_and_stderr_never_the_dsn(self, monkeypatch) -> None:
        """#202's ask: the next failure explains itself. CalledProcessError's
        message printed the argv, and with it the DSN's password."""

        def run(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr=TERMINATE_REFUSED)

        monkeypatch.setattr(scratch_db.subprocess, "run", run)
        with pytest.raises(PsqlError) as caught:
            scratch_db.psql("postgresql://u:s3cret@h/db", forced(NAME))
        assert caught.value.sqlstate == "42501"
        assert caught.value.stderr == TERMINATE_REFUSED
        message = str(caught.value)
        assert forced(NAME) in message
        assert "permission denied to terminate process" in message
        assert "s3cret" not in message

    def test_no_server_error_means_no_sqlstate(self) -> None:
        exc = PsqlError("SELECT 1", 2, 'psql: error: connection to server at "h" failed\n')
        assert exc.sqlstate is None


class TestWithDatabase:
    def test_swaps_the_database_and_keeps_the_query(self) -> None:
        assert (
            with_database("postgresql://u:p@h:5432/wslcb_test?sslmode=require", "x")
            == "postgresql://u:p@h:5432/x?sslmode=require"
        )

    def test_without_a_query(self) -> None:
        assert with_database("postgresql://u@h/wslcb_test", "x") == "postgresql://u@h/x"


class TestName:
    def test_carries_the_pid_namespace_and_the_pid(self) -> None:
        assert scratch_db.name(pid=PID, ns=NS) == NAME

    def test_defaults_to_this_process(self) -> None:
        assert scratch_db.name() == scratch_db.name(pid=os.getpid(), ns=scratch_db.pid_namespace())

    def test_the_namespace_is_this_processs_pid_namespace(self) -> None:
        assert scratch_db.pid_namespace() == Path("/proc/self/ns/pid").stat().st_ino


class TestStale:
    def test_is_this_namespaces_dead_pids_and_nothing_else(self) -> None:
        candidates = [
            f"wslcb_test_restore_{NS}_111",  # dead, ours: stale
            f"wslcb_test_restore_{NS}_222",  # alive
            NAME,  # this run, whatever alive() says
            "wslcb_test_restore_999_111",  # another pid namespace
            "wslcb_test_restore_3014207",  # the pre-#202 shape: never swept
            "wslcb_test",
            f"wslcb_test_restore_{NS}_111_x",
            f"xwslcb_test_restore_{NS}_111",
        ]
        stale = scratch_db.stale(candidates, ns=NS, own_pid=PID, alive=lambda pid: pid == 222)
        assert stale == [f"wslcb_test_restore_{NS}_111"]


class TestIsAlive:
    def test_this_process_is(self) -> None:
        assert scratch_db.is_alive(os.getpid())

    def test_a_reaped_child_is_not(self) -> None:
        assert not scratch_db.is_alive(scratch_db.dead_pid())

    def test_a_pid_no_process_could_have_is_not(self) -> None:
        """A hand-made name can carry one; setup's sweep must not crash on it."""
        assert not scratch_db.is_alive(10**30)

    def test_a_process_we_may_not_signal_still_is(self, monkeypatch) -> None:
        def kill(pid: int, sig: int) -> None:
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(scratch_db.os, "kill", kill)
        assert scratch_db.is_alive(1)


class TestDrop:
    def test_is_one_forced_drop_when_nothing_resists(self) -> None:
        run = FakeRun(None)
        assert scratch_db.drop(run, NAME) is True
        assert run.sent == [forced(NAME)]

    def test_a_backend_the_role_cannot_signal_falls_back_to_a_plain_drop(self) -> None:
        """The #202 failure: FORCE refuses the autovacuum worker, plain DROP
        signals it itself — no warning, no retry."""
        run = FakeRun(refused, None)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert scratch_db.drop(run, NAME) is True
        assert run.sent == [forced(NAME), plain(NAME)]

    def test_a_stray_connection_with_the_worker_clears_on_the_next_pass(self) -> None:
        """Plain DROP SIGTERMs the worker but cannot end the role's own stray
        connection (55006); the next FORCE can, once the worker is gone."""
        sleeps = Sleeps()
        run = FakeRun(refused, in_use, None)
        assert scratch_db.drop(run, NAME, sleep=sleeps) is True
        assert run.sent == [forced(NAME), plain(NAME), forced(NAME)]
        assert sleeps == [scratch_db.BACKOFF_SECONDS]

    def test_a_terminated_backend_slow_to_exit_is_retried_not_raised(self) -> None:
        """FORCE's own 55006: it signalled everyone, but one outlived the ~5 s wait."""
        sleeps = Sleeps()
        run = FakeRun(in_use, None)
        assert scratch_db.drop(run, NAME, sleep=sleeps) is True
        assert run.sent == [forced(NAME), forced(NAME)]
        assert sleeps == [scratch_db.BACKOFF_SECONDS]

    def test_a_database_that_stays_in_use_warns_naming_it_and_who_holds_it(self) -> None:
        sleeps = Sleeps()
        # What the test role sees of a backend it may not inspect: no usename,
        # no backend_type (an autovacuum worker, or another role's session).
        holder = "598125|NULL|NULL|<insufficient privilege>"
        run = FakeRun(*[refused, in_use] * scratch_db.ATTEMPTS, holder)
        with pytest.warns(ScratchDatabaseLeaked, match=NAME) as caught:
            assert scratch_db.drop(run, NAME, sleep=sleeps) is False

        message = str(caught[0].message)
        assert "598125" in message
        assert "55006" in message
        assert run.sent[: 2 * scratch_db.ATTEMPTS] == [forced(NAME), plain(NAME)] * (
            scratch_db.ATTEMPTS
        )
        assert "pg_stat_activity" in run.sent[-1]
        assert len(sleeps) == scratch_db.ATTEMPTS - 1
        assert sleeps == sorted(sleeps)
        assert sleeps[0] == scratch_db.BACKOFF_SECONDS

    def test_a_leak_is_a_user_warning_so_the_ratchet_lets_it_warn(self) -> None:
        """#216's ratchet errors RuntimeWarning and PytestWarning; a leak must not."""
        assert issubclass(ScratchDatabaseLeaked, UserWarning)
        assert not issubclass(ScratchDatabaseLeaked, (RuntimeWarning, pytest.PytestWarning))

    def test_any_other_refusal_of_force_is_raised(self) -> None:
        run = FakeRun(syntax)
        with pytest.raises(PsqlError):
            scratch_db.drop(run, NAME)
        assert run.sent == [forced(NAME)]

    def test_a_plain_drop_refused_on_privilege_is_raised_not_retried(self) -> None:
        """Plain DROP never terminates anyone, so its 42501 is "must be owner":
        a wrong role, not a transient."""
        run = FakeRun(refused, not_owner)
        with pytest.raises(PsqlError):
            scratch_db.drop(run, NAME)
        assert run.sent == [forced(NAME), plain(NAME)]

    @pytest.mark.parametrize(
        "name",
        ["wslcb_test", "wslcb_test_restore_3014207", f"{NAME}; DROP DATABASE wslcb", ""],
    )
    def test_refuses_any_name_without_the_scratch_shape(self, name: str) -> None:
        run = FakeRun()
        with pytest.raises(ValueError, match="scratch"):
            scratch_db.drop(run, name)
        assert run.sent == []


class TestSweep:
    def test_drops_the_stale_and_returns_their_names(self) -> None:
        stale = f"wslcb_test_restore_{NS}_111"
        listed = f"{stale}\n{NAME}\nwslcb_test_restore_111"
        run = FakeRun(listed, None)
        swept = scratch_db.sweep(run, ns=NS, own_pid=PID, alive=lambda _: False)
        assert swept == [stale]
        assert "pg_has_role" in run.sent[0], "only what this role could drop"
        assert run.sent[1:] == [forced(stale)]


# --- Against the test server -------------------------------------------------


@pytest.fixture
def dsn(pg_engine, pg_url):
    """The migrated test database, as libpq sees it (skips without PG)."""
    return pg_url.replace("postgresql+asyncpg://", "postgresql://", 1)


class Recording:
    """A real runner that also records what it was sent."""

    def __init__(self, run) -> None:
        self._run = run
        self.sent: list[str] = []

    def __call__(self, sql: str) -> str:
        self.sent.append(sql)
        return self._run(sql)


def _exists(run, name: str) -> bool:
    return run(f"SELECT count(*) FROM pg_database WHERE datname = '{name}'") == "1"


def test_drop_ends_the_roles_own_stray_connection(dsn) -> None:
    """A rehearsal that dies with a session open leaves this role connected;
    the forced drop is what ends it (a plain one would wait 5 s and fail)."""
    run = partial(scratch_db.psql, dsn)
    name = scratch_db.name(pid=scratch_db.dead_pid())
    run(f"CREATE DATABASE {name}")
    # Fixed argv; psql from PATH, as the rehearsal's is.
    target = with_database(dsn, name)
    argv = ["psql", "--no-psqlrc", f"--dbname={target}", "-c", "SELECT pg_sleep(60)"]
    held = subprocess.Popen(  # noqa: S603
        argv,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while run(f"SELECT count(*) FROM pg_stat_activity WHERE datname = '{name}'") == "0":
            assert time.monotonic() < deadline, "the held session never connected"
            time.sleep(0.1)
        recorded = Recording(run)
        assert scratch_db.drop(recorded, name) is True
        # FORCE alone, unless autovacuum visited the new database too: then
        # plain DROP ends the worker and a second FORCE ends the session.
        assert recorded.sent[-1] == forced(name)
        assert not _exists(run, name)
        assert held.wait(timeout=10) != 0, "the drop should have terminated it"
    finally:
        held.kill()
        held.wait()
        scratch_db.drop(run, name)


def test_sweep_drops_a_dead_runs_database_and_spares_the_rest(dsn) -> None:
    run = partial(scratch_db.psql, dsn)
    dead = scratch_db.name(pid=scratch_db.dead_pid())
    live = scratch_db.name()  # this run's own: spared
    legacy = f"wslcb_test_restore_{scratch_db.dead_pid()}"  # the pre-#202 shape: spared
    try:
        for name in (dead, live, legacy):
            run(f"CREATE DATABASE {name}")
        swept = scratch_db.sweep(run)
        assert dead in swept
        assert not set(swept) & {live, legacy}
        assert not _exists(run, dead)
        assert _exists(run, live)
        assert _exists(run, legacy)
    finally:
        for name in (dead, live):
            scratch_db.drop(run, name)
        # drop() refuses the old shape; plain DROP outwaits autovacuum, and no
        # session of ours is open on it.
        run(f"DROP DATABASE IF EXISTS {legacy}")
