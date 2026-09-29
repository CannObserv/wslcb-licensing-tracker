"""Drift guard: apt's needrestart hook lists restarts and never performs them (#184).

``/etc/apt/apt.conf.d/99needrestart`` runs ``apt-pinvoke -m u`` (which calls
needrestart) after every dpkg run, and Ubuntu's patch to needrestart 3.6 turns
that into *automatic* restarts while ``$nrconf{restart}`` is unset, which is the
stock state. Postgres maps libc6, libssl3t64, libxml2 and libsystemd0, and so do
``wslcb-web`` and the Docker daemon under SocratiCode's containers. A security apply would then
restart all of them mid-apply, outside the owner's window, and a Postgres
restart makes ``/api/v1/health`` 503, which ``wslcb-healthcheck`` answers by
restarting ``wslcb-web`` itself. ``infra/needrestart.conf.d/wslcb.conf`` sets
``'l'``, so the hook only lists. Maintainer scripts are beyond its reach:
postgresql-16 and containerd restart themselves.

Shape from CannObserv/address-validator#237. Two checks read host state, so they
run only on the host, and skip there until the file is installed.
"""

import shutil
import socket
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DROPIN = REPO_ROOT / "infra" / "needrestart.conf.d" / "wslcb.conf"
INSTALLED = Path("/etc/needrestart/conf.d/wslcb.conf")
MAIN_CONF = Path("/etc/needrestart/needrestart.conf")
HOST = "wslcb-licensing-tracker"

on_the_host = pytest.mark.skipif(socket.gethostname() != HOST, reason=f"not {HOST}")

# needrestart's config is Perl, eval'd into `%nrconf`; evaluating it the same way
# is the only honest parse. A syntax error makes needrestart die, and apt's hook
# swallows that with `|| true`: it restarts nothing, so it fails safe, but silently.
_NRCONF_EVAL = (
    "our %nrconf; our $LOGPREF = q(); "
    "eval do { local(@ARGV, $/) = $ARGV[0]; <> }; die $@ if $@; "
    "print defined $nrconf{restart} ? $nrconf{restart} : q(undef);"
)


def _restart_mode(conf: Path) -> str:
    """Evaluate ``conf`` as needrestart does and return ``$nrconf{restart}``."""
    perl = shutil.which("perl")
    if perl is None:
        pytest.skip("perl not available")
    return subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
        [perl, "-e", _NRCONF_EVAL, str(conf)], capture_output=True, text=True, check=True
    ).stdout


def test_the_drop_in_sets_list_only() -> None:
    assert _restart_mode(DROPIN) == "l"


def test_the_drop_in_sets_nothing_else() -> None:
    """One key. ``$nrconf{ui}`` stays unset: setting it without the restart key
    forces interactive mode instead."""
    lines = [
        line.strip()
        for line in DROPIN.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    assert lines == ["$nrconf{restart} = 'l';"]


@on_the_host
def test_the_installed_copy_matches_the_tracked_one() -> None:
    if not INSTALLED.exists():
        pytest.skip(f"{INSTALLED} not installed yet")
    assert INSTALLED.read_text(encoding="utf-8") == DROPIN.read_text(encoding="utf-8"), (
        f"{INSTALLED} differs from infra/needrestart.conf.d/{DROPIN.name}: "
        f"sudo install -m 644 infra/needrestart.conf.d/{DROPIN.name} {INSTALLED.parent}/"
    )


@on_the_host
def test_the_live_chain_resolves_to_list_only() -> None:
    """The main config globs ``conf.d/*.conf`` in sort order, so a later file
    could override this one: evaluate the chain needrestart actually reads."""
    if not INSTALLED.exists():
        pytest.skip(f"{INSTALLED} not installed yet")
    assert _restart_mode(MAIN_CONF) == "l"
