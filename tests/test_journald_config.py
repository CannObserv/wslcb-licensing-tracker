"""Drift guard: the journal persists, capped at 512M (#186).

The Feb-2026 exeuntu image masks ``systemd-journal-flush.service``, so journald
stayed in ``/run`` (tmpfs, so RAM on a host with no swap) despite the image's own
``Storage=persistent``, and every reboot lost it: #184's and #220's. The fix is
``systemctl unmask``; ``infra/journald.conf.d/wslcb.conf`` tracks the setting and
the cap. The host checks catch a re-mask (a rebuild from that image) and an
installed copy that drifted.

Shape from CannObserv/address-validator#241. The host checks run only on the
host. Unlike the template's, they fail rather than skip when the file is
missing: it was installed on 2026-10-28, so its absence is drift.
"""

import shutil
import socket
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DROPIN = REPO_ROOT / "infra" / "journald.conf.d" / "wslcb.conf"
INSTALLED = Path("/etc/systemd/journald.conf.d/wslcb.conf")
HOST = "wslcb-licensing-tracker"
# #200's disk-gc layer installs a journald drop-in under
# /usr/local/lib/systemd/journald.conf.d/ with the same cap; whichever sorts
# last wins, so the two must agree.
CAP = "512M"

on_the_host = pytest.mark.skipif(socket.gethostname() != HOST, reason=f"not {HOST}")


def _settings(text: str) -> dict[str, str]:
    """Last value wins per key, as journald reads a chain of conf files."""
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";", "[")):
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


def _systemctl(*args: str) -> str:
    systemctl = shutil.which("systemctl")
    assert systemctl
    return subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
        [systemctl, *args], capture_output=True, text=True, check=False
    ).stdout.strip()


def _assert_installed() -> None:
    assert INSTALLED.exists(), (
        f"{INSTALLED} is missing (installed 2026-10-28, #186): "
        f"sudo install -m 644 infra/journald.conf.d/{DROPIN.name} {INSTALLED.parent}/"
    )


def test_the_drop_in_sets_persistent_storage_and_the_cap_only() -> None:
    text = DROPIN.read_text(encoding="utf-8")
    assert "[Journal]" in text
    assert _settings(text) == {"Storage": "persistent", "SystemMaxUse": CAP}


@on_the_host
def test_the_installed_copy_matches_the_tracked_one() -> None:
    _assert_installed()
    assert INSTALLED.read_text(encoding="utf-8") == DROPIN.read_text(encoding="utf-8"), (
        f"{INSTALLED} differs from infra/journald.conf.d/{DROPIN.name}: "
        f"sudo install -m 644 infra/journald.conf.d/{DROPIN.name} {INSTALLED.parent}/"
    )


@on_the_host
def test_the_live_chain_resolves_to_persistent_and_the_cap() -> None:
    """Every conf.d file, in the order journald reads them, #200's included."""
    _assert_installed()
    analyze = shutil.which("systemd-analyze")
    assert analyze
    chain = subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
        [analyze, "cat-config", "systemd/journald.conf"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    settings = _settings(chain)
    assert settings.get("Storage") == "persistent"
    assert settings.get("SystemMaxUse") == CAP


@on_the_host
def test_the_flush_is_not_masked_and_the_journal_is_on_disk() -> None:
    """A mask leaves Storage=persistent inert: read where the journal is."""
    _assert_installed()
    state = _systemctl("is-enabled", "systemd-journal-flush.service")
    assert state != "masked", "sudo systemctl unmask systemd-journal-flush (DEPLOYMENT.md)"
    machine_id = Path("/etc/machine-id").read_text(encoding="utf-8").strip()
    assert Path(f"/var/log/journal/{machine_id}/system.journal").exists()
