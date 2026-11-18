"""Drift guard: PID 1's own lines stay in the journal after a ``daemon-reload`` (#254).

The exeuntu image's ``/etc/systemd/system.conf.d/container-overrides.conf`` sets
``LogTarget=console``. exe-init starts PID 1 with ``--log-target=syslog``, but
the first ``daemon-reload`` re-reads ``system.conf.d`` and applies the image's
target: from then until the next boot, systemd's ``Started`` / ``Finished`` /
``Failed with result`` / ``Deactivated successfully`` lines go only to ``hvc0``
and never reach ``journalctl -u``. Measured silent 2026-10-27 → 11-06 and from
2026-11-12 23:19Z (#244's deploy reload) until this file was installed.

``infra/system.conf.d/zz-cannobserv-log-target.conf`` is CannObserv/provisioner#62's
file, byte for byte from its ``image/rootfs`` at ``d2bdd36`` (same hash as
CannObserv/usa-wa#520). The ``#62`` in its comment is provisioner's issue, not
this repo's. Re-copy it from provisioner rather than editing it here.

The installed-copy check skips where the file isn't installed, as the
``TestInstalled`` classes of the ``infra`` unit tests do. What the live PID 1
reads (``systemd-analyze get-log-target``) is runtime state, verified at
install time: ``docs/DEPLOYMENT.md`` § PID 1's log target.
"""

import hashlib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DROPIN = REPO_ROOT / "infra" / "system.conf.d" / "zz-cannobserv-log-target.conf"
INSTALLED = Path("/etc/systemd/system.conf.d") / DROPIN.name

#: provisioner's image/rootfs/etc/systemd/system.conf.d/zz-cannobserv-log-target.conf
#: at d2bdd36. A local edit makes this a fork of the cohort file.
PROVISIONER_SHA256 = "0f16a1e6986d15ebb8d40999e05980ce0ce386d0cb8c1b8efa55881b478cb54d"

#: The image's drop-in this one must sort after (measured on this host 2026-11-18).
EXEUNTU_DROPIN = "container-overrides.conf"


def _settings(text: str) -> list[tuple[str, str]]:
    """Every ``(section, key=value)`` line, blanks and comments skipped."""
    section = ""
    out: list[tuple[str, str]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            continue
        out.append((section, line))
    return out


def test_the_drop_in_is_byte_identical_to_provisioners() -> None:
    assert hashlib.sha256(DROPIN.read_bytes()).hexdigest() == PROVISIONER_SHA256


def test_the_drop_in_sets_only_the_log_target() -> None:
    """One setting, so exeuntu's other ``[Manager]`` settings stay its own."""
    assert _settings(DROPIN.read_text(encoding="utf-8")) == [
        ("Manager", "LogTarget=journal-or-kmsg")
    ]


def test_the_drop_in_sorts_after_exeuntus() -> None:
    """Drop-ins apply in filename order across every ``system.conf.d``; the later
    one wins per setting. systemd reads only ``*.conf``."""
    assert DROPIN.is_file()
    assert DROPIN.suffix == ".conf"
    assert sorted([DROPIN.name, EXEUNTU_DROPIN]) == [EXEUNTU_DROPIN, DROPIN.name]


class TestInstalled:
    def test_installed_copy_matches_repo(self) -> None:
        if not INSTALLED.exists():
            pytest.skip(f"{INSTALLED} not installed on this host")
        assert INSTALLED.read_bytes() == DROPIN.read_bytes(), (
            f"{INSTALLED} differs from infra/system.conf.d/{DROPIN.name}: "
            f"sudo install -m 644 infra/system.conf.d/{DROPIN.name} {INSTALLED.parent}/"
        )
