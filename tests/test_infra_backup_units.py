"""Tests for infra/wslcb-backup.{service,timer} and the backup role script (#185).

The unit's sandbox is the job's security boundary, and most of its failure
modes are silent: an ``EnvironmentFile=`` with a leading ``-`` runs with no
bucket, an env file that sets ``GOOGLE_APPLICATION_CREDENTIALS`` aims at a key
the run cannot read, a uv-managed interpreter under ``~/.local`` vanishes behind
``ProtectHome=tmpfs`` (203/EXEC). Each is pinned here. Ported from watcher's
``tests/deploy/test_backup_units.py``. Nothing here talks to systemd except
the installed-copy parity check, which skips where the unit isn't installed.
"""

import re
from datetime import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SERVICE = REPO_ROOT / "infra" / "wslcb-backup.service"
TIMER = REPO_ROOT / "infra" / "wslcb-backup.timer"
ROLE_SQL = REPO_ROOT / "scripts" / "setup-backup-role.sql"
CHECKOUT = "/home/exedev/wslcb-licensing-tracker"


def _directives(path: Path, section: str) -> dict[str, list[str]]:
    """Every ``Key=value`` in ``[section]``, values in order, comments skipped."""
    found: dict[str, list[str]] = {}
    current = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1]
            continue
        if current != section or not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        found.setdefault(key.strip(), []).append(value.strip())
    return found


@pytest.fixture(scope="module")
def service():
    return _directives(SERVICE, "Service")


class TestIdentity:
    def test_dynamic_unprivileged_user(self, service):
        assert service["DynamicUser"] == ["yes"]
        assert service["User"] == ["wslcb_backup"]
        assert service["CapabilityBoundingSet"] == [""]
        assert service["NoNewPrivileges"] == ["yes"]

    def test_role_script_names_the_units_user(self, service):
        """Peer auth maps the OS user to the role of the same name."""
        assert f"\\set backup_role {service['User'][0]}" in ROLE_SQL.read_text()


class TestSecrets:
    def test_keys_are_credentials(self, service):
        assert service["LoadCredential"] == [
            "gcs:/etc/wslcb-licensing-tracker/co-wslcb-backup.json",
            "checkin-key:/etc/wslcb-licensing-tracker/backup-checkin.key",
        ]
        assert "GOOGLE_APPLICATION_CREDENTIALS=%d/gcs" in service["Environment"]

    def test_only_backup_env_and_it_is_required(self, service):
        """Never the app's .env; no leading '-', so missing config fails the start."""
        assert service["EnvironmentFile"] == ["/etc/wslcb-licensing-tracker/backup.env"]

    def test_app_env_files_are_hidden(self, service):
        hidden = service["InaccessiblePaths"]
        assert "/etc/wslcb-licensing-tracker" in hidden
        assert f"-{CHECKOUT}/.env" in hidden


class TestSandbox:
    def test_filesystem(self, service):
        assert service["ProtectSystem"] == ["strict"]
        assert service["ProtectHome"] == ["tmpfs"]
        assert service["BindReadOnlyPaths"] == [CHECKOUT]
        assert service["WorkingDirectory"] == [CHECKOUT]
        assert service["PrivateTmp"] == ["yes"]

    def test_runs_the_venv_python_not_uv(self, service):
        """uv wants a writable cache and may sync; the sandbox refuses both."""
        (exec_start,) = service["ExecStart"]
        assert exec_start.startswith(f"{CHECKOUT}/.venv/bin/python -m wslcb_licensing_tracker.cli")
        assert exec_start.endswith("ops backup --database wslcb")
        assert "uv " not in exec_start

    def test_venv_python_resolves_outside_home(self):
        """A uv-managed interpreter under ~/.local is hidden by ProtectHome=tmpfs."""
        python = Path(CHECKOUT) / ".venv" / "bin" / "python"
        if not python.exists():
            pytest.skip("no production venv on this host")
        target = python.resolve()
        assert not target.is_relative_to("/home") or target.is_relative_to(CHECKOUT)

    def test_bounded(self, service):
        assert service["TimeoutStartSec"] == ["3600"]
        assert service["Type"] == ["oneshot"]


class TestOrdering:
    def test_after_postgres_and_tailnet_without_binding(self):
        unit = _directives(SERVICE, "Unit")
        after = " ".join(unit["After"]).split()
        assert {"postgresql.service", "tailscaled.service", "network-online.target"} <= set(after)
        for key in ("Requires", "BindsTo", "PartOf"):
            assert key not in unit, key


class TestTimer:
    def test_daily_after_the_morning_scrape(self):
        """06:30 PT scrape + 5 min jitter + 36 min run ends by 07:11 PT."""
        timer = _directives(TIMER, "Timer")
        (calendar,) = timer["OnCalendar"]
        match = re.fullmatch(r"\*-\*-\* (\d\d):(\d\d):00 America/Los_Angeles", calendar)
        assert match, calendar
        assert time(int(match[1]), int(match[2])) > time(7, 11)
        assert timer["Persistent"] == ["true"]
        assert timer["Unit"] == ["wslcb-backup.service"]

    def test_monitor_grace_covers_jitter_and_timeout(self, service):
        """A run killed by its timeout cannot check in; the monitor's grace must
        outlast the jitter plus that timeout, or a slow night reads as silence."""
        grace = int(
            re.search(r"`grace_seconds` \| `(\d+)`", (REPO_ROOT / "docs/RECOVERY.md").read_text())[
                1
            ]
        )
        jitter = _directives(TIMER, "Timer")["RandomizedDelaySec"][0]
        assert jitter.endswith("min")
        assert int(jitter.removesuffix("min")) * 60 + int(service["TimeoutStartSec"][0]) < grace


class TestInstalled:
    @pytest.mark.parametrize("path", [SERVICE, TIMER])
    def test_installed_copy_matches_repo(self, path):
        installed = Path("/etc/systemd/system") / path.name
        if not installed.exists():
            pytest.skip(f"{path.name} not installed on this host")
        assert installed.read_text() == path.read_text()
