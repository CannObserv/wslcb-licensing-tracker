"""Tests for the weekly entity-classification timer and its check-in drop-in (#207).

Mirrors tests/test_infra_scrape_unit.py: the key is a credential, never an
environment variable; the tailnet is ordering only; the run is bounded; and
the monitor's window covers a week, the timer's jitter, the fall-back hour and
the run itself. Nothing talks to systemd except the installed-copy parity
checks, which skip where the files aren't installed.
"""

import re
from pathlib import Path

import pytest
from test_infra_scrape_unit import DST_SECONDS, _directives, _seconds

REPO_ROOT = Path(__file__).resolve().parent.parent
DROPIN_DIR = REPO_ROOT / "infra" / "wslcb-task@classify-entities.service.d"
DROPIN = DROPIN_DIR / "checkin.conf"
TIMER = REPO_ROOT / "infra" / "wslcb-entity-classification.timer"
MONITORING = REPO_ROOT / "docs" / "MONITORING.md"

WEEK_SECONDS = 7 * 86400
#: A capped run, start to check-in: 200 requests, 8 in flight, plus the backlog count.
RUN_BUDGET_SECONDS = 1800


def _monitor_field(name: str) -> int:
    """A field of the classify monitor's table in MONITORING.md."""
    text = MONITORING.read_text()
    section = text.split("## Entity classification check-in", 1)[1].split("\n## ", 1)[0]
    return int(re.search(rf"`{name}` \| `(\d+)`", section)[1])


class TestDropin:
    def test_key_is_a_credential(self):
        service = _directives(DROPIN, "Service")
        assert service["LoadCredential"] == [
            "checkin-key:/etc/wslcb-licensing-tracker/classify-checkin.key"
        ]
        assert "Environment" not in service, "the key is never an environment variable"

    def test_after_tailnet_without_binding(self):
        unit = _directives(DROPIN, "Unit")
        assert "tailscaled.service" in " ".join(unit["After"]).split()
        for key in ("Wants", "Requires", "BindsTo", "PartOf"):
            assert key not in unit, key

    def test_bounded(self):
        """The template's timeout is infinite; a hung run would block every later firing."""
        (timeout,) = _directives(DROPIN, "Service")["TimeoutStartSec"]
        assert 0 < _seconds(timeout) <= 2 * 3600

    def test_only_conf_files(self):
        assert sorted(p.name for p in DROPIN_DIR.iterdir()) == ["checkin.conf"]


class TestTimer:
    def test_timer_targets_registered_alias(self):
        """wslcb-task@%i runs ``cli %i``: the instance must be a top-level command."""
        from wslcb_licensing_tracker.cli import main

        (unit,) = _directives(TIMER, "Timer")["Unit"]
        assert unit.startswith("wslcb-task@")
        assert unit.endswith(".service")
        instance = unit.removeprefix("wslcb-task@").removesuffix(".service")
        assert instance == DROPIN_DIR.name.removeprefix("wslcb-task@").removesuffix(".service.d")
        assert instance in main.commands

    def test_weekly_on_pacific_time(self):
        timer = _directives(TIMER, "Timer")
        assert timer["OnCalendar"] == ["Sun *-*-* 04:00:00 America/Los_Angeles"]
        assert timer["Persistent"] == ["true"]

    def test_weekly_and_monitor_window(self):
        """One check-in a week; the grace covers jitter, the fall-back hour and the run."""
        jitter = _seconds(_directives(TIMER, "Timer")["RandomizedDelaySec"][0])
        assert _monitor_field("interval_seconds") == WEEK_SECONDS
        assert jitter + DST_SECONDS + RUN_BUDGET_SECONDS <= _monitor_field("grace_seconds")

    def test_run_fits_inside_the_grace(self):
        """A run that may outlast the grace would alarm while still working."""
        (timeout,) = _directives(DROPIN, "Service")["TimeoutStartSec"]
        jitter = _seconds(_directives(TIMER, "Timer")["RandomizedDelaySec"][0])
        assert jitter + DST_SECONDS + _seconds(timeout) <= _monitor_field("grace_seconds")


class TestInstalled:
    @pytest.mark.parametrize(
        ("installed", "repo"),
        [
            (Path("/etc/systemd/system") / DROPIN_DIR.name / DROPIN.name, DROPIN),
            (Path("/etc/systemd/system") / TIMER.name, TIMER),
        ],
    )
    def test_installed_copy_matches_repo(self, installed, repo):
        if not installed.exists():
            pytest.skip(f"{installed} not installed on this host")
        assert installed.read_text() == repo.read_text()
