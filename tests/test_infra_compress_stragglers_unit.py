"""Tests for the weekly data-straggler timer and its drop-in (#200).

The repo-side half of #200: the cohort's disk GC took the tooling tiers, and
the GC never touches app data, so the straggler compression keeps a weekly
timer of its own, in disk hygiene's old Sunday slot.
"""

from pathlib import Path

from test_infra_scrape_unit import _directives

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infra"
TIMER = INFRA / "wslcb-compress-stragglers.timer"
DROPIN_DIR = INFRA / "wslcb-task@compress-stragglers.service.d"
DROPIN = DROPIN_DIR / "override.conf"


class TestTimer:
    def test_timer_targets_registered_alias(self):
        """wslcb-task@%i runs ``cli %i``: the instance must be a top-level command."""
        from wslcb_licensing_tracker.cli import main

        (unit,) = _directives(TIMER, "Timer")["Unit"]
        instance = unit.removeprefix("wslcb-task@").removesuffix(".service")
        assert unit == f"wslcb-task@{instance}.service"
        assert instance == DROPIN_DIR.name.removeprefix("wslcb-task@").removesuffix(".service.d")
        assert instance in main.commands

    def test_weekly_in_disk_hygienes_old_slot(self):
        """Sun 03:00 PT, so the knob's Sunday quiet range still covers it."""
        timer = _directives(TIMER, "Timer")
        assert timer["OnCalendar"] == ["Sun *-*-* 03:00:00 America/Los_Angeles"]
        assert timer["Persistent"] == ["true"]


class TestDropin:
    def test_idle_priority(self):
        service = _directives(DROPIN, "Service")
        assert service["Nice"] == ["10"]
        assert service["IOSchedulingClass"] == ["idle"]

    def test_only_conf_files(self):
        assert sorted(p.name for p in DROPIN_DIR.iterdir()) == ["override.conf"]


def test_the_tooling_timer_is_retired():
    """The host must never run two GCs: the old timer and its drop-in are gone."""
    assert not (INFRA / "wslcb-disk-hygiene.timer").exists()
    assert not (INFRA / "wslcb-task@disk-hygiene.service.d").exists()
