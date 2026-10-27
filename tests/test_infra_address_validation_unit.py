"""Tests for the daily address-renewal timer and its timeout drop-in (#195).

The timer was weekly ``backfill-addresses``; the early-renewal queue only
flattens the 2027 renewal wave if something spends the spare budget every day,
so it now runs ``renew-addresses`` daily, between the two scrapes. The template
unit's timeout is infinite, so the drop-in bounds a hung run. Nothing here talks
to systemd: the installed copies change only with operator approval.
"""

import re
from pathlib import Path

from test_infra_scrape_unit import _directives, _seconds

REPO_ROOT = Path(__file__).resolve().parent.parent
TIMER = REPO_ROOT / "infra" / "wslcb-address-validation.timer"
SCRAPE_TIMER = REPO_ROOT / "infra" / "wslcb-scraper.timer"
DROPIN_DIR = REPO_ROOT / "infra" / "wslcb-task@renew-addresses.service.d"
DROPIN = DROPIN_DIR / "timeout.conf"
DEPLOYMENT = REPO_ROOT / "docs" / "DEPLOYMENT.md"

#: A full run: 360 rows at ~1 req/s plus answer latency, and up to 40 unplaced
#: rows spaced 13 s apart (~9 min). Measured hook backfills take 7-10 min.
RUN_BUDGET_SECONDS = 3600


def _hour(calendar: str) -> float:
    hh, mm, _ = re.search(r"(\d\d):(\d\d):(\d\d)", calendar).groups()
    return int(hh) + int(mm) / 60


class TestTimer:
    def test_targets_the_renew_instance(self):
        from wslcb_licensing_tracker.cli import main

        (unit,) = _directives(TIMER, "Timer")["Unit"]
        instance = unit.removeprefix("wslcb-task@").removesuffix(".service")
        assert instance == "renew-addresses"
        assert instance in main.commands
        assert instance == DROPIN_DIR.name.removeprefix("wslcb-task@").removesuffix(".service.d")

    def test_daily_on_pacific_time(self):
        timer = _directives(TIMER, "Timer")
        (calendar,) = timer["OnCalendar"]
        assert calendar == "*-*-* 02:00:00 America/Los_Angeles"
        assert timer["Persistent"] == ["true"]

    def test_runs_between_the_scrapes(self):
        """The run takes its share first; the next scrape hook finds the reserve.

        Bounded by the drop-in's timeout, the longest a run can last, so a run
        can never still hold the validation lock when the 06:30 scrape's hook
        starts (CR 12).
        """
        scrapes = sorted(_hour(c) for c in _directives(SCRAPE_TIMER, "Timer")["OnCalendar"])
        (calendar,) = _directives(TIMER, "Timer")["OnCalendar"]
        jitter = _seconds(_directives(TIMER, "Timer")["RandomizedDelaySec"][0]) / 3600
        (timeout,) = _directives(DROPIN, "Service")["TimeoutStartSec"]
        assert scrapes[0] < _hour(calendar)
        assert _hour(calendar) + jitter + _seconds(timeout) / 3600 <= scrapes[1]


class TestDropin:
    def test_bounded(self):
        (timeout,) = _directives(DROPIN, "Service")["TimeoutStartSec"]
        assert RUN_BUDGET_SECONDS <= _seconds(timeout) <= 2 * 3600

    def test_only_conf_files(self):
        assert sorted(p.name for p in DROPIN_DIR.iterdir()) == ["timeout.conf"]

    def test_install_block_copies_it(self):
        """A drop-in left out of the copy block leaves the run unbounded."""
        block = DEPLOYMENT.read_text().split("## After changing service files", 1)[1]
        block = block.split("```", 2)[1]
        assert "infra/wslcb-task@renew-addresses.service.d" in block
        assert "infra/wslcb-address-validation.timer" in block
