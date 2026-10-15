"""Tests for infra/wslcb-task@scrape.service.d/ — the scrape's check-in drop-in (#196).

The drop-in carries what only the scrape instance needs: the co-status
check-in key as a credential, tailnet ordering, and a timeout. Its failure
modes are quiet: a key passed by environment shows in ``/proc``, a ``Wants=``
lets a tailscale restart take a scrape down, a grace shorter than one DST
cycle alarms once a year for nothing. Each is pinned here. Nothing talks to
systemd except the installed-copy parity check, which skips where the drop-in
isn't installed.
"""

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = REPO_ROOT / "infra" / "wslcb-task@.service"
DROPIN_DIR = REPO_ROOT / "infra" / "wslcb-task@scrape.service.d"
DROPIN = DROPIN_DIR / "checkin.conf"
TIMER = REPO_ROOT / "infra" / "wslcb-scraper.timer"
MONITORING = REPO_ROOT / "docs" / "MONITORING.md"

#: The fall-back hour: the timer runs on Pacific time, the monitor counts seconds.
DST_SECONDS = 3600
#: From start to check-in: fetch (~13.5 MB), parse and ingest. Measured max 61 s
#: over 90 days of scrape_log; the check-in precedes the minutes-long backfill.
SCRAPE_BUDGET_SECONDS = 600


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


def _monitor_field(name: str) -> int:
    """A field of the scrape monitor's table in MONITORING.md § Scrape check-in."""
    section = MONITORING.read_text().split("## Scrape check-in", 1)[1].split("\n## ", 1)[0]
    return int(re.search(rf"`{name}` \| `(\d+)`", section)[1])


def _seconds(value: str) -> int:
    match = re.fullmatch(r"(\d+)(h|min|s)?", value)
    assert match, value
    return int(match[1]) * {"h": 3600, "min": 60, "s": 1, None: 1}[match[2]]


class TestDropin:
    def test_key_is_a_credential(self):
        service = _directives(DROPIN, "Service")
        assert service["LoadCredential"] == [
            "checkin-key:/etc/wslcb-licensing-tracker/scrape-checkin.key"
        ]
        assert "Environment" not in service, "the key is never an environment variable"

    def test_config_arrives_through_the_templates_env_file(self):
        """WSLCB_SCRAPE_CHECKIN_BASE_URL and _MONITOR_ID live in the app .env."""
        service = _directives(TEMPLATE, "Service")
        assert service["EnvironmentFile"] == ["/etc/wslcb-licensing-tracker/.env"]

    def test_after_tailnet_without_binding(self):
        """Ordering only: a tailscale restart must not take a scrape down."""
        unit = _directives(DROPIN, "Unit")
        assert "tailscaled.service" in " ".join(unit["After"]).split()
        for key in ("Wants", "Requires", "BindsTo", "PartOf"):
            assert key not in unit, key

    def test_bounded(self):
        """The template's timeout is infinite; a hung scrape would block every later firing."""
        (timeout,) = _directives(DROPIN, "Service")["TimeoutStartSec"]
        assert _seconds(timeout) == 2 * 3600

    def test_only_conf_files(self):
        """systemd reads only *.conf from a drop-in directory."""
        assert sorted(p.name for p in DROPIN_DIR.iterdir()) == ["checkin.conf"]


class TestMonitorTiming:
    def _calendar_times(self) -> list[timedelta]:
        times = []
        for calendar in _directives(TIMER, "Timer")["OnCalendar"]:
            match = re.fullmatch(r"\*-\*-\* (\d\d):(\d\d):00 America/Los_Angeles", calendar)
            assert match, calendar
            times.append(timedelta(hours=int(match[1]), minutes=int(match[2])))
        return sorted(times)

    def test_interval_covers_the_longest_gap(self):
        """The runs are 6 h then 18 h apart; an interval under the long gap
        alarms on a single missed morning run."""
        times = self._calendar_times()
        gaps = [b - a for a, b in zip(times, [*times[1:], times[0] + timedelta(days=1)])]
        assert _monitor_field("interval_seconds") >= max(gaps).total_seconds()

    def test_grace_covers_jitter_scrape_and_dst(self):
        """One cycle a year is 25 h; a check-in also waits for jitter and the scrape."""
        jitter = _seconds(_directives(TIMER, "Timer")["RandomizedDelaySec"][0])
        grace = _monitor_field("grace_seconds")
        assert jitter + SCRAPE_BUDGET_SECONDS + DST_SECONDS <= grace

    def test_health_goes_stale_when_the_monitor_alerts(self):
        """/api/v1/health's ``stale`` flag uses the monitor's own deadline (#197),
        so the endpoint and the alert never disagree about an outage."""
        from wslcb_licensing_tracker.scraper import STALE_AFTER

        deadline = _monitor_field("interval_seconds") + _monitor_field("grace_seconds")
        assert STALE_AFTER.total_seconds() == deadline

    def test_fall_back_really_is_an_hour_longer(self):
        """The DST term above is real, not a guess: 06:30 to 06:30 across 2026-11-01."""
        from zoneinfo import ZoneInfo

        pacific = ZoneInfo("America/Los_Angeles")
        # Same-tzinfo subtraction is wall-clock time; compare in UTC.
        before = datetime(2026, 10, 31, 6, 30, tzinfo=pacific).astimezone(UTC)
        after = datetime(2026, 11, 1, 6, 30, tzinfo=pacific).astimezone(UTC)
        assert (after - before).total_seconds() == 86400 + DST_SECONDS


class TestInstalled:
    def test_installed_copy_matches_repo(self):
        installed = Path("/etc/systemd/system") / DROPIN_DIR.name / DROPIN.name
        if not installed.exists():
            pytest.skip("scrape check-in drop-in not installed on this host")
        assert installed.read_text() == DROPIN.read_text()
