"""Drift guard: `.skills/patching-hosts` reads clean under the vendored reader (#192).

The patching-hosts skill reads this knob before every patch run. A malformed
line is never skipped: it makes the host report-only, so a typo in a `quiet`
or `datastore` line would silently turn the monthly run into a report. Running
the skill's own `read-knob.sh` here catches that at commit time, not in the
window.

The `inflight` check guards a measured trap: `wslcb-task@.service` is
`Type=oneshot` with no `RemainAfterExit`, so a running task is `activating`,
not `active`. `--state=active` (the skill's own example) counts 0 mid-scrape
and lets a step start on top of it.
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
KNOB = REPO_ROOT / ".skills" / "patching-hosts"
READ_KNOB = REPO_ROOT / "skills" / "patching-hosts" / "scripts" / "read-knob.sh"
HOST = "wslcb-licensing-tracker"

pytestmark = pytest.mark.skipif(
    not READ_KNOB.is_file(), reason="skills-vendor submodule not checked out"
)


# Past every review-by date the knob holds: each exception has expired.
FAR_FUTURE = "2099-12-31"


def _read_knob(today: str | None = None) -> dict:
    """The knob as the vendored reader resolves it for this host, on `today`."""
    bash = shutil.which("bash")
    assert bash
    argv = [bash, str(READ_KNOB), "--config", str(KNOB), "--host", HOST]
    if today:
        argv += ["--today", today]
    result = subprocess.run(  # noqa: S603 - fixed argv, repo-owned paths
        argv,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.fixture(scope="module")
def knob() -> dict:
    """The knob as the vendored reader resolves it for this host, today."""
    return _read_knob()


@pytest.mark.parametrize("today", [None, FAR_FUTURE])
def test_knob_has_no_findings(today: str | None) -> None:
    """No malformed line, on any date.

    A passed review-by date is an `expired-exception` finding: the probe's to
    report at patch time, not a gate on unrelated commits.
    """
    knob = _read_knob(today)
    assert knob["knob"]["present"]
    assert [f for f in knob["findings"] if f["kind"] != "expired-exception"] == []


def test_expired_exceptions_are_still_reported() -> None:
    knob = _read_knob(FAR_FUTURE)
    expired = {f["line"] for f in knob["findings"] if f["kind"] == "expired-exception"}
    assert expired == {e["line"] for e in knob["exception"]}
    assert knob["report_only"] is False


def test_host_is_not_report_only(knob: dict) -> None:
    assert knob["report_only"] is False, knob["report_only_reasons"]
    assert knob["class"]["value"] == "production"
    assert knob["posture"]["value"] == "scheduled"
    assert knob["window"], "no window: apply.sh refuses every step"


def test_datastore_names_every_cluster_database(knob: dict) -> None:
    (store,) = knob["datastore"]
    assert store["unit"] == "postgresql@16-main"
    assert set(store["databases"]) == {"wslcb", "wslcb_test"}


def test_inflight_counts_running_oneshot_tasks(knob: dict) -> None:
    (inflight,) = knob["inflight"]
    states = inflight["command"].split("--state=", 1)[1].split()[0].split(",")
    assert "activating" in states


def test_restarter_and_service(knob: dict) -> None:
    assert [r["unit"] for r in knob["restarter"]] == ["wslcb-healthcheck.timer"]
    assert [s["unit"] for s in knob["service"]] == ["wslcb-web.service"]


BACKUP_UNIT = REPO_ROOT / "infra" / "wslcb-backup.service"
PROBE_LIBS = ("_knob-lib.sh", "_probe-lib.sh")


def _backup_covers() -> tuple[list[str], str]:
    """The vendored `backup_covers` on each backup line: (refusals, covered).

    `read-knob.sh` accepts a backup line that the probe, the recovery point and
    the bulk refuse (#237, skills#480), so the refusal is read from the
    function they share.
    """
    bash = shutil.which("bash")
    assert bash
    libs = "".join(f'. "{READ_KNOB.parent / lib}"; ' for lib in PROBE_LIBS)
    script = (
        f"set -u; {libs}"
        'knob_load "$1" "$2" "$(date -u +%F)"; '
        'for r in ${KNOB_BACKUP[@]+"${KNOB_BACKUP[@]}"}; do backup_covers "$r"; '
        'for w in ${BC_WHY[@]+"${BC_WHY[@]}"}; do printf "why\\t%s\\n" "$w"; done; '
        'printf "covers\\t%s\\n" "$BC_COVERS"; done'
    )
    result = subprocess.run(  # noqa: S603 - fixed argv, repo-owned paths
        [bash, "-c", script, "backup-covers", str(KNOB), HOST],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    rows = [line.split("\t", 1) for line in result.stdout.splitlines()]
    return [v for k, v in rows if k == "why"], " ".join(v for k, v in rows if k == "covers")


def test_backup_regime_covers_what_the_unit_dumps(knob: dict) -> None:
    """#185's nightly backup stands in for the databases it dumps, no others (#220, #237).

    `wslcb_test` (pytest scratch) is left to the recovery point's own dump. The
    line must name the databases, or patching-hosts refuses it (skills#480).
    """
    (backup,) = knob["backup"]
    assert backup["unit"] == BACKUP_UNIT.name
    execs = [ln for ln in BACKUP_UNIT.read_text().splitlines() if ln.startswith("ExecStart=")]
    dumped = [db for ln in execs for db in re.findall(r"--database[= ](\S+)", ln)]
    assert dumped == ["wslcb"]
    assert backup["datastores"] == [f"postgresql@16-main:{','.join(dumped)}"]


def test_patching_hosts_accepts_the_backup_line() -> None:
    """The probe, recovery point and bulk share `backup_covers`; it must not refuse."""
    refusals, covers = _backup_covers()
    assert refusals == []
    assert covers == "postgresql@16-main.service:wslcb"


def test_tailscale_origin_is_followed_in_its_own_step(knob: dict) -> None:
    """Tailscale (installed 2026-10-09) is a third-party origin (#220).

    An undeclared origin is a probe finding. Its own held step means the
    tailnet drop (check-ins reach ``status`` over MagicDNS) is approved alone.
    """
    origins = {o["origin"]: o["policy"] for o in knob["origin"]}
    assert origins == {"pkgs.tailscale.com": "follow"}
    holds = {h["step"]: h["globs"] for h in knob["hold"]}
    assert holds.get("tailscale") == ["tailscale"]


TAILSCALE_PINS = REPO_ROOT / "infra" / "preferences.d-tailscale"


def _pin_stanzas(text: str) -> list[dict[str, str]]:
    """apt_preferences(5) stanzas: blank-line separated, `#` lines are comments."""
    stanzas = []
    for block in text.split("\n\n"):
        fields = dict(
            line.split(": ", 1)
            for line in block.splitlines()
            if line.strip() and not line.startswith("#")
        )
        if fields:
            stanzas.append(fields)
    return stanzas


def test_tailscale_pins_scope_the_followed_origin() -> None:
    """A followed origin is scoped by pins (policy.md "Two lanes", #220).

    The catch-all goes below 500 so Ubuntu's version wins for anything else the
    site serves; each package Tailscale was installed for goes above 500.
    """
    stanzas = _pin_stanzas(TAILSCALE_PINS.read_text())
    assert all(s["Pin"] == "origin pkgs.tailscale.com" for s in stanzas)
    by_package = {s["Package"]: int(s["Pin-Priority"]) for s in stanzas}
    assert by_package["*"] < 500
    assert by_package["tailscale tailscale-archive-keyring"] > 500
    assert len(by_package) == 2


# The quiet set derived from the timers (#223). A range starts before the
# timer's PDT start and ends this long after its PST start plus its jitter,
# outlasting every run with room to spare (scrape 7-10 min, a full renewal
# ~16 min projected, the rest seconds): the scrape ranges' convention. It
# also absorbs AccuracySec (systemd's default 1 min: a timer may fire that much
# late), which test_no_timer_fires_later_than_a_minute keeps from growing.
RUN_MARGIN_MIN = 30
PACIFIC_OFFSETS_H = (7, 8)  # PDT, PST
WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_ON_CALENDAR = re.compile(
    r"^(?:(?P<dow>Mon|Tue|Wed|Thu|Fri|Sat|Sun) )?\*-\*-\* "
    r"(?P<h>\d\d):(?P<m>\d\d):00 America/Los_Angeles$"
)
_JITTER = re.compile(r"^(?P<n>\d+)(?P<unit>min)?$")
_TIMESPAN = re.compile(r"^(?P<n>\d+)(?P<unit>us|ms|s|sec|m|min|h)?$")
_TIMESPAN_SECONDS = {
    "us": 1e-6,
    "ms": 1e-3,
    None: 1,
    "s": 1,
    "sec": 1,
    "m": 60,
    "min": 60,
    "h": 3600,
}


def _timers() -> list[Path]:
    return sorted((REPO_ROOT / "infra").glob("*.timer"))


def _timer_keys(timer: Path) -> dict[str, list[str]]:
    """Every ``Key=value`` in a unit file, comments skipped, in file order."""
    keys: dict[str, list[str]] = {}
    for raw in timer.read_text(encoding="utf-8").splitlines():
        key, sep, value = raw.partition("=")
        if sep and not raw.lstrip().startswith(("#", ";")):
            keys.setdefault(key.strip(), []).append(value.strip())
    return keys


def _timer_firings() -> list[tuple[str, str, int, int]]:
    """Each infra timer's (name, OnCalendar, PT minute of day, jitter minutes).

    A timer with no OnCalendar (the healthcheck, the knob's restarter) is
    monotonic, not a run to keep clear of. A form this parser doesn't know
    fails, rather than passing as uncovered-but-unseen.
    """
    firings = []
    for timer in _timers():
        keys = _timer_keys(timer)
        jitter = 0
        if "RandomizedDelaySec" in keys:
            m = _JITTER.match(keys["RandomizedDelaySec"][-1])
            assert m, f"{timer.name}: RandomizedDelaySec form unknown to this guard"
            jitter = int(m["n"]) if m["unit"] else -(-int(m["n"]) // 60)
        for cal in keys.get("OnCalendar", []):
            m = _ON_CALENDAR.match(cal)
            assert m, f"{timer.name}: OnCalendar={cal} is a form this guard can't read; teach it"
            firings.append((timer.name, cal, int(m["h"]) * 60 + int(m["m"]), jitter))
    return firings


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def _covered(knob: dict, dow: str | None, start: int, end: int) -> bool:
    """One quiet range on ``dow`` (or every day) holds [start, end], UTC minutes."""
    for q in knob["quiet"]:
        if dow is None and q["weekday"] is not None:
            continue
        if dow is not None and q["weekday"] not in (None, dow):
            continue
        qs, qe = _minutes(q["start"]), _minutes(q["end"])
        if q["wraps"]:
            qe += 24 * 60
        if qs <= start and end <= qe:
            return True
    return False


def test_the_guard_sees_every_scheduled_timer() -> None:
    names = {name for name, *_ in _timer_firings()}
    assert names == {
        "wslcb-address-validation.timer",
        "wslcb-backup.timer",
        "wslcb-compress-stragglers.timer",
        "wslcb-entity-classification.timer",
        "wslcb-scraper.timer",
    }


def test_no_timer_fires_later_than_a_minute() -> None:
    """A wider AccuracySec would let a timer fire past the margin unseen."""
    for timer in _timers():
        for value in _timer_keys(timer).get("AccuracySec", []):
            m = _TIMESPAN.match(value)
            assert m, f"{timer.name}: AccuracySec={value} is a form this guard can't read"
            assert int(m["n"]) * _TIMESPAN_SECONDS[m["unit"]] <= 60, f"{timer.name}: {value}"


@pytest.mark.parametrize("offset_h", PACIFIC_OFFSETS_H, ids=("PDT", "PST"))
@pytest.mark.parametrize(
    "firing",
    [
        pytest.param(f, id=f"{f[0].removesuffix('.timer')}@{f[2] // 60:02d}:{f[2] % 60:02d}PT")
        for f in _timer_firings()
    ],
)
def test_a_quiet_range_covers_each_timer_firing(
    knob: dict, firing: tuple[str, str, int, int], offset_h: int
) -> None:
    """A patch step can't start on top of a timer's run, under either offset (#223).

    The knob's `quiet` set went stale twice (#195 made renewal daily; the
    backup was never in it) because nothing tied it to `infra/*.timer`.
    """
    timer, calendar, pt_min, jitter = firing
    utc = pt_min + offset_h * 60
    dow = _ON_CALENDAR.match(calendar)["dow"]
    if dow and utc >= 24 * 60:
        dow = WEEKDAYS[(WEEKDAYS.index(dow) + 1) % 7]
    start = utc % (24 * 60)
    end = start + jitter + RUN_MARGIN_MIN
    assert _covered(knob, dow, start, end), (
        f"{timer} {calendar} fires {start // 60:02d}:{start % 60:02d}Z under UTC-{offset_h}"
        f"{' ' + dow if dow else ''}; no quiet range covers it through"
        f" {end // 60 % 24:02d}:{end % 60:02d}Z (jitter {jitter} + margin {RUN_MARGIN_MIN} min)"
    )


def _timer_service(timer: Path) -> str:
    """The service a timer starts: its ``Unit=``, else its own name's ``.service``."""
    return (_timer_keys(timer).get("Unit") or [f"{timer.stem}.service"])[-1]


def test_each_timer_is_a_restarter_or_excepted_as_none(knob: dict) -> None:
    """Every timer's service runs `python -m …cli`, which the probe can't follow (#238).

    Undeclared, each is a `restarters:unread:` finding at patch time; a new
    timer would reach the window that way, as the backup's quiet range did (#223).
    """
    restarters = {r["unit"] for r in knob["restarter"]}
    excepted = {e["what"] for e in knob["exception"]}
    missing = [
        timer.name
        for timer in _timers()
        if timer.name not in restarters and f"not-restarter:{_timer_service(timer)}" not in excepted
    ]
    assert missing == []
    services = {f"not-restarter:{_timer_service(timer)}" for timer in _timers()}
    stale = sorted(w for w in excepted if w.startswith("not-restarter:") and w not in services)
    assert stale == [], "an exception outlived its timer"


def test_the_not_restarter_exceptions_still_hold() -> None:
    """They say no task starts or restarts a service: src/ calls no systemctl (#238)."""
    src = REPO_ROOT / "src" / "wslcb_licensing_tracker"
    hits = [p.name for p in sorted(src.rglob("*.py")) if "systemctl" in p.read_text()]
    assert hits == []
