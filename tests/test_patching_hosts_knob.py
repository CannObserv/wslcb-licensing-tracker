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


def test_backup_regime_covers_the_cluster(knob: dict) -> None:
    """#185's nightly backup stands in for a second dump of the cluster (#220)."""
    (backup,) = knob["backup"]
    assert backup["unit"] == "wslcb-backup.service"
    assert backup["datastores"] == ["postgresql@16-main"]


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
