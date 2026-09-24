"""Tests for the #175 memory-pressure configs in infra/.

These three files are one mechanism split across three deploy targets, and
every coupling between them fails *silently* — the reason both configs carry
prose warnings saying so. Prose does not fail a build, so the couplings are
asserted here instead:

- `MemoryLow=` in a unit is capped by its ancestors' protection. This VM mounts
  cgroup2 without `memory_recursiveprot` and `system.slice` grants nothing by
  default, so the unit's reservation is inert unless the slice drop-in grants
  at least as much (CR 1 — it shipped inert).
- earlyoom's regexes match `/proc/<pid>/comm`, which the kernel truncates to 15
  characters. A `$`-anchored pattern therefore matches no npm process at all
  and lets `sshd-session` past `--avoid`, with no error either way.
- earlyoom 1.7 adds 300 to a `--prefer` match's badness, so the web service is
  only safely ranked below one while its own score stays under that. Measured
  on this host: adj 0 -> 674, -500 -> 341 (loses), -600 -> 274, -700 -> 208.
- earlyoom 1.7 (kill.c) drops any process at `oom_score_adj` -1000 from
  candidacy *after* the `--prefer` bonus, exactly as the kernel does. Session
  processes inherit -1000 from exe.dev's sshd, so a `--prefer` that only names
  session processes never fires (#178). It must name something killable.
- With `--prefer` inert, earlyoom's next pick by raw score was a Postgres
  backend or the checkpointer (#178, measured 2026-09-23). SIGKILL on any
  backend makes the postmaster reset every connection: a web outage by proxy.

One coupling lives outside infra/: the SocratiCode plugin session launches
`npx -y --prefer-online ${SOCRATICODE_SPEC:-socraticode@latest}`, so unless a
literal version reaches that launch it installs at every session start the
package moved — the measured peak, not the indexing (#180). Claude Code expands
it from its own *startup* environment: `.claude/settings.json` only declares the
value, and on a VS Code Remote host `claudeCode.environmentVariables` in the
machine settings is what delivers it (gregoryfoster/skills#332).
`scripts/verify-memory-pressure.sh` checks the launch itself.

Nothing here talks to systemd or the kernel; these parse the committed files —
except one test, which reads VS Code's machine settings and skips where there
are none.
"""

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infra"
WEB_UNIT = INFRA / "wslcb-web.service"
SLICE_DROPIN = INFRA / "system.slice.d-10-wslcb-memory.conf"
EARLYOOM = INFRA / "default-earlyoom"
CLAUDE_SETTINGS = REPO_ROOT / ".claude" / "settings.json"
DEPLOYMENT_DOC = REPO_ROOT / "docs" / "DEPLOYMENT.md"
# Host state, not committed: VS Code Remote's machine-scoped settings, the one
# place claudeCode.environmentVariables can live (it cannot be a workspace key).
VSCODE_SERVER = Path.home() / ".vscode-server"
VSCODE_MACHINE_SETTINGS = VSCODE_SERVER / "data" / "Machine" / "settings.json"

# earlyoom 1.7 adds this to a --prefer match's badness; the web service must
# rank below that to survive one. See the module docstring for measurements.
EARLYOOM_PREFER_BONUS = 300

# Real `ps -eo comm=` values from this host. comm is TASK_COMM_LEN (16) minus
# the NUL, so 15 characters — 'npm exec socraticode' arrives truncated.
COMM_SHOULD_PREFER = (
    "node",
    "npm exec socrat",
    "npx",
    # SocratiCode's Docker containers — killable (adj 0), the heaviest
    # transient load measured (#178), and restarted by Docker/ollama.
    "llama-server",
    "ollama",
    "qdrant",
)
COMM_SHOULD_AVOID = (
    "uvicorn",
    "sshd",
    "sshd-session",
    "systemd",
    "systemd-journal",
    "postgres",
    "(sd-pam)",
)

# Processes an agent session launches. They inherit oom_score_adj -1000 from
# exe.dev's sshd, and earlyoom 1.7 never kills a -1000 process (#178).
COMM_UNKILLABLE_SESSION = ("claude", "node", "npm exec socrat", "npx", "bash")

# systemd size suffixes are powers of 1024, not 1000 (systemd.syntax(7)):
# MemoryLow=256M lands in memory.low as 268435456, verified on this host.
_SIZE_UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}


def _directive(path: Path, key: str) -> str:
    """Return the value of a systemd directive, ignoring comments."""
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == key:
            return value.strip()
    msg = f"{path.name} declares no {key}="
    raise AssertionError(msg)


def _bytes(value: str) -> int:
    match = re.fullmatch(r"(\d+)([KMG]?)", value)
    assert match, f"unparseable systemd size {value!r}"
    return int(match.group(1)) * _SIZE_UNITS[match.group(2)]


def _earlyoom_regex(flag: str) -> str:
    args = _directive(EARLYOOM, "EARLYOOM_ARGS").strip('"')
    match = re.search(rf"{flag}\s+'([^']*)'", args)
    assert match, f"EARLYOOM_ARGS declares no {flag}"
    return match.group(1)


def test_slice_grant_covers_the_unit_reservation():
    """CR 1: without this, the unit's MemoryLow resolves to min(unit, 0) = 0."""
    unit = _bytes(_directive(WEB_UNIT, "MemoryLow"))
    parent = _bytes(_directive(SLICE_DROPIN, "MemoryLow"))
    assert parent >= unit, (
        f"system.slice grants {parent} B but wslcb-web.service asks for {unit} B; "
        "cgroup v2 caps effective protection at the ancestors', so the unit's "
        "reservation is silently inert"
    )


def test_web_unit_outranks_an_earlyoom_prefer_match():
    """-500 measured at 341 and would still lose to a --prefer match at +300."""
    adj = int(_directive(WEB_UNIT, "OOMScoreAdjust"))
    assert adj <= -600, (
        f"OOMScoreAdjust={adj} leaves the web service at or above earlyoom's "
        f"+{EARLYOOM_PREFER_BONUS} --prefer bonus; -600 measured at 274, -500 at 341"
    )


@pytest.mark.parametrize("flag", ["--prefer", "--avoid"])
def test_earlyoom_regexes_are_not_end_anchored(flag):
    """A trailing '$' makes these match nothing; comm is truncated to 15 chars."""
    pattern = _earlyoom_regex(flag)
    assert not pattern.endswith("$"), (
        f"{flag} pattern {pattern!r} is end-anchored; /proc/<pid>/comm is "
        "truncated to 15 characters, so this matches no real process"
    )


@pytest.mark.parametrize("comm", COMM_SHOULD_PREFER)
def test_earlyoom_prefers_the_transient_loads(comm):
    assert re.search(_earlyoom_regex("--prefer"), comm), (
        f"--prefer does not match {comm!r} — a transient load earlyoom should pick first"
    )


@pytest.mark.parametrize("comm", COMM_SHOULD_AVOID)
def test_earlyoom_avoids_the_service_and_supervisors(comm):
    assert re.search(_earlyoom_regex("--avoid"), comm), (
        f"--avoid does not match {comm!r} — earlyoom may kill it"
    )


def test_earlyoom_never_prefers_the_web_service():
    """The two regexes must not both claim uvicorn."""
    assert not re.search(_earlyoom_regex("--prefer"), "uvicorn")


def test_earlyoom_prefers_something_killable():
    """#178: a --prefer naming only -1000 session processes can never fire."""
    pattern = _earlyoom_regex("--prefer")
    killable = [
        c for c in COMM_SHOULD_PREFER if c not in COMM_UNKILLABLE_SESSION and re.search(pattern, c)
    ]
    assert killable, (
        f"--prefer {pattern!r} matches only session processes, which inherit "
        "oom_score_adj -1000 and which earlyoom 1.7 skips after applying the bonus"
    )


@pytest.mark.parametrize("flag", ["--prefer", "--avoid"])
def test_earlyoom_regexes_need_no_escaping(flag):
    """systemd's EnvironmentFile parsing sits between this file and earlyoom's argv.

    Its quote/backslash handling is one more layer to get wrong, and a wrong
    regex fails silently. Match literal punctuation with '.' instead.
    """
    pattern = _earlyoom_regex(flag)
    assert "\\" not in pattern, f"{flag} pattern {pattern!r} relies on a backslash escape"


def test_earlyoom_regexes_do_not_overlap():
    """No known comm may be both preferred and avoided."""
    prefer, avoid = _earlyoom_regex("--prefer"), _earlyoom_regex("--avoid")
    for comm in COMM_SHOULD_PREFER + COMM_SHOULD_AVOID:
        assert not (re.search(prefer, comm) and re.search(avoid, comm)), comm


def _session_spec() -> str:
    env = json.loads(CLAUDE_SETTINGS.read_text(encoding="utf-8")).get("env", {})
    return env.get("SOCRATICODE_SPEC", "")


def test_socraticode_session_pins_a_literal_version():
    """An unset or floating SOCRATICODE_SPEC installs at session start (#180)."""
    spec = _session_spec()
    assert re.fullmatch(r"socraticode@\d+\.\d+\.\d+", spec), (
        f"SOCRATICODE_SPEC is {spec!r}; pin the session to the driver's version "
        "in .claude/settings.json (docs/DEPLOYMENT.md 'Memory pressure')"
    )


def test_deployment_doc_names_the_session_pin():
    """The doc states the pinned version; a re-pin must not leave it behind (#180 CR 4)."""
    doc = DEPLOYMENT_DOC.read_text(encoding="utf-8")
    match = re.search(r"both\s+name\s+\*\*(\d+\.\d+\.\d+)\*\*", doc)  # survives a rewrap
    assert match, "docs/DEPLOYMENT.md 'Memory pressure' no longer states the pinned version"
    assert f"socraticode@{match.group(1)}" == _session_spec(), (
        f"docs/DEPLOYMENT.md says both launches name {match.group(1)}, but "
        f".claude/settings.json pins {_session_spec()!r}"
    )


def test_vscode_starts_claude_with_the_declared_session_spec():
    """The settings block declares the pin; only claude's startup env delivers it (#180).

    Claude Code expands the plugin's ${SOCRATICODE_SPEC:-…} before it merges the
    settings env block, which then reaches only the processes it launches: the
    server got the variable and still ran @latest.
    """
    if not VSCODE_SERVER.is_dir():
        pytest.skip("no VS Code server on this host")
    assert VSCODE_MACHINE_SETTINGS.is_file(), (
        f"{VSCODE_MACHINE_SETTINGS} is missing, so nothing delivers SOCRATICODE_SPEC "
        "to claude's startup environment (docs/DEPLOYMENT.md 'Memory pressure')"
    )
    settings = json.loads(VSCODE_MACHINE_SETTINGS.read_text(encoding="utf-8"))
    delivered = {
        e.get("name"): e.get("value") for e in settings.get("claudeCode.environmentVariables", [])
    }
    assert delivered.get("SOCRATICODE_SPEC") == _session_spec(), (
        f"VS Code starts claude with SOCRATICODE_SPEC={delivered.get('SOCRATICODE_SPEC')!r}, "
        f"but .claude/settings.json declares {_session_spec()!r} — re-pin all three"
    )
