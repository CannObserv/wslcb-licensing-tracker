"""Tests for the relay's laptop side and VM drop-in (#201).

``scripts/relay/relay-capture.sh`` runs on the operator's Mac, so nothing here
talks to a network: ``curl``, ``scp`` and ``ssh`` are stubs on ``PATH`` that
record their arguments. The plist and drop-in are pinned structurally.
"""

import os
import plistlib
import re
import subprocess
from pathlib import Path

import pytest

from wslcb_licensing_tracker.relay import DEFAULT_INBOX, parse_capture_name

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "relay" / "relay-capture.sh"
PLIST = REPO_ROOT / "scripts" / "relay" / "wslcb-relay.plist"
DROPIN = REPO_ROOT / "infra" / "wslcb-task@import-relayed.service.d" / "timeout.conf"
UNIT = "wslcb-task@import-relayed.service"

_STUB = """#!/bin/bash
args="$*"
echo "$(basename "$0") ${{args//$'\n'/ }}" >> "$STUB_LOG"  # one line per call
{body}
"""


def _stubs(tmp_path: Path, curl_body: str) -> dict[str, str]:
    """A PATH whose curl/scp/ssh are recording stubs; returns the run's env."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    bodies = {
        "curl": curl_body,
        "scp": "exit 0",
        # The remote command is the last argument; keep it whole for TestRemoteCommand.
        "ssh": 'printf "%s" "${@: -1}" > "$STUB_LOG.remote"',
    }
    for name, body in bodies.items():
        stub = bin_dir / name
        stub.write_text(_STUB.format(body=body))
        stub.chmod(0o755)
    return {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STUB_LOG": str(tmp_path / "calls.log"),
        "WSLCB_RELAY_HOST": "vm.example",
        "HOME": str(tmp_path),
    }


def _write_output(content: str) -> str:
    """A curl body that writes *content* to the --output path."""
    return (
        'while [ $# -gt 0 ]; do [ "$1" = --output ] && out="$2"; shift; done\n'
        f"printf '%s' '{content}' > \"$out\""
    )


def _run(env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603 — fixed script path, stubbed tools
        ["/bin/bash", str(SCRIPT)], env=env, capture_output=True, text=True, check=False
    )


def _calls(env: dict[str, str]) -> list[str]:
    log = Path(env["STUB_LOG"])
    return log.read_text().splitlines() if log.exists() else []


class TestScript:
    def test_syntax(self):
        subprocess.run(["/bin/bash", "-n", str(SCRIPT)], check=True)  # noqa: S603

    def test_fetch_upload_then_import_in_order(self, tmp_path):
        env = _stubs(tmp_path, _write_output("<html>page</html>"))
        result = _run(env)
        assert result.returncode == 0, result.stderr
        curl, scp, ssh = _calls(env)
        assert curl.startswith("curl ") and "--fail" in curl
        assert "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp" in curl
        upload = re.search(r"vm\.example:wslcb-relay-inbox/(\S+)\.part$", scp)
        assert upload, scp
        name = upload[1]
        parse_capture_name(name)  # the VM accepts the name the laptop writes
        assert ssh.startswith("ssh ") and "vm.example" in ssh
        assert f"mv 'wslcb-relay-inbox/{name}.part' 'wslcb-relay-inbox/{name}'" in ssh
        assert f"sudo -n systemctl start {UNIT}" in ssh

    def test_fetch_failure_uploads_nothing(self, tmp_path):
        env = _stubs(tmp_path, "exit 22")
        assert _run(env).returncode != 0
        assert [c.split()[0] for c in _calls(env)] == ["curl"]

    def test_empty_body_uploads_nothing(self, tmp_path):
        env = _stubs(tmp_path, _write_output(""))
        result = _run(env)
        assert result.returncode != 0
        assert "empty response" in result.stdout + result.stderr
        assert [c.split()[0] for c in _calls(env)] == ["curl"]

    def test_host_is_required(self, tmp_path):
        env = _stubs(tmp_path, _write_output("x"))
        del env["WSLCB_RELAY_HOST"]
        assert _run(env).returncode != 0
        assert _calls(env) == []

    def test_default_inbox_matches_the_vm_default(self):
        """The script's home-relative default is the importer's DEFAULT_INBOX."""
        assert re.search(r'INBOX="\$\{WSLCB_RELAY_INBOX:-(\S+)\}"', SCRIPT.read_text())[1] == (
            DEFAULT_INBOX.name
        )


class TestRemoteCommand:
    """What runs on the VM: the import, then its summary lines back to the Mac's log."""

    def _remote(self, tmp_path) -> str:
        env = _stubs(tmp_path, _write_output("<html>page</html>"))
        assert _run(env).returncode == 0
        return Path(env["STUB_LOG"] + ".remote").read_text()

    def _vm(self, tmp_path, remote: str, start_rc: int) -> subprocess.CompletedProcess:
        """Run *remote* as the VM would, with sudo/journalctl stubbed."""
        vm = tmp_path / "vm"
        name = re.search(r"mv '(wslcb-relay-inbox/\S+\.part)'", remote)[1]
        (vm / "wslcb-relay-inbox").mkdir(parents=True)
        (vm / name).write_text("upload")
        bin_dir = vm / "bin"
        bin_dir.mkdir()
        stubs = {
            "sudo": f"exit {start_rc}",
            "journalctl": (
                'echo \'{"level": "INFO", "message": "Importing"}\'\n'
                "echo '20261021T134000Z.html.gz: rejected: capture is 118 bytes'"
            ),
        }
        for tool, body in stubs.items():
            (bin_dir / tool).write_text(f"#!/bin/bash\n{body}\n")
            (bin_dir / tool).chmod(0o755)
        return subprocess.run(  # noqa: S603 — the script's own remote command, stubbed tools
            ["/bin/bash", "-c", remote],
            cwd=vm,
            env={"PATH": f"{bin_dir}:{os.environ['PATH']}"},
            capture_output=True,
            text=True,
            check=False,
        )

    def test_summary_lines_come_back_without_the_json_log(self, tmp_path):
        result = self._vm(tmp_path, self._remote(tmp_path), start_rc=0)
        assert result.returncode == 0
        assert "rejected: capture is 118 bytes" in result.stdout
        assert '"level"' not in result.stdout

    def test_the_import_exit_status_is_the_commands(self, tmp_path):
        result = self._vm(tmp_path, self._remote(tmp_path), start_rc=3)
        assert result.returncode == 3
        assert "rejected: capture is 118 bytes" in result.stdout


class TestPlist:
    @pytest.fixture(scope="class")
    def plist(self):
        return plistlib.loads(PLIST.read_bytes())

    def test_runs_ten_minutes_after_the_vm_scrape(self, plist):
        assert plist["StartCalendarInterval"] == [
            {"Hour": 0, "Minute": 40},
            {"Hour": 6, "Minute": 40},
        ]

    def test_runs_the_installed_script_with_its_host(self, plist):
        assert plist["ProgramArguments"] == [
            "/bin/bash",
            "@HOME@/.local/bin/wslcb-relay-capture.sh",
        ]
        assert plist["EnvironmentVariables"] == {"WSLCB_RELAY_HOST": "@HOST@"}

    def test_logs_somewhere_the_operator_can_read(self, plist):
        assert plist["StandardOutPath"] == plist["StandardErrorPath"]
        assert plist["StandardOutPath"].startswith("@HOME@/Library/Logs/")


class TestDropin:
    def test_bounded_timeout_and_no_checkin(self):
        text = DROPIN.read_text()
        assert re.search(r"^TimeoutStartSec=\d+(min|h)$", text, re.MULTILINE)
        assert not re.search(r"^(LoadCredential|After)=", text, re.MULTILINE)
