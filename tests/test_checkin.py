"""Tests for checkin.py — the dead-man check-ins of the backup (#185) and scrape (#196).

Ported from CannObserv/watcher's ``tests/ops/test_checkin.py``. No network:
every test replaces the one POST seam.
"""

import logging

import httpx
import pytest

from wslcb_licensing_tracker.checkin import BACKUP, SCRAPE, post_checkin

BASE = "http://status:9000"
MONITOR = "01K5ZQ8M3N4P5Q6R7S8T9V0W1X"


def _environ(tmp_path, *, base=BASE, monitor=MONITOR, key="sekrit"):
    """A unit-shaped environment: config in env vars, the key as a credential file."""
    creds = tmp_path / "creds"
    creds.mkdir(exist_ok=True)
    (creds / "checkin-key").write_text(key)
    env = {"CREDENTIALS_DIRECTORY": str(creds)}
    if base is not None:
        env["WSLCB_BACKUP_CHECKIN_BASE_URL"] = base
    if monitor is not None:
        env["WSLCB_BACKUP_MONITOR_ID"] = monitor
    return env


class _Recorder:
    """Answers each POST with the next of *codes*: a status, ``(status, body)``, or a raise."""

    def __init__(self, *codes):
        self.codes = list(codes) or [202]
        self.calls = []

    def __call__(self, url, payload, headers, timeout):
        self.calls.append((url, payload, headers))
        code = self.codes.pop(0)
        if isinstance(code, Exception):
            raise code
        return code if isinstance(code, tuple) else (code, "")


def _scrape_env(tmp_path):
    env = _environ(tmp_path, base=None, monitor=None)
    return env | {"WSLCB_SCRAPE_CHECKIN_BASE_URL": BASE, "WSLCB_SCRAPE_MONITOR_ID": MONITOR}


class TestPostCheckin:
    def test_ok_posts_to_the_monitor_with_the_key_header(self, tmp_path):
        """A configured check-in POSTs status + variables with X-API-Key."""
        post = _Recorder(202)
        assert (
            post_checkin("ok", {"a": 1}, monitor=BACKUP, environ=_environ(tmp_path), post=post)
            is True
        )
        url, payload, headers = post.calls[0]
        assert url == f"{BASE}/api/v1/monitors/{MONITOR}/checkin"
        assert payload == {"status": "ok", "variables": {"a": 1}}
        assert headers == {"X-API-Key": "sekrit"}

    def test_unconfigured_warns_and_posts_nothing(self, tmp_path, caplog):
        """No URL, no monitor, empty key: a WARNING, and no request."""
        post = _Recorder()
        env = _environ(tmp_path, base=None, monitor=None, key="")
        with caplog.at_level(logging.WARNING):
            assert post_checkin("ok", {}, monitor=BACKUP, environ=env, post=post) is False
        assert post.calls == []
        assert "not configured" in caplog.text

    def test_empty_key_file_is_half_configured(self, tmp_path, caplog):
        """URL and monitor set but the key file still empty: an ERROR."""
        post = _Recorder()
        with caplog.at_level(logging.ERROR):
            assert (
                post_checkin(
                    "ok", {}, monitor=BACKUP, environ=_environ(tmp_path, key=""), post=post
                )
                is False
            )
        assert post.calls == []
        assert "half-configured" in caplog.text

    def test_key_is_never_read_from_the_environment(self, tmp_path):
        """Without a credentials directory there is no key, whatever env says."""
        post = _Recorder()
        env = {
            "WSLCB_BACKUP_CHECKIN_BASE_URL": BASE,
            "WSLCB_BACKUP_MONITOR_ID": MONITOR,
            "WSLCB_BACKUP_CHECKIN_KEY": "from-env",
        }
        assert post_checkin("ok", {}, monitor=BACKUP, environ=env, post=post) is False
        assert post.calls == []

    def test_retries_once_on_5xx(self, tmp_path):
        post = _Recorder(503, 202)
        assert (
            post_checkin("alert", {}, monitor=BACKUP, environ=_environ(tmp_path), post=post) is True
        )
        assert len(post.calls) == 2

    def test_retries_once_on_transport_error(self, tmp_path):
        post = _Recorder(httpx.ConnectError("no route"), 202)
        assert post_checkin("ok", {}, monitor=BACKUP, environ=_environ(tmp_path), post=post) is True

    def test_4xx_is_not_retried(self, tmp_path):
        post = _Recorder(403, 202)
        assert (
            post_checkin("ok", {}, monitor=BACKUP, environ=_environ(tmp_path), post=post) is False
        )
        assert len(post.calls) == 1

    @pytest.mark.parametrize("code", [401, 403, 404])
    def test_auth_rejections_point_at_the_id_and_key(self, tmp_path, caplog, code):
        """Bad key, wrong tenant, unknown monitor: the id/key hint is the right one."""
        post = _Recorder(code)
        with caplog.at_level(logging.WARNING):
            assert (
                post_checkin("ok", {}, monitor=BACKUP, environ=_environ(tmp_path), post=post)
                is False
            )
        assert f"rejected with HTTP {code}" in caplog.text
        assert "check the monitor id and key" in caplog.text

    def test_422_names_the_payload_not_the_key(self, tmp_path, caplog):
        """A 422 is the payload refused — a template that can't render these
        variables, or a body co-status won't take as JSON. Nothing is recorded,
        and nothing is retried (#199)."""
        post = _Recorder(422, 202)
        with caplog.at_level(logging.WARNING):
            assert (
                post_checkin("alert", {}, monitor=SCRAPE, environ=_scrape_env(tmp_path), post=post)
                is False
            )
        assert len(post.calls) == 1
        assert "scrape check-in rejected with HTTP 422" in caplog.text
        assert "templates could not render these variables" in caplog.text
        assert "not valid JSON" in caplog.text
        assert "nothing was recorded" in caplog.text
        assert "monitor id and key" not in caplog.text

    @pytest.mark.parametrize("code", [400, 409, 413])
    def test_other_4xx_is_neutral(self, tmp_path, caplog, code):
        """Neither hint: a 4xx co-status gives no meaning to here has none to point at."""
        post = _Recorder(code)
        with caplog.at_level(logging.WARNING):
            assert (
                post_checkin("ok", {}, monitor=BACKUP, environ=_environ(tmp_path), post=post)
                is False
            )
        assert f"rejected with HTTP {code}" in caplog.text
        assert "monitor id and key" not in caplog.text
        assert "templates" not in caplog.text

    def test_rejection_logs_the_body_on_one_line_capped(self, tmp_path, caplog):
        """co-status's 422 names the field or template section at fault; logging
        it saves a round trip. One line, capped, and never the headers."""
        body = '{"detail": "body_template: \'window_expires\' is undefined"}\n' + "x" * 1000
        post = _Recorder((422, body))
        with caplog.at_level(logging.WARNING):
            post_checkin("alert", {}, monitor=BACKUP, environ=_environ(tmp_path), post=post)
        (line,) = [r.getMessage() for r in caplog.records if "HTTP 422" in r.getMessage()]
        assert "body_template: 'window_expires' is undefined" in line
        assert "\n" not in line
        assert len(line) < 600
        assert "sekrit" not in caplog.text

    def test_rejection_without_a_body_logs_no_body(self, tmp_path, caplog):
        post = _Recorder((403, "  "))
        with caplog.at_level(logging.WARNING):
            post_checkin("ok", {}, monitor=BACKUP, environ=_environ(tmp_path), post=post)
        assert "co-status said" not in caplog.text

    @pytest.mark.parametrize("base", ["status:9000", "ftp://status", "http://status:port"])
    def test_malformed_base_url_posts_nothing(self, tmp_path, base):
        post = _Recorder()
        assert (
            post_checkin("ok", {}, monitor=BACKUP, environ=_environ(tmp_path, base=base), post=post)
            is False
        )
        assert post.calls == []

    def test_monitor_id_must_be_a_bare_id(self, tmp_path):
        """The id becomes a path segment; a slash would re-aim the request."""
        post = _Recorder()
        env = _environ(tmp_path, monitor="../../admin")
        assert post_checkin("ok", {}, monitor=BACKUP, environ=env, post=post) is False
        assert post.calls == []

    def test_never_raises(self, tmp_path, caplog):
        """Whatever escapes the checks is logged by type and reported as not landed."""

        def boom(*_args):
            raise ValueError("contains the key: sekrit")

        with caplog.at_level(logging.ERROR):
            assert (
                post_checkin("ok", {}, monitor=BACKUP, environ=_environ(tmp_path), post=boom)
                is False
            )
        assert "ValueError" in caplog.text
        assert "sekrit" not in caplog.text


class TestMonitors:
    """One implementation, one configuration per monitor (#196)."""

    def test_scrape_reads_its_own_variables(self, tmp_path):
        """The scrape monitor ignores the backup's id: a shared id would let one
        job's heartbeat mask the other's silence."""
        post = _Recorder(202)
        env = _environ(tmp_path)  # the backup's variables only
        assert post_checkin("ok", {}, monitor=SCRAPE, environ=env, post=post) is False
        env |= {"WSLCB_SCRAPE_CHECKIN_BASE_URL": BASE, "WSLCB_SCRAPE_MONITOR_ID": "01SCRAPE"}
        assert post_checkin("ok", {}, monitor=SCRAPE, environ=env, post=post) is True
        assert post.calls[0][0] == f"{BASE}/api/v1/monitors/01SCRAPE/checkin"

    def test_log_lines_name_the_monitor(self, tmp_path, caplog):
        post = _Recorder()
        with caplog.at_level(logging.WARNING):
            post_checkin("ok", {}, monitor=SCRAPE, environ={}, post=post)
        assert "scrape check-in not configured" in caplog.text
        assert "a stopped scrape will not be noticed" in caplog.text
        assert "WSLCB_SCRAPE_MONITOR_ID" in caplog.text

    def test_metadata_is_sent_when_given(self, tmp_path):
        """``metadata.fault`` is the one key co-status reads: a new value is a new fault."""
        post = _Recorder(202)
        env = _environ(tmp_path)
        meta = {"fault": "HTTPStatusError 403"}
        assert post_checkin("alert", {}, monitor=BACKUP, environ=env, metadata=meta, post=post)
        assert post.calls[0][1] == {"status": "alert", "variables": {}, "metadata": meta}


def test_no_test_finds_a_configured_checkin():
    """conftest strips the check-in config, so no test can POST to co-status —
    not even one that runs a real command under a unit's environment."""
    import os

    from wslcb_licensing_tracker.checkin import CREDENTIALS_DIRECTORY_ENV

    monitors = (BACKUP, SCRAPE)
    names = [CREDENTIALS_DIRECTORY_ENV] + [
        n for m in monitors for n in (m.base_url_env, m.monitor_id_env)
    ]
    for name in names:
        assert name not in os.environ, name


def test_classify_monitor_uses_prefixed_names():
    """New variables take the #204 prefix; BACKUP and SCRAPE keep theirs until #204."""
    from wslcb_licensing_tracker.checkin import CLASSIFY
    from wslcb_licensing_tracker.config import ENV_PREFIX

    assert CLASSIFY.label == "classify"
    assert CLASSIFY.base_url_env == f"{ENV_PREFIX}CLASSIFY_CHECKIN_BASE_URL"
    assert CLASSIFY.monitor_id_env == f"{ENV_PREFIX}CLASSIFY_MONITOR_ID"
    assert not BACKUP.base_url_env.startswith(ENV_PREFIX)
    assert not SCRAPE.base_url_env.startswith(ENV_PREFIX)
