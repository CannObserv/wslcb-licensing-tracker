"""Tests for backup_checkin.py — the backup's dead-man check-in (#185).

Ported from CannObserv/watcher's ``tests/ops/test_checkin.py``. No network:
every test replaces the one POST seam.
"""

import logging

import httpx
import pytest

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
    def __init__(self, *codes):
        self.codes = list(codes) or [202]
        self.calls = []

    def __call__(self, url, payload, headers, timeout):
        self.calls.append((url, payload, headers))
        code = self.codes.pop(0)
        if isinstance(code, Exception):
            raise code
        return code


class TestPostCheckin:
    def test_ok_posts_to_the_monitor_with_the_key_header(self, tmp_path):
        """A configured check-in POSTs status + variables with X-API-Key."""
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder(202)
        assert post_checkin("ok", {"a": 1}, environ=_environ(tmp_path), post=post) is True
        url, payload, headers = post.calls[0]
        assert url == f"{BASE}/api/v1/monitors/{MONITOR}/checkin"
        assert payload == {"status": "ok", "variables": {"a": 1}}
        assert headers == {"X-API-Key": "sekrit"}

    def test_unconfigured_warns_and_posts_nothing(self, tmp_path, caplog):
        """No URL, no monitor, empty key: a WARNING, and no request."""
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder()
        env = _environ(tmp_path, base=None, monitor=None, key="")
        with caplog.at_level(logging.WARNING):
            assert post_checkin("ok", {}, environ=env, post=post) is False
        assert post.calls == []
        assert "not configured" in caplog.text

    def test_empty_key_file_is_half_configured(self, tmp_path, caplog):
        """URL and monitor set but the key file still empty: an ERROR."""
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder()
        with caplog.at_level(logging.ERROR):
            assert post_checkin("ok", {}, environ=_environ(tmp_path, key=""), post=post) is False
        assert post.calls == []
        assert "half-configured" in caplog.text

    def test_key_is_never_read_from_the_environment(self, tmp_path):
        """Without a credentials directory there is no key, whatever env says."""
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder()
        env = {
            "WSLCB_BACKUP_CHECKIN_BASE_URL": BASE,
            "WSLCB_BACKUP_MONITOR_ID": MONITOR,
            "WSLCB_BACKUP_CHECKIN_KEY": "from-env",
        }
        assert post_checkin("ok", {}, environ=env, post=post) is False
        assert post.calls == []

    def test_retries_once_on_5xx(self, tmp_path):
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder(503, 202)
        assert post_checkin("alert", {}, environ=_environ(tmp_path), post=post) is True
        assert len(post.calls) == 2

    def test_retries_once_on_transport_error(self, tmp_path):
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder(httpx.ConnectError("no route"), 202)
        assert post_checkin("ok", {}, environ=_environ(tmp_path), post=post) is True

    def test_4xx_is_not_retried(self, tmp_path):
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder(403, 202)
        assert post_checkin("ok", {}, environ=_environ(tmp_path), post=post) is False
        assert len(post.calls) == 1

    @pytest.mark.parametrize("base", ["status:9000", "ftp://status", "http://status:port"])
    def test_malformed_base_url_posts_nothing(self, tmp_path, base):
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder()
        assert post_checkin("ok", {}, environ=_environ(tmp_path, base=base), post=post) is False
        assert post.calls == []

    def test_monitor_id_must_be_a_bare_id(self, tmp_path):
        """The id becomes a path segment; a slash would re-aim the request."""
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        post = _Recorder()
        env = _environ(tmp_path, monitor="../../admin")
        assert post_checkin("ok", {}, environ=env, post=post) is False
        assert post.calls == []

    def test_never_raises(self, tmp_path, caplog):
        """Whatever escapes the checks is logged by type and reported as not landed."""
        from wslcb_licensing_tracker.backup_checkin import post_checkin

        def boom(*_args):
            raise ValueError("contains the key: sekrit")

        with caplog.at_level(logging.ERROR):
            assert post_checkin("ok", {}, environ=_environ(tmp_path), post=boom) is False
        assert "ValueError" in caplog.text
        assert "sekrit" not in caplog.text
