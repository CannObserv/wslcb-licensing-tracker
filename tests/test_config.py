"""Tests for runtime configuration helpers (config.py)."""

import importlib.metadata
import logging

import pytest

from wslcb_licensing_tracker import config
from wslcb_licensing_tracker.config import (
    BUILD_ID_ENV,
    ENV_PREFIX,
    JEV_USD_PER_MILLION_INPUT_TOKENS,
    LEGACY_ENV_NAMES,
    env,
    get_build_id,
    jev_confidence_threshold,
)


class TestGetBuildId:
    def setup_method(self):
        get_build_id.cache_clear()

    def teardown_method(self):
        get_build_id.cache_clear()

    def test_returns_env_value(self, monkeypatch):
        monkeypatch.setenv(BUILD_ID_ENV, "abc1234")
        assert get_build_id() == "abc1234"

    def test_defaults_to_dev_and_warns_when_unset(self, monkeypatch, caplog):
        monkeypatch.delenv(BUILD_ID_ENV, raising=False)
        with caplog.at_level(logging.WARNING):
            assert get_build_id() == "dev"
        assert any(BUILD_ID_ENV in r.message for r in caplog.records)

    def test_empty_string_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv(BUILD_ID_ENV, "")
        assert get_build_id() == "dev"

    def test_memoized_single_warning(self, monkeypatch, caplog):
        monkeypatch.delenv(BUILD_ID_ENV, raising=False)
        with caplog.at_level(logging.WARNING):
            get_build_id()
            get_build_id()
        warnings = [r for r in caplog.records if BUILD_ID_ENV in r.message]
        assert len(warnings) == 1


class TestEnv:
    """``config.env()``: the prefixed name, then the legacy one, with a one-time WARNING (#204)."""

    NEW = f"{ENV_PREFIX}DATABASE_URL"
    OLD = "DATABASE_URL"

    @pytest.fixture(autouse=True)
    def _fresh_warnings(self, monkeypatch):
        monkeypatch.setattr(config, "_DEPRECATION_WARNED", set())

    def _deprecations(self, caplog) -> list[logging.LogRecord]:
        return [r for r in caplog.records if "deprecated" in r.getMessage()]

    def test_new_name(self, caplog):
        with caplog.at_level(logging.WARNING):
            assert env(self.NEW, environ={self.NEW: "n"}) == "n"
        assert self._deprecations(caplog) == []

    def test_legacy_name_falls_back_and_warns_once(self, caplog):
        with caplog.at_level(logging.WARNING):
            assert env(self.NEW, environ={self.OLD: "o"}) == "o"
            assert env(self.NEW, environ={self.OLD: "o"}) == "o"
        (record,) = self._deprecations(caplog)
        assert record.levelno == logging.WARNING
        assert self.OLD in record.getMessage()
        assert self.NEW in record.getMessage()

    def test_never_logs_the_value(self, caplog):
        with caplog.at_level(logging.DEBUG):
            env(self.NEW, environ={self.OLD: "s3cret"})
            env(self.NEW, environ={self.NEW: "s3cret", self.OLD: "other"})
        assert "s3cret" not in caplog.text
        assert "other" not in caplog.text

    def test_new_wins_over_legacy_and_flags_the_leftover(self, caplog):
        with caplog.at_level(logging.WARNING):
            assert env(self.NEW, environ={self.NEW: "n", self.OLD: "o"}) == "n"
        (record,) = self._deprecations(caplog)
        assert "ignor" in record.getMessage()

    def test_a_present_but_empty_new_name_does_not_fall_back(self):
        assert env(self.NEW, environ={self.NEW: "", self.OLD: "o"}) == ""

    def test_default_when_neither_is_set(self):
        assert env(self.NEW, environ={}) is None
        assert env(self.NEW, "d", environ={}) == "d"

    def test_a_name_with_no_legacy_reads_plainly(self):
        name = f"{ENV_PREFIX}CONTACT_EMAIL"
        assert name not in LEGACY_ENV_NAMES
        assert env(name, environ={name: "x"}) == "x"
        assert env(name, environ={"CONTACT_EMAIL": "x"}) is None

    def test_reads_os_environ_by_default(self, monkeypatch):
        monkeypatch.delenv(self.NEW, raising=False)
        monkeypatch.setenv(self.OLD, "from-os")
        assert env(self.NEW) == "from-os"

    def test_wslcb_names_drop_the_redundant_wslcb(self):
        assert LEGACY_ENV_NAMES[f"{ENV_PREFIX}BACKUP_BUCKET"] == "WSLCB_BACKUP_BUCKET"
        assert LEGACY_ENV_NAMES[f"{ENV_PREFIX}RELAY_INBOX"] == "WSLCB_RELAY_INBOX"


class TestJevConfidenceThreshold:
    ENV = "CO_WSLCB_LICENSING_TRACKER_JEV_CONFIDENCE_THRESHOLD"

    def test_env_prefix(self):
        assert ENV_PREFIX == "CO_WSLCB_LICENSING_TRACKER_"
        assert self.ENV == ENV_PREFIX + "JEV_CONFIDENCE_THRESHOLD"

    def test_default_when_unset(self, monkeypatch):
        monkeypatch.delenv(self.ENV, raising=False)
        assert jev_confidence_threshold() == 0.7

    def test_valid_value_read_at_call_time(self, monkeypatch):
        monkeypatch.setenv(self.ENV, "0.85")
        assert jev_confidence_threshold() == 0.85
        monkeypatch.setenv(self.ENV, "1")
        assert jev_confidence_threshold() == 1.0

    def test_invalid_values_fall_back_with_warning(self, monkeypatch, caplog):
        for bad in ("abc", "1.5", "-0.1", "nan"):
            monkeypatch.setenv(self.ENV, bad)
            caplog.clear()
            with caplog.at_level(logging.WARNING):
                assert jev_confidence_threshold() == 0.7
            assert any(self.ENV in r.message for r in caplog.records), bad

    def test_price_constant(self):
        assert JEV_USD_PER_MILLION_INPUT_TOKENS == 0.042


# ── contact email + User-Agent (#243) ────────────────────────────────

_EMAIL_ENV = f"{ENV_PREFIX}CONTACT_EMAIL"
_VERSION = importlib.metadata.version("wslcb-licensing-tracker")
_URL = "https://github.com/CannObserv/wslcb-licensing-tracker"


class TestContactEmail:
    def test_env_name_follows_the_prefix(self):
        assert config.CONTACT_EMAIL_ENV == _EMAIL_ENV

    @pytest.mark.parametrize(
        "email",
        ["code@cannabis.observer", "a.b+tag@sub.example.org", "x_y-z@example.co"],
    )
    def test_valid(self, email):
        assert config.contact_email({_EMAIL_ENV: email}) == email

    def test_surrounding_whitespace_is_trimmed(self):
        assert config.contact_email({_EMAIL_ENV: "  code@cannabis.observer\n"}) == (
            "code@cannabis.observer"
        )

    def test_unset_is_an_error_naming_the_variable(self):
        with pytest.raises(config.ContactEmailError, match=_EMAIL_ENV):
            config.contact_email({})

    @pytest.mark.parametrize(
        "email",
        [
            "",
            "code",
            "code@",
            "@cannabis.observer",
            "code@localhost",
            "co de@cannabis.observer",
            "code@cannabis..observer",
            "code(x)@cannabis.observer",
            "code@cannabis.observer)",
            "co\\de@cannabis.observer",
            "code@cannabis.observer\r\nX-Injected: 1",
            "a@b@cannabis.observer",
        ],
    )
    def test_malformed_is_an_error(self, email):
        with pytest.raises(config.ContactEmailError, match=_EMAIL_ENV):
            config.contact_email({_EMAIL_ENV: email})


class TestUserAgent:
    def test_identifies_the_versioned_process_and_its_contact(self):
        ua = config.user_agent({_EMAIL_ENV: "code@cannabis.observer"})
        assert ua == (
            f"CannObserv-wslcb-licensing-tracker/{_VERSION} (+{_URL}; code@cannabis.observer)"
        )

    def test_strict_raises_without_a_contact(self):
        with pytest.raises(config.ContactEmailError):
            config.user_agent({})

    def test_fallback_drops_only_the_contact_and_logs_an_error(self, caplog):
        with caplog.at_level(logging.ERROR, logger="wslcb_licensing_tracker.config"):
            ua = config.user_agent_or_fallback({})
        assert ua == f"CannObserv-wslcb-licensing-tracker/{_VERSION} (+{_URL})"
        assert _EMAIL_ENV in caplog.text

    def test_fallback_is_the_full_string_when_configured(self):
        environ = {_EMAIL_ENV: "code@cannabis.observer"}
        assert config.user_agent_or_fallback(environ) == config.user_agent(environ)
