"""Tests for runtime configuration helpers (config.py)."""

import logging

from wslcb_licensing_tracker.config import (
    ENV_PREFIX,
    JEV_USD_PER_MILLION_INPUT_TOKENS,
    get_build_id,
    jev_confidence_threshold,
)


class TestGetBuildId:
    def setup_method(self):
        get_build_id.cache_clear()

    def teardown_method(self):
        get_build_id.cache_clear()

    def test_returns_env_value(self, monkeypatch):
        monkeypatch.setenv("BUILD_ID", "abc1234")
        assert get_build_id() == "abc1234"

    def test_defaults_to_dev_and_warns_when_unset(self, monkeypatch, caplog):
        monkeypatch.delenv("BUILD_ID", raising=False)
        with caplog.at_level(logging.WARNING):
            assert get_build_id() == "dev"
        assert any("BUILD_ID" in r.message for r in caplog.records)

    def test_empty_string_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("BUILD_ID", "")
        assert get_build_id() == "dev"

    def test_memoized_single_warning(self, monkeypatch, caplog):
        monkeypatch.delenv("BUILD_ID", raising=False)
        with caplog.at_level(logging.WARNING):
            get_build_id()
            get_build_id()
        warnings = [r for r in caplog.records if "BUILD_ID" in r.message]
        assert len(warnings) == 1


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
