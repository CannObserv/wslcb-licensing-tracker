"""Tests for runtime configuration helpers (config.py)."""

import logging

from wslcb_licensing_tracker.config import get_build_id


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
