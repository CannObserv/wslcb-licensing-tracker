"""Regression tests for structured logging (GH #162, backport of skills#81).

Pin two contracts:

1. The JSON formatter is single-sourced in ``build_json_formatter()`` and emits
   ``{level, logger, message, timestamp}`` — a bare ``JsonFormatter()`` would
   silently drop level/logger/timestamp (skills#69).
2. The uvicorn ``--log-config`` file (``log_config.json``) stays valid under
   ``logging.config.dictConfig`` and routes uvicorn's own loggers through that
   *same* factory, so access/error lines never regress to plain text alongside
   JSON app records.
"""

import importlib.resources
import json
import logging
import logging.config

from pythonjsonlogger.json import JsonFormatter

from wslcb_licensing_tracker import log_config
from wslcb_licensing_tracker.log_config import build_json_formatter

FACTORY_REF = "wslcb_licensing_tracker.log_config.build_json_formatter"


def _load_log_config() -> dict:
    return json.loads(
        importlib.resources.files("wslcb_licensing_tracker").joinpath("log_config.json").read_text()
    )


def test_build_json_formatter_includes_structured_fields():
    record = logging.LogRecord(
        name="wslcb_licensing_tracker.some.module",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    parsed = json.loads(build_json_formatter().format(record))
    assert parsed["message"] == "hello world"
    assert parsed["level"] == "WARNING"
    assert parsed["logger"] == "wslcb_licensing_tracker.some.module"
    assert "timestamp" in parsed


def test_shared_formatter_renders_uvicorn_access_record():
    """A uvicorn.access record formats to JSON with the same fields as app logs
    — the request line lands in ``message``, not a plain-text handler."""
    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='%s - "%s %s HTTP/%s" %d',
        args=("127.0.0.1:0", "GET", "/api/v1/health", "1.1", 200),
        exc_info=None,
    )
    parsed = json.loads(build_json_formatter().format(record))
    assert parsed["logger"] == "uvicorn.access"
    assert parsed["level"] == "INFO"
    assert parsed["message"] == '127.0.0.1:0 - "GET /api/v1/health HTTP/1.1" 200'
    assert "timestamp" in parsed


def test_uvicorn_log_config_is_valid_and_shares_formatter():
    """The uvicorn --log-config file wires uvicorn's loggers through the same
    factory as the app, and dictConfig accepts it (a malformed file would fail
    the service at boot, not in review)."""
    config = _load_log_config()

    # Single source of truth: the file builds its formatter from the factory,
    # not a duplicated fmt string.
    assert any(f.get("()") == FACTORY_REF for f in config["formatters"].values())
    # All three uvicorn loggers must be present, else they keep the plain default.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        assert name in config["loggers"]
        assert config["loggers"][name]["propagate"] is False

    names = ("", "uvicorn", "uvicorn.error", "uvicorn.access")
    saved = {
        n: (
            logging.getLogger(n).handlers[:],
            logging.getLogger(n).propagate,
            logging.getLogger(n).level,
        )
        for n in names
    }
    try:
        logging.config.dictConfig(config)  # raises on a malformed config
    finally:
        for n, (handlers, propagate, level) in saved.items():
            lg = logging.getLogger(n)
            lg.handlers, lg.propagate, lg.level = handlers, propagate, level


def test_setup_logging_non_tty_uses_shared_json_formatter(monkeypatch):
    """setup_logging()'s non-TTY (systemd) branch installs the shared JSON
    formatter — the same factory the log-config file references — so app records
    and uvicorn lines serialize identically."""
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    monkeypatch.setattr(log_config, "_configured", False)

    # setup_logging() replaces root's handlers AND reclaims the three uvicorn
    # loggers (clears handlers, sets propagate=True). Save/restore all four so
    # this test can't leak logger state into a later test (order-dependent flake).
    names = ("", "uvicorn", "uvicorn.access", "uvicorn.error")
    saved = {n: (logging.getLogger(n).handlers[:], logging.getLogger(n).propagate) for n in names}
    saved_level = logging.getLogger().level
    try:
        log_config.setup_logging()
        formatter = logging.getLogger().handlers[0].formatter
        assert isinstance(formatter, JsonFormatter)
        record = logging.LogRecord(
            name="wslcb_licensing_tracker.x",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="ok",
            args=(),
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert parsed["logger"] == "wslcb_licensing_tracker.x"
        assert parsed["level"] == "INFO"
        assert "timestamp" in parsed
    finally:
        for n, (handlers, propagate) in saved.items():
            lg = logging.getLogger(n)
            lg.handlers, lg.propagate = handlers, propagate
        logging.getLogger().level = saved_level
