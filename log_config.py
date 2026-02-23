"""Centralized logging configuration for WSLCB licensing tracker.

Call ``setup_logging()`` once at each entry point (app.py lifespan,
scraper.py main, backfill_snapshots.py main) before any work is done.

Behaviour:
- **TTY** (interactive terminal): human-readable format with timestamps.
- **Non-TTY** (systemd / pipe): JSON lines via *python-json-logger* for
  machine-parseable output that integrates cleanly with ``journalctl``.

All project modules should obtain their logger with::

    import logging
    logger = logging.getLogger(__name__)

and use ``logger.info()`` / ``logger.warning()`` / etc. instead of
``print()``.
"""
import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger for the application.

    Safe to call multiple times — subsequent calls are no-ops.

    Args:
        level: Minimum log level (default ``logging.INFO``).
    """
    root = logging.getLogger()

    # Guard against duplicate setup (e.g. tests, reimport)
    if getattr(root, "_wslcb_configured", False):
        return

    root.setLevel(level)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)

    if sys.stderr.isatty():
        # Human-readable for interactive use
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        # JSON lines for systemd journal / log collectors
        from pythonjsonlogger.json import JsonFormatter

        formatter = JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"},
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    handler.setFormatter(formatter)
    root.addHandler(handler)
    root._wslcb_configured = True  # type: ignore[attr-defined]
