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

_configured = False


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger for the application.

    Safe to call multiple times — subsequent calls are no-ops.

    Args:
        level: Minimum log level (default ``logging.INFO``).
    """
    global _configured
    if _configured:
        return

    root = logging.getLogger()
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

    # Reclaim uvicorn's loggers so they flow through our root handler.
    # Uvicorn's default dictConfig creates separate handlers on
    # 'uvicorn', 'uvicorn.access', and 'uvicorn.error' with
    # propagate=False.  Clearing those and re-enabling propagation
    # gives us consistent formatting (including JSON under systemd).
    for name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True

    _configured = True
