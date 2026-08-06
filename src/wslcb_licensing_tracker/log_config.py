"""Centralized logging configuration for WSLCB licensing tracker.

Call ``setup_logging()`` once at each entry point (app.py lifespan,
scraper.py main, backfill_snapshots.py main) before any work is done.

Behaviour:
- **TTY** (interactive terminal): human-readable format with timestamps.
- **Non-TTY** (systemd / pipe): JSON lines via *python-json-logger* for
  machine-parseable output that integrates cleanly with ``journalctl``.

Under uvicorn, ``--log-config log_config.json`` configures the whole logging
tree at boot so uvicorn's own ``uvicorn`` / ``uvicorn.access`` / ``uvicorn.error``
loggers emit the same JSON schema from the first boot line onward — otherwise
they ship with ``propagate=False`` and plain-text handlers that never reach the
root logger, mixing formats in journald (GH #162). That file and this module
share one formatter via :func:`build_json_formatter`.

All project modules should obtain their logger with::

    import logging
    logger = logging.getLogger(__name__)

and use ``logger.info()`` / ``logger.warning()`` / etc. instead of
``print()``.
"""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter

_configured = False


def build_json_formatter() -> JsonFormatter:
    """The single JSON formatter definition for the whole process.

    Referenced by BOTH ``setup_logging()``'s non-TTY branch and the uvicorn
    ``--log-config`` file (``log_config.json``, via the dictConfig ``"()"``
    factory key), so app records and uvicorn's own access/error lines serialize
    with one identical schema — no drift, one place to change (GH #162).

    Keys must be named in the fmt: a bare ``JsonFormatter()`` defaults to
    ``"%(message)s"`` and emits records with no level, logger, or timestamp
    (skills#69). Produces ``{level, logger, message, timestamp}``.
    """
    return JsonFormatter(
        "%(levelname)s %(name)s %(message)s",
        timestamp=True,
        rename_fields={"levelname": "level", "name": "logger"},
    )


class ColorMessageFilter(logging.Filter):
    """Drop uvicorn's ``color_message`` extra before anything serializes it.

    uvicorn logs its lifecycle lines with an ANSI-coloured duplicate of the
    message attached as ``extra={"color_message": ...}``, for its own
    colour-aware default formatter. Every extra reaches the JSON payload, so
    sharing the formatter alone still leaks a ``color_message`` key full of
    escape bytes into journald (GH #163).

    Attached to the uvicorn *loggers* in ``log_config.json`` rather than to the
    stdout handler or the formatter's ``reserved_attrs``: stripping at the
    record itself is sink-independent, so a handler that builds its payload
    straight from ``record.__dict__`` (OpenTelemetry's ``LoggingHandler`` does,
    against a reserved list that doesn't cover ``color_message``) can't
    resurrect the field the day the sink changes.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Strip the extra if present. Never drops a record."""
        record.__dict__.pop("color_message", None)
        return True


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger for the application.

    Safe to call multiple times — subsequent calls are no-ops.

    Args:
        level: Minimum log level (default ``logging.INFO``).
    """
    global _configured  # noqa: PLW0603
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
        # JSON lines for systemd journal / log collectors — same factory the
        # uvicorn --log-config file references, so every line shares one schema.
        formatter = build_json_formatter()

    handler.setFormatter(formatter)
    # Replace rather than append: under the service, uvicorn's --log-config has
    # already installed a root handler at boot; appending here would double-emit
    # every app record.
    root.handlers = [handler]

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
