"""Connection management and constants for the WSLCB licensing tracker.

This is the thin, stable base layer.  Schema creation and migrations
live in ``schema.py``; query helpers in ``queries.py``.
"""
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

# Source type constants (fixed IDs — must match seed data in schema.py)
SOURCE_TYPE_LIVE_SCRAPE = 1
SOURCE_TYPE_CO_ARCHIVE = 2
SOURCE_TYPE_INTERNET_ARCHIVE = 3
SOURCE_TYPE_CO_DIFF_ARCHIVE = 4
SOURCE_TYPE_MANUAL = 5

WSLCB_SOURCE_URL = "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp"

# All persistent data (DB + HTML snapshots) lives under DATA_DIR.
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent / "data"))
DB_PATH = DATA_DIR / "wslcb.db"


def get_connection(path: str | Path | None = None) -> sqlite3.Connection:
    """Open a connection to *path* (defaults to ``DB_PATH``).

    Passing ``":memory:"`` gives an ephemeral in-memory database —
    useful for tests.  When using the default on-disk path, the
    parent directory is created automatically.
    """
    effective = str(path) if path is not None else str(DB_PATH)
    if effective != ":memory:":
        Path(effective).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(effective)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db(path: str | Path | None = None):
    """Context manager wrapping :func:`get_connection`."""
    conn = get_connection(path)
    try:
        yield conn
    finally:
        conn.close()


def _normalize_raw_address(raw: str) -> str:
    """Normalize whitespace variants (NBSP → space) in raw address strings.

    The WSLCB source page sometimes uses non-breaking spaces (\\xa0)
    instead of regular spaces.  We normalize before lookup so that
    cosmetically-identical strings map to the same location row.
    """
    if not raw:
        return raw
    return re.sub(r'\xa0+', ' ', raw)
