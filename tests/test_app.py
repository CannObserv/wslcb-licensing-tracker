"""Tests for app-level configuration (Cache-Control headers, css_version global).

Covers non-route concerns such as static asset caching and Jinja2 globals
that are set up at module load time in app.py.
"""
import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """In-memory SQLite DB with cross-thread access enabled."""
    from wslcb_licensing_tracker.schema import init_db
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    yield conn
    conn.close()


def _make_client(db):
    """Return a (client, patches) pair with the DB patched in."""
    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)

    patches = (
        patch("wslcb_licensing_tracker.admin_auth.get_db", return_value=ctx),
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None),
        patch("wslcb_licensing_tracker.app.get_db", return_value=ctx),
        patch("wslcb_licensing_tracker.api_routes.get_db", return_value=ctx),
    )
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=True)
    return client, patches


def _stop(patches):
    """Stop all patches returned by ``_make_client``."""
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Cache-Control header test (#91)
# ---------------------------------------------------------------------------

def test_static_files_have_cache_control_header(db):
    """Static CSS/JS assets must have long-lived cache headers."""
    client, patches = _make_client(db)
    try:
        resp = client.get("/static/css/tailwind.css")
        assert resp.status_code == 200
        assert "public" in resp.headers.get("cache-control", "")
        assert "max-age=31536000" in resp.headers.get("cache-control", "")
    finally:
        _stop(patches)
