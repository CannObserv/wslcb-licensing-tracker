"""Tests for app-level configuration (Cache-Control headers, build_id global).

Covers non-route concerns such as static asset caching and Jinja2 globals
that are set up at module load time in app.py.
"""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_pg():
    """Prevent real DB connections during all tests in this module."""
    engine = MagicMock()
    engine.dispose = AsyncMock()
    with patch("wslcb_licensing_tracker.app.create_engine_from_env", return_value=engine), \
         patch("wslcb_licensing_tracker.app.run_pending_migrations", new_callable=AsyncMock):
        yield engine


# ---------------------------------------------------------------------------
# Cache-Control header test (#91)
# ---------------------------------------------------------------------------

def test_static_files_have_cache_control_header():
    """Static CSS/JS assets must have long-lived cache headers."""
    with TestClient(app) as client:
        resp = client.get("/static/css/tailwind.css")
        assert resp.status_code == 200
        assert "public" in resp.headers.get("cache-control", "")
        assert "max-age=31536000" in resp.headers.get("cache-control", "")


# ---------------------------------------------------------------------------
# Basic route tests
# ---------------------------------------------------------------------------

def _make_async_db_ctx(mock_conn: AsyncMock):
    """Return an async context manager that yields mock_conn."""
    @asynccontextmanager
    async def _ctx(engine):
        yield mock_conn
    return _ctx


def test_index_returns_200():
    """Index route must return 200 when DB returns empty stats."""
    mock_stats = {
        "total_records": 0,
        "new_application_count": 0,
        "approved_count": 0,
        "discontinued_count": 0,
        "date_range": (None, None),
        "unique_businesses": 0,
        "unique_licenses": 0,
        "unique_entities": 0,
        "last_scrape": None,
        "pipeline": {"total": 0, "pending": 0, "approved": 0, "discontinued": 0, "unknown": 0, "data_gap": 0},
    }
    mock_conn = AsyncMock()
    engine = MagicMock()
    engine.dispose = AsyncMock()

    async def mock_get_stats(conn):
        return mock_stats

    with patch("wslcb_licensing_tracker.app.create_engine_from_env", return_value=engine), \
         patch("wslcb_licensing_tracker.app.run_pending_migrations", new_callable=AsyncMock), \
         patch("wslcb_licensing_tracker.app.get_db", side_effect=_make_async_db_ctx(mock_conn)), \
         patch("wslcb_licensing_tracker.app.get_current_user", new_callable=AsyncMock, return_value=None), \
         patch("wslcb_licensing_tracker.app.get_stats", new=mock_get_stats):
        with TestClient(app) as client:
            resp = client.get("/")
    assert resp.status_code == 200


class TestBuildId:
    def test_build_id_jinja2_global_exists(self):
        """build_id must be set as a Jinja2 global."""
        from wslcb_licensing_tracker.app import templates
        assert "build_id" in templates.env.globals

    def test_build_id_is_string(self):
        """build_id Jinja2 global must be a string."""
        from wslcb_licensing_tracker.app import templates
        assert isinstance(templates.env.globals["build_id"], str)
        assert len(templates.env.globals["build_id"]) > 0

    def test_build_id_no_css_version_global(self):
        """css_version Jinja2 global must not exist (replaced by build_id)."""
        from wslcb_licensing_tracker.app import templates
        assert "css_version" not in templates.env.globals


def test_record_not_found_returns_404():
    """Unknown record_id must return 404."""
    mock_conn = AsyncMock()
    engine = MagicMock()
    engine.dispose = AsyncMock()

    async def mock_get_record_by_id(conn, record_id):
        return None

    with patch("wslcb_licensing_tracker.app.create_engine_from_env", return_value=engine), \
         patch("wslcb_licensing_tracker.app.run_pending_migrations", new_callable=AsyncMock), \
         patch("wslcb_licensing_tracker.app.get_db", side_effect=_make_async_db_ctx(mock_conn)), \
         patch("wslcb_licensing_tracker.app.get_current_user", new_callable=AsyncMock, return_value=None), \
         patch("wslcb_licensing_tracker.app.get_record_by_id", new=mock_get_record_by_id):
        with TestClient(app) as client:
            resp = client.get("/record/999999")
    assert resp.status_code == 404
