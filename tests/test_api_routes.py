"""Tests for versioned API routes (/api/v1/).

Covers envelope structure, /cities, /stats, /export, and /health.
Uses FastAPI TestClient with async dependency overrides; no disk DB,
no network calls.
"""
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app
from wslcb_licensing_tracker.api_routes import _get_db


# ---------------------------------------------------------------------------
# Async helpers
# ---------------------------------------------------------------------------


async def _async_empty_gen() -> AsyncGenerator[dict, None]:
    """Async generator that yields nothing (empty export cursor)."""
    return
    yield  # make it a generator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conn():
    """AsyncMock connection with execute returning a mock result."""
    conn = AsyncMock()
    conn.execute.return_value = MagicMock()
    return conn


@pytest.fixture
def mock_engine():
    """A mock AsyncEngine that can be stored on app.state."""
    engine = MagicMock()
    engine.dispose = AsyncMock()
    return engine


@pytest.fixture
def client(mock_conn, mock_engine):
    """TestClient with _get_db dependency overridden to yield mock_conn.

    Sets app.state.engine directly so routes that access it without going
    through the dependency (health, export) work without running the lifespan.
    """

    async def _fake_get_db():
        yield mock_conn

    app.dependency_overrides[_get_db] = _fake_get_db

    @asynccontextmanager
    async def _healthy_db_ctx(engine):
        conn = AsyncMock()
        yield conn

    # Patch pg_queries functions used by the routes.
    # app.get_current_user must also be patched: ADMIN_DEV_EMAIL in the environment
    # causes _lookup_admin() to be called from the 404 exception handler (_tpl)
    # via get_current_user, which hits the mock engine and produces a coroutine error.
    patches = (
        patch("wslcb_licensing_tracker.api_routes.get_cities_for_state", new_callable=AsyncMock),
        patch("wslcb_licensing_tracker.api_routes.get_stats", new_callable=AsyncMock),
        patch("wslcb_licensing_tracker.api_routes.export_records_cursor"),
        patch("wslcb_licensing_tracker.api_routes.get_db"),
        patch("wslcb_licensing_tracker.app.get_current_user", new_callable=AsyncMock),
    )
    mock_cities, mock_stats, mock_export, mock_get_db, mock_get_current_user = [p.start() for p in patches]
    mock_get_current_user.return_value = None

    mock_cities.return_value = ["SEATTLE", "TACOMA"]
    mock_stats.return_value = {
        "total_records": 0,
        "new_application_count": 0,
        "approved_count": 0,
        "discontinued_count": 0,
        "unique_businesses": 0,
        "unique_licenses": 0,
        "unique_entities": 0,
        "pipeline": {},
        "date_range": None,
        "last_scrape": None,
    }
    mock_export.return_value = _async_empty_gen()
    mock_get_db.side_effect = _healthy_db_ctx

    # Set engine on app.state directly — no lifespan needed
    app.state.engine = mock_engine
    tc = TestClient(app, raise_server_exceptions=True)

    yield tc

    for p in patches:
        p.stop()
    app.dependency_overrides.pop(_get_db, None)
    # Clean up state
    if hasattr(app.state, "engine"):
        del app.state.engine


# ---------------------------------------------------------------------------
# Envelope helper
# ---------------------------------------------------------------------------


def _assert_envelope(body: dict, ok: bool = True):
    """Assert the standard response envelope fields are present."""
    assert "ok" in body
    assert "message" in body
    assert "data" in body
    assert isinstance(body["message"], str)
    assert body["ok"] is ok


# ---------------------------------------------------------------------------
# GET /api/v1/cities
# ---------------------------------------------------------------------------


class TestCitiesEndpoint:
    def test_valid_state_returns_envelope(self, client):
        resp = client.get("/api/v1/cities?state=WA")
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)
        assert isinstance(body["data"], list)

    def test_unknown_state_returns_empty_list(self, client):
        resp = client.get("/api/v1/cities?state=ZZ")
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)
        assert body["data"] == []

    def test_missing_state_returns_empty_list(self, client):
        resp = client.get("/api/v1/cities")
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)
        assert body["data"] == []

    def test_cache_control_header_present(self, client):
        resp = client.get("/api/v1/cities?state=WA")
        assert "cache-control" in resp.headers


# ---------------------------------------------------------------------------
# GET /api/v1/stats
# ---------------------------------------------------------------------------


class TestStatsEndpoint:
    def test_returns_envelope(self, client):
        resp = client.get("/api/v1/stats")
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)

    def test_data_contains_expected_keys(self, client):
        resp = client.get("/api/v1/stats")
        data = resp.json()["data"]
        for key in (
            "total_records",
            "new_application_count",
            "approved_count",
            "discontinued_count",
            "unique_businesses",
            "unique_licenses",
            "unique_entities",
            "pipeline",
        ):
            assert key in data, f"Missing key: {key}"

    def test_date_range_is_list_or_null(self, client):
        resp = client.get("/api/v1/stats")
        date_range = resp.json()["data"]["date_range"]
        assert date_range is None or isinstance(date_range, list)


# ---------------------------------------------------------------------------
# GET /api/v1/export
# ---------------------------------------------------------------------------


class TestExportEndpoint:
    def test_returns_csv_content_type(self, client):
        resp = client.get("/api/v1/export")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]

    def test_csv_has_header_row(self, client):
        resp = client.get("/api/v1/export")
        first_line = resp.text.splitlines()[0]
        assert "section_type" in first_line
        assert "license_number" in first_line

    def test_content_disposition_header(self, client):
        resp = client.get("/api/v1/export")
        assert "attachment" in resp.headers.get("content-disposition", "")


# ---------------------------------------------------------------------------
# GET /api/v1/health
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_healthy_returns_200(self, client):
        # client fixture sets up _healthy_db_ctx as default get_db
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_healthy_envelope(self, client):
        resp = client.get("/api/v1/health")
        body = resp.json()
        _assert_envelope(body, ok=True)

    def test_healthy_data_contains_db_ok(self, client):
        resp = client.get("/api/v1/health")
        assert resp.json()["data"]["db"] == "ok"

    def test_db_error_returns_503(self, client):
        with patch("wslcb_licensing_tracker.api_routes.get_db") as mock_get_db:

            @asynccontextmanager
            async def _broken_get_db_ctx(engine):
                raise Exception("disk I/O error")
                yield  # noqa: unreachable

            mock_get_db.side_effect = _broken_get_db_ctx
            resp = client.get("/api/v1/health")
        assert resp.status_code == 503

    def test_db_error_envelope(self, client):
        with patch("wslcb_licensing_tracker.api_routes.get_db") as mock_get_db:

            @asynccontextmanager
            async def _broken_get_db_ctx(engine):
                raise Exception("disk I/O error")
                yield  # noqa: unreachable

            mock_get_db.side_effect = _broken_get_db_ctx
            resp = client.get("/api/v1/health")
        body = resp.json()
        _assert_envelope(body, ok=False)
        assert body["data"]["db"] == "error"
        assert "detail" in body["data"]

    def test_no_auth_required(self):
        """Health endpoint must respond without any auth headers."""
        # Intentionally uses a bare TestClient with no DB patch so we prove
        # the endpoint returns a response with no credentials at all — not
        # that the DB call succeeds.  The real on-disk DB is used here: on
        # this VM it returns 200; on a fresh CI environment without
        # data/wslcb.db it returns 503.  Either is acceptable — the assertion
        # only rules out 401/403, which is the actual invariant under test.
        #
        # Patch lifespan engine creation + migrations to avoid real PG connection.
        _mock_engine = MagicMock()
        _mock_engine.dispose = AsyncMock()

        with (
            patch("wslcb_licensing_tracker.app.create_engine_from_env", return_value=_mock_engine),
            patch("wslcb_licensing_tracker.app.run_pending_migrations", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.api_routes.get_db") as mock_get_db,
        ):

            @asynccontextmanager
            async def _fail_ctx(engine):
                raise Exception("no real DB in test")
                yield  # noqa: unreachable

            mock_get_db.side_effect = _fail_ctx
            plain_client = TestClient(app)
            resp = plain_client.get("/api/v1/health")
        assert resp.status_code in (200, 503)


# ---------------------------------------------------------------------------
# Old paths removed
# ---------------------------------------------------------------------------


class TestOldPathsRemoved:
    """The unversioned /api/* and /export paths must no longer exist."""

    def test_api_cities_gone(self, client):
        resp = client.get("/api/cities?state=WA")
        assert resp.status_code == 404

    def test_api_stats_gone(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 404

    def test_export_gone(self, client):
        resp = client.get("/export")
        assert resp.status_code == 404
