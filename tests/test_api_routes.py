"""Tests for versioned API routes (/api/v1/).

Covers envelope structure, /cities, /stats, /export, and /health.
Uses FastAPI TestClient with an in-memory DB patched in; no disk DB,
no network calls.
"""
import sqlite3
from contextlib import contextmanager
from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from app import app


@contextmanager
def _broken_db():
    """Context manager that always raises, simulating a DB failure."""
    if False:  # pragma: no cover — satisfies contextmanager's yield requirement
        yield
    raise Exception("disk I/O error")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """Cross-thread in-memory DB for TestClient use."""
    from schema import init_db
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def client(db):
    """TestClient with get_db patched to return the in-memory DB."""
    from contextlib import contextmanager

    @contextmanager
    def _fake_db():
        yield db

    with patch("api_routes.get_db", _fake_db):
        yield TestClient(app)


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
        for key in ("total_records", "new_application_count", "approved_count",
                    "discontinued_count", "unique_businesses", "unique_licenses",
                    "unique_entities", "pipeline"):
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
        with patch("api_routes.get_db", _broken_db):
            resp = client.get("/api/v1/health")
        assert resp.status_code == 503

    def test_db_error_envelope(self, client):
        with patch("api_routes.get_db", _broken_db):
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
