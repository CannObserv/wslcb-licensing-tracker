"""Tests for main public routes (/, /search).

Covers UI consistency requirements such as shared placeholder text.
Uses FastAPI TestClient with the ``db`` fixture patched in; no disk DB.
"""
import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app import app

# The canonical placeholder that both search inputs must display.
SEARCH_PLACEHOLDER = "Search business name, license #, location, applicant..."


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """In-memory SQLite DB with cross-thread access (needed for TestClient)."""
    from database import init_db
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    yield conn
    conn.close()


_EMPTY_STATS = {
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


def _make_client(db):
    """Return a (client, patches) pair with the DB patched in."""
    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)

    patches = (
        patch("admin_auth.get_db", return_value=ctx),
        patch("admin_auth._lookup_admin", return_value=None),
        patch("app.get_db", return_value=ctx),
        patch("app.get_stats", return_value=_EMPTY_STATS),
    )
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=True)
    return client, patches


def _stop(patches):
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Dashboard section-order test (#46)
# ---------------------------------------------------------------------------

class TestDashboardSectionOrder:
    """Dashboard sections must appear in the prescribed order (#46).

    Order: Search bar → Stats Cards → Application Pipeline → Last Scrape.
    We detect each section by its HTML comment anchor and assert that each
    anchor's position in the response body is strictly less than the next.
    """

    # Canonical HTML comment anchors present in templates/index.html
    SEARCH = "<!-- Quick Search -->"
    STATS = "<!-- Stats Cards -->"
    PIPELINE = "<!-- Application Pipeline -->"
    LAST_SCRAPE = "<!-- Last Scrape Info -->"

    def _positions(self, html: str) -> dict:
        return {
            "search": html.index(self.SEARCH),
            "stats": html.index(self.STATS),
            "pipeline": html.index(self.PIPELINE),
            "last_scrape": html.index(self.LAST_SCRAPE),
        }

    def test_section_order(self, db):
        """Search → Stats → Application Pipeline → Last Scrape."""
        client, patches = _make_client(db)
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            pos = self._positions(resp.text)
            assert pos["search"] < pos["stats"], (
                "Search bar must appear before Stats Cards"
            )
            assert pos["stats"] < pos["pipeline"], (
                "Stats Cards must appear before Application Pipeline"
            )
            assert pos["pipeline"] < pos["last_scrape"], (
                "Application Pipeline must appear before Last Scrape"
            )
        finally:
            _stop(patches)


# ---------------------------------------------------------------------------
# Placeholder consistency tests (#45)
# ---------------------------------------------------------------------------

class TestSearchPlaceholder:
    """Both the Dashboard and the Search screen must show identical placeholder text."""

    def test_dashboard_search_placeholder(self, db):
        """The landing page (/) uses the canonical search placeholder."""
        client, patches = _make_client(db)
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            assert SEARCH_PLACEHOLDER in resp.text, (
                f"Dashboard placeholder mismatch.\n"
                f"Expected: {SEARCH_PLACEHOLDER!r}\n"
                f"Found in response: {[l for l in resp.text.splitlines() if 'placeholder' in l.lower()]}"
            )
        finally:
            _stop(patches)

    def test_search_screen_placeholder(self, db):
        """The search results page (/search) uses the canonical search placeholder."""
        client, patches = _make_client(db)
        try:
            resp = client.get("/search")
            assert resp.status_code == 200
            assert SEARCH_PLACEHOLDER in resp.text, (
                f"Search screen placeholder mismatch.\n"
                f"Expected: {SEARCH_PLACEHOLDER!r}\n"
                f"Found in response: {[l for l in resp.text.splitlines() if 'placeholder' in l.lower()]}"
            )
        finally:
            _stop(patches)
