"""Tests for main public routes (/, /search).

Covers UI consistency requirements such as shared placeholder text
and dashboard section ordering.
Uses FastAPI TestClient with the ``db`` fixture patched in; no disk DB.
"""
import copy
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
    """In-memory SQLite DB with cross-thread access enabled.

    The conftest ``db`` fixture uses ``get_connection(":memory:")`` which
    does not set ``check_same_thread=False``.  FastAPI's TestClient runs
    the app in a background thread, so we need that flag here — hence
    this local override rather than reusing the shared fixture.
    """
    from database import init_db
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

# Template for an empty stats dict matching the exact shape of get_stats().
# _make_client copies this per call so tests can safely mutate their copy
# without bleeding state into other tests.
_EMPTY_STATS_TEMPLATE = {
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


def _make_client(db, stats: dict | None = None):
    """Return a (client, patches) pair with the DB and stats patched in.

    ``stats`` defaults to a fresh copy of ``_EMPTY_STATS_TEMPLATE``;
    callers may pass a modified copy without affecting other tests.
    """
    if stats is None:
        stats = copy.copy(_EMPTY_STATS_TEMPLATE)

    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)

    patches = (
        patch("admin_auth.get_db", return_value=ctx),
        patch("admin_auth._lookup_admin", return_value=None),
        patch("app.get_db", return_value=ctx),
        patch("app.get_stats", return_value=stats),
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
# Dashboard section-order test (#46)
# ---------------------------------------------------------------------------

class TestDashboardSectionOrder:
    """Dashboard sections must appear in the prescribed order (#46).

    Order: Search bar → Stats Cards → Application Pipeline → Last Scrape.
    We detect each section by its HTML comment anchor and assert that each
    anchor's position in the response body is strictly less than the next.
    The comment anchors are unconditional in the template so they are always
    present regardless of whether the pipeline / last-scrape data is populated.
    """

    # Canonical HTML comment anchors present in templates/index.html
    SEARCH = "<!-- Quick Search -->"
    STATS = "<!-- Stats Cards -->"
    PIPELINE = "<!-- Application Pipeline -->"
    LAST_SCRAPE = "<!-- Last Scrape Info -->"

    def _positions(self, html: str) -> dict:
        """Return byte positions of each section anchor, with clear failures."""
        result = {}
        for key, anchor in [
            ("search",     self.SEARCH),
            ("stats",      self.STATS),
            ("pipeline",   self.PIPELINE),
            ("last_scrape", self.LAST_SCRAPE),
        ]:
            assert anchor in html, (
                f"Section anchor missing from dashboard HTML: {anchor!r}"
            )
            result[key] = html.index(anchor)
        return result

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


# ---------------------------------------------------------------------------
# Quick Search button wrapping (#47)
# ---------------------------------------------------------------------------

class TestQuickSearchButtonWrapping:
    """Quick Search form must allow the button to wrap on narrow viewports (#47).

    On mobile portrait the button was overflowing the card's right border
    because the form used ``flex`` without ``flex-wrap``.  The fix is to add
    ``flex-wrap`` to the form and ``ml-auto`` to the button so it drops to
    the next line and stays right-aligned on small screens.
    """

    def test_form_has_flex_wrap(self, db):
        """The Quick Search form element must carry the flex-wrap class."""
        client, patches = _make_client(db)
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            # The form tag must include flex-wrap so rows can break.
            assert 'flex-wrap' in resp.text, (
                "Quick Search form is missing 'flex-wrap'; "
                "the Search button will overflow on narrow mobile viewports."
            )
        finally:
            _stop(patches)

    def test_button_has_ml_auto(self, db):
        """The Search button must carry ml-auto so it sits at the right on a new line."""
        client, patches = _make_client(db)
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            # Locate the Quick Search section then check for ml-auto on its button.
            search_section_start = resp.text.index("<!-- Quick Search -->")
            # Find the submit button within the section (appears before the closing </form>)
            form_end = resp.text.index("</form>", search_section_start)
            form_html = resp.text[search_section_start:form_end]
            assert 'type="submit"' in form_html, "Submit button not found in Quick Search form"
            # The submit button line must contain ml-auto.
            button_line_start = form_html.index('type="submit"')
            # Walk back to find the opening <button tag
            tag_start = form_html.rindex('<button', 0, button_line_start)
            tag_end = form_html.index('>', button_line_start) + 1
            button_tag = form_html[tag_start:tag_end]
            assert 'ml-auto' in button_tag, (
                f"Search button tag is missing 'ml-auto'; "
                f"button will not right-align when it wraps to a new line.\n"
                f"Button tag: {button_tag!r}"
            )
        finally:
            _stop(patches)
