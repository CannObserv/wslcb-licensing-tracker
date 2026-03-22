"""Tests for main public routes (/, /search).

Covers UI consistency requirements such as shared placeholder text
and dashboard section ordering.
Uses FastAPI TestClient with async pg_queries functions mocked; no disk DB.
"""
import copy
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app

# The canonical placeholder that both search inputs must display.
SEARCH_PLACEHOLDER = "Search business name, license #, location, applicant..."


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

_EMPTY_FILTERS = {
    "section_types": [],
    "application_types": [],
    "endorsements": [],
    "states": [],
    "outcome_statuses": [],
    "regulated_substance": [],
}


def _async_db_ctx(mock_conn: AsyncMock):
    """Return an asynccontextmanager factory that yields mock_conn."""
    @asynccontextmanager
    async def _ctx(engine):
        yield mock_conn
    return _ctx


async def _async_empty_gen() -> AsyncGenerator[dict, None]:
    """Async generator that yields nothing (empty export cursor)."""
    return
    yield  # make it a generator


def _make_client(stats: dict | None = None, entity_result: dict | None = None):
    """Return a (client, patches) pair with async query functions mocked.

    ``stats`` defaults to a fresh copy of ``_EMPTY_STATS_TEMPLATE``;
    callers may pass a modified copy without affecting other tests.
    ``entity_result`` defaults to empty; pass ``{"entities": [...], "total": N}`` to
    populate the entities route.
    """
    if stats is None:
        stats = copy.copy(_EMPTY_STATS_TEMPLATE)
    if entity_result is None:
        entity_result = {"entities": [], "total": 0}

    mock_conn = AsyncMock()
    engine = MagicMock()
    engine.dispose = AsyncMock()

    async def _get_stats(conn):
        return stats

    async def _search_records(conn, **kwargs):
        return [], 0

    async def _get_filter_options(conn):
        return copy.copy(_EMPTY_FILTERS)

    async def _get_cities_for_state(conn, state):
        return []

    async def _get_entities(conn, **kwargs):
        return entity_result

    patches = (
        patch("wslcb_licensing_tracker.app.create_engine_from_env", return_value=engine),
        patch("wslcb_licensing_tracker.app.run_pending_migrations", new_callable=AsyncMock),
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None),
        patch("wslcb_licensing_tracker.app.get_db", side_effect=_async_db_ctx(mock_conn)),
        patch("wslcb_licensing_tracker.app.get_stats", new=_get_stats),
        patch("wslcb_licensing_tracker.app.search_records", new=_search_records),
        patch("wslcb_licensing_tracker.app.get_filter_options", new=_get_filter_options),
        patch("wslcb_licensing_tracker.app.get_cities_for_state", new=_get_cities_for_state),
        patch("wslcb_licensing_tracker.app.get_entities", new=_get_entities),
        patch("wslcb_licensing_tracker.api_routes.get_db", side_effect=_async_db_ctx(mock_conn)),
        patch("wslcb_licensing_tracker.api_routes.export_records_cursor", return_value=_async_empty_gen()),
        patch("wslcb_licensing_tracker.api_routes.get_cities_for_state", new=_get_cities_for_state),
        patch("wslcb_licensing_tracker.api_routes.get_stats", new=_get_stats),
    )
    for p in patches:
        p.start()

    # Set engine on app.state so routes can access it without running lifespan
    app.state.engine = engine
    client = TestClient(app, raise_server_exceptions=True)
    return client, patches


def _stop(patches):
    """Stop all patches returned by ``_make_client``."""
    for p in patches:
        p.stop()


def _html_section(html: str, start_comment: str, end_comment: str) -> str:
    """Return the HTML slice from *start_comment* up to (not including) *end_comment*.

    Both arguments must be present in *html*; raises ``ValueError`` otherwise.
    Used by layout tests to isolate a dashboard section by its HTML comment anchors.
    """
    start = html.index(start_comment)
    end   = html.index(end_comment, start)
    return html[start:end]


def _card_tag(section: str, label: str) -> str:
    """Return the opening tag of the card element that directly wraps *label*.

    Walks backwards from the label text:
    1. Skips the inner label element (``<div`` or ``<span``) that contains the text.
    2. Finds the outer card wrapper (the ``<a`` or ``<div`` immediately before it).
    Returns the full opening tag string up to and including ``>``.
    """
    label_pos         = section.index(label)
    # Step 1: find the inner label element opening tag
    inner_start       = section.rindex("<div", 0, label_pos)
    # Step 2: find the outer card wrapper — could be <a or <div
    a_pos   = section.rfind("<a ",   0, inner_start)
    div_pos = section.rfind("<div",  0, inner_start)
    card_start = max(a_pos, div_pos)
    card_end   = section.index(">", card_start)
    return section[card_start:card_end + 1]


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

    def test_section_order(self):
        """Search → Stats → Application Pipeline → Last Scrape."""
        client, patches = _make_client()
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

    def test_dashboard_search_placeholder(self):
        """The landing page (/) uses the canonical search placeholder."""
        client, patches = _make_client()
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

    def test_search_screen_placeholder(self):
        """The search results page (/search) uses the canonical search placeholder."""
        client, patches = _make_client()
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

    def test_form_has_flex_wrap(self):
        """The Quick Search form element must carry the flex-wrap class."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            assert 'flex-wrap' in resp.text, (
                "Quick Search form is missing 'flex-wrap'; "
                "the Search button will overflow on narrow mobile viewports."
            )
        finally:
            _stop(patches)

    def test_button_has_ml_auto(self):
        """The Search button must carry ml-auto so it sits at the right on a new line."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            form_html = _html_section(resp.text, "<!-- Quick Search -->",
                                      "<!-- Stats Cards -->")
            assert 'type="submit"' in form_html, "Submit button not found in Quick Search form"
            button_line_start = form_html.index('type="submit"')
            tag_start = form_html.rindex('<button', 0, button_line_start)
            tag_end   = form_html.index('>', button_line_start) + 1
            button_tag = form_html[tag_start:tag_end]
            assert 'ml-auto' in button_tag, (
                f"Search button tag is missing 'ml-auto'; "
                f"button will not right-align when it wraps to a new line.\n"
                f"Button tag: {button_tag!r}"
            )
        finally:
            _stop(patches)


# ---------------------------------------------------------------------------
# Stats Cards mobile 2-per-row layout (#48)
# ---------------------------------------------------------------------------

class TestStatCardsMobileLayout:
    """Stat card grids must use grid-cols-2 at mobile so cards appear 2-per-row (#48).

    Before this fix both grids used grid-cols-1 at mobile, making every card
    full-width.  The fix is to add ``flex-wrap`` to the form and ``ml-auto`` to
    the button so it drops to the next line and stays right-aligned on small screens.
    The Date Range card is the sole exception: it must span both columns at mobile
    (col-span-2) to accommodate its wider text, then revert to a single column at
    md+ (md:col-span-1).
    """

    def test_stats_cards_grid_has_grid_cols_2(self):
        """The Stats Cards outer grid must carry grid-cols-2 for mobile 2-per-row."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text, "<!-- Stats Cards -->",
                                    "<!-- Additional Stats -->")
            first_div_end = section.index(">", section.index("<div"))
            grid_div = section[section.index("<div"):first_div_end + 1]
            assert "grid-cols-2" in grid_div, (
                f"Stats Cards grid is missing 'grid-cols-2'; "
                f"cards will be full-width on mobile.\nGrid div: {grid_div!r}"
            )
        finally:
            _stop(patches)

    def test_additional_stats_grid_has_grid_cols_2(self):
        """The Additional Stats outer grid must carry grid-cols-2 for mobile 2-per-row."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text, "<!-- Additional Stats -->",
                                    "<!-- Application Pipeline -->")
            first_div_end = section.index(">", section.index("<div"))
            grid_div = section[section.index("<div"):first_div_end + 1]
            assert "grid-cols-2" in grid_div, (
                f"Additional Stats grid is missing 'grid-cols-2'; "
                f"cards will be full-width on mobile.\nGrid div: {grid_div!r}"
            )
        finally:
            _stop(patches)

    def test_date_range_card_col_span_2(self):
        """The Date Range card must have col-span-2 so it is full-width on mobile."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text, "<!-- Additional Stats -->",
                                    "<!-- Application Pipeline -->")
            card_div = _card_tag(section, "Date Range")
            assert "col-span-2" in card_div, (
                f"Date Range card is missing 'col-span-2'; "
                f"it will be half-width on mobile instead of full-width.\n"
                f"Card div: {card_div!r}"
            )
        finally:
            _stop(patches)

    def test_date_range_card_md_col_span_1(self):
        """The Date Range card must reset to md:col-span-1 at tablet/desktop."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text, "<!-- Additional Stats -->",
                                    "<!-- Application Pipeline -->")
            card_div = _card_tag(section, "Date Range")
            assert "md:col-span-1" in card_div, (
                f"Date Range card is missing 'md:col-span-1'; "
                f"it will span 2 columns on tablet/desktop as well.\n"
                f"Card div: {card_div!r}"
            )
        finally:
            _stop(patches)


# ---------------------------------------------------------------------------
# Stat cards as linked anchors (#49)
# ---------------------------------------------------------------------------

class TestStatCardLinks:
    """Primary stat cards must be <a> anchors linking to search results (#49).

    Each card in Stats Cards and most Additional Stats cards should be rendered
    as a block <a> element, matching the Application Outcomes pattern.
    Exceptions that must NOT be links:
    - Date Range — static informational text.
    - Unique Entities — pending /entities landing page (#50).
    """

    # (label_text, expected_href) for every linked Stats Card.
    STATS_CARD_LINKS = [
        ("Total Records",      "/search"),
        ("New Applications",   "/search?section_type=new_application"),
        ("Approved",           "/search?section_type=approved"),
        ("Discontinued",       "/search?section_type=discontinued"),
    ]

    # (label_text, expected_href) for linked Additional Stats cards.
    ADDITIONAL_CARD_LINKS = [
        ("Unique Businesses",  "/search"),
        ("Unique Licenses",    "/search"),
    ]

    def test_stats_cards_are_links(self):
        """Every Stats Card (Total Records / New Apps / Approved / Discontinued) must be an <a>."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text,
                                    "<!-- Stats Cards -->",
                                    "<!-- Additional Stats -->")
            for label, href in self.STATS_CARD_LINKS:
                tag = _card_tag(section, label)
                assert tag.startswith("<a "), (
                    f"Stats Card '{label}' outer wrapper is not an <a> tag.\n"
                    f"Tag: {tag!r}"
                )
                assert f'href="{href}"' in tag, (
                    f"Stats Card '{label}' has wrong or missing href.\n"
                    f"Expected href=\"{href}\"\nTag: {tag!r}"
                )
        finally:
            _stop(patches)

    def test_additional_stats_cards_are_links(self):
        """Unique Businesses and Unique Licenses cards must be <a> links."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text,
                                    "<!-- Additional Stats -->",
                                    "<!-- Application Pipeline -->")
            for label, href in self.ADDITIONAL_CARD_LINKS:
                tag = _card_tag(section, label)
                assert tag.startswith("<a "), (
                    f"Additional Stats card '{label}' outer wrapper is not an <a> tag.\n"
                    f"Tag: {tag!r}"
                )
                assert f'href="{href}"' in tag, (
                    f"Additional Stats card '{label}' has wrong or missing href.\n"
                    f"Expected href=\"{href}\"\nTag: {tag!r}"
                )
        finally:
            _stop(patches)

    def test_date_range_card_is_not_a_link(self):
        """The Date Range card carries static text and must not be wrapped in an <a>."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text,
                                    "<!-- Additional Stats -->",
                                    "<!-- Application Pipeline -->")
            tag = _card_tag(section, "Date Range")
            assert not tag.startswith("<a "), (
                f"Date Range card should NOT be a link but its outer wrapper is an <a>.\n"
                f"Tag: {tag!r}"
            )
        finally:
            _stop(patches)

    def test_unique_entities_card_links_to_entities(self):
        """Unique Entities card links to /entities (#50)."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text,
                                    "<!-- Additional Stats -->",
                                    "<!-- Application Pipeline -->")
            tag = _card_tag(section, "Unique Entities")
            assert tag.startswith("<a "), (
                f"Unique Entities card should be an <a> link to /entities.\n"
                f"Tag: {tag!r}"
            )
            assert 'href="/entities"' in tag, (
                f"Unique Entities card href should be /entities.\n"
                f"Tag: {tag!r}"
            )
        finally:
            _stop(patches)


class TestAdditionalNamesNotice:
    """Detail page shows the additional-names notice when has_additional_names=1."""

    def _make_record_dict(self, record_id, has_flag, applicants="NOTICE TEST LLC"):
        return {
            "id": record_id,
            "section_type": "new_application",
            "record_date": "2025-06-01",
            "business_name": "NOTICE TEST LLC",
            "business_location": "",
            "applicants": applicants,
            "license_type": "CANNABIS RETAILER",
            "application_type": "RENEWAL",
            "license_number": "NTF001",
            "contact_phone": "",
            "city": "",
            "state": "WA",
            "zip_code": "",
            "has_additional_names": 1 if has_flag else 0,
            "endorsements": [],
            "entities": [],
            "outcome_status": None,
            "previous_business_name": "",
            "previous_applicants": "",
            "previous_business_location": "",
            "previous_city": "",
            "previous_state": "",
            "previous_zip_code": "",
            "location_id": None,
            "previous_location_id": None,
            "resolved_endorsements": "",
        }

    def _make_client_for_record(self, record_dict):
        """Return a (client, patches) pair with get_record_by_id mocked."""
        mock_conn = AsyncMock()
        engine = MagicMock()
        engine.dispose = AsyncMock()

        async def _get_record_by_id(conn, record_id):
            return record_dict

        async def _get_related_records(conn, record):
            return []

        async def _hydrate_records(conn, rows):
            return rows

        async def _get_record_sources(conn, record_id):
            return []

        async def _get_record_link(conn, record_id):
            return None

        async def _get_reverse_link_info(conn, record):
            return None

        def _get_outcome_status(record, link):
            return {"status": None}

        @asynccontextmanager
        async def _db_ctx(eng):
            yield mock_conn

        patches = (
            patch("wslcb_licensing_tracker.app.create_engine_from_env", return_value=engine),
            patch("wslcb_licensing_tracker.app.run_pending_migrations", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None),
            patch("wslcb_licensing_tracker.app.get_db", side_effect=_db_ctx),
            patch("wslcb_licensing_tracker.app.get_record_by_id", new=_get_record_by_id),
            patch("wslcb_licensing_tracker.app.get_related_records", new=_get_related_records),
            patch("wslcb_licensing_tracker.app.hydrate_records", new=_hydrate_records),
            patch("wslcb_licensing_tracker.app.get_record_sources", new=_get_record_sources),
            patch("wslcb_licensing_tracker.app.get_record_link", new=_get_record_link),
            patch("wslcb_licensing_tracker.app.get_reverse_link_info", new=_get_reverse_link_info),
            patch("wslcb_licensing_tracker.app.get_outcome_status", new=_get_outcome_status),
        )
        for p in patches:
            p.start()

        # Set engine directly so routes can access app.state.engine without lifespan
        app.state.engine = engine
        client = TestClient(app, raise_server_exceptions=True)
        return client, patches

    def test_notice_shown_when_flag_is_set(self):
        record = self._make_record_dict(1, has_flag=True,
                                        applicants="NOTICE TEST LLC; JANE DOE; BOB SMITH")
        client, patches = self._make_client_for_record(record)
        try:
            resp = client.get("/record/1")
            assert resp.status_code == 200
            assert "additional entities may be on file" in resp.text
        finally:
            _stop(patches)

    def test_notice_absent_when_flag_not_set(self):
        record = self._make_record_dict(2, has_flag=False,
                                        applicants="NOTICE TEST LLC; JANE DOE; BOB SMITH")
        client, patches = self._make_client_for_record(record)
        try:
            resp = client.get("/record/2")
            assert resp.status_code == 200
            assert "additional entities may be on file" not in resp.text
        finally:
            _stop(patches)

    def test_notice_shown_when_flag_set_and_no_entities(self):
        """Notice still appears via fallback branch when entities list is empty."""
        record = self._make_record_dict(3, has_flag=True, applicants="NOTICE TEST LLC")
        client, patches = self._make_client_for_record(record)
        try:
            resp = client.get("/record/3")
            assert resp.status_code == 200
            assert "additional entities may be on file" in resp.text
        finally:
            _stop(patches)


class TestExportCsvRoute:
    """Tests for GET /api/v1/export — streaming CSV export."""

    def test_empty_export_returns_csv_with_header_only(self):
        """An export with no matching records returns a valid CSV with only the header row."""
        client, patches = _make_client()
        try:
            resp = client.get("/api/v1/export?section_type=approved")
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/csv")
            lines = [l for l in resp.text.splitlines() if l.strip()]
            assert len(lines) == 1  # header only
            assert "section_type" in lines[0]
            assert "record_date" in lines[0]
        finally:
            _stop(patches)

    def test_export_content_disposition(self):
        """The Content-Disposition header indicates a CSV attachment."""
        client, patches = _make_client()
        try:
            resp = client.get("/api/v1/export")
            assert "attachment" in resp.headers["content-disposition"]
            assert "wslcb_records.csv" in resp.headers["content-disposition"]
        finally:
            _stop(patches)


class TestEntitiesRoute:
    """Tests for GET /entities landing page."""

    _ALICE = {"id": 1, "name": "ALICE JONES", "entity_type": "person", "record_count": 1}
    _BOB = {"id": 2, "name": "BOB SMITH", "entity_type": "person", "record_count": 1}
    _ACME = {"id": 3, "name": "ACME HOLDINGS LLC", "entity_type": "organization", "record_count": 1}

    def test_entities_page_renders(self):
        """GET /entities returns 200 with entity list."""
        entity_result = {
            "entities": [self._ALICE, self._BOB, self._ACME],
            "total": 3,
        }
        client, patches = _make_client(entity_result=entity_result)
        try:
            resp = client.get("/entities")
            assert resp.status_code == 200
            assert "ALICE JONES" in resp.text
            assert "BOB SMITH" in resp.text
            assert "ACME HOLDINGS LLC" in resp.text
        finally:
            _stop(patches)

    def test_entities_page_has_title(self):
        """GET /entities page has an Entities heading."""
        client, patches = _make_client()
        try:
            resp = client.get("/entities")
            assert resp.status_code == 200
            assert "Entities" in resp.text
        finally:
            _stop(patches)

    def test_htmx_returns_partial(self):
        """HX-Request header returns partial HTML without base layout."""
        entity_result = {"entities": [self._ALICE], "total": 1}
        client, patches = _make_client(entity_result=entity_result)
        try:
            resp = client.get("/entities", headers={"HX-Request": "true"})
            assert resp.status_code == 200
            # Partial should not include the full page chrome
            assert "<html" not in resp.text
            assert "ALICE JONES" in resp.text
        finally:
            _stop(patches)

    def test_search_filter(self):
        """?q= passes q arg to get_entities; route returns whatever it returns."""
        entity_result = {"entities": [self._ALICE], "total": 1}
        client, patches = _make_client(entity_result=entity_result)
        try:
            resp = client.get("/entities?q=alice", headers={"HX-Request": "true"})
            assert resp.status_code == 200
            assert "ALICE JONES" in resp.text
        finally:
            _stop(patches)

    def test_type_filter(self):
        """?type=organization passes entity_type arg; route returns whatever it returns."""
        entity_result = {"entities": [self._ACME], "total": 1}
        client, patches = _make_client(entity_result=entity_result)
        try:
            resp = client.get("/entities?type=organization", headers={"HX-Request": "true"})
            assert resp.status_code == 200
            assert "ACME HOLDINGS LLC" in resp.text
        finally:
            _stop(patches)

    def test_entities_link_to_detail(self):
        """Each entity row links to /entity/{id}."""
        entity_result = {"entities": [self._ALICE], "total": 1}
        client, patches = _make_client(entity_result=entity_result)
        try:
            resp = client.get("/entities")
            assert resp.status_code == 200
            assert "/entity/" in resp.text
        finally:
            _stop(patches)


class TestDashboardEntitiesLink:
    """Dashboard Unique Entities card links to /entities after #50 is built."""

    def test_unique_entities_card_is_a_link(self):
        """Unique Entities stat card must be an <a> linking to /entities."""
        client, patches = _make_client()
        try:
            resp = client.get("/")
            assert resp.status_code == 200
            section = _html_section(resp.text,
                                    "<!-- Additional Stats -->",
                                    "<!-- Application Pipeline -->")
            tag = _card_tag(section, "Unique Entities")
            assert tag.startswith("<a "), (
                f"Unique Entities card should be an <a> link to /entities.\n"
                f"Tag: {tag!r}"
            )
            assert 'href="/entities"' in tag, (
                f"Unique Entities card href should be /entities.\n"
                f"Tag: {tag!r}"
            )
        finally:
            _stop(patches)
