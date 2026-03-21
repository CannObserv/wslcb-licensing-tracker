"""Tests for the source viewer route GET /source/{source_id}/record/{record_id}.

Uses FastAPI TestClient with async pg_queries and SQLAlchemy Core queries mocked.
Parser extraction functions are also mocked so tests run without real snapshot files.
"""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_RECORD = {
    "id": 1,
    "section_type": "new_application",
    "record_date": "2025-06-15",
    "business_name": "ACME CANNABIS CO",
    "business_location": "123 MAIN ST, SEATTLE, WA 98101",
    "applicants": "ACME CANNABIS CO; JOHN DOE",
    "license_type": "CANNABIS RETAILER",
    "application_type": "NEW APPLICATION",
    "license_number": "078001",
    "contact_phone": "2065550100",
    "city": "SEATTLE",
    "state": "WA",
    "zip_code": "98101",
    "previous_business_name": "",
    "previous_applicants": "",
    "previous_business_location": "",
    "previous_city": "",
    "previous_state": "",
    "previous_zip_code": "",
    "location_id": None,
    "previous_location_id": None,
    "has_additional_names": 0,
    "resolved_endorsements": "",
    "endorsements": [],
    "entities": [],
    "outcome_status": None,
}

_SAMPLE_SOURCE = {
    "id": 1,
    "source_type": "live_scrape",
    "source_label": "Live Scrape",
    "snapshot_path": "path/live.html",
    "url": "https://example.com",
    "captured_at": "2025-06-15T12:00:00+00:00",
    "metadata": None,
}

_DIFF_SOURCE = {
    **_SAMPLE_SOURCE,
    "source_type": "co_diff_archive",
    "source_label": "CO Diff Archive",
    "snapshot_path": "path/diff.txt",
}


def _make_client(source_row=None, record=None, link_row=True):
    """Return (client, patches) with conn.execute mocked for source_viewer queries.

    source_row: mapping to return from source lookup (None → not found)
    record: dict to return from get_record_by_id (None → not found)
    link_row: truthy → link exists; falsy → not linked (404)
    """
    mock_conn = AsyncMock()
    engine = MagicMock()
    engine.dispose = AsyncMock()

    # Mock conn.execute to return appropriate results for each query
    source_mapping_result = MagicMock()
    source_mapping_result.mappings.return_value.one_or_none.return_value = source_row
    source_execute_result = source_mapping_result

    link_result = MagicMock()
    link_result.one_or_none.return_value = MagicMock() if link_row else None

    # conn.execute is called twice: once for source, once for link
    mock_conn.execute = AsyncMock(side_effect=[source_execute_result, link_result])

    async def _get_record_by_id(conn, record_id):
        return record

    @asynccontextmanager
    async def _db_ctx(eng):
        yield mock_conn

    patches = (
        patch("wslcb_licensing_tracker.app.create_engine_from_env", return_value=engine),
        patch("wslcb_licensing_tracker.app.run_pending_migrations", new_callable=AsyncMock),
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None),
        patch("wslcb_licensing_tracker.app.get_db", side_effect=_db_ctx),
        patch("wslcb_licensing_tracker.app.get_record_by_id", new=_get_record_by_id),
    )
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=True)
    return client, patches


def _stop(patches):
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSourceViewerRoute:
    def test_404_invalid_record_id(self):
        # source found, but record not found
        client, patches = _make_client(source_row=_SAMPLE_SOURCE, record=None)
        try:
            resp = client.get("/source/1/record/99999")
            assert resp.status_code == 404
        finally:
            _stop(patches)

    def test_404_invalid_source_id(self):
        # source not found
        client, patches = _make_client(source_row=None, record=_SAMPLE_RECORD)
        try:
            resp = client.get("/source/99999/record/1")
            assert resp.status_code == 404
        finally:
            _stop(patches)

    def test_404_no_record_source_link(self):
        """Valid IDs but no record_sources row -> 404."""
        client, patches = _make_client(
            source_row=_SAMPLE_SOURCE, record=_SAMPLE_RECORD, link_row=False
        )
        try:
            resp = client.get("/source/1/record/1")
            assert resp.status_code == 404
        finally:
            _stop(patches)

    def test_renders_iframe_when_tbody_found(self):
        """Returns 200 with iframe srcdoc when extractor returns HTML."""
        client, patches = _make_client(source_row=_SAMPLE_SOURCE, record=_SAMPLE_RECORD)
        try:
            with patch("wslcb_licensing_tracker.app.extract_tbody_from_snapshot",
                       return_value="<tbody><tr><td>Business Name:</td><td>ACME</td></tr></tbody>"):
                resp = client.get("/source/1/record/1")

            assert resp.status_code == 200
            assert "srcdoc" in resp.text
            assert "ACME" in resp.text
        finally:
            _stop(patches)

    def test_renders_not_found_message_when_extractor_returns_none(self):
        """Returns 200 with not-found notice when extractor returns None."""
        client, patches = _make_client(source_row=_SAMPLE_SOURCE, record=_SAMPLE_RECORD)
        try:
            with patch("wslcb_licensing_tracker.app.extract_tbody_from_snapshot", return_value=None):
                resp = client.get("/source/1/record/1")

            assert resp.status_code == 200
            assert "not found" in resp.text.lower() or "not available" in resp.text.lower()
        finally:
            _stop(patches)

    def test_diff_source_uses_diff_extractor(self):
        """co_diff_archive sources dispatch to extract_tbody_from_diff."""
        client, patches = _make_client(source_row=_DIFF_SOURCE, record=_SAMPLE_RECORD)
        try:
            with patch("wslcb_licensing_tracker.app.extract_tbody_from_diff",
                       return_value="<tbody><tr><td>License Number:</td><td>078001</td></tr></tbody>") as mock_diff:
                resp = client.get("/source/1/record/1")

            assert resp.status_code == 200
            mock_diff.assert_called_once()
            assert "srcdoc" in resp.text
        finally:
            _stop(patches)

    def test_anchor_tags_stripped_from_srcdoc(self):
        """Anchor tags in tbody HTML are stripped before embedding in srcdoc."""
        client, patches = _make_client(source_row=_SAMPLE_SOURCE, record=_SAMPLE_RECORD)
        try:
            anchored_tbody = (
                '<tbody><tr>'
                '<td>Business Name:</td>'
                '<td><a href="http://example.com">ACME CANNABIS CO</a></td>'
                '</tr></tbody>'
            )
            with patch("wslcb_licensing_tracker.app.extract_tbody_from_snapshot", return_value=anchored_tbody):
                resp = client.get("/source/1/record/1")

            assert resp.status_code == 200
            # Business name text must survive
            assert "ACME CANNABIS CO" in resp.text
            # The <a tag must not appear in the srcdoc content
            # (srcdoc value is HTML-escaped, so '<a' becomes '&lt;a')
            assert "&lt;a " not in resp.text and "&lt;a\n" not in resp.text
        finally:
            _stop(patches)
