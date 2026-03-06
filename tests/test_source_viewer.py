"""Tests for the source viewer route GET /source/{source_id}/record/{record_id}.

Uses FastAPI TestClient with the cross-thread in-memory DB pattern.
Parser extraction functions are mocked so tests run without real snapshot files.
"""
import sqlite3
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """In-memory SQLite DB with cross-thread access."""
    from database import init_db
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def client(db):
    """TestClient with app.get_db patched to use the in-memory DB."""
    from unittest.mock import MagicMock
    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)
    with patch("app.get_db", return_value=ctx):
        yield TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_record(db, license_number="078001", section_type="new_application",
                   record_date="2025-06-15", application_type="NEW APPLICATION"):
    from queries import insert_record
    rec = {
        "section_type": section_type,
        "record_date": record_date,
        "business_name": "ACME CANNABIS CO",
        "business_location": "123 MAIN ST, SEATTLE, WA 98101",
        "applicants": "ACME CANNABIS CO; JOHN DOE",
        "license_type": "CANNABIS RETAILER",
        "application_type": application_type,
        "license_number": license_number,
        "contact_phone": "2065550100",
        "city": "SEATTLE", "state": "WA", "zip_code": "98101",
        "previous_business_name": "", "previous_applicants": "",
        "previous_business_location": "",
        "previous_city": "", "previous_state": "", "previous_zip_code": "",
        "scraped_at": "2025-06-15T12:00:00+00:00",
    }
    record_id, _ = insert_record(db, rec)
    db.commit()
    return record_id


def _insert_source(db, source_type_id, snapshot_path="path/snap.html",
                   captured_at="2025-06-15T12:00:00+00:00"):
    from database import get_or_create_source
    source_id = get_or_create_source(
        db, source_type_id,
        snapshot_path=snapshot_path,
        url="https://example.com",
        captured_at=captured_at,
    )
    db.commit()
    return source_id


def _link(db, record_id, source_id, role="confirmed"):
    from database import link_record_source
    link_record_source(db, record_id, source_id, role)
    db.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSourceViewerRoute:
    def test_404_invalid_record_id(self, client):
        resp = client.get("/source/1/record/99999")
        assert resp.status_code == 404

    def test_404_invalid_source_id(self, client, db):
        record_id = _insert_record(db)
        resp = client.get(f"/source/99999/record/{record_id}")
        assert resp.status_code == 404

    def test_404_no_record_source_link(self, client, db):
        """Valid IDs but no record_sources row -> 404."""
        from database import SOURCE_TYPE_LIVE_SCRAPE
        record_id = _insert_record(db)
        source_id = _insert_source(db, SOURCE_TYPE_LIVE_SCRAPE)
        # Intentionally do NOT link them
        resp = client.get(f"/source/{source_id}/record/{record_id}")
        assert resp.status_code == 404

    def test_renders_iframe_when_tbody_found(self, client, db):
        """Returns 200 with iframe srcdoc when extractor returns HTML."""
        from database import SOURCE_TYPE_LIVE_SCRAPE
        record_id = _insert_record(db)
        source_id = _insert_source(db, SOURCE_TYPE_LIVE_SCRAPE, snapshot_path="path/live.html")
        _link(db, record_id, source_id)

        with patch("app.extract_tbody_from_snapshot",
                   return_value="<tbody><tr><td>Business Name:</td><td>ACME</td></tr></tbody>"):
            resp = client.get(f"/source/{source_id}/record/{record_id}")

        assert resp.status_code == 200
        assert "srcdoc" in resp.text
        assert "ACME" in resp.text

    def test_renders_not_found_message_when_extractor_returns_none(self, client, db):
        """Returns 200 with not-found notice when extractor returns None."""
        from database import SOURCE_TYPE_CO_ARCHIVE
        record_id = _insert_record(db)
        source_id = _insert_source(db, SOURCE_TYPE_CO_ARCHIVE, snapshot_path="path/archive.html")
        _link(db, record_id, source_id)

        with patch("app.extract_tbody_from_snapshot", return_value=None):
            resp = client.get(f"/source/{source_id}/record/{record_id}")

        assert resp.status_code == 200
        assert "not found" in resp.text.lower() or "not available" in resp.text.lower()

    def test_diff_source_uses_diff_extractor(self, client, db):
        """co_diff_archive sources dispatch to extract_tbody_from_diff."""
        from database import SOURCE_TYPE_CO_DIFF_ARCHIVE
        record_id = _insert_record(db)
        source_id = _insert_source(db, SOURCE_TYPE_CO_DIFF_ARCHIVE, snapshot_path="path/diff.txt")
        _link(db, record_id, source_id)

        with patch("app.extract_tbody_from_diff",
                   return_value="<tbody><tr><td>License Number:</td><td>078001</td></tr></tbody>") as mock_diff:
            resp = client.get(f"/source/{source_id}/record/{record_id}")

        assert resp.status_code == 200
        mock_diff.assert_called_once()
        assert "srcdoc" in resp.text

    def test_anchor_tags_stripped_from_srcdoc(self, client, db):
        """Anchor tags in tbody HTML are stripped before embedding in srcdoc."""
        from database import SOURCE_TYPE_LIVE_SCRAPE
        record_id = _insert_record(db)
        source_id = _insert_source(db, SOURCE_TYPE_LIVE_SCRAPE, snapshot_path="path/live.html")
        _link(db, record_id, source_id)

        anchored_tbody = (
            '<tbody><tr>'
            '<td>Business Name:</td>'
            '<td><a href="http://example.com">ACME CANNABIS CO</a></td>'
            '</tr></tbody>'
        )
        with patch("app.extract_tbody_from_snapshot", return_value=anchored_tbody):
            resp = client.get(f"/source/{source_id}/record/{record_id}")

        assert resp.status_code == 200
        # Business name text must survive
        assert "ACME CANNABIS CO" in resp.text
        # The <a tag must not appear in the srcdoc content
        # (srcdoc value is HTML-escaped, so '<a' becomes '&lt;a')
        assert "&lt;a " not in resp.text and "&lt;a\n" not in resp.text
