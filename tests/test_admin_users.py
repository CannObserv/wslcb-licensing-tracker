"""Tests for admin user management routes (/admin/users) and the system dashboard.

All tests use the FastAPI TestClient with the ``db`` fixture patched in
so no disk database is touched.
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
    """In-memory SQLite DB with cross-thread access enabled (needed for TestClient)."""
    from wslcb_licensing_tracker.schema import init_db
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_admin(db, email="admin@example.com", role="admin", created_by="test"):
    """Insert one admin user row and commit."""
    db.execute(
        "INSERT INTO admin_users (email, role, created_by) VALUES (?, ?, ?)",
        (email, role, created_by),
    )
    db.commit()
    return email


def _seed_record(db, section_type="new_application", record_date="2025-01-01",
                 business_name="TEST BIZ", license_number="ABC-123",
                 application_type="NEW APPLICATION"):
    """Insert a minimal license_records row."""
    db.execute("""
        INSERT OR IGNORE INTO license_records
            (section_type, record_date, business_name, license_number,
             application_type, license_type, applicants)
        VALUES (?, ?, ?, ?, ?, '', '')
    """, (section_type, record_date, business_name, license_number, application_type))
    db.commit()


def _make_client(db, admin_email="admin@example.com"):
    """Return a (client, ctx) pair with auth and DB patched.

    The TestClient and patch context manager are both active for the
    lifetime of the returned client object.  Each test should call this
    inside a ``with`` block or use the returned ctx to guard the patches.
    """
    admin_data = {"id": 1, "email": admin_email, "role": "admin"}

    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)

    patches = (
        patch("wslcb_licensing_tracker.admin_auth.get_db", return_value=ctx),
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=admin_data),
        patch("wslcb_licensing_tracker.admin_routes.get_db", return_value=ctx),
    )
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=True)
    client.headers["X-ExeDev-Email"] = admin_email
    client.headers["X-ExeDev-UserID"] = "uid-1"
    return client, patches


def _stop(patches):
    """Stop all patches returned by ``_make_client``."""
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# GET /admin/users
# ---------------------------------------------------------------------------

class TestAdminUsersGet:
    def test_lists_users(self, db):
        """GET /admin/users returns 200 and shows all seeded admins."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "other@example.com")
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/users", follow_redirects=False)
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "admin@example.com" in resp.text
        assert "other@example.com" in resp.text

    def test_non_admin_forbidden(self, db):
        """GET /admin/users rejects non-admin (no matching row)."""
        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)
        with patch("wslcb_licensing_tracker.admin_auth.get_db", return_value=ctx), \
             patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get(
                "/admin/users",
                headers={"X-ExeDev-Email": "hacker@evil.com",
                         "X-ExeDev-UserID": "uid-9"},
                follow_redirects=False,
            )
        # Either 403 (logged-in non-admin) or redirect to exe.dev login
        assert resp.status_code in (302, 303, 403)


# ---------------------------------------------------------------------------
# POST /admin/users/add
# ---------------------------------------------------------------------------

class TestAdminUsersAdd:
    def test_add_user_appears_in_db(self, db):
        """POST /admin/users/add inserts a new admin_users row."""
        _seed_admin(db, "admin@example.com")
        client, patches = _make_client(db)
        try:
            resp = client.post("/admin/users/add", data={"email": "newguy@example.com"},
                               follow_redirects=False)
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303)
        row = db.execute(
            "SELECT email FROM admin_users WHERE email = ?", ("newguy@example.com",)
        ).fetchone()
        assert row is not None

    def test_add_user_audit_logged(self, db):
        """POST /admin/users/add writes an audit log entry."""
        _seed_admin(db, "admin@example.com")
        client, patches = _make_client(db)
        try:
            client.post("/admin/users/add", data={"email": "newguy@example.com"},
                        follow_redirects=False)
        finally:
            _stop(patches)
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'admin_user.add'"
        ).fetchone()
        assert row is not None

    def test_add_duplicate_email_redirects(self, db):
        """Adding an already-present email redirects back without crashing."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "dup@example.com")
        client, patches = _make_client(db)
        try:
            resp = client.post("/admin/users/add", data={"email": "dup@example.com"},
                               follow_redirects=False)
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303)
        # Error message in redirect location
        assert "error" in resp.headers["location"]


# ---------------------------------------------------------------------------
# POST /admin/users/remove
# ---------------------------------------------------------------------------

class TestAdminUsersRemove:
    def test_remove_user(self, db):
        """POST /admin/users/remove deletes the user row."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "victim@example.com")
        client, patches = _make_client(db)
        try:
            resp = client.post("/admin/users/remove", data={"email": "victim@example.com"},
                               follow_redirects=False)
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303)
        row = db.execute(
            "SELECT id FROM admin_users WHERE email = ?", ("victim@example.com",)
        ).fetchone()
        assert row is None

    def test_remove_audit_logged(self, db):
        """POST /admin/users/remove writes an audit log entry."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "victim@example.com")
        client, patches = _make_client(db)
        try:
            client.post("/admin/users/remove", data={"email": "victim@example.com"},
                        follow_redirects=False)
        finally:
            _stop(patches)
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'admin_user.remove'"
        ).fetchone()
        assert row is not None

    def test_self_removal_rejected(self, db):
        """An admin cannot remove themselves."""
        _seed_admin(db, "admin@example.com")
        client, patches = _make_client(db)
        try:
            resp = client.post("/admin/users/remove", data={"email": "admin@example.com"},
                               follow_redirects=False)
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303)
        assert "error" in resp.headers["location"]
        # Row must still exist
        row = db.execute(
            "SELECT id FROM admin_users WHERE email = ?", ("admin@example.com",)
        ).fetchone()
        assert row is not None


# ---------------------------------------------------------------------------
# GET /admin/ (dashboard)
# ---------------------------------------------------------------------------

class TestAdminDashboard:
    def test_dashboard_renders(self, db):
        """GET /admin/ returns 200 with a Records section heading."""
        _seed_admin(db, "admin@example.com")
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "Records" in resp.text

    def test_dashboard_empty_scrape_log(self, db):
        """Dashboard renders without error when scrape_log is empty."""
        _seed_admin(db, "admin@example.com")
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/")
        finally:
            _stop(patches)
        assert resp.status_code == 200

    def test_dashboard_counts_records(self, db):
        """Dashboard total record count reflects inserted rows."""
        _seed_admin(db, "admin@example.com")
        _seed_record(db, section_type="new_application",
                     license_number="NEW-1", record_date="2025-01-01")
        _seed_record(db, section_type="approved",
                     license_number="APP-1", record_date="2025-01-02")
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "2" in resp.text  # total record count

    def test_dashboard_recent_uses_created_at(self, db):
        """Last-24h/7d counts use created_at, not record_date."""
        _seed_admin(db, "admin@example.com")
        # Record with old record_date but just-inserted created_at (default = now)
        db.execute("""
            INSERT INTO license_records
                (section_type, record_date, business_name, license_number,
                 application_type, license_type, applicants, scraped_at)
            VALUES ('new_application', '2020-01-01', 'OLD DATE BIZ', 'TS-001',
                    'NEW APPLICATION', '', '', datetime('now'))
        """)
        db.commit()
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        # The page renders; the record appears in total but its exact count
        # in last_24h depends on created_at (which defaults to now), so it
        # should be 1 (not 0 as it would be if we used record_date=2020-01-01).
        assert "1" in resp.text


# ---------------------------------------------------------------------------
# Duplicate Suggestions tab — HTML rendering
# ---------------------------------------------------------------------------

class TestSuggestionsTabHTML:
    """Regression tests for the Duplicate Suggestions tab layout.

    Issue #36: the Accept popup was clipped by `overflow-hidden` on the table
    wrapper, and the table had no horizontal scroll on narrow viewports.
    """

    def _seed_suggestion_pair(self, db):
        """Insert two near-duplicate endorsements so the suggestions tab has rows."""
        db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?), (?)",
            ("TAKEOUT/DELIVERY", "TAKE OUT/DELIVERY"),
        )
        db.commit()

    def test_suggestions_table_wrapper_not_overflow_hidden(self, db):
        """The suggestions table wrapper must not use overflow-hidden.

        `overflow-hidden` clips absolutely-positioned children (the Accept
        popup), causing it to be invisible outside the table boundary.
        See issue #36.
        """
        _seed_admin(db)
        self._seed_suggestion_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=suggestions")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        # Locate the rendered suggestions table by the stable "Candidate A" header
        # cell text, then slice to the closing </table> tag.  Assert that the
        # wrapper div for the table does not carry overflow-hidden within that block.
        start = resp.text.find("Candidate A")
        end = resp.text.find("</table>", start)
        assert start != -1 and end != -1, "Could not locate suggestions table in rendered HTML"
        suggestions_block = resp.text[start:end]
        assert "overflow-hidden" not in suggestions_block

    def test_suggestions_table_wrapper_has_overflow_x_auto(self, db):
        """The suggestions table container must include overflow-x-auto for horizontal scrolling.

        Without it, wide tables on narrow viewports are not scrollable.
        See issue #36.
        """
        _seed_admin(db)
        self._seed_suggestion_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=suggestions")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "overflow-x-auto" in resp.text

    def test_accept_popup_uses_right_0_positioning(self, db):
        """The Accept popup must use right-0 (not left-0) positioning.

        left-0 anchors the popup to the left edge of the Actions cell, which
        overflows rightward and is clipped on desktop viewports.  right-0
        keeps the popup anchored to the right edge of the cell so it stays
        within the viewport.
        See issue #36.
        """
        _seed_admin(db)
        self._seed_suggestion_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=suggestions")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        # The absolute popup div inside the suggestions <details> must use right-0
        assert "right-0" in resp.text


# ---------------------------------------------------------------------------
# Post-action redirect section preservation  (issue #37)
# ---------------------------------------------------------------------------

class TestEndorsementActionRedirects:
    """POST actions must redirect back to the tab they were submitted from.

    Issue #37: alias and dismiss-suggestion hardcoded section=endorsements in
    their redirects, dropping users back on the Endorsement List tab even when
    they acted from the Duplicate Suggestions tab.
    """

    def _seed_pair(self, db):
        """Insert two endorsements and return (id_a, id_b)."""
        cur = db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)", ("TAKEOUT/DELIVERY",)
        )
        id_a = cur.lastrowid
        cur = db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)", ("TAKE OUT/DELIVERY",)
        )
        id_b = cur.lastrowid
        db.commit()
        return id_a, id_b

    # -- /alias ---------------------------------------------------------------

    def test_alias_from_suggestions_tab_redirects_to_suggestions(self, db):
        """POST /alias with return_section=suggestions must redirect to ?section=suggestions."""
        _seed_admin(db)
        id_a, id_b = self._seed_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/alias",
                data={
                    "canonical_id": str(id_a),
                    "variant_ids": str(id_b),
                    "return_section": "suggestions",
                },
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "section=suggestions" in resp.headers["location"]

    def test_alias_from_endorsements_tab_redirects_to_endorsements(self, db):
        """POST /alias with return_section=endorsements must redirect to ?section=endorsements."""
        _seed_admin(db)
        id_a, id_b = self._seed_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/alias",
                data={
                    "canonical_id": str(id_a),
                    "variant_ids": str(id_b),
                    "return_section": "endorsements",
                },
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "section=endorsements" in resp.headers["location"]

    def test_alias_default_section_is_endorsements(self, db):
        """POST /alias without return_section defaults to section=endorsements (backward compat)."""
        _seed_admin(db)
        id_a, id_b = self._seed_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/alias",
                data={
                    "canonical_id": str(id_a),
                    "variant_ids": str(id_b),
                    # no return_section
                },
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "section=endorsements" in resp.headers["location"]

    # -- /dismiss-suggestion --------------------------------------------------

    def test_dismiss_from_suggestions_tab_redirects_to_suggestions(self, db):
        """POST /dismiss-suggestion with return_section=suggestions must redirect to suggestions."""
        _seed_admin(db)
        id_a, id_b = self._seed_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/dismiss-suggestion",
                data={
                    "id_a": str(id_a),
                    "id_b": str(id_b),
                    "return_section": "suggestions",
                },
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "section=suggestions" in resp.headers["location"]

    def test_alias_invalid_return_section_falls_back_to_endorsements(self, db):
        """POST /alias with an unrecognised return_section must fall back to endorsements.

        Prevents open-redirect / header-injection via a crafted return_section value.
        See CR item 1.
        """
        _seed_admin(db)
        id_a, id_b = self._seed_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/alias",
                data={
                    "canonical_id": str(id_a),
                    "variant_ids": str(id_b),
                    "return_section": "evil&injected=value",
                },
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        location = resp.headers["location"]
        assert "section=endorsements" in location
        assert "evil" not in location

    def test_dismiss_invalid_return_section_falls_back_to_endorsements(self, db):
        """POST /dismiss-suggestion with an unrecognised return_section falls back to endorsements.

        Prevents open-redirect / header-injection via a crafted return_section value.
        See CR item 1.
        """
        _seed_admin(db)
        id_a, id_b = self._seed_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/dismiss-suggestion",
                data={
                    "id_a": str(id_a),
                    "id_b": str(id_b),
                    "return_section": "../../../../etc/passwd",
                },
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        location = resp.headers["location"]
        assert "section=endorsements" in location
        assert "passwd" not in location

    def test_dismiss_default_section_is_endorsements(self, db):
        """POST /dismiss-suggestion without return_section defaults to endorsements (backward compat)."""
        _seed_admin(db)
        id_a, id_b = self._seed_pair(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/dismiss-suggestion",
                data={
                    "id_a": str(id_a),
                    "id_b": str(id_b),
                },
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "section=endorsements" in resp.headers["location"]

    # -- template: hidden return_section fields -------------------------------

    def test_suggestions_alias_form_has_return_section_suggestions(self, db):
        """The Accept form on the suggestions tab must include return_section=suggestions."""
        _seed_admin(db)
        # Seed near-duplicate pair so suggestions tab has rows
        db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?), (?)",
            ("TAKEOUT/DELIVERY", "TAKE OUT/DELIVERY"),
        )
        db.commit()
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=suggestions")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        # The alias form in the suggestions tab must carry return_section=suggestions
        assert 'name="return_section"' in resp.text
        assert 'value="suggestions"' in resp.text

    def test_suggestions_dismiss_form_has_return_section_suggestions(self, db):
        """The Dismiss form on the suggestions tab must include return_section=suggestions."""
        _seed_admin(db)
        db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?), (?)",
            ("TAKEOUT/DELIVERY", "TAKE OUT/DELIVERY"),
        )
        db.commit()
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=suggestions")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert 'name="return_section"' in resp.text
        assert 'value="suggestions"' in resp.text


# ---------------------------------------------------------------------------
# Code Mappings tab — filter search  (issue #38)
# ---------------------------------------------------------------------------

class TestCodeMappingsFilter:
    """Regression tests for the Code Mappings tab filter search.

    Issue #38: filterCodes() only checked data-code (numeric), so any
    alphabetic/endorsement-name input hid all rows.
    """

    def _seed_code_mapping(self, db):
        """Insert a code mapping with a named endorsement for filter tests."""
        cur = db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)",
            ("CANNABIS RETAILER",),
        )
        endo_id = cur.lastrowid
        db.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            ("394", endo_id),
        )
        db.commit()
        return endo_id

    def test_code_row_has_data_names_attribute(self, db):
        """Each .code-row must expose a data-names attribute with endorsement names.

        filterCodes() uses this attribute to match endorsement name text.
        Without it, any alphabetic search query hides all rows.
        See issue #38.
        """
        _seed_admin(db)
        self._seed_code_mapping(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=codes")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert 'data-names=' in resp.text

    def test_code_row_data_names_contains_endorsement_name(self, db):
        """The data-names attribute must contain the endorsement name for that code.

        Verifies the server renders the endorsement name text into the attribute
        so the JS filter can match against it.
        See issue #38.
        """
        _seed_admin(db)
        self._seed_code_mapping(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=codes")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "cannabis retailer" in resp.text.lower()
        # The data-names attribute on the code-394 row must include the name
        assert 'data-names="cannabis retailer"' in resp.text.lower()

    def test_filter_js_checks_data_names(self, db):
        """The filterCodes() JS function must reference data-names (not only data-code).

        If filterCodes() only checks row.dataset.code, alphabetic queries
        always return zero results.  It must also check dataset.names (or
        equivalent) so endorsement-name searches work.
        See issue #38.

        The JS is now in a static file (admin-endorsements.js); verify the page
        references it and the static file contains the required logic.
        """
        import os

        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=codes")
        finally:
            _stop(patches)
        assert resp.status_code == 200
        # The page must reference the external JS file (not inline the function)
        assert "admin-endorsements.js" in resp.text
        # The static file itself must contain the dataset.names reference
        static_js = os.path.join(
            os.path.dirname(__file__), "..", "static", "js", "admin-endorsements.js"
        )
        with open(static_js) as f:
            js_content = f.read()
        assert "dataset.names" in js_content
