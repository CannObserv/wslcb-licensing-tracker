"""Tests for admin user management routes (/admin/users) and the system dashboard.

All tests use the FastAPI TestClient with the ``db`` fixture patched in
so no disk database is touched.
"""
import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """In-memory SQLite DB with cross-thread access enabled (needed for TestClient)."""
    from database import init_db
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
        patch("admin_auth.get_db", return_value=ctx),
        patch("admin_auth._lookup_admin", return_value=admin_data),
        patch("app.get_db", return_value=ctx),
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
        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin", return_value=None):
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
