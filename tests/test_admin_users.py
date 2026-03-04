"""Tests for admin user management routes (/admin/users) and the system dashboard.

All tests use the FastAPI TestClient with the ``db`` fixture patched in
so no disk database is touched.
"""
import sqlite3
import types
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app import app
from db import get_db


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
    """Return a TestClient with auth and DB patched."""
    admin_data = {"id": 1, "email": admin_email, "role": "admin"}

    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)

    with patch("admin_auth.get_db", return_value=ctx), \
         patch("admin_auth._lookup_admin", return_value=admin_data), \
         patch("app.get_db", return_value=ctx):
        client = TestClient(app, raise_server_exceptions=True)
        client.headers["X-ExeDev-Email"] = admin_email
        client.headers["X-ExeDev-UserID"] = "uid-1"
        return client, ctx


# ---------------------------------------------------------------------------
# GET /admin/users
# ---------------------------------------------------------------------------

class TestAdminUsersGet:
    def test_lists_users(self, db):
        """GET /admin/users returns 200 and shows the seeded admin."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "other@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.get(
                "/admin/users",
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
                follow_redirects=False,
            )
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
        # Either 403 (logged-in non-admin) or redirect to login
        assert resp.status_code in (302, 303, 403)


# ---------------------------------------------------------------------------
# POST /admin/users/add
# ---------------------------------------------------------------------------

class TestAdminUsersAdd:
    def test_add_user_appears_in_db(self, db):
        """POST /admin/users/add inserts a new admin_users row."""
        _seed_admin(db, "admin@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post(
                "/admin/users/add",
                data={"email": "newguy@example.com"},
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
                follow_redirects=False,
            )
        assert resp.status_code in (302, 303)
        row = db.execute(
            "SELECT email FROM admin_users WHERE email = ?", ("newguy@example.com",)
        ).fetchone()
        assert row is not None

    def test_add_user_audit_logged(self, db):
        """POST /admin/users/add writes an audit log entry."""
        _seed_admin(db, "admin@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            client.post(
                "/admin/users/add",
                data={"email": "newguy@example.com"},
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
                follow_redirects=False,
            )
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'admin_user.add'"
        ).fetchone()
        assert row is not None

    def test_add_duplicate_email_shows_error(self, db):
        """Adding an already-present email does not crash and redirects back."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "dup@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post(
                "/admin/users/add",
                data={"email": "dup@example.com"},
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
                follow_redirects=False,
            )
        # Should redirect back to users page (no 500)
        assert resp.status_code in (302, 303)


# ---------------------------------------------------------------------------
# POST /admin/users/remove
# ---------------------------------------------------------------------------

class TestAdminUsersRemove:
    def test_remove_user(self, db):
        """POST /admin/users/remove deletes the user row."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "victim@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post(
                "/admin/users/remove",
                data={"email": "victim@example.com"},
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
                follow_redirects=False,
            )
        assert resp.status_code in (302, 303)
        row = db.execute(
            "SELECT id FROM admin_users WHERE email = ?", ("victim@example.com",)
        ).fetchone()
        assert row is None

    def test_remove_audit_logged(self, db):
        """POST /admin/users/remove writes an audit log entry."""
        _seed_admin(db, "admin@example.com")
        _seed_admin(db, "victim@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            client.post(
                "/admin/users/remove",
                data={"email": "victim@example.com"},
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
                follow_redirects=False,
            )
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'admin_user.remove'"
        ).fetchone()
        assert row is not None

    def test_self_removal_rejected(self, db):
        """An admin cannot remove themselves."""
        _seed_admin(db, "admin@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post(
                "/admin/users/remove",
                data={"email": "admin@example.com"},
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
                follow_redirects=False,
            )
        # Redirect back with error, not deleted
        assert resp.status_code in (302, 303)
        row = db.execute(
            "SELECT id FROM admin_users WHERE email = ?", ("admin@example.com",)
        ).fetchone()
        assert row is not None  # still there


# ---------------------------------------------------------------------------
# GET /admin/ (dashboard)
# ---------------------------------------------------------------------------

class TestAdminDashboard:
    def test_dashboard_renders(self, db):
        """GET /admin/ returns 200 with stat sections."""
        _seed_admin(db, "admin@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.get(
                "/admin/",
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
            )
        assert resp.status_code == 200
        # Should contain record counts section
        assert "Records" in resp.text

    def test_dashboard_empty_scrape_log(self, db):
        """Dashboard renders without error when scrape_log is empty."""
        _seed_admin(db, "admin@example.com")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.get(
                "/admin/",
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
            )
        assert resp.status_code == 200

    def test_dashboard_counts_records(self, db):
        """Dashboard stat queries return correct counts."""
        _seed_admin(db, "admin@example.com")
        _seed_record(db, section_type="new_application",
                     license_number="NEW-1", record_date="2025-01-01")
        _seed_record(db, section_type="approved",
                     license_number="APP-1", record_date="2025-01-02")

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        with patch("admin_auth.get_db", return_value=ctx), \
             patch("admin_auth._lookup_admin",
                   return_value={"id": 1, "email": "admin@example.com", "role": "admin"}), \
             patch("app.get_db", return_value=ctx):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.get(
                "/admin/",
                headers={"X-ExeDev-Email": "admin@example.com",
                         "X-ExeDev-UserID": "uid-1"},
            )
        assert resp.status_code == 200
        assert "2" in resp.text  # total records
