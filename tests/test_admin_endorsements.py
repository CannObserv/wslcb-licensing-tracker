"""Tests for admin substance management routes (/admin/endorsements/substances/*).

Uses FastAPI TestClient with the same cross-thread in-memory DB pattern as
test_admin_users.py.  Covers all three substance POST endpoints plus GET
section rendering and regression checks for existing tabs.
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
    """In-memory SQLite DB with cross-thread access for TestClient."""
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

def _seed_admin(db, email="admin@example.com"):
    db.execute(
        "INSERT INTO admin_users (email, role, created_by) VALUES (?, 'admin', 'test')",
        (email,),
    )
    db.commit()
    return email


def _seed_substance(db, name="Test Substance", display_order=99):
    """Insert a substance (or reuse existing) and return its id.

    Uses a unique default name to avoid clashing with the 'Cannabis' / 'Alcohol'
    rows seeded by migration 009.
    """
    db.execute(
        "INSERT OR IGNORE INTO regulated_substances (name, display_order) VALUES (?, ?)",
        (name, display_order),
    )
    db.commit()
    return db.execute(
        "SELECT id FROM regulated_substances WHERE name = ?", (name,)
    ).fetchone()[0]


def _seed_endorsement(db, name="CANNABIS RETAILER"):
    db.execute(
        "INSERT OR IGNORE INTO license_endorsements (name) VALUES (?)", (name,)
    )
    db.commit()
    return db.execute(
        "SELECT id FROM license_endorsements WHERE name = ?", (name,)
    ).fetchone()[0]


def _make_client(db, admin_email="admin@example.com"):
    """Return (client, patches) with auth and DB patched in.  Call _stop() after."""
    admin_data = {"id": 1, "email": admin_email, "role": "admin"}

    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)

    patches = (
        patch("admin_auth.get_db", return_value=ctx),
        patch("admin_auth._lookup_admin", return_value=admin_data),
        patch("admin_routes.get_db", return_value=ctx),
    )
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=True)
    client.headers["X-ExeDev-Email"] = admin_email
    client.headers["X-ExeDev-UserID"] = "uid-1"
    return client, patches


def _stop(patches):
    for p in patches:
        p.stop()


def _make_noauth_client(db):
    """Return (client, patches) with no admin in DB — auth will fail."""
    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)

    patches = (
        patch("admin_auth.get_db", return_value=ctx),
        patch("admin_auth._lookup_admin", return_value=None),
        patch("admin_routes.get_db", return_value=ctx),
    )
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=False)
    client.headers["X-ExeDev-Email"] = "notadmin@example.com"
    client.headers["X-ExeDev-UserID"] = "uid-9"
    return client, patches


# ---------------------------------------------------------------------------
# GET /admin/endorsements — default tab and section routing
# ---------------------------------------------------------------------------

class TestAdminEndorsementsGet:
    def test_default_tab_is_substances(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements", follow_redirects=True)
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "Regulated Substances" in resp.text

    def test_section_substances_renders(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=substances", follow_redirects=True)
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "Regulated Substances" in resp.text

    def test_section_endorsements_still_renders(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=endorsements", follow_redirects=True)
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "Endorsement List" in resp.text

    def test_section_suggestions_still_renders(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=suggestions", follow_redirects=True)
        finally:
            _stop(patches)
        assert resp.status_code == 200

    def test_section_codes_still_renders(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.get("/admin/endorsements?section=codes", follow_redirects=True)
        finally:
            _stop(patches)
        assert resp.status_code == 200
        assert "Code Mappings" in resp.text


# ---------------------------------------------------------------------------
# POST /admin/endorsements/substances/add
# ---------------------------------------------------------------------------

class TestSubstanceAdd:
    def test_inserts_substance_and_redirects(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/substances/add",
                data={"name": "Hemp"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "section=substances" in resp.headers["location"]
        assert "substance_added" in resp.headers["location"]
        row = db.execute(
            "SELECT name FROM regulated_substances WHERE name = 'Hemp'"
        ).fetchone()
        assert row is not None

    def test_audit_log_written(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            client.post(
                "/admin/endorsements/substances/add",
                data={"name": "Hemp"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'substance.add'"
        ).fetchone()
        assert row is not None

    def test_blank_name_redirects_with_error_flash(self, db):
        _seed_admin(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/substances/add",
                data={"name": "   "},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "substance_name_required" in resp.headers["location"]
        row = db.execute(
            "SELECT id FROM regulated_substances WHERE name = ''"
            " OR name = '   '"
        ).fetchone()
        assert row is None  # blank name not inserted

    def test_non_admin_forbidden(self, db):
        # No admin seeded — lookup returns None → redirect/403
        client, patches = _make_noauth_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/substances/add",
                data={"name": "Hemp"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)
        row = db.execute(
            "SELECT id FROM regulated_substances WHERE name = 'Hemp'"
        ).fetchone()
        assert row is None  # Hemp not inserted


# ---------------------------------------------------------------------------
# POST /admin/endorsements/substances/remove
# ---------------------------------------------------------------------------

class TestSubstanceRemove:
    def test_deletes_substance_and_redirects(self, db):
        _seed_admin(db)
        sid = _seed_substance(db)
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/substances/remove",
                data={"substance_id": str(sid)},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "substance_removed" in resp.headers["location"]
        row = db.execute(
            "SELECT id FROM regulated_substances WHERE id = ?", (sid,)
        ).fetchone()
        assert row is None

    def test_cascades_junction_rows(self, db):
        _seed_admin(db)
        sid = _seed_substance(db)
        eid = _seed_endorsement(db, "CANNABIS RETAILER")
        db.execute(
            "INSERT INTO regulated_substance_endorsements VALUES (?, ?)", (sid, eid)
        )
        db.commit()
        client, patches = _make_client(db)
        try:
            client.post(
                "/admin/endorsements/substances/remove",
                data={"substance_id": str(sid)},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        count = db.execute(
            "SELECT COUNT(*) FROM regulated_substance_endorsements WHERE substance_id = ?",
            (sid,),
        ).fetchone()[0]
        assert count == 0

    def test_audit_log_written(self, db):
        _seed_admin(db)
        sid = _seed_substance(db)
        client, patches = _make_client(db)
        try:
            client.post(
                "/admin/endorsements/substances/remove",
                data={"substance_id": str(sid)},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'substance.remove'"
        ).fetchone()
        assert row is not None

    def test_non_admin_forbidden(self, db):
        sid = _seed_substance(db)
        client, patches = _make_noauth_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/substances/remove",
                data={"substance_id": str(sid)},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)
        row = db.execute(
            "SELECT id FROM regulated_substances WHERE id = ?", (sid,)
        ).fetchone()
        assert row is not None  # not deleted


# ---------------------------------------------------------------------------
# POST /admin/endorsements/substances/set-endorsements
# ---------------------------------------------------------------------------

class TestSubstanceSetEndorsements:
    def test_replaces_associations_and_redirects(self, db):
        _seed_admin(db)
        sid = _seed_substance(db)
        eid1 = _seed_endorsement(db, "CANNABIS RETAILER")
        eid2 = _seed_endorsement(db, "CANNABIS PROCESSOR")
        db.execute(
            "INSERT INTO regulated_substance_endorsements VALUES (?, ?)", (sid, eid1)
        )
        db.commit()
        client, patches = _make_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/substances/set-endorsements",
                data={"substance_id": str(sid), "endorsement_ids": str(eid2)},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code == 303
        assert "substance_updated" in resp.headers["location"]
        rows = db.execute(
            "SELECT endorsement_id FROM regulated_substance_endorsements WHERE substance_id = ?",
            (sid,),
        ).fetchall()
        assert {r[0] for r in rows} == {eid2}

    def test_clears_all_when_no_ids_sent(self, db):
        _seed_admin(db)
        sid = _seed_substance(db)
        eid = _seed_endorsement(db, "CANNABIS RETAILER")
        db.execute(
            "INSERT INTO regulated_substance_endorsements VALUES (?, ?)", (sid, eid)
        )
        db.commit()
        client, patches = _make_client(db)
        try:
            client.post(
                "/admin/endorsements/substances/set-endorsements",
                data={"substance_id": str(sid)},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        count = db.execute(
            "SELECT COUNT(*) FROM regulated_substance_endorsements WHERE substance_id = ?",
            (sid,),
        ).fetchone()[0]
        assert count == 0

    def test_audit_log_written(self, db):
        _seed_admin(db)
        sid = _seed_substance(db)
        client, patches = _make_client(db)
        try:
            client.post(
                "/admin/endorsements/substances/set-endorsements",
                data={"substance_id": str(sid)},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'substance.set_endorsements'"
        ).fetchone()
        assert row is not None

    def test_non_admin_forbidden(self, db):
        sid = _seed_substance(db)
        eid = _seed_endorsement(db, "CANNABIS RETAILER")
        db.execute(
            "INSERT INTO regulated_substance_endorsements VALUES (?, ?)", (sid, eid)
        )
        db.commit()
        client, patches = _make_noauth_client(db)
        try:
            resp = client.post(
                "/admin/endorsements/substances/set-endorsements",
                data={"substance_id": str(sid), "endorsement_ids": ""},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)
        count = db.execute(
            "SELECT COUNT(*) FROM regulated_substance_endorsements WHERE substance_id = ?",
            (sid,),
        ).fetchone()[0]
        assert count == 1  # unchanged
