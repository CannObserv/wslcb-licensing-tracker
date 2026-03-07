"""Tests for admin_auth.py — authentication middleware and helpers."""
import os
import pytest
from unittest.mock import MagicMock, patch

from admin_auth import (
    get_current_user,
    require_admin,
    AdminRedirectException,
    _lookup_admin,
)


# ---- helpers -------------------------------------------------------

def _seed_admin(db, email="admin@example.com", role="admin"):
    """Insert one admin user row and return the email."""
    db.execute(
        "INSERT INTO admin_users (email, role, created_by) VALUES (?, ?, 'test')",
        (email, role),
    )
    db.commit()
    return email


def _make_request(headers: dict | None = None) -> MagicMock:
    """Build a mock FastAPI Request with the given headers.

    ``request.state`` uses a real ``SimpleNamespace`` so that
    ``hasattr`` / attribute assignment behave like the real Starlette State.
    """
    import types
    req = MagicMock()
    req.headers = headers or {}
    req.url.path = "/admin/"
    req.url.query = ""
    req.state = types.SimpleNamespace()
    return req


# ---- _lookup_admin -------------------------------------------------

def test_lookup_admin_returns_row(db):
    email = _seed_admin(db)
    with patch("admin_auth.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = lambda s: db
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        result = _lookup_admin(email)
    assert result is not None
    assert result["email"] == email
    assert result["role"] == "admin"


def test_lookup_admin_missing_returns_none(db):
    with patch("admin_auth.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = lambda s: db
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        result = _lookup_admin("nobody@example.com")
    assert result is None


def test_lookup_admin_case_insensitive(db):
    _seed_admin(db, email="Admin@Example.COM")
    with patch("admin_auth.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = lambda s: db
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        result = _lookup_admin("admin@example.com")
    assert result is not None
    assert result["email"].lower() == "admin@example.com"


# ---- get_current_user ----------------------------------------------

@pytest.mark.asyncio
async def test_get_current_user_with_valid_header(db):
    email = _seed_admin(db)
    req = _make_request({"X-ExeDev-Email": email, "X-ExeDev-UserID": "usr_1"})
    with patch("admin_auth.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = lambda s: db
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        result = await get_current_user(req)
    assert result is not None
    assert result["email"] == email
    assert result["user_id"] == "usr_1"


@pytest.mark.asyncio
async def test_get_current_user_no_header_returns_none(db):
    req = _make_request({})
    # ensure no env var set
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("ADMIN_DEV_EMAIL", None)
        with patch("admin_auth.get_db") as mock_get_db:
            mock_get_db.return_value.__enter__ = lambda s: db
            mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
            result = await get_current_user(req)
    assert result is None


@pytest.mark.asyncio
async def test_get_current_user_dev_email_fallback(db):
    email = _seed_admin(db)
    req = _make_request({})
    with patch.dict(os.environ, {"ADMIN_DEV_EMAIL": email, "ADMIN_DEV_USERID": "dev"}):
        with patch("admin_auth.get_db") as mock_get_db:
            mock_get_db.return_value.__enter__ = lambda s: db
            mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
            result = await get_current_user(req)
    assert result is not None
    assert result["email"] == email


@pytest.mark.asyncio
async def test_get_current_user_not_in_admin_table(db):
    req = _make_request({"X-ExeDev-Email": "stranger@example.com", "X-ExeDev-UserID": "usr_x"})
    with patch("admin_auth.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = lambda s: db
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        result = await get_current_user(req)
    assert result is None


# ---- require_admin -------------------------------------------------

@pytest.mark.asyncio
async def test_require_admin_valid(db):
    email = _seed_admin(db)
    req = _make_request({"X-ExeDev-Email": email, "X-ExeDev-UserID": "usr_1"})
    with patch("admin_auth.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = lambda s: db
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        result = await require_admin(req)
    assert result["email"] == email


@pytest.mark.asyncio
async def test_require_admin_no_credentials_raises_redirect(db):
    req = _make_request({})
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("ADMIN_DEV_EMAIL", None)
        with pytest.raises(AdminRedirectException) as exc_info:
            await require_admin(req)
    assert "/__exe.dev/login" in exc_info.value.location


@pytest.mark.asyncio
async def test_require_admin_not_in_table_raises_403(db):
    from fastapi import HTTPException
    req = _make_request({"X-ExeDev-Email": "stranger@example.com", "X-ExeDev-UserID": "usr_x"})
    with patch("admin_auth.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = lambda s: db
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        with pytest.raises(HTTPException) as exc_info:
            await require_admin(req)
    assert exc_info.value.status_code == 403


# ---- CLI admin commands -------------------------------------------

def _patch_db(db):
    """Context manager that patches db.get_db (via cli) to use the in-memory db."""
    mock = MagicMock()
    mock.__enter__ = lambda s: db
    mock.__exit__ = MagicMock(return_value=False)
    return patch("db.get_db", return_value=mock)


def test_cli_add_and_list_and_remove_users(db):
    """Round-trip: add-user, list-users, remove-user via CLI command functions."""
    from cli import cmd_admin_add_user, cmd_admin_list_users, cmd_admin_remove_user
    import types

    with _patch_db(db):
        # add first user
        cmd_admin_add_user(types.SimpleNamespace(email="first@example.com"))
        row = db.execute("SELECT email FROM admin_users WHERE email = 'first@example.com'").fetchone()
        assert row is not None

        # add second user
        cmd_admin_add_user(types.SimpleNamespace(email="second@example.com"))

        # list (no assertion on stdout, just check it doesn't crash)
        cmd_admin_list_users(types.SimpleNamespace())

        # remove first user succeeds (second still exists)
        cmd_admin_remove_user(types.SimpleNamespace(email="first@example.com"))
        row = db.execute("SELECT email FROM admin_users WHERE email = 'first@example.com'").fetchone()
        assert row is None


def test_cli_remove_last_user_exits(db):
    """Removing the only admin user should exit with error."""
    from cli import cmd_admin_add_user, cmd_admin_remove_user
    import types

    with _patch_db(db):
        cmd_admin_add_user(types.SimpleNamespace(email="solo@example.com"))

        with pytest.raises(SystemExit):
            cmd_admin_remove_user(types.SimpleNamespace(email="solo@example.com"))


def test_cli_add_duplicate_user_is_noop(db):
    """Adding an already-existing email is a no-op (no error, no duplicate row)."""
    from cli import cmd_admin_add_user
    import types

    with _patch_db(db):
        cmd_admin_add_user(types.SimpleNamespace(email="dup@example.com"))
        cmd_admin_add_user(types.SimpleNamespace(email="dup@example.com"))  # no-op

    count = db.execute("SELECT COUNT(*) FROM admin_users WHERE email = 'dup@example.com'").fetchone()[0]
    assert count == 1
