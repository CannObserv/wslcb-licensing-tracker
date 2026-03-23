"""Tests for admin_auth.py — authentication middleware and helpers."""

import os
import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wslcb_licensing_tracker.admin_auth import (
    AdminRedirectException,
    _lookup_admin,
    get_current_user,
    require_admin,
)

# ---- helpers -------------------------------------------------------


def _make_conn(row=None):
    """Build a mock AsyncConnection whose execute() returns *row* as fetchone()."""
    conn = AsyncMock()
    result = MagicMock()
    result.fetchone.return_value = row
    conn.execute.return_value = result
    return conn


def _make_request(headers: dict | None = None, conn=None) -> MagicMock:
    """Build a mock FastAPI Request with the given headers and a PG engine mock."""

    @asynccontextmanager
    async def _get_db(_engine):
        yield conn or AsyncMock()

    req = MagicMock()
    req.headers = headers or {}
    req.url.path = "/admin/"
    req.url.query = ""
    req.state = types.SimpleNamespace()
    req.app.state.engine = MagicMock()
    req._mock_get_db = _get_db
    return req


# ---- _lookup_admin -------------------------------------------------


@pytest.mark.asyncio
async def test_lookup_admin_returns_row():
    conn = _make_conn(row=(1, "admin@example.com", "admin"))
    req = _make_request(conn=conn)
    with patch("wslcb_licensing_tracker.admin_auth.get_db", req._mock_get_db):
        found = await _lookup_admin(req, "admin@example.com")
    assert found is not None
    assert found["email"] == "admin@example.com"
    assert found["role"] == "admin"


@pytest.mark.asyncio
async def test_lookup_admin_missing_returns_none():
    conn = _make_conn(row=None)
    req = _make_request(conn=conn)
    with patch("wslcb_licensing_tracker.admin_auth.get_db", req._mock_get_db):
        found = await _lookup_admin(req, "nobody@example.com")
    assert found is None


# ---- get_current_user ----------------------------------------------


@pytest.mark.asyncio
async def test_get_current_user_with_valid_header():
    conn = _make_conn(row=(1, "admin@example.com", "admin"))
    req = _make_request(
        headers={"X-ExeDev-Email": "admin@example.com", "X-ExeDev-UserID": "usr_1"}, conn=conn
    )
    with patch("wslcb_licensing_tracker.admin_auth.get_db", req._mock_get_db):
        result = await get_current_user(req)
    assert result is not None
    assert result["email"] == "admin@example.com"
    assert result["user_id"] == "usr_1"


@pytest.mark.asyncio
async def test_get_current_user_no_header_returns_none():
    req = _make_request(headers={})
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("ADMIN_DEV_EMAIL", None)
        result = await get_current_user(req)
    assert result is None


@pytest.mark.asyncio
async def test_get_current_user_dev_email_fallback():
    conn = _make_conn(row=(1, "admin@example.com", "admin"))
    req = _make_request(headers={}, conn=conn)
    with patch.dict(
        os.environ, {"ADMIN_DEV_EMAIL": "admin@example.com", "ADMIN_DEV_USERID": "dev"}
    ):
        with patch("wslcb_licensing_tracker.admin_auth.get_db", req._mock_get_db):
            result = await get_current_user(req)
    assert result is not None
    assert result["email"] == "admin@example.com"


@pytest.mark.asyncio
async def test_get_current_user_not_in_admin_table():
    conn = _make_conn(row=None)
    req = _make_request(
        headers={"X-ExeDev-Email": "stranger@example.com", "X-ExeDev-UserID": "usr_x"}, conn=conn
    )
    with patch("wslcb_licensing_tracker.admin_auth.get_db", req._mock_get_db):
        result = await get_current_user(req)
    assert result is None


# ---- require_admin -------------------------------------------------


@pytest.mark.asyncio
async def test_require_admin_valid():
    conn = _make_conn(row=(1, "admin@example.com", "admin"))
    req = _make_request(
        headers={"X-ExeDev-Email": "admin@example.com", "X-ExeDev-UserID": "usr_1"}, conn=conn
    )
    with patch("wslcb_licensing_tracker.admin_auth.get_db", req._mock_get_db):
        result = await require_admin(req)
    assert result["email"] == "admin@example.com"


@pytest.mark.asyncio
async def test_require_admin_no_credentials_raises_redirect():
    req = _make_request(headers={})
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("ADMIN_DEV_EMAIL", None)
        with pytest.raises(AdminRedirectException) as exc_info:
            await require_admin(req)
    assert "/__exe.dev/login" in exc_info.value.location


@pytest.mark.asyncio
async def test_require_admin_not_in_table_raises_403():
    from fastapi import HTTPException

    conn = _make_conn(row=None)
    req = _make_request(
        headers={"X-ExeDev-Email": "stranger@example.com", "X-ExeDev-UserID": "usr_x"}, conn=conn
    )
    with patch("wslcb_licensing_tracker.admin_auth.get_db", req._mock_get_db):
        with pytest.raises(HTTPException) as exc_info:
            await require_admin(req)
    assert exc_info.value.status_code == 403


# ---- CLI admin commands -------------------------------------------


def _make_async_get_db(conn):
    """Return an async context manager that yields *conn*."""

    @asynccontextmanager
    async def _get_db(_engine):
        yield conn

    return _get_db


def _make_execute_result(fetchone=None, fetchall=None, scalar_one=None):
    """Build a synchronous MagicMock result object for conn.execute() return value."""
    result = MagicMock()
    result.fetchone.return_value = fetchone
    result.fetchall.return_value = fetchall if fetchall is not None else []
    result.scalar_one.return_value = scalar_one
    return result


def test_cli_add_and_list_and_remove_users():
    """Round-trip: add-user, list-users, remove-user via CLI command functions."""
    from wslcb_licensing_tracker.cli import (
        cmd_admin_add_user,
        cmd_admin_list_users,
        cmd_admin_remove_user,
    )

    conn = AsyncMock()

    # cmd_admin_add_user: SELECT returns None (no existing user)
    conn.execute.return_value = _make_execute_result(fetchone=None)
    with (
        patch("wslcb_licensing_tracker.cli.create_engine_from_env", return_value=MagicMock()),
        patch("wslcb_licensing_tracker.cli.get_db", _make_async_get_db(conn)),
    ):
        cmd_admin_add_user(types.SimpleNamespace(email="first@example.com"))
    assert conn.commit.called

    # cmd_admin_list_users: fetchall returns empty list → prints "No admin users."
    conn.reset_mock()
    conn.execute.return_value = _make_execute_result(fetchall=[])
    with (
        patch("wslcb_licensing_tracker.cli.create_engine_from_env", return_value=MagicMock()),
        patch("wslcb_licensing_tracker.cli.get_db", _make_async_get_db(conn)),
    ):
        cmd_admin_list_users(types.SimpleNamespace())

    # cmd_admin_remove_user: fetchone returns a row (user exists), count = 2 (not last)
    conn.reset_mock()
    # execute is called three times: SELECT id, SELECT COUNT, DELETE
    results = [
        _make_execute_result(fetchone=MagicMock()),  # SELECT id
        _make_execute_result(scalar_one=2),  # SELECT COUNT
        _make_execute_result(),  # DELETE
    ]
    conn.execute.side_effect = results
    with (
        patch("wslcb_licensing_tracker.cli.create_engine_from_env", return_value=MagicMock()),
        patch("wslcb_licensing_tracker.cli.get_db", _make_async_get_db(conn)),
    ):
        cmd_admin_remove_user(types.SimpleNamespace(email="first@example.com"))
    assert conn.commit.called


def test_cli_remove_last_user_exits():
    """Removing the only admin user should exit with error."""
    from wslcb_licensing_tracker.cli import cmd_admin_remove_user

    conn = AsyncMock()
    results = [
        _make_execute_result(fetchone=MagicMock()),  # SELECT id → found
        _make_execute_result(scalar_one=1),  # COUNT → 1 (last user)
    ]
    conn.execute.side_effect = results

    with (
        patch("wslcb_licensing_tracker.cli.create_engine_from_env", return_value=MagicMock()),
        patch("wslcb_licensing_tracker.cli.get_db", _make_async_get_db(conn)),
    ):
        with pytest.raises(SystemExit):
            cmd_admin_remove_user(types.SimpleNamespace(email="solo@example.com"))


def test_cli_add_duplicate_user_is_noop():
    """Adding an already-existing email is a no-op (no INSERT executed)."""
    from wslcb_licensing_tracker.cli import cmd_admin_add_user

    conn = AsyncMock()
    # SELECT returns existing user → no INSERT, no commit
    conn.execute.return_value = _make_execute_result(fetchone=MagicMock(id=1))

    with (
        patch("wslcb_licensing_tracker.cli.create_engine_from_env", return_value=MagicMock()),
        patch("wslcb_licensing_tracker.cli.get_db", _make_async_get_db(conn)),
    ):
        cmd_admin_add_user(types.SimpleNamespace(email="dup@example.com"))

    # Only the SELECT was executed, no INSERT (commit not called)
    assert not conn.commit.called
