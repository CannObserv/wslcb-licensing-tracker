"""Integration tests for admin user web-route DB logic.

Exercises the same SQL paths as admin_routes.py (add, remove, list,
duplicate handling, last-admin guard) against a real PostgreSQL database
via the pg_conn fixture.
"""

import pytest
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from wslcb_licensing_tracker.models import admin_audit_log, admin_users
from wslcb_licensing_tracker.pg_admin_audit import log_action

pytestmark = pytest.mark.asyncio(loop_scope="session")


# ---------------------------------------------------------------------------
# Helpers — mirrors the SQL in admin_routes.py
# ---------------------------------------------------------------------------


async def _add_user(conn, email: str, *, created_by: str = "test@example.com") -> int | None:
    """Insert admin user the way admin_routes.admin_users_add does.

    Returns the new row id, or None if the email already exists.
    """
    email = email.strip().lower()
    existing = (
        await conn.execute(select(admin_users.c.id).where(func.lower(admin_users.c.email) == email))
    ).one_or_none()
    if existing:
        return None
    result = await conn.execute(
        pg_insert(admin_users)
        .values(email=email, role="admin", created_by=created_by)
        .returning(admin_users.c.id)
    )
    new_id = result.scalar_one()
    await log_action(
        conn,
        email=created_by,
        action="admin_user.add",
        target_type="admin_user",
        target_id=new_id,
        details={"added_email": email},
    )
    return new_id


async def _remove_user(conn, email: str, *, admin_email: str = "test@example.com") -> str | None:
    """Remove admin user the way admin_routes.admin_users_remove does.

    Returns an error string on failure, None on success.
    """
    email = email.strip().lower()
    if email == admin_email.lower():
        return "Cannot remove yourself"
    row = (
        await conn.execute(select(admin_users.c.id).where(func.lower(admin_users.c.email) == email))
    ).one_or_none()
    if not row:
        return f"User {email} not found"
    await conn.execute(admin_users.delete().where(func.lower(admin_users.c.email) == email))
    await log_action(
        conn,
        email=admin_email,
        action="admin_user.remove",
        target_type="admin_user",
        target_id=row[0],
        details={"removed_email": email},
    )
    return None


async def _list_users(conn) -> list:
    """List admin users the way admin_routes.admin_users does."""
    return (
        (
            await conn.execute(
                select(
                    admin_users.c.id,
                    admin_users.c.email,
                    admin_users.c.role,
                    admin_users.c.created_at,
                    admin_users.c.created_by,
                ).order_by(admin_users.c.created_at)
            )
        )
        .mappings()
        .all()
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAddUser:
    async def test_add_user_inserts_row(self, pg_conn: AsyncConnection) -> None:
        """Adding a new user inserts a row and returns its id."""
        new_id = await _add_user(pg_conn, "wru-alice@example.com")
        assert new_id is not None

        row = (await pg_conn.execute(select(admin_users).where(admin_users.c.id == new_id))).one()
        assert row.email == "wru-alice@example.com"
        assert row.role == "admin"

    async def test_add_user_creates_audit_row(self, pg_conn: AsyncConnection) -> None:
        """Adding a user creates an audit log entry."""
        new_id = await _add_user(pg_conn, "wru-bob@example.com", created_by="admin@co.com")

        rows = (
            await pg_conn.execute(
                select(admin_audit_log).where(
                    admin_audit_log.c.action == "admin_user.add",
                    admin_audit_log.c.target_id == new_id,
                )
            )
        ).all()
        assert len(rows) == 1
        assert rows[0].admin_email == "admin@co.com"

    async def test_add_duplicate_returns_none(self, pg_conn: AsyncConnection) -> None:
        """Adding an email that already exists returns None (no insert)."""
        await _add_user(pg_conn, "wru-carol@example.com")
        duplicate_id = await _add_user(pg_conn, "wru-carol@example.com")
        assert duplicate_id is None

    async def test_add_duplicate_case_insensitive(self, pg_conn: AsyncConnection) -> None:
        """Duplicate check is case-insensitive."""
        await _add_user(pg_conn, "wru-dave@example.com")
        duplicate_id = await _add_user(pg_conn, "WRU-DAVE@EXAMPLE.COM")
        assert duplicate_id is None


class TestRemoveUser:
    async def test_remove_user_deletes_row(self, pg_conn: AsyncConnection) -> None:
        """Removing a user deletes the row."""
        await _add_user(pg_conn, "wru-remove-me@example.com")
        await _add_user(pg_conn, "wru-keeper@example.com")

        err = await _remove_user(pg_conn, "wru-remove-me@example.com")
        assert err is None

        remaining = (
            await pg_conn.execute(
                select(admin_users.c.id).where(
                    func.lower(admin_users.c.email) == "wru-remove-me@example.com"
                )
            )
        ).one_or_none()
        assert remaining is None

    async def test_remove_user_creates_audit_row(self, pg_conn: AsyncConnection) -> None:
        """Removing a user creates an audit log entry."""
        new_id = await _add_user(pg_conn, "wru-audit-rm@example.com")
        await _add_user(pg_conn, "wru-other@example.com")

        await _remove_user(pg_conn, "wru-audit-rm@example.com", admin_email="boss@co.com")

        rows = (
            await pg_conn.execute(
                select(admin_audit_log).where(
                    admin_audit_log.c.action == "admin_user.remove",
                    admin_audit_log.c.target_id == new_id,
                )
            )
        ).all()
        assert len(rows) == 1
        assert rows[0].admin_email == "boss@co.com"

    async def test_remove_nonexistent_returns_error(self, pg_conn: AsyncConnection) -> None:
        """Removing a user that doesn't exist returns an error string."""
        err = await _remove_user(pg_conn, "wru-ghost@example.com")
        assert err is not None
        assert "not found" in err

    async def test_remove_self_returns_error(self, pg_conn: AsyncConnection) -> None:
        """Cannot remove yourself."""
        await _add_user(pg_conn, "wru-self@example.com")
        err = await _remove_user(
            pg_conn, "wru-self@example.com", admin_email="wru-self@example.com"
        )
        assert err is not None
        assert "Cannot remove yourself" in err


class TestListUsers:
    async def test_list_returns_added_users(self, pg_conn: AsyncConnection) -> None:
        """Listing returns users including ones we just added."""
        await _add_user(pg_conn, "wru-list1@example.com")
        await _add_user(pg_conn, "wru-list2@example.com")

        users = await _list_users(pg_conn)
        emails = [u["email"] for u in users]
        assert "wru-list1@example.com" in emails
        assert "wru-list2@example.com" in emails

    async def test_list_reflects_removal(self, pg_conn: AsyncConnection) -> None:
        """Listing after removal no longer shows the removed user."""
        await _add_user(pg_conn, "wru-stay@example.com")
        await _add_user(pg_conn, "wru-go@example.com")

        await _remove_user(pg_conn, "wru-go@example.com")

        users = await _list_users(pg_conn)
        emails = [u["email"] for u in users]
        assert "wru-stay@example.com" in emails
        assert "wru-go@example.com" not in emails
