"""Integration tests for admin user CLI DB logic.

Exercises the same SQL paths as cli.py admin commands (add-user,
list-users, remove-user, duplicate noop, last-admin rejection) against
a real PostgreSQL database via the pg_conn fixture.
"""

import pytest
from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from wslcb_licensing_tracker.models import admin_users

pytestmark = pytest.mark.asyncio(loop_scope="session")


# ---------------------------------------------------------------------------
# Helpers — mirrors the SQL in cli.py admin commands
# ---------------------------------------------------------------------------


async def _cli_add_user(conn, email: str) -> bool:
    """Insert admin user the way cli.admin_add_user does.

    Returns True if inserted, False if already exists.
    """
    email = email.strip()
    existing = (
        await conn.execute(
            select(admin_users.c.id).where(
                text("lower(email) = lower(:email)").bindparams(email=email)
            )
        )
    ).fetchone()
    if existing:
        return False
    await conn.execute(pg_insert(admin_users).values(email=email, created_by="cli"))
    return True


async def _cli_list_users(conn) -> list:
    """List admin users the way cli.admin_list_users does."""
    result = await conn.execute(
        select(
            admin_users.c.email,
            admin_users.c.role,
            admin_users.c.created_at,
            admin_users.c.created_by,
        ).order_by(admin_users.c.created_at)
    )
    return result.fetchall()


async def _cli_remove_user(conn, email: str) -> str | None:
    """Remove admin user the way cli.admin_remove_user does.

    Returns error message string on failure, None on success.
    """
    email = email.strip()
    row = (
        await conn.execute(
            select(admin_users.c.id).where(
                text("lower(email) = lower(:email)").bindparams(email=email)
            )
        )
    ).fetchone()
    if not row:
        return f"User not found: {email}"
    count = (await conn.execute(select(func.count()).select_from(admin_users))).scalar_one()
    if count <= 1:
        return "Cannot remove the last admin user."
    await conn.execute(
        delete(admin_users).where(text("lower(email) = lower(:email)").bindparams(email=email))
    )
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCliAddUser:
    async def test_add_user_inserts_row(self, pg_conn: AsyncConnection) -> None:
        """add-user inserts a row with created_by='cli'."""
        inserted = await _cli_add_user(pg_conn, "cli-new@example.com")
        assert inserted is True

        row = (
            await pg_conn.execute(
                select(admin_users).where(func.lower(admin_users.c.email) == "cli-new@example.com")
            )
        ).one()
        assert row.email == "cli-new@example.com"
        assert row.created_by == "cli"

    async def test_add_duplicate_is_noop(self, pg_conn: AsyncConnection) -> None:
        """add-user with existing email returns False (no insert)."""
        await _cli_add_user(pg_conn, "cli-dup@example.com")
        inserted = await _cli_add_user(pg_conn, "cli-dup@example.com")
        assert inserted is False

    async def test_add_duplicate_case_insensitive(self, pg_conn: AsyncConnection) -> None:
        """Duplicate check is case-insensitive."""
        await _cli_add_user(pg_conn, "cli-case@example.com")
        inserted = await _cli_add_user(pg_conn, "CLI-CASE@EXAMPLE.COM")
        assert inserted is False


class TestCliListUsers:
    async def test_list_returns_added_users(self, pg_conn: AsyncConnection) -> None:
        """list-users returns users including ones we just added."""
        await _cli_add_user(pg_conn, "cli-alpha@example.com")
        await _cli_add_user(pg_conn, "cli-beta@example.com")

        rows = await _cli_list_users(pg_conn)
        emails = [r.email for r in rows]
        assert "cli-alpha@example.com" in emails
        assert "cli-beta@example.com" in emails

    async def test_list_shows_created_by_cli(self, pg_conn: AsyncConnection) -> None:
        """Users added via CLI show created_by='cli'."""
        await _cli_add_user(pg_conn, "cli-src@example.com")
        rows = await _cli_list_users(pg_conn)
        cli_rows = [r for r in rows if r.email == "cli-src@example.com"]
        assert len(cli_rows) == 1
        assert cli_rows[0].created_by == "cli"


class TestCliRemoveUser:
    async def test_remove_user_deletes_row(self, pg_conn: AsyncConnection) -> None:
        """remove-user deletes the target row."""
        await _cli_add_user(pg_conn, "cli-rm-target@example.com")
        await _cli_add_user(pg_conn, "cli-rm-keeper@example.com")

        err = await _cli_remove_user(pg_conn, "cli-rm-target@example.com")
        assert err is None

        remaining = (
            await pg_conn.execute(
                select(admin_users.c.id).where(
                    func.lower(admin_users.c.email) == "cli-rm-target@example.com"
                )
            )
        ).one_or_none()
        assert remaining is None

    async def test_remove_nonexistent_returns_error(self, pg_conn: AsyncConnection) -> None:
        """remove-user for missing email returns error."""
        err = await _cli_remove_user(pg_conn, "cli-nobody@example.com")
        assert err is not None
        assert "not found" in err.lower()

    async def test_remove_last_user_blocked(self, pg_conn: AsyncConnection) -> None:
        """Cannot remove the last admin user.

        Delete only our own prefixed rows to avoid interfering with other
        test files sharing the session-scoped connection, then add exactly
        one user and verify the guard prevents removal.
        """
        await pg_conn.execute(delete(admin_users).where(admin_users.c.email.like("cli-%")))
        await _cli_add_user(pg_conn, "cli-solo@example.com")

        # Verify this is the only user overall (not just cli- prefixed)
        count = (await pg_conn.execute(select(func.count()).select_from(admin_users))).scalar_one()
        if count > 1:
            # Other test files left rows; remove them so we can test the guard
            await pg_conn.execute(
                delete(admin_users).where(admin_users.c.email != "cli-solo@example.com")
            )

        err = await _cli_remove_user(pg_conn, "cli-solo@example.com")
        assert err is not None
        assert "last admin" in err.lower()

    async def test_remove_case_insensitive(self, pg_conn: AsyncConnection) -> None:
        """remove-user is case-insensitive."""
        await _cli_add_user(pg_conn, "cli-mixcase@example.com")
        await _cli_add_user(pg_conn, "cli-other2@example.com")

        err = await _cli_remove_user(pg_conn, "CLI-MIXCASE@EXAMPLE.COM")
        assert err is None

        remaining = (
            await pg_conn.execute(
                select(admin_users.c.id).where(
                    func.lower(admin_users.c.email) == "cli-mixcase@example.com"
                )
            )
        ).one_or_none()
        assert remaining is None
