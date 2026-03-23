"""Tests for pg_admin_audit.py — async audit log helpers."""

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from wslcb_licensing_tracker.pg_admin_audit import get_audit_log, log_action


class TestLogAction:
    """Tests for log_action function."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_int_id(self, pg_conn: AsyncConnection) -> None:
        """log_action returns an integer id > 0."""
        row_id = await log_action(
            pg_conn,
            "test@example.com",
            "endorsement.test",
            "endorsement",
            target_id=1,
            details={"key": "value"},
        )
        assert isinstance(row_id, int)
        assert row_id > 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_null_details(self, pg_conn: AsyncConnection) -> None:
        """log_action handles None details."""
        row_id = await log_action(pg_conn, "test@example.com", "endorsement.noop", "endorsement")
        assert isinstance(row_id, int)


class TestGetAuditLog:
    """Tests for get_audit_log function."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_rows_and_count(self, pg_conn: AsyncConnection) -> None:
        """get_audit_log returns rows and total count."""
        await log_action(pg_conn, "a@example.com", "test.action", "record", target_id=99)
        rows, total = await get_audit_log(pg_conn)
        assert total >= 1
        assert isinstance(rows, list)
        assert all("details_parsed" in r for r in rows)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_filter_by_action(self, pg_conn: AsyncConnection) -> None:
        """get_audit_log filters by action."""
        await log_action(pg_conn, "b@example.com", "unique.action.xyz", "record")
        rows, total = await get_audit_log(pg_conn, filters={"action": "unique.action.xyz"})
        assert total >= 1
        assert all(r["action"] == "unique.action.xyz" for r in rows)
