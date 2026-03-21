"""Tests for admin_routes.py — async PostgreSQL port.

Minimal smoke tests for the ported admin routes. Uses FastAPI
dependency_overrides to inject a mock AsyncConnection; no disk DB,
no network, no PostgreSQL required.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app
from wslcb_licensing_tracker.admin_routes import _get_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_admin_client(mock_conn: AsyncMock) -> tuple[TestClient, list]:
    """Return a (client, patches) pair with auth stubbed and _get_db overridden."""
    admin_data = {"id": 1, "email": "admin@example.com", "role": "admin"}

    async def _fake_get_db() -> AsyncGenerator:
        yield mock_conn

    app.dependency_overrides[_get_db] = _fake_get_db

    patches = [
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=admin_data),
    ]
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=True)
    client.headers["X-ExeDev-Email"] = "admin@example.com"
    client.headers["X-ExeDev-UserID"] = "uid-1"
    return client, patches


def _stop(client: TestClient, patches: list) -> None:
    """Clean up dependency overrides and patches."""
    app.dependency_overrides.pop(_get_db, None)
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# test_admin_dashboard_requires_auth
# ---------------------------------------------------------------------------


class TestAdminDashboardRequiresAuth:
    """Unauthenticated requests to /admin/ must be rejected."""

    def test_unauthenticated_redirected_or_forbidden(self):
        """GET /admin/ without valid admin headers → 302/303/403."""
        with patch(
            "wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None
        ):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get(
                "/admin/",
                headers={"X-ExeDev-Email": "hacker@evil.com", "X-ExeDev-UserID": "uid-9"},
                follow_redirects=False,
            )
        assert resp.status_code in (302, 303, 403)


# ---------------------------------------------------------------------------
# test_admin_endorsements_page_loads
# ---------------------------------------------------------------------------


class TestAdminEndorsementsPageLoads:
    """GET /admin/endorsements renders successfully with mocked DB."""

    def test_page_loads_200(self):
        """GET /admin/endorsements returns 200 with mocked endorsement data."""
        mock_conn = AsyncMock()

        # Patch get_db to return an async context manager yielding mock_conn
        @asynccontextmanager
        async def _fake_get_db_ctx(engine):
            yield mock_conn

        # Patch pg module functions so no real DB calls happen
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch(
                "wslcb_licensing_tracker.admin_routes.get_regulated_substances",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.get_endorsement_list",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.get_code_mappings",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches = _make_admin_client(mock_conn)
            try:
                resp = client.get("/admin/endorsements")
            finally:
                _stop(client, patches)
                del app.state.engine

        assert resp.status_code == 200
