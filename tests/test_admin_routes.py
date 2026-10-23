"""Tests for admin_routes.py — async PostgreSQL port.

Minimal smoke tests for the ported admin routes. Uses FastAPI
dependency_overrides to inject a mock AsyncConnection; no disk DB,
no network, no PostgreSQL required.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from wslcb_licensing_tracker.admin_routes import render_admin
from wslcb_licensing_tracker.app import app

from ._support import HEALTH as _HEALTH

# Autouse fixture: stamps app.state.engine for every test — routes read it
# without a lifespan (#155). Imported for its pytest side effect.
from ._support import (
    _classification_health_stub,  # noqa: F401 — autouse (#208)
    _stamp_engine,  # noqa: F401
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_admin_client(mock_conn: AsyncMock) -> tuple[TestClient, list]:
    """Return a (client, patches) pair with auth stubbed and _get_db overridden."""
    admin_data = {"id": 1, "email": "admin@example.com", "role": "admin"}

    async def _fake_get_db() -> AsyncGenerator:
        yield mock_conn

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
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# test_admin_dashboard_requires_auth
# ---------------------------------------------------------------------------


class TestAdminDashboardRequiresAuth:
    """Unauthenticated requests to /admin/ must be rejected."""

    def test_unauthenticated_redirected_or_forbidden(self):
        """GET /admin/ without valid admin headers → 302/303/403."""
        with patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None):
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


class TestAdminDashboard:
    """GET /admin/ renders the dashboard page with mocked DB and integrity checks."""

    def test_dashboard_200(self):
        """GET /admin/ returns 200 with mocked record counts and integrity results."""
        mock_conn = AsyncMock()

        # Build mock results for each execute call in order:
        # 1. aggregate counts, 2. recent counts, 3. scrape log, 4. user count
        def _mapping_result(row_dict):
            result = MagicMock()
            mapping = MagicMock()
            mapping.one.return_value = row_dict
            result.mappings.return_value = mapping
            return result

        def _all_result(rows):
            result = MagicMock()
            mapping = MagicMock()
            mapping.all.return_value = rows
            result.mappings.return_value = mapping
            return result

        def _scalar_result(value):
            result = MagicMock()
            result.scalar_one.return_value = value
            return result

        mock_conn.execute.side_effect = [
            _mapping_result({"total": 100, "new_apps": 50, "approved": 30, "discontinued": 20}),
            _mapping_result({"last_24h": 5, "last_7d": 20}),
            _all_result([]),
            _scalar_result(1),
        ]

        @asynccontextmanager
        async def _fake_get_db_ctx(engine):
            yield mock_conn

        with (
            patch(
                "wslcb_licensing_tracker.admin_routes.get_db",
                side_effect=_fake_get_db_ctx,
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.check_orphaned_locations",
                new_callable=AsyncMock,
                return_value={"count": 0, "details": []},
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.check_unenriched_records",
                new_callable=AsyncMock,
                return_value={
                    "no_endorsements": 0,
                    "no_entities": 0,
                    "no_provenance": 0,
                    "no_enrichment_tracking": 0,
                },
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.check_endorsement_anomalies",
                new_callable=AsyncMock,
                return_value={"unresolved_codes": 0, "placeholder_endorsements": 0},
            ),
        ):
            client, patches = _make_admin_client(mock_conn)
            try:
                resp = client.get("/admin/")
            finally:
                _stop(client, patches)

        assert resp.status_code == 200

    def test_dashboard_shows_record_counts(self):
        """Dashboard HTML includes the total record count from the DB."""
        mock_conn = AsyncMock()

        def _mapping_result(row_dict):
            result = MagicMock()
            mapping = MagicMock()
            mapping.one.return_value = row_dict
            result.mappings.return_value = mapping
            return result

        def _all_result(rows):
            result = MagicMock()
            mapping = MagicMock()
            mapping.all.return_value = rows
            result.mappings.return_value = mapping
            return result

        def _scalar_result(value):
            result = MagicMock()
            result.scalar_one.return_value = value
            return result

        mock_conn.execute.side_effect = [
            _mapping_result(
                {"total": 9999, "new_apps": 1000, "approved": 5000, "discontinued": 3999}
            ),
            _mapping_result({"last_24h": 12, "last_7d": 88}),
            _all_result([]),
            _scalar_result(2),
        ]

        @asynccontextmanager
        async def _fake_get_db_ctx(engine):
            yield mock_conn

        with (
            patch(
                "wslcb_licensing_tracker.admin_routes.get_db",
                side_effect=_fake_get_db_ctx,
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.check_orphaned_locations",
                new_callable=AsyncMock,
                return_value={"count": 0, "details": []},
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.check_unenriched_records",
                new_callable=AsyncMock,
                return_value={
                    "no_endorsements": 0,
                    "no_entities": 0,
                    "no_provenance": 0,
                    "no_enrichment_tracking": 0,
                },
            ),
            patch(
                "wslcb_licensing_tracker.admin_routes.check_endorsement_anomalies",
                new_callable=AsyncMock,
                return_value={"unresolved_codes": 0, "placeholder_endorsements": 0},
            ),
        ):
            client, patches = _make_admin_client(mock_conn)
            try:
                resp = client.get("/admin/")
            finally:
                _stop(client, patches)

        assert resp.status_code == 200
        assert "9999" in resp.text


# ---------------------------------------------------------------------------
# #100 — init_router removed, _tpl via app.state
# ---------------------------------------------------------------------------


class TestNoInitRouter:
    """admin_routes must not expose init_router or module-level _tpl."""

    def test_no_init_router_function(self):
        """init_router() must not exist on admin_routes module."""
        import wslcb_licensing_tracker.admin_routes as mod

        assert not hasattr(mod, "init_router"), "init_router should be removed"

    def test_no_module_level_tpl(self):
        """Module-level _tpl mutable variable must not exist."""
        import wslcb_licensing_tracker.admin_routes as mod

        # render_admin still exists as a helper, but _tpl as a module global should not
        assert not hasattr(mod, "_tpl"), "_tpl module-level variable should be removed"

    def test_render_reads_from_app_state(self):
        """render_admin should read tpl from request.app.state, not a module-level variable."""
        import inspect

        source = inspect.getsource(render_admin)
        assert "request.app.state" in source


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
            patch(
                "wslcb_licensing_tracker.admin_endorsement_routes.get_db",
                side_effect=_fake_get_db_ctx,
            ),
            patch(
                "wslcb_licensing_tracker.admin_endorsement_routes.get_regulated_substances",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "wslcb_licensing_tracker.admin_endorsement_routes.get_endorsement_list",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "wslcb_licensing_tracker.admin_endorsement_routes.get_code_mappings",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            client, patches = _make_admin_client(mock_conn)
            try:
                resp = client.get("/admin/endorsements")
            finally:
                _stop(client, patches)

        assert resp.status_code == 200


def _dashboard_html() -> str:
    """GET /admin/ with the record/scrape/integrity queries mocked out."""
    mock_conn = AsyncMock()
    one = MagicMock()
    one.mappings.return_value.one.return_value = {
        "total": 1,
        "new_apps": 0,
        "approved": 0,
        "discontinued": 0,
        "last_24h": 0,
        "last_7d": 0,
    }
    scrapes = MagicMock()
    scrapes.mappings.return_value.all.return_value = []
    users = MagicMock()
    users.scalar_one.return_value = 1
    mock_conn.execute.side_effect = [one, one, scrapes, users]

    @asynccontextmanager
    async def _fake_get_db_ctx(engine):
        yield mock_conn

    integrity = {
        "check_orphaned_locations": {"count": 0, "details": []},
        "check_unenriched_records": {"no_endorsements": 0, "no_entities": 0},
        "check_endorsement_anomalies": {"unresolved_codes": 0, "placeholder_endorsements": 0},
    }
    patches = [patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx)]
    patches += [
        patch(
            f"wslcb_licensing_tracker.admin_routes.{name}", new_callable=AsyncMock, return_value=v
        )
        for name, v in integrity.items()
    ]
    for p in patches:
        p.start()
    client, auth = _make_admin_client(mock_conn)
    try:
        resp = client.get("/admin/")
    finally:
        _stop(client, auth)
        for p in patches:
            p.stop()
    assert resp.status_code == 200
    return resp.text


class TestDashboardClassificationPanel:
    """The dashboard's "Entity classification" section (#208)."""

    def test_dashboard_shows_classification_health(self, classification_health):
        classification_health.return_value = {
            **_HEALTH,
            "enabled": True,
            "queue_main": 12,
            "queue_unknown": 5,
            "problems": ["missing_api_key"],
        }
        body = _dashboard_html()
        for text in (
            "Entity classification",
            "Enabled",
            "API key missing",
            "67,202",
            'href="/admin/entities/review"',
            'href="/admin/entities/review?tab=unknown"',
            ">12<",
            ">5<",
        ):
            assert text in body, text
        assert "missing_api_key" not in body

    def test_dashboard_shows_disabled(self, classification_health):
        body = _dashboard_html()
        assert "Disabled" in body
        assert "API key missing" not in body
        assert classification_health.await_args.kwargs["now"].tzinfo is not None

    def test_dashboard_shows_spend_and_last_success(self, classification_health):
        classification_health.return_value = {
            **_HEALTH,
            "last_success_at": datetime(2026, 10, 2, 9, 30, tzinfo=UTC),
            "failures_7d": 4,
            "unfinished_runs": 1,
            "tokens_month": 750,
            "tokens_all": 638419,
            "spend_month_usd": 750 * 0.042 / 1e6,
            "spend_all_usd": 638419 * 0.042 / 1e6,
            "problems": ["recent_failures"],
        }
        body = _dashboard_html()
        for text in ("2026-10-02 09:30", "638,419", "$0.0268", "Recent failures", "jev-1.14.0"):
            assert text in body, text
