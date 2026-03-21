"""Tests for admin user management routes (/admin/users) and the system dashboard.

Ported to async PostgreSQL mocking pattern.  Tests verify routing behaviour
(status codes, redirect locations, helper invocations) without a real database.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app
from wslcb_licensing_tracker.admin_routes import _get_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(admin_email: str = "admin@example.com") -> tuple[TestClient, list, AsyncMock]:
    """Return (client, patches, mock_conn) with auth stubbed and _get_db overridden."""
    admin_data = {"id": 1, "email": admin_email, "role": "admin"}
    mock_conn = AsyncMock()

    async def _fake_get_db():
        yield mock_conn

    app.dependency_overrides[_get_db] = _fake_get_db

    patches = [
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=admin_data),
    ]
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=True)
    client.headers["X-ExeDev-Email"] = admin_email
    client.headers["X-ExeDev-UserID"] = "uid-1"
    return client, patches, mock_conn


def _make_noauth_client() -> tuple[TestClient, list]:
    """Return (client, patches) with no admin in DB — auth will fail."""
    patches = [
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None),
    ]
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=False)
    client.headers["X-ExeDev-Email"] = "hacker@evil.com"
    client.headers["X-ExeDev-UserID"] = "uid-9"
    return client, patches


def _stop(patches: list) -> None:
    app.dependency_overrides.pop(_get_db, None)
    for p in patches:
        p.stop()


@asynccontextmanager
async def _fake_get_db_ctx(engine):
    """Async context manager yielding a fresh AsyncMock conn."""
    yield AsyncMock()


# ---------------------------------------------------------------------------
# GET /admin/users
# ---------------------------------------------------------------------------


class TestAdminUsersGet:
    def test_lists_users(self):
        """GET /admin/users returns 200."""
        users_data = [
            {"id": 1, "email": "admin@example.com", "role": "admin", "created_at": "2025-01-01", "created_by": "test"},
            {"id": 2, "email": "other@example.com", "role": "admin", "created_at": "2025-01-02", "created_by": "test"},
        ]

        # Use plain MagicMock for execute result so .mappings() is sync, not a coroutine.
        exec_result = MagicMock()
        exec_result.mappings.return_value.all.return_value = users_data

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            conn.execute.return_value = exec_result
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/users", follow_redirects=False)
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "admin@example.com" in resp.text
        assert "other@example.com" in resp.text

    def test_non_admin_forbidden(self):
        """GET /admin/users rejects non-admin."""
        client, patches = _make_noauth_client()
        try:
            resp = client.get(
                "/admin/users",
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)


# ---------------------------------------------------------------------------
# POST /admin/users/add
# ---------------------------------------------------------------------------


class TestAdminUsersAdd:
    def test_add_user_appears_in_db(self):
        """POST /users/add redirects to /admin/users on success."""

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            # existing lookup → None
            no_existing = MagicMock()
            no_existing.one_or_none.return_value = None
            # insert returning → scalar_one returns new id
            insert_result = MagicMock()
            insert_result.scalar_one.return_value = 99
            conn.execute.side_effect = [no_existing, insert_result]
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/users/add",
                    data={"email": "newguy@example.com"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code in (302, 303)
        assert "/admin/users" in resp.headers["location"]

    def test_add_user_audit_logged(self):
        """POST /users/add calls log_action."""
        mock_log_action = AsyncMock()

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            no_existing = MagicMock()
            no_existing.one_or_none.return_value = None
            insert_result = MagicMock()
            insert_result.scalar_one.return_value = 99
            conn.execute.side_effect = [no_existing, insert_result]
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.log_action", mock_log_action),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/users/add",
                    data={"email": "newguy@example.com"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        mock_log_action.assert_called_once()

    def test_add_duplicate_email_redirects(self):
        """Adding an existing email redirects back with error."""

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            existing = MagicMock()
            existing.one_or_none.return_value = (1,)
            conn.execute.side_effect = [existing]
            yield conn

        with patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/users/add",
                    data={"email": "dup@example.com"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code in (302, 303)
        assert "error" in resp.headers["location"]


# ---------------------------------------------------------------------------
# POST /admin/users/remove
# ---------------------------------------------------------------------------


class TestAdminUsersRemove:
    def test_remove_user(self):
        """POST /users/remove deletes the user and redirects."""

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            lookup_result = MagicMock()
            lookup_result.one_or_none.return_value = (5,)
            delete_result = MagicMock()
            conn.execute.side_effect = [lookup_result, delete_result]
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/users/remove",
                    data={"email": "victim@example.com"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code in (302, 303)
        assert "/admin/users" in resp.headers["location"]

    def test_remove_audit_logged(self):
        """POST /users/remove calls log_action."""
        mock_log_action = AsyncMock()

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            lookup_result = MagicMock()
            lookup_result.one_or_none.return_value = (5,)
            delete_result = MagicMock()
            conn.execute.side_effect = [lookup_result, delete_result]
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.log_action", mock_log_action),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/users/remove",
                    data={"email": "victim@example.com"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        mock_log_action.assert_called_once()

    def test_self_removal_rejected(self):
        """An admin cannot remove themselves — redirects with error before DB call."""
        client, patches, _ = _make_client(admin_email="admin@example.com")
        try:
            resp = client.post(
                "/admin/users/remove",
                data={"email": "admin@example.com"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303)
        assert "error" in resp.headers["location"]


# ---------------------------------------------------------------------------
# GET /admin/ (dashboard)
# ---------------------------------------------------------------------------


class TestAdminDashboard:
    """Admin dashboard rendering tests using async mocking."""

    def _make_dashboard_conn(self):
        """Return an AsyncMock conn that returns plausible dashboard results."""
        conn = AsyncMock()

        # agg query: mappings().one() → dict-like with total etc.
        agg_mapping = {"total": 2, "new_apps": 1, "approved": 1, "discontinued": 0}
        recent_mapping = {"last_24h": 1, "last_7d": 2}

        # We need execute() to return different values on sequential calls.
        # Use side_effect list.
        agg_result = MagicMock()
        agg_result.mappings.return_value.one.return_value = agg_mapping

        recent_result = MagicMock()
        recent_result.mappings.return_value.one.return_value = recent_mapping

        scrapes_result = MagicMock()
        scrapes_result.mappings.return_value.all.return_value = []

        user_count_result = MagicMock()
        user_count_result.scalar_one.return_value = 1

        conn.execute.side_effect = [
            agg_result, recent_result, scrapes_result, user_count_result
        ]
        return conn

    def test_dashboard_renders(self):
        """GET /admin/ returns 200 with a Records section heading."""

        @asynccontextmanager
        async def _ctx(engine):
            yield self._make_dashboard_conn()

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.check_orphaned_locations", new_callable=AsyncMock,
                  return_value={"count": 0, "details": []}),
            patch("wslcb_licensing_tracker.admin_routes.check_unenriched_records", new_callable=AsyncMock,
                  return_value={"no_endorsements": 0, "no_entities": 0}),
            patch("wslcb_licensing_tracker.admin_routes.check_endorsement_anomalies", new_callable=AsyncMock,
                  return_value={"unresolved_codes": 0, "placeholder_endorsements": 0}),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "Records" in resp.text

    def test_dashboard_empty_scrape_log(self):
        """Dashboard renders without error when scrape_log is empty."""

        @asynccontextmanager
        async def _ctx(engine):
            yield self._make_dashboard_conn()

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.check_orphaned_locations", new_callable=AsyncMock,
                  return_value={"count": 0, "details": []}),
            patch("wslcb_licensing_tracker.admin_routes.check_unenriched_records", new_callable=AsyncMock,
                  return_value={"no_endorsements": 0, "no_entities": 0}),
            patch("wslcb_licensing_tracker.admin_routes.check_endorsement_anomalies", new_callable=AsyncMock,
                  return_value={"unresolved_codes": 0, "placeholder_endorsements": 0}),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200

    def test_dashboard_counts_records(self):
        """Dashboard total record count reflects aggregated data."""

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            agg_result = MagicMock()
            agg_result.mappings.return_value.one.return_value = {
                "total": 2, "new_apps": 1, "approved": 1, "discontinued": 0
            }
            recent_result = MagicMock()
            recent_result.mappings.return_value.one.return_value = {"last_24h": 0, "last_7d": 0}
            scrapes_result = MagicMock()
            scrapes_result.mappings.return_value.all.return_value = []
            user_count_result = MagicMock()
            user_count_result.scalar_one.return_value = 1
            conn.execute.side_effect = [agg_result, recent_result, scrapes_result, user_count_result]
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.check_orphaned_locations", new_callable=AsyncMock,
                  return_value={"count": 0, "details": []}),
            patch("wslcb_licensing_tracker.admin_routes.check_unenriched_records", new_callable=AsyncMock,
                  return_value={"no_endorsements": 0, "no_entities": 0}),
            patch("wslcb_licensing_tracker.admin_routes.check_endorsement_anomalies", new_callable=AsyncMock,
                  return_value={"unresolved_codes": 0, "placeholder_endorsements": 0}),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "2" in resp.text  # total record count

    def test_dashboard_recent_uses_created_at(self):
        """last_24h/last_7d counts appear in the rendered page."""

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            agg_result = MagicMock()
            agg_result.mappings.return_value.one.return_value = {
                "total": 1, "new_apps": 1, "approved": 0, "discontinued": 0
            }
            recent_result = MagicMock()
            recent_result.mappings.return_value.one.return_value = {"last_24h": 1, "last_7d": 1}
            scrapes_result = MagicMock()
            scrapes_result.mappings.return_value.all.return_value = []
            user_count_result = MagicMock()
            user_count_result.scalar_one.return_value = 1
            conn.execute.side_effect = [agg_result, recent_result, scrapes_result, user_count_result]
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.check_orphaned_locations", new_callable=AsyncMock,
                  return_value={"count": 0, "details": []}),
            patch("wslcb_licensing_tracker.admin_routes.check_unenriched_records", new_callable=AsyncMock,
                  return_value={"no_endorsements": 0, "no_entities": 0}),
            patch("wslcb_licensing_tracker.admin_routes.check_endorsement_anomalies", new_callable=AsyncMock,
                  return_value={"unresolved_codes": 0, "placeholder_endorsements": 0}),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "1" in resp.text


# ---------------------------------------------------------------------------
# Duplicate Suggestions tab — HTML rendering
# ---------------------------------------------------------------------------


class TestSuggestionsTabHTML:
    """Regression tests for the Duplicate Suggestions tab layout."""

    def test_suggestions_table_wrapper_not_overflow_hidden(self):
        """The suggestions table wrapper must not use overflow-hidden."""
        suggestions = [
            {"id_a": 1, "id_b": 2, "name_a": "TAKEOUT/DELIVERY", "name_b": "TAKE OUT/DELIVERY",
             "score": 0.9, "dismissed": False, "count_a": 5, "count_b": 3}
        ]

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.suggest_duplicate_endorsements",
                  new_callable=AsyncMock, return_value=suggestions),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=suggestions")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        start = resp.text.find("Candidate A")
        end = resp.text.find("</table>", start)
        assert start != -1 and end != -1, "Could not locate suggestions table in rendered HTML"
        suggestions_block = resp.text[start:end]
        assert "overflow-hidden" not in suggestions_block

    def test_suggestions_table_wrapper_has_overflow_x_auto(self):
        """The suggestions table container must include overflow-x-auto."""
        suggestions = [
            {"id_a": 1, "id_b": 2, "name_a": "TAKEOUT/DELIVERY", "name_b": "TAKE OUT/DELIVERY",
             "score": 0.9, "dismissed": False, "count_a": 5, "count_b": 3}
        ]

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.suggest_duplicate_endorsements",
                  new_callable=AsyncMock, return_value=suggestions),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=suggestions")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "overflow-x-auto" in resp.text

    def test_accept_popup_uses_right_0_positioning(self):
        """The Accept popup must use right-0 positioning."""
        suggestions = [
            {"id_a": 1, "id_b": 2, "name_a": "TAKEOUT/DELIVERY", "name_b": "TAKE OUT/DELIVERY",
             "score": 0.9, "dismissed": False, "count_a": 5, "count_b": 3}
        ]

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.suggest_duplicate_endorsements",
                  new_callable=AsyncMock, return_value=suggestions),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=suggestions")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "right-0" in resp.text


# ---------------------------------------------------------------------------
# Post-action redirect section preservation
# ---------------------------------------------------------------------------


class TestEndorsementActionRedirects:
    """POST actions must redirect back to the tab they were submitted from."""

    def _seed_pair_mocks(self):
        """Return a context manager that mocks the alias route's DB calls."""

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            conn.execute.return_value.scalar_one_or_none.return_value = "ENDORSEMENT NAME"
            yield conn

        return _ctx

    # -- /alias ---------------------------------------------------------------

    def test_alias_from_suggestions_tab_redirects_to_suggestions(self):
        """POST /alias with return_section=suggestions must redirect to ?section=suggestions."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=self._seed_pair_mocks()),
            patch("wslcb_licensing_tracker.admin_routes.set_canonical_endorsement", new_callable=AsyncMock, return_value=1),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/alias",
                    data={"canonical_id": "1", "variant_ids": "2", "return_section": "suggestions"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "section=suggestions" in resp.headers["location"]

    def test_alias_from_endorsements_tab_redirects_to_endorsements(self):
        """POST /alias with return_section=endorsements must redirect to ?section=endorsements."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=self._seed_pair_mocks()),
            patch("wslcb_licensing_tracker.admin_routes.set_canonical_endorsement", new_callable=AsyncMock, return_value=1),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/alias",
                    data={"canonical_id": "1", "variant_ids": "2", "return_section": "endorsements"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "section=endorsements" in resp.headers["location"]

    def test_alias_default_section_is_endorsements(self):
        """POST /alias without return_section defaults to section=endorsements."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=self._seed_pair_mocks()),
            patch("wslcb_licensing_tracker.admin_routes.set_canonical_endorsement", new_callable=AsyncMock, return_value=1),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/alias",
                    data={"canonical_id": "1", "variant_ids": "2"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "section=endorsements" in resp.headers["location"]

    # -- /dismiss-suggestion --------------------------------------------------

    def test_dismiss_from_suggestions_tab_redirects_to_suggestions(self):
        """POST /dismiss-suggestion with return_section=suggestions → suggestions tab."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.dismiss_suggestion", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/dismiss-suggestion",
                    data={"id_a": "1", "id_b": "2", "return_section": "suggestions"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "section=suggestions" in resp.headers["location"]

    def test_alias_invalid_return_section_falls_back_to_endorsements(self):
        """POST /alias with unrecognised return_section falls back to endorsements."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=self._seed_pair_mocks()),
            patch("wslcb_licensing_tracker.admin_routes.set_canonical_endorsement", new_callable=AsyncMock, return_value=1),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/alias",
                    data={"canonical_id": "1", "variant_ids": "2", "return_section": "evil&injected=value"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        location = resp.headers["location"]
        assert "section=endorsements" in location
        assert "evil" not in location

    def test_dismiss_invalid_return_section_falls_back_to_endorsements(self):
        """POST /dismiss-suggestion with invalid return_section falls back to endorsements."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.dismiss_suggestion", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/dismiss-suggestion",
                    data={"id_a": "1", "id_b": "2", "return_section": "../../../../etc/passwd"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        location = resp.headers["location"]
        assert "section=endorsements" in location
        assert "passwd" not in location

    def test_dismiss_default_section_is_endorsements(self):
        """POST /dismiss-suggestion without return_section defaults to endorsements."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.dismiss_suggestion", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/dismiss-suggestion",
                    data={"id_a": "1", "id_b": "2"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "section=endorsements" in resp.headers["location"]

    # -- template: hidden return_section fields -------------------------------

    def test_suggestions_alias_form_has_return_section_suggestions(self):
        """The Accept form on the suggestions tab must include return_section=suggestions."""
        suggestions = [
            {"id_a": 1, "id_b": 2, "name_a": "TAKEOUT/DELIVERY", "name_b": "TAKE OUT/DELIVERY",
             "score": 0.9, "dismissed": False, "count_a": 5, "count_b": 3}
        ]

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.suggest_duplicate_endorsements",
                  new_callable=AsyncMock, return_value=suggestions),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=suggestions")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert 'name="return_section"' in resp.text
        assert 'value="suggestions"' in resp.text

    def test_suggestions_dismiss_form_has_return_section_suggestions(self):
        """The Dismiss form on the suggestions tab must include return_section=suggestions."""
        suggestions = [
            {"id_a": 1, "id_b": 2, "name_a": "TAKEOUT/DELIVERY", "name_b": "TAKE OUT/DELIVERY",
             "score": 0.9, "dismissed": False, "count_a": 5, "count_b": 3}
        ]

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.suggest_duplicate_endorsements",
                  new_callable=AsyncMock, return_value=suggestions),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=suggestions")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert 'name="return_section"' in resp.text
        assert 'value="suggestions"' in resp.text


# ---------------------------------------------------------------------------
# Code Mappings tab — filter search
# ---------------------------------------------------------------------------


class TestCodeMappingsFilter:
    """Regression tests for the Code Mappings tab filter search."""

    def test_code_row_has_data_names_attribute(self):
        """Each .code-row must expose a data-names attribute with endorsement names."""
        code_mappings = [
            {"code": "394", "endorsements": [{"id": 1, "name": "Cannabis Retailer"}]}
        ]

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=code_mappings),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=codes")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert 'data-names=' in resp.text

    def test_code_row_data_names_contains_endorsement_name(self):
        """The data-names attribute must contain the endorsement name for that code."""
        code_mappings = [
            {"code": "394", "endorsements": [{"id": 1, "name": "Cannabis Retailer"}]}
        ]

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=code_mappings),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=codes")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "cannabis retailer" in resp.text.lower()
        assert 'data-names="cannabis retailer"' in resp.text.lower()

    def test_filter_js_checks_data_names(self):
        """The filterCodes() JS function must reference data-names."""
        import os

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[]),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=codes")
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "admin-endorsements.js" in resp.text
        static_js = os.path.join(
            os.path.dirname(__file__), "..", "static", "js", "admin-endorsements.js"
        )
        with open(static_js) as f:
            js_content = f.read()
        assert "dataset.names" in js_content
