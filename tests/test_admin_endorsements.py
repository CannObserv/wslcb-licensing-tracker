"""Tests for admin substance management routes (/admin/endorsements/substances/*).

Ported to async PostgreSQL mocking pattern.  Routes now call async pg_*
functions; tests verify routing behaviour (status codes, redirect locations,
correct helper invocation) without a real database.
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


def _make_client(admin_email: str = "admin@example.com") -> tuple[TestClient, list]:
    """Return (client, patches) with auth stubbed and _get_db overridden."""
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
    """Return (client, patches) with auth returning None → rejected."""
    patches = [
        patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=None),
    ]
    for p in patches:
        p.start()

    client = TestClient(app, raise_server_exceptions=False)
    client.headers["X-ExeDev-Email"] = "notadmin@example.com"
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
# GET /admin/endorsements — default tab and section routing
# ---------------------------------------------------------------------------


class TestAdminEndorsementsGet:
    def test_default_tab_is_substances(self):
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
                resp = client.get("/admin/endorsements", follow_redirects=True)
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "Regulated Substances" in resp.text

    def test_section_substances_renders(self):
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
                resp = client.get("/admin/endorsements?section=substances", follow_redirects=True)
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "Regulated Substances" in resp.text

    def test_section_endorsements_still_renders(self):
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
                resp = client.get("/admin/endorsements?section=endorsements", follow_redirects=True)
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "Endorsement List" in resp.text

    def test_section_suggestions_still_renders(self):
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.suggest_duplicate_endorsements", new_callable=AsyncMock, return_value=[]),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=suggestions", follow_redirects=True)
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200

    def test_section_codes_still_renders(self):
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.get_regulated_substances", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_endorsement_list", new_callable=AsyncMock, return_value=[]),
            patch("wslcb_licensing_tracker.admin_routes.get_code_mappings", new_callable=AsyncMock, return_value=[{"code": "394", "endorsements": []}]),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.get("/admin/endorsements?section=codes", follow_redirects=True)
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 200
        assert "Code Mappings" in resp.text


# ---------------------------------------------------------------------------
# POST /admin/endorsements/substances/add
# ---------------------------------------------------------------------------


class TestSubstanceAdd:
    def test_inserts_substance_and_redirects(self):
        """POST /add calls add_substance and redirects with substance_added flash."""

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            conn.execute.return_value.scalar_one.return_value = 1
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.add_substance", new_callable=AsyncMock, return_value=42),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/substances/add",
                    data={"name": "Hemp"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "section=substances" in resp.headers["location"]
        assert "substance_added" in resp.headers["location"]

    def test_audit_log_written(self):
        """POST /add calls log_action after adding substance."""
        mock_log_action = AsyncMock()

        @asynccontextmanager
        async def _ctx(engine):
            conn = AsyncMock()
            conn.execute.return_value.scalar_one.return_value = 1
            yield conn

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.add_substance", new_callable=AsyncMock, return_value=42),
            patch("wslcb_licensing_tracker.admin_routes.log_action", mock_log_action),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/endorsements/substances/add",
                    data={"name": "Hemp"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        mock_log_action.assert_called_once()
        call_kwargs = mock_log_action.call_args
        assert call_kwargs.args[2] == "substance.add" or call_kwargs[0][2] == "substance.add"

    def test_blank_name_redirects_with_error_flash(self):
        """Blank name → 303 with substance_name_required flash (no DB call)."""
        client, patches, _ = _make_client()
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

    def test_non_admin_forbidden(self):
        client, patches = _make_noauth_client()
        try:
            resp = client.post(
                "/admin/endorsements/substances/add",
                data={"name": "Hemp"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)


# ---------------------------------------------------------------------------
# POST /admin/endorsements/substances/remove
# ---------------------------------------------------------------------------


class TestSubstanceRemove:
    def test_deletes_substance_and_redirects(self):
        """POST /remove calls remove_substance and redirects with substance_removed flash."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.remove_substance", new_callable=AsyncMock, return_value="Test Substance"),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/substances/remove",
                    data={"substance_id": "5"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "substance_removed" in resp.headers["location"]

    def test_cascades_junction_rows(self):
        """remove_substance is called (it handles cascade internally)."""
        mock_remove = AsyncMock(return_value="Test Substance")

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.remove_substance", mock_remove),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/endorsements/substances/remove",
                    data={"substance_id": "5"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        mock_remove.assert_called_once()

    def test_audit_log_written(self):
        """POST /remove calls log_action."""
        mock_log_action = AsyncMock()

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.remove_substance", new_callable=AsyncMock, return_value="Test"),
            patch("wslcb_licensing_tracker.admin_routes.log_action", mock_log_action),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/endorsements/substances/remove",
                    data={"substance_id": "5"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        mock_log_action.assert_called_once()

    def test_non_admin_forbidden(self):
        client, patches = _make_noauth_client()
        try:
            resp = client.post(
                "/admin/endorsements/substances/remove",
                data={"substance_id": "5"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)


# ---------------------------------------------------------------------------
# POST /admin/endorsements/substances/set-endorsements
# ---------------------------------------------------------------------------


class TestSubstanceSetEndorsements:
    def test_replaces_associations_and_redirects(self):
        """POST /set-endorsements calls set_substance_endorsements and redirects."""
        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.set_substance_endorsements", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/substances/set-endorsements",
                    data={"substance_id": "5", "endorsement_ids": "3"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "substance_updated" in resp.headers["location"]

    def test_clears_all_when_no_ids_sent(self):
        """POST /set-endorsements with no endorsement_ids calls set_substance_endorsements with empty list."""
        mock_set = AsyncMock()

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.set_substance_endorsements", mock_set),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/endorsements/substances/set-endorsements",
                    data={"substance_id": "5"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        # called with substance_id=5 and empty list
        mock_set.assert_called_once()
        _, call_args = mock_set.call_args[0], mock_set.call_args[0]
        # third arg should be the endorsement_ids list (empty)
        assert mock_set.call_args[0][2] == []

    def test_audit_log_written(self):
        """POST /set-endorsements calls log_action."""
        mock_log_action = AsyncMock()

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_fake_get_db_ctx),
            patch("wslcb_licensing_tracker.admin_routes.set_substance_endorsements", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.log_action", mock_log_action),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/endorsements/substances/set-endorsements",
                    data={"substance_id": "5"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        mock_log_action.assert_called_once()

    def test_non_admin_forbidden(self):
        client, patches = _make_noauth_client()
        try:
            resp = client.post(
                "/admin/endorsements/substances/set-endorsements",
                data={"substance_id": "5"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)


# ---------------------------------------------------------------------------
# POST /admin/endorsements/unalias
# ---------------------------------------------------------------------------


_UNSET = object()


def _make_unalias_conn(*, has_alias: bool = True, affected_records: list | None = None):
    """Build an AsyncMock conn for the unalias route's sequential execute() calls.

    The unalias route makes these execute() calls in order:
    1. exists check → one_or_none()
    2. alias_row check → one_or_none()
    3. variant_name → scalar_one_or_none()
    4. canonical_name → scalar_one_or_none()
    5. affected records → fetchall()
    """
    if affected_records is None:
        affected_records = []

    def _r(*, one_or_none=_UNSET, scalar_one_or_none=_UNSET, fetchall=_UNSET):
        result = MagicMock()
        if one_or_none is not _UNSET:
            result.one_or_none.return_value = one_or_none
        if scalar_one_or_none is not _UNSET:
            result.scalar_one_or_none.return_value = scalar_one_or_none
        if fetchall is not _UNSET:
            result.fetchall.return_value = fetchall
        return result

    exists_result = _r(one_or_none=(10,))
    alias_result = _r(one_or_none=(99,) if has_alias else None)
    variant_name_result = _r(scalar_one_or_none="VARIANT NAME")
    canonical_name_result = _r(scalar_one_or_none="CANONICAL NAME")
    affected_result = _r(fetchall=affected_records)

    conn = AsyncMock()
    if has_alias:
        conn.execute.side_effect = [
            exists_result, alias_result, variant_name_result, canonical_name_result, affected_result
        ]
    else:
        conn.execute.side_effect = [exists_result, alias_result]
    return conn


class TestAdminUnalias:
    def test_removes_alias_and_redirects(self):
        """POST /unalias calls remove_alias and redirects with flash=unaliased."""

        @asynccontextmanager
        async def _ctx(engine):
            yield _make_unalias_conn()

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.remove_alias", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.process_record", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/unalias",
                    data={"endorsement_id": "10"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 303
        assert "flash=unaliased" in resp.headers["location"]

    def test_reprocesses_resolved_endorsements(self):
        """POST /unalias calls process_record for each affected record."""
        mock_process = AsyncMock()

        @asynccontextmanager
        async def _ctx(engine):
            yield _make_unalias_conn(affected_records=[(99, "VARIANT NAME")])

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.remove_alias", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.process_record", mock_process),
            patch("wslcb_licensing_tracker.admin_routes.log_action", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/endorsements/unalias",
                    data={"endorsement_id": "10"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert mock_process.call_count >= 1

    def test_422_when_not_a_variant(self):
        """POST /unalias for a non-variant endorsement returns 422."""

        @asynccontextmanager
        async def _ctx(engine):
            yield _make_unalias_conn(has_alias=False)

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                resp = client.post(
                    "/admin/endorsements/unalias",
                    data={"endorsement_id": "10"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        assert resp.status_code == 422

    def test_audit_log_written(self):
        """POST /unalias calls log_action."""
        mock_log_action = AsyncMock()

        @asynccontextmanager
        async def _ctx(engine):
            yield _make_unalias_conn()

        with (
            patch("wslcb_licensing_tracker.admin_routes.get_db", side_effect=_ctx),
            patch("wslcb_licensing_tracker.admin_routes.remove_alias", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.process_record", new_callable=AsyncMock),
            patch("wslcb_licensing_tracker.admin_routes.log_action", mock_log_action),
            patch("wslcb_licensing_tracker.admin_routes.invalidate_all_filter_caches"),
        ):
            mock_engine = MagicMock()
            app.state.engine = mock_engine
            client, patches, _ = _make_client()
            try:
                client.post(
                    "/admin/endorsements/unalias",
                    data={"endorsement_id": "10"},
                    follow_redirects=False,
                )
            finally:
                _stop(patches)
                del app.state.engine

        mock_log_action.assert_called_once()

    def test_non_admin_forbidden(self):
        """POST /unalias without admin auth is rejected."""
        client, patches = _make_noauth_client()
        try:
            resp = client.post(
                "/admin/endorsements/unalias",
                data={"endorsement_id": "10"},
                follow_redirects=False,
            )
        finally:
            _stop(patches)
        assert resp.status_code in (302, 303, 403)
