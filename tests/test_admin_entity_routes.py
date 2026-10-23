"""Tests for admin_entity_routes.py — classification review routes (#208).

Auth is stubbed, ``get_db`` yields a mock connection, and the
classification_review / jev_classifier functions are patched where the routes
module imports them.  No PostgreSQL, no network.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker import admin_entity_routes as routes
from wslcb_licensing_tracker.app import app
from wslcb_licensing_tracker.config import ENV_PREFIX
from wslcb_licensing_tracker.jev_classifier import RunSummary

# Autouse: stamps app.state.engine (#155). Imported for its pytest side effect.
from ._support import _stamp_engine  # noqa: F401

MOD = "wslcb_licensing_tracker.admin_entity_routes"
ADMIN = {"id": 1, "email": "admin@example.com", "role": "admin"}
ENABLED_ENV = f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION"


@pytest.fixture
def conn():
    """The mock connection every ``get_db`` call yields; ``.opened`` counts the calls."""
    mock = AsyncMock()
    mock.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=7))
    mock.opened = 0

    @asynccontextmanager
    async def _fake_get_db(_engine):
        mock.opened += 1
        yield mock

    with patch(f"{MOD}.get_db", side_effect=_fake_get_db):
        yield mock


@pytest.fixture
def client(conn):
    with patch("wslcb_licensing_tracker.admin_auth._lookup_admin", return_value=dict(ADMIN)):
        c = TestClient(app, raise_server_exceptions=True)
        c.headers["X-ExeDev-Email"] = ADMIN["email"]
        c.headers["X-ExeDev-UserID"] = "uid-1"
        yield c


def _item(**kw) -> dict:
    item = {
        "entity_id": 7,
        "name": "HOPE HOUSE",
        "entity_type": "person",
        "entity_subtype": None,
        "classification_source": "regex",
        "reasons": ["flag", "low_confidence"],
        "flags": [
            {"id": 1, "reason": "maybe a nonprofit", "flagged_by": "x@y", "created_at": None}
        ],
        "jev": {
            "entity_type": "organization",
            "entity_subtype": "nonprofit",
            "confidence": 0.55,
            "model": "jev-1.14.0",
        },
        "regex": {"entity_type": "person", "entity_subtype": None},
    }
    return {**item, **kw}


def _summary(**kw) -> RunSummary:
    base = {"origin": "admin", "enabled": True, "key_present": True, "model": "jev-1.14.0"}
    return RunSummary(**{**base, **kw})


# ── review page ──────────────────────────────────────────────────────


def test_review_page_renders_items(client):
    with (
        patch(f"{MOD}.review_queue", new_callable=AsyncMock, return_value=[_item()]) as rq,
        patch(
            f"{MOD}.review_counts", new_callable=AsyncMock, return_value={"main": 1, "unknown": 4}
        ),
    ):
        resp = client.get("/admin/entities/review")
    assert resp.status_code == 200
    assert rq.await_args.kwargs["tab"] == "main"
    body = resp.text
    for text in (
        "HOPE HOUSE",
        "maybe a nonprofit",
        "Low confidence",
        "Flagged",
        "organization · nonprofit",
        "55%",
        "jev-1.14.0",
        "Accept AI",
        "Accept rules",
        "Set other",
        "Not a name",
        "Only the audit log keeps a record",
        "Subtype unknown",
        'action="/admin/entities/7/accept"',
        'value="/admin/entities/review?tab=main"',
    ):
        assert text in body, text
    # nav link, marked active
    assert 'href="/admin/entities/review"' in body
    assert "bg-co-purple-50 text-co-purple font-medium" in body.split("Entity review")[0][-400:]


def test_review_page_unknown_tab_and_bad_tab(client):
    with (
        patch(f"{MOD}.review_queue", new_callable=AsyncMock, return_value=[]) as rq,
        patch(
            f"{MOD}.review_counts", new_callable=AsyncMock, return_value={"main": 0, "unknown": 0}
        ),
    ):
        resp = client.get("/admin/entities/review?tab=unknown")
        assert rq.await_args.kwargs["tab"] == "unknown"
        assert "Nothing to review." in resp.text
        client.get("/admin/entities/review?tab=bogus")
        assert rq.await_args.kwargs["tab"] == "main"


def test_review_page_hides_accept_ai_for_not_a_name(client):
    item = _item(
        jev={"entity_type": "not_a_name", "entity_subtype": None, "confidence": 0.9, "model": "m"}
    )
    with (
        patch(f"{MOD}.review_queue", new_callable=AsyncMock, return_value=[item]),
        patch(
            f"{MOD}.review_counts", new_callable=AsyncMock, return_value={"main": 1, "unknown": 0}
        ),
    ):
        body = client.get("/admin/entities/review").text
    assert "Accept AI" not in body
    assert "not a name · 90%" in " ".join(body.split())


# ── POST actions ─────────────────────────────────────────────────────


def test_classify_posts_and_redirects_to_next(client, conn):
    with patch(f"{MOD}.set_classification", new_callable=AsyncMock, return_value=1) as fn:
        resp = client.post(
            "/admin/entities/7/classify",
            data={
                "entity_type": "organization",
                "entity_subtype": "nonprofit",
                "note": "  ",
                "next": "/admin/entities/review?tab=main",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/entities/review?tab=main"
    fn.assert_awaited_once_with(
        conn,
        7,
        entity_type="organization",
        entity_subtype="nonprofit",
        note=None,
        admin_email=ADMIN["email"],
    )
    conn.commit.assert_awaited_once()


def test_classify_empty_subtype_is_none(client, conn):
    with patch(f"{MOD}.set_classification", new_callable=AsyncMock, return_value=1) as fn:
        resp = client.post(
            "/admin/entities/7/classify",
            data={"entity_type": "person", "entity_subtype": "", "note": "checked"},
            follow_redirects=False,
        )
    assert resp.headers["location"] == "/entity/7"
    assert fn.await_args.kwargs["entity_subtype"] is None
    assert fn.await_args.kwargs["note"] == "checked"


def test_classify_invalid_combination_400_no_commit(client, conn):
    err = ValueError("person takes no subtype (got '<script>alert(1)</script>')")
    with patch(f"{MOD}.set_classification", new_callable=AsyncMock, side_effect=err):
        resp = client.post(
            "/admin/entities/7/classify",
            data={"entity_type": "person", "entity_subtype": "<script>alert(1)</script>"},
            follow_redirects=False,
        )
    assert resp.status_code == 400
    assert "<script>" not in resp.text
    assert "&lt;script&gt;" in resp.text
    conn.commit.assert_not_awaited()


ACTIONS = [
    ("classify", "set_classification", {"entity_type": "person"}),
    ("accept", "accept_answer", {"source": "jev"}),
    ("not-a-name", "mark_not_a_name", {}),
    ("clear-manual", "clear_override", {}),
    ("flag", "flag_entity", {"reason": "x"}),
]


@pytest.mark.parametrize(("path", "fn_name", "data"), ACTIONS)
def test_missing_entity_404(client, conn, path, fn_name, data):
    with patch(f"{MOD}.{fn_name}", new_callable=AsyncMock, side_effect=LookupError("gone")):
        resp = client.post(f"/admin/entities/7/{path}", data=data, follow_redirects=False)
    assert resp.status_code == 404
    conn.commit.assert_not_awaited()


@pytest.mark.parametrize(("path", "fn_name", "data"), ACTIONS)
def test_each_action_commits_and_redirects(client, conn, path, fn_name, data):
    with patch(f"{MOD}.{fn_name}", new_callable=AsyncMock, return_value=1) as fn:
        resp = client.post(
            f"/admin/entities/7/{path}", data={**data, "next": "/entity/7"}, follow_redirects=False
        )
    assert (resp.status_code, resp.headers["location"]) == (303, "/entity/7")
    assert fn.await_args.args[1] == 7
    assert fn.await_args.kwargs["admin_email"] == ADMIN["email"]
    conn.commit.assert_awaited_once()


def test_flag_blank_reason_400(client, conn):
    with patch(f"{MOD}.flag_entity", new_callable=AsyncMock, side_effect=ValueError("reason")):
        resp = client.post("/admin/entities/7/flag", data={"reason": ""}, follow_redirects=False)
    assert resp.status_code == 400
    conn.commit.assert_not_awaited()


@pytest.mark.parametrize(
    "nxt", ["https://evil.example", "//evil", "/\\evil", "/\t/evil", "evil", "", None]
)
def test_external_next_falls_back(client, nxt):
    data = {"reason": "x"} | ({} if nxt is None else {"next": nxt})
    with patch(f"{MOD}.flag_entity", new_callable=AsyncMock, return_value=1):
        resp = client.post("/admin/entities/7/flag", data=data, follow_redirects=False)
    assert resp.headers["location"] == "/entity/7"


def test_safe_next_accepts_local_paths():
    assert routes._safe_next("/admin/entities/review?tab=unknown", 7) == (
        "/admin/entities/review?tab=unknown"
    )
    assert routes._safe_next("/entity/9#x", 7) == "/entity/9#x"


# ── re-classify with Jev ─────────────────────────────────────────────


def test_jev_disabled_409(client, monkeypatch):
    monkeypatch.delenv(ENABLED_ENV, raising=False)
    with patch(f"{MOD}.classify_entities", new_callable=AsyncMock) as fn:
        resp = client.post("/admin/entities/7/jev", follow_redirects=False)
    assert resp.status_code == 409
    assert "Jev classification is disabled" in resp.text
    fn.assert_not_awaited()


def test_jev_enabled_calls_single_entity_with_include_manual(client, conn, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "true")
    summary = _summary(candidates=1, names_sent=1, batches_ok=1, input_tokens=10, run_id=3)
    with (
        patch(f"{MOD}.classify_entities", new_callable=AsyncMock, return_value=summary) as fn,
        patch(f"{MOD}.log_action", new_callable=AsyncMock) as audit,
    ):
        resp = client.post(
            "/admin/entities/7/jev", data={"next": "/entity/7?x=1"}, follow_redirects=False
        )
    assert (resp.status_code, resp.headers["location"]) == (303, "/entity/7?x=1")
    fn.assert_awaited_once_with(
        conn, origin="admin", mode="ids", ids=[7], include_manual=True, limit=1
    )
    audit.assert_awaited_once()
    _, email, action, target_type, target_id, details = audit.await_args.args
    assert (email, action, target_type, target_id) == (
        ADMIN["email"],
        "entity.reclassify_jev",
        "entity",
        7,
    )
    assert details["names_sent"] == 1
    assert details["run_id"] == 3
    assert details["error"] is None
    # existence check, the run, and the audit each get their own connection
    assert conn.opened == 3
    conn.commit.assert_awaited_once()


def test_jev_missing_entity_404(client, conn, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "true")
    conn.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=None))
    with patch(f"{MOD}.classify_entities", new_callable=AsyncMock) as fn:
        resp = client.post("/admin/entities/7/jev", follow_redirects=False)
    assert resp.status_code == 404
    fn.assert_not_awaited()


@pytest.mark.parametrize(
    ("summary", "text"),
    [
        (_summary(error="auth_failed", names_sent=1, batches_failed=1), "auth_failed"),
        (_summary(error="missing_api_key"), "missing_api_key"),
        (_summary(names_sent=1, batches_failed=1), "the request failed"),
    ],
)
def test_jev_failure_502(client, conn, monkeypatch, summary, text):
    monkeypatch.setenv(ENABLED_ENV, "true")
    with (
        patch(f"{MOD}.classify_entities", new_callable=AsyncMock, return_value=summary),
        patch(f"{MOD}.log_action", new_callable=AsyncMock) as audit,
    ):
        resp = client.post("/admin/entities/7/jev", follow_redirects=False)
    assert resp.status_code == 502
    assert text in resp.text
    audit.assert_awaited_once()
    conn.commit.assert_awaited_once()


def test_jev_exception_is_audited_then_502(client, conn, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "true")
    with (
        patch(f"{MOD}.classify_entities", new_callable=AsyncMock, side_effect=RuntimeError("boom")),
        patch(f"{MOD}.log_action", new_callable=AsyncMock) as audit,
    ):
        resp = client.post("/admin/entities/7/jev", follow_redirects=False)
    assert resp.status_code == 502
    assert "RuntimeError" in resp.text
    assert audit.await_args.args[5] == {"error": "RuntimeError"}


def test_jev_busy_503_with_retry_after(client, conn, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "true")
    with (
        patch(
            f"{MOD}.classify_entities",
            new_callable=AsyncMock,
            return_value=_summary(skipped_locked=True),
        ),
        patch(f"{MOD}.log_action", new_callable=AsyncMock) as audit,
    ):
        resp = client.post("/admin/entities/7/jev", follow_redirects=False)
    assert resp.status_code == 503
    assert resp.headers["retry-after"] == "60"
    assert "busy" in resp.text
    assert audit.await_args.args[5]["skipped_locked"] is True


# ── auth ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("method", "path"),
    [("get", "/admin/entities/review")]
    + [("post", f"/admin/entities/7/{p}") for p, _, _ in ACTIONS]
    + [("post", "/admin/entities/7/jev")],
)
def test_requires_admin(conn, monkeypatch, method, path):
    monkeypatch.delenv("ADMIN_DEV_EMAIL", raising=False)
    c = TestClient(app, raise_server_exceptions=True)
    resp = getattr(c, method)(path, follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"].startswith("/__exe.dev/login")
    conn.commit.assert_not_awaited()
