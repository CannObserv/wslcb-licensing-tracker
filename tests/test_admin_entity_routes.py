"""Tests for admin_entity_routes.py — classification review routes (#208).

Auth is stubbed, ``get_db`` yields a mock connection, and the
classification_review / jev_classifier functions are patched where the routes
module imports them.  No PostgreSQL, no network.
"""

import re
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wslcb_licensing_tracker import admin_entity_routes as routes
from wslcb_licensing_tracker.app import app
from wslcb_licensing_tracker.config import ADMIN_DEV_EMAIL_ENV, ENV_PREFIX
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
    nav = re.search(r'<a href="/admin/entities/review"\s+class="([^"]*)"', body)
    assert nav, "no Entity review nav link"
    assert "font-medium" in nav.group(1)


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
    monkeypatch.delenv(ADMIN_DEV_EMAIL_ENV, raising=False)
    c = TestClient(app, raise_server_exceptions=True)
    resp = getattr(c, method)(path, follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"].startswith("/__exe.dev/login")
    conn.commit.assert_not_awaited()


def test_jev_call_has_a_deadline_and_is_audited(client, conn, monkeypatch):
    """A hung Jev call gives up, is audited as TimeoutError and answers 502 (CR 7)."""
    import asyncio

    monkeypatch.setenv(ENABLED_ENV, "true")
    monkeypatch.setattr(routes, "ADMIN_JEV_TIMEOUT_SECONDS", 0.05)

    async def _hang(*_a, **_kw):
        await asyncio.sleep(30)

    with (
        patch(f"{MOD}.classify_entities", new=_hang),
        patch(f"{MOD}.log_action", new_callable=AsyncMock) as audit,
    ):
        resp = client.post("/admin/entities/7/jev", follow_redirects=False)
    assert resp.status_code == 502
    assert "TimeoutError" in resp.text
    assert audit.await_args.args[5] == {"error": "TimeoutError"}


# ── #264: alias actions, lookup, the Links tab ──────────────────────


@pytest.fixture(autouse=True)
def _no_suggestions():
    """The review page counts the Links tab; tests that don't care see none."""
    with patch(f"{MOD}.suggestions", new_callable=AsyncMock, return_value=[]) as fn:
        yield fn


def _suggestion(**kw) -> dict:
    s = {
        "kind": "single_token",
        "label": "single-token name",
        "variant_id": 24142,
        "variant_name": "TY",
        "canonical_id": 38707,
        "canonical_name": "TY PENDERGRAFT",
        "scope_license": "355693",
        "shared_licences": ["355693"],
        "shared_records": 2,
    }
    return {**s, **kw}


def test_links_tab_renders_suggestions(client, _no_suggestions):
    _no_suggestions.return_value = [
        _suggestion(),
        _suggestion(
            kind="legal_designator",
            label="legal designator",
            variant_id=5964,
            variant_name="SUGAR MOUNTAIN CAPITAL",
            canonical_id=41265,
            canonical_name="SUGAR MOUNTAIN CAPITAL LLC",
            scope_license=None,
            shared_licences=[],
            shared_records=0,
        ),
    ]
    with (
        patch(f"{MOD}.review_queue", new_callable=AsyncMock) as rq,
        patch(
            f"{MOD}.review_counts", new_callable=AsyncMock, return_value={"main": 0, "unknown": 0}
        ),
    ):
        resp = client.get("/admin/entities/review?tab=links")
    assert resp.status_code == 200
    rq.assert_not_awaited()
    body = " ".join(resp.text.split())
    for text in (
        "TY PENDERGRAFT",
        "single-token name",
        "licence 355693",
        "SUGAR MOUNTAIN CAPITAL LLC",
        "everywhere",
        'action="/admin/entities/24142/alias"',
        'action="/admin/entities/24142/not-linked"',
        'name="target" value="38707"',
        'name="licence" value="355693"',
        'name="suggestion" value="single_token"',
        'value="/admin/entities/review?tab=links"',
    ):
        assert text in body, text
    # The tab count
    assert re.search(r"Links\s*<span[^>]*>\s*2\s*</span>", resp.text)


def test_alias_posts_link_with_scope(client, conn):
    with (
        patch(f"{MOD}.parse_target", new_callable=AsyncMock, return_value=38708) as pt,
        patch(f"{MOD}.link", new_callable=AsyncMock, return_value=5) as fn,
    ):
        resp = client.post(
            "/admin/entities/38707/alias",
            data={
                "target": "38708 · TYLER DAVID PENDERGRAFT",
                "scope": "licence",
                "licence": " 355693 ",
                "note": " nickname ",
                "next": "/entity/38707?stay=1",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/entity/38707?stay=1"
    pt.assert_awaited_once_with(conn, "38708 · TYLER DAVID PENDERGRAFT")
    fn.assert_awaited_once_with(
        conn,
        38707,
        38708,
        scope_license="355693",
        note="nickname",
        admin_email=ADMIN["email"],
        suggestion=None,
    )
    conn.commit.assert_awaited_once()


def test_alias_everywhere_and_suggestion(client, conn):
    with (
        patch(f"{MOD}.parse_target", new_callable=AsyncMock, return_value=41265),
        patch(f"{MOD}.link", new_callable=AsyncMock, return_value=5) as fn,
    ):
        client.post(
            "/admin/entities/5964/alias",
            data={
                "target": "41265",
                "scope": "everywhere",
                "licence": "401176",
                "suggestion": "legal_designator",
            },
            follow_redirects=False,
        )
    assert fn.await_args.kwargs["scope_license"] is None
    assert fn.await_args.kwargs["suggestion"] == "legal_designator"


@pytest.mark.parametrize(
    ("data", "detail"),
    [
        ({"target": "1", "scope": "licence", "licence": " "}, "choose a licence"),
        ({"target": "1", "scope": "somewhere"}, "scope"),
    ],
)
def test_alias_bad_scope_400(client, conn, data, detail):
    with (
        patch(f"{MOD}.parse_target", new_callable=AsyncMock, return_value=1),
        patch(f"{MOD}.link", new_callable=AsyncMock) as fn,
    ):
        resp = client.post("/admin/entities/7/alias", data=data, follow_redirects=False)
    assert resp.status_code == 400
    assert detail in resp.text
    fn.assert_not_awaited()
    conn.commit.assert_not_awaited()


def test_alias_refusal_400_names_the_cycle(client, conn):
    from wslcb_licensing_tracker.entity_aliases import AliasError

    err = AliasError(
        "linking A (1) → B (2) would close a cycle: B (2) → A (1) [manual, everywhere]"
    )
    with (
        patch(f"{MOD}.parse_target", new_callable=AsyncMock, return_value=2),
        patch(f"{MOD}.link", new_callable=AsyncMock, side_effect=err),
    ):
        resp = client.post(
            "/admin/entities/1/alias",
            data={"target": "2", "scope": "everywhere"},
            follow_redirects=False,
        )
    assert resp.status_code == 400
    assert "would close a cycle: B (2) → A (1)" in resp.text
    conn.commit.assert_not_awaited()


def test_not_linked_posts_dismiss(client, conn):
    with (
        patch(f"{MOD}.parse_target", new_callable=AsyncMock, return_value=9),
        patch(f"{MOD}.dismiss", new_callable=AsyncMock, return_value=3) as fn,
    ):
        resp = client.post(
            "/admin/entities/7/not-linked",
            data={"target": "9", "scope": "everywhere", "note": ""},
            follow_redirects=False,
        )
    assert resp.headers["location"] == "/entity/7?stay=1"
    fn.assert_awaited_once_with(
        conn, 7, 9, scope_license=None, note=None, admin_email=ADMIN["email"], suggestion=None
    )
    conn.commit.assert_awaited_once()


def test_unlink_and_undo(client, conn):
    with patch(f"{MOD}.unlink", new_callable=AsyncMock, return_value="removed") as fn:
        resp = client.post(
            "/admin/entities/aliases/12/unlink",
            data={"next": "//evil.example/"},
            follow_redirects=False,
        )
    assert resp.headers["location"] == "/admin/entities/review?tab=links"
    fn.assert_awaited_once_with(conn, 12, admin_email=ADMIN["email"])
    with patch(f"{MOD}.undismiss", new_callable=AsyncMock, return_value=None) as fn:
        resp = client.post(
            "/admin/entities/dismissals/4/undo",
            data={"next": "/entity/7?stay=1"},
            follow_redirects=False,
        )
    assert resp.headers["location"] == "/entity/7?stay=1"
    fn.assert_awaited_once_with(conn, 4, admin_email=ADMIN["email"])
    assert conn.commit.await_count == 2


def test_unlink_missing_404(client, conn):
    with patch(f"{MOD}.unlink", new_callable=AsyncMock, side_effect=LookupError("alias 12")):
        resp = client.post("/admin/entities/aliases/12/unlink", follow_redirects=False)
    assert resp.status_code == 404
    conn.commit.assert_not_awaited()


def test_lookup_returns_datalist_options(client, conn):
    rows = [
        {"id": 41265, "name": "SUGAR MOUNTAIN CAPITAL LLC", "entity_type": "organization"},
        {"id": 7, "name": "A <B> & C", "entity_type": "person"},
    ]
    with patch(f"{MOD}.lookup", new_callable=AsyncMock, return_value=rows) as fn:
        resp = client.get("/admin/entities/lookup", params={"target": "sugar"})
    assert resp.status_code == 200
    fn.assert_awaited_once_with(conn, "sugar")
    assert '<option value="41265 · SUGAR MOUNTAIN CAPITAL LLC">organization</option>' in resp.text
    assert "A &lt;B&gt; &amp; C" in resp.text


def test_alias_routes_require_admin():
    c = TestClient(app)
    c.headers["X-Forwarded-Proto"] = "https"
    for path in (
        "/admin/entities/7/alias",
        "/admin/entities/7/not-linked",
        "/admin/entities/aliases/1/unlink",
        "/admin/entities/dismissals/1/undo",
    ):
        resp = c.post(path, data={"target": "1", "scope": "everywhere"}, follow_redirects=False)
        assert resp.status_code in (302, 303, 401, 403), path
    resp = c.get("/admin/entities/lookup?target=ab", follow_redirects=False)
    assert resp.status_code in (302, 303, 401, 403)
