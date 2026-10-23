"""Tests for classification_review.py — audited operator actions and review queries (#208).

Run against PostgreSQL (``pg_conn``, rolled back after each test).  Jev rows
are seeded by inserting into ``entity_classifications`` directly.
"""

import json
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, func, insert, select, update

from wslcb_licensing_tracker import classification_review as cr
from wslcb_licensing_tracker.config import ENV_PREFIX
from wslcb_licensing_tracker.entities import get_or_create_entity, reclassify_entities
from wslcb_licensing_tracker.entity_classification import record_manual, resolve_entities
from wslcb_licensing_tracker.jev_classifier import awaiting_count
from wslcb_licensing_tracker.models import (
    admin_audit_log,
    classification_runs,
    entities,
    entity_classifications,
    entity_review_flags,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")

THRESHOLD_ENV = "CO_WSLCB_LICENSING_TRACKER_JEV_CONFIDENCE_THRESHOLD"
ADMIN = "admin@example.com"


@pytest.fixture(autouse=True)
def _default_threshold(monkeypatch):
    monkeypatch.delenv(THRESHOLD_ENV, raising=False)


async def _entity(conn, name: str) -> int:
    return await get_or_create_entity(conn, f"T208 {name}")


async def _jev(conn, eid: int, entity_type: str, subtype: str | None, conf: float, **kw) -> int:
    return (
        await conn.execute(
            insert(entity_classifications)
            .values(
                entity_id=eid,
                source="jev",
                entity_type=entity_type,
                entity_subtype=subtype,
                confidence=conf,
                model=kw.pop("model", "jev-1.14.0"),
                **kw,
            )
            .returning(entity_classifications.c.id)
        )
    ).scalar_one()


async def _flag(conn, eid: int, reason: str = "looks wrong") -> int:
    return (
        await conn.execute(
            insert(entity_review_flags)
            .values(entity_id=eid, flagged_by="other@example.com", reason=reason)
            .returning(entity_review_flags.c.id)
        )
    ).scalar_one()


async def _resolved(conn, eid: int) -> tuple:
    row = (
        await conn.execute(
            select(
                entities.c.entity_type,
                entities.c.entity_subtype,
                entities.c.classification_source,
            ).where(entities.c.id == eid)
        )
    ).one()
    return tuple(row)


async def _audit(conn, eid: int) -> list[tuple[str, str, dict]]:
    rows = (
        await conn.execute(
            select(
                admin_audit_log.c.action, admin_audit_log.c.admin_email, admin_audit_log.c.details
            )
            .where(
                (admin_audit_log.c.target_type == "entity") & (admin_audit_log.c.target_id == eid)
            )
            .order_by(admin_audit_log.c.id)
        )
    ).fetchall()
    return [(a, e, json.loads(d)) for a, e, d in rows]


async def _flags(conn, eid: int) -> list[tuple]:
    erf = entity_review_flags
    return [
        tuple(r)
        for r in (
            await conn.execute(
                select(erf.c.resolved_at.is_not(None), erf.c.resolved_by, erf.c.resolution)
                .where(erf.c.entity_id == eid)
                .order_by(erf.c.id)
            )
        ).fetchall()
    ]


async def _manual_rows(conn, eid: int) -> list[tuple]:
    ec = entity_classifications
    return [
        tuple(r)
        for r in (
            await conn.execute(
                select(ec.c.entity_type, ec.c.entity_subtype, ec.c.created_by, ec.c.note)
                .where((ec.c.entity_id == eid) & (ec.c.source == "manual"))
                .order_by(ec.c.id)
            )
        ).fetchall()
    ]


# ── 3.1 operator actions ─────────────────────────────────────────────


async def test_set_classification_writes_manual_audit_and_resolves_flags(pg_conn):
    eid = await _entity(pg_conn, "GREEN THUMB")
    await _flag(pg_conn, eid)
    await _flag(pg_conn, eid, "second")

    row_id = await cr.set_classification(
        pg_conn,
        eid,
        entity_type="organization",
        entity_subtype="nonprofit",
        note="charity",
        admin_email=ADMIN,
    )

    assert isinstance(row_id, int)
    assert await _resolved(pg_conn, eid) == ("organization", "nonprofit", "manual")
    assert await _manual_rows(pg_conn, eid) == [("organization", "nonprofit", ADMIN, "charity")]
    assert await _flags(pg_conn, eid) == [(True, ADMIN, "entity.classify_set")] * 2
    [(action, email, details)] = await _audit(pg_conn, eid)
    assert (action, email) == ("entity.classify_set", ADMIN)
    assert details["old"] == ["person", None, "regex"]
    assert details["new"] == ["organization", "nonprofit", "manual"]
    assert details["flags_resolved"] == 2
    assert details["note"] == "charity"


async def test_set_classification_bad_input_writes_nothing(pg_conn):
    eid = await _entity(pg_conn, "BAD INPUT")
    with pytest.raises(ValueError, match="subtype"):
        await cr.set_classification(
            pg_conn,
            eid,
            entity_type="person",
            entity_subtype="business",
            note=None,
            admin_email=ADMIN,
        )
    with pytest.raises(LookupError):
        await cr.set_classification(
            pg_conn,
            2_000_000_000,
            entity_type="person",
            entity_subtype=None,
            note=None,
            admin_email=ADMIN,
        )
    assert await _manual_rows(pg_conn, eid) == []
    assert await _audit(pg_conn, eid) == []


async def test_accept_jev_copies_answer(pg_conn):
    eid = await _entity(pg_conn, "HOPE HOUSE")
    await _jev(pg_conn, eid, "organization", "nonprofit", 0.5)
    await _flag(pg_conn, eid)

    await cr.accept_answer(pg_conn, eid, source="jev", admin_email=ADMIN)

    assert await _resolved(pg_conn, eid) == ("organization", "nonprofit", "manual")
    [(etype, sub, by, note)] = await _manual_rows(pg_conn, eid)
    assert (etype, sub, by) == ("organization", "nonprofit", ADMIN)
    assert "jev-1.14.0" in note
    assert await _flags(pg_conn, eid) == [(True, ADMIN, "entity.classify_accept_jev")]
    [(action, _, details)] = await _audit(pg_conn, eid)
    assert action == "entity.classify_accept_jev"
    assert details["accepted"] == {
        "source": "jev",
        "entity_type": "organization",
        "entity_subtype": "nonprofit",
        "confidence": 0.5,
        "model": "jev-1.14.0",
    }


async def test_accept_regex_copies_answer(pg_conn):
    eid = await _entity(pg_conn, "ACME HOLDINGS LLC")
    await _jev(pg_conn, eid, "person", None, 0.4)

    await cr.accept_answer(pg_conn, eid, source="regex", admin_email=ADMIN)

    assert await _resolved(pg_conn, eid) == ("organization", "business", "manual")
    [(action, _, _)] = await _audit(pg_conn, eid)
    assert action == "entity.classify_accept_regex"


async def test_accept_jev_org_without_subtype_stores_unknown(pg_conn):
    eid = await _entity(pg_conn, "NULL SUBTYPE")
    await _jev(pg_conn, eid, "organization", None, 0.5)
    await cr.accept_answer(pg_conn, eid, source="jev", admin_email=ADMIN)
    assert await _resolved(pg_conn, eid) == ("organization", "unknown", "manual")


async def test_accept_jev_not_a_name_marks_noise(pg_conn):
    eid = await _entity(pg_conn, "SEE ATTACHED")
    await _jev(pg_conn, eid, "not_a_name", None, 0.9)
    await _flag(pg_conn, eid)

    await cr.accept_answer(pg_conn, eid, source="jev", admin_email=ADMIN)

    [(etype, sub, by, _)] = await _manual_rows(pg_conn, eid)
    assert (etype, sub, by) == ("not_a_name", None, ADMIN)
    assert await _flags(pg_conn, eid) == [(True, ADMIN, "entity.classify_not_a_name")]
    [(action, _, _)] = await _audit(pg_conn, eid)
    assert action == "entity.classify_not_a_name"


async def test_accept_missing_source_raises(pg_conn):
    eid = await _entity(pg_conn, "NO JEV YET")
    with pytest.raises(LookupError, match="jev"):
        await cr.accept_answer(pg_conn, eid, source="jev", admin_email=ADMIN)
    with pytest.raises(LookupError):
        await cr.accept_answer(pg_conn, 2_000_000_000, source="regex", admin_email=ADMIN)
    with pytest.raises(ValueError, match="source"):
        await cr.accept_answer(pg_conn, eid, source="manual", admin_email=ADMIN)
    assert await _manual_rows(pg_conn, eid) == []
    assert await _audit(pg_conn, eid) == []


async def test_mark_not_a_name_then_reclassify_purges(pg_conn):
    eid = await _entity(pg_conn, "PURGE ME PLEASE")
    await _flag(pg_conn, eid)

    await cr.mark_not_a_name(pg_conn, eid, admin_email=ADMIN, note="fragment")

    # Unchanged until the purge: the resolver never picks not_a_name.
    assert await _resolved(pg_conn, eid) == ("person", None, "regex")
    assert await _flags(pg_conn, eid) == [(True, ADMIN, "entity.classify_not_a_name")]
    [(action, _, details)] = await _audit(pg_conn, eid)
    assert action == "entity.classify_not_a_name"
    assert details["note"] == "fragment"

    await reclassify_entities(pg_conn)
    gone = (await pg_conn.execute(select(func.count()).where(entities.c.id == eid))).scalar_one()
    assert gone == 0
    # The audit log is the lasting record.
    assert [a for a, _, _ in await _audit(pg_conn, eid)] == ["entity.classify_not_a_name"]


async def test_clear_override_audited_and_flags_untouched(pg_conn):
    eid = await _entity(pg_conn, "CLEAR ME")
    await cr.set_classification(
        pg_conn,
        eid,
        entity_type="organization",
        entity_subtype="trust",
        note=None,
        admin_email=ADMIN,
    )
    await _flag(pg_conn, eid)

    cleared = await cr.clear_override(pg_conn, eid, admin_email=ADMIN)

    assert cleared == 1
    assert await _resolved(pg_conn, eid) == ("person", None, "regex")
    assert await _flags(pg_conn, eid) == [(False, None, None)]
    actions = await _audit(pg_conn, eid)
    assert [a for a, _, _ in actions] == ["entity.classify_set", "entity.classify_clear"]
    details = actions[-1][2]
    assert details["old"] == ["organization", "trust", "manual"]
    assert details["new"] == ["person", None, "regex"]
    assert details["cleared"] == 1


async def test_flag_entity_validation(pg_conn):
    eid = await _entity(pg_conn, "FLAG ME")
    with pytest.raises(ValueError, match="reason"):
        await cr.flag_entity(pg_conn, eid, reason="   ", admin_email=ADMIN)
    with pytest.raises(LookupError):
        await cr.flag_entity(pg_conn, 2_000_000_000, reason="x", admin_email=ADMIN)
    assert await _flags(pg_conn, eid) == []

    flag_id = await cr.flag_entity(pg_conn, eid, reason="  maybe a trust  ", admin_email=ADMIN)

    erf = entity_review_flags
    row = (
        await pg_conn.execute(
            select(erf.c.flagged_by, erf.c.reason, erf.c.resolved_at).where(erf.c.id == flag_id)
        )
    ).one()
    assert tuple(row) == (ADMIN, "maybe a trust", None)
    [(action, _, details)] = await _audit(pg_conn, eid)
    assert action == "entity.flag"
    assert details["reason"] == "maybe a trust"
    assert details["flag_id"] == flag_id


# ── 3.2 queue, detail, health ────────────────────────────────────────


async def _manual(conn, eid: int, entity_type: str = "person", subtype: str | None = None) -> None:
    await record_manual(
        conn, eid, entity_type=entity_type, entity_subtype=subtype, created_by="m@example.com"
    )


async def _queue(conn, tab: str, ids: list[int]) -> list[dict]:
    """The tab's items for *ids* only, in queue order (the test DB holds others' entities)."""
    return [
        i for i in await cr.review_queue(conn, tab=tab, limit=1_000_000) if i["entity_id"] in ids
    ]


async def test_queue_membership_matrix(pg_conn):
    flag = await _entity(pg_conn, "M FLAG")
    await _flag(pg_conn, flag)
    flag_manual = await _entity(pg_conn, "M FLAG MANUAL")
    await _manual(pg_conn, flag_manual)
    await _flag(pg_conn, flag_manual)
    low = await _entity(pg_conn, "M LOW")
    await _jev(pg_conn, low, "person", None, 0.5)
    noise = await _entity(pg_conn, "M NOISE")
    await _jev(pg_conn, noise, "not_a_name", None, 0.9)
    disagree = await _entity(pg_conn, "M DISAGREE LLC")
    await _jev(pg_conn, disagree, "person", None, 0.9)
    manual_only = await _entity(pg_conn, "M MANUAL ONLY")
    await _jev(pg_conn, manual_only, "person", None, 0.3)
    await _manual(pg_conn, manual_only)
    unknown = await _entity(pg_conn, "M UNKNOWN")
    await _jev(pg_conn, unknown, "organization", "business", 0.9)
    unknown_manual = await _entity(pg_conn, "M UNKNOWN MANUAL")
    await _manual(pg_conn, unknown_manual, "organization", "unknown")
    unknown_flag = await _entity(pg_conn, "M UNKNOWN FLAG")
    await _jev(pg_conn, unknown_flag, "organization", "business", 0.9)
    await _flag(pg_conn, unknown_flag)
    ids = [
        flag,
        flag_manual,
        low,
        noise,
        disagree,
        manual_only,
        unknown,
        unknown_manual,
        unknown_flag,
    ]
    await resolve_entities(pg_conn, ids)
    assert (await _resolved(pg_conn, unknown))[:2] == ("organization", "unknown")

    main = {i["entity_id"]: i["reasons"] for i in await _queue(pg_conn, "main", ids)}
    assert main == {
        flag: ["flag"],
        flag_manual: ["flag"],
        low: ["low_confidence"],
        noise: ["not_a_name"],
        unknown_flag: ["flag", "unknown_subtype"],
    }
    unknown_tab = {i["entity_id"]: i["reasons"] for i in await _queue(pg_conn, "unknown", ids)}
    assert unknown_tab == {unknown: ["unknown_subtype"]}


async def test_queue_dedups_with_both_reasons(pg_conn):
    eid = await _entity(pg_conn, "DEDUP")
    await _jev(pg_conn, eid, "person", None, 0.45)
    await _flag(pg_conn, eid, "one")
    await _flag(pg_conn, eid, "two")

    [item] = await _queue(pg_conn, "main", [eid])

    assert item["reasons"] == ["flag", "low_confidence"]
    assert [f["reason"] for f in item["flags"]] == ["one", "two"]
    assert item["flags"][0]["flagged_by"] == "other@example.com"
    assert item["jev"] == {
        "entity_type": "person",
        "entity_subtype": None,
        "confidence": 0.45,
        "model": "jev-1.14.0",
    }
    assert item["regex"] == {"entity_type": "person", "entity_subtype": None}
    assert (item["name"], item["entity_type"], item["classification_source"]) == (
        "T208 DEDUP",
        "person",
        "regex",
    )


async def test_queue_order(pg_conn):
    flagged_jev = await _entity(pg_conn, "O FLAGGED JEV")
    await _jev(pg_conn, flagged_jev, "person", None, 0.6)
    await _flag(pg_conn, flagged_jev)
    flagged = await _entity(pg_conn, "O FLAGGED")
    await _flag(pg_conn, flagged)
    mid = await _entity(pg_conn, "O MID")
    await _jev(pg_conn, mid, "person", None, 0.5)
    lowest = await _entity(pg_conn, "O LOWEST")
    await _jev(pg_conn, lowest, "person", None, 0.3)
    noise = await _entity(pg_conn, "O NOISE")
    await _jev(pg_conn, noise, "not_a_name", None, 0.9)
    ids = [flagged_jev, flagged, mid, lowest, noise]

    order = [i["entity_id"] for i in await _queue(pg_conn, "main", ids)]

    assert order == [flagged_jev, flagged, lowest, mid, noise]


async def test_queue_limit(pg_conn):
    for n in range(3):
        await _flag(pg_conn, await _entity(pg_conn, f"LIMIT {n}"))
    assert len(await cr.review_queue(pg_conn, tab="main", limit=2)) == 2


async def test_review_counts_match_queue(pg_conn):
    await _flag(pg_conn, await _entity(pg_conn, "C FLAG"))
    u = await _entity(pg_conn, "C UNKNOWN")
    await _jev(pg_conn, u, "organization", "business", 0.9)
    await resolve_entities(pg_conn, [u])

    counts = await cr.review_counts(pg_conn)

    assert counts == {
        "main": len(await cr.review_queue(pg_conn, tab="main", limit=1_000_000)),
        "unknown": len(await cr.review_queue(pg_conn, tab="unknown", limit=1_000_000)),
    }
    assert counts["main"] >= 1
    assert counts["unknown"] >= 1


async def test_detail_shows_each_source_and_history(pg_conn):
    eid = await _entity(pg_conn, "DETAIL")
    old = await _jev(pg_conn, eid, "organization", "business", 0.8, model="jev-old")
    ec = entity_classifications
    await pg_conn.execute(update(ec).where(ec.c.id == old).values(ignored_at=func.now()))
    await _jev(pg_conn, eid, "person", None, 0.55)
    await record_manual(
        pg_conn,
        eid,
        entity_type="organization",
        entity_subtype="trust",
        created_by=ADMIN,
        note="family trust",
    )
    await _flag(pg_conn, eid, "check")

    detail = await cr.classification_detail(pg_conn, eid)

    assert detail["resolved"] == {
        "entity_type": "organization",
        "entity_subtype": "trust",
        "classification_source": "manual",
    }
    current = detail["current"]
    assert set(current) == {"regex", "jev", "manual"}
    assert current["regex"]["model"] == "regex-1"
    assert (current["jev"]["confidence"], current["jev"]["model"]) == (0.55, "jev-1.14.0")
    assert current["manual"]["entity_subtype"] == "trust"
    assert (current["manual"]["created_by"], current["manual"]["note"]) == (ADMIN, "family trust")
    history = detail["history"]
    assert [(h["source"], h["model"]) for h in history] == [
        ("manual", None),
        ("jev", "jev-1.14.0"),
        ("jev", "jev-old"),
        ("regex", "regex-1"),
    ]
    assert [h["ignored_at"] is not None for h in history] == [False, False, True, False]
    assert [f["reason"] for f in detail["open_flags"]] == ["check"]
    with pytest.raises(LookupError):
        await cr.classification_detail(pg_conn, 2_000_000_000)


def _run(started: str, **kw) -> dict:
    """One ``classification_runs`` row; every row has every key (executemany needs that)."""
    return {
        "origin": "cli",
        "model": "jev-1.14.0",
        "started_at": datetime.fromisoformat(started),
        "finished_at": None,
        "batches_ok": 0,
        "batches_failed": 0,
        "input_tokens": 0,
        "error": None,
        **kw,
    }


async def test_health_numbers(pg_conn, monkeypatch):
    for var in ("ENABLE_JEV_CLASSIFICATION", "TYPESAFE_API_KEY", "JEV_MODEL"):
        monkeypatch.delenv(f"{ENV_PREFIX}{var}", raising=False)
    cruns = classification_runs
    await pg_conn.execute(delete(cruns))
    t = datetime.fromisoformat
    await pg_conn.execute(
        insert(cruns),
        [
            # last month: counts all-time only
            _run(
                "2026-09-30T23:00+00:00",
                finished_at=t("2026-09-30T23:10+00:00"),
                batches_ok=2,
                input_tokens=1000,
            ),
            _run(
                "2026-10-01T01:00+00:00",
                finished_at=t("2026-10-01T01:05+00:00"),
                batches_ok=1,
                batches_failed=1,
                input_tokens=500,
            ),
            # died or still running
            _run("2026-10-02T00:00+00:00", batches_ok=1, input_tokens=250),
            # outside the 7-day window
            _run("2026-09-20T00:00+00:00", finished_at=t("2026-09-20T00:01+00:00"), error="x"),
            _run(
                "2026-10-02T10:00+00:00",
                finished_at=t("2026-10-02T10:01+00:00"),
                batches_failed=3,
                error="consecutive_failures",
            ),
        ],
    )

    h = await cr.classification_health(pg_conn, now=t("2026-10-03T12:00+00:00"))

    assert (h["enabled"], h["key_present"], h["model"]) == (False, False, "jev-1.14.0")
    assert h["last_success_at"] == t("2026-10-01T01:05+00:00")
    assert h["failures_7d"] == 5
    assert h["unfinished_runs"] == 1
    assert (h["tokens_month"], h["tokens_all"]) == (750, 1750)
    assert h["spend_month_usd"] == pytest.approx(750 * 0.042 / 1e6)
    assert h["spend_all_usd"] == pytest.approx(1750 * 0.042 / 1e6)
    assert h["awaiting"] == await awaiting_count(pg_conn)
    assert (h["queue_main"], h["queue_unknown"]) == tuple(
        (await cr.review_counts(pg_conn)).values()
    )
    assert h["problems"] == ["recent_failures"]


async def test_health_empty(pg_conn):
    await pg_conn.execute(delete(classification_runs))
    h = await cr.classification_health(pg_conn, now=datetime.now(UTC))
    assert h["last_success_at"] is None
    assert (h["failures_7d"], h["tokens_month"], h["tokens_all"], h["unfinished_runs"]) == (
        0,
        0,
        0,
        0,
    )
    assert h["spend_all_usd"] == 0
    assert h["problems"] == []


async def test_health_missing_key_problem(pg_conn, monkeypatch):
    await pg_conn.execute(delete(classification_runs))
    monkeypatch.setenv(f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION", "true")
    monkeypatch.delenv(f"{ENV_PREFIX}TYPESAFE_API_KEY", raising=False)
    h = await cr.classification_health(pg_conn, now=datetime.now(UTC))
    assert h["enabled"] is True
    assert h["problems"] == ["missing_api_key"]

    monkeypatch.setenv(f"{ENV_PREFIX}TYPESAFE_API_KEY", "k")
    h = await cr.classification_health(pg_conn, now=datetime.now(UTC))
    assert h["key_present"] is True
    assert h["problems"] == []


async def test_set_classification_refuses_not_a_name(pg_conn):
    """Not-a-name has its own audited action; set must not write one (CR 1)."""
    eid = await _entity(pg_conn, "SET NOT A NAME")
    with pytest.raises(ValueError, match="not-a-name"):
        await cr.set_classification(
            pg_conn,
            eid,
            entity_type="not_a_name",
            entity_subtype=None,
            note=None,
            admin_email=ADMIN,
        )
    assert await _manual_rows(pg_conn, eid) == []
    assert await _audit(pg_conn, eid) == []


async def test_note_and_reason_are_bounded(pg_conn):
    """The form's maxlength is advisory; the actions enforce it (CR 2)."""
    eid = await _entity(pg_conn, "LONG TEXT")
    long = "x" * (cr.MAX_TEXT_CHARS + 1)
    with pytest.raises(ValueError, match="note"):
        await cr.set_classification(
            pg_conn, eid, entity_type="person", entity_subtype=None, note=long, admin_email=ADMIN
        )
    with pytest.raises(ValueError, match="note"):
        await cr.mark_not_a_name(pg_conn, eid, admin_email=ADMIN, note=long)
    with pytest.raises(ValueError, match="reason"):
        await cr.flag_entity(pg_conn, eid, reason=long, admin_email=ADMIN)
    assert await _manual_rows(pg_conn, eid) == []
    assert await _flags(pg_conn, eid) == []
    assert await _audit(pg_conn, eid) == []
    ok = "y" * cr.MAX_TEXT_CHARS
    await cr.flag_entity(pg_conn, eid, reason=ok, admin_email=ADMIN)
