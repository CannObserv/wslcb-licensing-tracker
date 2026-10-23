"""Tests for classification_review.py — audited operator actions and review queries (#208).

Run against PostgreSQL (``pg_conn``, rolled back after each test).  Jev rows
are seeded by inserting into ``entity_classifications`` directly.
"""

import json

import pytest
from sqlalchemy import func, insert, select

from wslcb_licensing_tracker import classification_review as cr
from wslcb_licensing_tracker.entities import get_or_create_entity, reclassify_entities
from wslcb_licensing_tracker.models import (
    admin_audit_log,
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
