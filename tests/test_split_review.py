"""Tests for split_review.py — audited couple verdicts (#269, spec #246 §3, §7).

Run against PostgreSQL (``pg_conn``, rolled back after each test).
"""

import json

import pytest
from sqlalchemy import select

from wslcb_licensing_tracker import split_review as sr
from wslcb_licensing_tracker.entities import get_or_create_entity, parse_and_link_entities
from wslcb_licensing_tracker.entity_aliases import add_manual_alias
from wslcb_licensing_tracker.entity_classification import record_manual
from wslcb_licensing_tracker.models import (
    admin_audit_log,
    entities,
    entity_split_members,
    entity_splits,
    record_entities,
)
from wslcb_licensing_tracker.pipeline import insert_record

pytestmark = pytest.mark.asyncio(loop_scope="session")

ADMIN = "admin@example.com"
COUPLE = "T269R JASON AND WENDI MINAMI"


async def _rec(conn, base, licence, *names):
    rec = base | {"license_number": licence, "applicants": "; ".join(["T269R LLC", *names])}
    rid = (await insert_record(conn, rec))[0]
    await parse_and_link_entities(conn, rid, rec["applicants"], delete_existing=True)
    return rid


async def _id(conn, name):
    return await get_or_create_entity(conn, name)


async def _audits(conn, action):
    rows = await conn.execute(
        select(admin_audit_log.c.target_id, admin_audit_log.c.details)
        .where(admin_audit_log.c.action == action)
        .order_by(admin_audit_log.c.id)
    )
    return [(t, json.loads(d)) for t, d in rows.fetchall()]


async def _member_rows(conn, rid):
    rows = await conn.execute(
        select(entities.c.name)
        .join(record_entities, record_entities.c.entity_id == entities.c.id)
        .where((record_entities.c.record_id == rid) & record_entities.c.via_entity_id.is_not(None))
        .order_by(record_entities.c.position)
    )
    return list(rows.scalars())


async def _stored(conn, cid):
    verdict = (
        await conn.execute(
            select(entity_splits.c.verdict).where(entity_splits.c.combined_id == cid)
        )
    ).scalar_one_or_none()
    members = (
        await conn.execute(
            select(entity_split_members.c.member_name)
            .where(entity_split_members.c.combined_id == cid)
            .order_by(entity_split_members.c.ordinal)
        )
    ).scalars()
    return verdict, list(members)


async def test_split_writes_verdict_members_audit_and_links(pg_conn, standard_new_application):
    rid = await _rec(pg_conn, standard_new_application, "T269R1", COUPLE)
    cid = await _id(pg_conn, COUPLE)
    out = await sr.set_split(
        pg_conn,
        cid,
        ["t269r jason minami", "  ", "T269R WENDI MINAMI"],
        admin_email=ADMIN,
        note="#246 table",
    )
    assert out["records"] == 1
    assert await _stored(pg_conn, cid) == ("split", ["T269R JASON MINAMI", "T269R WENDI MINAMI"])
    assert await _member_rows(pg_conn, rid) == ["T269R JASON MINAMI", "T269R WENDI MINAMI"]
    [(target, details)] = await _audits(pg_conn, "entity.split_set")
    assert target == cid
    assert details["name"] == COUPLE
    assert details["members"] == ["T269R JASON MINAMI", "T269R WENDI MINAMI"]
    assert details["previous"] is None
    assert details["note"] == "#246 table"


async def test_keep_then_clear_audit_the_previous_verdict(pg_conn, standard_new_application):
    rid = await _rec(pg_conn, standard_new_application, "T269R2", COUPLE)
    cid = await _id(pg_conn, COUPLE)
    await sr.set_split(
        pg_conn, cid, ["T269R JASON MINAMI", "T269R WENDI MINAMI"], admin_email=ADMIN
    )
    await sr.keep_whole(pg_conn, cid, admin_email=ADMIN, note="not sure")
    assert await _stored(pg_conn, cid) == ("keep", [])
    assert await _member_rows(pg_conn, rid) == []
    [(_, keep)] = await _audits(pg_conn, "entity.split_keep")
    assert keep["previous"] == {
        "verdict": "split",
        "members": ["T269R JASON MINAMI", "T269R WENDI MINAMI"],
    }
    await sr.clear_verdict(pg_conn, cid, admin_email=ADMIN)
    assert await _stored(pg_conn, cid) == (None, [])
    [(_, clear)] = await _audits(pg_conn, "entity.split_clear")
    assert clear["name"] == COUPLE
    assert clear["previous"] == {"verdict": "keep", "members": []}


async def test_resplit_replaces_the_members(pg_conn, standard_new_application):
    rid = await _rec(pg_conn, standard_new_application, "T269R3", COUPLE)
    cid = await _id(pg_conn, COUPLE)
    await sr.set_split(
        pg_conn, cid, ["T269R JASON MINAMI", "T269R WENDI MINAMI"], admin_email=ADMIN
    )
    await sr.set_split(
        pg_conn, cid, ["T269R JASON MINAMI", "T269R WENDY MINAMI"], admin_email=ADMIN
    )
    assert await _member_rows(pg_conn, rid) == ["T269R JASON MINAMI", "T269R WENDY MINAMI"]


@pytest.mark.parametrize(
    ("members", "match"),
    [
        (["T269R JASON MINAMI"], "at least 2"),
        (["T269R JASON MINAMI", "t269r jason minami"], "twice"),
        (["T269R JASON MINAMI", COUPLE], "the combined name"),
        (["T269R JASON MINAMI", "T269R A & B MINAMI"], "AND"),
        (["T269R JASON MINAMI", "..."], "empty"),
        (["T269R JASON MINAMI", "X" * 501], "long"),
    ],
)
async def test_split_validation(pg_conn, standard_new_application, members, match):
    await _rec(pg_conn, standard_new_application, "T269R4", COUPLE)
    cid = await _id(pg_conn, COUPLE)
    with pytest.raises(ValueError, match=match):
        await sr.set_split(pg_conn, cid, members, admin_email=ADMIN)
    assert await _stored(pg_conn, cid) == (None, [])


async def test_split_refuses_a_tombstoned_member(pg_conn, standard_new_application):
    await _rec(pg_conn, standard_new_application, "T269R5", COUPLE)
    cid = await _id(pg_conn, COUPLE)
    tomb = await _id(pg_conn, "T269R WENDI MINAMI")
    await record_manual(
        pg_conn, tomb, entity_type="not_a_name", entity_subtype=None, created_by="x"
    )
    with pytest.raises(ValueError, match="not-a-name"):
        await sr.set_split(
            pg_conn, cid, ["T269R JASON MINAMI", "T269R WENDI MINAMI"], admin_email=ADMIN
        )


async def test_split_refuses_a_non_person(pg_conn, standard_new_application):
    await _rec(pg_conn, standard_new_application, "T269R6", COUPLE)
    cid = await _id(pg_conn, COUPLE)
    await record_manual(
        pg_conn, cid, entity_type="organization", entity_subtype="trust", created_by="x"
    )
    with pytest.raises(ValueError, match="person"):
        await sr.set_split(pg_conn, cid, ["T269R A MINAMI", "T269R B MINAMI"], admin_email=ADMIN)
    with pytest.raises(ValueError, match="person"):
        await sr.keep_whole(pg_conn, cid, admin_email=ADMIN)


async def test_split_refuses_an_entity_in_a_manual_link(pg_conn, standard_new_application):
    await _rec(pg_conn, standard_new_application, "T269R7", COUPLE)
    cid = await _id(pg_conn, COUPLE)
    other = await _id(pg_conn, "T269R OTHER MINAMI")
    await add_manual_alias(pg_conn, other, cid, created_by="x")
    with pytest.raises(ValueError, match="unlink"):
        await sr.set_split(pg_conn, cid, ["T269R A MINAMI", "T269R B MINAMI"], admin_email=ADMIN)


async def test_missing_entity_and_missing_verdict(pg_conn):
    with pytest.raises(LookupError):
        await sr.set_split(pg_conn, 999_999_999, ["A B", "C D"], admin_email=ADMIN)
    cid = await _id(pg_conn, "T269R NO VERDICT & X")
    with pytest.raises(LookupError, match="verdict"):
        await sr.clear_verdict(pg_conn, cid, admin_email=ADMIN)
