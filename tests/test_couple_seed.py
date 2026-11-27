"""Tests for couple_seed.py — the #246 seeded pass (#269).

Run against PostgreSQL (``pg_conn``, rolled back after each test), with a
small table of test entities in place of the 14 prod ids.
"""

import json

import pytest
from sqlalchemy import func, insert, select, update

from wslcb_licensing_tracker import couple_seed as cs
from wslcb_licensing_tracker.entities import get_or_create_entity, parse_and_link_entities
from wslcb_licensing_tracker.models import (
    admin_audit_log,
    admin_users,
    entities,
    entity_splits,
    record_entities,
)
from wslcb_licensing_tracker.pipeline import insert_record
from wslcb_licensing_tracker.split_review import keep_whole

pytestmark = pytest.mark.asyncio(loop_scope="session")

ADMIN = "seed-admin@example.com"
SPLIT = "T269S JO AND AL ZQPARK"
KEEP = "T269S ZQLEE & ZQLEE"
TRUST = "T269S ZQKIM AND ZQKIM CHILDREN"


async def _table(conn, base):
    rec = base | {
        "license_number": "T269S1",
        "applicants": f"T269S LLC; {SPLIT}; {KEEP}; {TRUST}; T269S JO ZQPARK",
    }
    rid = (await insert_record(conn, rec))[0]
    await parse_and_link_entities(conn, rid, rec["applicants"], delete_existing=True)
    ids = {n: await get_or_create_entity(conn, n) for n in (SPLIT, KEEP, TRUST)}
    table = cs.SeedTable(
        splits={ids[SPLIT]: (SPLIT, ("T269S JO ZQPARK", "T269S AL ZQPARK"), "as prefilled")},
        keeps={ids[KEEP]: (KEEP, "which two?")},
        trusts={ids[TRUST]: (TRUST, "beneficiaries")},
    )
    return rid, ids, table


async def _verdicts(conn, ids):
    rows = await conn.execute(
        select(entity_splits.c.combined_id, entity_splits.c.verdict).where(
            entity_splits.c.combined_id.in_(ids)
        )
    )
    return dict(rows.tuples().all())


async def test_dry_run_reports_and_writes_nothing(pg_conn, standard_new_application):
    _, ids, table = await _table(pg_conn, standard_new_application)
    r = await cs.seed_couples(pg_conn, admin_email=ADMIN, dry_run=True, table=table)
    assert (r.splits, r.keeps, r.trusts, r.records) == (1, 1, 1, 1)
    assert r.member_links == 1  # JO is printed; AL is linked through the couple
    assert r.new_entities == ["T269S AL ZQPARK"]
    assert r.printed_unchanged
    assert await _verdicts(pg_conn, list(ids.values())) == {}


async def test_real_run_then_rerun(pg_conn, standard_new_application):
    rid, ids, table = await _table(pg_conn, standard_new_application)
    r = await cs.seed_couples(pg_conn, admin_email=ADMIN, table=table)
    assert not r.already_applied and r.member_links == 1
    assert await _verdicts(pg_conn, list(ids.values())) == {
        ids[SPLIT]: "split",
        ids[KEEP]: "keep",
    }
    trust = (
        await pg_conn.execute(
            select(entities.c.entity_type, entities.c.entity_subtype).where(
                entities.c.id == ids[TRUST]
            )
        )
    ).one()
    assert tuple(trust) == ("organization", "trust")
    actions = (
        await pg_conn.execute(
            select(admin_audit_log.c.action, admin_audit_log.c.details).where(
                admin_audit_log.c.target_id.in_(list(ids.values()))
            )
        )
    ).all()
    assert sorted(a for a, _ in actions) == [
        "entity.classify_set",
        "entity.split_keep",
        "entity.split_set",
    ]
    assert all("#246 seeded pass" in json.loads(d)["note"] for _, d in actions)
    again = await cs.seed_couples(pg_conn, admin_email=ADMIN, table=table)
    assert again.already_applied
    links = (
        await pg_conn.execute(
            select(func.count())
            .select_from(record_entities)
            .where(record_entities.c.record_id == rid)
        )
    ).scalar_one()
    assert links == 5  # 4 printed + AL's member row


async def test_refuses_a_renamed_entity(pg_conn, standard_new_application):
    _, ids, table = await _table(pg_conn, standard_new_application)
    await pg_conn.execute(
        update(entities).where(entities.c.id == ids[KEEP]).values(name="T269S RENAMED")
    )
    with pytest.raises(cs.SeedRefused, match="renamed"):
        await cs.seed_couples(pg_conn, admin_email=ADMIN, table=table)
    assert await _verdicts(pg_conn, list(ids.values())) == {}


async def test_refuses_a_missing_entity(pg_conn, standard_new_application):
    _, ids, table = await _table(pg_conn, standard_new_application)
    table = cs.SeedTable(
        splits=table.splits, keeps={**table.keeps, 999_999_999: ("GONE & X", "")}, trusts={}
    )
    with pytest.raises(cs.SeedRefused, match="missing"):
        await cs.seed_couples(pg_conn, admin_email=ADMIN, table=table)


async def test_refuses_an_existing_verdict(pg_conn, standard_new_application):
    _, ids, table = await _table(pg_conn, standard_new_application)
    await keep_whole(pg_conn, ids[SPLIT], admin_email=ADMIN)
    with pytest.raises(cs.SeedRefused, match="already has a 'keep' verdict"):
        await cs.seed_couples(pg_conn, admin_email=ADMIN, table=table)


async def test_resolve_admin(pg_conn):
    await pg_conn.execute(admin_users.delete())
    with pytest.raises(cs.SeedRefused, match="0 admin accounts"):
        await cs.resolve_admin(pg_conn, None)
    await pg_conn.execute(insert(admin_users).values(email=ADMIN))
    assert await cs.resolve_admin(pg_conn, None) == ADMIN
    assert await cs.resolve_admin(pg_conn, "SEED-ADMIN@example.com") == ADMIN
    with pytest.raises(cs.SeedRefused, match="not an admin"):
        await cs.resolve_admin(pg_conn, "other@example.com")
    await pg_conn.execute(insert(admin_users).values(email="second@example.com"))
    with pytest.raises(cs.SeedRefused, match="pass --admin-email"):
        await cs.resolve_admin(pg_conn, None)


async def test_the_approved_table():
    """The spec's table: 9 split, 3 keep, 2 trust; 18 member names."""
    t = cs.SEED_246
    assert (len(t.splits), len(t.keeps), len(t.trusts)) == (9, 3, 2)
    assert sum(len(m) for _, m, _ in t.splits.values()) == 18
    assert len(t.names()) == 14
