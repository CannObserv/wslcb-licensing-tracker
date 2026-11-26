"""Tests for alias_review.py — audited operator link actions and the Links panel (#264).

Run against PostgreSQL (``pg_conn``, rolled back after each test).
"""

import json

import pytest
from sqlalchemy import select

from wslcb_licensing_tracker import alias_review as ar
from wslcb_licensing_tracker.entities import get_or_create_entity, parse_and_link_entities
from wslcb_licensing_tracker.entity_aliases import AliasError, eff_many, relink
from wslcb_licensing_tracker.entity_classification import record_manual
from wslcb_licensing_tracker.models import (
    admin_audit_log,
    entity_alias_dismissals,
    entity_aliases,
)
from wslcb_licensing_tracker.pipeline import insert_record

pytestmark = pytest.mark.asyncio(loop_scope="session")

ADMIN = "admin@example.com"


async def _rec(conn, base, licence, *names, day=10):
    """Insert a record on *licence* listing *names*, link them; return the record id."""
    rec = base | {
        "license_number": licence,
        "applicants": "; ".join(["T264 BIZ LLC", *names]),
        "record_date": f"2025-08-{day:02d}",
    }
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


async def _party(conn, eid, lic=None):
    return (await eff_many(conn, [(eid, lic)]))[(eid, lic)]


# ── link ─────────────────────────────────────────────────────────────


async def test_link_scoped_writes_row_and_audit_with_names(pg_conn, standard_new_application):
    await _rec(pg_conn, standard_new_application, "T264L1", "TY ZQPEND", "TYLER ZQPEND")
    ty = await _id(pg_conn, "TY ZQPEND")
    tyler = await _id(pg_conn, "TYLER ZQPEND")

    alias_id = await ar.link(
        pg_conn, ty, tyler, scope_license="T264L1", note="nickname", admin_email=ADMIN
    )

    assert await _party(pg_conn, ty, "T264L1") == tyler
    assert await _party(pg_conn, ty) == ty
    [(target, details)] = await _audits(pg_conn, "entity.alias_set")
    assert target == ty
    assert details == {
        "alias_id": alias_id,
        "variant_id": ty,
        "variant_name": "TY ZQPEND",
        "canonical_id": tyler,
        "canonical_name": "TYLER ZQPEND",
        "scope_license": "T264L1",
        "source": "manual",
        "rule": None,
        "note": "nickname",
        "suggestion": None,
    }


async def test_link_scope_must_be_one_of_the_variants_licences(pg_conn, standard_new_application):
    await _rec(pg_conn, standard_new_application, "T264L2", "ZQSCOPE ALPHA")
    await _rec(pg_conn, standard_new_application, "T264L3", "ZQSCOPE BETA")
    a = await _id(pg_conn, "ZQSCOPE ALPHA")
    b = await _id(pg_conn, "ZQSCOPE BETA")
    with pytest.raises(AliasError, match="T264L3"):
        await ar.link(pg_conn, a, b, scope_license="T264L3", note=None, admin_email=ADMIN)
    assert await _audits(pg_conn, "entity.alias_set") == []


async def test_link_cycle_message_names_the_rows(pg_conn):
    a = await _id(pg_conn, "ZQCYC ALPHA")
    b = await _id(pg_conn, "ZQCYC BETA")
    await ar.link(pg_conn, a, b, scope_license=None, note=None, admin_email=ADMIN)
    with pytest.raises(AliasError, match=r"ZQCYC BETA.*ZQCYC ALPHA"):
        await ar.link(pg_conn, b, a, scope_license=None, note=None, admin_email=ADMIN)


async def test_link_note_too_long_is_refused(pg_conn):
    a = await _id(pg_conn, "ZQNOTE ALPHA")
    b = await _id(pg_conn, "ZQNOTE BETA")
    with pytest.raises(ValueError, match="note"):
        await ar.link(pg_conn, a, b, scope_license=None, note="x" * 501, admin_email=ADMIN)


# ── unlink ───────────────────────────────────────────────────────────


async def test_unlink_manual_row_deletes_it(pg_conn):
    a = await _id(pg_conn, "ZQUNL ALPHA")
    b = await _id(pg_conn, "ZQUNL BETA")
    alias_id = await ar.link(pg_conn, a, b, scope_license=None, note=None, admin_email=ADMIN)

    assert await ar.unlink(pg_conn, alias_id, admin_email=ADMIN) == "removed"

    assert await _party(pg_conn, a) == a
    [(target, details)] = await _audits(pg_conn, "entity.alias_remove")
    assert target == a
    assert details["variant_name"] == "ZQUNL ALPHA"
    assert details["canonical_name"] == "ZQUNL BETA"
    assert details["alias_id"] == alias_id
    assert details["source"] == "manual"


async def test_unlink_rule_row_writes_a_dismissal_in_its_scope(pg_conn):
    comma = await _id(pg_conn, "ZQRULE, GAMMA")
    natural = await _id(pg_conn, "GAMMA ZQRULE")
    await relink(pg_conn, [comma, natural])
    alias_id = (
        await pg_conn.execute(
            select(entity_aliases.c.id).where(entity_aliases.c.variant_id == comma)
        )
    ).scalar_one()

    assert await ar.unlink(pg_conn, alias_id, admin_email=ADMIN) == "dismissed"

    assert await _party(pg_conn, comma) == comma
    dismissals = (
        await pg_conn.execute(
            select(
                entity_alias_dismissals.c.entity_id_a,
                entity_alias_dismissals.c.entity_id_b,
                entity_alias_dismissals.c.scope_license,
            )
        )
    ).fetchall()
    assert (min(comma, natural), max(comma, natural), None) in {tuple(r) for r in dismissals}
    [(target, details)] = await _audits(pg_conn, "entity.alias_dismiss")
    assert target == comma
    assert details["entity_name"] == "ZQRULE, GAMMA"
    assert details["other_name"] == "GAMMA ZQRULE"
    assert details["source"] == "rule"
    assert details["rule"] == "format_key"
    assert details["scope_license"] is None


async def test_unlink_missing_row(pg_conn):
    with pytest.raises(LookupError):
        await ar.unlink(pg_conn, 2_000_000_000, admin_email=ADMIN)


# ── dismiss / undismiss ──────────────────────────────────────────────


async def test_dismiss_and_undo(pg_conn):
    comma = await _id(pg_conn, "ZQDIS, DELTA")
    natural = await _id(pg_conn, "DELTA ZQDIS")
    await relink(pg_conn, [comma, natural])
    assert await _party(pg_conn, comma) == natural

    dismissal_id = await ar.dismiss(
        pg_conn, natural, comma, scope_license=None, note="two people", admin_email=ADMIN
    )
    assert await _party(pg_conn, comma) == comma
    [(target, details)] = await _audits(pg_conn, "entity.alias_dismiss")
    assert target == natural
    assert details["dismissal_id"] == dismissal_id
    assert (details["entity_name"], details["other_name"]) == ("DELTA ZQDIS", "ZQDIS, DELTA")
    assert (details["source"], details["note"]) == ("manual", "two people")

    with pytest.raises(AliasError, match="already"):
        await ar.dismiss(pg_conn, comma, natural, scope_license=None, note=None, admin_email=ADMIN)

    await ar.undismiss(pg_conn, dismissal_id, admin_email=ADMIN)
    assert await _party(pg_conn, comma) == natural
    [(_, undo)] = await _audits(pg_conn, "entity.alias_undismiss")
    assert {undo["entity_name"], undo["other_name"]} == {"DELTA ZQDIS", "ZQDIS, DELTA"}
    assert undo["note"] == "two people"
    with pytest.raises(LookupError):
        await ar.undismiss(pg_conn, dismissal_id, admin_email=ADMIN)


# ── target parsing ───────────────────────────────────────────────────


async def test_parse_target(pg_conn):
    a = await _id(pg_conn, "ZQTARGET ALPHA")
    assert await ar.parse_target(pg_conn, f"{a}") == a
    assert await ar.parse_target(pg_conn, f" #{a} · ZQTARGET ALPHA") == a
    assert await ar.parse_target(pg_conn, "zqtarget alpha") == a
    # CR 1: a name that starts with digits is a name, not an id.
    seven = await _id(pg_conn, "7-ZQELEVEN INC")
    coins = await _id(pg_conn, "13 ZQCOINS HOLDINGS LLC")
    assert await ar.parse_target(pg_conn, "7-ZQELEVEN INC") == seven
    assert await ar.parse_target(pg_conn, "13 zqcoins holdings llc") == coins
    with pytest.raises(AliasError, match="no entity"):
        await ar.parse_target(pg_conn, "13 ZQCOINS")
    with pytest.raises(AliasError, match="no entity"):
        await ar.parse_target(pg_conn, "ZQTARGET NOBODY")
    with pytest.raises(AliasError, match="target"):
        await ar.parse_target(pg_conn, "  ")


async def test_lookup_prefix_first_and_literal_wildcards(pg_conn):
    inner = await _id(pg_conn, "ACME ZQLOOK_UP CO")
    prefix = await _id(pg_conn, "ZQLOOK_UP HOLDINGS")
    other = await _id(pg_conn, "ZQLOOKXUP LLC")
    ids = [r["id"] for r in await ar.lookup(pg_conn, "zqlook_up")]
    assert ids[:2] == [prefix, inner]
    assert other not in ids  # "_" is not a wildcard
    assert await ar.lookup(pg_conn, "z") == []
    assert await ar.lookup(pg_conn, "ZQLOOK%HOLD") == []


# ── the panel ────────────────────────────────────────────────────────


async def test_links_detail(pg_conn, standard_new_application):
    await _rec(pg_conn, standard_new_application, "T264P1", "ZQPANEL, EPS", "EPS ZQPANEL")
    await _rec(pg_conn, standard_new_application, "T264P2", "EPS ZQPANEL", "EPSY ZQPANEL")
    comma = await _id(pg_conn, "ZQPANEL, EPS")
    natural = await _id(pg_conn, "EPS ZQPANEL")
    epsy = await _id(pg_conn, "EPSY ZQPANEL")
    other = await _id(pg_conn, "ZQPANEL OTHER")
    await relink(pg_conn, [comma, natural])
    await ar.link(pg_conn, epsy, natural, scope_license="T264P2", note="n", admin_email=ADMIN)
    await ar.dismiss(pg_conn, natural, other, scope_license=None, note=None, admin_email=ADMIN)

    d = await ar.links_detail(pg_conn, natural)

    assert d["licences"] == ["T264P1", "T264P2"]
    assert d["outgoing"] == []
    incoming = {(r["other_id"], r["other_name"], r["source"], r["scope"]) for r in d["incoming"]}
    assert incoming == {
        (comma, "ZQPANEL, EPS", "rule", None),
        (epsy, "EPSY ZQPANEL", "manual", "T264P2"),
    }
    assert [(x["other_id"], x["other_name"]) for x in d["dismissals"]] == [(other, "ZQPANEL OTHER")]
    assert d["in_manual"] is True

    v = await ar.links_detail(pg_conn, epsy)
    [out] = v["outgoing"]
    assert (out["other_id"], out["created_by"], out["note"]) == (natural, ADMIN, "n")
    assert v["licences"] == ["T264P2"]


# ── not-a-name ───────────────────────────────────────────────────────


async def test_not_a_name_refused_while_in_a_manual_row(pg_conn):
    a = await _id(pg_conn, "ZQNAN ALPHA")
    b = await _id(pg_conn, "ZQNAN BETA")
    await ar.link(pg_conn, a, b, scope_license=None, note=None, admin_email=ADMIN)
    with pytest.raises(ValueError, match="unlink"):
        await record_manual(
            pg_conn, a, entity_type="not_a_name", entity_subtype=None, created_by=ADMIN
        )


async def test_link_scope_may_be_a_licence_the_entity_holds_through_a_variant(
    pg_conn, standard_new_application
):
    """#265: a middle-name pair's parties may be on the licence only through their spellings."""
    await _rec(pg_conn, standard_new_application, "T265C1", "ZQSCO, ANN", "ZQSCO, ANN Q")
    natural, target = await _id(pg_conn, "ANN ZQSCO"), await _id(pg_conn, "ANN Q ZQSCO")
    await relink(pg_conn, [natural, target])
    await ar.link(pg_conn, natural, target, scope_license="T265C1", note=None, admin_email=ADMIN)
    assert await _party(pg_conn, await _id(pg_conn, "ZQSCO, ANN"), "T265C1") == target


async def test_links_panel_offers_licences_held_through_a_variant(
    pg_conn, standard_new_application
):
    """CR 6: the panel's licence choices match what the scope check accepts."""
    await _rec(pg_conn, standard_new_application, "T265V1", "ZQVIA, ROSE")
    comma, natural = await _id(pg_conn, "ZQVIA, ROSE"), await _id(pg_conn, "ROSE ZQVIA")
    await relink(pg_conn, [comma, natural])
    assert (await ar.links_detail(pg_conn, natural))["licences"] == ["T265V1"]
