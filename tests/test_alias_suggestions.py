"""Tests for alias_suggestions.py — the review queue's Links tab (#264, spec §2/§4).

Run against PostgreSQL (``pg_conn``, rolled back after each test).  The test
database may hold other tests' committed rows, so each test looks only at the
suggestions touching its own entities.
"""

import pytest
from sqlalchemy import insert, update

from wslcb_licensing_tracker import alias_review as ar
from wslcb_licensing_tracker.alias_suggestions import designator_base, suggestions
from wslcb_licensing_tracker.entities import get_or_create_entity, parse_and_link_entities
from wslcb_licensing_tracker.entity_aliases import relink
from wslcb_licensing_tracker.models import entities, entity_classifications
from wslcb_licensing_tracker.pipeline import insert_record

PG = pytest.mark.asyncio(loop_scope="session")

ADMIN = "admin@example.com"


async def _ent(conn, name, etype="person"):
    eid = await get_or_create_entity(conn, name)
    await conn.execute(
        update(entities)
        .where(entities.c.id == eid)
        .values(entity_type=etype, entity_subtype="business" if etype == "organization" else None)
    )
    return eid


async def _rec(conn, base, licence, *names, day=10):
    rec = base | {
        "license_number": licence,
        "applicants": "; ".join(["T264 BIZ LLC", *names]),
        "record_date": f"2025-09-{day:02d}",
    }
    rid = (await insert_record(conn, rec))[0]
    await parse_and_link_entities(conn, rid, rec["applicants"], delete_existing=True)
    return rid


async def _mine(conn, *ids):
    """Suggestions whose variant or canonical is one of *ids*."""
    want = set(ids)
    return [s for s in await suggestions(conn) if {s["variant_id"], s["canonical_id"]} & want]


def _pair(s):
    return (s["kind"], s["variant_id"], s["canonical_id"], s["scope_license"])


@pytest.mark.parametrize(
    ("key", "base"),
    [
        ("SUGAR MOUNTAIN CAPITAL LLC", "SUGAR MOUNTAIN CAPITAL"),
        ("ELWOOD INVESTMENTS L L C", "ELWOOD INVESTMENTS"),
        ("INFINITY HOLDING COMPANY LLC", "INFINITY HOLDING"),
        ("REGAL CINEMAS CORPORATION", "REGAL CINEMAS"),
        ("BESTASIANGROCER COM LLC", "BESTASIANGROCER COM"),
        ("PALACE CELLARS", "PALACE CELLARS"),
        ("LLC", "LLC"),
    ],
)
def test_designator_base(key, base):
    assert designator_base(key) == base


@PG
async def test_comma_drop_trap(pg_conn):
    comma = await _ent(pg_conn, "ZQTHOM, DANIA")
    dropped = await _ent(pg_conn, "ZQTHOM DANIA")
    natural = await _ent(pg_conn, "DANIA ZQTHOM")
    await relink(pg_conn, [comma, dropped, natural])

    [s] = await _mine(pg_conn, comma, dropped, natural)

    assert _pair(s) == ("comma_drop", dropped, natural, None)
    assert s["label"] == "comma-drop trap"
    assert (s["variant_name"], s["canonical_name"]) == ("ZQTHOM DANIA", "DANIA ZQTHOM")


@PG
async def test_comma_dropped_only(pg_conn):
    comma = await _ent(pg_conn, "ZQHAAG, GRANTO")
    dropped = await _ent(pg_conn, "ZQHAAG GRANTO")
    await relink(pg_conn, [comma, dropped])

    [s] = await _mine(pg_conn, comma, dropped)

    assert _pair(s) == ("comma_drop", dropped, comma, None)
    assert s["label"] == "comma-dropped only"


@PG
async def test_comma_drop_whose_dropped_form_is_another_natural_order(pg_conn):
    comma = await _ent(pg_conn, "ZQCHAR, KAURA")  # natural order: KAURA ZQCHAR
    other = await _ent(pg_conn, "KAURA, ZQCHAR")  # natural order: ZQCHAR KAURA
    dropped = await _ent(pg_conn, "ZQCHAR KAURA")
    await relink(pg_conn, [comma, other, dropped])

    [s] = await _mine(pg_conn, comma, dropped)

    assert _pair(s) == ("comma_drop", dropped, comma, None)
    assert s["label"] == "also the natural order of KAURA, ZQCHAR"


@PG
async def test_legal_designator(pg_conn):
    bare = await _ent(pg_conn, "ZQSUGAR MTN CAPITAL", "organization")
    llc = await _ent(pg_conn, "ZQSUGAR MTN CAPITAL LLC", "organization")
    await relink(pg_conn, [bare, llc])

    [s] = await _mine(pg_conn, bare, llc)

    assert _pair(s) == ("legal_designator", bare, llc, None)


@PG
async def test_single_token(pg_conn, standard_new_application):
    await _rec(pg_conn, standard_new_application, "T264S1", "ZQTYY", "ZQTYY ZQPENDX", day=1)
    await _rec(pg_conn, standard_new_application, "T264S2", "ZQHALL", "ZQHALL ZQA", "ZQHALL ZQB")
    ty = await _ent(pg_conn, "ZQTYY")
    full = await _ent(pg_conn, "ZQTYY ZQPENDX")
    hal = await _ent(pg_conn, "ZQHALL")
    await relink(pg_conn, [ty, full, hal])

    [s] = await _mine(pg_conn, ty, full)
    assert _pair(s) == ("single_token", ty, full, "T264S1")
    assert s["shared_licences"] == ["T264S1"]
    assert s["shared_records"] == 1
    # Two candidate parties on the licence: no suggestion.
    assert await _mine(pg_conn, hal) == []


@PG
async def test_linked_dismissed_and_tombstoned_pairs_drop_out(pg_conn, standard_new_application):
    a = await _ent(pg_conn, "ZQGONE CO", "organization")
    b = await _ent(pg_conn, "ZQGONE CO LLC", "organization")
    c = await _ent(pg_conn, "ZQDISM, ROSA")
    d = await _ent(pg_conn, "ZQDISM ROSA")
    e = await _ent(pg_conn, "ZQTOMB, IVO")
    f = await _ent(pg_conn, "ZQTOMB IVO")
    await relink(pg_conn, [a, b, c, d, e, f])
    assert len(await _mine(pg_conn, a, b, c, d, e, f)) == 3

    await ar.link(pg_conn, a, b, scope_license=None, note=None, admin_email=ADMIN)
    await ar.dismiss(pg_conn, d, c, scope_license=None, note=None, admin_email=ADMIN)
    await pg_conn.execute(
        insert(entity_classifications).values(
            entity_id=f, source="manual", entity_type="not_a_name", created_by=ADMIN
        )
    )

    assert await _mine(pg_conn, a, b, c, d, e, f) == []


@PG
async def test_split_couple_pairs_drop_out(pg_conn, standard_new_application):
    """#269 CR 1: a split couple takes no alias link, so it is never suggested."""
    from wslcb_licensing_tracker.models import entity_split_members, entity_splits

    await _rec(pg_conn, standard_new_application, "T269SS", "ZQTYZ", "ZQTYZ ZQPENDZ", day=2)
    ty = await _ent(pg_conn, "ZQTYZ")
    full = await _ent(pg_conn, "ZQTYZ ZQPENDZ")
    await relink(pg_conn, [ty, full])
    assert len(await _mine(pg_conn, ty, full)) == 1
    await pg_conn.execute(
        insert(entity_splits).values(combined_id=full, verdict="split", created_by=ADMIN)
    )
    await pg_conn.execute(
        insert(entity_split_members).values(
            [
                {"combined_id": full, "ordinal": 0, "member_name": "ZQA ZQPENDZ"},
                {"combined_id": full, "ordinal": 1, "member_name": "ZQB ZQPENDZ"},
            ]
        )
    )
    assert await _mine(pg_conn, ty, full) == []


@PG
async def test_middle_name_conflicts(pg_conn, standard_new_application):
    """#265: a conflicting group lists each pair, shorter → longer, scoped to the licence."""
    await _rec(
        pg_conn, standard_new_application, "T265M1", "ZQJON ZQMID", "ZQJON A ZQMID", "ZQJON B ZQMID"
    )
    await _rec(pg_conn, standard_new_application, "T265M2", "ZQJON ZQMID", "ZQJON A ZQMID", day=2)
    bare, a, b = [await _ent(pg_conn, n) for n in ("ZQJON ZQMID", "ZQJON A ZQMID", "ZQJON B ZQMID")]
    await relink(pg_conn, [bare, a, b])

    got = {_pair(s): s["label"] for s in await _mine(pg_conn, bare, a, b)}
    assert got == {
        ("middle_names", b, a, "T265M1"): "middle names: B / A",
        ("middle_names", bare, a, "T265M1"): "middle names: none / A",
        ("middle_names", bare, b, "T265M1"): "middle names: none / B",
    }  # T265M2's group is compatible: the rule linked it, nothing to suggest

    await ar.link(pg_conn, bare, a, scope_license="T265M1", note=None, admin_email=ADMIN)
    assert [_pair(s) for s in await _mine(pg_conn, bare, a, b)] == [
        ("middle_names", b, a, "T265M1")
    ]


@PG
async def test_middle_name_conflicts_with_a_suffix(pg_conn, standard_new_application):
    """The SQL cut groups by the raw last token: for a suffixed key that is the suffix."""
    names = ("ZQJAY A ZQSUF JR", "ZQJAY B ZQSUF JR", "ZQJAY C ZQSUF")
    await _rec(pg_conn, standard_new_application, "T265M3", *names)
    a, b, c = [await _ent(pg_conn, n) for n in names]
    await relink(pg_conn, [a, b, c])
    assert [_pair(s) for s in await _mine(pg_conn, a, b, c)] == [("middle_names", b, a, "T265M3")]
