"""Tests for entity_splits.py — couple-name splitting (#269, spec #246)."""

import pytest

from wslcb_licensing_tracker.entity_splits import (
    has_couple_token,
    suggest_split,
    truncation_suspect,
)

# The 14 couple entities on prod (spec #246, *Test set*), with the raw part
# WSLCB printed and the names printed beside them in the same role (prod,
# read-only, 2026-11-27).  Persons and organizations are listed separately:
# step 2 reads every token printed on the record, step 3 persons only.
_BLACKWELLS = [
    "JAYNE MARIE BLACKWELL",
    "JEFFREY BLACKWELL",
    "JEFFREY EUGENE BLACKWELL",
    "JESSICA ANN BLACKWELL",
    "KAREN MARIE BLACKWELL",
    "TREVOR EUGENE BLACKWELL",
]
_MBAR = ["ARTHUR RUBINFELD", "GERARD BAZ", "JIHAD TABBARA"]
CASES = {
    13439: ("TREVOR & JAYNE BLACKWELL", None, _BLACKWELLS, []),
    15477: (
        "JASON AND WENDI MINAMI",
        None,
        [
            "CHRISTOPHER ALLEN OFELT",
            "HAYLEY TAPP",
            "JASON WEIGEL",
            "JEFF OFELT",
            "JEFFREY CURTIS OFELT",
            "JENA THORNTON",
        ],
        ["CCFW"],
    ),
    16331: (
        "CHRISTINA & RAYMUND AND RIVAS",
        "CHRISTINA &  RAYMUND AND RIVAS",
        ["CHRISTINA RIVAS", "RAMUNDO RIVAS JR", "RAYMUNDO PALOMAREZ RIVAS"],
        [],
    ),
    17210: (
        "HEATHER TWIST PHILLI THE ROBERT PHILLIPS AND",
        None,
        [
            "ALEXANDRA K PHILLIPS",
            "HEATHER T PHILLIPS",
            "KATHARINE P L'HEUREAUX",
            "ROBERT B PHILLIPS",
            "WILLIAM R PHILLIPS",
        ],
        [],
    ),
    21867: (
        "GARTH GEHLEN AND NATHAN DOIDGE",
        "GARTH GEHLEN AND NATHAN DOIDGE PER WAC 314-55",
        [],
        [],
    ),
    30292: ("JAMES AND WENDY HULL", None, [], []),
    45444: (
        "KEVIN AND ANNA LANGE CHILDREN",
        None,
        ["EDWARD BROWN", "JUSTIN MEIGS", "KERON LENZ"],
        ["EIGHTY EIGHT CAPITAL, LLC", "JPD 2019 GIFT TRUST"],
    ),
    45609: (
        "RENEE AND TIM OCHS",
        None,
        [
            "DIANE LYNN NAGLER",
            "KENNETH GENE NEMIROW",
            "RENEE ELIZABETH OCHS",
            "RENEE OCHS",
            "TIMOTHY FLOYD OCHS",
        ],
        [],
    ),
    49087: (
        "LOPEZ & LOPEZ",
        None,
        [
            "ARMANDO G LOPEZ CARMONA",
            "ARMANDO GUILLER LOPEZ CARMON",
            "JOSE RAMON LOPEZ",
            "STEPHANY VERONICA LOPEZ",
        ],
        [],
    ),
    49945: (
        "DONALD P CLARK AND SALLY A CLA",
        None,
        ["BRENDA ANNETTE OLSON", "CHRISTOPHER BEDWELL", "LISA BEDWELL"],
        ["SALLY A CLARK, TTEE OF THE CLA", "STEPHEN E OLSON AS TTEE OF THE"],
    ),
    51098: ("WASSEF & RACHA HAROUN", None, ["RACHA BARAZI HAROUN", "WASSEF HAROUN"], []),
    57311: (
        "COLIN & MARTHA MOSELEY",
        None,
        [*_MBAR, "MERRITT KEVIN & PATR AULD"],
        ["778, LLC"],
    ),
    57314: (
        "MERRITT KEVIN & PATR AULD",
        None,
        [*_MBAR, "COLIN & MARTHA MOSELEY"],
        ["778, LLC"],
    ),
    66317: ("FRANCISCO & VILLA VILLA", None, ["BLANCA ALICIA CASTELLANOS"], []),
}

# The spec's *Prefill* column, and whether the truncation badge shows.
EXPECTED = {
    13439: (["TREVOR EUGENE BLACKWELL", "JAYNE MARIE BLACKWELL"], False),
    15477: (["JASON MINAMI", "WENDI MINAMI"], False),
    16331: (None, True),  # two conjunctions; raw part is 30 characters
    17210: (None, False),  # trailing conjunction
    21867: (["GARTH GEHLEN", "NATHAN DOIDGE"], False),  # 45 raw: not at the cut
    30292: (["JAMES HULL", "WENDY HULL"], False),
    45444: (None, True),
    45609: (["RENEE ELIZABETH OCHS", "TIM OCHS"], False),
    49087: (None, False),  # two bare tokens
    49945: (["DONALD P CLARK", "SALLY A CLARK"], True),
    51098: (["WASSEF HAROUN", "RACHA BARAZI HAROUN"], False),
    57311: (["COLIN MOSELEY", "MARTHA MOSELEY"], False),
    57314: (["MERRITT KEVIN", "PATR AULD"], False),
    66317: (["FRANCISCO VILLA", "VILLA VILLA"], False),
}


@pytest.mark.parametrize("entity_id", sorted(CASES))
def test_prefill_matches_the_spec_table(entity_id):
    name, raw, persons, orgs = CASES[entity_id]
    prefill = suggest_split(
        name, raw_part=raw or name, record_names=[name, *persons, *orgs], persons=persons
    )
    members, badge = EXPECTED[entity_id]
    assert prefill.members == members
    assert prefill.truncated is badge


@pytest.mark.parametrize("entity_id", sorted(CASES))
def test_all_14_are_couple_shaped(entity_id):
    assert has_couple_token(CASES[entity_id][0])


@pytest.mark.parametrize(
    "name",
    ["B&P LLC", "ANDREW SMITH", "SANDRA ANDERSON", "JOHN SMITH", "R & D", "FRANCISCO & "],
)
def test_couple_token_must_stand_alone(name):
    # A glued & or an AND inside a word is no conjunction; one at an edge is
    # (17210 ends in AND, and is on the queue).
    expected = name in ("R & D", "FRANCISCO & ")
    assert has_couple_token(name) is expected


def test_truncation_badge_is_the_30_character_cut():
    assert truncation_suspect("X" * 28)
    assert truncation_suspect("X" * 30)
    assert not truncation_suspect("X" * 27)
    assert not truncation_suspect("X" * 31)
    assert not truncation_suspect("  " + "X" * 26 + "  ")


def test_completion_needs_exactly_one_longer_token():
    # Two candidate completions: leave the cut-off token alone.
    prefill = suggest_split(
        "DONALD P CLARK AND SALLY A CLA",
        raw_part="DONALD P CLARK AND SALLY A CLA",
        record_names=["CLARKE HOLDINGS", "CLARKSON LLC"],
        persons=[],
    )
    assert prefill.members == ["DONALD P CLARK", "SALLY A CLA"]


def test_no_completion_without_the_badge():
    prefill = suggest_split(
        "JO AND AL CLA", raw_part="JO AND AL CLA", record_names=["CLARK LLC"], persons=[]
    )
    assert prefill.members == ["JO CLA", "AL CLA"]


def test_fuller_form_must_be_the_unique_longest():
    # Two equally long compatible forms: keep the printed part.
    prefill = suggest_split(
        "TREVOR & JAYNE BLACKWELL",
        raw_part="TREVOR & JAYNE BLACKWELL",
        record_names=[],
        persons=["TREVOR EUGENE BLACKWELL", "TREVOR EDWARD BLACKWELL"],
    )
    assert prefill.members == ["TREVOR BLACKWELL", "JAYNE BLACKWELL"]


def test_fuller_form_ignores_couples_and_other_suffixes():
    prefill = suggest_split(
        "TREVOR & JAYNE BLACKWELL",
        raw_part="TREVOR & JAYNE BLACKWELL",
        record_names=[],
        persons=["TREVOR & JAYNE BLACKWELL", "TREVOR EUGENE BLACKWELL JR"],
    )
    assert prefill.members == ["TREVOR BLACKWELL", "JAYNE BLACKWELL"]


# ── PG: extraction (spec §2) ─────────────────────────────────────────

from sqlalchemy import delete, func, insert, select  # noqa: E402

from wslcb_licensing_tracker.entities import (  # noqa: E402
    get_or_create_entity,
    parse_and_link_entities,
    reprocess_entities,
)
from wslcb_licensing_tracker.entity_classification import record_manual  # noqa: E402
from wslcb_licensing_tracker.models import (  # noqa: E402
    entities,
    entity_split_members,
    entity_splits,
    record_entities,
)
from wslcb_licensing_tracker.pipeline import insert_record  # noqa: E402

COUPLE = "T269 TREVOR & JAYNE BLACKWELL"
APPLICANTS = f"T269 FARMS LLC; {COUPLE}; T269 TREVOR EUGENE BLACKWELL"
MEMBERS = ["T269 TREVOR EUGENE BLACKWELL", "T269 JAYNE MARIE BLACKWELL"]


async def _record(conn, rec, licence, applicants=APPLICANTS):
    rec = rec | {"license_number": licence, "applicants": applicants}
    rid = (await insert_record(conn, rec))[0]
    await parse_and_link_entities(conn, rid, applicants, delete_existing=True)
    return rid


async def _id(conn, name):
    return (await conn.execute(select(entities.c.id).where(entities.c.name == name))).scalar_one()


async def _verdict(conn, combined_id, members, verdict="split"):
    await conn.execute(
        insert(entity_splits).values(combined_id=combined_id, verdict=verdict, created_by="t@x")
    )
    if members:
        await conn.execute(
            insert(entity_split_members).values(
                [
                    {"combined_id": combined_id, "ordinal": i, "member_name": m}
                    for i, m in enumerate(members)
                ]
            )
        )


async def _links(conn, rid):
    rows = await conn.execute(
        select(entities.c.name, record_entities.c.position, record_entities.c.via_entity_id)
        .join(entities, entities.c.id == record_entities.c.entity_id)
        .where(record_entities.c.record_id == rid)
        .order_by(record_entities.c.position)
    )
    return [tuple(r) for r in rows]


@pytest.mark.asyncio(loop_scope="session")
async def test_split_adds_member_rows_after_the_printed_ones(pg_conn, standard_new_application):
    rid = await _record(pg_conn, standard_new_application, "t269_a")
    couple = await _id(pg_conn, COUPLE)
    await _verdict(pg_conn, couple, MEMBERS)
    await reprocess_entities(pg_conn, record_ids=[rid])
    # TREVOR is printed: his printed row wins and he gets no member row.
    assert await _links(pg_conn, rid) == [
        (COUPLE, 0, None),
        ("T269 TREVOR EUGENE BLACKWELL", 1, None),
        ("T269 JAYNE MARIE BLACKWELL", 2, couple),
    ]


@pytest.mark.asyncio(loop_scope="session")
async def test_ingest_path_reads_the_verdict_without_a_map(pg_conn, standard_new_application):
    first = await _record(pg_conn, standard_new_application, "t269_b")
    await _verdict(pg_conn, await _id(pg_conn, COUPLE), MEMBERS)
    # A later record printing the couple, linked the way ingest links it.
    later = await _record(pg_conn, standard_new_application, "t269_b2", f"T269 X LLC; {COUPLE}")
    assert [n for n, _, via in await _links(pg_conn, later) if via] == MEMBERS
    assert first != later


@pytest.mark.asyncio(loop_scope="session")
async def test_keep_verdict_changes_nothing(pg_conn, standard_new_application):
    rid = await _record(pg_conn, standard_new_application, "t269_c")
    before = await _links(pg_conn, rid)
    await _verdict(pg_conn, await _id(pg_conn, COUPLE), [], verdict="keep")
    await reprocess_entities(pg_conn, record_ids=[rid])
    assert await _links(pg_conn, rid) == before


@pytest.mark.asyncio(loop_scope="session")
async def test_full_reprocess_rebuilds_member_rows_and_clear_drops_them(
    pg_conn, standard_new_application
):
    """The spec's PG test: verdict, full reprocess, clear, reprocess."""
    rid = await _record(pg_conn, standard_new_application, "t269_d")
    couple = await _id(pg_conn, COUPLE)
    printed = await _links(pg_conn, rid)
    await _verdict(pg_conn, couple, MEMBERS)
    await reprocess_entities(pg_conn)
    after = await _links(pg_conn, rid)
    assert [r for r in after if r[2] is None] == printed
    assert [r[0] for r in after if r[2] == couple] == ["T269 JAYNE MARIE BLACKWELL"]
    await pg_conn.execute(delete(entity_splits).where(entity_splits.c.combined_id == couple))
    await reprocess_entities(pg_conn)
    assert await _links(pg_conn, rid) == printed


@pytest.mark.asyncio(loop_scope="session")
async def test_tombstoned_member_is_skipped(pg_conn, standard_new_application):
    rid = await _record(pg_conn, standard_new_application, "t269_e")
    jayne = await get_or_create_entity(pg_conn, "T269 JAYNE MARIE BLACKWELL")
    await record_manual(
        pg_conn, jayne, entity_type="not_a_name", entity_subtype=None, created_by="t@x"
    )
    await _verdict(pg_conn, await _id(pg_conn, COUPLE), MEMBERS)
    await reprocess_entities(pg_conn, record_ids=[rid])
    assert all(via is None for _, _, via in await _links(pg_conn, rid))


@pytest.mark.asyncio(loop_scope="session")
async def test_link_check_ignores_member_rows(pg_conn, standard_new_application):
    from wslcb_licensing_tracker.entity_wac_repair import _unmatched_records

    rid = await _record(pg_conn, standard_new_application, "t269_f")
    await _verdict(pg_conn, await _id(pg_conn, COUPLE), MEMBERS)
    await reprocess_entities(pg_conn, record_ids=[rid])
    assert await _unmatched_records(pg_conn, [(rid, "applicant")]) == []


# ── PG: guards (spec §5, §5a) ────────────────────────────────────────

from wslcb_licensing_tracker.entities import (  # noqa: E402
    merge_duplicate_entities,
    merge_entity_into,
)
from wslcb_licensing_tracker.entity_aliases import (  # noqa: E402
    AliasError,
    add_manual_alias,
    relink,
)
from wslcb_licensing_tracker.entity_classification import (  # noqa: E402
    SPLIT_NOT_PERSON_REASON,
    protected_ids,
    resolve_entities,
)
from wslcb_licensing_tracker.models import (  # noqa: E402
    entity_aliases,
    entity_classifications,
    entity_review_flags,
)


async def _person(conn, name):
    return await get_or_create_entity(conn, name)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("verdict", ["split", "keep"])
async def test_a_verdict_protects_the_entity(pg_conn, verdict):
    eid = await _person(pg_conn, f"T269 PROTECT {verdict} A & B")
    await _verdict(pg_conn, eid, ["T269 A", "T269 B"] if verdict == "split" else [], verdict)
    assert eid in await protected_ids(pg_conn)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(("etype", "sub"), [("not_a_name", None), ("organization", "trust")])
async def test_manual_type_change_refused_while_a_verdict_exists(pg_conn, etype, sub):
    eid = await _person(pg_conn, "T269 CLASSIFY C & D")
    await _verdict(pg_conn, eid, [], "keep")
    with pytest.raises(ValueError, match="clear the verdict"):
        await record_manual(pg_conn, eid, entity_type=etype, entity_subtype=sub, created_by="t@x")
    # person stays allowed
    await record_manual(pg_conn, eid, entity_type="person", entity_subtype=None, created_by="t@x")


@pytest.mark.asyncio(loop_scope="session")
async def test_automated_flip_flags_a_decided_entity(pg_conn):
    eid = await _person(pg_conn, "T269 FLIP E & F")
    await _verdict(pg_conn, eid, ["T269 E", "T269 F"])
    await pg_conn.execute(
        insert(entity_classifications).values(
            entity_id=eid,
            source="jev",
            entity_type="organization",
            entity_subtype="business",
            confidence=0.99,
            model="t269",
        )
    )
    await resolve_entities(pg_conn, [eid])
    await resolve_entities(pg_conn, [eid])  # no second flag
    reasons = (
        (
            await pg_conn.execute(
                select(entity_review_flags.c.reason).where(
                    (entity_review_flags.c.entity_id == eid)
                    & entity_review_flags.c.resolved_at.is_(None)
                )
            )
        )
        .scalars()
        .all()
    )
    assert reasons == [SPLIT_NOT_PERSON_REASON]
    # The verdict stays in force.
    assert (await pg_conn.execute(select(entity_splits.c.verdict))).scalars().all().count(
        "split"
    ) >= 1


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("side", ["dirty", "canon"])
async def test_merge_refuses_a_decided_entity(pg_conn, side):
    decided = await _person(pg_conn, f"T269 MERGE {side} G & H")
    other = await _person(pg_conn, f"T269 MERGE {side} OTHER")
    await _verdict(pg_conn, decided, [], "keep")
    dirty, canon = (decided, other) if side == "dirty" else (other, decided)
    with pytest.raises(ValueError, match="verdict"):
        await merge_entity_into(pg_conn, dirty, canon)


@pytest.mark.asyncio(loop_scope="session")
async def test_case_dupe_merge_skips_a_decided_entity(pg_conn):
    canon = await _person(pg_conn, "T269 CASE I & J")
    dupe = (
        await pg_conn.execute(
            insert(entities)
            .values(name="t269 case i & j", entity_type="person")
            .returning(entities.c.id)
        )
    ).scalar_one()
    await _verdict(pg_conn, canon, [], "keep")
    await merge_duplicate_entities(pg_conn)
    assert (
        await pg_conn.execute(select(entities.c.id).where(entities.c.id == dupe))
    ).scalar_one_or_none() == dupe


@pytest.mark.asyncio(loop_scope="session")
async def test_split_entity_takes_no_format_key_row(pg_conn):
    a = await _person(pg_conn, "T269 K & L SMITH")
    b = await _person(pg_conn, "SMITH, T269 K & L")
    await relink(pg_conn, [a, b])

    async def _rows():
        return (
            (
                await pg_conn.execute(
                    select(entity_aliases.c.variant_id).where(
                        entity_aliases.c.variant_id.in_([a, b])
                        | entity_aliases.c.canonical_id.in_([a, b])
                    )
                )
            )
            .scalars()
            .all()
        )

    assert await _rows()  # the format key links the two spellings
    await _verdict(pg_conn, a, ["T269 K SMITH", "T269 L SMITH"])
    await relink(pg_conn, [a, b])
    assert await _rows() == []


@pytest.mark.asyncio(loop_scope="session")
async def test_split_entity_takes_no_middle_name_row(pg_conn, standard_new_application):
    names = "ZQ269 BIZ; ZQJON ZQSMYTHE; ZQJON ZQK ZQSMYTHE"
    rid = await _record(pg_conn, standard_new_application, "t269_mn", names)
    short = await _id(pg_conn, "ZQJON ZQSMYTHE")
    await relink(pg_conn, [short])

    async def _mid():
        return (
            await pg_conn.execute(
                select(func.count())
                .select_from(entity_aliases)
                .where(
                    (entity_aliases.c.rule == "middle_names")
                    & (entity_aliases.c.variant_id == short)
                )
            )
        ).scalar_one()

    assert await _mid() == 1
    await _verdict(pg_conn, short, ["ZQANN ZQSMYTHE", "ZQBOB ZQSMYTHE"])
    await reprocess_entities(pg_conn, record_ids=[rid])
    assert await _mid() == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_manual_link_refused_on_a_split_entity(pg_conn):
    split = await _person(pg_conn, "T269 ALIAS M & N")
    other = await _person(pg_conn, "T269 ALIAS OTHER")
    await _verdict(pg_conn, split, ["T269 M", "T269 N"])
    for v, c in ((split, other), (other, split)):
        with pytest.raises(AliasError, match="split"):
            await add_manual_alias(pg_conn, v, c, created_by="t@x")


@pytest.mark.asyncio(loop_scope="session")
async def test_merging_a_member_into_a_printed_entity_keeps_the_printed_row(
    pg_conn, standard_new_application
):
    names = f"T269 Q LLC; {COUPLE}; T269 JAYNE M BLACKWELL"
    rid = await _record(pg_conn, standard_new_application, "t269_merge", names)
    await _verdict(pg_conn, await _id(pg_conn, COUPLE), MEMBERS)
    await reprocess_entities(pg_conn, record_ids=[rid])
    jayne_m = await _id(pg_conn, "T269 JAYNE M BLACKWELL")
    jayne_marie = await _id(pg_conn, "T269 JAYNE MARIE BLACKWELL")
    # The printed spelling folds into the member: the survivor is printed here.
    await merge_entity_into(pg_conn, jayne_m, jayne_marie)
    vias = {n: via for n, _, via in await _links(pg_conn, rid)}
    assert vias["T269 JAYNE MARIE BLACKWELL"] is None
