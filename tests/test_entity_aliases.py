"""Tests for entity_aliases.py — the entity alias layer (#262, spec #247 §1-§3).

The pure functions (``link_key``, ``elect_canonical``, ``resolve``,
``find_cycle``) run without a database; the cases come from the spec's
worked examples.  PG tests (``pg_conn``) cover the write API, ``relink`` and
the hooks.
"""

import pytest
from sqlalchemy import func, insert, select, update

from wslcb_licensing_tracker.entities import (
    _purge_entities,
    get_or_create_entity,
    merge_entity_into,
    reprocess_entities,
)
from wslcb_licensing_tracker.entity_aliases import (
    AliasError,
    AliasRow,
    Member,
    Party,
    add_manual_alias,
    conflict_pairs,
    dismiss_pair,
    eff_many,
    elect_canonical,
    find_cycle,
    link_key,
    middles_compatible,
    parse_person_key,
    plan_licence,
    relink,
    remove_manual_alias,
    resolve,
    undismiss_pair,
)
from wslcb_licensing_tracker.entity_classification import clear_manual, protected_ids, record_manual
from wslcb_licensing_tracker.models import (
    entities,
    entity_alias_dismissals,
    entity_aliases,
    entity_classifications,
    entity_review_flags,
    license_records,
    record_entities,
)
from wslcb_licensing_tracker.pipeline import IngestOptions, ingest_record, insert_record

# ── link_key (spec §2) ───────────────────────────────────────────────


@pytest.mark.parametrize(
    ("name", "etype", "expected"),
    [
        # LAST, FIRST [MIDDLE…] reorders
        ("ZHAO, QIAN", "person", "QIAN ZHAO"),
        ("PENDERGRAFT, TY", "person", "TY PENDERGRAFT"),
        ("SMITH, JOHN ALLEN", "person", "JOHN ALLEN SMITH"),
        # no comma: unchanged apart from , . → space
        ("ZHAO QIAN", "person", "ZHAO QIAN"),
        ("H.S. WRIGHT III", "person", "H S WRIGHT III"),
        ("H S WRIGHT III", "person", "H S WRIGHT III"),
        # a generational tail is not a reorder
        ("JOHN SMITH, JR", "person", "JOHN SMITH JR"),
        ("JOHN SMITH, JR.", "person", "JOHN SMITH JR"),
        ("JOHN SMITH, III", "person", "JOHN SMITH III"),
        # two commas: no reorder, punctuation only
        ("SMITH, JOHN, JR", "person", "SMITH JOHN JR"),
        # organization: role annotation stripped, then punctuation
        ("BP WEST COAST PRODUCTS, LLC", "organization", "BP WEST COAST PRODUCTS LLC"),
        ("BP WEST COAST PRODUCTS LLC (FRANCHISOR)", "organization", "BP WEST COAST PRODUCTS LLC"),
        ("7-ELEVEN, INC. - FRANCHISOR", "organization", "7-ELEVEN INC"),
        ("7-ELEVEN,INC", "organization", "7-ELEVEN INC"),
        ("7-ELEVEN INC FRANCHISEE", "organization", "7-ELEVEN INC"),
        # an org comma never reorders
        ("VUE, SUNLIGHT", "organization", "VUE SUNLIGHT"),
        ("VUE, SUNLIGHT", "person", "SUNLIGHT VUE"),
        # legacy / unknown types have no key
        ("ANYTHING", "", None),
        ("ANYTHING", "not_a_name", None),
    ],
)
def test_link_key(name, etype, expected):
    assert link_key(name, etype) == expected


# ── elect_canonical (spec §1) ────────────────────────────────────────

# Real prod names and link counts (spec Appendix B).
BP = [
    Member(19027, "BP WEST COAST PRODUCTS LLC, FRANCHISOR", 6),
    Member(19360, "BP WEST COAST PRODUCTS LLC", 13),
    Member(27626, "BP WEST COAST PRODUCTS, LLC", 18),
    Member(27630, "BP WEST COAST PRODUCTS,LLC", 2),
    Member(28073, "BP WEST COAST PRODUCTS, LLC FRANCHISOR", 1),
]
SEVEN_ELEVEN = [
    Member(18334, "7-ELEVEN, INC (FRANCHISOR)", 1),
    Member(18338, "7-ELEVEN, INC. FRANCHISOR", 3),
    Member(18623, "7-ELEVEN,INC", 14),
    Member(19771, "7-ELEVEN, INC. (FRANCHISOR)", 1),
    Member(20242, "7-ELEVEN, INC. - FRANCHISOR", 1),
    Member(25508, "7-ELEVEN, INC., FRANCHISOR", 1),
    Member(29097, "7-ELEVEN, INC.", 4),
    Member(29325, "7-ELEVEN INC. FRANCHISOR", 1),
]


@pytest.mark.parametrize("group", [BP, SEVEN_ELEVEN], ids=["bp", "7-eleven"])
def test_real_groups_share_one_key(group):
    assert len({link_key(m.name, "organization") for m in group}) == 1


def test_elect_seven_eleven():
    assert elect_canonical("organization", SEVEN_ELEVEN) == 18623


def test_elect_org_prefers_no_annotation_then_links():
    assert elect_canonical("organization", BP) == 27626


def test_elect_person_prefers_natural_order_over_links():
    members = [Member(19742, "ZHAO, QIAN", 50), Member(80, "QIAN ZHAO", 5)]
    assert elect_canonical("person", members) == 80


def test_elect_person_generational_comma_counts_as_natural():
    members = [Member(1, "SMITH, JOHN", 9), Member(2, "JOHN SMITH, JR", 1)]
    # "JOHN SMITH, JR" has no reorder comma; "SMITH, JOHN" does.
    assert elect_canonical("person", members) == 2


def test_elect_ties_break_on_lowest_id():
    members = [Member(7, "A B", 3), Member(5, "A. B", 3)]
    assert elect_canonical("person", members) == 5


def test_elect_current_canonical_sticks_while_eligible():
    assert elect_canonical("organization", BP, current=27630) == 27630


def test_elect_current_dropped_when_it_left_the_group():
    assert elect_canonical("organization", BP, current=99999) == 27626


def test_elect_manual_target_preferred():
    assert elect_canonical("organization", BP, manual_targets={28073}) == 28073


def test_elect_manual_variant_never_elected_even_if_current():
    got = elect_canonical("organization", BP, current=27626, manual_variants={27626})
    assert got == 19360


def test_elect_all_manual_variants_gives_none():
    members = [Member(1, "A B", 1), Member(2, "A. B", 1)]
    assert elect_canonical("person", members, manual_variants={1, 2}) is None


# ── resolve (spec §1 Resolution; worked case Pendergraft) ────────────

LIC = "355693"
PENDERGRAFT = [
    AliasRow(24234, 38707, None, "rule"),  # PENDERGRAFT, TY → TY PENDERGRAFT
    AliasRow(83154, 38708, LIC, "rule"),  # TYLER → TYLER DAVID
    AliasRow(38707, 38708, LIC, "manual"),  # TY PENDERGRAFT → TYLER DAVID
    AliasRow(24142, 38707, LIC, "manual"),  # TY → TY PENDERGRAFT
]


@pytest.mark.parametrize("eid", [24234, 83154, 38707, 24142, 38708])
def test_resolve_pendergraft_all_reach_38708_on_licence(eid):
    assert resolve(eid, LIC, PENDERGRAFT) == 38708


def test_resolve_outside_licence_scoped_rows_do_not_apply():
    assert resolve(24142, "999999", PENDERGRAFT) == 24142
    assert resolve(24234, "999999", PENDERGRAFT) == 38707
    assert resolve(24234, None, PENDERGRAFT) == 38707


def test_resolve_scoped_beats_global():
    rows = [AliasRow(1, 2, None, "manual"), AliasRow(1, 3, "L", "rule")]
    assert resolve(1, "L", rows) == 3
    assert resolve(1, "M", rows) == 2


def test_resolve_manual_beats_rule_at_same_scope():
    rows = [AliasRow(1, 2, None, "rule"), AliasRow(1, 3, None, "manual")]
    assert resolve(1, None, rows) == 3


def test_resolve_caps_at_four_steps():
    rows = [AliasRow(i, i + 1, None, "rule") for i in range(1, 10)]
    assert resolve(1, None, rows) == 5


# ── find_cycle ───────────────────────────────────────────────────────


def test_find_cycle_global_closes_through_scoped():
    rows = [AliasRow(2, 3, "L", "manual"), AliasRow(3, 1, None, "rule")]
    # new 1 → 2 globally: on licence L, 2 → 3 → 1 closes the loop
    assert find_cycle(1, 2, None, rows) == rows


def test_find_cycle_scopes_on_different_licences_do_not_combine():
    rows = [AliasRow(2, 3, "L", "manual"), AliasRow(3, 1, "M", "manual")]
    assert find_cycle(1, 2, None, rows) is None
    assert find_cycle(1, 2, "L", rows) is None


def test_find_cycle_scoped_new_row_sees_global_and_same_scope():
    rows = [AliasRow(2, 3, None, "manual"), AliasRow(3, 1, "L", "manual")]
    assert find_cycle(1, 2, "L", rows) is not None
    assert find_cycle(1, 2, "M", rows) is None


def test_find_cycle_self_link():
    assert find_cycle(1, 1, None, []) == []


# ── middle-name rule, pure (#265; spec §3) ───────────────────────────


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("TYLER DAVID PENDERGRAFT", ("TYLER", "PENDERGRAFT", None, ("DAVID",))),
        ("TYLER PENDERGRAFT", ("TYLER", "PENDERGRAFT", None, ())),
        ("JOHN SMITH JR", ("JOHN", "SMITH", "JR", ())),
        ("JOHN A SMITH III", ("JOHN", "SMITH", "III", ("A",))),
        ("H S WRIGHT III", ("H", "WRIGHT", "III", ("S",))),
        ("JOHN B C SMITH", ("JOHN", "SMITH", None, ("B", "C"))),
        # one token, couples, nothing: no part in the rule
        ("TY", None),
        ("WASSEF & RACHA HAROUN", None),
        ("RENEE AND TIM OCHS", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_person_key(key, expected):
    got = parse_person_key(key)
    assert (got if got is None else (got.first, got.last, got.suffix, got.middles)) == expected


@pytest.mark.parametrize(
    ("a", "b", "ok"),
    [
        ((), ("DAVID",), True),
        ((), ("B", "C"), True),
        (("A",), ("ALLEN",), True),
        (("ALLEN",), ("A",), True),
        (("ALLEN",), ("ALLEN",), True),
        (("J", "R"), ("JAMES", "R"), True),
        (("ALLEN",), ("ANDREW",), False),
        (("A",), ("B",), False),
        (("B",), ("B", "C"), False),
        (("AL",), ("ALLEN",), False),
    ],
)
def test_middles_compatible(a, b, ok):
    assert middles_compatible(a, b) is ok
    assert middles_compatible(b, a) is ok


def _p(pid, name, roles=("applicant",), links=1):
    return Party(pid, name, link_key(name, "person"), frozenset(roles), links)


def test_plan_licence_pendergraft():
    plan = plan_licence([_p(83154, "TYLER PENDERGRAFT"), _p(38708, "TYLER DAVID PENDERGRAFT")])
    assert plan.rows == {83154: 38708}
    assert plan.conflicts == []


def test_plan_licence_links_every_form_to_the_longest():
    plan = plan_licence(
        [_p(1, "JOHN SMITH"), _p(2, "JOHN A SMITH"), _p(3, "JOHN ALLEN SMITH"), _p(4, "MARY SMITH")]
    )
    assert plan.rows == {1: 3, 2: 3}


def test_plan_licence_suffix_only_matches_the_same_suffix():
    plan = plan_licence(
        [
            _p(1, "JOHN SMITH"),
            _p(2, "JOHN A SMITH JR"),
            _p(3, "JOHN SMITH JR"),
            _p(4, "JOHN SMITH SR"),
        ]
    )
    assert plan.rows == {3: 2}


@pytest.mark.parametrize(
    "names",
    [
        ["JOHN B SMITH", "JOHN B C SMITH"],
        ["JOHN SMITH", "JOHN A SMITH", "JOHN B SMITH"],  # one conflict poisons the group
        ["JOHN ALLEN SMITH", "JOHN ANDREW SMITH"],
    ],
)
def test_plan_licence_a_conflict_writes_nothing(names):
    plan = plan_licence([_p(i, n) for i, n in enumerate(names, 1)])
    assert plan.rows == {}
    assert [sorted(p.id for p in g) for g in plan.conflicts] == [list(range(1, len(names) + 1))]


def test_plan_licence_needs_a_shared_role():
    plan = plan_licence([_p(1, "JOHN SMITH", roles=("previous_applicant",)), _p(2, "JOHN A SMITH")])
    assert plan.rows == {}
    assert plan.skipped_role == 1
    both = plan_licence(
        [_p(1, "JOHN SMITH", roles=("previous_applicant", "applicant")), _p(2, "JOHN A SMITH")]
    )
    assert both.rows == {1: 2}


def test_plan_licence_skips_a_dismissed_pair_only():
    plan = plan_licence(
        [_p(1, "JOHN SMITH"), _p(2, "JOHN A SMITH"), _p(3, "JOHN ALLEN SMITH")],
        dismissed={(1, 3)},
    )
    assert plan.rows == {2: 3}
    assert plan.skipped_dismissed == 1


def test_plan_licence_couples_and_single_tokens_take_no_part():
    plan = plan_licence(
        [
            _p(1, "RENEE OCHS"),
            _p(2, "RENEE ELIZABETH OCHS"),
            _p(3, "RENEE AND TIM OCHS"),
            _p(4, "RENEE"),
        ]
    )
    assert plan.rows == {1: 2}
    assert plan.conflicts == []


def test_plan_licence_tie_breaks():
    # most characters, then natural order, then most links, then lowest id
    assert plan_licence([_p(1, "JOHN A SMITH"), _p(2, "JOHN ALLEN SMITH")]).rows == {1: 2}
    assert plan_licence([_p(1, "SMITH, JOHN A"), _p(2, "JOHN A SMITH")]).rows == {1: 2}
    assert plan_licence([_p(1, "JOHN A SMITH"), _p(2, "JOHN A. SMITH", links=5)]).rows == {1: 2}
    assert plan_licence([_p(2, "JOHN A SMITH"), _p(1, "JOHN A. SMITH")]).rows == {2: 1}


def test_conflict_pairs_run_shorter_to_longer_and_need_a_shared_role():
    group = [
        _p(1, "JOHN SMITH"),
        _p(2, "JOHN A SMITH"),
        _p(3, "JOHN B SMITH", links=3),
        _p(4, "JOHN C SMITH", roles=("previous_applicant",)),
    ]
    assert conflict_pairs(group) == [(2, 3), (1, 3), (1, 2)]


# ── PG: relink + write API ───────────────────────────────────────────

PG = pytest.mark.asyncio(loop_scope="session")


async def _ent(conn, name, etype="person", subtype=None):
    eid = await get_or_create_entity(conn, name)
    if etype != "person" or subtype is not None:
        await conn.execute(
            update(entities)
            .where(entities.c.id == eid)
            .values(entity_type=etype, entity_subtype=subtype)
        )
    return eid


async def _org(conn, name):
    return await _ent(conn, name, "organization", "business")


async def _rows(conn, *ids):
    stmt = select(
        entity_aliases.c.variant_id,
        entity_aliases.c.canonical_id,
        entity_aliases.c.scope_license,
        entity_aliases.c.source,
        entity_aliases.c.rule,
    ).where(entity_aliases.c.variant_id.in_(ids) | entity_aliases.c.canonical_id.in_(ids))
    return {tuple(r) for r in (await conn.execute(stmt)).fetchall()}


def _fk(v, c):
    return (v, c, None, "rule", "format_key")


def _mid(v, c, lic):
    return (v, c, lic, "rule", "middle_names")


@PG
async def test_relink_groups_comma_and_annotation_not_across_types(pg_conn):
    comma = await _ent(pg_conn, "ZQTEST, ALPHA")
    natural = await _ent(pg_conn, "ALPHA ZQTEST")
    org_a = await _org(pg_conn, "ZQTEST WIDGETS, LLC")
    org_b = await _org(pg_conn, "ZQTEST WIDGETS LLC (FRANCHISOR)")
    p = await _ent(pg_conn, "VUEZQ, SUNLIGHT")
    o = await _org(pg_conn, "SUNLIGHT VUEZQ")
    ids = [comma, natural, org_a, org_b, p, o]

    result = await relink(pg_conn, ids)

    assert await _rows(pg_conn, *ids) == {_fk(comma, natural), _fk(org_b, org_a)}
    assert result.inserted == 2
    keys = dict(
        (
            await pg_conn.execute(
                select(entities.c.id, entities.c.link_key).where(entities.c.id.in_(ids))
            )
        ).fetchall()
    )
    assert keys[comma] == keys[natural] == "ALPHA ZQTEST"
    assert keys[p] == keys[o] == "SUNLIGHT VUEZQ"  # same key, different types: no row


@PG
async def test_get_or_create_entity_writes_link_key(pg_conn):
    eid = await get_or_create_entity(pg_conn, "ZQNEW, OMEGA")
    key = (
        await pg_conn.execute(select(entities.c.link_key).where(entities.c.id == eid))
    ).scalar_one()
    assert key == "OMEGA ZQNEW"


@PG
async def test_relink_second_run_changes_nothing(pg_conn):
    a = await _ent(pg_conn, "ZQTWICE, BETA")
    b = await _ent(pg_conn, "BETA ZQTWICE")
    first = await relink(pg_conn, [a, b])
    assert first.changed > 0
    second = await relink(pg_conn, [a, b])
    assert (second.changed, second.kept) == (0, 1)


@PG
async def test_relink_dry_run_writes_nothing(pg_conn):
    a = await _ent(pg_conn, "ZQDRY, GAMMA")
    b = await _ent(pg_conn, "GAMMA ZQDRY")
    result = await relink(pg_conn, [a, b], dry_run=True)
    assert result.inserted == 1
    assert await _rows(pg_conn, a, b) == set()


@PG
async def test_relink_current_canonical_sticks(pg_conn):
    comma = await _ent(pg_conn, "ZQSTICK, DELTA")
    natural = await _ent(pg_conn, "DELTA ZQSTICK")
    await pg_conn.execute(
        insert(entity_aliases).values(
            variant_id=natural, canonical_id=comma, source="rule", rule="format_key"
        )
    )
    result = await relink(pg_conn, [comma, natural])
    assert (result.inserted, result.deleted, result.kept) == (0, 0, 1)
    assert await _rows(pg_conn, comma) == {_fk(natural, comma)}


@PG
async def test_relink_type_change_drops_row(pg_conn):
    a = await _ent(pg_conn, "ZQTYPE, EPSILON")
    b = await _ent(pg_conn, "EPSILON ZQTYPE")
    await relink(pg_conn, [a, b])
    await pg_conn.execute(
        update(entities)
        .where(entities.c.id == a)
        .values(entity_type="organization", entity_subtype="business")
    )
    await relink(pg_conn, [a])
    assert await _rows(pg_conn, a, b) == set()


@PG
async def test_relink_excludes_tombstones(pg_conn):
    a = await _ent(pg_conn, "ZQTOMB, ZETA")
    b = await _ent(pg_conn, "ZETA ZQTOMB")
    await relink(pg_conn, [a, b])
    await pg_conn.execute(
        insert(entity_classifications).values(
            entity_id=a, source="manual", entity_type="not_a_name", created_by="t@example.com"
        )
    )
    await relink(pg_conn, [a])
    assert await _rows(pg_conn, a, b) == set()


@PG
async def test_dismissal_blocks_rule_and_undismiss_restores(pg_conn):
    a = await _ent(pg_conn, "ZQDISM, ETA")
    b = await _ent(pg_conn, "ETA ZQDISM")
    await relink(pg_conn, [a, b])
    assert await dismiss_pair(pg_conn, b, a, scope_license="123456", dismissed_by="t@example.com")
    assert await _rows(pg_conn, a, b) == set()
    assert not await dismiss_pair(
        pg_conn, a, b, scope_license="123456", dismissed_by="t@example.com"
    )
    assert await undismiss_pair(pg_conn, a, b, scope_license="123456")
    assert await _rows(pg_conn, a, b) == {_fk(a, b)}


@PG
async def test_rule_row_closing_a_cycle_is_skipped(pg_conn):
    v = await _ent(pg_conn, "ZQCYC, THETA")
    c = await _ent(pg_conn, "THETA ZQCYC")
    x = await _ent(pg_conn, "THETA Q ZQCYC")
    y = await _ent(pg_conn, "ZQCYC OTHER")
    # operator rows c → x → v on licence L1; y → c keeps c the elected canonical
    await _raw(pg_conn, c, x, scope="L1")
    await _raw(pg_conn, x, v, scope="L1")
    await _raw(pg_conn, y, c)
    result = await relink(pg_conn, [v, c, x])
    assert result.skipped_cycle == 1
    assert _fk(v, c) not in await _rows(pg_conn, v)


@PG
async def test_manual_rows_survive_full_relink(pg_conn):
    a = await _ent(pg_conn, "ZQMAN IOTA")
    b = await _ent(pg_conn, "IOTA ZQMAN")
    rid = await add_manual_alias(
        pg_conn, a, b, scope_license="355693", created_by="t@example.com", note="nickname"
    )
    await relink(pg_conn)
    assert await _rows(pg_conn, a, b) == {(a, b, "355693", "manual", None)}
    assert isinstance(rid, int)


@PG
async def test_add_manual_alias_refusals(pg_conn):
    a = await _ent(pg_conn, "ZQREF KAPPA")
    b = await _ent(pg_conn, "ZQREF LAMBDA")
    c = await _ent(pg_conn, "ZQREF MU")
    with pytest.raises(AliasError, match="itself"):
        await add_manual_alias(pg_conn, a, a, created_by="t@example.com")
    with pytest.raises(LookupError):
        await add_manual_alias(pg_conn, a, 2_000_000_000, created_by="t@example.com")
    await add_manual_alias(pg_conn, a, b, created_by="t@example.com")
    await add_manual_alias(pg_conn, b, c, scope_license="L9", created_by="t@example.com")
    with pytest.raises(AliasError, match="already linked"):
        await add_manual_alias(pg_conn, a, c, created_by="t@example.com")
    with pytest.raises(AliasError, match=f"cycle: {c} → {a}|cycle: .*{b}"):
        await add_manual_alias(pg_conn, c, a, scope_license="L9", created_by="t@example.com")
    # on another licence the scoped b → c row doesn't apply: no cycle
    await add_manual_alias(pg_conn, c, a, scope_license="L8", created_by="t@example.com")
    await pg_conn.execute(
        insert(entity_classifications).values(
            entity_id=c, source="manual", entity_type="not_a_name", created_by="t@example.com"
        )
    )
    d = await _ent(pg_conn, "ZQREF NU")
    with pytest.raises(AliasError, match="not-a-name"):
        await add_manual_alias(pg_conn, d, c, created_by="t@example.com")


@PG
async def test_cross_type_manual_link_opens_one_flag(pg_conn):
    p = await _ent(pg_conn, "ZQX XI")
    o = await _org(pg_conn, "ZQX XI HOLDINGS LLC")
    q = await _ent(pg_conn, "ZQX OMICRON")
    await add_manual_alias(pg_conn, p, o, created_by="t@example.com")
    await add_manual_alias(pg_conn, q, o, scope_license="L1", created_by="t@example.com")
    flags = (
        (
            await pg_conn.execute(
                select(entity_review_flags.c.reason).where(entity_review_flags.c.entity_id == o)
            )
        )
        .scalars()
        .all()
    )
    assert flags == ["members disagree on type"]


@PG
async def test_remove_manual_alias(pg_conn):
    a = await _ent(pg_conn, "ZQRM PI")
    b = await _ent(pg_conn, "ZQRM RHO")
    rid = await add_manual_alias(pg_conn, a, b, created_by="t@example.com")
    assert (await remove_manual_alias(pg_conn, rid)).variant == a
    assert await _rows(pg_conn, a, b) == set()
    with pytest.raises(LookupError):
        await remove_manual_alias(pg_conn, rid)


@PG
async def test_protected_ids_cover_manual_rows_and_dismissals(pg_conn):
    a = await _ent(pg_conn, "ZQPROT SIGMA")
    b = await _ent(pg_conn, "ZQPROT TAU")
    c = await _ent(pg_conn, "ZQPROT UPSILON")
    d = await _ent(pg_conn, "ZQPROT PHI")
    await add_manual_alias(pg_conn, a, b, created_by="t@example.com")
    await dismiss_pair(pg_conn, c, d, dismissed_by="t@example.com")
    assert {a, b, c, d} <= await protected_ids(pg_conn)


@PG
async def test_eff_many_follows_db_rows(pg_conn):
    a = await _ent(pg_conn, "ZQEFF, CHI")
    b = await _ent(pg_conn, "CHI ZQEFF")
    c = await _ent(pg_conn, "CHI PSI ZQEFF")
    await relink(pg_conn, [a, b])
    await add_manual_alias(pg_conn, c, b, scope_license="L5", created_by="t@example.com")
    got = await eff_many(pg_conn, [(a, "L5"), (a, None), (c, "L5"), (c, "L6"), (b, None)])
    assert got == {(a, "L5"): b, (a, None): b, (c, "L5"): b, (c, "L6"): c, (b, None): b}


@PG
async def test_global_manual_variant_is_never_a_rule_canonical(pg_conn):
    """Spec §1: a global manual variant makes its format-key group re-elect."""
    a = await _ent(pg_conn, "ZQMV, PSI")
    b = await _ent(pg_conn, "PSI ZQMV")
    c = await _ent(pg_conn, "PSI Q ZQMV")
    await relink(pg_conn, [a, b])
    assert await _rows(pg_conn, a, b) == {_fk(a, b)}
    await add_manual_alias(pg_conn, b, c, created_by="t@example.com")
    assert await _rows(pg_conn, a, b) == {_fk(b, a), (b, c, None, "manual", None)}
    got = await eff_many(pg_conn, [(b, None), (a, None)])
    assert got == {(b, None): c, (a, None): a}


@PG
async def test_scoped_manual_variant_stays_canonical(pg_conn):
    """A scoped manual variant keeps its group; on its licence the chain runs through it."""
    a = await _ent(pg_conn, "ZQSV, OMEGA")
    b = await _ent(pg_conn, "OMEGA ZQSV")
    c = await _ent(pg_conn, "OMEGA Q ZQSV")
    await relink(pg_conn, [a, b])
    await add_manual_alias(pg_conn, b, c, scope_license="L5", created_by="t@example.com")
    assert _fk(a, b) in await _rows(pg_conn, a)
    got = await eff_many(pg_conn, [(a, "L5"), (a, "L6")])
    assert got == {(a, "L5"): c, (a, "L6"): b}


@PG
async def test_worked_case_pendergraft(pg_conn, standard_new_application):
    """Spec worked case: on 355693 all five entities resolve to TYLER DAVID PENDERGRAFT."""
    lic = "355693"
    _, ids = await _link(
        pg_conn,
        standard_new_application,
        lic,
        "PENDERGRAFT, TY",
        "TY PENDERGRAFT",
        "TYLER PENDERGRAFT",
        "TYLER DAVID PENDERGRAFT",
        "TY",
    )
    comma_ty, ty_p, tyler, tyler_david, ty = ids
    await relink(pg_conn, ids)  # steps 1 and 2: format key, middle names
    assert await _rows(pg_conn, tyler) == {_mid(tyler, tyler_david, lic)}
    await add_manual_alias(pg_conn, ty_p, tyler_david, scope_license=lic, created_by="t@x")
    await add_manual_alias(pg_conn, ty, ty_p, scope_license=lic, created_by="t@x")
    await relink(pg_conn)

    got = await eff_many(pg_conn, [(e, lic) for e in ids])
    assert set(got.values()) == {tyler_david}
    outside = await eff_many(pg_conn, [(ty, "999999"), (comma_ty, "999999")])
    assert outside == {(ty, "999999"): ty, (comma_ty, "999999"): ty_p}


# ── PG: hooks (plan step 5) ──────────────────────────────────────────


@PG
async def test_ingest_links_a_new_spelling_into_its_group(pg_conn, standard_new_application):
    natural = await _ent(pg_conn, "ALPHA ZQINGEST")
    record = standard_new_application | {
        "applicants": "ACME CANNABIS CO; ZQINGEST, ALPHA",
        "license_number": "ZQ0001",
    }
    result = await ingest_record(pg_conn, record, IngestOptions(link_outcomes=False))
    comma = (
        await pg_conn.execute(select(entities.c.id).where(entities.c.name == "ZQINGEST, ALPHA"))
    ).scalar_one()
    assert result.is_new
    assert await _rows(pg_conn, natural, comma) == {_fk(comma, natural)}


@PG
async def test_type_change_relinks_in_the_same_transaction(pg_conn):
    a = await _ent(pg_conn, "ZQFLIP, BETA")
    b = await _ent(pg_conn, "BETA ZQFLIP")
    await relink(pg_conn, [a, b])
    await record_manual(
        pg_conn, a, entity_type="organization", entity_subtype="business", created_by="t@x"
    )
    assert await _rows(pg_conn, a, b) == set()
    await clear_manual(pg_conn, a)
    assert await _rows(pg_conn, a, b) == {_fk(a, b)}


@PG
async def test_type_flip_on_a_manual_member_opens_a_flag(pg_conn):
    a = await _ent(pg_conn, "ZQMFLIP GAMMA")
    b = await _ent(pg_conn, "ZQMFLIP DELTA")
    await add_manual_alias(pg_conn, a, b, created_by="t@x")
    await record_manual(
        pg_conn, a, entity_type="organization", entity_subtype="trust", created_by="t@x"
    )
    reasons = (
        (
            await pg_conn.execute(
                select(entity_review_flags.c.reason).where(entity_review_flags.c.entity_id == b)
            )
        )
        .scalars()
        .all()
    )
    assert reasons == ["members disagree on type"]
    assert (a, b, None, "manual", None) in await _rows(pg_conn, a)


@PG
async def test_not_a_name_mark_drops_rule_rows_and_is_refused_on_manual_members(pg_conn):
    a = await _ent(pg_conn, "ZQNAN, EPSILON")
    b = await _ent(pg_conn, "EPSILON ZQNAN")
    await relink(pg_conn, [a, b])
    await record_manual(pg_conn, a, entity_type="not_a_name", entity_subtype=None, created_by="t@x")
    assert await _rows(pg_conn, a, b) == set()

    c = await _ent(pg_conn, "ZQNAN ZETA")
    d = await _ent(pg_conn, "ZQNAN ETA")
    await add_manual_alias(pg_conn, c, d, created_by="t@x")
    with pytest.raises(ValueError, match="unlink"):
        await record_manual(
            pg_conn, d, entity_type="not_a_name", entity_subtype=None, created_by="t@x"
        )


@PG
async def test_merge_repoints_operator_rows_and_relinks(pg_conn):
    dirty = await _ent(pg_conn, "ZQMERGE THETA DUP")
    canon = await _ent(pg_conn, "ZQMERGE THETA")
    other = await _ent(pg_conn, "ZQMERGE IOTA")
    third = await _ent(pg_conn, "ZQMERGE KAPPA")
    await add_manual_alias(pg_conn, dirty, other, scope_license="L1", created_by="t@x")
    await add_manual_alias(pg_conn, third, dirty, created_by="t@x")
    await dismiss_pair(pg_conn, dirty, third, scope_license="L2", dismissed_by="t@x")
    await dismiss_pair(pg_conn, dirty, canon, dismissed_by="t@x")

    await merge_entity_into(pg_conn, dirty, canon)

    assert await _rows(pg_conn, canon, other, third) == {
        (canon, other, "L1", "manual", None),
        (third, canon, None, "manual", None),
    }
    pairs = (
        await pg_conn.execute(
            select(
                entity_alias_dismissals.c.entity_id_a,
                entity_alias_dismissals.c.entity_id_b,
                entity_alias_dismissals.c.scope_license,
            ).where(
                entity_alias_dismissals.c.entity_id_a.in_([canon, third])
                | entity_alias_dismissals.c.entity_id_b.in_([canon, third])
            )
        )
    ).fetchall()
    assert {tuple(p) for p in pairs} == {(*sorted((canon, third)), "L2")}


@PG
async def test_purge_re_elects_the_group_canonical(pg_conn):
    comma = await _ent(pg_conn, "ZQPURGE, LAMBDA")
    natural = await _ent(pg_conn, "LAMBDA ZQPURGE")
    dotted = await _ent(pg_conn, "LAMBDA. ZQPURGE")
    await relink(pg_conn, [comma, natural, dotted])
    assert await _rows(pg_conn, natural) == {_fk(comma, natural), _fk(dotted, natural)}
    await _purge_entities(pg_conn, [natural])
    assert await _rows(pg_conn, comma, dotted) == {_fk(comma, dotted)}


@PG
async def test_relink_never_deletes_classification_rows(pg_conn):
    a = await _ent(pg_conn, "ZQJEV, MU")
    b = await _ent(pg_conn, "MU ZQJEV")
    await pg_conn.execute(
        insert(entity_classifications).values(
            entity_id=a, source="jev", entity_type="person", confidence=0.99, model="jev-test"
        )
    )
    before = (
        await pg_conn.execute(select(func.count()).select_from(entity_classifications))
    ).scalar_one()
    await relink(pg_conn, [a, b])
    await relink(pg_conn)
    after = (
        await pg_conn.execute(select(func.count()).select_from(entity_classifications))
    ).scalar_one()
    assert before == after


# ── CR findings ──────────────────────────────────────────────────────


async def _raw(conn, v, c, *, scope=None, **values):
    row = {"source": "manual", "rule": None} | values
    await conn.execute(
        insert(entity_aliases).values(variant_id=v, canonical_id=c, scope_license=scope, **row)
    )


@PG
async def test_targeted_relink_sees_rule_rows_of_unloaded_entities(pg_conn):
    """CR 1: a loop through an unloaded entity's format-key row is still caught."""
    v = await _ent(pg_conn, "ZQOUT, ALPHA")
    c = await _ent(pg_conn, "ALPHA ZQOUT")
    x = await _ent(pg_conn, "ZQOUTX, BETA")
    y = await _ent(pg_conn, "BETA ZQOUTX")
    z = await _ent(pg_conn, "ZQOUT GAMMA")
    await _raw(pg_conn, c, x, scope="L1")
    await _raw(pg_conn, x, y, source="rule", rule="format_key")
    await _raw(pg_conn, y, v, scope="L1")
    await _raw(pg_conn, z, c)  # c stays the elected canonical (both are manual targets)

    result = await relink(pg_conn, [v])

    assert result.skipped_cycle == 1
    assert _fk(v, c) not in await _rows(pg_conn, v)


@PG
async def test_merge_drops_a_repointed_row_that_would_close_a_cycle(pg_conn):
    """CR 2: X → canon and dirty → X; merging dirty into canon must not leave canon → X."""
    dirty = await _ent(pg_conn, "ZQLOOP NU DUP")
    canon = await _ent(pg_conn, "ZQLOOP NU")
    x = await _ent(pg_conn, "ZQLOOP XI")
    await add_manual_alias(pg_conn, x, canon, created_by="t@x")
    await add_manual_alias(pg_conn, dirty, x, scope_license="L1", created_by="t@x")

    await merge_entity_into(pg_conn, dirty, canon)

    assert await _rows(pg_conn, canon, x) == {(x, canon, None, "manual", None)}


@PG
async def test_manual_link_against_a_rule_row_wins_and_regroups(pg_conn):
    """CR 3: rule A → B exists; operator B → A on L1 is accepted and A becomes the canonical."""
    a = await _ent(pg_conn, "ZQWIN, OMICRON")
    b = await _ent(pg_conn, "OMICRON ZQWIN")
    await relink(pg_conn, [a, b])
    assert await _rows(pg_conn, a, b) == {_fk(a, b)}

    await add_manual_alias(pg_conn, b, a, scope_license="L1", created_by="t@x")

    assert await _rows(pg_conn, a, b) == {_fk(b, a), (b, a, "L1", "manual", None)}
    got = await eff_many(pg_conn, [(b, "L1"), (b, "L2"), (a, "L1"), (a, None)])
    assert set(got.values()) == {a}


@PG
async def test_manual_cycle_among_manual_rows_is_still_refused(pg_conn):
    a = await _ent(pg_conn, "ZQSTILL PI")
    b = await _ent(pg_conn, "ZQSTILL RHO")
    await add_manual_alias(pg_conn, a, b, created_by="t@x")
    with pytest.raises(AliasError, match="cycle"):
        await add_manual_alias(pg_conn, b, a, created_by="t@x")


@PG
async def test_dismissing_a_tombstone_is_refused(pg_conn):
    """CR 6: reclassify purges confirmed non-names, so such a dismissal would vanish."""
    a = await _ent(pg_conn, "ZQTD SIGMA")
    b = await _ent(pg_conn, "ZQTD TAU")
    await record_manual(pg_conn, a, entity_type="not_a_name", entity_subtype=None, created_by="t@x")
    with pytest.raises(AliasError, match="not-a-name"):
        await dismiss_pair(pg_conn, a, b, dismissed_by="t@x")


@PG
async def test_reprocess_relinks_the_entities_it_links(pg_conn, standard_new_application):
    """CR 7: a spelling first created by a reprocess joins its format-key group."""
    natural = await _ent(pg_conn, "UPSILON ZQREPRO")
    rec = standard_new_application | {
        "license_number": "zq_cr7",
        "applicants": "ACME LLC; ZQREPRO, UPSILON",
    }
    record_id = (await insert_record(pg_conn, rec))[0]

    await reprocess_entities(pg_conn, record_id=record_id)

    comma = (
        await pg_conn.execute(select(entities.c.id).where(entities.c.name == "ZQREPRO, UPSILON"))
    ).scalar_one()
    assert await _rows(pg_conn, natural, comma) == {_fk(comma, natural)}


# ── #263: record_entities.party_id kept equal to eff(entity, licence) ─


async def _link(conn, standard_new_application, licence, *names, role="applicant"):
    """Insert a record on *licence* and link *names* to it; return (record id, entity ids)."""
    rec = standard_new_application | {
        "license_number": licence,
        "applicants": "; ".join(["ACME LLC", *names]),
        "record_date": f"2025-06-{len(names) + 10:02d}",
    }
    rid = (await insert_record(conn, rec))[0]
    ids = []
    for pos, name in enumerate(names):
        eid = await get_or_create_entity(conn, name)
        await conn.execute(
            insert(record_entities).values(record_id=rid, entity_id=eid, role=role, position=pos)
        )
        ids.append(eid)
    return rid, ids


async def _assert_parties_current(conn, *entity_ids):
    rows = (
        await conn.execute(
            select(
                record_entities.c.entity_id,
                record_entities.c.party_id,
                license_records.c.license_number,
            )
            .join(license_records, license_records.c.id == record_entities.c.record_id)
            .where(record_entities.c.entity_id.in_(entity_ids))
        )
    ).fetchall()
    assert rows, "no links to check"
    want = await eff_many(conn, [(e, lic) for e, _, lic in rows])
    assert [(e, p) for e, p, _ in rows] == [(e, want[(e, lic)]) for e, _, lic in rows]
    return {(e, lic): p for e, p, lic in rows}


@PG
async def test_relink_sets_party_for_a_format_key_group(pg_conn, standard_new_application):
    _, (comma,) = await _link(pg_conn, standard_new_application, "ZQP1", "ZQPARTY, ALPHA")
    _, (natural,) = await _link(pg_conn, standard_new_application, "ZQP2", "ALPHA ZQPARTY")
    result = await relink(pg_conn, [comma, natural])
    parties = await _assert_parties_current(pg_conn, comma, natural)
    assert set(parties.values()) == {natural}
    assert result.parties_updated == 1


@PG
async def test_party_follows_the_scoped_pendergraft_chain(pg_conn, standard_new_application):
    lic = "355693"
    _, (ty, ty_p, tyler_david) = await _link(
        pg_conn, standard_new_application, lic, "ZQTY", "ZQTY PENDERGRAFT", "ZQTYLER D PENDERGRAFT"
    )
    _, (ty_else,) = await _link(pg_conn, standard_new_application, "999999", "ZQTY")
    assert ty_else == ty
    await add_manual_alias(pg_conn, ty_p, tyler_david, scope_license=lic, created_by="t@x")
    await add_manual_alias(pg_conn, ty, ty_p, scope_license=lic, created_by="t@x")
    parties = await _assert_parties_current(pg_conn, ty, ty_p)
    assert parties[(ty, lic)] == tyler_david
    assert parties[(ty, "999999")] == ty


@PG
async def test_party_resets_on_type_flip_and_unlink(pg_conn, standard_new_application):
    _, (a,) = await _link(pg_conn, standard_new_application, "ZQP3", "ZQFLIPP, BETA")
    _, (b,) = await _link(pg_conn, standard_new_application, "ZQP4", "BETA ZQFLIPP")
    await relink(pg_conn, [a, b])
    await record_manual(
        pg_conn, a, entity_type="organization", entity_subtype="business", created_by="t@x"
    )
    parties = await _assert_parties_current(pg_conn, a, b)
    assert parties[(a, "ZQP3")] == a

    c = await _ent(pg_conn, "ZQFLIPP GAMMA")
    rid = await add_manual_alias(pg_conn, b, c, created_by="t@x")
    assert (await _assert_parties_current(pg_conn, b))[(b, "ZQP4")] == c
    await remove_manual_alias(pg_conn, rid)
    assert (await _assert_parties_current(pg_conn, b))[(b, "ZQP4")] == b


@PG
async def test_party_after_merge_and_purge(pg_conn, standard_new_application):
    _, (dirty,) = await _link(pg_conn, standard_new_application, "ZQP5", "ZQMP DELTA DUP")
    _, (canon,) = await _link(pg_conn, standard_new_application, "ZQP6", "ZQMP DELTA")
    x = await _ent(pg_conn, "ZQMP EPSILON")
    await add_manual_alias(pg_conn, dirty, x, created_by="t@x")
    await merge_entity_into(pg_conn, dirty, canon)
    parties = await _assert_parties_current(pg_conn, canon)
    assert set(parties.values()) == {x}

    _, (comma,) = await _link(pg_conn, standard_new_application, "ZQP7", "ZQPURGP, ZETA")
    _, (natural,) = await _link(pg_conn, standard_new_application, "ZQP8", "ZETA ZQPURGP")
    await relink(pg_conn, [comma, natural])
    await _purge_entities(pg_conn, [natural])
    assert (await _assert_parties_current(pg_conn, comma))[(comma, "ZQP7")] == comma


@PG
async def test_full_relink_twice_updates_no_party(pg_conn, standard_new_application):
    _, (a,) = await _link(pg_conn, standard_new_application, "ZQP9", "ZQTWICEP, ETA")
    _, (b,) = await _link(pg_conn, standard_new_application, "ZQP10", "ETA ZQTWICEP")
    await relink(pg_conn)
    second = await relink(pg_conn)
    assert (second.parties_updated, second.changed) == (0, 0)
    await _assert_parties_current(pg_conn, a, b)


# ── #265: the middle-name rule in relink ─────────────────────────────


@PG
async def test_middle_names_link_short_to_long_on_the_licence_only(
    pg_conn, standard_new_application
):
    _, (tyler, tyler_d, comma) = await _link(
        pg_conn,
        standard_new_application,
        "ZQM1",
        "ZQTYLER ZQPEND",
        "ZQTYLER DAVID ZQPEND",
        "ZQPEND, ZQTYLER",
    )
    await _link(pg_conn, standard_new_application, "ZQM2", "ZQPEND, ZQTYLER")
    result = await relink(pg_conn, [tyler, tyler_d, comma])

    assert await _rows(pg_conn, tyler, tyler_d, comma) == {
        _fk(comma, tyler),
        _mid(tyler, tyler_d, "ZQM1"),  # one row per party: the comma form needs none
    }
    assert (result.middle.rows, result.middle.variants, result.middle.licences) == (1, 1, 1)
    parties = await _assert_parties_current(pg_conn, tyler, comma)
    assert parties[(comma, "ZQM1")] == tyler_d
    assert parties[(comma, "ZQM2")] == tyler
    again = await relink(pg_conn, [tyler, tyler_d, comma])
    assert (again.changed, again.kept) == (0, 2)


@PG
async def test_middle_names_conflict_writes_nothing(pg_conn, standard_new_application):
    _, ids = await _link(
        pg_conn, standard_new_application, "ZQM3", "ZQJOHN ZQSM", "ZQJOHN A ZQSM", "ZQJOHN B ZQSM"
    )
    result = await relink(pg_conn, ids)
    assert await _rows(pg_conn, *ids) == set()
    assert result.middle.conflicts == 1


@PG
async def test_middle_names_couple_strings_take_no_part(pg_conn, standard_new_application):
    _, (short, long_, couple) = await _link(
        pg_conn,
        standard_new_application,
        "ZQM4",
        "ZQRENEE ZQOCHS",
        "ZQRENEE ELIZABETH ZQOCHS",
        "ZQRENEE AND TIM ZQOCHS",
    )
    await relink(pg_conn, [short, long_, couple])
    assert await _rows(pg_conn, short, long_, couple) == {_mid(short, long_, "ZQM4")}


@PG
async def test_middle_names_need_a_shared_role(pg_conn, standard_new_application):
    _, (prev,) = await _link(
        pg_conn, standard_new_application, "ZQM5", "ZQJANE ZQHAND", role="previous_applicant"
    )
    _, (cur, _) = await _link(pg_conn, standard_new_application, "ZQM5", "ZQJANE Q ZQHAND", "ZQX Y")
    result = await relink(pg_conn, [prev, cur])
    assert await _rows(pg_conn, prev, cur) == set()
    assert result.middle.skipped_role == 1


@PG
async def test_ingest_of_a_long_form_adds_the_row(pg_conn, standard_new_application):
    _, (short,) = await _link(pg_conn, standard_new_application, "ZQ0265", "ZQIRIS ZQPENDLE")
    record = standard_new_application | {
        "applicants": "ACME CANNABIS CO; ZQIRIS MAE ZQPENDLE",
        "license_number": "ZQ0265",
        "record_date": "2025-07-01",
    }
    assert (await ingest_record(pg_conn, record, IngestOptions(link_outcomes=False))).is_new
    long_ = (
        await pg_conn.execute(select(entities.c.id).where(entities.c.name == "ZQIRIS MAE ZQPENDLE"))
    ).scalar_one()
    assert await _rows(pg_conn, short) == {_mid(short, long_, "ZQ0265")}
    assert (await _assert_parties_current(pg_conn, short))[(short, "ZQ0265")] == long_


@PG
async def test_type_change_drops_the_middle_row(pg_conn, standard_new_application):
    _, (short, long_) = await _link(
        pg_conn, standard_new_application, "ZQM6", "ZQKAI ZQFLOP", "ZQKAI R ZQFLOP"
    )
    await relink(pg_conn, [short, long_])
    await record_manual(
        pg_conn, short, entity_type="organization", entity_subtype="business", created_by="t@x"
    )
    assert await _rows(pg_conn, short, long_) == set()
    await clear_manual(pg_conn, short)
    assert await _rows(pg_conn, short, long_) == {_mid(short, long_, "ZQM6")}


@PG
async def test_operator_link_wins_over_a_middle_row(pg_conn, standard_new_application):
    _, (short, long_) = await _link(
        pg_conn, standard_new_application, "ZQM7", "ZQLEO ZQWIN", "ZQLEO M ZQWIN"
    )
    await relink(pg_conn, [short, long_])
    # the reverse direction loops only through the rule row: accepted, rule row dropped
    await add_manual_alias(pg_conn, long_, short, scope_license="ZQM7", created_by="t@x")
    assert await _rows(pg_conn, short, long_) == {(long_, short, "ZQM7", "manual", None)}
    assert (await _assert_parties_current(pg_conn, long_))[(long_, "ZQM7")] == short


@PG
async def test_unlink_then_undo_a_middle_row(pg_conn, standard_new_application):
    _, (short, long_, comma) = await _link(
        pg_conn, standard_new_application, "ZQM8", "ZQMIA ZQUNDO", "ZQMIA J ZQUNDO", "ZQUNDO, ZQMIA"
    )
    await relink(pg_conn, [short, long_, comma])
    assert _mid(short, long_, "ZQM8") in await _rows(pg_conn, short)

    await dismiss_pair(pg_conn, short, long_, scope_license="ZQM8", dismissed_by="t@x")
    assert _mid(short, long_, "ZQM8") not in await _rows(pg_conn, short)
    await undismiss_pair(pg_conn, short, long_, scope_license="ZQM8")
    assert _mid(short, long_, "ZQM8") in await _rows(pg_conn, short)

    # a dismissal on a member counts against its party; another licence's does not
    await dismiss_pair(pg_conn, comma, long_, scope_license="ZQOTHER", dismissed_by="t@x")
    assert _mid(short, long_, "ZQM8") in await _rows(pg_conn, short)
    await dismiss_pair(pg_conn, comma, long_, scope_license="ZQM8", dismissed_by="t@x")
    assert _mid(short, long_, "ZQM8") not in await _rows(pg_conn, short)
    assert (await _assert_parties_current(pg_conn, comma))[(comma, "ZQM8")] == short


@PG
async def test_full_relink_writes_middle_rows_once(pg_conn, standard_new_application):
    _, (short, long_) = await _link(
        pg_conn, standard_new_application, "ZQM9", "ZQNOA ZQFULL", "ZQNOA P ZQFULL"
    )
    await pg_conn.execute(  # a stale row the full run must drop
        insert(entity_aliases).values(
            variant_id=long_,
            canonical_id=short,
            scope_license="ZQGONE",
            source="rule",
            rule="middle_names",
        )
    )
    first = await relink(pg_conn)
    assert await _rows(pg_conn, short, long_) == {_mid(short, long_, "ZQM9")}
    assert first.middle.rows >= 1
    second = await relink(pg_conn)
    assert second.changed == 0


@PG
@pytest.mark.parametrize("licence", [None, "", "  "])
async def test_middle_names_never_from_a_record_without_a_licence(
    pg_conn, standard_new_application, licence
):
    """CR 1: a NULL or blank licence must not group people, least of all as scope NULL."""
    _, (short, long_) = await _link(
        pg_conn, standard_new_application, licence, "ZQNUL ZQLIC", "ZQNUL K ZQLIC"
    )
    await relink(pg_conn, [short, long_])
    await relink(pg_conn)
    assert await _rows(pg_conn, short, long_) == set()


@PG
async def test_purging_a_middle_name_canonical_refreshes_its_variants(
    pg_conn, standard_new_application
):
    """CR 2: the variant is no key-mate of the purged canonical; its party must not dangle."""
    _, (short, long_) = await _link(
        pg_conn, standard_new_application, "ZQPG1", "ZQPUR ZQMID", "ZQPUR T ZQMID"
    )
    await relink(pg_conn, [short, long_])
    assert (await _assert_parties_current(pg_conn, short))[(short, "ZQPG1")] == long_
    await _purge_entities(pg_conn, [long_])
    assert (await _assert_parties_current(pg_conn, short))[(short, "ZQPG1")] == short
