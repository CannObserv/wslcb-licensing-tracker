"""Tests for entity_aliases.py — the entity alias layer (#262, spec #247 §1-§3).

The pure functions (``link_key``, ``elect_canonical``, ``resolve``,
``find_cycle``) run without a database; the cases come from the spec's
worked examples.  PG tests (``pg_conn``) cover the write API, ``relink`` and
the hooks.
"""

import pytest

from wslcb_licensing_tracker.entity_aliases import (
    AliasRow,
    Member,
    elect_canonical,
    find_cycle,
    link_key,
    resolve,
)

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
