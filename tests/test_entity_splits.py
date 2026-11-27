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
