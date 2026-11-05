"""Tests for text_utils.py — pure-string normalization helpers."""

import pytest

from wslcb_licensing_tracker.text_utils import (
    _normalize_raw_address,
    clean_applicants_string,
    clean_entity_name,
    strip_duplicate_marker,
    strip_wac_annotation,
)

# ── _normalize_raw_address ──────────────────────────────────────────


class TestNormalizeRawAddress:
    def test_nbsp_replaced(self):
        assert _normalize_raw_address("123\xa0MAIN\xa0ST") == "123 MAIN ST"

    def test_regular_space_unchanged(self):
        assert _normalize_raw_address("123 MAIN ST") == "123 MAIN ST"

    def test_empty_passthrough(self):
        assert _normalize_raw_address("") == ""

    def test_none_passthrough(self):
        assert _normalize_raw_address(None) is None


# ── strip_duplicate_marker ──────────────────────────────────────────


class TestStripDuplicateMarker:
    def test_parenthesized(self):
        assert strip_duplicate_marker("ADAM (DUPLICATE) BENTON") == "ADAM BENTON"

    def test_parenthesized_numbered_2(self):
        assert strip_duplicate_marker("KATIE (DUPLICATE 2) DAVIS") == "KATIE DAVIS"

    def test_parenthesized_numbered_3(self):
        assert strip_duplicate_marker("KATIE (DUPLICATE 3) DAVIS") == "KATIE DAVIS"

    def test_inline_mid(self):
        assert strip_duplicate_marker("ANNA MARIE DUPLICATE ADAMS") == "ANNA MARIE ADAMS"

    def test_inline_prefix(self):
        assert strip_duplicate_marker("DUPLICATE ITALIAN SUPPLY, LLC") == "ITALIAN SUPPLY, LLC"

    def test_asterisk_variant(self):
        assert strip_duplicate_marker("PAUL *DUPLICATE* SONG") == "PAUL SONG"

    def test_trailing_with_paren(self):
        assert strip_duplicate_marker("JAY WON (DUPLICATE)") == "JAY WON"

    def test_unclosed_paren(self):
        # WSLCB source has one case with an unclosed open paren
        assert strip_duplicate_marker("ELIZABETH (DUPLICATE A MATTHEWS") == "ELIZABETH A MATTHEWS"

    def test_no_marker_unchanged(self):
        assert strip_duplicate_marker("ALICE SMITH") == "ALICE SMITH"

    def test_collapse_extra_spaces(self):
        assert strip_duplicate_marker("ANNA  DUPLICATE  MARIE") == "ANNA MARIE"

    def test_result_has_no_double_space(self):
        assert "  " not in strip_duplicate_marker("NEALY DUPLICATE EVANS")


# ── clean_entity_name ──────────────────────────────────────────────


class TestCleanEntityName:
    def test_uppercases(self):
        assert clean_entity_name("foo bar") == "FOO BAR"

    def test_strips_whitespace(self):
        assert clean_entity_name("  FOO  ") == "FOO"

    def test_collapses_internal_spaces(self):
        assert clean_entity_name("FOO  BAR") == "FOO BAR"

    def test_strips_trailing_period(self):
        assert clean_entity_name("WOLDU ARAYA BERAKI.") == "WOLDU ARAYA BERAKI"

    def test_strips_trailing_comma(self):
        assert clean_entity_name("SMITH, JOHN,") == "SMITH, JOHN"

    def test_preserves_inc_dot(self):
        assert clean_entity_name("ACME INC.") == "ACME INC."

    def test_preserves_llc_dot(self):
        assert clean_entity_name("WIDGETS LLC.") == "WIDGETS LLC."

    def test_preserves_ltd_dot(self):
        assert clean_entity_name("HOLDINGS LTD.") == "HOLDINGS LTD."

    def test_preserves_corp_dot(self):
        assert clean_entity_name("MEGACORP CORP.") == "MEGACORP CORP."

    def test_empty_string(self):
        assert clean_entity_name("") == ""


# ── clean_applicants_string ────────────────────────────────────────


class TestCleanApplicantsString:
    def test_none_passthrough(self):
        assert clean_applicants_string(None) is None

    def test_empty_passthrough(self):
        assert clean_applicants_string("") == ""

    def test_parenthesized_marker_stripped(self):
        result = clean_applicants_string("BIZ; ADAM (DUPLICATE) BENTON; ADAM BENTON")
        assert "(DUPLICATE)" not in result
        assert "DUPLICATE" not in result

    def test_inline_marker_stripped(self):
        result = clean_applicants_string("BIZ; NEALY DUPLICATE EVANS; NEALY EVANS")
        assert "DUPLICATE" not in result

    def test_deduplication_after_stripping(self):
        result = clean_applicants_string("BIZ; ADAM (DUPLICATE) BENTON; ADAM BENTON")
        parts = [p.strip() for p in result.split(";")]
        assert parts.count("ADAM BENTON") == 1

    def test_only_duplicate_token_synthesizes_clean_name(self):
        result = clean_applicants_string("BIZ; LORIE DUPLICATE FAZIO")
        assert "LORIE FAZIO" in result
        assert "DUPLICATE" not in result

    def test_empty_parts_dropped(self):
        # Extra semicolons / blank parts should not appear in output
        result = clean_applicants_string("FOO; ; BAR")
        assert ";;" not in result
        parts = [p.strip() for p in result.split(";") if p.strip()]
        assert "FOO" in parts
        assert "BAR" in parts


# ── strip_wac_annotation (#211) ────────────────────────────────────

# Every suffix shape on prod on 2026-11-05 (37 entities), with its cleaned result.
WAC_SUFFIXED = [
    # bare " PER WAC <cite>"
    ("7-ELEVEN, INC. PER WAC 314-07-035", "7-ELEVEN, INC."),
    ("HTLV SEATTLE TRS LLC PER WAC 314-07-035", "HTLV SEATTLE TRS LLC"),
    ("GO BANANAS CO PER WAC 314-07-035", "GO BANANAS CO"),
    # bracketed
    ("7-ELEVEN, INC. (PER WAC 314-07-035)", "7-ELEVEN, INC."),
    ("7-ELEVEN, INC., (PER WAC 314-07-035)", "7-ELEVEN, INC."),
    ("CK DESIGNS, LP (PER WAC 314-07-035)", "CK DESIGNS, LP"),
    ("SEATTLE ASIAN ART MUSEUM (WAC 314-07-035)", "SEATTLE ASIAN ART MUSEUM"),
    ("DOUBLECHECK LLC (PER 314-07-035)", "DOUBLECHECK LLC"),
    ("ASSOCIATES, LLC (PER WAC 314-07-035)", "ASSOCIATES, LLC"),
    # comma lead-in, with and without a space
    ("CTYD III CORPORATION, PER WAC 314-07-035", "CTYD III CORPORATION"),
    ("1000 FIRST AVENUE LLC, PER WAC 314.07.035", "1000 FIRST AVENUE LLC"),
    ("44TH AVENUE HOTEL OPCO,LLC,PER WAC 314-12-030", "44TH AVENUE HOTEL OPCO,LLC"),
    # word lead-ins
    ("JAMES AND WENDY HULL AS PER WAC 314-55-035", "JAMES AND WENDY HULL"),
    ("GOMAD LLC, ON LICENSE PER WAC 314-07-035", "GOMAD LLC"),
    ("TERRY GROUP, LLC, LICENSE PER WAC 314.07.035", "TERRY GROUP, LLC"),
    ("GRANT COUNTY LICENSED PER WAC 314-07-035", "GRANT COUNTY"),
    # separators
    ("BP WEST COAST PRODUCTS LLC PER WAC 314 07 035", "BP WEST COAST PRODUCTS LLC"),
    ("BP WEST COAST PRODUCTS,LLC PER WAC 314.07.035", "BP WEST COAST PRODUCTS,LLC"),
    # truncated
    ("TUKWILA HOTEL GROUP, LLC PER WAC 314-07-03", "TUKWILA HOTEL GROUP, LLC"),
    ("BP WEST COAST PRODUCTS, LLC PER WAC 314.07.03", "BP WEST COAST PRODUCTS, LLC"),
    ("BP WEST COAST PRODUCTS, LLC PER WAC 314.07", "BP WEST COAST PRODUCTS, LLC"),
    ("BP PRODUCTS NORTH AMERICA, INC PER WAC 314.07", "BP PRODUCTS NORTH AMERICA, INC"),
    ("BP WEST COAST PRODUCTS LLC PER WAC 314 07", "BP WEST COAST PRODUCTS LLC"),
    ("BP WEST COAST PRODUCTS LLC PER WAC 0314-07-", "BP WEST COAST PRODUCTS LLC"),
    ("GARTH GEHLEN AND NATHAN DOIDGE PER WAC 314-55", "GARTH GEHLEN AND NATHAN DOIDGE"),
    # citation split off into the next applicant part
    ("BP WEST COAST PRODUCTS, LLC PER WAC", "BP WEST COAST PRODUCTS, LLC"),
    ("W BY W INTERNATIONAL BLVD I, LLC ON LIC PER", "W BY W INTERNATIONAL BLVD I, LLC"),
    # subsection, unclosed bracket, lower case
    ("ACME LLC PER WAC 314-07-035(1)", "ACME LLC"),
    ("ACME LLC (PER WAC 314-07-035", "ACME LLC"),
    ("acme llc per wac 314-07-035", "ACME LLC"),
]

# Names that merely contain WAC / PER / 314 and must not change.
WAC_LOOKALIKES = [
    "WACHAREE BOWERS",
    "BOWERS, WACHAREE",
    "AMARJIT S PUREWAC",
    "BILLY KARWACKI",
    "MUKUL SINGH SHEKHAWAC",
    "ALYSSA JEAN WACHTVEITL",
    "WAC FARMS, LLC",
    "JOHN WAC",
    "HANSEN, PER",
    "PER LARSEN",
    "SUPER LICENSE PER LLC",
    "314 HOLDINGS LLC",
    "ACME LLC PER WAC 314-07-035 AND SONS",
]


class TestStripWacAnnotation:
    @pytest.mark.parametrize(("raw", "expected"), WAC_SUFFIXED)
    def test_strips_suffix(self, raw, expected):
        assert clean_entity_name(strip_wac_annotation(raw)) == expected

    @pytest.mark.parametrize("name", WAC_LOOKALIKES)
    def test_leaves_lookalikes(self, name):
        assert strip_wac_annotation(name) == name

    @pytest.mark.parametrize(
        "annotation",
        [
            "PER WAC 314-07-035",
            "(PER WAC 314-07-035)",
            "WAC 314-07-035",
            "ON LIC PER WAC 314-07-035",
        ],
    )
    def test_pure_annotation_strips_to_empty(self, annotation):
        assert clean_entity_name(strip_wac_annotation(annotation)) == ""

    def test_empty_passthrough(self):
        assert strip_wac_annotation("") == ""
