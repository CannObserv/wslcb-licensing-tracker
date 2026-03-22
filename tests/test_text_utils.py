"""Tests for text_utils.py — pure-string normalization helpers."""

import pytest

from wslcb_licensing_tracker.text_utils import (
    _normalize_raw_address,
    clean_applicants_string,
    clean_entity_name,
    strip_duplicate_marker,
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
