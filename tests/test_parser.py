"""Tests for parser.py — pure HTML parsing functions.

All tests use static HTML fixtures; no network calls, no database.
"""
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from parser import (
    normalize_date,
    parse_location,
    parse_records_from_table,
    parse_snapshot,
    is_valid_record,
    SECTION_MAP,
)
from tests.conftest import FIXTURES_DIR


# ── Helper ────────────────────────────────────────────────────────────


def _load_table(fixture_name: str, section_type: str | None = None):
    """Load an HTML fixture, find the first <table>, and parse it."""
    html = (FIXTURES_DIR / fixture_name).read_text()
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    assert table is not None, f"No <table> found in {fixture_name}"
    # Infer section_type from the header if not given
    if section_type is None:
        th = table.find("th")
        header = th.get_text(strip=True).replace('\xa0', ' ')
        for key, val in SECTION_MAP.items():
            if key in header:
                section_type = val
                break
        assert section_type, f"Could not infer section_type from header: {header}"
    return parse_records_from_table(table, section_type)


# ── normalize_date ──────────────────────────────────────────────────


class TestNormalizeDate:
    def test_standard_format(self):
        assert normalize_date("6/15/2025") == "2025-06-15"

    def test_leading_zeros(self):
        assert normalize_date("01/02/2025") == "2025-01-02"

    def test_empty_string(self):
        assert normalize_date("") == ""

    def test_whitespace(self):
        assert normalize_date("  6/15/2025  ") == "2025-06-15"

    def test_already_iso(self):
        # If already ISO, it passes through as-is (doesn't match M/D/Y)
        assert normalize_date("2025-06-15") == "2025-06-15"

    def test_garbage_passthrough(self):
        assert normalize_date("not-a-date") == "not-a-date"


# ── parse_location ──────────────────────────────────────────────────


class TestParseLocation:
    def test_full_address(self):
        assert parse_location("123 MAIN ST, SEATTLE, WA 98101") == (
            "SEATTLE", "WA", "98101"
        )

    def test_zip_plus_four(self):
        assert parse_location("100 1ST AVE, KENT, WA 98032-1234") == (
            "KENT", "WA", "98032-1234"
        )

    def test_no_zip(self):
        city, state, zip_code = parse_location("100 1ST AVE, KENT, WA")
        assert city == "KENT"
        assert state == "WA"
        assert zip_code == ""

    def test_empty(self):
        assert parse_location("") == ("", "WA", "")

    def test_multi_word_city(self):
        assert parse_location("1 ELM DR, LAKE FOREST PARK, WA 98155") == (
            "LAKE FOREST PARK", "WA", "98155"
        )


# ── parse_records_from_table: standard records ────────────────────


class TestParseNewApplications:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.records = _load_table("new_applications.html")

    def test_record_count(self):
        assert len(self.records) == 2

    def test_first_record_fields(self):
        r = self.records[0]
        assert r["section_type"] == "new_application"
        assert r["record_date"] == "2025-06-15"
        assert r["business_name"] == "ACME CANNABIS CO"
        assert r["business_location"] == "123 MAIN ST, SEATTLE, WA 98101"
        assert r["applicants"] == "ACME CANNABIS CO; JOHN DOE; JANE SMITH"
        assert r["license_type"] == "CANNABIS RETAILER"
        assert r["application_type"] == "NEW APPLICATION"
        assert r["license_number"] == "078001"
        assert r["contact_phone"] == "(206) 555-0100"

    def test_location_parsing(self):
        r = self.records[0]
        assert r["city"] == "SEATTLE"
        assert r["state"] == "WA"
        assert r["zip_code"] == "98101"

    def test_second_record(self):
        r = self.records[1]
        assert r["license_number"] == "412345"
        assert r["application_type"] == "RENEWAL"
        assert r["city"] == "SPOKANE"

    def test_previous_fields_empty(self):
        """Standard records should have empty previous_* fields."""
        for r in self.records:
            assert r["previous_business_name"] == ""
            assert r["previous_applicants"] == ""
            assert r["previous_business_location"] == ""


# ── parse_records_from_table: ASSUMPTION records ──────────────────


class TestParseAssumptionRecord:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.records = _load_table("assumption_record.html")

    def test_record_count(self):
        assert len(self.records) == 1

    def test_buyer_is_business_name(self):
        """New Business Name maps to business_name (the buyer)."""
        assert self.records[0]["business_name"] == "NEW LEAF DISPENSARY"

    def test_seller_is_previous(self):
        """Current Business Name maps to previous_business_name (the seller)."""
        assert self.records[0]["previous_business_name"] == "OLD SMOKE SHOP"

    def test_buyer_applicants(self):
        assert self.records[0]["applicants"] == "NEW LEAF DISPENSARY; CAROL NEWBY"

    def test_seller_applicants(self):
        assert self.records[0]["previous_applicants"] == "OLD SMOKE SHOP; ALICE OLDEN"

    def test_application_type(self):
        assert self.records[0]["application_type"] == "ASSUMPTION"


# ── parse_records_from_table: CHANGE OF LOCATION records ───────────


class TestParseChangeOfLocation:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.records = _load_table("change_of_location.html")

    def test_record_count(self):
        assert len(self.records) == 1

    def test_new_location_is_primary(self):
        """New Business Location maps to business_location."""
        r = self.records[0]
        assert r["business_location"] == "200 NEW BLVD, OLYMPIA, WA 98502"
        assert r["city"] == "OLYMPIA"
        assert r["zip_code"] == "98502"

    def test_old_location_is_previous(self):
        """Current Business Location maps to previous_business_location."""
        r = self.records[0]
        assert r["previous_business_location"] == "100 OLD RD, OLYMPIA, WA 98501"
        assert r["previous_city"] == "OLYMPIA"
        assert r["previous_zip_code"] == "98501"

    def test_backslash_application_type(self):
        """The \\Application Type: label (with leading backslash) is parsed."""
        assert self.records[0]["application_type"] == "CHANGE OF LOCATION"


# ── parse_records_from_table: approved section with numeric codes ───


class TestParseApprovedSection:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.records = _load_table("approved_section.html", "approved")

    def test_record_count(self):
        assert len(self.records) == 1

    def test_section_type(self):
        assert self.records[0]["section_type"] == "approved"

    def test_date_field_mapping(self):
        """Approved Date: is correctly used as record_date."""
        assert self.records[0]["record_date"] == "2025-06-11"

    def test_numeric_license_type(self):
        """Numeric codes are preserved as-is in license_type."""
        assert self.records[0]["license_type"] == "349,"

    def test_no_applicants(self):
        """Approved records don't have an applicants field."""
        assert self.records[0]["applicants"] == ""


# ── parse_records_from_table: discontinued section ─────────────────


class TestParseDiscontinuedSection:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.records = _load_table("discontinued_section.html", "discontinued")

    def test_record_count(self):
        assert len(self.records) == 1

    def test_section_type(self):
        assert self.records[0]["section_type"] == "discontinued"

    def test_date_field_mapping(self):
        assert self.records[0]["record_date"] == "2025-06-09"

    def test_code_name_license_type(self):
        """Historical CODE, NAME format is preserved in license_type."""
        assert self.records[0]["license_type"] == "450, GROCERY STORE - BEER/WINE"


# ── parse_snapshot (full page) ─────────────────────────────────────


class TestParseSnapshot:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.records = parse_snapshot(FIXTURES_DIR / "full_snapshot.html")

    def test_finds_all_sections(self):
        """Full snapshot should yield records from all three sections."""
        section_types = {r["section_type"] for r in self.records}
        assert section_types == {"new_application", "approved", "discontinued"}

    def test_total_count(self):
        assert len(self.records) == 3

    def test_each_section_has_one_record(self):
        from collections import Counter
        counts = Counter(r["section_type"] for r in self.records)
        assert counts["new_application"] == 1
        assert counts["approved"] == 1
        assert counts["discontinued"] == 1


# ── is_valid_record ─────────────────────────────────────────────────


class TestIsValidRecord:
    def test_valid(self, standard_new_application):
        assert is_valid_record(standard_new_application) is True

    def test_missing_license_number(self, standard_new_application):
        standard_new_application["license_number"] = ""
        assert is_valid_record(standard_new_application) is False

    def test_missing_section_type(self, standard_new_application):
        standard_new_application["section_type"] = ""
        assert is_valid_record(standard_new_application) is False

    def test_missing_application_type(self, standard_new_application):
        standard_new_application["application_type"] = ""
        assert is_valid_record(standard_new_application) is False

    def test_bad_date_format(self, standard_new_application):
        standard_new_application["record_date"] = "6/15/2025"  # not ISO
        assert is_valid_record(standard_new_application) is False

    def test_missing_date(self, standard_new_application):
        standard_new_application["record_date"] = ""
        assert is_valid_record(standard_new_application) is False

    def test_empty_dict(self):
        assert is_valid_record({}) is False


# ── Edge cases ──────────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_table(self):
        """A table with only a header row produces no records."""
        html = '<table><tr><th colspan="2">STATEWIDE NEW LICENSE APPLICATIONS</th></tr></table>'
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        assert parse_records_from_table(table, "new_application") == []

    def test_blank_rows_between_records(self):
        """Blank separator rows (whitespace-only cells) don't break parsing."""
        # The new_applications fixture has a blank row between records
        records = _load_table("new_applications.html")
        assert len(records) == 2

    def test_no_matching_sections_in_page(self):
        """A page with no recognized section headers returns empty."""
        html = "<html><body><table><tr><td>Nothing here</td></tr></table></body></html>"
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".html", mode="w", delete=False) as f:
            f.write(html)
            f.flush()
            records = parse_snapshot(Path(f.name))
        assert records == []

    def test_record_without_license_number_skipped(self):
        """A partial record missing license_number is not emitted."""
        html = """<table>
        <tr><td>Notification Date:</td><td>1/1/2025</td></tr>
        <tr><td>Business Name:</td><td>PARTIAL BIZ</td></tr>
        </table>"""
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        records = parse_records_from_table(table, "new_application")
        assert records == []
