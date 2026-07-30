"""Tests for parser.py — pure HTML parsing functions.

All tests use static HTML fixtures; no network calls, no database.
"""

import gzip
from datetime import UTC, datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from wslcb_licensing_tracker.parser import (
    SECTION_MAP,
    _read_snapshot,
    extract_records_from_diff,
    extract_snapshot_date,
    glob_with_gz,
    is_valid_record,
    normalize_date,
    parse_diff_timestamp,
    parse_location,
    parse_records_from_table,
    parse_snapshot,
    snapshot_paths,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


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
        header = th.get_text(strip=True).replace("\xa0", " ")
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
        assert parse_location("123 MAIN ST, SEATTLE, WA 98101") == ("SEATTLE", "WA", "98101")

    def test_zip_plus_four(self):
        assert parse_location("100 1ST AVE, KENT, WA 98032-1234") == ("KENT", "WA", "98032-1234")

    def test_no_zip(self):
        city, state, zip_code = parse_location("100 1ST AVE, KENT, WA")
        assert city == "KENT"
        assert state == "WA"
        assert zip_code == ""

    def test_empty(self):
        assert parse_location("") == ("", "WA", "")

    def test_multi_word_city(self):
        assert parse_location("1 ELM DR, LAKE FOREST PARK, WA 98155") == (
            "LAKE FOREST PARK",
            "WA",
            "98155",
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

    def test_scraped_at_is_timezone_aware_datetime(self):
        """scraped_at must be a timezone-aware datetime, not an ISO string."""
        for r in self.records:
            assert isinstance(r["scraped_at"], datetime)
            assert r["scraped_at"].tzinfo is not None

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

    def test_no_matching_sections_in_page(self, tmp_path):
        """A page with no recognized section headers returns empty."""
        html = "<html><body><table><tr><td>Nothing here</td></tr></table></body></html>"
        p = tmp_path / "empty.html"
        p.write_text(html)
        assert parse_snapshot(p) == []


# ── Gzip snapshot support ────────────────────────────────────────────


class TestReadSnapshot:
    def test_reads_plain_html(self, tmp_path):
        """`_read_snapshot` reads an uncompressed .html file."""
        p = tmp_path / "test.html"
        p.write_text("hello", encoding="utf-8")
        assert _read_snapshot(p) == "hello"

    def test_reads_gz_directly(self, tmp_path):
        """`_read_snapshot` reads a .html.gz file when given its path."""
        p = tmp_path / "test.html.gz"
        p.write_bytes(gzip.compress(b"hello"))
        assert _read_snapshot(p) == "hello"

    def test_falls_back_to_gz_when_html_missing(self, tmp_path):
        """`_read_snapshot` finds .html.gz when the .html path doesn't exist."""
        gz = tmp_path / "test.html.gz"
        gz.write_bytes(gzip.compress(b"hello"))
        plain = tmp_path / "test.html"  # does not exist
        assert _read_snapshot(plain) == "hello"

    def test_raises_when_neither_exists(self, tmp_path):
        """`_read_snapshot` raises FileNotFoundError when no file is found."""
        p = tmp_path / "missing.html"
        with pytest.raises((FileNotFoundError, OSError)):
            _read_snapshot(p)


class TestReadReplayExtract:
    def test_reads_gz_extract(self, tmp_path):
        """`read_replay_extract` returns the decompressed extract content."""
        from wslcb_licensing_tracker.parser import read_replay_extract

        p = tmp_path / "994002-new-application.html.gz"
        p.write_bytes(gzip.compress(b"<tbody><tr><td>x</td></tr></tbody>"))
        assert read_replay_extract(p) == "<tbody><tr><td>x</td></tr></tbody>"

    def test_missing_extract_returns_none(self, tmp_path):
        """`read_replay_extract` returns None (not raises) for a missing file."""
        from wslcb_licensing_tracker.parser import read_replay_extract

        assert read_replay_extract(tmp_path / "missing.html.gz") is None


class TestSnapshotPaths:
    def test_finds_html_files(self, tmp_path):
        d = tmp_path / "wslcb" / "licensinginfo" / "2025" / "2025_12_01"
        d.mkdir(parents=True)
        (d / "2025_12_01-test-v1.html").write_text("x")
        paths = snapshot_paths(tmp_path)
        assert len(paths) == 1
        assert paths[0].suffix == ".html"

    def test_finds_gz_files(self, tmp_path):
        d = tmp_path / "wslcb" / "licensinginfo" / "2026" / "2026_01_01"
        d.mkdir(parents=True)
        (d / "2026_01_01-test-v1.html.gz").write_bytes(gzip.compress(b"x"))
        paths = snapshot_paths(tmp_path)
        assert len(paths) == 1
        assert paths[0].name.endswith(".html.gz")

    def test_gz_shadows_html_sibling(self, tmp_path):
        """When .html and .html.gz both exist, only .html.gz is returned."""
        d = tmp_path / "wslcb" / "licensinginfo" / "2026" / "2026_01_01"
        d.mkdir(parents=True)
        base = "2026_01_01-test-v1"
        (d / f"{base}.html").write_text("x")
        (d / f"{base}.html.gz").write_bytes(gzip.compress(b"x"))
        paths = snapshot_paths(tmp_path)
        assert len(paths) == 1
        assert paths[0].name.endswith(".html.gz")

    def test_mixed_years(self, tmp_path):
        """Returns files from multiple years sorted chronologically."""
        for year, date in [("2025", "2025_12_01"), ("2026", "2026_01_01")]:
            d = tmp_path / "wslcb" / "licensinginfo" / year / date
            d.mkdir(parents=True)
            (d / f"{date}-test-v1.html.gz").write_bytes(gzip.compress(b"x"))
        paths = snapshot_paths(tmp_path)
        assert len(paths) == 2
        assert "2025" in paths[0].parts
        assert "2026" in paths[1].parts


class TestGlobWithGz:
    """Tests for `glob_with_gz` — the generic dedup-glob `snapshot_paths` is built on."""

    def test_finds_plain_files(self, tmp_path):
        (tmp_path / "a.txt").write_text("x")
        paths = glob_with_gz(tmp_path, "*.txt")
        assert paths == [tmp_path / "a.txt"]

    def test_finds_gz_files(self, tmp_path):
        (tmp_path / "a.txt.gz").write_bytes(gzip.compress(b"x"))
        paths = glob_with_gz(tmp_path, "*.txt")
        assert paths == [tmp_path / "a.txt.gz"]

    def test_gz_shadows_plain_sibling(self, tmp_path):
        (tmp_path / "a.txt").write_text("x")
        (tmp_path / "a.txt.gz").write_bytes(gzip.compress(b"x"))
        paths = glob_with_gz(tmp_path, "*.txt")
        assert paths == [tmp_path / "a.txt.gz"]

    def test_sorted_and_unaffected_by_other_extensions(self, tmp_path):
        (tmp_path / "b.txt").write_text("x")
        (tmp_path / "a.txt").write_text("x")
        (tmp_path / "c.csv").write_text("x")
        paths = glob_with_gz(tmp_path, "*.txt")
        assert paths == [tmp_path / "a.txt", tmp_path / "b.txt"]


class TestParseSnapshotGz:
    def test_parse_gz_snapshot_returns_same_records(self, tmp_path):
        """parse_snapshot on a .html.gz file returns the same records as the .html."""
        src = FIXTURES_DIR / "full_snapshot.html"
        gz = tmp_path / "2026_01_01-test-v1.html.gz"
        gz.write_bytes(gzip.compress(src.read_bytes()))
        records_gz = parse_snapshot(gz)
        records_html = parse_snapshot(src)
        assert len(records_gz) == len(records_html)
        assert {r["section_type"] for r in records_gz} == {r["section_type"] for r in records_html}

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


# ── extract_tbody_from_snapshot ──────────────────────────────────────


class TestExtractTbodyFromSnapshot:
    def test_found_first_record(self, tmp_path):
        """Returns the <tbody> HTML for the first matching record."""
        from wslcb_licensing_tracker.parser import extract_tbody_from_snapshot

        src = FIXTURES_DIR / "snapshot_two_records.html"
        result = extract_tbody_from_snapshot(
            src, "new_application", "078001", "2025-06-15", "NEW APPLICATION"
        )
        assert result is not None
        assert "ACME CANNABIS CO" in result
        assert "<tbody" in result.lower()

    def test_found_second_record(self, tmp_path):
        """Returns the <tbody> for the second record in the same table."""
        from wslcb_licensing_tracker.parser import extract_tbody_from_snapshot

        src = FIXTURES_DIR / "snapshot_two_records.html"
        result = extract_tbody_from_snapshot(
            src, "new_application", "412345", "2025-06-14", "RENEWAL"
        )
        assert result is not None
        assert "BOB'S BEER BARN" in result
        assert "ACME CANNABIS CO" not in result

    def test_not_found_wrong_license(self):
        """Returns None when the license number doesn't match."""
        from wslcb_licensing_tracker.parser import extract_tbody_from_snapshot

        src = FIXTURES_DIR / "snapshot_two_records.html"
        result = extract_tbody_from_snapshot(
            src, "new_application", "999999", "2025-06-15", "NEW APPLICATION"
        )
        assert result is None

    def test_not_found_wrong_section(self):
        """Returns None when section_type has no matching table."""
        from wslcb_licensing_tracker.parser import extract_tbody_from_snapshot

        src = FIXTURES_DIR / "snapshot_two_records.html"
        result = extract_tbody_from_snapshot(
            src, "approved", "078001", "2025-06-15", "NEW APPLICATION"
        )
        assert result is None


# ── extract_tbody_from_diff ──────────────────────────────────────────


class TestExtractTbodyFromDiff:
    def test_found_in_added_lines(self):
        """Returns the reconstructed <tbody> for a record in the added lines."""
        from wslcb_licensing_tracker.parser import extract_tbody_from_diff

        src = FIXTURES_DIR / "diff_two_records.txt"
        result = extract_tbody_from_diff(
            src, "new_application", "078001", "2025-06-15", "NEW APPLICATION"
        )
        assert result is not None
        assert "ACME CANNABIS CO" in result
        assert "<tbody" in result.lower()

    def test_found_in_removed_lines(self):
        """Returns the reconstructed <tbody> for a record only in removed lines."""
        from wslcb_licensing_tracker.parser import extract_tbody_from_diff

        src = FIXTURES_DIR / "diff_two_records.txt"
        result = extract_tbody_from_diff(src, "new_application", "412345", "2025-06-14", "RENEWAL")
        assert result is not None
        assert "BOB'S BEER BARN" in result

    def test_not_found(self):
        """Returns None when the record key isn't in the diff."""
        from wslcb_licensing_tracker.parser import extract_tbody_from_diff

        src = FIXTURES_DIR / "diff_two_records.txt"
        result = extract_tbody_from_diff(
            src, "new_application", "999999", "2025-06-15", "NEW APPLICATION"
        )
        assert result is None

    def test_falls_back_to_gz_when_txt_missing(self, tmp_path):
        """Reads a .txt.gz sibling when the DB's stale .txt snapshot_path is missing on disk.

        Regression test for the source_viewer route (app.py), which passes
        DATA_DIR / sources.snapshot_path straight through — that path keeps
        its original .txt extension after compress-diffs renames the file.
        """
        from wslcb_licensing_tracker.parser import extract_tbody_from_diff

        src = FIXTURES_DIR / "diff_two_records.txt"
        gz = tmp_path / "test.txt.gz"
        gz.write_bytes(gzip.compress(src.read_bytes()))
        plain = tmp_path / "test.txt"  # does not exist, mirrors a stale DB path
        result = extract_tbody_from_diff(
            plain, "new_application", "078001", "2025-06-15", "NEW APPLICATION"
        )
        assert result is not None
        assert "ACME CANNABIS CO" in result


class TestStripAnchorTags:
    """Tests for strip_anchor_tags() — removes <a> wrappers, preserves text."""

    def test_simple_anchor_removed(self):
        from wslcb_licensing_tracker.parser import strip_anchor_tags

        html = '<td><a href="tel:2065551234">206-555-1234</a></td>'
        result = strip_anchor_tags(html)
        assert "<a" not in result
        assert "206-555-1234" in result

    def test_nested_anchor_removed(self):
        from wslcb_licensing_tracker.parser import strip_anchor_tags

        html = '<td><a href="http://example.com"><b>ACME CO</b></a></td>'
        result = strip_anchor_tags(html)
        assert "<a" not in result
        # Inner content preserved
        assert "ACME CO" in result

    def test_multiple_anchors_removed(self):
        from wslcb_licensing_tracker.parser import strip_anchor_tags

        html = '<tr><td><a href="/a">First</a></td><td><a href="/b">Second</a></td></tr>'
        result = strip_anchor_tags(html)
        assert "<a" not in result
        assert "First" in result
        assert "Second" in result

    def test_no_anchors_unchanged(self):
        from wslcb_licensing_tracker.parser import strip_anchor_tags

        html = "<td>Plain text <b>bold</b></td>"
        result = strip_anchor_tags(html)
        assert "Plain text" in result
        assert "<b>" in result

    def test_empty_string(self):
        from wslcb_licensing_tracker.parser import strip_anchor_tags

        assert strip_anchor_tags("") == ""

    def test_anchor_with_no_href(self):
        from wslcb_licensing_tracker.parser import strip_anchor_tags

        html = '<td><a name="top">Anchor text</a></td>'
        result = strip_anchor_tags(html)
        assert "<a" not in result
        assert "Anchor text" in result


# ── parse_diff_timestamp ─────────────────────────────────────────────


class TestParseDiffTimestamp:
    def test_returns_datetime(self):
        result = parse_diff_timestamp("--- @\tWed, 07 Sep 2022 06:15:05 -0700")
        assert isinstance(result, datetime)

    def test_parsed_value_is_correct(self):
        result = parse_diff_timestamp("--- @\tWed, 07 Sep 2022 06:15:05 -0700")
        assert result.year == 2022
        assert result.month == 9
        assert result.day == 7

    def test_result_is_timezone_aware(self):
        result = parse_diff_timestamp("--- @\tWed, 07 Sep 2022 06:15:05 -0700")
        assert result.tzinfo is not None

    def test_malformed_header_returns_utc_datetime(self):
        result = parse_diff_timestamp("--- malformed line with no tab")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_empty_string_returns_datetime(self):
        result = parse_diff_timestamp("")
        assert isinstance(result, datetime)


# ── extract_records_from_diff (.gz support) ──────────────────────────


class TestDiffMergeBoundary:
    """Regression tests for the cross-record merge bug (#150 follow-up).

    When a diff's changed-lines-only stream omits a later block's
    ``Approved Date:`` / ``Business Name:`` rows (because they were unchanged
    context), that block's remaining changed fields must NOT be merged into the
    preceding record's accumulator. Doing so produced hybrid records: one
    entity's name/date glued onto another license's number/type/location
    (e.g. real record 31982 — 'TTL HOLDINGS LLC' stamped on the '2020 CANNABIS
    SOLUTIONS' retailer license 355469).
    """

    def _records(self):
        src = FIXTURES_DIR / "diff_merge_hybrid.txt"
        return extract_records_from_diff(src, "approved")

    def _by_license(self, recs, lic):
        return next((r for r in recs if r["license_number"] == lic), None)

    @staticmethod
    def _table_from_rows(rows):
        """Build a <table> of two-cell <tr> label/value rows, as the diff parser
        sees a changed-only line stream (see parse_html_lines)."""
        html = (
            "<table>"
            + "".join(f"<tr><td>{label}</td><td>{value}</td></tr>" for label, value in rows)
            + "</table>"
        )
        return BeautifulSoup(html, "lxml").find("table")

    def test_no_hybrid_record(self):
        """No record pairs block A's business name with block B's license."""
        recs = self._records()
        hybrids = [
            r
            for r in recs
            if r["license_number"] == "222222" and r["business_name"] == "AAA PRODUCERS LLC"
        ]
        assert hybrids == [], f"hybrid record leaked: {hybrids}"

    def test_block_a_intact(self):
        """Block A keeps its own license and fields."""
        a = self._by_license(self._records(), "111111")
        assert a is not None
        assert a["business_name"] == "AAA PRODUCERS LLC"
        assert a["record_date"] == "2025-06-15"
        assert a["license_type"] == "391, CANNABIS PRODUCER TIER 2"

    def test_block_b_recovered_correctly(self):
        """Block B is recovered via the context pass with its own name."""
        b = self._by_license(self._records(), "222222")
        assert b is not None
        assert b["business_name"] == "BBB RETAIL INC"
        assert b["record_date"] == "2025-06-14"

    def test_no_merge_when_block_a_license_is_context(self):
        """Boundary is detected even when block A's own License Number row was
        unchanged context (dropped from the changed-only stream), so the trigger
        can't rely on license_number being set. An overwritten field (here the
        business location) marks the new block."""
        # Simulates the added-line stream: block A has no License Number row
        # (it was context), block B follows with only its later fields present.
        rows = [
            ("Approved Date:", "6/15/2025"),
            ("Business Name:", "AAA PRODUCERS LLC"),
            ("Business Location:", "111 FIRST ST,  SEATTLE, WA 98101"),
            ("License Type:", "391, CANNABIS PRODUCER TIER 2"),
            ("Application Type:", "ADDED FEES"),
            ("Contact Phone:", "2065550111"),
            # block B (date/name were context, dropped):
            ("Business Location:", "222 SECOND AVE,  BELLINGHAM, WA 98226"),
            ("License Type:", "394, CANNABIS RETAILER"),
            ("Application Type:", "ADDED/CHANGE OF CLASS/IN LIEU"),
            ("License Number:", "222222"),
            ("Contact Phone:", "2065550222"),
        ]
        recs = parse_records_from_table(self._table_from_rows(rows), "approved")
        # No record may pair block A's name with block B's license.
        assert not [
            r
            for r in recs
            if r["business_name"] == "AAA PRODUCERS LLC" and r["license_number"] == "222222"
        ]

    def test_class_b_location_and_type_not_borrowed(self):
        """Class-B case: block B's differing location/type must NOT overwrite
        block A when block B carries no second License Number (its own date,
        name and license rows were unchanged context). Block A must keep its own
        location and type; nothing may pair block A's license with block B's
        location/type."""
        rows = [
            ("Approved Date:", "6/15/2025"),
            ("Business Name:", "AAA PRODUCERS LLC"),
            ("Business Location:", "111 FIRST ST,  SEATTLE, WA 98101"),
            ("License Type:", "391, CANNABIS PRODUCER TIER 2"),
            ("Application Type:", "ADDED FEES"),
            ("License Number:", "111111"),
            ("Contact Phone:", "2065550111"),
            # block B: only its later fields present (date/name/license context):
            ("Business Location:", "222 SECOND AVE,  BELLINGHAM, WA 98226"),
            ("License Type:", "394, CANNABIS RETAILER"),
            ("Application Type:", "ADDED/CHANGE OF CLASS/IN LIEU"),
            ("Contact Phone:", "2065550222"),
        ]
        recs = parse_records_from_table(self._table_from_rows(rows), "approved")
        a = self._by_license(recs, "111111")
        assert a is not None
        assert a["business_location"] == "111 FIRST ST,  SEATTLE, WA 98101"
        assert a["license_type"] == "391, CANNABIS PRODUCER TIER 2"
        assert a["city"] == "SEATTLE"
        assert not [
            r
            for r in recs
            if r["license_number"] == "111111" and r["license_type"] == "394, CANNABIS RETAILER"
        ]

    def test_new_current_pairs_do_not_split_a_single_block(self):
        """The New/Current field pairs map to distinct keys, so an ASSUMPTION
        block (which legitimately carries both) is parsed as ONE record even
        though the same logical position appears twice — the overwrite boundary
        check must not fire on them."""
        rows = [
            ("Notification Date:", "6/15/2025"),
            ("New Business Name:", "NEW LEAF DISPENSARY"),
            ("Current Business Name:", "OLD SMOKE SHOP"),
            ("New Applicant(s):", "NEW LEAF DISPENSARY; CAROL NEWBY"),
            ("Current Applicant(s):", "OLD SMOKE SHOP; ALICE OLDEN"),
            ("License Type:", "CANNABIS RETAILER"),
            ("Application Type:", "ASSUMPTION"),
            ("License Number:", "078123"),
            ("Contact Phone:", "2065550178"),
        ]
        recs = parse_records_from_table(self._table_from_rows(rows), "new_application")
        assert len(recs) == 1
        r = recs[0]
        assert r["business_name"] == "NEW LEAF DISPENSARY"
        assert r["previous_business_name"] == "OLD SMOKE SHOP"
        assert r["applicants"] == "NEW LEAF DISPENSARY; CAROL NEWBY"
        assert r["previous_applicants"] == "OLD SMOKE SHOP; ALICE OLDEN"
        assert r["license_number"] == "078123"


class TestExtractRecordsFromDiffGz:
    def test_reads_gz_directly(self, tmp_path):
        """extract_records_from_diff reads a .txt.gz file when given its path."""
        src = FIXTURES_DIR / "diff_two_records.txt"
        gz = tmp_path / "test.txt.gz"
        gz.write_bytes(gzip.compress(src.read_bytes()))
        records_gz = extract_records_from_diff(gz, "new_application")
        records_plain = extract_records_from_diff(src, "new_application")
        assert len(records_gz) == len(records_plain)
        assert records_gz[0]["license_number"] == records_plain[0]["license_number"]

    def test_falls_back_to_gz_when_txt_missing(self, tmp_path):
        """extract_records_from_diff finds .txt.gz when the .txt path doesn't exist."""
        src = FIXTURES_DIR / "diff_two_records.txt"
        gz = tmp_path / "test.txt.gz"
        gz.write_bytes(gzip.compress(src.read_bytes()))
        plain = tmp_path / "test.txt"  # does not exist
        records = extract_records_from_diff(plain, "new_application")
        assert len(records) == len(extract_records_from_diff(src, "new_application"))

    def test_bad_utf8_still_raises_for_plain_txt(self, tmp_path):
        """Strict UTF-8 decoding is preserved for plain .txt (no latin-1 fallback)."""
        p = tmp_path / "bad.txt"
        p.write_bytes(b"\xff\xfe")
        with pytest.raises(UnicodeDecodeError):
            extract_records_from_diff(p, "new_application")


# ── extract_snapshot_date ─────────────────────────────────────────────


class TestExtractSnapshotDate:
    def test_returns_datetime_for_valid_filename(self, tmp_path):
        p = tmp_path / "2025_12_16-licensinginfo.lcb.wa.gov-v1.html"
        result = extract_snapshot_date(p)
        assert result == datetime(2025, 12, 16, 0, 0, 0, tzinfo=UTC)

    def test_result_is_utc_midnight(self, tmp_path):
        p = tmp_path / "2024_01_05-licensinginfo.html"
        result = extract_snapshot_date(p)
        assert result is not None
        assert result.hour == 0
        assert result.minute == 0
        assert result.tzinfo == UTC

    def test_no_date_in_filename_returns_none(self, tmp_path):
        p = tmp_path / "no_date_here.html"
        assert extract_snapshot_date(p) is None

    def test_partial_date_returns_none(self, tmp_path):
        p = tmp_path / "2025_12.html"
        assert extract_snapshot_date(p) is None
