"""Tests for the rebuild-from-sources module."""
import sqlite3
from pathlib import Path

import pytest

from schema import init_db
from rebuild import (
    rebuild_from_sources,
    compare_databases,
    RebuildResult,
    ComparisonResult,
)


# -- Helpers ---------------------------------------------------------------

def _make_record(**overrides) -> dict:
    """Create a minimal record dict with sensible defaults."""
    base = {
        "section_type": "new_application",
        "record_date": "2025-01-01",
        "business_name": "DEFAULT BUSINESS",
        "business_location": "100 MAIN ST, SEATTLE, WA 98101",
        "applicants": "",
        "license_type": "",
        "application_type": "NEW APPLICATION",
        "license_number": "000000",
        "contact_phone": "",
        "city": "SEATTLE",
        "state": "WA",
        "zip_code": "98101",
        "previous_business_name": "",
        "previous_applicants": "",
        "previous_business_location": "",
        "previous_city": "",
        "previous_state": "",
        "previous_zip_code": "",
        "scraped_at": "2025-01-01T00:00:00+00:00",
    }
    base.update(overrides)
    return base


_MINIMAL_SNAPSHOT_HTML = """
<html><body>
<table>
<thead><tr><th colspan="7">STATEWIDE&nbsp;NEW LICENSE APPLICATIONS</th></tr></thead>
<tr><td>&nbsp;</td><td>&nbsp;</td></tr>
<tr><td>Notification Date:</td><td>06/01/2025</td></tr>
<tr><td>Business Name:</td><td>SNAPSHOT BUSINESS LLC</td></tr>
<tr><td>Business Location:</td><td>100 FIRST AVE, SEATTLE, WA 98101</td></tr>
<tr><td>Applicant(s):</td><td>SNAPSHOT BUSINESS LLC; JANE SMITH</td></tr>
<tr><td>License Type:</td><td>CANNABIS RETAILER</td></tr>
<tr><td>Application Type:</td><td>NEW APPLICATION</td></tr>
<tr><td>License Number:</td><td>999001</td></tr>
<tr><td>Contact Phone:</td><td>(206) 555-0100</td></tr>
<tr><td>&nbsp;</td><td>&nbsp;</td></tr>
<tr><td>Notification Date:</td><td>06/01/2025</td></tr>
<tr><td>Business Name:</td><td>SECOND BUSINESS INC.</td></tr>
<tr><td>Business Location:</td><td>200 SECOND AVE, TACOMA, WA 98402</td></tr>
<tr><td>Applicant(s):</td><td>SECOND BUSINESS INC.; BOB JONES</td></tr>
<tr><td>License Type:</td><td>GROCERY STORE - BEER/WINE</td></tr>
<tr><td>Application Type:</td><td>RENEWAL</td></tr>
<tr><td>License Number:</td><td>999002</td></tr>
<tr><td>Contact Phone:</td><td>(253) 555-0200</td></tr>
</table>
</body></html>
"""


# -- Fixtures --------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """Return path to a temp DB file for rebuild output."""
    return tmp_path / "rebuilt.db"


@pytest.fixture
def prod_db(tmp_path):
    """Create a small 'production' DB with some records for comparison."""
    from db import get_connection
    from queries import insert_record
    from endorsements import seed_endorsements

    path = tmp_path / "prod.db"
    conn = get_connection(path)
    init_db(conn)
    seed_endorsements(conn)

    insert_record(conn, _make_record(
        section_type="new_application",
        record_date="2025-01-15",
        business_name="TEST BUSINESS",
        business_location="123 MAIN ST, SEATTLE, WA 98101",
        applicants="TEST BUSINESS; JOHN DOE",
        license_type="CANNABIS RETAILER",
        application_type="NEW APPLICATION",
        license_number="000001",
        contact_phone="(555) 123-4567",
        city="SEATTLE", state="WA", zip_code="98101",
    ))
    insert_record(conn, _make_record(
        section_type="approved",
        record_date="2025-02-01",
        business_name="OTHER BUSINESS",
        business_location="456 OAK AVE, TACOMA, WA 98402",
        license_type="349,",
        application_type="RENEWAL",
        license_number="000002",
        city="TACOMA", state="WA", zip_code="98402",
    ))
    conn.commit()
    conn.close()
    return path


# -- TestRebuildFromSources ------------------------------------------------

class TestRebuildFromSources:
    """Tests for rebuild_from_sources()."""

    def test_empty_data_dir_returns_zero_counts(self, tmp_path, tmp_db):
        """Rebuild with no snapshots or diffs inserts nothing."""
        empty_data = tmp_path / "empty_data"
        empty_data.mkdir()
        result = rebuild_from_sources(output_path=tmp_db, data_dir=empty_data)
        assert isinstance(result, RebuildResult)
        assert result.records == 0
        assert result.from_snapshots == 0
        assert result.from_diffs == 0

    def test_creates_output_database(self, tmp_path, tmp_db):
        """Rebuild creates the output DB file even with no data."""
        empty_data = tmp_path / "empty_data"
        empty_data.mkdir()
        rebuild_from_sources(output_path=tmp_db, data_dir=empty_data)
        assert tmp_db.exists()
        # Verify it's a valid SQLite DB with the right schema
        conn = sqlite3.connect(str(tmp_db))
        conn.row_factory = sqlite3.Row
        tables = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "license_records" in tables
        assert "locations" in tables
        assert "entities" in tables
        conn.close()

    def test_refuses_to_overwrite_existing_db(self, tmp_path, tmp_db):
        """Rebuild refuses to overwrite an existing output file."""
        tmp_db.write_text("existing")
        with pytest.raises(FileExistsError):
            rebuild_from_sources(output_path=tmp_db, data_dir=tmp_path)

    def test_force_overwrites_existing_db(self, tmp_path, tmp_db):
        """Rebuild with force=True removes and recreates."""
        tmp_db.write_text("existing")
        empty_data = tmp_path / "empty_data"
        empty_data.mkdir()
        result = rebuild_from_sources(
            output_path=tmp_db, data_dir=empty_data, force=True,
        )
        assert tmp_db.exists()
        assert result.records == 0

    def test_ingests_from_snapshots(self, tmp_path, tmp_db):
        """Rebuild ingests records from snapshot HTML files."""
        data_dir = tmp_path / "data"
        snap_dir = data_dir / "wslcb" / "licensinginfo" / "2025" / "2025_06_01"
        snap_dir.mkdir(parents=True)
        snap_file = snap_dir / "2025_06_01-licensinginfo.lcb.wa.gov-v1.html"
        snap_file.write_text(_MINIMAL_SNAPSHOT_HTML)

        result = rebuild_from_sources(output_path=tmp_db, data_dir=data_dir)
        assert result.from_snapshots == 2
        assert result.records == 2

        # Verify records in DB
        conn = sqlite3.connect(str(tmp_db))
        conn.row_factory = sqlite3.Row
        count = conn.execute(
            "SELECT COUNT(*) as c FROM license_records"
        ).fetchone()["c"]
        assert count == 2
        conn.close()

    def test_result_includes_timing(self, tmp_path, tmp_db):
        """RebuildResult includes elapsed time."""
        empty_data = tmp_path / "empty_data"
        empty_data.mkdir()
        result = rebuild_from_sources(output_path=tmp_db, data_dir=empty_data)
        assert result.elapsed_seconds >= 0


# -- TestCompareResult -----------------------------------------------------

class TestCompareResult:
    """Tests for compare_databases()."""

    def test_identical_databases(self, tmp_path, prod_db):
        """Comparing a DB to itself reports no differences."""
        result = compare_databases(prod_db, prod_db)
        assert isinstance(result, ComparisonResult)
        assert result.prod_count == result.rebuilt_count
        assert result.missing_from_rebuilt == 0
        assert result.extra_in_rebuilt == 0

    def test_missing_records_detected(self, tmp_path, prod_db):
        """Records in prod but not rebuilt are reported."""
        from db import get_connection

        rebuilt_path = tmp_path / "rebuilt.db"
        conn = get_connection(rebuilt_path)
        init_db(conn)
        conn.close()

        result = compare_databases(prod_db, rebuilt_path)
        assert result.prod_count == 2
        assert result.rebuilt_count == 0
        assert result.missing_from_rebuilt == 2

    def test_extra_records_detected(self, tmp_path, prod_db):
        """Records in rebuilt but not prod are reported."""
        from db import get_connection
        from queries import insert_record
        from endorsements import seed_endorsements

        rebuilt_path = tmp_path / "rebuilt.db"
        conn = get_connection(rebuilt_path)
        init_db(conn)
        seed_endorsements(conn)

        # Insert the same 2 records as prod + 1 extra
        for rec in [
            _make_record(
                section_type="new_application",
                record_date="2025-01-15",
                business_name="TEST BUSINESS",
                business_location="123 MAIN ST, SEATTLE, WA 98101",
                applicants="TEST BUSINESS; JOHN DOE",
                license_type="CANNABIS RETAILER",
                application_type="NEW APPLICATION",
                license_number="000001",
                contact_phone="(555) 123-4567",
                city="SEATTLE", state="WA", zip_code="98101",
            ),
            _make_record(
                section_type="approved",
                record_date="2025-02-01",
                business_name="OTHER BUSINESS",
                business_location="456 OAK AVE, TACOMA, WA 98402",
                license_type="349,",
                application_type="RENEWAL",
                license_number="000002",
                city="TACOMA", state="WA", zip_code="98402",
            ),
            _make_record(
                section_type="discontinued",
                record_date="2025-03-01",
                business_name="EXTRA BUSINESS",
                business_location="789 ELM ST, OLYMPIA, WA 98501",
                license_type="CANNABIS PRODUCER TIER 1",
                application_type="DISCONTINUED",
                license_number="000003",
                city="OLYMPIA", state="WA", zip_code="98501",
            ),
        ]:
            insert_record(conn, rec)
        conn.commit()
        conn.close()

        result = compare_databases(prod_db, rebuilt_path)
        assert result.prod_count == 2
        assert result.rebuilt_count == 3
        assert result.extra_in_rebuilt == 1
        assert result.missing_from_rebuilt == 0

    def test_section_breakdown(self, tmp_path, prod_db):
        """Comparison includes per-section counts."""
        from db import get_connection

        rebuilt_path = tmp_path / "rebuilt.db"
        conn = get_connection(rebuilt_path)
        init_db(conn)
        conn.close()

        result = compare_databases(prod_db, rebuilt_path)
        assert "new_application" in result.section_counts
        assert result.section_counts["new_application"]["prod"] == 1
        assert result.section_counts["new_application"]["rebuilt"] == 0
