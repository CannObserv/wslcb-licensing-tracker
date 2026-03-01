"""Tests for integrity.py — database integrity checks.

Verifies each check function detects the intended anomaly and
that fix functions resolve it.  All tests use in-memory SQLite.
"""
import pytest

from endorsements import seed_endorsements, _ensure_endorsement, _link_endorsement, process_record
from queries import insert_record


def _make_record(db, **overrides):
    """Insert a record with sensible defaults, returning its id."""
    defaults = {
        "section_type": "new_application",
        "record_date": "2025-06-15",
        "business_name": "TEST BIZ",
        "business_location": "123 MAIN ST, SEATTLE, WA 98101",
        "applicants": "TEST BIZ; JOHN DOE",
        "license_type": "CANNABIS RETAILER",
        "application_type": "NEW APPLICATION",
        "license_number": "999001",
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
        "scraped_at": "2025-06-15T12:00:00+00:00",
    }
    defaults.update(overrides)
    result = insert_record(db, defaults)
    assert result is not None
    return result[0]


# ── Orphaned locations ───────────────────────────────────────


class TestCheckOrphanedLocations:
    def test_detects_orphan(self, db):
        """A location not referenced by any record is orphaned."""
        from integrity import check_orphaned_locations

        seed_endorsements(db)
        # Create a location directly, not via a record
        db.execute(
            "INSERT INTO locations (raw_address, city, state, zip_code) "
            "VALUES ('NOWHERE ST, VOID, WA 00000', 'VOID', 'WA', '00000')"
        )
        db.commit()

        orphans = check_orphaned_locations(db)
        assert len(orphans) >= 1

    def test_no_orphans(self, db):
        """Locations referenced by records should not be flagged."""
        from integrity import check_orphaned_locations

        seed_endorsements(db)
        _make_record(db)  # creates a location
        db.commit()

        orphans = check_orphaned_locations(db)
        assert len(orphans) == 0

    def test_fix_removes_orphans(self, db):
        """fix_orphaned_locations should delete unreferenced rows."""
        from integrity import check_orphaned_locations, fix_orphaned_locations

        seed_endorsements(db)
        db.execute(
            "INSERT INTO locations (raw_address, city, state, zip_code) "
            "VALUES ('ORPHAN ST, GONE, WA 99999', 'GONE', 'WA', '99999')"
        )
        db.commit()

        removed = fix_orphaned_locations(db)
        assert removed >= 1
        assert len(check_orphaned_locations(db)) == 0


# ── Broken FKs ───────────────────────────────────────────────


class TestCheckBrokenFKs:
    def test_detects_broken_location_fk(self, db):
        """A record with location_id pointing to nonexistent location."""
        from integrity import check_broken_fks

        seed_endorsements(db)
        rec_id = _make_record(db)
        db.commit()  # close open transaction before changing PRAGMA
        # Disable FK enforcement to simulate a corrupt DB
        db.execute("PRAGMA foreign_keys = OFF")
        db.execute("UPDATE license_records SET location_id = 99999 WHERE id = ?", (rec_id,))
        db.commit()
        db.execute("PRAGMA foreign_keys = ON")

        broken = check_broken_fks(db)
        assert len(broken) >= 1
        assert any(b["record_id"] == rec_id for b in broken)

    def test_no_broken_fks(self, db):
        """Valid records should have no broken FKs."""
        from integrity import check_broken_fks

        seed_endorsements(db)
        _make_record(db)
        db.commit()

        broken = check_broken_fks(db)
        assert len(broken) == 0


# ── Un-enriched records ──────────────────────────────────────


class TestCheckUnenriched:
    def test_detects_missing_endorsement(self, db):
        """Records without endorsement enrichment should be detected."""
        from integrity import check_unenriched_records

        seed_endorsements(db)
        rec_id = _make_record(db)
        # Remove the endorsement links
        db.execute("DELETE FROM record_endorsements WHERE record_id = ?", (rec_id,))
        db.commit()

        unenriched = check_unenriched_records(db)
        assert unenriched["no_endorsements"] >= 1

    def test_detects_missing_provenance(self, db):
        """Records without provenance should be detected."""
        from integrity import check_unenriched_records

        seed_endorsements(db)
        _make_record(db)
        db.commit()

        unenriched = check_unenriched_records(db)
        # Our test records have no source provenance
        assert unenriched["no_provenance"] >= 1

    def test_clean_record_passes(self, db):
        """Fully enriched record with provenance should pass."""
        from integrity import check_unenriched_records
        from pipeline import ingest_record, IngestOptions
        from database import get_or_create_source, SOURCE_TYPE_LIVE_SCRAPE

        seed_endorsements(db)
        source_id = get_or_create_source(
            db, SOURCE_TYPE_LIVE_SCRAPE,
            snapshot_path="test/path.html",
            url="https://example.com",
            captured_at="2025-06-15T12:00:00+00:00",
        )
        opts = IngestOptions(
            validate_addresses=False, link_outcomes=False,
            source_id=source_id,
        )
        rec = {
            "section_type": "new_application",
            "record_date": "2025-06-15",
            "business_name": "CLEAN BIZ",
            "business_location": "100 CLEAN ST, SEATTLE, WA 98101",
            "applicants": "CLEAN BIZ; CLEAN PERSON",
            "license_type": "CANNABIS RETAILER",
            "application_type": "NEW APPLICATION",
            "license_number": "CLEAN01",
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
            "scraped_at": "2025-06-15T12:00:00+00:00",
        }
        ingest_record(db, rec, opts)
        db.commit()

        unenriched = check_unenriched_records(db)
        assert unenriched["no_endorsements"] == 0
        assert unenriched["no_provenance"] == 0


# ── Endorsement anomalies ────────────────────────────────────


class TestCheckEndorsementAnomalies:
    def test_detects_unresolved_code(self, db):
        """Numeric license_type with no endorsement links."""
        from integrity import check_endorsement_anomalies

        seed_endorsements(db)
        rec_id = _make_record(db, license_type="9999,", license_number="ANOM01")
        # Remove any endorsements that were auto-created
        db.execute("DELETE FROM record_endorsements WHERE record_id = ?", (rec_id,))
        db.commit()

        anomalies = check_endorsement_anomalies(db)
        assert anomalies["unresolved_codes"] >= 1


# ── Entity duplicates ────────────────────────────────────────


class TestCheckEntityDuplicates:
    def test_detects_case_duplicates(self, db):
        """Entities that differ only by case should be flagged."""
        from integrity import check_entity_duplicates

        seed_endorsements(db)
        # Insert entities that differ only by case
        db.execute("INSERT INTO entities (name, entity_type) VALUES ('John Doe', 'person')")
        db.execute("INSERT INTO entities (name, entity_type) VALUES ('JOHN DOE', 'person')")
        db.commit()

        dupes = check_entity_duplicates(db)
        assert len(dupes) >= 1


# ── run_all_checks ───────────────────────────────────────────


class TestRunAllChecks:
    def test_returns_report_dict(self, db):
        """run_all_checks should return a structured report."""
        from integrity import run_all_checks

        seed_endorsements(db)
        _make_record(db)
        db.commit()

        report = run_all_checks(db)
        assert "orphaned_locations" in report
        assert "broken_fks" in report
        assert "unenriched" in report
        assert "endorsement_anomalies" in report
        assert "entity_duplicates" in report

    def test_fix_mode_cleans_orphans(self, db):
        """run_all_checks with fix=True should auto-fix safe issues."""
        from integrity import run_all_checks

        seed_endorsements(db)
        db.execute(
            "INSERT INTO locations (raw_address, city, state, zip_code) "
            "VALUES ('FIX ME ST, NOWHERE, WA 00000', 'NOWHERE', 'WA', '00000')"
        )
        db.commit()

        report = run_all_checks(db, fix=True)
        assert report["orphaned_locations"]["fixed"] >= 1
