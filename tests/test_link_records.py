"""Tests for link_records.py — application→outcome record linking.

Verifies both bulk and incremental linking after the deduplication
refactor (#24).  All tests use in-memory SQLite via the ``db`` fixture.
"""
import pytest

from endorsements import seed_endorsements
from queries import insert_record
from link_records import (
    build_all_links,
    link_new_record,
    get_outcome_status,
    get_reverse_link_info,
    _link_section,
    _link_incremental,
)


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
    assert result is not None, f"insert_record returned None for {defaults}"
    return result[0]


# ── Bulk linking (_link_section) ─────────────────────────────────


class TestLinkSection:
    """Tests for the unified _link_section helper."""

    def test_approval_high_confidence(self, db):
        """Mutual best match → high confidence link."""
        seed_endorsements(db)
        na_id = _make_record(db, section_type="new_application",
                            application_type="RENEWAL", license_number="L001",
                            record_date="2025-06-10")
        ap_id = _make_record(db, section_type="approved",
                            application_type="RENEWAL", license_number="L001",
                            record_date="2025-06-12", applicants="")

        high, med = _link_section(db, mode="approval")
        assert high == 1
        assert med == 0

        link = db.execute(
            "SELECT * FROM record_links WHERE new_app_id = ?", (na_id,)
        ).fetchone()
        assert link["outcome_id"] == ap_id
        assert link["confidence"] == "high"
        assert link["days_gap"] == 2

    def test_discontinuance_linking(self, db):
        """DISC. LIQUOR SALES → discontinued/DISCONTINUED."""
        seed_endorsements(db)
        na_id = _make_record(db, section_type="new_application",
                            application_type="DISC. LIQUOR SALES",
                            license_number="L002", record_date="2025-06-10")
        dc_id = _make_record(db, section_type="discontinued",
                            application_type="DISCONTINUED",
                            license_number="L002", record_date="2025-06-12",
                            applicants="")

        high, med = _link_section(db, mode="discontinuance")
        assert high == 1
        assert med == 0

        link = db.execute(
            "SELECT * FROM record_links WHERE new_app_id = ?", (na_id,)
        ).fetchone()
        assert link["outcome_id"] == dc_id

    def test_medium_confidence_forward_only(self, db):
        """Forward-only match (backward prefers a different app) → medium."""
        seed_endorsements(db)
        # Two applications for same license, one outcome
        na1_id = _make_record(db, section_type="new_application",
                             application_type="RENEWAL", license_number="L003",
                             record_date="2025-06-08")
        na2_id = _make_record(db, section_type="new_application",
                             application_type="RENEWAL", license_number="L003",
                             record_date="2025-06-10")
        ap_id = _make_record(db, section_type="approved",
                            application_type="RENEWAL", license_number="L003",
                            record_date="2025-06-12", applicants="")

        high, med = _link_section(db, mode="approval")
        # na2 is the backward best match → high
        # na1 forward matches same outcome but backward prefers na2 → medium
        assert high == 1
        assert med == 1

    def test_no_match_outside_tolerance(self, db):
        """Outcome too far before application → no link."""
        seed_endorsements(db)
        _make_record(db, section_type="new_application",
                    application_type="RENEWAL", license_number="L004",
                    record_date="2025-06-20")
        _make_record(db, section_type="approved",
                    application_type="RENEWAL", license_number="L004",
                    record_date="2025-06-01", applicants="")  # 19 days before

        high, med = _link_section(db, mode="approval")
        assert high == 0
        assert med == 0

    def test_all_approval_link_types(self, db):
        """All application types in _APPROVAL_LINK_TYPES can link."""
        from link_records import _APPROVAL_LINK_TYPES
        seed_endorsements(db)

        for i, app_type in enumerate(sorted(_APPROVAL_LINK_TYPES)):
            lic = f"A{i:04d}"
            _make_record(db, section_type="new_application",
                        application_type=app_type, license_number=lic,
                        record_date="2025-06-10")
            _make_record(db, section_type="approved",
                        application_type=app_type, license_number=lic,
                        record_date="2025-06-12", applicants="")

        high, med = _link_section(db, mode="approval")
        assert high == len(_APPROVAL_LINK_TYPES)


# ── Incremental linking (_link_incremental / link_new_record) ────


class TestLinkIncremental:
    """Tests for the unified _link_incremental helper."""

    def test_new_app_finds_existing_outcome(self, db):
        """Inserting a new_application finds an existing approved record."""
        seed_endorsements(db)
        ap_id = _make_record(db, section_type="approved",
                            application_type="RENEWAL", license_number="L010",
                            record_date="2025-06-12", applicants="")
        na_id = _make_record(db, section_type="new_application",
                            application_type="RENEWAL", license_number="L010",
                            record_date="2025-06-10")

        result = link_new_record(db, na_id)
        assert result == ap_id

    def test_outcome_finds_existing_new_app(self, db):
        """Inserting an outcome finds an existing new_application."""
        seed_endorsements(db)
        na_id = _make_record(db, section_type="new_application",
                            application_type="RENEWAL", license_number="L011",
                            record_date="2025-06-10")
        ap_id = _make_record(db, section_type="approved",
                            application_type="RENEWAL", license_number="L011",
                            record_date="2025-06-12", applicants="")

        result = link_new_record(db, ap_id)
        assert result == na_id

    def test_disc_links_to_discontinued(self, db):
        """DISC. LIQUOR SALES incrementally links to discontinued."""
        seed_endorsements(db)
        dc_id = _make_record(db, section_type="discontinued",
                            application_type="DISCONTINUED",
                            license_number="L012", record_date="2025-06-12",
                            applicants="")
        na_id = _make_record(db, section_type="new_application",
                            application_type="DISC. LIQUOR SALES",
                            license_number="L012", record_date="2025-06-10")

        result = link_new_record(db, na_id)
        assert result == dc_id

    def test_non_linkable_type_returns_none(self, db):
        """Application types not in LINKABLE_TYPES return None."""
        seed_endorsements(db)
        na_id = _make_record(db, section_type="new_application",
                            application_type="SOME RANDOM TYPE",
                            license_number="L013", record_date="2025-06-10")

        result = link_new_record(db, na_id)
        assert result is None

    def test_nonexistent_record_returns_none(self, db):
        """Linking a record that doesn't exist returns None."""
        result = link_new_record(db, 99999)
        assert result is None


# ── build_all_links ──────────────────────────────────────────────


class TestBuildAllLinks:
    def test_clears_and_rebuilds(self, db):
        """build_all_links clears existing links and rebuilds."""
        seed_endorsements(db)
        na_id = _make_record(db, section_type="new_application",
                            application_type="RENEWAL", license_number="L020",
                            record_date="2025-06-10")
        ap_id = _make_record(db, section_type="approved",
                            application_type="RENEWAL", license_number="L020",
                            record_date="2025-06-12", applicants="")

        result1 = build_all_links(db)
        assert result1["total"] == 1

        # Rebuild should produce identical results
        result2 = build_all_links(db)
        assert result2["total"] == 1

        # Exactly one link should exist
        count = db.execute("SELECT count(*) FROM record_links").fetchone()[0]
        assert count == 1

    def test_mixed_approvals_and_discontinuances(self, db):
        """Both approval and discontinuance links in one build."""
        seed_endorsements(db)
        # Approval
        _make_record(db, section_type="new_application",
                    application_type="RENEWAL", license_number="L021",
                    record_date="2025-06-10")
        _make_record(db, section_type="approved",
                    application_type="RENEWAL", license_number="L021",
                    record_date="2025-06-12", applicants="")
        # Discontinuance
        _make_record(db, section_type="new_application",
                    application_type="DISC. LIQUOR SALES",
                    license_number="L022", record_date="2025-06-10")
        _make_record(db, section_type="discontinued",
                    application_type="DISCONTINUED",
                    license_number="L022", record_date="2025-06-12",
                    applicants="")

        result = build_all_links(db)
        assert result["high"] == 2
        assert result["total"] == 2


# ── get_outcome_status ───────────────────────────────────────────


class TestGetOutcomeStatus:
    def test_non_new_application_returns_none(self):
        """Non-new_application records have no outcome status."""
        record = {"section_type": "approved", "application_type": "RENEWAL"}
        result = get_outcome_status(record, None)
        assert result["status"] is None

    def test_approved_link(self):
        """Record with approved outcome link."""
        record = {
            "section_type": "new_application",
            "application_type": "RENEWAL",
            "record_date": "2025-06-10",
        }
        link = {
            "outcome_section_type": "approved",
            "outcome_date": "2025-06-12",
            "outcome_id": 42,
            "confidence": "high",
            "days_gap": 2,
        }
        result = get_outcome_status(record, link)
        assert result["status"] == "approved"
        assert result["linked_record_id"] == 42

    def test_discontinued_link(self):
        """Record with discontinued outcome link."""
        record = {
            "section_type": "new_application",
            "application_type": "DISC. LIQUOR SALES",
            "record_date": "2025-06-10",
        }
        link = {
            "outcome_section_type": "discontinued",
            "outcome_date": "2025-06-12",
            "outcome_id": 43,
            "confidence": "high",
            "days_gap": 2,
        }
        result = get_outcome_status(record, link)
        assert result["status"] == "discontinued"

    def test_data_gap_status(self):
        """Post-cutoff NEW APPLICATION without link → data_gap."""
        from link_records import DATA_GAP_CUTOFF
        record = {
            "section_type": "new_application",
            "application_type": "NEW APPLICATION",
            "record_date": "2025-06-01",  # after cutoff
        }
        result = get_outcome_status(record, None)
        assert result["status"] == "data_gap"

    def test_non_linkable_type_returns_none(self):
        """Application types not in LINKABLE_TYPES have no status."""
        record = {
            "section_type": "new_application",
            "application_type": "SOME RANDOM TYPE",
            "record_date": "2025-06-10",
        }
        result = get_outcome_status(record, None)
        assert result["status"] is None


# ── get_reverse_link_info ────────────────────────────────────────


class TestGetReverseLinkInfo:
    def test_returns_link_info(self, db):
        """Approved record returns info about its originating application."""
        seed_endorsements(db)
        na_id = _make_record(db, section_type="new_application",
                            application_type="RENEWAL", license_number="L030",
                            record_date="2025-06-10")
        ap_id = _make_record(db, section_type="approved",
                            application_type="RENEWAL", license_number="L030",
                            record_date="2025-06-12", applicants="")
        build_all_links(db)

        record = dict(db.execute(
            "SELECT * FROM license_records WHERE id = ?", (ap_id,)
        ).fetchone())
        info = get_reverse_link_info(db, record)
        assert info is not None
        assert info["new_app_id"] == na_id

    def test_returns_none_for_new_application(self, db):
        """new_application records have no reverse link."""
        seed_endorsements(db)
        na_id = _make_record(db, section_type="new_application",
                            application_type="RENEWAL", license_number="L031",
                            record_date="2025-06-10")
        record = dict(db.execute(
            "SELECT * FROM license_records WHERE id = ?", (na_id,)
        ).fetchone())
        info = get_reverse_link_info(db, record)
        assert info is None
