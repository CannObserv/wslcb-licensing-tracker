"""Tests for endorsements.py — license endorsement normalization.

Verifies the _merge_endorsement helper and the repair functions
that use it, after the deduplication refactor (#24).
"""
import pytest

from endorsements import (
    seed_endorsements,
    process_record,
    _ensure_endorsement,
    _link_endorsement,
    _merge_endorsement,
    merge_mixed_case_endorsements,
    repair_code_name_endorsements,
    _merge_seeded_placeholders,
    get_endorsement_options,
    get_record_endorsements,
)
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


# ── _merge_endorsement helper ─────────────────────────────────


class TestMergeEndorsement:
    """Tests for the shared _merge_endorsement helper."""

    def test_migrates_record_links(self, db):
        """Record links should move from old to new endorsement."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        old_eid = _ensure_endorsement(db, "OLD ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "NEW ENDORSEMENT")
        _link_endorsement(db, rec_id, old_eid)

        _merge_endorsement(db, old_eid, new_eid)

        # Old link gone
        old_links = db.execute(
            "SELECT * FROM record_endorsements WHERE endorsement_id = ?",
            (old_eid,),
        ).fetchall()
        assert len(old_links) == 0

        # New link exists
        new_links = db.execute(
            "SELECT * FROM record_endorsements WHERE endorsement_id = ?",
            (new_eid,),
        ).fetchall()
        assert len(new_links) == 1
        assert new_links[0]["record_id"] == rec_id

    def test_migrates_code_mappings(self, db):
        """Code mappings should move from old to new endorsement."""
        seed_endorsements(db)

        old_eid = _ensure_endorsement(db, "OLD ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "NEW ENDORSEMENT")
        db.execute(
            "INSERT INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            ("999", old_eid),
        )

        _merge_endorsement(db, old_eid, new_eid)

        # Code should now point to new endorsement
        row = db.execute(
            "SELECT endorsement_id FROM endorsement_codes WHERE code = '999'"
        ).fetchone()
        assert row["endorsement_id"] == new_eid

    def test_deletes_old_endorsement(self, db):
        """The old endorsement row should be deleted."""
        seed_endorsements(db)

        old_eid = _ensure_endorsement(db, "DOOMED ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "KEEPER ENDORSEMENT")

        _merge_endorsement(db, old_eid, new_eid)

        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (old_eid,)
        ).fetchone()
        assert row is None

    def test_handles_duplicate_links_gracefully(self, db):
        """If record already linked to new endorsement, merge should not fail."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        old_eid = _ensure_endorsement(db, "OLD ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "NEW ENDORSEMENT")
        _link_endorsement(db, rec_id, old_eid)
        _link_endorsement(db, rec_id, new_eid)  # already linked to target

        _merge_endorsement(db, old_eid, new_eid)  # should not raise

        links = db.execute(
            "SELECT * FROM record_endorsements WHERE record_id = ? AND endorsement_id = ?",
            (rec_id, new_eid),
        ).fetchall()
        assert len(links) == 1

    def test_delete_old_endorsement_false(self, db):
        """When delete_old=False, old endorsement should be kept."""
        seed_endorsements(db)

        old_eid = _ensure_endorsement(db, "KEPT ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "TARGET ENDORSEMENT")

        _merge_endorsement(db, old_eid, new_eid, delete_old=False)

        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (old_eid,)
        ).fetchone()
        assert row is not None


# ── process_record ────────────────────────────────────────────


class TestProcessRecord:
    def test_text_endorsements(self, db):
        """Semicolon-separated text names."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="CANNABIS RETAILER; CANNABIS PROCESSOR")
        count = process_record(db, rec_id, "CANNABIS RETAILER; CANNABIS PROCESSOR")
        assert count == 2

    def test_numeric_code(self, db):
        """Bare numeric code resolves via seed map."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="394,")
        count = process_record(db, rec_id, "394,")
        assert count >= 1

        endorsements = db.execute(
            "SELECT le.name FROM record_endorsements re "
            "JOIN license_endorsements le ON le.id = re.endorsement_id "
            "WHERE re.record_id = ?",
            (rec_id,),
        ).fetchall()
        assert any(e["name"] == "CANNABIS RETAILER" for e in endorsements)

    def test_code_name_format(self, db):
        """Historical CODE, NAME format."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="450, GROCERY STORE - BEER/WINE")
        count = process_record(db, rec_id, "450, GROCERY STORE - BEER/WINE")
        assert count >= 1

    def test_empty_license_type(self, db):
        """Empty string returns 0."""
        seed_endorsements(db)
        rec_id = _make_record(db)
        count = process_record(db, rec_id, "")
        assert count == 0


# ── Repair functions (thin wrappers) ────────────────────────────


class TestMergeMixedCase:
    def test_renames_in_place_when_no_upper_exists(self, db):
        """Mixed-case endorsement with no upper counterpart → rename."""
        seed_endorsements(db)
        # Insert a mixed-case endorsement manually
        db.execute("INSERT INTO license_endorsements (name) VALUES (?)", ("Mixed Case",))
        db.commit()

        count = merge_mixed_case_endorsements(db)
        assert count >= 1

        row = db.execute(
            "SELECT name FROM license_endorsements WHERE name = 'MIXED CASE'"
        ).fetchone()
        assert row is not None

    def test_merges_when_upper_exists(self, db):
        """Mixed-case endorsement with existing upper → merge."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        upper_eid = _ensure_endorsement(db, "TEST ENDORSEMENT")
        mixed_eid = db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)", ("Test Endorsement",)
        ).lastrowid
        _link_endorsement(db, rec_id, mixed_eid)
        db.commit()

        merge_mixed_case_endorsements(db)

        # Mixed-case row gone
        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (mixed_eid,)
        ).fetchone()
        assert row is None

        # Link migrated to upper
        link = db.execute(
            "SELECT * FROM record_endorsements WHERE record_id = ? AND endorsement_id = ?",
            (rec_id, upper_eid),
        ).fetchone()
        assert link is not None


class TestRepairCodeName:
    def test_migrates_code_name_endorsement(self, db):
        """CODE, NAME endorsement should be resolved and deleted."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        # Create a spurious "450, GROCERY STORE - BEER/WINE" endorsement
        bogus_eid = db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)",
            ("450, GROCERY STORE - BEER/WINE",),
        ).lastrowid
        _link_endorsement(db, rec_id, bogus_eid)
        db.commit()

        count = repair_code_name_endorsements(db)
        assert count >= 1

        # Bogus endorsement should be gone
        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (bogus_eid,)
        ).fetchone()
        assert row is None


class TestQueryHelpers:
    def test_get_endorsement_options(self, db):
        """Returns endorsement names linked to records."""
        seed_endorsements(db)
        rec_id = _make_record(db)
        process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()

        options = get_endorsement_options(db)
        assert "CANNABIS RETAILER" in options

    def test_get_record_endorsements_batch(self, db):
        """Batch fetch endorsements for multiple records."""
        seed_endorsements(db)
        id1 = _make_record(db, license_number="E001")
        id2 = _make_record(db, license_number="E002")
        process_record(db, id1, "CANNABIS RETAILER")
        process_record(db, id2, "CANNABIS PROCESSOR")
        db.commit()

        result = get_record_endorsements(db, [id1, id2])
        assert "CANNABIS RETAILER" in result[id1]
        assert "CANNABIS PROCESSOR" in result[id2]
