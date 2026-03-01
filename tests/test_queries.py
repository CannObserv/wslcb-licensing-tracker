"""Tests for queries.py — insert_record and search helpers.

All tests use in-memory SQLite via the ``db`` fixture.
"""
import pytest

from queries import insert_record


# ── insert_record ──────────────────────────────────────────────────


class TestInsertRecord:
    def test_insert_new_record(self, db, standard_new_application):
        result = insert_record(db, standard_new_application)
        assert result is not None
        record_id, is_new = result
        assert is_new is True
        assert isinstance(record_id, int)

    def test_duplicate_returns_existing_id(self, db, standard_new_application):
        id1, new1 = insert_record(db, standard_new_application)
        id2, new2 = insert_record(db, standard_new_application)
        assert id1 == id2
        assert new1 is True
        assert new2 is False

    def test_creates_location(self, db, standard_new_application):
        insert_record(db, standard_new_application)
        loc = db.execute(
            "SELECT raw_address, city, state, zip_code FROM locations"
        ).fetchone()
        assert loc is not None
        assert loc["raw_address"] == "123 MAIN ST, SEATTLE, WA 98101"
        assert loc["city"] == "SEATTLE"

    def test_assumption_record(self, db, assumption_record):
        result = insert_record(db, assumption_record)
        assert result is not None
        record_id, is_new = result
        assert is_new is True

        row = db.execute(
            "SELECT business_name, previous_business_name, "
            "applicants, previous_applicants "
            "FROM license_records WHERE id = ?",
            (record_id,),
        ).fetchone()
        assert row["business_name"] == "NEW LEAF DISPENSARY"
        assert row["previous_business_name"] == "OLD SMOKE SHOP"
        assert "CAROL NEWBY" in row["applicants"]
        assert "ALICE OLDEN" in row["previous_applicants"]

    def test_change_of_location_creates_two_locations(self, db, change_of_location_record):
        result = insert_record(db, change_of_location_record)
        assert result is not None
        record_id, _ = result

        row = db.execute(
            "SELECT location_id, previous_location_id "
            "FROM license_records WHERE id = ?",
            (record_id,),
        ).fetchone()
        assert row["location_id"] is not None
        assert row["previous_location_id"] is not None
        assert row["location_id"] != row["previous_location_id"]

    def test_creates_entities(self, db, standard_new_application):
        """insert_record should create entity rows for applicants."""
        result = insert_record(db, standard_new_application)
        record_id, _ = result

        entities = db.execute(
            "SELECT e.name, re.role FROM record_entities re "
            "JOIN entities e ON e.id = re.entity_id "
            "WHERE re.record_id = ? ORDER BY re.position",
            (record_id,),
        ).fetchall()
        # Business name (first element) is excluded; only JOHN DOE, JANE SMITH
        names = [e["name"] for e in entities]
        assert "JOHN DOE" in names
        assert "JANE SMITH" in names
        assert "ACME CANNABIS CO" not in names  # business name excluded

    def test_cleans_business_name(self, db, standard_new_application):
        """Stray trailing punctuation is stripped from business_name."""
        record = {**standard_new_application,
                  "business_name": "dirty name.",
                  "license_number": "CLEAN01"}
        record_id, _ = insert_record(db, record)
        row = db.execute(
            "SELECT business_name FROM license_records WHERE id = ?",
            (record_id,),
        ).fetchone()
        assert row["business_name"] == "DIRTY NAME"

    def test_no_location_for_empty_address(self, db, approved_numeric_code):
        """Records with no address should have location_id = NULL."""
        record = {**approved_numeric_code,
                  "business_location": "",
                  "city": "", "zip_code": "",
                  "license_number": "NOADDR01"}
        record_id, _ = insert_record(db, record)
        row = db.execute(
            "SELECT location_id FROM license_records WHERE id = ?",
            (record_id,),
        ).fetchone()
        assert row["location_id"] is None
