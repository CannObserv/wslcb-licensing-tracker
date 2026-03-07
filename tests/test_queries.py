"""Tests for queries.py — search, filter, stats, and export helpers.

Record insertion tests use ``insert_record`` from ``pipeline`` (its
canonical home); provenance tests use ``get_primary_source`` from
``database``.  All tests use in-memory SQLite via the ``db`` fixture.
"""
import pytest

from pipeline import insert_record
from database import get_primary_source


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


class TestHasAdditionalNamesFlag:
    """insert_record() correctly sets has_additional_names."""

    def _base_record(self, **kwargs):
        base = {
            "section_type": "new_application",
            "record_date": "2025-06-01",
            "business_name": "ACME LLC",
            "business_location": "",
            "applicants": "ACME LLC; JANE DOE",
            "license_type": "CANNABIS RETAILER",
            "application_type": "NEW APPLICATION",
            "license_number": "HAF001",
            "contact_phone": "",
            "previous_business_name": "",
            "previous_applicants": "",
            "previous_business_location": "",
            "city": "", "state": "WA", "zip_code": "",
            "previous_city": "", "previous_state": "", "previous_zip_code": "",
            "scraped_at": "2025-06-01T00:00:00+00:00",
        }
        base.update(kwargs)
        return base

    def test_flag_false_for_normal_record(self, db):
        from pipeline import insert_record
        rec = self._base_record()
        record_id, _ = insert_record(db, rec)
        row = db.execute(
            "SELECT has_additional_names FROM license_records WHERE id = ?",
            (record_id,)
        ).fetchone()
        assert row["has_additional_names"] == 0

    def test_flag_true_for_exact_marker_in_applicants(self, db):
        from pipeline import insert_record
        rec = self._base_record(
            license_number="HAF002",
            applicants="ACME LLC; ADDITIONAL NAMES ON FILE; JANE DOE",
        )
        record_id, _ = insert_record(db, rec)
        row = db.execute(
            "SELECT has_additional_names FROM license_records WHERE id = ?",
            (record_id,)
        ).fetchone()
        assert row["has_additional_names"] == 1

    def test_flag_true_for_typo_marker(self, db):
        from pipeline import insert_record
        rec = self._base_record(
            license_number="HAF003",
            applicants="ACME LLC; ADDTIONAL NAMES ON FILE; BOB SMITH",
        )
        record_id, _ = insert_record(db, rec)
        row = db.execute(
            "SELECT has_additional_names FROM license_records WHERE id = ?",
            (record_id,)
        ).fetchone()
        assert row["has_additional_names"] == 1

    def test_flag_in_record_columns(self, db):
        """has_additional_names is included in RECORD_COLUMNS and hydrated."""
        from pipeline import insert_record; from queries import get_record_by_id
        rec = self._base_record(
            license_number="HAF004",
            applicants="ACME LLC; ADDITIONAL NAMES ON FILE; JANE DOE",
        )
        record_id, _ = insert_record(db, rec)
        db.commit()
        hydrated = get_record_by_id(db, record_id)
        assert hydrated is not None
        assert hydrated["has_additional_names"] == 1


# ── Multi-value endorsement filter ───────────────────────────────────────────

class TestMultiEndorsementFilter:
    """_build_where_clause with endorsements as a list."""

    def _insert_with_endorsement(self, db, record_dict, endorsement_name):
        from pipeline import insert_record
        from endorsements import _ensure_endorsement, _link_endorsement
        rec_id, _ = insert_record(db, record_dict)
        eid = _ensure_endorsement(db, endorsement_name)
        _link_endorsement(db, rec_id, eid)
        db.commit()
        return rec_id

    def test_empty_list_returns_all(self, db, standard_new_application, approved_numeric_code):
        from queries import _build_where_clause
        import copy
        r1 = copy.deepcopy(standard_new_application)
        r2 = copy.deepcopy(approved_numeric_code)
        r2["license_number"] = "DIFF001"
        from pipeline import insert_record
        insert_record(db, r1)
        insert_record(db, r2)
        db.commit()
        where, params, _ = _build_where_clause(db, endorsements=[])
        assert where == ""

    def test_single_endorsement_filters_correctly(self, db, standard_new_application):
        from queries import _build_where_clause, search_records
        import copy
        r1 = copy.deepcopy(standard_new_application)
        r2 = copy.deepcopy(standard_new_application)
        r2["license_number"] = "DIFF002"
        self._insert_with_endorsement(db, r1, "CANNABIS RETAILER")
        from pipeline import insert_record
        from endorsements import _ensure_endorsement, _link_endorsement
        id2, _ = insert_record(db, r2)
        eid2 = _ensure_endorsement(db, "BEER DISTRIBUTOR")
        _link_endorsement(db, id2, eid2)
        db.commit()
        records, total = search_records(db, endorsements=["CANNABIS RETAILER"])
        assert total == 1
        assert records[0]["license_number"] == standard_new_application["license_number"]

    def test_two_endorsements_returns_union(self, db, standard_new_application):
        from queries import search_records
        import copy
        r1 = copy.deepcopy(standard_new_application)
        r2 = copy.deepcopy(standard_new_application)
        r3 = copy.deepcopy(standard_new_application)
        r1["license_number"] = "MULTI001"
        r2["license_number"] = "MULTI002"
        r3["license_number"] = "MULTI003"
        self._insert_with_endorsement(db, r1, "CANNABIS RETAILER")
        self._insert_with_endorsement(db, r2, "BEER DISTRIBUTOR")
        self._insert_with_endorsement(db, r3, "WINE DISTRIBUTOR")
        db.commit()
        records, total = search_records(
            db, endorsements=["CANNABIS RETAILER", "BEER DISTRIBUTOR"]
        )
        assert total == 2
        nums = {r["license_number"] for r in records}
        assert nums == {"MULTI001", "MULTI002"}

    def test_unknown_endorsement_returns_zero(self, db, standard_new_application):
        from queries import search_records
        from pipeline import insert_record
        insert_record(db, standard_new_application)
        db.commit()
        records, total = search_records(db, endorsements=["NONEXISTENT XYZ"])
        assert total == 0

    def test_mixed_known_and_unknown_returns_known_matches(self, db, standard_new_application):
        """When some endorsement names are known and some are not, the known
        ones still filter correctly (OR semantics; unknown names are ignored)."""
        from queries import search_records
        import copy
        r1 = copy.deepcopy(standard_new_application)
        r1["license_number"] = "MIXED001"
        self._insert_with_endorsement(db, r1, "CANNABIS RETAILER")
        db.commit()
        records, total = search_records(
            db, endorsements=["CANNABIS RETAILER", "NONEXISTENT XYZ"]
        )
        assert total == 1
        assert records[0]["license_number"] == "MIXED001"


# ── get_primary_source ─────────────────────────────────────────────

class TestGetPrimarySource:
    def _make_source(self, conn, source_type_id: int, snapshot_path: str | None, captured_at: str) -> int:
        from database import get_or_create_source
        return get_or_create_source(
            conn, source_type_id,
            snapshot_path=snapshot_path,
            url="https://example.com",
            captured_at=captured_at,
        )

    def _link(self, conn, record_id: int, source_id: int, role: str):
        from database import link_record_source
        link_record_source(conn, record_id, source_id, role)

    def test_returns_none_when_no_sources(self, db, standard_new_application):
        from pipeline import insert_record; from database import get_primary_source
        record_id, _ = insert_record(db, standard_new_application)
        assert get_primary_source(db, record_id) is None

    def test_first_seen_preferred_over_confirmed(self, db, standard_new_application):
        from database import SOURCE_TYPE_LIVE_SCRAPE
        from pipeline import insert_record; from database import get_primary_source
        record_id, _ = insert_record(db, standard_new_application)

        s_confirmed = self._make_source(db, SOURCE_TYPE_LIVE_SCRAPE, "path/a.html", "2025-06-15T12:00:00")
        s_first_seen = self._make_source(db, SOURCE_TYPE_LIVE_SCRAPE, "path/b.html", "2025-06-14T12:00:00")
        db.commit()
        self._link(db, record_id, s_confirmed, "confirmed")
        self._link(db, record_id, s_first_seen, "first_seen")
        db.commit()

        result = get_primary_source(db, record_id)
        assert result is not None
        assert result["id"] == s_first_seen

    def test_snapshot_path_preferred_within_role(self, db, standard_new_application):
        from database import SOURCE_TYPE_LIVE_SCRAPE
        from pipeline import insert_record; from database import get_primary_source
        record_id, _ = insert_record(db, standard_new_application)

        s_no_path = self._make_source(db, SOURCE_TYPE_LIVE_SCRAPE, None, "2025-06-15T12:00:00")
        s_with_path = self._make_source(db, SOURCE_TYPE_LIVE_SCRAPE, "path/c.html", "2025-06-14T12:00:00")
        db.commit()
        self._link(db, record_id, s_no_path, "confirmed")
        self._link(db, record_id, s_with_path, "confirmed")
        db.commit()

        result = get_primary_source(db, record_id)
        assert result is not None
        assert result["id"] == s_with_path

    def test_returns_source_dict_fields(self, db, standard_new_application):
        from database import SOURCE_TYPE_CO_ARCHIVE
        from pipeline import insert_record; from database import get_primary_source
        record_id, _ = insert_record(db, standard_new_application)

        s = self._make_source(db, SOURCE_TYPE_CO_ARCHIVE, "path/d.html", "2025-06-10T00:00:00")
        db.commit()
        self._link(db, record_id, s, "first_seen")
        db.commit()

        result = get_primary_source(db, record_id)
        assert result is not None
        assert "snapshot_path" in result
        assert "source_type" in result
        assert "captured_at" in result


# ── export_records ────────────────────────────────────────────────


class TestExportRecords:
    """Tests for export_records() — correlated subquery link columns."""

    def _insert_linked_pair(self, db, new_app, approved):
        """Insert a new_application + approved pair and link them."""
        from pipeline import insert_record
        from link_records import build_all_links

        insert_record(db, new_app)
        insert_record(db, approved)
        db.commit()
        build_all_links(db)
        db.commit()

    def test_unlinked_record_has_null_link_columns(self, db, standard_new_application):
        """A new_application with no outcome has NULL days_to_outcome and outcome_date."""
        from pipeline import insert_record; from queries import export_records

        insert_record(db, standard_new_application)
        db.commit()

        rows = export_records(db)
        assert len(rows) == 1
        assert rows[0]["days_to_outcome"] is None
        assert rows[0]["outcome_date"] is None

    def test_linked_record_has_correct_link_columns(self, db, standard_new_application):
        """A linked new_application has days_to_outcome and outcome_date populated."""
        from queries import export_records

        approved = {
            **standard_new_application,
            "section_type": "approved",
            "record_date": "2025-07-01",
            "applicants": "",
        }
        self._insert_linked_pair(db, standard_new_application, approved)

        rows = export_records(db, section_type="new_application")
        assert len(rows) == 1
        row = rows[0]
        assert row["days_to_outcome"] == 16  # 2025-07-01 - 2025-06-15
        assert row["outcome_date"] == "2025-07-01"
        assert row["outcome_status"] == "approved"

    def test_high_confidence_link_preferred(self, db, standard_new_application):
        """When multiple links exist, high-confidence is returned."""
        from pipeline import insert_record; from queries import export_records

        approved_early = {
            **standard_new_application,
            "section_type": "approved",
            "record_date": "2025-06-20",
            "applicants": "",
        }
        approved_late = {
            **standard_new_application,
            "section_type": "approved",
            "record_date": "2025-06-30",
            "applicants": "",
            "license_number": "078002",
        }
        new_id, _ = insert_record(db, standard_new_application)
        early_id, _ = insert_record(db, approved_early)
        late_id, _ = insert_record(db, approved_late)
        db.commit()
        # Insert links manually: late=high, early=medium
        db.execute(
            "INSERT INTO record_links (new_app_id, outcome_id, confidence, days_gap, linked_at)"
            " VALUES (?, ?, 'medium', 5, datetime('now'))",
            (new_id, early_id),
        )
        db.execute(
            "INSERT INTO record_links (new_app_id, outcome_id, confidence, days_gap, linked_at)"
            " VALUES (?, ?, 'high', 15, datetime('now'))",
            (new_id, late_id),
        )
        db.commit()

        rows = export_records(db, section_type="new_application")
        assert len(rows) == 1
        assert rows[0]["days_to_outcome"] == 15  # high-confidence link

    def test_non_new_application_has_null_link_columns(self, db, standard_new_application):
        """Approved and discontinued records always have NULL link columns."""
        from pipeline import insert_record; from queries import export_records

        approved = {
            **standard_new_application,
            "section_type": "approved",
            "record_date": "2025-07-01",
            "applicants": "",
        }
        insert_record(db, approved)
        db.commit()

        rows = export_records(db, section_type="approved")
        assert len(rows) == 1
        assert rows[0]["days_to_outcome"] is None
        assert rows[0]["outcome_date"] is None
