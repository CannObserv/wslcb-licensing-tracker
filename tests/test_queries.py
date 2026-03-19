"""Tests for queries.py — search, filter, stats, and export helpers.

Record insertion tests use ``insert_record`` from ``pipeline`` (its
canonical home); provenance tests use ``get_primary_source`` from
``database``.  All tests use in-memory SQLite via the ``db`` fixture.
"""
import pytest

from wslcb_licensing_tracker.pipeline import insert_record
from wslcb_licensing_tracker.db import get_primary_source


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
        from wslcb_licensing_tracker.pipeline import insert_record
        rec = self._base_record()
        record_id, _ = insert_record(db, rec)
        row = db.execute(
            "SELECT has_additional_names FROM license_records WHERE id = ?",
            (record_id,)
        ).fetchone()
        assert row["has_additional_names"] == 0

    def test_flag_true_for_exact_marker_in_applicants(self, db):
        from wslcb_licensing_tracker.pipeline import insert_record
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
        from wslcb_licensing_tracker.pipeline import insert_record
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
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.queries import get_record_by_id
        rec = self._base_record(
            license_number="HAF004",
            applicants="ACME LLC; ADDITIONAL NAMES ON FILE; JANE DOE",
        )
        record_id, _ = insert_record(db, rec)
        db.commit()
        hydrated = get_record_by_id(db, record_id)
        assert hydrated is not None
        assert hydrated["has_additional_names"] == 1


# ── std_address_line alias rename ────────────────────────────────────────────


class TestStdAddressLineAliases:
    """RECORD_COLUMNS and _EXPORT_SELECT expose std_address_line_1/2 keys."""

    def _insert_record_with_address(self, db, record_dict):
        from wslcb_licensing_tracker.pipeline import insert_record

        rec_id, _ = insert_record(db, record_dict)
        db.commit()
        return rec_id

    def test_record_columns_exposes_std_address_line_1(self, db, standard_new_application):
        """get_record_by_id result has std_address_line_1, not address_line_1."""
        from wslcb_licensing_tracker.queries import get_record_by_id

        rec_id = self._insert_record_with_address(db, standard_new_application)
        row = get_record_by_id(db, rec_id)
        assert row is not None
        assert "std_address_line_1" in row
        assert "address_line_1" not in row

    def test_record_columns_exposes_std_address_line_2(self, db, standard_new_application):
        """get_record_by_id result has std_address_line_2, not address_line_2."""
        from wslcb_licensing_tracker.queries import get_record_by_id

        rec_id = self._insert_record_with_address(db, standard_new_application)
        row = get_record_by_id(db, rec_id)
        assert row is not None
        assert "std_address_line_2" in row
        assert "address_line_2" not in row

    def test_record_columns_exposes_prev_std_address_line_1(self, db, change_of_location_record):
        """get_record_by_id result has prev_std_address_line_1, not prev_address_line_1."""
        from wslcb_licensing_tracker.queries import get_record_by_id

        rec_id = self._insert_record_with_address(db, change_of_location_record)
        row = get_record_by_id(db, rec_id)
        assert row is not None
        assert "prev_std_address_line_1" in row
        assert "prev_address_line_1" not in row

    def test_record_columns_exposes_prev_std_address_line_2(self, db, change_of_location_record):
        """get_record_by_id result has prev_std_address_line_2, not prev_address_line_2."""
        from wslcb_licensing_tracker.queries import get_record_by_id

        rec_id = self._insert_record_with_address(db, change_of_location_record)
        row = get_record_by_id(db, rec_id)
        assert row is not None
        assert "prev_std_address_line_2" in row
        assert "prev_address_line_2" not in row

    def test_export_select_exposes_std_address_line_1(self, db, standard_new_application):
        """export_records result dicts have std_address_line_1, not address_line_1."""
        from wslcb_licensing_tracker.queries import export_records

        self._insert_record_with_address(db, standard_new_application)
        rows = export_records(db)
        assert len(rows) > 0
        assert "std_address_line_1" in rows[0]
        assert "address_line_1" not in rows[0]

    def test_export_select_exposes_std_address_line_2(self, db, standard_new_application):
        """export_records result dicts have std_address_line_2, not address_line_2."""
        from wslcb_licensing_tracker.queries import export_records

        self._insert_record_with_address(db, standard_new_application)
        rows = export_records(db)
        assert len(rows) > 0
        assert "std_address_line_2" in rows[0]
        assert "address_line_2" not in rows[0]

    def test_export_select_exposes_prev_std_address_line_1(self, db, change_of_location_record):
        """export_records result dicts have prev_std_address_line_1, not prev_address_line_1."""
        from wslcb_licensing_tracker.queries import export_records

        self._insert_record_with_address(db, change_of_location_record)
        rows = export_records(db)
        assert len(rows) > 0
        assert "prev_std_address_line_1" in rows[0]
        assert "prev_address_line_1" not in rows[0]

    def test_export_select_exposes_prev_std_address_line_2(self, db, change_of_location_record):
        """export_records result dicts have prev_std_address_line_2, not prev_address_line_2."""
        from wslcb_licensing_tracker.queries import export_records

        self._insert_record_with_address(db, change_of_location_record)
        rows = export_records(db)
        assert len(rows) > 0
        assert "prev_std_address_line_2" in rows[0]
        assert "prev_address_line_2" not in rows[0]


# ── Multi-value endorsement filter ───────────────────────────────────────────

class TestMultiEndorsementFilter:
    """_build_where_clause with endorsements as a list."""

    def _insert_with_endorsement(self, db, record_dict, endorsement_name):
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.endorsements import ensure_endorsement, link_endorsement
        rec_id, _ = insert_record(db, record_dict)
        eid = ensure_endorsement(db, endorsement_name)
        link_endorsement(db, rec_id, eid)
        db.commit()
        return rec_id

    def test_empty_list_returns_all(self, db, standard_new_application, approved_numeric_code):
        from wslcb_licensing_tracker.queries import _build_where_clause
        import copy
        r1 = copy.deepcopy(standard_new_application)
        r2 = copy.deepcopy(approved_numeric_code)
        r2["license_number"] = "DIFF001"
        from wslcb_licensing_tracker.pipeline import insert_record
        insert_record(db, r1)
        insert_record(db, r2)
        db.commit()
        where, params, _ = _build_where_clause(db, endorsements=[])
        assert where == ""

    def test_single_endorsement_filters_correctly(self, db, standard_new_application):
        from wslcb_licensing_tracker.queries import _build_where_clause, search_records
        import copy
        r1 = copy.deepcopy(standard_new_application)
        r2 = copy.deepcopy(standard_new_application)
        r2["license_number"] = "DIFF002"
        self._insert_with_endorsement(db, r1, "CANNABIS RETAILER")
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.endorsements import ensure_endorsement, link_endorsement
        id2, _ = insert_record(db, r2)
        eid2 = ensure_endorsement(db, "BEER DISTRIBUTOR")
        link_endorsement(db, id2, eid2)
        db.commit()
        records, total = search_records(db, endorsements=["CANNABIS RETAILER"])
        assert total == 1
        assert records[0]["license_number"] == standard_new_application["license_number"]

    def test_two_endorsements_returns_union(self, db, standard_new_application):
        from wslcb_licensing_tracker.queries import search_records
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
        from wslcb_licensing_tracker.queries import search_records
        from wslcb_licensing_tracker.pipeline import insert_record
        insert_record(db, standard_new_application)
        db.commit()
        records, total = search_records(db, endorsements=["NONEXISTENT XYZ"])
        assert total == 0

    def test_mixed_known_and_unknown_returns_known_matches(self, db, standard_new_application):
        """When some endorsement names are known and some are not, the known
        ones still filter correctly (OR semantics; unknown names are ignored)."""
        from wslcb_licensing_tracker.queries import search_records
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
        from wslcb_licensing_tracker.db import get_or_create_source
        return get_or_create_source(
            conn, source_type_id,
            snapshot_path=snapshot_path,
            url="https://example.com",
            captured_at=captured_at,
        )

    def _link(self, conn, record_id: int, source_id: int, role: str):
        from wslcb_licensing_tracker.db import link_record_source
        link_record_source(conn, record_id, source_id, role)

    def test_returns_none_when_no_sources(self, db, standard_new_application):
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.db import get_primary_source
        record_id, _ = insert_record(db, standard_new_application)
        assert get_primary_source(db, record_id) is None

    def test_first_seen_preferred_over_confirmed(self, db, standard_new_application):
        from wslcb_licensing_tracker.db import SOURCE_TYPE_LIVE_SCRAPE
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.db import get_primary_source
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
        from wslcb_licensing_tracker.db import SOURCE_TYPE_LIVE_SCRAPE
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.db import get_primary_source
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
        from wslcb_licensing_tracker.db import SOURCE_TYPE_CO_ARCHIVE
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.db import get_primary_source
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
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.link_records import build_all_links

        insert_record(db, new_app)
        insert_record(db, approved)
        db.commit()
        build_all_links(db)
        db.commit()

    def test_unlinked_record_has_null_link_columns(self, db, standard_new_application):
        """A new_application with no outcome has NULL days_to_outcome and outcome_date."""
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.queries import export_records

        insert_record(db, standard_new_application)
        db.commit()

        rows = export_records(db)
        assert len(rows) == 1
        assert rows[0]["days_to_outcome"] is None
        assert rows[0]["outcome_date"] is None

    def test_linked_record_has_correct_link_columns(self, db, standard_new_application):
        """A linked new_application has days_to_outcome and outcome_date populated."""
        from wslcb_licensing_tracker.queries import export_records

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
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.queries import export_records

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
        from wslcb_licensing_tracker.pipeline import insert_record; from wslcb_licensing_tracker.queries import export_records

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

    def test_outcome_status_pending(self, db, standard_new_application):
        """Unlinked linkable new_application with recent date → 'pending'.

        Uses RENEWAL (not NEW APPLICATION) to avoid the data_gap branch,
        which fires for any NEW APPLICATION after DATA_GAP_CUTOFF.
        """
        from datetime import date, timedelta
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import export_records

        recent = (date.today() - timedelta(days=10)).isoformat()
        rec = {**standard_new_application, "record_date": recent,
               "application_type": "RENEWAL"}
        insert_record(db, rec)
        db.commit()

        rows = export_records(db, section_type="new_application")
        assert len(rows) == 1
        assert rows[0]["outcome_status"] == "pending"

    def test_outcome_status_unknown(self, db, standard_new_application):
        """Unlinked linkable new_application older than PENDING_CUTOFF_DAYS → 'unknown'.

        Uses RENEWAL (not NEW APPLICATION) to avoid the data_gap branch.
        """
        from datetime import date, timedelta
        from wslcb_licensing_tracker.link_records import PENDING_CUTOFF_DAYS
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import export_records

        old = (date.today() - timedelta(days=PENDING_CUTOFF_DAYS + 10)).isoformat()
        rec = {**standard_new_application, "record_date": old,
               "application_type": "RENEWAL"}
        insert_record(db, rec)
        db.commit()

        rows = export_records(db, section_type="new_application")
        assert len(rows) == 1
        assert rows[0]["outcome_status"] == "unknown"

    def test_outcome_status_data_gap(self, db, standard_new_application):
        """Unlinked NEW APPLICATION after DATA_GAP_CUTOFF → 'data_gap'."""
        from wslcb_licensing_tracker.link_records import DATA_GAP_CUTOFF
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import export_records
        from datetime import date

        # Use a date just after the cutoff that is also old enough to not be 'pending'
        gap_date = "2025-05-15"  # after DATA_GAP_CUTOFF (2025-05-12)
        assert gap_date > DATA_GAP_CUTOFF
        rec = {**standard_new_application, "record_date": gap_date}
        insert_record(db, rec)
        db.commit()

        rows = export_records(db, section_type="new_application")
        assert len(rows) == 1
        assert rows[0]["outcome_status"] == "data_gap"

    def test_outcome_status_null_for_non_linkable_type(self, db, standard_new_application):
        """new_application with non-linkable application_type → NULL outcome_status."""
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import export_records

        rec = {**standard_new_application, "application_type": "SOME UNLINKABLE TYPE"}
        insert_record(db, rec)
        db.commit()

        rows = export_records(db, section_type="new_application")
        assert len(rows) == 1
        assert rows[0]["outcome_status"] is None


class TestExportRecordsCursor:
    """Tests for export_records_cursor() — streaming generator variant."""

    def test_yields_dicts_matching_export_records(self, db, standard_new_application):
        """export_records_cursor yields the same rows as export_records."""
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import export_records, export_records_cursor

        insert_record(db, standard_new_application)
        db.commit()

        expected = export_records(db)
        streamed = list(export_records_cursor(db))
        assert streamed == expected

    def test_returns_generator(self, db):
        """export_records_cursor returns a generator (not a list)."""
        import types
        from wslcb_licensing_tracker.queries import export_records_cursor

        assert isinstance(export_records_cursor(db), types.GeneratorType)

    def test_empty_db_yields_nothing(self, db):
        """export_records_cursor on an empty DB yields no rows."""
        from wslcb_licensing_tracker.queries import export_records_cursor

        rows = list(export_records_cursor(db))
        assert rows == []

    def test_filters_applied(self, db, standard_new_application):
        """export_records_cursor respects filter arguments (section_type)."""
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import export_records_cursor

        insert_record(db, standard_new_application)
        approved = {
            **standard_new_application,
            "section_type": "approved",
            "record_date": "2025-07-01",
            "applicants": "",
        }
        insert_record(db, approved)
        db.commit()

        rows = list(export_records_cursor(db, section_type="approved"))
        assert len(rows) == 1
        assert rows[0]["section_type"] == "approved"


# ── get_entities ──────────────────────────────────────────────────


class TestGetEntities:
    """Tests for get_entities() — paginated, searchable entity list."""

    def _insert_entities(self, db):
        """Insert a mix of persons and an organization with record links."""
        from wslcb_licensing_tracker.pipeline import insert_record

        # applicants format: "BUSINESS NAME; PERSON1; PERSON2"
        # parse_and_link_entities skips the first element (business name).
        records = [
            {"section_type": "new_application", "record_date": "2025-06-01",
             "business_name": "ACME CO", "business_location": "1 MAIN ST, SEATTLE, WA",
             "applicants": "ACME CO; ALICE JONES", "license_type": "CANNABIS RETAILER",
             "application_type": "NEW APPLICATION", "license_number": "111001",
             "contact_phone": "", "city": "SEATTLE", "state": "WA", "zip_code": "98101",
             "previous_business_name": "", "previous_applicants": "",
             "previous_business_location": "", "previous_city": "",
             "previous_state": "", "previous_zip_code": "",
             "scraped_at": "2025-06-01T12:00:00+00:00"},
            {"section_type": "new_application", "record_date": "2025-06-02",
             "business_name": "BOB SHOP", "business_location": "2 ELM ST, TACOMA, WA",
             "applicants": "BOB SHOP; BOB SMITH", "license_type": "CANNABIS RETAILER",
             "application_type": "NEW APPLICATION", "license_number": "111002",
             "contact_phone": "", "city": "TACOMA", "state": "WA", "zip_code": "98402",
             "previous_business_name": "", "previous_applicants": "",
             "previous_business_location": "", "previous_city": "",
             "previous_state": "", "previous_zip_code": "",
             "scraped_at": "2025-06-02T12:00:00+00:00"},
            {"section_type": "new_application", "record_date": "2025-06-03",
             "business_name": "ACME CO", "business_location": "1 MAIN ST, SEATTLE, WA",
             "applicants": "ACME CO; ALICE JONES; ACME HOLDINGS LLC",
             "license_type": "CANNABIS RETAILER",
             "application_type": "NEW APPLICATION", "license_number": "111003",
             "contact_phone": "", "city": "SEATTLE", "state": "WA", "zip_code": "98101",
             "previous_business_name": "", "previous_applicants": "",
             "previous_business_location": "", "previous_city": "",
             "previous_state": "", "previous_zip_code": "",
             "scraped_at": "2025-06-03T12:00:00+00:00"},
        ]
        for r in records:
            insert_record(db, r)
        db.commit()

    def test_returns_all_entities(self, db):
        """No filters returns all entities with record counts."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db)
        assert result["total"] == 3  # ALICE JONES, BOB SMITH, ACME HOLDINGS LLC
        assert len(result["entities"]) == 3

    def test_result_fields(self, db):
        """Each row has id, name, entity_type, record_count."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db)
        row = result["entities"][0]
        assert "id" in row
        assert "name" in row
        assert "entity_type" in row
        assert "record_count" in row

    def test_default_sort_by_count_desc(self, db):
        """Default sort is record_count descending (most active first)."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db)
        counts = [r["record_count"] for r in result["entities"]]
        assert counts == sorted(counts, reverse=True)

    def test_sort_by_name_asc(self, db):
        """sort='name' returns entities alphabetically."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db, sort="name")
        names = [r["name"] for r in result["entities"]]
        assert names == sorted(names)

    def test_search_by_name(self, db):
        """q filter narrows results to matching entity names."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db, q="alice")
        assert result["total"] == 1
        assert result["entities"][0]["name"] == "ALICE JONES"

    def test_search_case_insensitive(self, db):
        """Name search is case-insensitive."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db, q="ALICE")
        assert result["total"] == 1

    def test_search_partial_match(self, db):
        """Partial name matches are returned."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db, q="jones")
        assert result["total"] == 1

    def test_filter_by_type_person(self, db):
        """entity_type='person' returns only person entities."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db, entity_type="person")
        assert all(r["entity_type"] == "person" for r in result["entities"])
        assert result["total"] == 2

    def test_filter_by_type_organization(self, db):
        """entity_type='organization' returns only org entities."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db, entity_type="organization")
        assert result["total"] == 1
        assert result["entities"][0]["name"] == "ACME HOLDINGS LLC"

    def test_pagination(self, db):
        """page and per_page control which rows are returned."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        page1 = get_entities(db, per_page=2, page=1)
        page2 = get_entities(db, per_page=2, page=2)
        assert len(page1["entities"]) == 2
        assert len(page2["entities"]) == 1
        assert page1["total"] == 3
        assert page2["total"] == 3
        # No overlap between pages
        ids1 = {r["id"] for r in page1["entities"]}
        ids2 = {r["id"] for r in page2["entities"]}
        assert ids1.isdisjoint(ids2)

    def test_empty_db(self, db):
        """Empty entities table returns empty list and zero total."""
        from wslcb_licensing_tracker.queries import get_entities

        result = get_entities(db)
        assert result["total"] == 0
        assert result["entities"] == []

    def test_search_no_match(self, db):
        """Search that matches nothing returns empty list, total=0."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result = get_entities(db, q="zzznomatch")
        assert result["total"] == 0
        assert result["entities"] == []

    def test_page_zero_clamped_to_one(self, db):
        """page=0 is clamped to page=1; does not produce negative OFFSET."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result_zero = get_entities(db, per_page=2, page=0)
        result_one = get_entities(db, per_page=2, page=1)
        assert result_zero["entities"] == result_one["entities"]

    def test_negative_page_clamped_to_one(self, db):
        """Negative page values are clamped to page=1."""
        from wslcb_licensing_tracker.queries import get_entities

        self._insert_entities(db)
        result_neg = get_entities(db, per_page=2, page=-5)
        result_one = get_entities(db, per_page=2, page=1)
        assert result_neg["entities"] == result_one["entities"]


class TestGetStats:
    """Tests for get_stats() — batched queries and short-TTL cache."""

    def teardown_method(self):
        from wslcb_licensing_tracker import queries

        queries._stats_cache.clear()

    def test_returns_expected_keys(self, db, standard_new_application):
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import get_stats

        insert_record(db, standard_new_application)
        db.commit()
        stats = get_stats(db)
        for key in (
            "total_records", "new_application_count", "approved_count",
            "discontinued_count", "date_range", "unique_businesses",
            "unique_licenses", "unique_entities", "last_scrape", "pipeline",
        ):
            assert key in stats, f"missing key: {key}"

    def test_unique_businesses_counts_distinct_names(self, db, standard_new_application):
        import copy
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import get_stats

        r2 = copy.deepcopy(standard_new_application)
        r2["license_number"] = "STAT001"
        r2["business_name"] = "OTHER BUSINESS LLC"
        insert_record(db, standard_new_application)
        insert_record(db, r2)
        db.commit()
        assert get_stats(db)["unique_businesses"] == 2

    def test_unique_businesses_deduplicates_same_name(self, db, standard_new_application):
        import copy
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import get_stats

        r2 = copy.deepcopy(standard_new_application)
        r2["license_number"] = "STAT002"
        r2["record_date"] = "2025-07-01"
        insert_record(db, standard_new_application)
        insert_record(db, r2)
        db.commit()
        assert get_stats(db)["unique_businesses"] == 1

    def test_unique_licenses_counts_distinct_numbers(self, db, standard_new_application):
        import copy
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import get_stats

        r2 = copy.deepcopy(standard_new_application)
        r2["license_number"] = "STAT003"
        r2["record_date"] = "2025-07-01"
        insert_record(db, standard_new_application)
        insert_record(db, r2)
        db.commit()
        assert get_stats(db)["unique_licenses"] == 2

    def test_unique_entities_counts_entity_rows(self, db, standard_new_application):
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import get_stats

        # standard_new_application creates 2 entities: JOHN DOE, JANE SMITH
        insert_record(db, standard_new_application)
        db.commit()
        assert get_stats(db)["unique_entities"] == 2

    def test_pipeline_has_expected_keys(self, db):
        from wslcb_licensing_tracker.queries import get_stats

        pipeline = get_stats(db)["pipeline"]
        for key in ("total", "approved", "discontinued", "pending", "data_gap", "unknown"):
            assert key in pipeline, f"missing pipeline key: {key}"

    def test_result_is_cached_within_ttl(self, db, standard_new_application):
        import copy
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker.queries import get_stats

        insert_record(db, standard_new_application)
        db.commit()
        first = get_stats(db)

        # Insert another record; within TTL, get_stats must return stale cache
        r2 = copy.deepcopy(standard_new_application)
        r2["license_number"] = "CACHED001"
        insert_record(db, r2)
        db.commit()
        second = get_stats(db)

        assert second["total_records"] == first["total_records"]

    def test_invalidate_clears_stats_cache(self, db, standard_new_application):
        from wslcb_licensing_tracker.pipeline import insert_record
        from wslcb_licensing_tracker import queries

        insert_record(db, standard_new_application)
        db.commit()
        queries.get_stats(db)
        assert queries._stats_cache, "cache should be populated after get_stats"
        queries.invalidate_all_filter_caches()
        assert not queries._stats_cache, "invalidate should clear _stats_cache"


class TestInvalidateAllFilterCaches:
    def teardown_method(self):
        from wslcb_licensing_tracker import queries

        queries._filter_cache.clear()
        queries._city_cache.clear()

    def test_clears_both_caches(self):
        from wslcb_licensing_tracker import queries

        queries._filter_cache["data"] = "stale"
        queries._city_cache["WA"] = (0.0, ["Seattle"])
        queries.invalidate_all_filter_caches()
        assert not queries._filter_cache
        assert not queries._city_cache

    def test_idempotent_on_empty_caches(self):
        from wslcb_licensing_tracker import queries

        queries._filter_cache.clear()
        queries._city_cache.clear()
        queries.invalidate_all_filter_caches()  # should not raise
        assert not queries._filter_cache
        assert not queries._city_cache
