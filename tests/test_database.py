"""Tests for db.py location/source/provenance helpers.

Covers get_or_create_location, get_or_create_source, link_record_source,
get_primary_source, and get_record_sources.  Connection and constant tests
live in test_db.py; schema/migration tests live in test_schema.py.

All tests use in-memory SQLite via the ``db`` fixture.
"""
import sqlite3

import pytest

from db import (
    get_connection,
    get_or_create_location,
    get_or_create_source,
    link_record_source,
    SOURCE_TYPE_LIVE_SCRAPE,
    SOURCE_TYPE_CO_ARCHIVE,
)
from schema import init_db


# ── get_or_create_location ────────────────────────────────────────────────


class TestGetOrCreateLocation:
    def test_creates_new_location(self, db):
        loc_id = get_or_create_location(
            db, "123 MAIN ST, SEATTLE, WA 98101",
            city="SEATTLE", state="WA", zip_code="98101",
        )
        assert loc_id is not None
        assert isinstance(loc_id, int)

    def test_returns_same_id_for_duplicate(self, db):
        addr = "123 MAIN ST, SEATTLE, WA 98101"
        id1 = get_or_create_location(db, addr, city="SEATTLE")
        id2 = get_or_create_location(db, addr, city="SEATTLE")
        assert id1 == id2

    def test_returns_none_for_empty(self, db):
        assert get_or_create_location(db, "") is None
        assert get_or_create_location(db, None) is None
        assert get_or_create_location(db, "   ") is None

    def test_nbsp_normalization(self, db):
        """Addresses with NBSP and regular spaces map to the same location."""
        addr_nbsp = "123 MAIN\xa0ST, SEATTLE, WA 98101"
        addr_space = "123 MAIN ST, SEATTLE, WA 98101"
        id1 = get_or_create_location(db, addr_nbsp)
        id2 = get_or_create_location(db, addr_space)
        assert id1 == id2

    def test_stores_city_state_zip(self, db):
        loc_id = get_or_create_location(
            db, "456 OAK AVE, SPOKANE, WA 99201",
            city="SPOKANE", state="WA", zip_code="99201",
        )
        row = db.execute(
            "SELECT city, state, zip_code FROM locations WHERE id = ?",
            (loc_id,),
        ).fetchone()
        assert row["city"] == "SPOKANE"
        assert row["state"] == "WA"
        assert row["zip_code"] == "99201"


# ── get_or_create_source ──────────────────────────────────────────────────


class TestGetOrCreateSource:
    def test_creates_source(self, db):
        sid = get_or_create_source(
            db, SOURCE_TYPE_CO_ARCHIVE,
            snapshot_path="wslcb/test/snapshot.html",
        )
        assert isinstance(sid, int)

    def test_idempotent_with_path(self, db):
        path = "wslcb/test/snapshot.html"
        id1 = get_or_create_source(db, SOURCE_TYPE_CO_ARCHIVE, snapshot_path=path)
        id2 = get_or_create_source(db, SOURCE_TYPE_CO_ARCHIVE, snapshot_path=path)
        assert id1 == id2

    def test_null_path_with_scrape_log_id(self, db):
        """Distinct scrape_log_ids with NULL path get separate source rows."""
        db.execute(
            "INSERT INTO scrape_log (started_at, status) VALUES ('2025-01-01', 'ok')"
        )
        log1 = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO scrape_log (started_at, status) VALUES ('2025-01-02', 'ok')"
        )
        log2 = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        id1 = get_or_create_source(
            db, SOURCE_TYPE_LIVE_SCRAPE, scrape_log_id=log1,
        )
        id2 = get_or_create_source(
            db, SOURCE_TYPE_LIVE_SCRAPE, scrape_log_id=log2,
        )
        assert id1 != id2


# ── link_record_source ─────────────────────────────────────────────────────


class TestLinkRecordSource:
    def test_link_and_idempotent(self, db):
        """Linking the same record+source twice doesn't raise."""
        db.execute(
            """INSERT INTO license_records
               (section_type, record_date, business_name, applicants,
                license_type, application_type, license_number,
                contact_phone, scraped_at)
               VALUES ('new_application', '2025-01-01', 'TEST', '', '',
                       'NEW APPLICATION', '999999', '', '2025-01-01')"""
        )
        rec_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        src_id = get_or_create_source(
            db, SOURCE_TYPE_CO_ARCHIVE, snapshot_path="test.html",
        )
        link_record_source(db, rec_id, src_id, "first_seen")
        link_record_source(db, rec_id, src_id, "first_seen")  # idempotent

        count = db.execute(
            "SELECT count(*) FROM record_sources WHERE record_id = ?",
            (rec_id,),
        ).fetchone()[0]
        assert count == 1

    def test_multiple_roles(self, db):
        """Same record+source can have different roles."""
        db.execute(
            """INSERT INTO license_records
               (section_type, record_date, business_name, applicants,
                license_type, application_type, license_number,
                contact_phone, scraped_at)
               VALUES ('new_application', '2025-01-01', 'TEST', '', '',
                       'NEW APPLICATION', '999998', '', '2025-01-01')"""
        )
        rec_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        src_id = get_or_create_source(
            db, SOURCE_TYPE_CO_ARCHIVE, snapshot_path="test2.html",
        )
        link_record_source(db, rec_id, src_id, "first_seen")
        link_record_source(db, rec_id, src_id, "confirmed")

        count = db.execute(
            "SELECT count(*) FROM record_sources WHERE record_id = ?",
            (rec_id,),
        ).fetchone()[0]
        assert count == 2
