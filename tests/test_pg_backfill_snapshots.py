"""Tests for pg_backfill_snapshots.py — two-phase snapshot ingest and repair helpers."""

import os
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.models import license_records
from wslcb_licensing_tracker.pg_backfill_snapshots import (
    _repair_assumptions,
    _repair_change_of_location,
    backfill_from_snapshots,
)
from wslcb_licensing_tracker.pg_db import (
    SOURCE_TYPE_CO_ARCHIVE,
    WSLCB_SOURCE_URL,
    get_or_create_location,
    get_or_create_source,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
SNAPSHOT_FIXTURE = FIXTURES_DIR / "snapshot_two_records.html"

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)

_SCRAPED_AT = datetime(2025, 6, 10, 12, 0, 0, tzinfo=UTC)


# ── DB helpers ────────────────────────────────────────────────────────


async def _insert_assumption_empty_name(conn, license_number: str) -> int:
    """Insert an ASSUMPTION record with an empty business_name for repair tests."""
    loc_id = await get_or_create_location(
        conn, "789 PINE ST, TACOMA, WA 98402", city="TACOMA", state="WA", zip_code="98402"
    )
    result = await conn.execute(
        pg_insert(license_records)
        .values(
            section_type="new_application",
            record_date="2025-06-10",
            business_name="",
            location_id=loc_id,
            application_type="ASSUMPTION",
            license_number=license_number,
            scraped_at=_SCRAPED_AT,
        )
        .returning(license_records.c.id)
    )
    return result.scalar_one()


async def _insert_change_of_location_no_prev(conn, license_number: str) -> int:
    """Insert a CHANGE OF LOCATION record with previous_location_id=NULL."""
    loc_id = await get_or_create_location(
        conn, "200 NEW BLVD, OLYMPIA, WA 98502", city="OLYMPIA", state="WA", zip_code="98502"
    )
    result = await conn.execute(
        pg_insert(license_records)
        .values(
            section_type="new_application",
            record_date="2025-06-12",
            business_name="MOVING SPIRITS LLC",
            location_id=loc_id,
            application_type="CHANGE OF LOCATION",
            license_number=license_number,
            scraped_at=_SCRAPED_AT,
            previous_location_id=None,
        )
        .returning(license_records.c.id)
    )
    return result.scalar_one()


# ── _repair_assumptions ───────────────────────────────────────────────


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_repair_assumptions_updates_empty_business_name(pg_conn):
    """_repair_assumptions fills in empty business_name from the repair record."""
    license_number = "RASM001"
    record_id = await _insert_assumption_empty_name(pg_conn, license_number)

    source_id = await get_or_create_source(pg_conn, SOURCE_TYPE_CO_ARCHIVE, url=WSLCB_SOURCE_URL)

    repair_records = [
        {
            "application_type": "ASSUMPTION",
            "section_type": "new_application",
            "record_date": "2025-06-10",
            "license_number": license_number,
            "business_name": "NEW LEAF DISPENSARY",
            "applicants": "NEW LEAF DISPENSARY; CAROL NEWBY",
        }
    ]
    updated = await _repair_assumptions(pg_conn, repair_records, source_id)
    assert updated == 1

    row = (
        await pg_conn.execute(
            select(license_records.c.business_name).where(license_records.c.id == record_id)
        )
    ).one()
    assert row.business_name == "NEW LEAF DISPENSARY"


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_repair_assumptions_skips_already_named_record(pg_conn):
    """_repair_assumptions does not overwrite a record that already has a name."""
    license_number = "RASM002"
    loc_id = await get_or_create_location(
        pg_conn, "789 PINE ST, TACOMA, WA 98402", city="TACOMA", state="WA", zip_code="98402"
    )
    await pg_conn.execute(
        pg_insert(license_records).values(
            section_type="new_application",
            record_date="2025-06-10",
            business_name="EXISTING NAME",
            location_id=loc_id,
            application_type="ASSUMPTION",
            license_number=license_number,
            scraped_at=_SCRAPED_AT,
        )
    )

    source_id = await get_or_create_source(pg_conn, SOURCE_TYPE_CO_ARCHIVE, url=WSLCB_SOURCE_URL)
    repair_records = [
        {
            "application_type": "ASSUMPTION",
            "section_type": "new_application",
            "record_date": "2025-06-10",
            "license_number": license_number,
            "business_name": "REPLACEMENT NAME",
            "applicants": "",
        }
    ]
    updated = await _repair_assumptions(pg_conn, repair_records, source_id)
    assert updated == 0


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_repair_assumptions_ignores_non_assumption_records(pg_conn):
    """_repair_assumptions skips records with application_type != ASSUMPTION."""
    source_id = await get_or_create_source(pg_conn, SOURCE_TYPE_CO_ARCHIVE, url=WSLCB_SOURCE_URL)
    non_assumption = [
        {
            "application_type": "NEW APPLICATION",
            "section_type": "new_application",
            "record_date": "2025-06-10",
            "license_number": "RASM003",
            "business_name": "SOME BUSINESS",
            "applicants": "",
        }
    ]
    updated = await _repair_assumptions(pg_conn, non_assumption, source_id)
    assert updated == 0


# ── _repair_change_of_location ────────────────────────────────────────


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_repair_change_of_location_sets_previous_location(pg_conn):
    """_repair_change_of_location fills in a NULL previous_location_id."""
    license_number = "RCOL001"
    record_id = await _insert_change_of_location_no_prev(pg_conn, license_number)

    source_id = await get_or_create_source(pg_conn, SOURCE_TYPE_CO_ARCHIVE, url=WSLCB_SOURCE_URL)

    repair_records = [
        {
            "application_type": "CHANGE OF LOCATION",
            "section_type": "new_application",
            "record_date": "2025-06-12",
            "license_number": license_number,
            "previous_business_location": "100 OLD RD, OLYMPIA, WA 98501",
            "previous_city": "OLYMPIA",
            "previous_state": "WA",
            "previous_zip_code": "98501",
        }
    ]
    updated = await _repair_change_of_location(pg_conn, repair_records, source_id)
    assert updated == 1

    row = (
        await pg_conn.execute(
            select(license_records.c.previous_location_id).where(license_records.c.id == record_id)
        )
    ).one()
    assert row.previous_location_id is not None


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_repair_change_of_location_skips_missing_previous_address(pg_conn):
    """Skips records where previous_business_location is absent."""
    source_id = await get_or_create_source(pg_conn, SOURCE_TYPE_CO_ARCHIVE, url=WSLCB_SOURCE_URL)
    repair_records = [
        {
            "application_type": "CHANGE OF LOCATION",
            "section_type": "new_application",
            "record_date": "2025-06-12",
            "license_number": "RCOL002",
            "previous_business_location": "",
        }
    ]
    updated = await _repair_change_of_location(pg_conn, repair_records, source_id)
    assert updated == 0


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_repair_change_of_location_ignores_non_matching_records(pg_conn):
    """Returns 0 when no DB record matches the repair data."""
    source_id = await get_or_create_source(pg_conn, SOURCE_TYPE_CO_ARCHIVE, url=WSLCB_SOURCE_URL)
    repair_records = [
        {
            "application_type": "CHANGE OF LOCATION",
            "section_type": "new_application",
            "record_date": "2025-06-12",
            "license_number": "DOESNOTEXIST999",
            "previous_business_location": "100 OLD RD, OLYMPIA, WA 98501",
            "previous_city": "OLYMPIA",
            "previous_state": "WA",
            "previous_zip_code": "98501",
        }
    ]
    updated = await _repair_change_of_location(pg_conn, repair_records, source_id)
    assert updated == 0


# ── backfill_from_snapshots ───────────────────────────────────────────


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_from_snapshots_completes_without_error(pg_engine):
    """backfill_from_snapshots runs to completion with the fixture snapshot."""
    with (
        patch(
            "wslcb_licensing_tracker.pg_backfill_snapshots.snapshot_paths",
            return_value=[SNAPSHOT_FIXTURE],
        ),
        patch("wslcb_licensing_tracker.pg_backfill_snapshots.DATA_DIR", FIXTURES_DIR),
    ):
        # Should not raise.
        await backfill_from_snapshots(pg_engine)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_from_snapshots_is_idempotent(pg_engine):
    """Running backfill_from_snapshots twice does not raise or corrupt data."""
    with (
        patch(
            "wslcb_licensing_tracker.pg_backfill_snapshots.snapshot_paths",
            return_value=[SNAPSHOT_FIXTURE],
        ),
        patch("wslcb_licensing_tracker.pg_backfill_snapshots.DATA_DIR", FIXTURES_DIR),
    ):
        await backfill_from_snapshots(pg_engine)
        await backfill_from_snapshots(pg_engine)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_from_snapshots_skips_unparseable_snapshot(pg_engine, tmp_path):
    """A snapshot that raises on parse is skipped; function still completes."""
    bad_snap = tmp_path / "bad_2025_01_01.html"
    bad_snap.write_bytes(b"\xff\xfe not valid html")
    with (
        patch(
            "wslcb_licensing_tracker.pg_backfill_snapshots.snapshot_paths",
            return_value=[bad_snap],
        ),
        patch("wslcb_licensing_tracker.pg_backfill_snapshots.DATA_DIR", tmp_path),
    ):
        # Should not raise even though the snapshot is malformed.
        await backfill_from_snapshots(pg_engine)
