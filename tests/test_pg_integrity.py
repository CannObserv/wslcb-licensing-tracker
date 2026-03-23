"""Tests for pg_integrity.py — async PostgreSQL integrity check functions."""

import os

import pytest
import pytest_asyncio

from wslcb_licensing_tracker.pg_integrity import (
    check_broken_fks,
    check_endorsement_anomalies,
    check_entity_duplicates,
    check_orphaned_locations,
    check_unenriched_records,
    fix_orphaned_locations,
    print_report,
    run_all_checks,
)

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)


@pytest_asyncio.fixture(loop_scope="session")
async def conn(pg_engine):
    """Async DB connection from the shared session-scoped pg_engine."""
    async with pg_engine.connect() as c:
        yield c


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_check_orphaned_locations_returns_dict(conn):
    result = await check_orphaned_locations(conn)
    assert "count" in result
    assert "details" in result
    assert isinstance(result["count"], int)
    assert isinstance(result["details"], list)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_check_unenriched_records_returns_dict(conn):
    result = await check_unenriched_records(conn)
    for key in ("no_endorsements", "no_entities", "no_provenance", "no_enrichment_tracking"):
        assert key in result
        assert isinstance(result[key], int)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_check_endorsement_anomalies_returns_dict(conn):
    result = await check_endorsement_anomalies(conn)
    for key in ("unresolved_codes", "placeholder_endorsements"):
        assert key in result
        assert isinstance(result[key], int)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_run_all_checks_returns_complete_report(conn):
    report = await run_all_checks(conn)
    for key in (
        "orphaned_locations",
        "broken_fks",
        "unenriched",
        "endorsement_anomalies",
        "entity_duplicates",
    ):
        assert key in report


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_check_broken_fks_returns_list(conn):
    result = await check_broken_fks(conn)
    assert isinstance(result, list)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_check_entity_duplicates_returns_list(conn):
    result = await check_entity_duplicates(conn)
    assert isinstance(result, list)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_fix_orphaned_locations_returns_int(conn):
    removed = await fix_orphaned_locations(conn)
    assert isinstance(removed, int)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_fix_orphaned_locations_removes_orphan(conn):
    """An orphaned location (no license_records reference) is deleted when fix=True."""
    from sqlalchemy import text

    # Guard: this test commits data — confirm we're targeting the test database.
    assert "wslcb_test" in os.environ.get("TEST_DATABASE_URL", ""), (
        "Must run against wslcb_test database"
    )
    # SAVEPOINT lets us roll back the INSERT if fix_orphaned_locations raises,
    # preventing a stale orphan from leaking into later tests in this session.
    await conn.execute(text("SAVEPOINT test_fix_orphan"))
    try:
        await conn.execute(
            text(
                "INSERT INTO locations (raw_address, city, state, zip_code) "
                "VALUES ('999 Orphan St', '', 'WA', '')"
            )
        )
        removed = await fix_orphaned_locations(conn)
        assert removed >= 1
    except Exception:
        await conn.execute(text("ROLLBACK TO SAVEPOINT test_fix_orphan"))
        raise
    finally:
        # RELEASE cleans up the savepoint marker whether the block succeeded or
        # was rolled back (after ROLLBACK TO SAVEPOINT the marker still exists).
        await conn.execute(text("RELEASE SAVEPOINT test_fix_orphan"))
    await conn.commit()


def test_print_report_returns_zero_for_clean_report():
    report = {
        "orphaned_locations": {"count": 0},
        "broken_fks": {"count": 0, "details": []},
        "unenriched": {
            "no_endorsements": 0,
            "no_entities": 0,
            "no_provenance": 0,
            "no_enrichment_tracking": 0,
        },
        "endorsement_anomalies": {"unresolved_codes": 0, "placeholder_endorsements": 0},
        "entity_duplicates": {"count": 0, "details": []},
    }
    assert print_report(report) == 0


def test_print_report_returns_nonzero_for_issues():
    report = {
        "orphaned_locations": {"count": 3},
        "broken_fks": {"count": 0, "details": []},
        "unenriched": {
            "no_endorsements": 2,
            "no_entities": 0,
            "no_provenance": 0,
            "no_enrichment_tracking": 0,
        },
        "endorsement_anomalies": {"unresolved_codes": 0, "placeholder_endorsements": 0},
        "entity_duplicates": {"count": 0, "details": []},
    }
    assert print_report(report) == 5  # 3 + 2
