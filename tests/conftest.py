"""Shared pytest fixtures for the WSLCB licensing tracker test suite.

Provides sample record dicts representing the main record variants, and
path helpers for HTML fixtures.  All fixtures are designed for speed —
no network calls, no disk I/O for the database.

PostgreSQL tests use the pg_engine / pg_conn fixtures defined below,
which require TEST_DATABASE_URL to be set in the environment.
"""
import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ── CI enforcement ───────────────────────────────────────────────────


def pytest_sessionstart(session: pytest.Session) -> None:
    """Abort the session immediately if REQUIRE_PG_TESTS=1 but TEST_DATABASE_URL is unset.

    Catches all PG test skip paths (fixture-based, @pytest.mark.skipif, standalone
    connection fixtures) before any collection or test execution occurs.
    """
    if os.environ.get("REQUIRE_PG_TESTS") and not os.environ.get("TEST_DATABASE_URL"):
        pytest.exit(
            "TEST_DATABASE_URL is not set but REQUIRE_PG_TESTS=1 — "
            "PostgreSQL integration tests are required in this environment",
            returncode=1,
        )


# ── Sample record dicts ──────────────────────────────────────────────
# These mirror the dict shape produced by parser.parse_records_from_table.


@pytest.fixture
def standard_new_application():
    """A plain NEW APPLICATION record (new_application section)."""
    return {
        "section_type": "new_application",
        "record_date": "2025-06-15",
        "business_name": "ACME CANNABIS CO",
        "business_location": "123 MAIN ST, SEATTLE, WA 98101",
        "applicants": "ACME CANNABIS CO; JOHN DOE; JANE SMITH",
        "license_type": "CANNABIS RETAILER",
        "application_type": "NEW APPLICATION",
        "license_number": "TST001",
        "contact_phone": "(206) 555-0100",
        "city": "SEATTLE",
        "state": "WA",
        "zip_code": "98101",
        "previous_business_name": "",
        "previous_applicants": "",
        "previous_business_location": "",
        "previous_city": "",
        "previous_state": "",
        "previous_zip_code": "",
        "scraped_at": datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC),
    }


@pytest.fixture
def assumption_record():
    """An ASSUMPTION record with buyer/seller fields."""
    return {
        "section_type": "new_application",
        "record_date": "2025-06-10",
        "business_name": "NEW LEAF DISPENSARY",
        "business_location": "789 PINE ST, TACOMA, WA 98402",
        "applicants": "NEW LEAF DISPENSARY; CAROL NEWBY",
        "license_type": "CANNABIS RETAILER",
        "application_type": "ASSUMPTION",
        "license_number": "415678",
        "contact_phone": "(253) 555-0300",
        "city": "TACOMA",
        "state": "WA",
        "zip_code": "98402",
        "previous_business_name": "OLD SMOKE SHOP",
        "previous_applicants": "OLD SMOKE SHOP; ALICE OLDEN",
        "previous_business_location": "",
        "previous_city": "",
        "previous_state": "",
        "previous_zip_code": "",
        "scraped_at": datetime(2025, 6, 10, 12, 0, 0, tzinfo=UTC),
    }


@pytest.fixture
def change_of_location_record():
    """A CHANGE OF LOCATION record with previous/new addresses."""
    return {
        "section_type": "new_application",
        "record_date": "2025-06-12",
        "business_name": "MOVING SPIRITS LLC",
        "business_location": "200 NEW BLVD, OLYMPIA, WA 98502",
        "applicants": "MOVING SPIRITS LLC; DAN MOVER",
        "license_type": "SPIRITS RETAILER",
        "application_type": "CHANGE OF LOCATION",
        "license_number": "080123",
        "contact_phone": "(360) 555-0400",
        "city": "OLYMPIA",
        "state": "WA",
        "zip_code": "98502",
        "previous_business_name": "",
        "previous_applicants": "",
        "previous_business_location": "100 OLD RD, OLYMPIA, WA 98501",
        "previous_city": "OLYMPIA",
        "previous_state": "WA",
        "previous_zip_code": "98501",
        "scraped_at": datetime(2025, 6, 12, 12, 0, 0, tzinfo=UTC),
    }


@pytest.fixture
def approved_numeric_code():
    """An approved-section record with a bare numeric license type code."""
    return {
        "section_type": "approved",
        "record_date": "2025-06-11",
        "business_name": "GREEN GARDENS LLC",
        "business_location": "500 GARDEN WAY, BELLINGHAM, WA 98225",
        "applicants": "",
        "license_type": "349,",
        "application_type": "NEW APPLICATION",
        "license_number": "420001",
        "contact_phone": "(360) 555-0500",
        "city": "BELLINGHAM",
        "state": "WA",
        "zip_code": "98225",
        "previous_business_name": "",
        "previous_applicants": "",
        "previous_business_location": "",
        "previous_city": "",
        "previous_state": "",
        "previous_zip_code": "",
        "scraped_at": datetime(2025, 6, 11, 12, 0, 0, tzinfo=UTC),
    }


@pytest.fixture
def discontinued_code_name():
    """A discontinued-section record with CODE, NAME license type format."""
    return {
        "section_type": "discontinued",
        "record_date": "2025-06-09",
        "business_name": "CLOSED SHOP INC.",
        "business_location": "999 END RD, VANCOUVER, WA 98660",
        "applicants": "",
        "license_type": "450, GROCERY STORE - BEER/WINE",
        "application_type": "DISCONTINUED",
        "license_number": "300100",
        "contact_phone": "(360) 555-0600",
        "city": "VANCOUVER",
        "state": "WA",
        "zip_code": "98660",
        "previous_business_name": "",
        "previous_applicants": "",
        "previous_business_location": "",
        "previous_city": "",
        "previous_state": "",
        "previous_zip_code": "",
        "scraped_at": datetime(2025, 6, 9, 12, 0, 0, tzinfo=UTC),
    }


@pytest.fixture(scope="session")
def pg_url() -> str | None:
    """PostgreSQL connection URL from TEST_DATABASE_URL env var."""
    return os.environ.get("TEST_DATABASE_URL")


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pg_engine(pg_url) -> AsyncGenerator[AsyncEngine, None]:
    """Session-scoped async engine with Alembic migrations applied.

    Skips PG tests when TEST_DATABASE_URL is not set. In CI, set
    REQUIRE_PG_TESTS=1 to turn missing TEST_DATABASE_URL into a hard
    failure (enforced by pytest_sessionstart before this fixture runs).
    """
    if not pg_url:
        pytest.skip("TEST_DATABASE_URL not set — skipping PostgreSQL tests")

    from alembic import command
    from alembic.config import Config

    from wslcb_licensing_tracker.database import create_engine_from_env

    original = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = pg_url
    engine = create_engine_from_env()

    # Run Alembic migrations
    def _run_upgrade(connection):
        cfg = Config("alembic.ini")
        cfg.attributes["connection"] = connection
        command.upgrade(cfg, "head")

    # Tables to wipe before each test session, in FK-safe order.
    # source_types is reference data seeded by migration 0001 — do NOT truncate it.
    _TRUNCATE_TABLES = [
        "record_links", "record_enrichments", "record_sources", "record_endorsements",
        "record_entities", "scrape_log", "sources", "license_records", "locations",
    ]

    try:
        async with engine.connect() as conn:
            await conn.run_sync(_run_upgrade)
            await conn.commit()

        # Truncate all test-writable tables for a clean slate each session.
        # Prevents stale committed data from previous runs causing false failures.
        async with engine.connect() as conn:
            for table in _TRUNCATE_TABLES:
                await conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            # Reseed source_types reference data — the migration INSERT has no ON CONFLICT
            # guard, so if the table was previously wiped it won't be repopulated by
            # alembic upgrade (which skips already-applied migrations).
            await conn.execute(text("""
                INSERT INTO source_types (id, slug, label, description) VALUES
                    (1, 'live_scrape',      'Live Scrape',       'Direct scrape of the WSLCB licensing page'),
                    (2, 'co_archive',       'CO Page Archive',   'Cannabis Observer archived HTML snapshots'),
                    (3, 'internet_archive', 'Internet Archive',  'Wayback Machine snapshots'),
                    (4, 'co_diff_archive',  'CO Diff Archive',   'Cannabis Observer diff-detected change snapshots'),
                    (5, 'manual',           'Manual Entry',      'Manually entered or corrected records')
                ON CONFLICT (id) DO NOTHING
            """))
            await conn.commit()

        yield engine
    finally:
        if original is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = original
        await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def pg_conn(pg_engine: AsyncEngine) -> AsyncGenerator[AsyncConnection, None]:
    """AsyncConnection in a rolled-back transaction for test isolation.

    Each test gets a clean transaction that is rolled back after the test,
    so tests never see each other's data. Suitable for helpers and
    insert_record tests that don't commit internally.

    For ingest_batch (which commits), use pg_engine directly.
    """
    async with pg_engine.connect() as conn:
        trans = await conn.begin()
        yield conn
        await trans.rollback()
