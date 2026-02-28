"""Shared pytest fixtures for the WSLCB licensing tracker test suite.

Provides an in-memory SQLite database with full schema, sample record
dicts representing the main record variants, and path helpers for HTML
fixtures.  All fixtures are designed for speed — no network calls, no
disk I/O for the database.
"""
import sys
from pathlib import Path

import pytest

# Ensure the project root is on sys.path so tests can import modules
# directly (flat layout, no package).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def db():
    """In-memory SQLite database with full schema initialized.

    The connection is returned directly (not via context manager) and
    closed after the test.  WAL mode is silently downgraded to
    ``memory`` journal by SQLite — this is expected and harmless.
    """
    from database import get_connection, init_db

    conn = get_connection(":memory:")
    init_db(conn)
    yield conn
    conn.close()


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
        "license_number": "078001",
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
        "scraped_at": "2025-06-15T12:00:00+00:00",
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
        "scraped_at": "2025-06-10T12:00:00+00:00",
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
        "scraped_at": "2025-06-12T12:00:00+00:00",
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
        "scraped_at": "2025-06-11T12:00:00+00:00",
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
        "scraped_at": "2025-06-09T12:00:00+00:00",
    }
