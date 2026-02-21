"""Client module for the address validation API service.

Provides functions to standardize and validate business addresses against
the address-validator API, and to backfill address data for existing
database records.

Configuration:
    API key is loaded from ./env file (ADDRESS_VALIDATOR_API_KEY=...)
    with fallback to the ADDRESS_VALIDATOR_API_KEY environment variable.
"""

import logging
import os
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://address-validator.exe.xyz:8000"
TIMEOUT = 5.0

_cached_api_key: str | None = None


def _load_api_key() -> str:
    """Load the API key from the ./env file or environment variable.

    Reads from the ./env file first (looking for ADDRESS_VALIDATOR_API_KEY=...),
    falls back to os.environ, and returns an empty string if neither is found.
    The result is cached in a module-level variable after the first call.
    """
    global _cached_api_key
    if _cached_api_key is not None:
        return _cached_api_key

    # Try reading from ./env file
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "env")
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or not line:
                    continue
                if line.startswith("ADDRESS_VALIDATOR_API_KEY="):
                    _cached_api_key = line.split("=", 1)[1].strip()
                    return _cached_api_key
    except FileNotFoundError:
        logger.debug("No ./env file found, falling back to environment variable")
    except OSError as e:
        logger.warning("Error reading ./env file: %s", e)

    # Fallback to environment variable
    _cached_api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    return _cached_api_key


def standardize(address: str) -> dict | None:
    """Standardize an address via the address validation API.

    Calls POST /api/standardize with the given address string.

    Args:
        address: The raw address string to standardize.

    Returns:
        A dict with keys (address_line_1, address_line_2, city, state,
        zip_code, standardized, components) on success, or None on any
        failure (network error, non-200 status, timeout).
    """
    api_key = _load_api_key()
    if not api_key:
        logger.warning("No API key configured for address validation")
        return None

    url = f"{BASE_URL}/api/standardize"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    try:
        with httpx.Client(timeout=TIMEOUT) as client:
            response = client.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            logger.warning(
                "Address validation API returned status %d for address: %s",
                response.status_code,
                address,
            )
            return None

        return response.json()

    except httpx.TimeoutException:
        logger.warning("Timeout calling address validation API for: %s", address)
        return None
    except httpx.HTTPError as e:
        logger.warning("HTTP error calling address validation API: %s", e)
        return None
    except Exception as e:
        logger.warning("Unexpected error calling address validation API: %s", e)
        return None


def validate_record(conn, record_id: int, business_location: str) -> bool:
    """Validate and update a single record's address in the database.

    Calls standardize() on the business_location, then UPDATEs the record's
    address columns (address_line_1, address_line_2, std_city, std_state,
    std_zip, address_validated_at).

    Does NOT commit — the caller is responsible for committing.
    Skips (returns False) if business_location is empty or None.

    Args:
        conn: A database connection with cursor() support.
        record_id: The ID of the record to update.
        business_location: The raw business address to validate.

    Returns:
        True if the record was successfully updated, False otherwise.
    """
    if not business_location or not business_location.strip():
        return False

    result = standardize(business_location)
    if result is None:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE license_records SET
                address_line_1 = ?,
                address_line_2 = ?,
                std_city = ?,
                std_state = ?,
                std_zip = ?,
                address_validated_at = ?
            WHERE id = ?
            """,
            (
                result.get("address_line_1", ""),
                result.get("address_line_2", ""),
                result.get("city", ""),
                result.get("state", ""),
                result.get("zip_code", ""),
                datetime.now(timezone.utc).isoformat(),
                record_id,
            ),
        )
        return True
    except Exception as e:
        logger.warning("Failed to update record %d: %s", record_id, e)
        return False


def backfill_addresses(conn, batch_size: int = 100) -> int:
    """Backfill standardized addresses for all un-validated records.

    Queries all records where address_validated_at IS NULL and
    business_location is non-empty, then calls validate_record for each.
    Commits every batch_size records and prints progress every 100 records.
    Sleeps 0.05s between API requests to be polite.

    Args:
        conn: A database connection with cursor() and commit() support.
        batch_size: Number of records to process before each commit.

    Returns:
        Total number of records processed (attempted validation).
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, business_location FROM license_records
        WHERE address_validated_at IS NULL
          AND business_location IS NOT NULL
          AND business_location != ''
        """
    )
    rows = cursor.fetchall()

    total = len(rows)
    if total == 0:
        logger.info("No records to backfill")
        return 0

    logger.info("Backfilling addresses for %d records", total)
    processed = 0

    for row in rows:
        record_id, business_location = row[0], row[1]
        validate_record(conn, record_id, business_location)
        processed += 1

        if processed % batch_size == 0:
            conn.commit()

        if processed % 100 == 0:
            print(f"Progress: {processed}/{total} records processed")

        time.sleep(0.05)

    # Final commit for any remaining records
    conn.commit()
    print(f"Done: {processed}/{total} records processed")
    logger.info("Backfill complete: %d records processed", processed)

    return processed
