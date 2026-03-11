"""Client module for the address validation API service.

Provides functions to validate business addresses against the address-validator
API. Operates on the `locations` table — each unique raw address is validated
once and shared across all license records that reference it.

Configuration:
    API key is loaded from the project-root ./env file (ADDRESS_VALIDATOR_API_KEY=...)
    with fallback to the ADDRESS_VALIDATOR_API_KEY environment variable.
"""

import logging
import os
import sqlite3
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

    # Try reading from the project-root ./env file.
    # __file__ is src/wslcb_licensing_tracker/address_validator.py, so walk up
    # three levels (module dir → src/ → project root) to locate ./env.
    _module_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(os.path.dirname(os.path.dirname(_module_dir)), "env")
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
        pass  # Fall through to environment variable
    except OSError as e:
        logger.warning("Error reading ./env file: %s", e)

    # Fallback to environment variable
    _cached_api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    return _cached_api_key


def validate(address: str, client: httpx.Client | None = None) -> dict | None:
    """Validate an address via POST /api/v1/validate.

    Sends the full raw address string. The server runs parse → standardize
    internally before calling the USPS DPV provider.

    Args:
        address: The raw address string to validate.
        client: Optional httpx.Client to reuse for connection pooling.
            If None, a one-shot request is made.

    Returns:
        A dict with keys (address_line_1, address_line_2, city, region,
        postal_code, country, validated, components, validation, latitude,
        longitude) on success, or None on any failure (network error,
        non-200 status, timeout). A 200 response with
        validation.status='not_confirmed' or 'unavailable' is returned
        as a dict (not None) — the caller decides how to handle it.
    """
    api_key = _load_api_key()
    if not api_key:
        return None

    url = f"{BASE_URL}/api/v1/validate"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    try:
        if client is not None:
            response = client.post(url, json=payload, headers=headers)
        else:
            response = httpx.post(url, json=payload, headers=headers, timeout=TIMEOUT)

        if response.status_code != 200:
            logger.warning(
                "Address validation API returned status %d for: %s",
                response.status_code, address,
            )
            return None

        data = response.json()
        for warn in data.get("warnings") or []:
            logger.warning("Address API warning for %r: %s", address, warn)
        return data

    except httpx.TimeoutException:
        logger.warning("Timeout calling address validation API for: %s", address)
        return None
    except httpx.HTTPError as e:
        logger.warning("HTTP error calling address validation API: %s", e)
        return None
    except Exception as e:
        logger.warning("Unexpected error calling address validation API: %s", e)
        return None


def validate_location(
    conn: sqlite3.Connection,
    location_id: int,
    raw_address: str,
    client: httpx.Client | None = None,
) -> bool:
    """Validate and update a single location row via POST /api/v1/validate.

    On a confirmed or corrected response (address_line_1 is non-null):
    writes all std_address_line_1/2, std_city, std_region, std_postal_code,
    std_country, validated_address, validation_status, dpv_match_code,
    latitude, longitude, and sets address_validated_at.

    On not_confirmed or unavailable (address_line_1 is null): writes
    validation_status and dpv_match_code only, leaves address_validated_at
    NULL so backfill-addresses will retry.

    Does NOT commit — the caller is responsible for committing.
    Skips (returns False) if raw_address is empty or None.

    Args:
        conn: SQLite database connection.
        location_id: The ID of the location row to update.
        raw_address: The raw business address to validate.
        client: Optional httpx.Client for connection reuse.

    Returns:
        True if address_validated_at was set (confirmed/corrected), False otherwise.
    """
    if not raw_address or not raw_address.strip():
        return False

    result = validate(raw_address, client=client)
    if result is None:
        return False

    validation = result.get("validation") or {}
    status = validation.get("status", "")
    dpv = validation.get("dpv_match_code")

    # address_line_1 is None when the provider could not confirm the address
    has_address = result.get("address_line_1") is not None

    try:
        if has_address:
            raw_country = result.get("country", "")
            # Only store country if it is a valid ISO 3166-1 alpha-2 code (2 ASCII letters).
            # .isascii() guards against Unicode letters that pass .isalpha() (e.g. 'ÜS').
            std_country = (
                raw_country
                if (len(raw_country) == 2 and raw_country.isalpha() and raw_country.isascii())
                else ""
            )
            conn.execute(
                """UPDATE locations SET
                    std_address_line_1 = ?, std_address_line_2 = ?,
                    std_city = ?, std_region = ?, std_postal_code = ?, std_country = ?,
                    validated_address = ?, validation_status = ?, dpv_match_code = ?,
                    latitude = ?, longitude = ?,
                    address_validated_at = ?
                WHERE id = ?""",
                (
                    result.get("address_line_1", ""),
                    result.get("address_line_2", ""),
                    result.get("city", ""),
                    result.get("region", ""),
                    result.get("postal_code", ""),
                    std_country,
                    result.get("validated"),
                    status,
                    dpv,
                    result.get("latitude"),
                    result.get("longitude"),
                    datetime.now(timezone.utc).isoformat(),
                    location_id,
                ),
            )
            return True
        else:
            # not_confirmed or unavailable: store status so we know validation was
            # attempted, but leave address_validated_at NULL for backfill retry.
            conn.execute(
                "UPDATE locations SET validation_status = ?, dpv_match_code = ? WHERE id = ?",
                (status, dpv, location_id),
            )
            return False
    except Exception as e:
        logger.warning("Failed to update location %d: %s", location_id, e)
        return False


def _validate_record_location(
    conn: sqlite3.Connection,
    record_id: int,
    fk_column: str,
    client: httpx.Client | None = None,
) -> bool:
    """Validate a location FK on a license record.

    Looks up *fk_column* (e.g. 'location_id' or 'previous_location_id')
    on the record and validates the referenced location row.
    Returns True if already validated; False if the FK is NULL or
    validation fails.
    """
    row = conn.execute(
        f"SELECT {fk_column} FROM license_records WHERE id = ?", (record_id,)
    ).fetchone()
    if not row or not row[0]:
        return False
    loc = conn.execute(
        "SELECT id, raw_address, address_validated_at FROM locations WHERE id = ?",
        (row[0],),
    ).fetchone()
    if not loc:
        return False
    if loc["address_validated_at"]:
        return True  # Already validated
    return validate_location(conn, loc["id"], loc["raw_address"], client=client)


def validate_record(
    conn: sqlite3.Connection,
    record_id: int,
    client: httpx.Client | None = None,
) -> bool:
    """Validate the primary location for a license record."""
    return _validate_record_location(conn, record_id, "location_id", client)


def validate_previous_location(
    conn: sqlite3.Connection,
    record_id: int,
    client: httpx.Client | None = None,
) -> bool:
    """Validate the previous location for a CHANGE OF LOCATION record."""
    return _validate_record_location(conn, record_id, "previous_location_id", client)


def _validate_batch(
    conn: sqlite3.Connection,
    rows: list,
    label: str,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> int:
    """Validate a list of (location_id, raw_address) rows against the API.

    Commits and logs progress every *batch_size* records.
    Sleeps *rate_limit* seconds between API requests to be polite.

    Returns:
        Number of locations successfully validated.
    """
    total = len(rows)
    if total == 0:
        logger.info("No locations to %s", label.lower())
        return 0

    logger.info("%s for %d locations", label, total)
    succeeded = 0
    attempted = 0

    with httpx.Client(timeout=TIMEOUT) as client:
        for row in rows:
            location_id, address = row[0], row[1]
            ok = validate_location(conn, location_id, address, client=client)
            attempted += 1
            if ok:
                succeeded += 1

            if attempted % batch_size == 0:
                conn.commit()
                logger.debug("Progress: %d/%d (%d succeeded)", attempted, total, succeeded)

            time.sleep(rate_limit)

    conn.commit()
    logger.info("Done: %d/%d succeeded (%d failed)", succeeded, total, total - succeeded)
    return succeeded


def backfill_addresses(conn: sqlite3.Connection, batch_size: int = 100, rate_limit: float = 0.1) -> int:
    """Backfill validated addresses for all un-validated locations.

    Queries all locations where address_validated_at IS NULL and
    raw_address is non-empty, then validates each one.

    Returns:
        Number of locations successfully validated.
    """
    api_key = _load_api_key()
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    rows = conn.execute(
        """SELECT id, raw_address FROM locations
        WHERE address_validated_at IS NULL
          AND raw_address IS NOT NULL
          AND raw_address != ''"""
    ).fetchall()

    return _validate_batch(conn, rows, "Backfilling addresses", batch_size=batch_size, rate_limit=rate_limit)


def refresh_addresses(conn: sqlite3.Connection, batch_size: int = 100, rate_limit: float = 0.1) -> int:
    """Re-validate all locations, regardless of current validation status.

    Useful when the upstream address-validator service has been updated and
    validated values may have changed.

    Safe to interrupt — each location's address_validated_at timestamp is
    updated individually on success.

    Returns:
        Number of locations successfully validated.
    """
    api_key = _load_api_key()
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    rows = conn.execute(
        """SELECT id, raw_address FROM locations
        WHERE raw_address IS NOT NULL AND raw_address != ''"""
    ).fetchall()

    return _validate_batch(conn, rows, "Refreshing addresses", batch_size=batch_size, rate_limit=rate_limit)
