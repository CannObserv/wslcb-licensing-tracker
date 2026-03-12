"""Client module for the address validation API service.

Provides functions to standardize and optionally validate business addresses
against the address-validator API.  Operates on the ``locations`` table —
each unique raw address is processed once and shared across all license
records that reference it.

Pipeline
--------
1. **Always**: :func:`standardize_location` calls ``POST /api/v1/standardize``
   and writes ``std_*`` columns + ``address_standardized_at``.
2. **Optional**: :func:`validate_location` calls ``POST /api/v1/validate``
   (which runs standardize internally) and overlays DPV validation fields +
   ``address_validated_at``.  Gated by the ``ENABLE_ADDRESS_VALIDATION``
   environment variable (``"1"``, ``"true"``, or ``"yes"``).

Configuration
-------------
API key is loaded from the project-root ``./env`` file
(``ADDRESS_VALIDATOR_API_KEY=...``) with fallback to the
``ADDRESS_VALIDATOR_API_KEY`` environment variable.
"""

import logging
import os
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://address-validator.exe.xyz:8000"
TIMEOUT = 5.0
HTTP_OK = 200
ISO_ALPHA2_LEN = 2

_cached_api_key: str | None = None


def _load_api_key() -> str:
    """Load the API key from the ./env file or environment variable.

    Reads from the ./env file first (looking for ADDRESS_VALIDATOR_API_KEY=...),
    falls back to os.environ, and returns an empty string if neither is found.
    The result is cached in a module-level variable after the first call.
    """
    global _cached_api_key  # noqa: PLW0603  # module-level cache is the intended pattern
    if _cached_api_key is not None:
        return _cached_api_key

    # Candidate env file paths, checked in order:
    # 1. /etc/wslcb-licensing-tracker/env  — production (outside repo, root-owned)
    # 2. <project-root>/env                — local dev fallback
    _module_dir = Path(__file__).resolve().parent
    _project_root = _module_dir.parent.parent
    _env_candidates = [
        Path("/etc/wslcb-licensing-tracker/env"),
        _project_root / "env",
    ]
    for env_path in _env_candidates:
        try:
            with env_path.open() as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if line.startswith("#") or not line:
                        continue
                    if line.startswith("ADDRESS_VALIDATOR_API_KEY="):
                        _cached_api_key = line.split("=", 1)[1].strip()
                        return _cached_api_key
        except FileNotFoundError:
            continue
        except OSError as e:
            logger.warning("Error reading env file %s: %s", env_path, e)

    # Fallback to environment variable
    _cached_api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    return _cached_api_key


def _is_validation_enabled() -> bool:
    """Return True if ENABLE_ADDRESS_VALIDATION is set to a truthy value.

    Checks the environment variable at call time (not cached) so that tests
    and runtime overrides take effect immediately.
    """
    return os.environ.get("ENABLE_ADDRESS_VALIDATION", "").lower() in ("1", "true", "yes")


def standardize(address: str, client: httpx.Client | None = None) -> dict | None:
    """Standardize an address via POST /api/v1/standardize.

    Sends the full raw address string.  The server parses and standardizes
    the address according to USPS Publication 28 rules.

    Args:
        address: The raw address string to standardize.
        client: Optional httpx.Client to reuse for connection pooling.
            If None, a one-shot request is made.

    Returns:
        A dict with keys (address_line_1, address_line_2, city, region,
        postal_code, country, standardized, components, warnings) on success,
        or None on any failure (network error, non-200 status, timeout).
    """
    api_key = _load_api_key()
    if not api_key:
        return None

    url = f"{BASE_URL}/api/v1/standardize"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    try:
        if client is not None:
            response = client.post(url, json=payload, headers=headers)
        else:
            response = httpx.post(url, json=payload, headers=headers, timeout=TIMEOUT)

        if response.status_code != HTTP_OK:
            logger.warning(
                "Address standardize API returned status %d for: %s",
                response.status_code,
                address,
            )
            return None

        data = response.json()
        for warn in data.get("warnings") or []:
            logger.warning("Address API warning for %r: %s", address, warn)
    except httpx.TimeoutException:
        logger.warning("Timeout calling address standardize API for: %s", address)
        return None
    except httpx.HTTPError as e:
        logger.warning("HTTP error calling address standardize API: %s", e)
        return None
    except Exception as e:  # noqa: BLE001  # catch-all for unexpected API client errors
        logger.warning("Unexpected error calling address standardize API: %s", e)
        return None
    else:
        return data


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

        if response.status_code != HTTP_OK:
            logger.warning(
                "Address validation API returned status %d for: %s",
                response.status_code,
                address,
            )
            return None

        data = response.json()
        for warn in data.get("warnings") or []:
            logger.warning("Address API warning for %r: %s", address, warn)
    except httpx.TimeoutException:
        logger.warning("Timeout calling address validation API for: %s", address)
        return None
    except httpx.HTTPError as e:
        logger.warning("HTTP error calling address validation API: %s", e)
        return None
    except Exception as e:  # noqa: BLE001  # catch-all for unexpected API client errors
        logger.warning("Unexpected error calling address validation API: %s", e)
        return None
    else:
        return data


def standardize_location(
    conn: sqlite3.Connection,
    location_id: int,
    raw_address: str,
    client: httpx.Client | None = None,
) -> bool:
    """Standardize and update a single location row via POST /api/v1/standardize.

    Always runs regardless of the ENABLE_ADDRESS_VALIDATION flag.

    On success, writes all std_address_line_1/2, std_city, std_region,
    std_postal_code, std_country, std_address_string, validation_status
    (set to "standardized"), and sets address_standardized_at.
    dpv_match_code, latitude, and longitude are left NULL — those are
    populated only by validate_location() when validation is enabled.

    Does NOT commit — the caller is responsible for committing.
    Skips (returns False) if raw_address is empty or None.

    Args:
        conn: SQLite database connection.
        location_id: The ID of the location row to update.
        raw_address: The raw business address to standardize.
        client: Optional httpx.Client for connection reuse.

    Returns:
        True if address_standardized_at was set, False otherwise.
    """
    if not raw_address or not raw_address.strip():
        return False

    result = standardize(raw_address, client=client)
    if result is None:
        return False

    raw_country = result.get("country", "")
    # Only store country if it is a valid ISO 3166-1 alpha-2 code (2 ASCII letters).
    std_country = (
        raw_country
        if (len(raw_country) == ISO_ALPHA2_LEN and raw_country.isalpha() and raw_country.isascii())
        else ""
    )

    try:
        conn.execute(
            """UPDATE locations SET
                std_address_line_1 = ?, std_address_line_2 = ?,
                std_city = ?, std_region = ?, std_postal_code = ?, std_country = ?,
                std_address_string = ?, validation_status = ?,
                address_standardized_at = ?
            WHERE id = ?""",
            (
                result.get("address_line_1", ""),
                result.get("address_line_2", ""),
                result.get("city", ""),
                result.get("region", ""),
                result.get("postal_code", ""),
                std_country,
                result.get("standardized"),
                "standardized",
                datetime.now(UTC).isoformat(),
                location_id,
            ),
        )
    except Exception as e:  # noqa: BLE001  # catch-all to avoid crashing the batch loop
        logger.warning("Failed to update location %d: %s", location_id, e)
        return False
    else:
        return True


def validate_location(
    conn: sqlite3.Connection,
    location_id: int,
    raw_address: str,
    client: httpx.Client | None = None,
) -> bool:
    """Optionally validate a location row via POST /api/v1/validate.

    Gated by the ENABLE_ADDRESS_VALIDATION environment variable.  When the
    flag is off this function is a no-op and returns False immediately without
    making any API call or DB write.

    The /validate endpoint runs standardize internally, so this function does
    NOT call standardize_location() — that is the caller's responsibility.

    On a confirmed or corrected response (address_line_1 is non-null):
    overlays std_address_string (from validated), std_address_line_1/2,
    std_city, std_region, std_postal_code, std_country, validation_status,
    dpv_match_code, latitude, longitude, and sets address_validated_at.

    On not_confirmed or unavailable (address_line_1 is null): writes
    validation_status and dpv_match_code only, leaves address_validated_at
    NULL so backfill-addresses will retry.

    Does NOT commit — the caller is responsible for committing.
    Skips (returns False) if raw_address is empty, None, or flag is off.

    Args:
        conn: SQLite database connection.
        location_id: The ID of the location row to update.
        raw_address: The raw business address to validate.
        client: Optional httpx.Client for connection reuse.

    Returns:
        True if address_validated_at was set (confirmed/corrected), False otherwise.
    """
    if not _is_validation_enabled():
        return False

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
            std_country = (
                raw_country
                if (
                    len(raw_country) == ISO_ALPHA2_LEN
                    and raw_country.isalpha()
                    and raw_country.isascii()
                )
                else ""
            )
            conn.execute(
                """UPDATE locations SET
                    std_address_line_1 = ?, std_address_line_2 = ?,
                    std_city = ?, std_region = ?, std_postal_code = ?, std_country = ?,
                    std_address_string = ?, validation_status = ?, dpv_match_code = ?,
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
                    datetime.now(UTC).isoformat(),
                    location_id,
                ),
            )
            return True
        # not_confirmed or unavailable: store status so we know validation was
        # attempted, but leave address_validated_at NULL for backfill retry.
        conn.execute(
            "UPDATE locations SET validation_status = ?, dpv_match_code = ? WHERE id = ?",
            (status, dpv, location_id),
        )
    except Exception as e:  # noqa: BLE001  # catch-all to avoid crashing the batch loop
        logger.warning("Failed to update location %d: %s", location_id, e)
        return False
    else:
        return False


def _validate_record_location(
    conn: sqlite3.Connection,
    record_id: int,
    fk_column: str,
    client: httpx.Client | None = None,
) -> bool:
    """Standardize (and optionally validate) a location FK on a license record.

    Looks up *fk_column* (e.g. 'location_id' or 'previous_location_id')
    on the record and processes the referenced location row.

    Skips if the location is already fully processed for the current config:
    - address_standardized_at is set AND (validation disabled OR address_validated_at is set).

    Returns True if the location was already processed or standardization succeeded.
    """
    row = conn.execute(
        f"SELECT {fk_column} FROM license_records WHERE id = ?",
        (record_id,),
    ).fetchone()
    if not row or not row[0]:
        return False
    loc = conn.execute(
        "SELECT id, raw_address, address_standardized_at, address_validated_at"
        " FROM locations WHERE id = ?",
        (row[0],),
    ).fetchone()
    if not loc:
        return False

    already_std = bool(loc["address_standardized_at"])
    already_val = bool(loc["address_validated_at"])
    if already_std and (not _is_validation_enabled() or already_val):
        return True  # Nothing more to do

    ok = standardize_location(conn, loc["id"], loc["raw_address"], client=client)
    validate_location(conn, loc["id"], loc["raw_address"], client=client)
    return ok


def validate_record(
    conn: sqlite3.Connection,
    record_id: int,
    client: httpx.Client | None = None,
) -> bool:
    """Standardize (and optionally validate) the primary location for a license record."""
    return _validate_record_location(conn, record_id, "location_id", client)


def validate_previous_location(
    conn: sqlite3.Connection,
    record_id: int,
    client: httpx.Client | None = None,
) -> bool:
    """Standardize (and optionally validate) the previous location for a CHANGE OF LOCATION record."""  # noqa: E501
    return _validate_record_location(conn, record_id, "previous_location_id", client)


def _validate_batch(
    conn: sqlite3.Connection,
    rows: list,
    label: str,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> int:
    """Standardize (and optionally validate) a list of (location_id, raw_address) rows.

    Calls standardize_location() for every row, then validate_location() (which
    self-gates on the ENABLE_ADDRESS_VALIDATION flag).  Commits after every
    record so the SQLite write lock is held for milliseconds per update rather
    than for the full batch window.

    Logs progress every *batch_size* records.
    Sleeps *rate_limit* seconds between API requests to be polite.

    Returns:
        Number of locations successfully standardized.
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
            ok = standardize_location(conn, location_id, address, client=client)
            validate_location(conn, location_id, address, client=client)
            conn.commit()
            attempted += 1
            if ok:
                succeeded += 1

            if attempted % batch_size == 0:
                logger.debug("Progress: %d/%d (%d succeeded)", attempted, total, succeeded)

            time.sleep(rate_limit)

    logger.info("Done: %d/%d succeeded (%d failed)", succeeded, total, total - succeeded)
    return succeeded


def backfill_addresses(
    conn: sqlite3.Connection,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> int:
    """Standardize (and optionally validate) locations that need processing.

    Queries all locations where:
    - address_standardized_at IS NULL (never standardized), OR
    - address_validated_at IS NULL (standardized but not yet validated, picked
      up when ENABLE_ADDRESS_VALIDATION is turned on).

    Locations with both timestamps set are skipped.

    Returns:
        Number of locations successfully standardized.
    """
    api_key = _load_api_key()
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    rows = conn.execute(
        """SELECT id, raw_address FROM locations
        WHERE (address_standardized_at IS NULL OR address_validated_at IS NULL)
          AND raw_address IS NOT NULL
          AND raw_address != ''"""
    ).fetchall()

    return _validate_batch(
        conn,
        rows,
        "Backfilling addresses",
        batch_size=batch_size,
        rate_limit=rate_limit,
    )


def refresh_addresses(
    conn: sqlite3.Connection,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> int:
    """Re-standardize (and optionally re-validate) all locations.

    Useful when the upstream address-validator service has been updated and
    standardized values may have changed.

    Safe to interrupt — each location's address_standardized_at timestamp is
    updated individually on success.

    Returns:
        Number of locations successfully standardized.
    """
    api_key = _load_api_key()
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    rows = conn.execute(
        """SELECT id, raw_address FROM locations
        WHERE raw_address IS NOT NULL AND raw_address != ''"""
    ).fetchall()

    return _validate_batch(
        conn,
        rows,
        "Refreshing addresses",
        batch_size=batch_size,
        rate_limit=rate_limit,
    )


def refresh_specific_addresses(
    conn: sqlite3.Connection,
    location_ids: list[int],
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> int:
    """Re-standardize (and optionally re-validate) a specific set of locations by ID.

    Intended for targeted re-runs after lock-contention failures: extract the
    failed IDs from the journal, pass them here to refresh only those rows
    without re-processing the full location set.

    Safe to interrupt — each location is committed individually.

    Args:
        conn: Database connection.
        location_ids: List of locations.id values to re-process.
        batch_size: How often to log progress (default 100).
        rate_limit: Seconds to sleep between API calls (default 0.1).

    Returns:
        Number of locations successfully standardized.
    """
    if not location_ids:
        logger.info("No location IDs provided — nothing to refresh")
        return 0

    api_key = _load_api_key()
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    placeholders = ",".join("?" * len(location_ids))
    rows = conn.execute(
        f"SELECT id, raw_address FROM locations WHERE id IN ({placeholders})"
        " AND raw_address IS NOT NULL AND raw_address != ''",
        location_ids,
    ).fetchall()

    return _validate_batch(
        conn,
        rows,
        "Refreshing specific addresses",
        batch_size=batch_size,
        rate_limit=rate_limit,
    )
