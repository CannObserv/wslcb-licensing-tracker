"""Client module for the address validation API service.

Provides functions to standardize and validate business addresses against
the address-validator API, backfill address data for un-validated records,
and refresh all addresses when the upstream service is updated.

Configuration:
    API key is loaded from ./env file (ADDRESS_VALIDATOR_API_KEY=...)
    with fallback to the ADDRESS_VALIDATOR_API_KEY environment variable.
"""

import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import httpx

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
        pass  # Fall through to environment variable
    except OSError as e:
        print(f"WARNING: Error reading ./env file: {e}", file=sys.stderr)

    # Fallback to environment variable
    _cached_api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    return _cached_api_key


def standardize(address: str, client: httpx.Client | None = None) -> dict | None:
    """Standardize an address via the address validation API.

    Calls POST /api/standardize with the given address string.

    Args:
        address: The raw address string to standardize.
        client: Optional httpx.Client to reuse for connection pooling.
            If None, a one-shot request is made.

    Returns:
        A dict with keys (address_line_1, address_line_2, city, state,
        zip_code, standardized, components) on success, or None on any
        failure (network error, non-200 status, timeout).

    Note:
        When *client* is provided its timeout setting takes precedence
        over this module's TIMEOUT constant.  One-shot requests (no
        client) always use TIMEOUT.
    """
    api_key = _load_api_key()
    if not api_key:
        return None

    url = f"{BASE_URL}/api/standardize"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    try:
        if client is not None:
            response = client.post(url, json=payload, headers=headers)
        else:
            response = httpx.post(url, json=payload, headers=headers, timeout=TIMEOUT)

        if response.status_code != 200:
            print(
                f"WARNING: Address validation API returned status {response.status_code}"
                f" for: {address}",
                file=sys.stderr,
            )
            return None

        return response.json()

    except httpx.TimeoutException:
        print(f"WARNING: Timeout calling address validation API for: {address}", file=sys.stderr)
        return None
    except httpx.HTTPError as e:
        print(f"WARNING: HTTP error calling address validation API: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"WARNING: Unexpected error calling address validation API: {e}", file=sys.stderr)
        return None


def validate_record(
    conn: sqlite3.Connection,
    record_id: int,
    business_location: str,
    client: httpx.Client | None = None,
) -> bool:
    """Validate and update a single record's address in the database.

    Calls standardize() on the business_location, then UPDATEs the record's
    address columns (address_line_1, address_line_2, std_city, std_state,
    std_zip, address_validated_at).

    Does NOT commit — the caller is responsible for committing.
    Skips (returns False) if business_location is empty or None.

    Args:
        conn: SQLite database connection.
        record_id: The ID of the record to update.
        business_location: The raw business address to validate.
        client: Optional httpx.Client for connection reuse.

    Returns:
        True if the record was successfully updated, False otherwise.
    """
    if not business_location or not business_location.strip():
        return False

    result = standardize(business_location, client=client)
    if result is None:
        return False

    try:
        conn.execute(
            """UPDATE license_records SET
                address_line_1 = ?, address_line_2 = ?,
                std_city = ?, std_state = ?, std_zip = ?,
                address_validated_at = ?
            WHERE id = ?""",
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
        print(f"WARNING: Failed to update record {record_id}: {e}", file=sys.stderr)
        return False


def _validate_batch(
    conn: sqlite3.Connection,
    rows: list,
    label: str,
    batch_size: int = 100,
) -> int:
    """Validate a list of (id, business_location) rows against the API.

    Commits and prints progress every *batch_size* records.
    Sleeps 0.05 s between API requests to be polite.

    Args:
        conn: SQLite database connection.
        rows: Sequence of (record_id, business_location) tuples/rows.
        label: Human-readable verb for log messages (e.g. "Backfilling", "Refreshing").
        batch_size: Records per commit/progress log.

    Returns:
        Number of records successfully validated.
    """
    total = len(rows)
    if total == 0:
        print(f"No records to {label.lower()}")
        return 0

    print(f"{label} addresses for {total} records")
    succeeded = 0
    attempted = 0

    with httpx.Client(timeout=TIMEOUT) as client:
        for row in rows:
            record_id, business_location = row[0], row[1]
            ok = validate_record(conn, record_id, business_location, client=client)
            attempted += 1
            if ok:
                succeeded += 1

            if attempted % batch_size == 0:
                conn.commit()
                print(f"Progress: {attempted}/{total} ({succeeded} succeeded)")

            time.sleep(0.05)

    conn.commit()
    print(f"Done: {succeeded}/{total} succeeded ({total - succeeded} failed)")
    return succeeded


def backfill_addresses(conn: sqlite3.Connection, batch_size: int = 100) -> int:
    """Backfill standardized addresses for all un-validated records.

    Queries all records where address_validated_at IS NULL and
    business_location is non-empty, then validates each one.
    Commits and prints progress every batch_size records.

    Args:
        conn: SQLite database connection.
        batch_size: Number of records to process before each commit.

    Returns:
        Number of records successfully validated.
    """
    api_key = _load_api_key()
    if not api_key:
        print("ERROR: No API key configured for address validation", file=sys.stderr)
        return 0

    rows = conn.execute(
        """SELECT id, business_location FROM license_records
        WHERE address_validated_at IS NULL
          AND business_location IS NOT NULL
          AND business_location != ''"""
    ).fetchall()

    return _validate_batch(conn, rows, "Backfilling", batch_size)


def refresh_addresses(conn: sqlite3.Connection, batch_size: int = 100) -> int:
    """Re-validate all addresses, regardless of current validation status.

    Unlike backfill_addresses() which only processes un-validated records,
    this function re-validates every record with a non-empty business_location.
    Useful when the upstream address-validator service has been updated and
    standardized values may have changed.

    Safe to interrupt — each record's address_validated_at timestamp is
    updated individually on success, so partial runs leave the database
    in a consistent state.

    Args:
        conn: SQLite database connection.
        batch_size: Number of records to process before each commit/progress log.

    Returns:
        Number of records successfully validated.
    """
    api_key = _load_api_key()
    if not api_key:
        print("ERROR: No API key configured for address validation", file=sys.stderr)
        return 0

    rows = conn.execute(
        """SELECT id, business_location FROM license_records
        WHERE business_location IS NOT NULL AND business_location != ''"""
    ).fetchall()

    return _validate_batch(conn, rows, "Refreshing", batch_size)
