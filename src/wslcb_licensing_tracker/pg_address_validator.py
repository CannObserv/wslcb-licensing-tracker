"""Async PostgreSQL address validation DB layer for the WSLCB licensing tracker.

Ports all DB-facing functions from address_validator.py to async SQLAlchemy Core.
HTTP functions (standardize, validate) use httpx.AsyncClient natively — no thread
wrappers needed.

Pipeline
--------
1. **Preferred**: :func:`process_location` picks the best single endpoint based
   on config — ``/validate`` when ENABLE_ADDRESS_VALIDATION is on (covers both
   standardization and validation in one call), ``/standardize`` when off.
2. **Direct**: :func:`standardize_location` and :func:`validate_location` call
   their respective endpoints — retained for callers that need a specific path.

Caller-commits convention: no ``await conn.commit()`` inside single-row helpers.
:func:`_validate_batch` manages its own transaction lifecycle (savepoints +
periodic commits) because it is a long-running bulk operation.
"""

import asyncio
import logging
import os
from datetime import UTC, datetime

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import license_records, locations

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pure HTTP helpers (no DB dependency)
# ---------------------------------------------------------------------------

BASE_URL = "https://address-validator.exe.xyz:8000"
TIMEOUT = 15.0
HTTP_OK = 200
HTTP_TOO_MANY_REQUESTS = 429
HTTP_INTERNAL_SERVER_ERROR = 500
DEFAULT_RETRY_AFTER = 2.0
MAX_RETRIES = 3
ISO_ALPHA2_LEN = 2

# Shared connection pool for all address validation HTTP calls.
# httpx.AsyncClient binds to the event loop lazily (on first request), so
# module-level construction is safe before any event loop exists.
_shared_client: httpx.AsyncClient = httpx.AsyncClient(timeout=TIMEOUT)


async def close_shared_client() -> None:
    """Close the module-level httpx client, releasing TLS sessions cleanly."""
    await _shared_client.aclose()


def _is_validation_enabled() -> bool:
    """Return True if ENABLE_ADDRESS_VALIDATION is set to a truthy value.

    Checks the environment variable at call time (not cached) so that tests
    and runtime overrides take effect immediately.
    """
    return os.environ.get("ENABLE_ADDRESS_VALIDATION", "").lower() in ("1", "true", "yes")


def _parse_retry_after(response: httpx.Response) -> float:
    """Extract Retry-After seconds from a response, falling back to DEFAULT_RETRY_AFTER."""
    raw = response.headers.get("Retry-After", "")
    try:
        return max(float(raw), 0.5)
    except (ValueError, TypeError):
        return DEFAULT_RETRY_AFTER


async def _post_with_retry(
    url: str,
    payload: dict,
    headers: dict,
    client: httpx.AsyncClient,
    label: str,
) -> httpx.Response | None:
    """POST with retry on HTTP 429 (service rate limit) and 500 (proxy throttle).

    Retries up to MAX_RETRIES times.  On 429, reads Retry-After header and
    sleeps that duration (doubling on each subsequent retry).  On 500, falls
    back to DEFAULT_RETRY_AFTER with the same exponential backoff.  Returns the
    final successful Response, or None if all retries exhausted or a
    non-retryable error occurs.
    """
    backoff_multiplier = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.post(url, json=payload, headers=headers)
        except httpx.TimeoutException:
            logger.warning("Timeout calling %s API (attempt %d/%d)", label, attempt, MAX_RETRIES)
            return None
        except httpx.HTTPError as e:
            logger.warning("HTTP error calling %s API: %s", label, e)
            return None
        except Exception as e:  # noqa: BLE001
            logger.warning("Unexpected error calling %s API: %s", label, e)
            return None

        if response.status_code in (HTTP_TOO_MANY_REQUESTS, HTTP_INTERNAL_SERVER_ERROR):
            wait = _parse_retry_after(response) * backoff_multiplier
            if response.status_code == HTTP_TOO_MANY_REQUESTS:
                logger.warning(
                    "%s API returned 429 (rate limited by service, attempt %d/%d),"
                    " retrying in %.1fs",
                    label,
                    attempt,
                    MAX_RETRIES,
                    wait,
                )
            else:
                logger.warning(
                    "%s API returned 500 (proxy throttle, attempt %d/%d), retrying in %.1fs",
                    label,
                    attempt,
                    MAX_RETRIES,
                    wait,
                )
            await asyncio.sleep(wait)
            backoff_multiplier *= 2.0
            continue

        return response

    logger.warning("%s API: exhausted %d retries on 429/500", label, MAX_RETRIES)
    return None


async def standardize(address: str, client: httpx.AsyncClient | None = None) -> dict | None:
    """Standardize an address via POST /api/v1/standardize.

    Sends the full raw address string.  The server parses and standardizes
    the address according to USPS Publication 28 rules.

    Retries on HTTP 429 (service rate limit) and 500 (proxy throttle) with
    exponential backoff (up to MAX_RETRIES).
    Returns a dict on success, or None on any failure.
    """
    api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    if not api_key:
        return None

    url = f"{BASE_URL}/api/v1/standardize"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    _client = client if client is not None else _shared_client
    response = await _post_with_retry(url, payload, headers, _client, "address standardize")
    if response is None:
        return None

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
    return data


async def validate(address: str, client: httpx.AsyncClient | None = None) -> dict | None:
    """Validate an address via POST /api/v1/validate.

    Sends the full raw address string. The server runs parse → standardize
    internally before calling the USPS DPV provider.

    Retries on HTTP 429 (service rate limit) and 500 (proxy throttle) with
    exponential backoff (up to MAX_RETRIES).
    Returns a dict on success, or None on any failure.
    A 200 response with validation.status='not_confirmed' or 'unavailable'
    is returned as a dict (not None) — the caller decides how to handle it.
    """
    api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    if not api_key:
        return None

    url = f"{BASE_URL}/api/v1/validate"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    _client = client if client is not None else _shared_client
    response = await _post_with_retry(url, payload, headers, _client, "address validation")
    if response is None:
        return None

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
    return data


def _sanitize_country(raw: str) -> str:
    """Return raw if it looks like an ISO 3166-1 alpha-2 code, else empty string."""
    return raw if (len(raw) == ISO_ALPHA2_LEN and raw.isalpha() and raw.isascii()) else ""


async def standardize_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Standardize and update a single location row via POST /api/v1/standardize.

    Always runs regardless of the ENABLE_ADDRESS_VALIDATION flag.

    On success writes std_address_line_1/2, std_city, std_region,
    std_postal_code, std_country, std_address_string, validation_status
    (set to "standardized"), and address_standardized_at.

    Does NOT commit — the caller is responsible for committing.
    Returns False if raw_address is empty/None or the API call fails.

    Args:
        conn: Async SQLAlchemy connection.
        location_id: The ID of the location row to update.
        raw_address: The raw business address to standardize.
        client: Optional httpx.AsyncClient for connection reuse.

    Returns:
        True if address_standardized_at was set, False otherwise.
    """
    if not raw_address or not raw_address.strip():
        return False

    try:
        result = await standardize(raw_address, client)
    except Exception:
        logger.exception("Standardize failed for location %d", location_id)
        return False

    if result is None:
        return False

    try:
        await conn.execute(
            update(locations)
            .where(locations.c.id == location_id)
            .values(
                std_address_line_1=result.get("address_line_1", ""),
                std_address_line_2=result.get("address_line_2", ""),
                std_city=result.get("city", ""),
                std_region=result.get("region", ""),
                std_postal_code=result.get("postal_code", ""),
                std_country=_sanitize_country(result.get("country", "")),
                std_address_string=result.get("standardized"),
                validation_status="standardized",
                address_standardized_at=datetime.now(UTC),
            )
        )
    except Exception:
        logger.exception("Failed to update location %d", location_id)
        return False

    return True


async def validate_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Optionally validate a location row via POST /api/v1/validate.

    Gated by ENABLE_ADDRESS_VALIDATION env var. No-op (returns False) when
    the flag is off.

    On confirmed/corrected response: overlays std_* columns, validation_status,
    dpv_match_code, latitude, longitude, and sets address_validated_at.
    On not_confirmed/unavailable: writes validation_status and dpv_match_code
    only, leaves address_validated_at NULL for retry.

    Does NOT commit — the caller is responsible for committing.

    Args:
        conn: Async SQLAlchemy connection.
        location_id: The ID of the location row to update.
        raw_address: The raw business address to validate.
        client: Optional httpx.AsyncClient for connection reuse.

    Returns:
        True if address_validated_at was set (confirmed/corrected), False otherwise.
    """
    if not _is_validation_enabled():
        return False

    if not raw_address or not raw_address.strip():
        return False

    try:
        result = await validate(raw_address, client)
    except Exception:
        logger.exception("Validate failed for location %d", location_id)
        return False

    if result is None:
        return False

    validation = result.get("validation") or {}
    status = validation.get("status", "")
    dpv = validation.get("dpv_match_code")

    has_address = result.get("address_line_1") is not None

    try:
        if has_address:
            await conn.execute(
                update(locations)
                .where(locations.c.id == location_id)
                .values(
                    std_address_line_1=result.get("address_line_1", ""),
                    std_address_line_2=result.get("address_line_2", ""),
                    std_city=result.get("city", ""),
                    std_region=result.get("region", ""),
                    std_postal_code=result.get("postal_code", ""),
                    std_country=_sanitize_country(result.get("country", "")),
                    std_address_string=result.get("validated"),
                    validation_status=status,
                    dpv_match_code=dpv,
                    latitude=result.get("latitude"),
                    longitude=result.get("longitude"),
                    address_standardized_at=datetime.now(UTC),
                    address_validated_at=datetime.now(UTC),
                )
            )
            return True

        # not_confirmed or unavailable — store status, leave address_validated_at NULL
        await conn.execute(
            update(locations)
            .where(locations.c.id == location_id)
            .values(validation_status=status, dpv_match_code=dpv)
        )
    except Exception:
        logger.exception("Failed to update location %d during validate", location_id)

    return False


async def process_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Smart dispatcher: standardize and/or validate a location in one API call.

    When ENABLE_ADDRESS_VALIDATION is on, calls ``/validate`` which returns a
    superset of ``/standardize`` — writing all std_* columns, validation fields,
    and both timestamps in a single round-trip.

    When validation is off, calls ``/standardize`` only.

    Does NOT commit — the caller is responsible for committing.

    Returns True if the location was successfully processed, False otherwise.
    """
    if not raw_address or not raw_address.strip():
        return False

    if _is_validation_enabled():
        # Single /validate call covers both standardization and validation.
        try:
            result = await validate(raw_address, client)
        except Exception:
            logger.exception("Validate failed for location %d", location_id)
            return False

        if result is None:
            return False

        validation = result.get("validation") or {}
        status = validation.get("status", "")
        dpv = validation.get("dpv_match_code")
        has_address = result.get("address_line_1") is not None

        try:
            if has_address:
                await conn.execute(
                    update(locations)
                    .where(locations.c.id == location_id)
                    .values(
                        std_address_line_1=result.get("address_line_1", ""),
                        std_address_line_2=result.get("address_line_2", ""),
                        std_city=result.get("city", ""),
                        std_region=result.get("region", ""),
                        std_postal_code=result.get("postal_code", ""),
                        std_country=_sanitize_country(result.get("country", "")),
                        std_address_string=result.get("validated"),
                        validation_status=status,
                        dpv_match_code=dpv,
                        latitude=result.get("latitude"),
                        longitude=result.get("longitude"),
                        address_standardized_at=datetime.now(UTC),
                        address_validated_at=datetime.now(UTC),
                    )
                )
                return True

            # not_confirmed or unavailable — store status only
            await conn.execute(
                update(locations)
                .where(locations.c.id == location_id)
                .values(validation_status=status, dpv_match_code=dpv)
            )
        except Exception:
            logger.exception("Failed to update location %d during process", location_id)

        return False

    # Validation disabled — standardize only.
    return await standardize_location(conn, location_id, raw_address, client)


async def _validate_record_location(
    conn: AsyncConnection,
    record_id: int,
    fk_column: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Standardize (and optionally validate) a location FK on a license record.

    Looks up *fk_column* ('location_id' or 'previous_location_id') on the
    record and processes the referenced location row.

    Skips if the location is already fully processed for the current config.

    Returns True if the location was already processed or standardization succeeded.
    """
    col = getattr(license_records.c, fk_column)
    row = (
        await conn.execute(select(col).where(license_records.c.id == record_id))
    ).scalar_one_or_none()
    if not row:
        return False

    loc_row = (
        (
            await conn.execute(
                select(
                    locations.c.id,
                    locations.c.raw_address,
                    locations.c.address_standardized_at,
                    locations.c.address_validated_at,
                ).where(locations.c.id == row)
            )
        )
        .mappings()
        .one_or_none()
    )
    if not loc_row:
        return False

    already_std = bool(loc_row["address_standardized_at"])
    already_val = bool(loc_row["address_validated_at"])
    if already_std and (not _is_validation_enabled() or already_val):
        return True

    return await process_location(conn, loc_row["id"], loc_row["raw_address"], client=client)


async def validate_record(
    conn: AsyncConnection,
    record_id: int,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Standardize (and optionally validate) the primary location for a license record."""
    return await _validate_record_location(conn, record_id, "location_id", client)


async def validate_previous_location(
    conn: AsyncConnection,
    record_id: int,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Standardize (and optionally validate) the previous location for a CHANGE OF LOCATION record."""  # noqa: E501
    return await _validate_record_location(conn, record_id, "previous_location_id", client)


async def _validate_batch(
    conn: AsyncConnection,
    rows: list,
    label: str,
    batch_size: int = 100,
    rate_limit: float = 0.5,
) -> int:
    """Standardize (and optionally validate) a list of location rows.

    Each row must have 'id' and 'raw_address' keys (mappings).

    Uses :func:`process_location` for a single API call per row.
    Wraps each row in a savepoint so a single DB failure does not poison the
    batch.  Commits every *batch_size* rows to flush progress incrementally.

    Returns:
        Number of locations successfully processed.
    """
    total = len(rows)
    if total == 0:
        logger.info("No locations to %s", label.lower())
        return 0

    logger.info("%s for %d locations", label, total)
    succeeded = 0
    errors = 0

    for attempted, row in enumerate(rows, start=1):
        location_id = row["id"]
        address = row["raw_address"]

        try:
            async with conn.begin_nested():
                ok = await process_location(conn, location_id, address)
            if ok:
                succeeded += 1
        except Exception as exc:  # noqa: BLE001 — intentionally broad; savepoint isolates damage
            logger.warning("Savepoint rollback for location %d", location_id, exc_info=True)
            errors += 1
            # If the outer transaction entered an aborted state (e.g. InFailedSQLTransactionError),
            # begin_nested() itself will fail on every subsequent row.  Rollback to recover a clean
            # transaction before continuing; break if the rollback also fails.
            orig = getattr(exc, "orig", exc.__cause__)
            if orig is not None and "InFailedSQLTransaction" in str(orig):
                logger.warning("Outer transaction aborted; rolling back to recover")
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception("Rollback failed; aborting batch")
                    break

        if attempted % batch_size == 0:
            await conn.commit()
            logger.info("Progress: %d/%d (%d ok, %d err)", attempted, total, succeeded, errors)

        if rate_limit:
            await asyncio.sleep(rate_limit)

    # Final commit for any remaining rows after the last batch_size boundary.
    await conn.commit()
    logger.info("Done: %d/%d succeeded (%d failed)", succeeded, total, total - succeeded)
    return succeeded


async def backfill_addresses(
    conn: AsyncConnection,
    batch_size: int = 100,
    rate_limit: float = 0.5,
) -> int:
    """Standardize (and optionally validate) locations that need processing.

    Queries all locations where address_standardized_at IS NULL or
    address_validated_at IS NULL.

    Returns:
        Number of locations successfully standardized.
    """
    api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    rows = (
        (
            await conn.execute(
                select(locations.c.id, locations.c.raw_address)
                .where(
                    (locations.c.address_standardized_at.is_(None))
                    | (locations.c.address_validated_at.is_(None))
                )
                .where(locations.c.raw_address.isnot(None))
                .where(locations.c.raw_address != "")
            )
        )
        .mappings()
        .all()
    )

    return await _validate_batch(
        conn,
        rows,
        "Backfilling addresses",
        batch_size=batch_size,
        rate_limit=rate_limit,
    )


async def refresh_addresses(
    conn: AsyncConnection,
    batch_size: int = 100,
    rate_limit: float = 0.5,
) -> int:
    """Re-standardize (and optionally re-validate) all locations.

    Useful when the upstream address-validator service has been updated.

    Returns:
        Number of locations successfully standardized.
    """
    api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    rows = (
        (
            await conn.execute(
                select(locations.c.id, locations.c.raw_address)
                .where(locations.c.raw_address.isnot(None))
                .where(locations.c.raw_address != "")
            )
        )
        .mappings()
        .all()
    )

    return await _validate_batch(
        conn,
        rows,
        "Refreshing addresses",
        batch_size=batch_size,
        rate_limit=rate_limit,
    )


async def refresh_specific_addresses(
    conn: AsyncConnection,
    location_ids: list[int],
    batch_size: int = 100,
    rate_limit: float = 0.5,
) -> int:
    """Re-standardize (and optionally re-validate) a specific set of locations by ID.

    Args:
        conn: Async SQLAlchemy connection.
        location_ids: List of locations.id values to re-process.
        batch_size: How often to log progress (default 100).
        rate_limit: Seconds to sleep between API calls (default 0.5).

    Returns:
        Number of locations successfully standardized.
    """
    if not location_ids:
        logger.info("No location IDs provided — nothing to refresh")
        return 0

    api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
    if not api_key:
        logger.error("No API key configured for address validation")
        return 0

    rows = (
        (
            await conn.execute(
                select(locations.c.id, locations.c.raw_address)
                .where(locations.c.id.in_(location_ids))
                .where(locations.c.raw_address.isnot(None))
                .where(locations.c.raw_address != "")
            )
        )
        .mappings()
        .all()
    )

    return await _validate_batch(
        conn,
        rows,
        "Refreshing specific addresses",
        batch_size=batch_size,
        rate_limit=rate_limit,
    )
