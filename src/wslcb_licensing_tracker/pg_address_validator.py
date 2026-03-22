"""Async PostgreSQL address validation DB layer for the WSLCB licensing tracker.

Ports all DB-facing functions from address_validator.py to async SQLAlchemy Core.
HTTP functions (standardize, validate) use httpx.AsyncClient natively — no thread
wrappers needed.

Pipeline
--------
1. **Always**: :func:`standardize_location` calls ``POST /api/v1/standardize``
   and writes ``std_*`` columns + ``address_standardized_at``.
2. **Optional**: :func:`validate_location` calls ``POST /api/v1/validate``
   (gated by ENABLE_ADDRESS_VALIDATION env var) and overlays DPV fields +
   ``address_validated_at``.

Caller-commits convention: no ``await conn.commit()`` inside these functions.
"""

import asyncio
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import license_records, locations

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pure HTTP helpers (no DB dependency)
# ---------------------------------------------------------------------------

BASE_URL = "https://address-validator.exe.xyz:8000"
TIMEOUT = 5.0
HTTP_OK = 200
ISO_ALPHA2_LEN = 2

_cached_api_key: str | None = None

# Candidate env file paths, checked in order:
# 1. /etc/wslcb-licensing-tracker/env  — production (outside repo, root-owned)
# 2. <project-root>/env                — local dev fallback
# Exposed as a module-level list so tests can monkeypatch it.
_env_candidates: list[Path] = [
    Path("/etc/wslcb-licensing-tracker/env"),
    Path(__file__).resolve().parent.parent.parent / "env",
]


def _load_api_key() -> str:
    """Load the API key from the ./env file or environment variable.

    Reads from the ./env file first (looking for ADDRESS_VALIDATOR_API_KEY=...),
    falls back to os.environ, and returns an empty string if neither is found.
    The result is cached in a module-level variable after the first call.
    """
    global _cached_api_key  # noqa: PLW0603  # module-level cache is the intended pattern
    if _cached_api_key is not None:
        return _cached_api_key

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


async def standardize(address: str, client: httpx.AsyncClient | None = None) -> dict | None:
    """Standardize an address via POST /api/v1/standardize.

    Sends the full raw address string.  The server parses and standardizes
    the address according to USPS Publication 28 rules.

    Returns a dict on success, or None on any failure (network error,
    non-200 status, timeout).
    """
    api_key = _load_api_key()
    if not api_key:
        return None

    url = f"{BASE_URL}/api/v1/standardize"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    try:
        if client is not None:
            response = await client.post(url, json=payload, headers=headers, timeout=TIMEOUT)
        else:
            async with httpx.AsyncClient() as c:
                response = await c.post(url, json=payload, headers=headers, timeout=TIMEOUT)

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
    except Exception as e:  # noqa: BLE001
        logger.warning("Unexpected error calling address standardize API: %s", e)
        return None
    else:
        return data


async def validate(address: str, client: httpx.AsyncClient | None = None) -> dict | None:
    """Validate an address via POST /api/v1/validate.

    Sends the full raw address string. The server runs parse → standardize
    internally before calling the USPS DPV provider.

    Returns a dict on success, or None on any failure.
    A 200 response with validation.status='not_confirmed' or 'unavailable'
    is returned as a dict (not None) — the caller decides how to handle it.
    """
    api_key = _load_api_key()
    if not api_key:
        return None

    url = f"{BASE_URL}/api/v1/validate"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    try:
        if client is not None:
            response = await client.post(url, json=payload, headers=headers, timeout=TIMEOUT)
        else:
            async with httpx.AsyncClient() as c:
                response = await c.post(url, json=payload, headers=headers, timeout=TIMEOUT)

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
    except Exception as e:  # noqa: BLE001
        logger.warning("Unexpected error calling address validation API: %s", e)
        return None
    else:
        return data


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
                std_address_line_1=result.get("std_address_line_1", ""),
                std_address_line_2=result.get("std_address_line_2", ""),
                std_city=result.get("std_city", ""),
                std_region=result.get("std_region", ""),
                std_postal_code=result.get("std_postal_code", ""),
                std_country=result.get("std_country", ""),
                std_address_string=result.get("std_address_string"),
                validation_status=result.get("validation_status", "standardized"),
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
            await conn.execute(
                update(locations)
                .where(locations.c.id == location_id)
                .values(
                    std_address_line_1=result.get("address_line_1", ""),
                    std_address_line_2=result.get("address_line_2", ""),
                    std_city=result.get("city", ""),
                    std_region=result.get("region", ""),
                    std_postal_code=result.get("postal_code", ""),
                    std_country=std_country,
                    std_address_string=result.get("validated"),
                    validation_status=status,
                    dpv_match_code=dpv,
                    latitude=result.get("latitude"),
                    longitude=result.get("longitude"),
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

    ok = await standardize_location(conn, loc_row["id"], loc_row["raw_address"], client=client)
    await validate_location(conn, loc_row["id"], loc_row["raw_address"], client=client)
    return ok


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
    rate_limit: float = 0.1,
) -> int:
    """Standardize (and optionally validate) a list of location rows.

    Each row must have 'id' and 'raw_address' keys (mappings).

    Logs progress every *batch_size* records.
    Sleeps *rate_limit* seconds between API requests.
    Does NOT commit — the caller is responsible.

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

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for row in rows:
            location_id = row["id"]
            address = row["raw_address"]
            ok = await standardize_location(conn, location_id, address, client=client)
            await validate_location(conn, location_id, address, client=client)
            attempted += 1
            if ok:
                succeeded += 1

            if attempted % batch_size == 0:
                logger.debug("Progress: %d/%d (%d succeeded)", attempted, total, succeeded)

            if rate_limit:
                await asyncio.sleep(rate_limit)

    logger.info("Done: %d/%d succeeded (%d failed)", succeeded, total, total - succeeded)
    return succeeded


async def backfill_addresses(
    conn: AsyncConnection,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> int:
    """Standardize (and optionally validate) locations that need processing.

    Queries all locations where address_standardized_at IS NULL or
    address_validated_at IS NULL.

    Returns:
        Number of locations successfully standardized.
    """
    api_key = _load_api_key()
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
    rate_limit: float = 0.1,
) -> int:
    """Re-standardize (and optionally re-validate) all locations.

    Useful when the upstream address-validator service has been updated.

    Returns:
        Number of locations successfully standardized.
    """
    api_key = _load_api_key()
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
    rate_limit: float = 0.1,
) -> int:
    """Re-standardize (and optionally re-validate) a specific set of locations by ID.

    Args:
        conn: Async SQLAlchemy connection.
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
