"""Async PostgreSQL address validation DB layer for the WSLCB licensing tracker.

DB-facing orchestration over the transport client in address_client.py
(#141): per-location writes, record-FK helpers, and batch backfill/refresh.

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
from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .address_client import (
    CONFIRMED_STATUSES,
    get_api_key,
    is_validation_enabled,
    standardize,
    validate,
)
from .models import license_records, locations

logger = logging.getLogger(__name__)

ISO_ALPHA2_LEN = 2

# Renewal TTL for validated addresses (#150). A location whose
# address_validation_attempted_at is older than this is re-validated by the
# backfill (post-scrape, twice daily, and the weekly timer) so upstream
# validator/USPS improvements are picked up. Scheduling keys on attempted_at,
# not validated_at, so a row is re-checked at most once per TTL whether the
# check confirms or not — and a failed re-check never degrades the prior
# confirmation (see validate_location / process_location).
VALIDATION_TTL_DAYS = 180

# Upper bound on /validate calls per UTC day across all automatic backfill runs
# (both twice-daily post-scrape hooks and the weekly timer share it). Kept well
# under the upstream USPS 10K/day cap so we never 429 into the Google fallback
# (160/day). attempted_at is stamped on every call, so counting rows with
# attempted_at >= start-of-day is an exact same-day call count. #150.
DAILY_VALIDATION_LIMIT = 5000


def _sanitize_country(raw: str) -> str:
    """Return raw if it looks like an ISO 3166-1 alpha-2 code, else empty string."""
    return raw if (len(raw) == ISO_ALPHA2_LEN and raw.isalpha() and raw.isascii()) else ""


async def standardize_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Standardize and update a single location row via POST /api/v2/standardize.

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
    """Optionally validate a location row via POST /api/v2/validate.

    Gated by ENABLE_ADDRESS_VALIDATION env var. No-op (returns False) when
    the flag is off.

    On confirmed/corrected response: overlays std_* columns, validation_status,
    dpv_match_code, latitude, longitude, and sets both address_validated_at and
    address_validation_attempted_at.
    On not_confirmed/unavailable: writes validation_status, dpv_match_code, and
    address_validation_attempted_at only — std_* and address_validated_at are
    left intact so a failed re-check never degrades a prior confirmation (#150).

    Does NOT commit — the caller is responsible for committing.

    Args:
        conn: Async SQLAlchemy connection.
        location_id: The ID of the location row to update.
        raw_address: The raw business address to validate.
        client: Optional httpx.AsyncClient for connection reuse.

    Returns:
        True if address_validated_at was set (confirmed/corrected), False otherwise.
    """
    if not is_validation_enabled():
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

    # Gate on validation status: v2 returns address_line_1="" (not None) for unconfirmed.
    has_address = status in CONFIRMED_STATUSES

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
                    address_validation_attempted_at=datetime.now(UTC),
                )
            )
            return True

        # not_confirmed or unavailable — record the attempt and status, but leave
        # std_* and address_validated_at intact (non-destructive; #150).
        await conn.execute(
            update(locations)
            .where(locations.c.id == location_id)
            .values(
                validation_status=status,
                dpv_match_code=dpv,
                address_validation_attempted_at=datetime.now(UTC),
            )
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
    superset of ``/standardize``. address_validation_attempted_at is always
    stamped on the /validate call; a confirmed result also overlays std_* and
    address_validated_at, while a not_confirmed/unavailable result leaves those
    intact (non-destructive re-check; #150).

    When validation is off, calls ``/standardize`` only (no attempted_at).

    Does NOT commit — the caller is responsible for committing.

    Returns True if the location was successfully processed, False otherwise.
    """
    if not raw_address or not raw_address.strip():
        return False

    if is_validation_enabled():
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
        # Gate on validation status: v2 returns address_line_1="" (not None) for unconfirmed.
        has_address = status in CONFIRMED_STATUSES

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
                        address_validation_attempted_at=datetime.now(UTC),
                    )
                )
                return True

            # not_confirmed or unavailable — record the attempt and status, but leave
            # std_* and address_validated_at intact (non-destructive; #150).
            await conn.execute(
                update(locations)
                .where(locations.c.id == location_id)
                .values(
                    validation_status=status,
                    dpv_match_code=dpv,
                    address_validation_attempted_at=datetime.now(UTC),
                )
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
    if already_std and (not is_validation_enabled() or already_val):
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
    rate_limit: float = 1.0,
    daily_limit: int = DAILY_VALIDATION_LIMIT,
) -> int:
    """Standardize (and optionally validate) locations that need processing.

    Mode-aware selection (#150):

    * Validation enabled — selects rows never attempted
      (address_validation_attempted_at IS NULL) or whose last attempt has aged
      past VALIDATION_TTL_DAYS, oldest first, capped at the remaining daily
      budget. Scheduling keys on attempted_at (not std/validated_at) so a row is
      re-checked at most once per TTL and a not_confirmed re-check does not churn.
    * Validation disabled — selects rows never standardized
      (address_standardized_at IS NULL); attempted_at is never written in this
      mode so it cannot be the scheduling key.

    The daily ceiling (validation-enabled path only) bounds /validate calls per
    UTC day across all automatic runs to stay within upstream limits. attempted_at
    is stamped on every call, so counting rows attempted since start-of-day is an
    exact same-day call count shared with any manual refresh run.

    Returns:
        Number of locations successfully standardized.
    """
    if not get_api_key():
        logger.error("No API key configured for address validation")
        return 0

    base = select(locations.c.id, locations.c.raw_address).where(
        locations.c.raw_address.isnot(None), locations.c.raw_address != ""
    )

    if not is_validation_enabled():
        # Standardize-only: attempted_at is never set here, so key on std_at.
        stmt = base.where(locations.c.address_standardized_at.is_(None))
    else:
        now = datetime.now(UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        used_today = (
            await conn.execute(
                select(func.count())
                .select_from(locations)
                .where(locations.c.address_validation_attempted_at >= day_start)
            )
        ).scalar_one()
        budget = max(0, daily_limit - used_today)
        if budget == 0:
            logger.info(
                "Daily validation limit reached (%d used of %d); skipping backfill",
                used_today,
                daily_limit,
            )
            return 0

        ttl_cutoff = now - timedelta(days=VALIDATION_TTL_DAYS)
        stmt = (
            base.where(
                (locations.c.address_validation_attempted_at.is_(None))
                | (locations.c.address_validation_attempted_at < ttl_cutoff)
            )
            .order_by(locations.c.address_validation_attempted_at.asc().nulls_first())
            .limit(budget)
        )

    rows = (await conn.execute(stmt)).mappings().all()

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
    if not get_api_key():
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

    if not get_api_key():
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
