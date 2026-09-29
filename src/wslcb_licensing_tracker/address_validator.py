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
   Both validate paths share :func:`_validate_and_write`.

Caller-commits convention: no ``await conn.commit()`` inside single-row helpers.
:func:`_validate_batch` manages its own transaction lifecycle (savepoints +
periodic commits) because it is a long-running bulk operation.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from enum import Enum, auto

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .address_client import (
    CONFIRMED_STATUSES,
    UNAVAILABLE_STATUS,
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
# provider answers or not — and a failed re-check never degrades the prior
# confirmation (see _validate_and_write). A call the provider never answers
# (transport failure, status 'unavailable') writes nothing, so it is retried on
# the next run rather than parked for a TTL (#183).
VALIDATION_TTL_DAYS = 180

# Upper bound on /validate calls per UTC day across all automatic backfill runs
# (both twice-daily post-scrape hooks and the weekly timer share it). Kept well
# under the upstream USPS 10K/day cap so we never 429 into the Google fallback
# (160/day). attempted_at is stamped on every *answered* call, so counting rows
# with attempted_at >= start-of-day counts same-day answered calls. #150.
# Unanswered calls leave no stamp and are invisible to this count; during an
# outage MAX_CONSECUTIVE_NO_ANSWER is what bounds them (#183).
DAILY_VALIDATION_LIMIT = 5000

# _validate_batch stops after this many consecutive rows get no provider answer
# (#183). At the ~7% 'unavailable' rate seen in normal operation, 10 in a row
# by chance is vanishingly unlikely. A real outage trips it after 10 rows —
# at most 10 x MAX_RETRIES = 30 HTTP attempts, ~8.5 min with 15s timeouts plus
# backoff — instead of spending the whole batch against a provider that is down.
MAX_CONSECUTIVE_NO_ANSWER = 10


class LocationOutcome(Enum):
    """What processing one location did — drives _validate_batch's breaker (#183)."""

    WRITTEN = auto()  # standardized, or validation confirmed: std_* overlaid
    RECORDED = auto()  # provider answered without confirming: status + attempt stamped
    NO_ANSWER = auto()  # transport failure or 'unavailable': nothing written
    FAILED = auto()  # empty address, standardize failure, or DB write error


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
        result = await standardize(raw_address, client, log_ref=location_id)
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


async def _validate_and_write(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None,
) -> LocationOutcome:
    """Call /validate for one location and write the result.

    * confirmed — overlays std_* columns, validation_status, dpv_match_code,
      latitude, longitude, and sets address_standardized_at,
      address_validated_at and address_validation_attempted_at → WRITTEN.
    * any other answer (not_confirmed, invalid, not_found) — writes
      validation_status, dpv_match_code and address_validation_attempted_at
      only; std_* and address_validated_at are left intact so a failed
      re-check never degrades a prior confirmation (#150) → RECORDED.
    * no answer (transport failure, or 'unavailable': USPS/Google down or
      rate-limited) — writes nothing, so the row is retried next run and a
      prior status is not overwritten (#183) → NO_ANSWER.
    """
    try:
        result = await validate(raw_address, client, log_ref=location_id)
    except Exception:
        logger.exception("Validate failed for location %d", location_id)
        return LocationOutcome.NO_ANSWER

    if result is None:
        return LocationOutcome.NO_ANSWER

    validation = result.get("validation") or {}
    status = validation.get("status", "")
    if status == UNAVAILABLE_STATUS:
        logger.info("Validation provider unavailable for location %d; left for retry", location_id)
        return LocationOutcome.NO_ANSWER

    dpv = validation.get("dpv_match_code")
    # Gate on validation status: v2 returns address_line_1="" (not None) for unconfirmed.
    has_address = status in CONFIRMED_STATUSES
    now = datetime.now(UTC)

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
                    address_standardized_at=now,
                    address_validated_at=now,
                    address_validation_attempted_at=now,
                )
            )
            return LocationOutcome.WRITTEN

        # Answered but not confirmed — record the attempt and status, but leave
        # std_* and address_validated_at intact (non-destructive; #150).
        await conn.execute(
            update(locations)
            .where(locations.c.id == location_id)
            .values(
                validation_status=status,
                dpv_match_code=dpv,
                address_validation_attempted_at=now,
            )
        )
    except Exception:
        logger.exception("Failed to update location %d during validate", location_id)
        return LocationOutcome.FAILED

    return LocationOutcome.RECORDED


async def validate_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Optionally validate a location row via POST /api/v2/validate.

    Gated by ENABLE_ADDRESS_VALIDATION env var. No-op (returns False) when
    the flag is off. Writes as described in :func:`_validate_and_write`.

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

    outcome = await _validate_and_write(conn, location_id, raw_address, client)
    return outcome is LocationOutcome.WRITTEN


async def _process_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> LocationOutcome:
    """:func:`process_location`, reporting the full :class:`LocationOutcome`.

    The standardize-only path reports WRITTEN or FAILED — it never trips the
    no-answer breaker, which guards the metered /validate providers.
    """
    if not raw_address or not raw_address.strip():
        return LocationOutcome.FAILED

    if is_validation_enabled():
        # Single /validate call covers both standardization and validation.
        return await _validate_and_write(conn, location_id, raw_address, client)

    # Validation disabled — standardize only.
    ok = await standardize_location(conn, location_id, raw_address, client)
    return LocationOutcome.WRITTEN if ok else LocationOutcome.FAILED


async def process_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Smart dispatcher: standardize and/or validate a location in one API call.

    When ENABLE_ADDRESS_VALIDATION is on, calls ``/validate`` which returns a
    superset of ``/standardize`` and writes as described in
    :func:`_validate_and_write`: a confirmed result overlays std_* and
    address_validated_at, any other answer stamps only status and
    address_validation_attempted_at (non-destructive re-check; #150), and no
    answer — including 'unavailable' — writes nothing (#183).

    When validation is off, calls ``/standardize`` only (no attempted_at).

    Does NOT commit — the caller is responsible for committing.

    Returns True if the location was successfully processed, False otherwise.
    """
    outcome = await _process_location(conn, location_id, raw_address, client)
    return outcome is LocationOutcome.WRITTEN


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
    Stops early after MAX_CONSECUTIVE_NO_ANSWER consecutive rows get no
    provider answer (#183); those rows stay unstamped for the next run.

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
    no_answer_streak = 0

    for attempted, row in enumerate(rows, start=1):
        location_id = row["id"]
        address = row["raw_address"]

        try:
            async with conn.begin_nested():
                outcome = await _process_location(conn, location_id, address)
            if outcome is LocationOutcome.WRITTEN:
                succeeded += 1
            no_answer_streak = no_answer_streak + 1 if outcome is LocationOutcome.NO_ANSWER else 0
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

        if no_answer_streak >= MAX_CONSECUTIVE_NO_ANSWER:
            logger.warning(
                "Stopping: %d consecutive locations got no answer from the validator"
                " (provider outage?); %d left for the next run",
                no_answer_streak,
                total - attempted,
            )
            break

        if attempted % batch_size == 0:
            await conn.commit()
            logger.info("Progress: %d/%d (%d ok, %d err)", attempted, total, succeeded, errors)

        if rate_limit:
            await asyncio.sleep(rate_limit)

    # Final commit for any remaining rows after the last batch_size boundary.
    await conn.commit()
    logger.info("Done: %d/%d attempted, %d succeeded", attempted, total, succeeded)
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
    is stamped on every answered call, so counting rows attempted since
    start-of-day counts same-day answered calls, shared with any manual refresh
    run. Unanswered calls are invisible to it; _validate_batch's no-answer
    breaker bounds those (#183).

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
