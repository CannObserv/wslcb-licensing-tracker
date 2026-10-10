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
from sqlalchemy import ColumnElement, case, func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .address_client import (
    CONFIRMED_STATUSES,
    MAX_RETRY_AFTER,
    RETRY_HINT,
    UNAVAILABLE_STATUS,
    UNDETERMINED_STATUS,
    QuotaExhaustedError,
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
# confirmation (see _validate_and_write). A call nobody answers (transport
# failure, 429/5xx after retries, or no provider configured on the validator)
# writes nothing, so it is retried on the next run rather than parked for a TTL
# (#183). A USPS "no delivery-point determination" ('unavailable' with a
# provider) *is* an answer and waits a full TTL like any other (#187). An answer
# carrying the retry hint is not final, so it writes nothing either (#191).
VALIDATION_TTL_DAYS = 180

# Upper bound on /validate calls in any rolling 24h across all automatic runs
# (both twice-daily post-scrape hooks and the weekly timer share it, as does a
# budgeted refresh). Kept under the upstream USPS quota of 500/day, which rolls
# over 24h, so we never 429 into the Google fallback (160/day). #150, #189/#190.
# attempted_at is stamped on every *answered* call, so counting rows attempted
# in the last 24h counts answered calls (validations_used). Unanswered calls
# leave no stamp and are invisible to this count; during an outage
# MAX_CONSECUTIVE_NO_ANSWER is what bounds them (#183). Retry-later answers
# (#191) are invisible too; MAX_CONSECUTIVE_RETRY_LATER bounds those.
DAILY_VALIDATION_LIMIT = 450
VALIDATION_WINDOW = timedelta(hours=24)

# _validate_batch stops after this many consecutive rows get no answer (#183).
# In normal operation a no-answer is rare (every HTTP 200 is an answer; #187),
# so 10 in a row means the validator or its providers are down. It trips after 10 rows —
# at most 10 x MAX_RETRIES = 30 HTTP attempts, ~8.5 min with 15s timeouts plus
# backoff — instead of spending the whole batch against a provider that is down.
# A daily quota that is out needs no breaker: its 429 carries a Retry-After past
# MAX_RETRY_AFTER, and the batch stops on the first one (QuotaExhaustedError; #187).
MAX_CONSECUTIVE_NO_ANSWER = 10

# _validate_batch stops after this many consecutive retry-later answers. One
# such answer means USPS returned 429/5xx and Google answered in its place, so a
# streak means USPS is out (most likely its daily quota) and every further row
# spends one of Google's 160/day calls for nothing the tracker writes (#189/#190).
MAX_CONSECUTIVE_RETRY_LATER = 3


# Statuses meaning "no provider determined this address" (address-validator#250).
NO_DETERMINATION_STATUSES = frozenset({UNAVAILABLE_STATUS, UNDETERMINED_STATUS})


class LocationOutcome(Enum):
    """What processing one location did — drives _validate_batch's breaker (#183)."""

    WRITTEN = auto()  # standardized, or validation confirmed: std_* overlaid
    RECORDED = auto()  # provider answered without confirming: status + attempt stamped
    NO_ANSWER = auto()  # transport failure, 429/5xx, or no provider: nothing written
    RETRY_LATER = auto()  # answered, but a fallback provider was out: nothing written
    FAILED = auto()  # empty address, standardize failure, or DB write error


def _sanitize_country(raw: str) -> str:
    """Return raw if it looks like an ISO 3166-1 alpha-2 code, else empty string."""
    return raw if (len(raw) == ISO_ALPHA2_LEN and raw.isalpha() and raw.isascii()) else ""


def _upper(value: str | None) -> str | None:
    """Uppercase *value*, passing None through (a NULL column stays NULL)."""
    return value.upper() if value is not None else None


def _std_columns(result: dict, string_key: str) -> dict:
    """Map a /standardize or /validate result onto the std_* columns, uppercased.

    USPS answers arrive uppercase (Pub 28); Google-grade ones arrive mixed case
    (CannObserv/address-validator#263), which split the city filter in two
    (#188). Uppercasing here keeps one spelling whichever provider answered.
    *string_key* names the full-address field: "standardized" or "validated".
    """
    return {
        "std_address_line_1": _upper(result.get("address_line_1", "")),
        "std_address_line_2": _upper(result.get("address_line_2", "")),
        "std_city": _upper(result.get("city", "")),
        "std_region": _upper(result.get("region", "")),
        "std_postal_code": _upper(result.get("postal_code", "")),
        "std_country": _sanitize_country(_upper(result.get("country", "")) or ""),
        "std_address_string": _upper(result.get(string_key)),
    }


async def standardize_location(
    conn: AsyncConnection,
    location_id: int,
    raw_address: str,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Standardize and update a single location row via POST /api/v2/standardize.

    Always runs regardless of the ENABLE_ADDRESS_VALIDATION flag.

    On success writes std_address_line_1/2, std_city, std_region,
    std_postal_code, std_country, std_address_string (all uppercased; #188),
    validation_status (set to "standardized"), and address_standardized_at.

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
    except QuotaExhaustedError:
        raise  # the batch decides to stop (#187)
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
                **_std_columns(result, "standardized"),
                validation_status="standardized",
                address_standardized_at=datetime.now(UTC),
            )
        )
    except Exception:
        logger.exception("Failed to update location %d", location_id)
        return False

    return True


async def _write(conn: AsyncConnection, location_id: int, values: dict) -> bool:
    """UPDATE one location row; log and return False on a DB error."""
    try:
        await conn.execute(update(locations).where(locations.c.id == location_id).values(**values))
    except Exception:
        logger.exception("Failed to update location %d during validate", location_id)
        return False
    return True


async def _record_attempt(
    conn: AsyncConnection,
    location_id: int,
    now: datetime,
    status_if_none: str,
    replaces: ColumnElement[bool] | None = None,
) -> LocationOutcome:
    """Stamp the attempt but keep the row's status, dpv and std_* (#187).

    For an answer with nothing to replace what the row holds: a provider's
    no-determination, or a DPV-less confirmation of a row USPS already
    confirmed. A NULL status — or a row matching *replaces* — takes
    *status_if_none*, so a never-answered row shows why it has no confirmation.
    """
    current = locations.c.validation_status
    replaced = current.is_(None) if replaces is None else current.is_(None) | replaces
    ok = await _write(
        conn,
        location_id,
        {
            "validation_status": case((replaced, status_if_none), else_=current),
            "address_validation_attempted_at": now,
        },
    )
    return LocationOutcome.RECORDED if ok else LocationOutcome.FAILED


async def _is_usps_confirmed(conn: AsyncConnection, location_id: int) -> bool:
    """True if the row's std_* come from a confirmation not known to be DPV-less.

    Keys on address_validated_at, not the DPV code: a later non-confirming
    answer (pre-#183 transient stamping, a DPV-less invalid/not_found) clears
    dpv_match_code but keeps the confirmed std_*. Only a confirmed status with
    no DPV code marks a Google-grade confirmation, which has nothing to protect
    (#189).
    """
    row = (
        await conn.execute(
            select(
                locations.c.validation_status,
                locations.c.dpv_match_code,
                locations.c.address_validated_at,
            ).where(locations.c.id == location_id)
        )
    ).one_or_none()
    if not (row and row.address_validated_at):
        return False
    return bool(row.dpv_match_code) or row.validation_status not in CONFIRMED_STATUSES


async def _apply_no_determination(
    conn: AsyncConnection, location_id: int, validation: dict, now: datetime
) -> LocationOutcome:
    """Record a provider's no-determination answer (CannObserv/address-validator#250)."""
    status = validation.get("status", "")
    provider = validation.get("provider")
    logger.info("No determination for location %d (%s, %s)", location_id, status, provider)
    # A no-determination status gives way to the newer one, so pre-v2 USPS
    # 'unavailable' rows take 'undetermined' (#189). So does a DPV-less
    # (Google-grade) confirmation, which no provider will now repeat (#190);
    # its std_* and address_validated_at stay as the best text we have. Real
    # answers — USPS confirmations, not_confirmed, invalid, not_found — are kept.
    current = locations.c.validation_status
    replaces = current.in_(NO_DETERMINATION_STATUSES) | (
        current.in_(CONFIRMED_STATUSES) & locations.c.dpv_match_code.is_(None)
    )
    return await _record_attempt(conn, location_id, now, status, replaces)


async def _apply_confirmation(
    conn: AsyncConnection, location_id: int, result: dict, now: datetime
) -> LocationOutcome:
    """Overlay a confirmation — unless it lacks a DPV code and the row is USPS-confirmed.

    A DPV-less (Google-grade) confirmation can alter the street, suite or ZIP
    (CannObserv/address-validator#258), so it never replaces a USPS
    confirmation; it fills a row that has none (#187).
    """
    validation = result.get("validation") or {}
    status = validation.get("status", "")
    dpv = validation.get("dpv_match_code")
    if dpv is None and await _is_usps_confirmed(conn, location_id):
        logger.info("Kept USPS confirmation of location %d over a DPV-less one", location_id)
        return await _record_attempt(conn, location_id, now, status)
    ok = await _write(
        conn,
        location_id,
        {
            **_std_columns(result, "validated"),
            "validation_status": status,
            "dpv_match_code": dpv,
            "latitude": result.get("latitude"),
            "longitude": result.get("longitude"),
            "address_standardized_at": now,
            "address_validated_at": now,
            "address_validation_attempted_at": now,
        },
    )
    return LocationOutcome.WRITTEN if ok else LocationOutcome.FAILED


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
      A confirmation without a DPV code (Google-grade) does not overlay a row
      USPS already confirmed; it only stamps the attempt (#187) → RECORDED.
    * any other answer (not_confirmed, invalid, not_found) — writes
      validation_status, dpv_match_code and address_validation_attempted_at
      only; std_* and address_validated_at are left intact so a failed
      re-check never degrades a prior confirmation (#150) → RECORDED.
    * no determination — 'undetermined', or 'unavailable' from a named
      provider (pre-v2 USPS blank DPV) — an answer, deterministic per address
      (CannObserv/address-validator#250): stamps the attempt so the row waits a
      full TTL and keeps any prior status/dpv (#187), save that a prior
      no-determination status (#189) or DPV-less confirmation (#190) takes
      the new one → RECORDED.
    * any answer carrying the "later retry may produce a determination"
      warning — a fallback provider was out, so it is not final (undetermined,
      or since CannObserv/address-validator#275 a DPV-less invalid/not_found):
      writes nothing, so the row is retried next run (#191) → RETRY_LATER.
    * no answer (transport failure or 429/5xx after retries → None, or
      'unavailable' with no provider: none configured on the validator) —
      writes nothing, so the row is retried next run (#183) → NO_ANSWER.
    * daily quota out (Retry-After past MAX_RETRY_AFTER) — writes nothing
      and raises QuotaExhaustedError so the batch stops (#187).
    """
    try:
        result = await validate(raw_address, client, log_ref=location_id)
    except QuotaExhaustedError:
        raise  # the batch decides to stop (#187)
    except Exception:
        logger.exception("Validate failed for location %d", location_id)
        result = None

    if result is None:
        return LocationOutcome.NO_ANSWER

    validation = result.get("validation") or {}
    status = validation.get("status", "")
    now = datetime.now(UTC)

    if status == UNAVAILABLE_STATUS and not validation.get("provider"):
        logger.info("Validator has no provider for location %d; left for retry", location_id)
        return LocationOutcome.NO_ANSWER
    # Checked before any status branch: the hint can ride on any non-final
    # answer, and recording one would park the row for a full TTL (#191). It
    # never rides on an answer with a DPV code — upstream's chain returns those
    # on sight — so this cannot discard a USPS confirmation.
    if any(RETRY_HINT in str(w) for w in result.get("warnings") or []):
        logger.info(
            "Answer '%s' for location %d came while a fallback was unreachable; left for retry",
            status,
            location_id,
        )
        return LocationOutcome.RETRY_LATER
    if status in NO_DETERMINATION_STATUSES:
        return await _apply_no_determination(conn, location_id, validation, now)
    # Gate on validation status: v2 returns address_line_1="" (not None) for unconfirmed.
    if status in CONFIRMED_STATUSES:
        return await _apply_confirmation(conn, location_id, result, now)

    # Answered but not confirmed — record the attempt and status, but leave
    # std_* and address_validated_at intact (non-destructive; #150).
    ok = await _write(
        conn,
        location_id,
        {
            "validation_status": status,
            "dpv_match_code": validation.get("dpv_match_code"),
            "address_validation_attempted_at": now,
        },
    )
    return LocationOutcome.RECORDED if ok else LocationOutcome.FAILED


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

    Raises:
        QuotaExhaustedError: the validator asked for a wait past
            MAX_RETRY_AFTER (a daily provider quota is out); nothing is written.
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
    answer writes nothing (#183), and a USPS "no determination" only stamps
    the attempt (#187).

    When validation is off, calls ``/standardize`` only (no attempted_at).

    Does NOT commit — the caller is responsible for committing.

    Returns True if the location was successfully processed, False otherwise.
    Raises QuotaExhaustedError, writing nothing, when the validator asks for a
    wait past MAX_RETRY_AFTER: a daily provider quota is out (#187).
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
    Raises QuotaExhaustedError as :func:`process_location` does.
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
    """Standardize (and optionally validate) the primary location for a license record.

    Raises QuotaExhaustedError as :func:`process_location` does.
    """
    return await _validate_record_location(conn, record_id, "location_id", client)


async def validate_previous_location(
    conn: AsyncConnection,
    record_id: int,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Standardize (and optionally validate) a CHANGE OF LOCATION record's previous location.

    Raises QuotaExhaustedError as :func:`process_location` does.
    """
    return await _validate_record_location(conn, record_id, "previous_location_id", client)


async def _recover_outer_transaction(conn: AsyncConnection, exc: Exception) -> bool:
    """Roll back an aborted outer transaction after a row error.

    If the outer transaction entered an aborted state (e.g.
    InFailedSQLTransactionError), begin_nested() itself fails on every
    subsequent row, so roll back to a clean transaction. Returns False when the
    rollback also fails and the batch must abort.
    """
    orig = getattr(exc, "orig", exc.__cause__)
    if orig is None or "InFailedSQLTransaction" not in str(orig):
        return True
    logger.warning("Outer transaction aborted; rolling back to recover")
    try:
        await conn.rollback()
    except Exception:
        logger.exception("Rollback failed; aborting batch")
        return False
    return True


def _streaks(outcome: LocationOutcome, no_answer: int, retry_later: int) -> tuple[int, int]:
    """Advance the breakers' streaks for one row's outcome.

    A no-answer leaves the retry-later streak as it is: with USPS out, Google's
    per-minute 429s turn some rows into no-answers between retry-laters, and
    resetting on those would hide the streak. A row failure (empty address, DB
    error) says nothing about the provider, so it leaves both. Only a final
    answer clears them.
    """
    if outcome is LocationOutcome.FAILED:
        return no_answer, retry_later
    if outcome is LocationOutcome.NO_ANSWER:
        return no_answer + 1, retry_later
    if outcome is LocationOutcome.RETRY_LATER:
        return 0, retry_later + 1
    return 0, 0


def _breaker_tripped(no_answer_streak: int, retry_later_streak: int, left: int) -> bool:
    """True, with a warning, when a streak says a provider is out (#183, #189/#190)."""
    if no_answer_streak >= MAX_CONSECUTIVE_NO_ANSWER:
        logger.warning(
            "Stopping: %d consecutive locations got no answer from the validator"
            " (provider outage?); %d left for the next run",
            no_answer_streak,
            left,
        )
        return True
    if retry_later_streak >= MAX_CONSECUTIVE_RETRY_LATER:
        logger.warning(
            "Stopping: %d consecutive answers came while a fallback provider was"
            " unreachable (USPS quota or outage?); %d left for the next run",
            retry_later_streak,
            left,
        )
        return True
    return False


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
    provider answer (#183), after MAX_CONSECUTIVE_RETRY_LATER consecutive
    retry-later answers (USPS out, Google spent in its place; #189/#190), or at
    once when the validator reports a daily quota out (QuotaExhaustedError;
    #187); untried rows stay unstamped for the next run.

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
    retry_later_streak = 0

    for attempted, row in enumerate(rows, start=1):
        location_id = row["id"]
        address = row["raw_address"]

        try:
            async with conn.begin_nested():
                outcome = await _process_location(conn, location_id, address)
            if outcome is LocationOutcome.WRITTEN:
                succeeded += 1
            no_answer_streak, retry_later_streak = _streaks(
                outcome, no_answer_streak, retry_later_streak
            )
        except QuotaExhaustedError as exc:
            logger.warning(
                "Stopping: validator returned HTTP %d with Retry-After %.0fs, over the"
                " %.0fs cap (a 429 means a daily provider quota is out); %d left for the next run",
                exc.status,
                exc.retry_after,
                MAX_RETRY_AFTER,
                total - attempted + 1,
            )
            break
        except Exception as exc:  # noqa: BLE001 — intentionally broad; savepoint isolates damage
            logger.warning("Savepoint rollback for location %d", location_id, exc_info=True)
            errors += 1
            if not await _recover_outer_transaction(conn, exc):
                break

        if _breaker_tripped(no_answer_streak, retry_later_streak, total - attempted):
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


async def validations_used(conn: AsyncConnection, now: datetime) -> int:
    """Answered /validate calls in the VALIDATION_WINDOW before *now*.

    USPS's daily quota rolls over 24h, so the budget does too: a UTC-day count
    would let runs either side of midnight spend two days' budget in hours.
    """
    return (
        await conn.execute(
            select(func.count())
            .select_from(locations)
            .where(
                locations.c.address_validation_attempted_at >= now - VALIDATION_WINDOW,
                locations.c.address_validation_attempted_at <= now,
            )
        )
    ).scalar_one()


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

    The daily ceiling (validation-enabled path only) bounds /validate calls in
    any rolling 24h across all automatic runs to stay within upstream limits.
    attempted_at is stamped on every answered call, so counting rows attempted
    in the window counts answered calls, shared with any refresh run
    (validations_used). Unanswered and retry-later calls are invisible to it;
    _validate_batch's breakers bound those (#183, #189/#190).

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
        used = await validations_used(conn, now)
        budget = max(0, daily_limit - used)
        if budget == 0:
            logger.info(
                "Daily validation limit reached (%d used of %d in 24h); skipping backfill",
                used,
                daily_limit,
            )
            return 0

        ttl_cutoff = now - timedelta(days=VALIDATION_TTL_DAYS)
        stmt = (
            base.where(
                (locations.c.address_validation_attempted_at.is_(None))
                | (locations.c.address_validation_attempted_at < ttl_cutoff)
            )
            # Newest first among never-attempted rows: a fresh scrape's locations
            # don't queue behind old rows that keep getting no answer (#187).
            .order_by(
                locations.c.address_validation_attempted_at.asc().nulls_first(),
                locations.c.id.desc(),
            )
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


async def refresh_specific_addresses(  # noqa: PLR0913 — budget is two knobs
    conn: AsyncConnection,
    location_ids: list[int],
    batch_size: int = 100,
    rate_limit: float = 0.5,
    daily_limit: int | None = None,
    reserve: int = 0,
) -> int:
    """Re-standardize (and optionally re-validate) a specific set of locations by ID.

    Rows run in the order of *location_ids*. With *daily_limit* the run takes
    only what the rolling 24h budget has left after *reserve* calls kept back
    for the scrape hooks, so a remediation run can be scheduled without
    starving them or overrunning the upstream quota (#189/#190).

    Args:
        conn: Async SQLAlchemy connection.
        location_ids: List of locations.id values to re-process.
        batch_size: How often to log progress (default 100).
        rate_limit: Seconds to sleep between API calls (default 0.5).
        daily_limit: Budget for calls in any rolling 24h; None runs every id.
        reserve: Calls of *daily_limit* left unspent for other runs.

    Returns:
        Number of locations successfully standardized.
    """
    if not location_ids:
        logger.info("No location IDs provided — nothing to refresh")
        return 0

    if not get_api_key():
        logger.error("No API key configured for address validation")
        return 0

    found = {
        row["id"]: row
        for row in (
            await conn.execute(
                select(locations.c.id, locations.c.raw_address)
                .where(locations.c.id.in_(location_ids))
                .where(locations.c.raw_address.isnot(None))
                .where(locations.c.raw_address != "")
            )
        ).mappings()
    }
    rows = [found[i] for i in dict.fromkeys(location_ids) if i in found]

    if daily_limit is not None:
        used = await validations_used(conn, datetime.now(UTC))
        budget = max(0, daily_limit - used - reserve)
        if budget == 0:
            logger.info(
                "Refresh budget spent (%d used in 24h, limit %d, %d reserved); %d ids wait",
                used,
                daily_limit,
                reserve,
                len(rows),
            )
            return 0
        logger.info(
            "Budget: %d of %d ids (%d used in 24h, limit %d, %d reserved)",
            min(budget, len(rows)),
            len(rows),
            used,
            daily_limit,
            reserve,
        )
        rows = rows[:budget]

    return await _validate_batch(
        conn,
        rows,
        "Refreshing specific addresses",
        batch_size=batch_size,
        rate_limit=rate_limit,
    )
