"""HTTP client for the address-validator service.

Pure transport layer — no database dependency.  Owns the shared
connection pool, API-key/config reads, 429/500 retry policy, and the
``/standardize`` and ``/validate`` endpoint wrappers.  The DB-facing
orchestration lives in pg_address_validator.py.

Split from pg_address_validator.py (#141).
"""

import asyncio
import logging
import os

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://address-validator.exe.xyz:8000"
API_PATH_PREFIX = "/api/v2"
CONFIRMED_STATUSES = frozenset(
    {"confirmed", "confirmed_missing_secondary", "confirmed_bad_secondary"}
)
TIMEOUT = 15.0
HTTP_OK = 200
HTTP_TOO_MANY_REQUESTS = 429
HTTP_INTERNAL_SERVER_ERROR = 500
DEFAULT_RETRY_AFTER = 2.0
# Upper bound on any single retry sleep. Bounds an adversarial or buggy
# Retry-After header (and its backoff-multiplied product) so no single retry
# sleep exceeds 60s — worst-case ~180s total across MAX_RETRIES. See issue #118.
MAX_RETRY_AFTER = 60.0
MAX_RETRIES = 3

# Shared connection pool for all address validation HTTP calls.
# httpx.AsyncClient binds to the event loop lazily (on first request), so
# module-level construction is safe before any event loop exists.
_shared_client: httpx.AsyncClient = httpx.AsyncClient(timeout=TIMEOUT)


async def close_shared_client() -> None:
    """Close the module-level httpx client, releasing TLS sessions cleanly."""
    await _shared_client.aclose()


def get_api_key() -> str:
    """Return ADDRESS_VALIDATOR_API_KEY from the environment ("" when unset).

    Read at call time (not cached) so tests and runtime overrides take
    effect immediately.
    """
    return os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")


def is_validation_enabled() -> bool:
    """Return True if ENABLE_ADDRESS_VALIDATION is set to a truthy value.

    Checks the environment variable at call time (not cached) so that tests
    and runtime overrides take effect immediately.
    """
    return os.environ.get("ENABLE_ADDRESS_VALIDATION", "").lower() in ("1", "true", "yes")


def _parse_retry_after(response: httpx.Response) -> float:
    """Extract Retry-After seconds from a response, clamped to [0.5, MAX_RETRY_AFTER].

    Falls back to DEFAULT_RETRY_AFTER on a missing/unparseable header. A value
    above MAX_RETRY_AFTER is clamped and logged — a Retry-After that large
    signals a misbehaving upstream, not a transient blip.
    """
    raw = response.headers.get("Retry-After", "")
    try:
        # Only the RFC 7231 delay-seconds form is honored. The HTTP-date form
        # ("Wed, 21 Oct 2015 07:28:00 GMT") is intentionally treated as
        # unparseable and falls back to DEFAULT_RETRY_AFTER — our validator
        # emits numeric values, and the fallback degrades safely (2s, not a stall).
        parsed = max(float(raw), 0.5)
    except (ValueError, TypeError):
        return DEFAULT_RETRY_AFTER
    if parsed > MAX_RETRY_AFTER:
        logger.warning(
            "Retry-After %.1fs exceeds cap; clamping to %.1fs (possible service issue)",
            parsed,
            MAX_RETRY_AFTER,
        )
        return MAX_RETRY_AFTER
    return parsed


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
            # Cap the multiplied wait too — the backoff multiplier must not push
            # an already-capped Retry-After back over the ceiling.
            wait = min(_parse_retry_after(response) * backoff_multiplier, MAX_RETRY_AFTER)
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


async def _call_endpoint(
    endpoint: str,
    address: str,
    client: httpx.AsyncClient | None,
    label: str,
) -> dict | None:
    """POST *address* to *endpoint*, returning the parsed JSON dict or None."""
    api_key = get_api_key()
    if not api_key:
        return None

    url = f"{BASE_URL}{API_PATH_PREFIX}/{endpoint}"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    _client = client if client is not None else _shared_client
    response = await _post_with_retry(url, payload, headers, _client, label)
    if response is None:
        return None

    if response.status_code != HTTP_OK:
        logger.warning(
            "%s API returned status %d for: %s",
            label.capitalize(),
            response.status_code,
            address,
        )
        return None

    data = response.json()
    for warn in data.get("warnings") or []:
        logger.warning("Address API warning for %r: %s", address, warn)
    return data


async def standardize(address: str, client: httpx.AsyncClient | None = None) -> dict | None:
    """Standardize an address via POST /api/v2/standardize.

    Sends the full raw address string.  The server parses and standardizes
    the address according to USPS Publication 28 rules.

    Retries on HTTP 429 (service rate limit) and 500 (proxy throttle) with
    exponential backoff (up to MAX_RETRIES).
    Returns a dict on success, or None on any failure.
    """
    return await _call_endpoint("standardize", address, client, "address standardize")


async def validate(address: str, client: httpx.AsyncClient | None = None) -> dict | None:
    """Validate an address via POST /api/v2/validate.

    Sends the full raw address string. The server runs parse → standardize
    internally before calling the USPS DPV provider.

    Retries on HTTP 429 (service rate limit) and 500 (proxy throttle) with
    exponential backoff (up to MAX_RETRIES).
    Returns a dict on success, or None on any failure.
    A 200 response with validation.status='not_confirmed' or 'unavailable'
    is returned as a dict (not None) — the caller decides how to handle it.
    """
    return await _call_endpoint("validate", address, client, "address validation")
