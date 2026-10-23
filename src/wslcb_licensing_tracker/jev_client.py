"""HTTP client for TypeSafe Jev entity classification (#207).

Pure transport layer — no database dependency.  Owns the request shape (25
names per request, a ``kind`` and an ``org`` question per name), the API-key
and config reads, the 429/529 retry policy, and the response validation.  The
DB-facing orchestration lives in jev_classifier.py.

Plain ``httpx`` rather than ``typesafe-sdk``: the host has no RAM to spare
(#175).  The ``kind`` wording and the context string are copied verbatim from
``docs/research/person-vs-org/evaluate.py`` (``FIVE``, ``CTX``), which the gold
set scored; changing them, or ``ORG_CRITERIA``, means re-scoring it.

A malformed response fails the whole batch: nothing is stored, and the names
are picked up by the next run.  The API key is never logged, and never part of
an error message.
"""

import asyncio
import logging
import math
import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

import httpx

from .config import ENV_PREFIX

logger = logging.getLogger(__name__)

URL = "https://api.typesafe.ai/v1/systemone"
DEFAULT_MODEL = "jev-1.14.0"
BATCH_SIZE = 25
CONCURRENCY = 8
TIMEOUT = 60.0
MAX_ATTEMPTS = 6
# Rate limit and overload; slept 2**attempt seconds (1, 2, 4, 8, 16).
RETRY_STATUSES = frozenset({429, 529})
_AUTH_STATUSES = frozenset({401, 403})
_DEFAULT_MAX_NAMES_PER_RUN = 5000

_ENABLED_ENV = f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION"
_API_KEY_ENV = f"{ENV_PREFIX}TYPESAFE_API_KEY"
_MODEL_ENV = f"{ENV_PREFIX}JEV_MODEL"
_MAX_NAMES_ENV = f"{ENV_PREFIX}JEV_MAX_NAMES_PER_RUN"

CONTEXT = (
    "An applicant-list entry from a Washington State Liquor and Cannabis Board licensing record. "
    'Entries are uppercase, may be "LAST, FIRST", and may be cut off at 30 characters.'
)
KIND_CRITERIA = {
    "person": "One or more natural individual humans named directly "
    "(e.g. JOHN SMITH, SMITH, JOHN A, or a couple like JOHN & MARY SMITH)",
    "organization": "A business, nonprofit, club, lodge, church, tribe, school, or government body",
    "trust": "A trust, or a trustee acting for a trust "
    "(e.g. SMITH FAMILY TRUST, JOHN DOE TTEE, REVOCABLE TR)",
    "estate": "The estate of a deceased individual (e.g. JOHN ESTATE SMITH, ESTATE OF JANE DOE)",
    "not_a_name": "Administrative text, regulation citations, counts, or numbers "
    "rather than a name",
}
# Unvalidated until #210 (task 5.1) scores it on the gold set.
ORG_CRITERIA = {
    "business": "A for-profit company or business of any size, including stores, bars, "
    "restaurants, wineries, breweries and holding companies "
    "(e.g. FAMILY DOLLAR, LIBERTY BAR, ACME HOLDINGS LLC)",
    "nonprofit": "A nonprofit, charity, club, fraternal lodge, veterans post, church, museum, "
    "theater or civic association (e.g. ELKS LODGE 1234, AMERICAN LEGION POST 12)",
    "tribe": "A Native American tribe, band or nation, or a tribal government "
    "(e.g. LOWER ELWHA KLALLAM TRIBE, CONFEDERATED TRIBES OF THE COLVILLE RESERVATION)",
    "government": "A city, town, county, state, port, public school or university, public utility, "
    "or other government agency (e.g. CITY OF LONGVIEW, PORT OF SEATTLE)",
}
_KIND_INSTRUCTIONS = "What kind of entity does `names.n{i}` refer to?"
_ORG_INSTRUCTIONS = "If `names.n{i}` is an organization, which kind of organization is it?"


@dataclass(frozen=True)
class JevAnswer:
    """One name's two answers; ``raw`` keeps both answer objects as returned."""

    kind: str
    kind_confidence: float
    org: str
    org_confidence: float
    raw: dict


@dataclass(frozen=True)
class JevBatch:
    """One request's result, with ``answers`` in the order the names were sent."""

    model: str
    input_tokens: int
    answers: list[JevAnswer]


class JevError(Exception):
    """A batch that stored nothing: a failed request or a malformed response.

    ``status`` is the HTTP status, or None for a transport error.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        """Record the message and the HTTP status (None when no response came)."""
        super().__init__(message)
        self.status = status

    @property
    def auth(self) -> bool:
        """True for 401/403: a wrong or revoked key, which no retry will fix."""
        return self.status in _AUTH_STATUSES


def is_classification_enabled() -> bool:
    """Return True if ``…_ENABLE_JEV_CLASSIFICATION`` is ``1``/``true``/``yes``.

    Read at call time.  Gates Jev calls only; resolution never reads it.
    """
    return os.environ.get(_ENABLED_ENV, "").strip().lower() in ("1", "true", "yes")


def get_api_key() -> str:
    """Return ``…_TYPESAFE_API_KEY`` ("" when unset).  Read at call time."""
    return os.environ.get(_API_KEY_ENV, "").strip()


def get_model() -> str:
    """Return the pinned Jev model, ``…_JEV_MODEL`` or ``DEFAULT_MODEL``."""
    return os.environ.get(_MODEL_ENV, "").strip() or DEFAULT_MODEL


def max_names_per_run() -> int:
    """Return the per-run name cap, ``…_JEV_MAX_NAMES_PER_RUN`` (default 5000).

    A non-integer or a value below 1 logs a WARNING and gives the default.
    """
    raw = os.environ.get(_MAX_NAMES_ENV, "").strip()
    if not raw:
        return _DEFAULT_MAX_NAMES_PER_RUN
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        logger.warning(
            "%s=%r is not a positive integer; using %d",
            _MAX_NAMES_ENV,
            raw,
            _DEFAULT_MAX_NAMES_PER_RUN,
        )
        return _DEFAULT_MAX_NAMES_PER_RUN
    return value


def build_request(names: Sequence[str], model: str) -> dict:
    """Return the request body: shared state, and a kind and an org question per name."""
    questions: dict[str, dict] = {}
    for i in range(len(names)):
        questions[f"kind_{i}"] = {
            "type": "choice",
            "instructions": _KIND_INSTRUCTIONS.format(i=i),
            "criteria": KIND_CRITERIA,
        }
        questions[f"org_{i}"] = {
            "type": "choice",
            "instructions": _ORG_INSTRUCTIONS.format(i=i),
            "criteria": ORG_CRITERIA,
        }
    return {
        "model": model,
        "state": {"context": CONTEXT, "names": {f"n{i}": n for i, n in enumerate(names)}},
        "questions": questions,
    }


def _malformed(detail: str) -> JevError:
    return JevError(f"malformed Jev response: {detail}", status=httpx.codes.OK)


def _choice(answers: dict, key: str, criteria: dict) -> tuple[str, float, dict]:
    """Validate one answer object; return ``(choice, confidence, answer)``."""
    answer = answers.get(key)
    if not isinstance(answer, dict):
        msg = f"no answer object for {key}"
        raise _malformed(msg)
    choice = answer.get("choice")
    if choice not in criteria:
        msg = f"{key} choice {choice!r} is not an option"
        raise _malformed(msg)
    confidence = answer.get("confidence")
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, int | float)
        or not math.isfinite(confidence)
        or not 0.0 <= confidence <= 1.0
    ):
        msg = f"{key} confidence {confidence!r} is not in [0, 1]"
        raise _malformed(msg)
    return choice, float(confidence), answer


def parse_response(data: object, n: int, model: str) -> JevBatch:
    """Validate a response body for *n* names; raise ``JevError`` if anything is off.

    A missing or empty ``model`` echo falls back to the requested *model*.
    """
    if not isinstance(data, dict):
        msg = "body is not an object"
        raise _malformed(msg)
    usage = data.get("usage")
    tokens = usage.get("input_tokens") if isinstance(usage, dict) else None
    if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
        msg = "usage.input_tokens is not a non-negative integer"
        raise _malformed(msg)
    answers = data.get("answers")
    if not isinstance(answers, dict):
        msg = "answers is not an object"
        raise _malformed(msg)
    parsed = []
    for i in range(n):
        kind, kind_conf, kind_raw = _choice(answers, f"kind_{i}", KIND_CRITERIA)
        org, org_conf, org_raw = _choice(answers, f"org_{i}", ORG_CRITERIA)
        parsed.append(JevAnswer(kind, kind_conf, org, org_conf, {"kind": kind_raw, "org": org_raw}))
    echoed = data.get("model")
    return JevBatch(
        model=echoed if isinstance(echoed, str) and echoed.strip() else model,
        input_tokens=tokens,
        answers=parsed,
    )


async def classify_batch(
    names: Sequence[str],
    *,
    api_key: str,
    model: str,
    client: httpx.AsyncClient,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> JevBatch:
    """Classify up to ``BATCH_SIZE`` names in one request.

    Retries 429/529 up to ``MAX_ATTEMPTS`` attempts, sleeping ``2**attempt``
    seconds between them.  Raises ``ValueError`` for an empty or oversized
    batch (before any request), and ``JevError`` for any other status,
    exhausted retries, a request error (transport, decoding) or a malformed
    response.
    """
    if not 0 < len(names) <= BATCH_SIZE:
        msg = f"a Jev batch holds 1 to {BATCH_SIZE} names (got {len(names)})"
        raise ValueError(msg)
    body = build_request(names, model)
    headers = {"Authorization": f"Bearer {api_key}"}
    for attempt in range(MAX_ATTEMPTS):
        try:
            response = await client.post(URL, json=body, headers=headers, timeout=TIMEOUT)
        except httpx.RequestError as e:
            # Transport, decoding or redirect failures: one failed batch, never a
            # crashed run. By type only: the message may echo request details.
            msg = f"Jev request failed: {type(e).__name__}"
            raise JevError(msg) from None
        status = response.status_code
        if status in RETRY_STATUSES:
            if attempt + 1 < MAX_ATTEMPTS:
                wait = 2**attempt
                logger.warning(
                    "Jev: HTTP %d on attempt %d/%d, retrying in %ds",
                    status,
                    attempt + 1,
                    MAX_ATTEMPTS,
                    wait,
                )
                await sleep(wait)
                continue
            msg = f"Jev: HTTP {status} after {MAX_ATTEMPTS} attempts"
            raise JevError(msg, status=status)
        if status != httpx.codes.OK:
            msg = f"Jev: HTTP {status}"
            raise JevError(msg, status=status)
        try:
            data = response.json()
        except ValueError:
            msg = "body is not JSON"
            raise _malformed(msg) from None
        return parse_response(data, len(names), model)
    raise AssertionError  # pragma: no cover — the loop always returns or raises
