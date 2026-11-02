"""Runtime configuration helpers shared across the application.

Central home for environment-variable reads that more than one module
needs.  The full set of environment variables the project consumes is
documented in ``.env.example`` at the repo root.
"""

import importlib.metadata
import logging
import os
import re
from collections.abc import Mapping
from functools import cache

logger = logging.getLogger(__name__)


@cache
def get_build_id() -> str:
    """Return the BUILD_ID for static-asset cache busting.

    Falls back to ``"dev"`` (with a one-time warning) when unset — cache
    busting is then disabled, which is fine everywhere except production.
    Memoized so every caller sees the same value for the process lifetime.
    """
    build_id = os.environ.get("BUILD_ID")
    if not build_id:
        logger.warning("BUILD_ID not set; static asset cache-busting disabled")
        return "dev"
    return build_id


# Prefix for every environment variable added from #204 on.  Existing
# variables keep their names.
ENV_PREFIX = "CO_WSLCB_LICENSING_TRACKER_"

# TypeSafe Jev list price, for spend reporting (#205).
JEV_USD_PER_MILLION_INPUT_TOKENS = 0.042

_JEV_CONFIDENCE_THRESHOLD_ENV = f"{ENV_PREFIX}JEV_CONFIDENCE_THRESHOLD"
_JEV_CONFIDENCE_THRESHOLD_DEFAULT = 0.7


def jev_confidence_threshold() -> float:
    """Return the minimum Jev confidence that wins classification resolution.

    Read at call time (not memoized): a change takes effect at the next
    re-resolve.  Unset gives 0.7; a non-numeric value, or one outside
    ``[0, 1]``, logs a WARNING and gives 0.7.
    """
    raw = os.environ.get(_JEV_CONFIDENCE_THRESHOLD_ENV)
    if raw is None or not raw.strip():
        return _JEV_CONFIDENCE_THRESHOLD_DEFAULT
    try:
        value = float(raw)
    except ValueError:
        value = None
    if value is None or not 0.0 <= value <= 1.0:
        logger.warning(
            "%s=%r is not a number in [0, 1]; using %s",
            _JEV_CONFIDENCE_THRESHOLD_ENV,
            raw,
            _JEV_CONFIDENCE_THRESHOLD_DEFAULT,
        )
        return _JEV_CONFIDENCE_THRESHOLD_DEFAULT
    return value


# -- Outbound identity (#243) ----------------------------------------------
# Every outbound HTTP client says who we are: WSLCB's WAF flagged the bare
# ``python-httpx`` User-Agent (#198).

CONTACT_EMAIL_ENV = f"{ENV_PREFIX}CONTACT_EMAIL"
PROJECT_URL = "https://github.com/CannObserv/wslcb-licensing-tracker"
_PRODUCT = "CannObserv-wslcb-licensing-tracker"

_LOCAL_ATOM = r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+"
_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
#: Syntax only, no DNS or deliverability check. Conservative on purpose: it
#: cannot admit ``(``, ``)``, ``\``, whitespace or control characters, any of
#: which would break the User-Agent comment the address sits in (RFC 9110
#: §5.6.5).
_EMAIL = re.compile(rf"{_LOCAL_ATOM}(?:\.{_LOCAL_ATOM})*@{_LABEL}(?:\.{_LABEL})*\.[A-Za-z]{{2,}}")


class ContactEmailError(ValueError):
    """The contact email for the User-Agent is unset or malformed."""


def contact_email(environ: Mapping[str, str] = os.environ) -> str:
    """Return the validated contact email from ``CONTACT_EMAIL_ENV``.

    Read at call time. Raises ``ContactEmailError`` naming the variable when
    it is unset, empty, or not a syntactically valid address.
    """
    raw = environ.get(CONTACT_EMAIL_ENV)
    if raw is None or not raw.strip():
        msg = f"{CONTACT_EMAIL_ENV} is not set; outbound requests identify us by it"
        raise ContactEmailError(msg)
    email = raw.strip()
    if not _EMAIL.fullmatch(email):
        msg = f"{CONTACT_EMAIL_ENV}={email!r} is not a valid email address"
        raise ContactEmailError(msg)
    return email


@cache
def _version() -> str:
    return importlib.metadata.version("wslcb-licensing-tracker")


def user_agent(environ: Mapping[str, str] = os.environ) -> str:
    """The User-Agent every outbound client sends; raises ``ContactEmailError``.

    For the clients whose requests must identify us (WSLCB, the address
    validator, Jev): a missing or malformed contact fails the call.
    """
    return f"{_PRODUCT}/{_version()} (+{PROJECT_URL}; {contact_email(environ)})"


def user_agent_or_fallback(environ: Mapping[str, str] = os.environ) -> str:
    """``user_agent()``, or the same string without the contact, logging an ERROR.

    For the paths that must not fail over it: the co-status check-in (a config
    slip must not silence the alert reporting it) and GCS backup/restore.
    """
    try:
        return user_agent(environ)
    except ContactEmailError as exc:
        logger.error("%s; sending the User-Agent without a contact", exc)  # noqa: TRY400 — no traceback to add
        return f"{_PRODUCT}/{_version()} (+{PROJECT_URL})"
