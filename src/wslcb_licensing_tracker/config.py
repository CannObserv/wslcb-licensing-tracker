"""Runtime configuration helpers shared across the application.

Central home for environment-variable reads. Every variable the project
consumes carries ``ENV_PREFIX`` and is read through ``env()``, which falls
back to the pre-#204 name during the rename window. The full set is
documented in ``.env.example`` at the repo root.
"""

import importlib.metadata
import logging
import os
import re
from collections.abc import Mapping
from functools import cache
from typing import overload

logger = logging.getLogger(__name__)


# Prefix for every environment variable the project consumes (#204).
ENV_PREFIX = "CO_WSLCB_LICENSING_TRACKER_"

DATABASE_URL_ENV = f"{ENV_PREFIX}DATABASE_URL"
TEST_DATABASE_URL_ENV = f"{ENV_PREFIX}TEST_DATABASE_URL"
REQUIRE_PG_TESTS_ENV = f"{ENV_PREFIX}REQUIRE_PG_TESTS"
DATA_DIR_ENV = f"{ENV_PREFIX}DATA_DIR"
BUILD_ID_ENV = f"{ENV_PREFIX}BUILD_ID"
ADMIN_DEV_EMAIL_ENV = f"{ENV_PREFIX}ADMIN_DEV_EMAIL"
ADMIN_DEV_USERID_ENV = f"{ENV_PREFIX}ADMIN_DEV_USERID"
ENABLE_ADDRESS_VALIDATION_ENV = f"{ENV_PREFIX}ENABLE_ADDRESS_VALIDATION"
ADDRESS_VALIDATOR_API_KEY_ENV = f"{ENV_PREFIX}ADDRESS_VALIDATOR_API_KEY"

#: Each renamed variable's pre-#204 name, which ``env()`` still reads, with a
#: one-time WARNING. ``WSLCB_*`` names drop the redundant ``WSLCB_``. The
#: ``RELAY_HOST``/``URL``/``CHECKOUT`` names are read only by the Mac's
#: ``relay-capture.sh``; they are here so one map covers the rename.
LEGACY_ENV_NAMES: Mapping[str, str] = {
    f"{ENV_PREFIX}{suffix}": legacy
    for suffix, legacy in (
        ("DATABASE_URL", "DATABASE_URL"),
        ("TEST_DATABASE_URL", "TEST_DATABASE_URL"),
        ("REQUIRE_PG_TESTS", "REQUIRE_PG_TESTS"),
        ("DATA_DIR", "DATA_DIR"),
        ("BUILD_ID", "BUILD_ID"),
        ("ADMIN_DEV_EMAIL", "ADMIN_DEV_EMAIL"),
        ("ADMIN_DEV_USERID", "ADMIN_DEV_USERID"),
        ("ENABLE_ADDRESS_VALIDATION", "ENABLE_ADDRESS_VALIDATION"),
        ("ADDRESS_VALIDATOR_API_KEY", "ADDRESS_VALIDATOR_API_KEY"),
        ("ARCHIVE_BUCKET", "WSLCB_ARCHIVE_BUCKET"),
        ("BACKUP_BUCKET", "WSLCB_BACKUP_BUCKET"),
        ("BACKUP_PREFIX", "WSLCB_BACKUP_PREFIX"),
        ("BACKUP_CHECKIN_BASE_URL", "WSLCB_BACKUP_CHECKIN_BASE_URL"),
        ("BACKUP_MONITOR_ID", "WSLCB_BACKUP_MONITOR_ID"),
        ("SCRAPE_CHECKIN_BASE_URL", "WSLCB_SCRAPE_CHECKIN_BASE_URL"),
        ("SCRAPE_MONITOR_ID", "WSLCB_SCRAPE_MONITOR_ID"),
        ("RELAY_INBOX", "WSLCB_RELAY_INBOX"),
        ("RELAY_HOST", "WSLCB_RELAY_HOST"),
        ("RELAY_URL", "WSLCB_RELAY_URL"),
        ("RELAY_CHECKOUT", "WSLCB_RELAY_CHECKOUT"),
    )
}

#: Legacy names already warned about: one WARNING per name per process.
_DEPRECATION_WARNED: set[str] = set()


@overload
def env(name: str, default: str, *, environ: Mapping[str, str] = ...) -> str: ...
@overload
def env(name: str, default: None = None, *, environ: Mapping[str, str] = ...) -> str | None: ...
def env(
    name: str, default: str | None = None, *, environ: Mapping[str, str] = os.environ
) -> str | None:
    """Return env var *name* (a prefixed name), falling back to its legacy name.

    Every project env read goes through here (``tests/test_env_prefix.py``).
    If *name* is present it wins, even when empty. If only its legacy name
    (``LEGACY_ENV_NAMES``) is set, that value is returned. Either way, a set
    legacy name logs one deprecation WARNING per process, naming both
    variables and never the value. Read at call time.
    """
    legacy = LEGACY_ENV_NAMES.get(name)
    if legacy is None or legacy not in environ:
        return environ.get(name, default)
    if legacy not in _DEPRECATION_WARNED:
        _DEPRECATION_WARNED.add(legacy)
        if name in environ:
            logger.warning(
                "%s is deprecated and ignored: %s is set (#204); remove the old line", legacy, name
            )
        else:
            logger.warning("%s is deprecated (#204); rename it to %s", legacy, name)
    return environ[name] if name in environ else environ[legacy]


@cache
def get_build_id() -> str:
    """Return ``BUILD_ID_ENV`` for static-asset cache busting.

    Falls back to ``"dev"`` (with a one-time warning) when unset — cache
    busting is then disabled, which is fine everywhere except production.
    Memoized so every caller sees the same value for the process lifetime.
    """
    build_id = env(BUILD_ID_ENV)
    if not build_id:
        logger.warning("%s not set; static asset cache-busting disabled", BUILD_ID_ENV)
        return "dev"
    return build_id


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
    raw = env(_JEV_CONFIDENCE_THRESHOLD_ENV)
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
    raw = env(CONTACT_EMAIL_ENV, environ=environ)
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
