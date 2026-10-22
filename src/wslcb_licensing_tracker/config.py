"""Runtime configuration helpers shared across the application.

Central home for environment-variable reads that more than one module
needs.  The full set of environment variables the project consumes is
documented in ``.env.example`` at the repo root.
"""

import logging
import os
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
