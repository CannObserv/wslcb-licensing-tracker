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
