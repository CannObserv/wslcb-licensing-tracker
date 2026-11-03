"""Test-suite environment normalisation for the #204 rename.

Imported by ``conftest.py`` at session start; kept out of conftest so its
own test can import it.
"""

import os
from collections.abc import MutableMapping

from wslcb_licensing_tracker.config import LEGACY_ENV_NAMES


def promote_legacy_env_names(environ: MutableMapping[str, str] = os.environ) -> None:
    """Move each pre-#204 name the shell set onto its prefixed name.

    The suite then sees one name per variable whichever spelling the shell
    used. Tests set and clear only the prefixed name, and a legacy name the
    shell exported can't override one of them through ``config.env()``.
    A prefixed name already set wins, as it does in ``env()``. The fallback
    itself is tested against explicit mappings (``test_config.TestEnv``).
    """
    for new, old in LEGACY_ENV_NAMES.items():
        if old in environ:
            value = environ.pop(old)
            environ.setdefault(new, value)
