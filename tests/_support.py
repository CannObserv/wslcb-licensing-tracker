"""Shared test helpers that are imported directly (not pytest-collected fixtures).

Kept out of ``conftest.py`` because conftest is pytest's auto-collected
fixture/hook module; importing plain helper functions from it couples test
modules to conftest's path. Import from here instead::

    from ._support import stamped_engine
"""

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wslcb_licensing_tracker.app import app

_UNSET = object()


@contextmanager
def stamped_engine(engine=None) -> Iterator[object]:
    """Stamp ``app.state.engine`` for the block, restoring the prior value on exit.

    Bare ``TestClient(app)`` calls (no ``with``) never drive the lifespan that
    sets ``app.state.engine`` (app.py:66-67), so routes that dereference it fail
    standalone (#155). Tests that don't run the lifespan use this to stamp the
    attribute directly. ``app`` is a process-wide singleton, so the prior value
    is saved and restored — no mock leaks onto whatever test module runs next.

    engine: value to stamp (defaults to a fresh ``MagicMock`` when the routes
    under test never dereference it — ``get_db`` is typically patched).
    """
    prev = getattr(app.state, "engine", _UNSET)
    app.state.engine = MagicMock() if engine is None else engine
    try:
        yield app.state.engine
    finally:
        if prev is _UNSET:
            delattr(app.state, "engine")
        else:
            app.state.engine = prev


@pytest.fixture(autouse=True)
def _stamp_engine():
    """Autouse: stamp app.state.engine for every test in the importing module.

    Import it (``from ._support import _stamp_engine  # noqa: F401``) into any
    module whose routes read ``app.state.engine`` without driving the lifespan
    (#155). pytest activates autouse fixtures visible in a module's namespace,
    so the bare import is enough — no per-module redefinition.
    """
    with stamped_engine():
        yield


# A ``classification_review.classification_health`` result: flag off, nothing run.
HEALTH = {
    "enabled": False,
    "key_present": False,
    "model": "jev-1.14.0",
    "threshold": 0.7,
    "last_success_at": None,
    "awaiting": 67202,
    "failures_7d": 0,
    "unfinished_runs": 0,
    "tokens_month": 0,
    "tokens_all": 0,
    "spend_month_usd": 0.0,
    "spend_all_usd": 0.0,
    "queue_main": 0,
    "queue_unknown": 0,
    "problems": [],
}


@pytest.fixture(autouse=True, name="classification_health")
def _classification_health_stub():
    """Autouse: stub the admin dashboard's Jev health query (#208).

    The dashboard tests feed ``conn.execute`` a fixed sequence of mocked
    results; the health query would consume more.  Import
    ``_classification_health_stub``; tests that check the panel take the
    ``classification_health`` fixture and set its ``return_value``.
    """
    with patch(
        "wslcb_licensing_tracker.admin_routes.classification_health",
        new_callable=AsyncMock,
        return_value=dict(HEALTH),
    ) as mock:
        yield mock
