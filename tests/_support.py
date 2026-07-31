"""Shared test helpers that are imported directly (not pytest-collected fixtures).

Kept out of ``conftest.py`` because conftest is pytest's auto-collected
fixture/hook module; importing plain helper functions from it couples test
modules to conftest's path. Import from here instead::

    from ._support import stamped_engine
"""

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

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
