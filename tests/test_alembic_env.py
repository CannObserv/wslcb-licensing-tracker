"""``alembic/env.py`` can't be silently retargeted at prod by the rename (#204).

Unset, alembic falls back to ``alembic.ini``'s URL, which is the shared
prod/dev ``wslcb``. Prod's ``uv run alembic upgrade head`` relies on that.
Once the prefixed variable is set, it has to win: an env file that alembic
didn't read would fall through to prod without a word. The order is the
prefixed name, then the ini; the pre-#204 ``DATABASE_URL`` is no longer read
(#244), so set alone it means the ini.

env.py runs against a stubbed ``alembic.context`` in offline mode, and the
URL it would migrate is what ``context.configure`` receives.
"""

import runpy
from pathlib import Path
from unittest.mock import MagicMock

import alembic
import pytest

from wslcb_licensing_tracker.config import DATABASE_URL_ENV

ENV_PY = Path(__file__).resolve().parent.parent / "alembic" / "env.py"
INI_URL = "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"
PROD = "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb"
TEST = "postgresql+asyncpg://wslcb:wslcb@localhost/wslcb_test"


def _migrated_url(monkeypatch) -> str:
    context = MagicMock()
    context.config.config_file_name = None
    context.config.get_main_option.return_value = INI_URL
    context.config.attributes = {}
    context.is_offline_mode.return_value = True
    monkeypatch.setattr(alembic, "context", context)
    runpy.run_path(str(ENV_PY))
    return context.configure.call_args.kwargs["url"]


@pytest.fixture
def clean(monkeypatch):
    monkeypatch.delenv(DATABASE_URL_ENV, raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    return monkeypatch


def test_new_name_wins_over_a_leftover_legacy_prod_url(clean):
    clean.setenv(DATABASE_URL_ENV, TEST)
    clean.setenv("DATABASE_URL", PROD)
    assert _migrated_url(clean).endswith("/wslcb_test")


def test_new_name_alone(clean):
    clean.setenv(DATABASE_URL_ENV, TEST)
    assert _migrated_url(clean) == TEST.replace("+asyncpg", "")


def test_legacy_name_alone_is_ignored(clean):
    """#244: only the prefixed name is read, so the old one alone means the ini."""
    clean.setenv("DATABASE_URL", TEST)
    assert _migrated_url(clean) == INI_URL.replace("+asyncpg", "")


def test_unset_falls_back_to_the_ini(clean):
    assert _migrated_url(clean) == INI_URL.replace("+asyncpg", "")


def test_set_but_empty_never_means_prod(clean):
    """An empty value fails to connect; falling through to the ini would migrate prod."""
    clean.setenv(DATABASE_URL_ENV, "")
    clean.setenv("DATABASE_URL", TEST)
    assert _migrated_url(clean) == ""
