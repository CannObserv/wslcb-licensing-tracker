"""The htmx harness's checked-in fixtures must match the templates (#232).

``tests/js/test_htmx.js`` runs htmx against ``tests/js/fixtures/``. They are
rendered from the templates by ``tests/htmx_fixtures.py``; if a template
changes without a regeneration, the JS tests would pass against stale markup.
"""

from .htmx_fixtures import FIXTURE_DIR, render_all

REGEN = "regenerate with: uv run python -m tests.htmx_fixtures"


def test_fixture_files_match_a_fresh_render():
    """Every fixture file is byte-identical to what the templates render now."""
    rendered = render_all()
    on_disk = {p.name: p.read_text(encoding="utf-8") for p in FIXTURE_DIR.iterdir()}
    assert sorted(on_disk) == sorted(rendered), f"fixture set differs; {REGEN}"
    stale = sorted(name for name, text in rendered.items() if on_disk[name] != text)
    assert not stale, f"stale fixtures {stale}; {REGEN}"


def test_render_is_deterministic():
    """Two renders agree, so the drift check can't flap."""
    assert render_all() == render_all()
