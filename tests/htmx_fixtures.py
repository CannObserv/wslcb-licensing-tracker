"""Render the pages the htmx jsdom harness serves (#232).

``tests/js/htmx_harness.js`` runs the vendored htmx in jsdom against these
files, so they are rendered from the real templates through ``TestClient``
with the data layer mocked. They are checked in so ``npm test`` needs no
Python; ``tests/test_htmx_fixtures.py`` re-renders them and fails on drift.

Regenerate after a template change::

    uv run python -m tests.htmx_fixtures
"""

import json
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from wslcb_licensing_tracker.app import app, templates

from ._support import stamped_engine

FIXTURE_DIR = Path(__file__).parent / "js" / "fixtures"
MANIFEST = "manifest.json"

# Pinned so the fixtures don't change with every commit's BUILD_ID.
BUILD_ID = "fixture"

FILTERS = {
    "section_type": ["approved", "discontinued", "new_application"],
    "application_type": ["ASSUMPTION", "NEW APPLICATION"],
    "state": [{"code": "OR", "name": "Oregon"}, {"code": "WA", "name": "Washington"}],
    "endorsement": ["BEER/WINE GIFT DELIVERY", "CANNABIS PRODUCER TIER 1", "CANNABIS RETAILER"],
    "regulated_substance": [
        {
            "id": 1,
            "name": "Cannabis",
            "display_order": 1,
            "endorsements": ["CANNABIS PRODUCER TIER 1", "CANNABIS RETAILER"],
        },
    ],
}
CITIES = {"WA": ["SEATTLE", "SPOKANE"], "OR": ["PORTLAND"]}
ENTITIES = [
    {"id": 1, "name": "LODGE HALL TRIBE", "entity_type": "organization", "record_count": 3},
    {"id": 2, "name": "LOCAL LOGGING CO", "entity_type": "organization", "record_count": 2},
    {"id": 3, "name": "LOTUS LLC", "entity_type": "organization", "record_count": 1},
    {"id": 4, "name": "ACME HOLDINGS LLC", "entity_type": "organization", "record_count": 1},
]

# /entities: the #215 start state, then a live search for "lo", then "lodge".
_ENT = "type=organization&subtype=tribe&sort=name"
ENTITIES_START = f"/entities?{_ENT}"
ENTITIES_LO = f"/entities?q=lo&{_ENT}"
ENTITIES_LODGE = f"/entities?q=lodge&{_ENT}"

# /search: two submits that change every control. Params in the form's DOM
# order, as htmx sends them (the harness compares them order-insensitively).
SEARCH_1 = (
    "/search?q=cafe&section_type=new_application&application_type=NEW+APPLICATION"
    "&endorsement=CANNABIS+RETAILER&state=WA&outcome_status=pending&city=SEATTLE"
    "&date_from=2026-01-01&date_to=2026-06-30"
)
SEARCH_2 = (
    "/search?q=bar&section_type=approved&application_type=ASSUMPTION"
    "&endorsement=BEER%2FWINE+GIFT+DELIVERY&state=OR&outcome_status=&city=PORTLAND"
    "&date_from=2026-02-01&date_to="
)

# (file stem, url, sent as an htmx request?) — every response the harness may be asked for.
CASES = [
    ("entities-start", ENTITIES_START, False),
    ("entities-lo", ENTITIES_LO, False),
    ("entities-lodge", ENTITIES_LODGE, False),
    ("entities-lo", ENTITIES_LO, True),
    ("entities-lodge", ENTITIES_LODGE, True),
    ("search", "/search", False),
    ("search-1", SEARCH_1, False),
    ("search-2", SEARCH_2, False),
    ("search-1", SEARCH_1, True),
    ("search-2", SEARCH_2, True),
    ("cities-wa", "/api/v1/cities?state=WA", False),
    ("cities-or", "/api/v1/cities?state=OR", False),
]


def _db_ctx(*_args, **_kwargs):
    """Stand in for ``get_db``: an async context yielding a dummy connection."""
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=MagicMock())
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def _search_records(_conn, **_kwargs):
    # One hit, no rows: the results partial then shows its export link, whose
    # query string echoes every filter the server saw.
    return [], 1


async def _get_filter_options(_conn):
    return FILTERS


async def _get_cities_for_state(_conn, state):
    return CITIES.get(state, [])


async def _get_entities(_conn, *, q=None, **_kwargs):
    rows = [e for e in ENTITIES if not q or q.upper() in e["name"]]
    return {"entities": rows, "total": len(rows)}


def fixture_name(stem: str, url: str, *, hx: bool) -> str:
    """File name for one case: ``<stem>[.hx].<html|json>``."""
    ext = "json" if url.startswith("/api/") else "html"
    return f"{stem}{'.hx' if hx else ''}.{ext}"


def render_all() -> dict[str, str]:
    """Render every case; return ``{file name: content}`` including the manifest."""
    files: dict[str, str] = {}
    manifest = []
    with ExitStack() as stack:
        stack.enter_context(stamped_engine())
        for target, new in [
            ("wslcb_licensing_tracker.admin_auth._lookup_admin", AsyncMock(return_value=None)),
            ("wslcb_licensing_tracker.app.get_db", _db_ctx),
            ("wslcb_licensing_tracker.api_routes.get_db", _db_ctx),
            ("wslcb_licensing_tracker.app.search_records", _search_records),
            ("wslcb_licensing_tracker.app.get_filter_options", _get_filter_options),
            ("wslcb_licensing_tracker.app.get_cities_for_state", _get_cities_for_state),
            ("wslcb_licensing_tracker.api_routes.get_cities_for_state", _get_cities_for_state),
            ("wslcb_licensing_tracker.app.get_entities", _get_entities),
        ]:
            stack.enter_context(patch(target, new=new))
        stack.enter_context(patch.dict(templates.env.globals, {"build_id": BUILD_ID}))
        client = TestClient(app)
        for stem, url, hx in CASES:
            resp = client.get(url, headers={"HX-Request": "true"} if hx else {})
            resp.raise_for_status()
            name = fixture_name(stem, url, hx=hx)
            files[name] = resp.text
            manifest.append(
                {
                    "url": url,
                    "hx": hx,
                    "file": name,
                    "content_type": resp.headers["content-type"],
                }
            )
    files[MANIFEST] = json.dumps(manifest, indent=2) + "\n"
    return files


def write_all() -> list[str]:
    """Write the rendered fixtures, removing stale ones; return the names written."""
    files = render_all()
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for old in FIXTURE_DIR.iterdir():
        if old.name not in files:
            old.unlink()
    for name, content in files.items():
        (FIXTURE_DIR / name).write_text(content, encoding="utf-8")
    return sorted(files)


if __name__ == "__main__":
    names = write_all()
    print(f"wrote {len(names)} files to {FIXTURE_DIR}", file=sys.stderr)  # noqa: T201
