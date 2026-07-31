"""Structural integrity guards for seed_code_map.json.

``seed_code_map.json`` is the hand-edited source of truth for the
``endorsement_codes`` derived table. These tests catch corruption from a
bad manual edit (duplicate/non-numeric key, empty name) before it reaches
``seed_endorsements`` — where it would fail silently or mis-seed.
"""

import json
from pathlib import Path

from wslcb_licensing_tracker import data_migration
from wslcb_licensing_tracker.endorsements_seed import _SEED_CODE_MAP_PATH, SEED_CODE_MAP

# Codes resolved in #159 (Tier A + B); guard against accidental removal.
_ISSUE_159_MAPPINGS = {
    "374": "COMBO GROCERY OFF PREM S/B/W CLS",
    "396": "CATERING BEER ONLY",
    "432": "BEER/WINE SPECIALTY SHOP GROWLERS",
    "437": "SPORTS ENTERTAINMENT FACILITY",
    "441": "WINE RETAILER RESELLER-SPECIALTY",
    "484": "SLS SPIRITS RETAILER",
    "485": "WINE RETAILER RESELLER",
    "486": "BEER AND WINE TASTING",
    "490": "CATERER DUPLICATE",
    "492": "FARMER'S MARKET WINE SALES",
    "493": "FARMER'S MARKET BEER SALES",
    "499": "NON-CLUB EVENT",
}


def test_seed_code_map_is_valid_json_without_duplicate_keys():
    raw = Path(_SEED_CODE_MAP_PATH).read_text()
    seen: list[str] = []

    def _reject_dupes(pairs):
        for key, _ in pairs:
            assert key not in seen, f"duplicate key in seed_code_map.json: {key!r}"
            seen.append(key)
        return dict(pairs)

    json.loads(raw, object_pairs_hook=_reject_dupes)


def test_all_keys_are_numeric_codes():
    for code in SEED_CODE_MAP:
        assert code.isdigit(), f"non-numeric seed code: {code!r}"


def test_every_code_maps_to_nonempty_names():
    for code, names in SEED_CODE_MAP.items():
        assert isinstance(names, list) and names, f"code {code!r} has no names"
        for name in names:
            assert isinstance(name, str) and name.strip(), (
                f"code {code!r} has an empty/blank name: {name!r}"
            )
            assert name == name.upper(), f"code {code!r} name not upper-case: {name!r}"


def test_issue_159_mappings_present():
    for code, expected in _ISSUE_159_MAPPINGS.items():
        assert SEED_CODE_MAP.get(code) == [expected], (
            f"expected {code!r} -> [{expected!r}], got {SEED_CODE_MAP.get(code)!r}"
        )


def test_reseed_migration_registered():
    """0007 must re-run seed_endorsements so #159 additions reach migrated DBs."""
    from wslcb_licensing_tracker.endorsements_seed import seed_endorsements

    registry = dict(data_migration._MIGRATIONS)
    assert registry.get("0007_reseed_endorsement_codes") is seed_endorsements
