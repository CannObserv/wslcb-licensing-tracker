"""Guard: certifi no longer trusts the compromised eMudhra chain (#220).

The private keys of eight eMudhra TLS intermediates chaining to
``emSign ECC Root CA - G3`` leaked (exe.dev advisory, 2026-10-20). httpx and
requests verify against certifi's bundle, not the system store, so the
host's ``ca-certificates`` fix never reaches the scraper, the address and Jev
clients, or the GCS backup. certifi 2026.10.19 (PYSEC-2026-4320) drops the
root. These tests fail if the lock or an installed tree goes back below it.

2026.10.19 keeps three other emSign roots it judged unaffected, so the check
is for the G3 root's subject, never for "no emSign at all".
"""

import re
import tomllib
from pathlib import Path

import certifi

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXED = (2026, 10, 19)
COMPROMISED_ROOT = "CN=emSign ECC Root CA - G3"


def _version(text: str) -> tuple[int, ...]:
    """certifi's CalVer as a tuple: '2026.02.25' and '2026.2.25' compare equal."""
    return tuple(int(part) for part in re.findall(r"\d+", text))


def test_lock_pins_fixed_certifi() -> None:
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    (pkg,) = [p for p in lock["package"] if p["name"] == "certifi"]
    assert _version(pkg["version"]) >= FIXED, pkg["version"]


def test_installed_certifi_is_fixed() -> None:
    assert _version(certifi.__version__) >= FIXED, certifi.__version__


def test_installed_bundle_drops_compromised_root() -> None:
    bundle = Path(certifi.where()).read_text()
    assert bundle.count(COMPROMISED_ROOT) == 0
