"""Tests that MONITORING.md's key rotation names every copy of the co-wslcb key (#241).

The tenant has one check-in key, and this host holds a copy per unit. A
rotation that misses a copy leaves that monitor answering 401, and a rebuild
that misses one leaves a unit failing at start or silent. These tests read
the repo, not the host: every `LoadCredential=checkin-key:` source under
`infra/` must appear in the section with its mode, and so must the disk GC's
copy, which lives outside `infra/` (the installed GC layer's unit, #200).
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS = REPO_ROOT / "docs"
INFRA = REPO_ROOT / "infra"

HEADING = "## Key rotation (#241)"
ANCHOR = "MONITORING.md#key-rotation-241"
DISK_GC_KEY = "/etc/cannobserv/disk-gc/checkin.key"
CREDENTIAL_RE = re.compile(r"^LoadCredential=checkin-key:(\S+)$", re.MULTILINE)


def _section() -> str:
    text = (DOCS / "MONITORING.md").read_text(encoding="utf-8")
    assert HEADING in text, f"docs/MONITORING.md has no '{HEADING}' section"
    body = text.split(HEADING, 1)[1]
    return body.split("\n## ", 1)[0]


def _infra_key_paths() -> set[str]:
    paths = set()
    for unit in INFRA.rglob("*"):
        if unit.is_file():
            paths.update(CREDENTIAL_RE.findall(unit.read_text(encoding="utf-8")))
    return paths


def _row(section: str, path: str) -> str:
    rows = [line for line in section.splitlines() if line.startswith("|") and path in line]
    assert rows, f"no table row in MONITORING.md § Key rotation names {path}"
    return rows[0]


def test_infra_loads_the_key_somewhere():
    """A guard over nothing passes vacuously: the regex must still find the units."""
    assert len(_infra_key_paths()) >= 3


def test_every_infra_copy_is_in_the_table_at_0400():
    section = _section()
    for path in sorted(_infra_key_paths()):
        assert "0400" in _row(section, path), f"{path}'s row doesn't say 0400"


def test_disk_gc_copy_is_in_the_table_at_0600():
    assert "0600" in _row(_section(), DISK_GC_KEY)


def test_rebuild_and_gc_docs_point_at_the_section():
    for doc in ("RECOVERY.md", "DISK_GC.md"):
        text = (DOCS / doc).read_text(encoding="utf-8")
        assert ANCHOR in text, f"docs/{doc} doesn't link {ANCHOR}"
