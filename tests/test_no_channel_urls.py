"""No live Mayfly channel URL in any tracked file (#181).

A Mayfly channel URL, `/c/<id>#<key>`, is the channel's whole access-control
model: whoever holds it can read the conversation, post into two agents'
working context, and delete the record — no owner, no revocation. This repo is
public, so a URL that reaches a tracked file is published on the next push.
The vendored `using-mayfly-chat` skill's Iron Law keeps the URL out of every
durable store; this is the part of that rule the repo can enforce on itself.
Ported from gregoryfoster/skills' `tests/structural/test_no_channel_urls.py`.

- **Match the key, not the host.** The keyless view URL a joiner's first
  `curl` returns is harmless, and a guard firing on `mayfly.chat/c/` cries wolf
  every session. The pattern requires the 22-character id AND the `#` with its
  43-character key, on any host.
- **The pattern cannot match itself.** It is assembled from a character class,
  which no live URL contains; `test_the_detector_is_live` builds its positive
  control at runtime so a broken assembly fails rather than passing vacuously.

Untracked scratch files are the session's own business, covered by the
skill's goodbye gate.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# base64url alphabet; an id is 16 bytes (22 chars), a key 32 bytes (43 chars).
_B64URL = "[A-Za-z0-9_-]"
CHANNEL_URL = re.compile("/c/" + _B64URL + "{22}" + "#" + _B64URL + "{43}")


def _tracked_files() -> list[Path]:
    # Absolute path, not a bare "git": resolving it off $PATH is what S607 flags.
    git = shutil.which("git")
    if git is None:
        pytest.skip("git not on PATH")
    out = subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
        [git, "ls-files", "-z"], cwd=REPO_ROOT, capture_output=True, check=True
    )
    paths = [REPO_ROOT / p for p in out.stdout.decode().split("\0") if p]
    # Submodule gitlinks and directory symlinks are not files; skip them.
    return [p for p in paths if p.is_file()]


def test_the_detector_is_live():
    """A positive control, built at runtime so this file never holds one."""
    live = "https://example.test/c/" + "A" * 22 + "#" + "b" * 43
    assert CHANNEL_URL.search(live), "the assembled pattern must match a live URL"
    # The keyless view URL and the docs' placeholder are both fine.
    assert not CHANNEL_URL.search("https://example.test/c/" + "A" * 22)
    assert not CHANNEL_URL.search("https://example.test/c/<ID>#<key>")


def test_no_tracked_file_holds_a_channel_url():
    offenders: list[str] = []
    for path in _tracked_files():
        try:
            text = path.read_bytes().decode("utf-8")
        except UnicodeDecodeError:
            continue
        for match in CHANNEL_URL.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(REPO_ROOT)}:{line}")
    assert not offenders, (
        "A live Mayfly channel URL (id plus #key) is committed here, which "
        "publishes read, write and delete access to that channel:\n  "
        + "\n  ".join(offenders)
        + "\nRemove it, then treat the channel as leaked: stop the agents "
        "using it, delete it, and distribute a new URL privately "
        "(skills/using-mayfly-chat/references/security.md)."
    )
