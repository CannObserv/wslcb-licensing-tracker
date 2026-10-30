"""Data-straggler compression: uncompressed snapshot/diff archives in ``data/``.

All that's left of the weekly disk-hygiene job (#138). Its tooling tiers
(editor builds and extensions, the npm/npx/uv/pre-commit caches, orphaned
worktree directories) and its usage warning retired to the cohort's disk GC
and headroom sensor (#200, CannObserv/provisioner#61), which never touches
app data. So this half stays repo-side: ``wslcb ingest compress-stragglers``,
run weekly by ``wslcb-compress-stragglers.timer``.

The scraper and the relay write ``.gz`` directly; a straggler is a
hand-placed file or one an interrupted compress left. ``compress_files`` is
also the body of ``ingest compress-snapshots`` / ``compress-diffs``.
"""

import gzip
import logging
from pathlib import Path
from typing import NamedTuple

from .db import DATA_DIR, DIFF_GLOB, SNAPSHOT_GLOB

logger = logging.getLogger(__name__)


class CompressResult(NamedTuple):
    """Summary counters returned by :func:`compress_files`."""

    compressed: int
    skipped: int
    would_unlink: int
    saved_bytes: int


def compress_files(paths: list[Path], dry_run: bool) -> CompressResult:  # noqa: FBT001
    """Gzip each of *paths* to a ``.gz`` sibling, removing the original.

    Cleans up orphaned plain files whose ``.gz`` sibling already exists
    (covers a previously-interrupted run).
    """
    compressed = 0
    skipped = 0
    would_unlink = 0
    saved_bytes = 0

    for path in paths:
        gz_path = path.parent / (path.name + ".gz")
        if gz_path.exists():
            if dry_run:
                would_unlink += 1
            else:
                path.unlink()
            skipped += 1
            continue
        original_size = path.stat().st_size
        if dry_run:
            logger.info("[dry-run] Would compress %s", path.name)
            compressed += 1
            continue
        content = path.read_bytes()
        gz_path.write_bytes(gzip.compress(content))
        compressed_size = gz_path.stat().st_size
        saved_bytes += original_size - compressed_size
        path.unlink()
        compressed += 1

    return CompressResult(compressed, skipped, would_unlink, saved_bytes)


def compress_data_stragglers(*, dry_run: bool) -> dict[str, CompressResult | None]:
    """Detect and compress any leftover uncompressed snapshot/diff archive files.

    Each value is None when that kind had no straggler.
    """
    html_files = sorted(DATA_DIR.glob(SNAPSHOT_GLOB))
    txt_files = sorted(DATA_DIR.glob(DIFF_GLOB))
    return {
        "snapshots": compress_files(html_files, dry_run) if html_files else None,
        "diffs": compress_files(txt_files, dry_run) if txt_files else None,
    }
