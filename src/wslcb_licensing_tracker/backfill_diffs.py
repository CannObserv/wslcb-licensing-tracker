"""Async backfill of records from unified-diff archives into PostgreSQL.

Since #151, each section's archive is ingested by *replaying* its diff chain
(``diff_replay.replay_diff_chain``) instead of parsing each diff's
changed-only line stream. Replay reconstructs the full page state at every
capture, which eliminates the cross-record "bleed" hybrids the two-pass
parser produced, and recovers records the changed-only stream loses.

Each record is stamped with the diff file that evidenced it (its ``entry`` /
``exit`` / ``mutation`` / boundary-state file), so provenance stays
per-file even though parsing is per-chain.

Safe to re-run — duplicates are detected by the UNIQUE constraint and
skipped. Address validation is deferred; run ``wslcb backfill-addresses``
afterward.
"""

import logging
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine

from .db import DATA_DIR, SOURCE_TYPE_CO_ARCHIVE, WSLCB_SOURCE_URL, get_or_create_source
from .diff_replay import ReplayResult, replay_diff_chain
from .engine import get_db
from .parser import SECTION_DIR_MAP, glob_with_gz
from .pipeline import IngestOptions, ingest_batch

logger = logging.getLogger(__name__)


def diff_section_dirs(data_dir: Path, section: str | None = None) -> list[tuple[str, Path]]:
    """Return (section_type, dir_path) pairs for diff archives to process."""
    diffs_root = data_dir / "wslcb" / "licensinginfo-diffs"
    results = []
    for dir_name, section_type in SECTION_DIR_MAP.items():
        if section and dir_name != section:
            continue
        section_dir = diffs_root / dir_name
        if section_dir.exists():
            results.append((section_type, section_dir))
    return results


def _infer_section_type(filename: str) -> str | None:
    """Infer the section type from a diff filename.

    Archive files embed their section directory name, e.g.
    ``2022_09_07-00_15_00-approvals-diff.txt.gz``. Matching is by substring
    in ``SECTION_DIR_MAP`` iteration order — archive names carry exactly one
    section token, so precedence never matters in practice.
    """
    for dir_name, section_type in SECTION_DIR_MAP.items():
        if dir_name in filename:
            return section_type
    return None


def _build_work(
    section: str | None,
    single_file: str | None,
    limit: int | None,
) -> tuple[list[tuple[str, list[Path]]], int]:
    """Resolve (section_type, chain files) work items.

    Returns ``(work, errors)`` — *errors* counts files that could not be
    assigned a section (single-file mode with an unrecognizable name).
    """
    if single_file:
        path = Path(single_file)
        section_type = _infer_section_type(path.name)
        if section_type is None:
            logger.error("Cannot infer section from diff filename: %s", path.name)
            return [], 1
        return [(section_type, [path])], 0

    work: list[tuple[str, list[Path]]] = []
    remaining = limit
    for section_type, section_dir in diff_section_dirs(DATA_DIR, section):
        files = glob_with_gz(section_dir, "*.txt")
        if remaining is not None:
            files = files[:remaining]
            remaining -= len(files)
        if files:
            work.append((section_type, files))
        if remaining == 0:
            break
    return work, 0


async def _ingest_replay_result(
    engine: AsyncEngine,
    result: ReplayResult,
    files: list[Path],
    totals: dict[str, int],
) -> None:
    """Ingest one section's replayed records, grouped by evidencing file."""
    path_by_name = {p.name: p for p in files}
    by_file: dict[str, list[dict]] = {}
    for rec in result.records:
        by_file.setdefault(rec["source_file"], []).append(rec)

    async with get_db(engine) as conn:
        for fname in sorted(by_file):
            path = path_by_name.get(fname)
            if path is None:  # cannot happen: source_file comes from *files*
                logger.error("Replay record references unknown file %s", fname)
                totals["errors"] += len(by_file[fname])
                continue
            try:
                rel_path = str(path.relative_to(DATA_DIR))
            except ValueError:
                rel_path = str(path)
            source_id = await get_or_create_source(
                conn,
                SOURCE_TYPE_CO_ARCHIVE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=None,
            )
            opts = IngestOptions(link_outcomes=False, source_id=source_id)
            batch_result = await ingest_batch(conn, by_file[fname], opts)
            await conn.commit()
            totals["inserted"] += batch_result.inserted
            totals["skipped"] += batch_result.skipped
            totals["errors"] += batch_result.errors
            logger.debug(
                "%s: inserted=%d skipped=%d",
                fname,
                batch_result.inserted,
                batch_result.skipped,
            )


async def backfill_diffs(
    engine: AsyncEngine,
    *,
    section: str | None = None,
    single_file: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest records from CO diff archives into PostgreSQL via chain replay.

    Args:
        engine: AsyncEngine connected to the PostgreSQL database.
        section: If set, only process diffs for this section (e.g. "notifications").
        single_file: If set, replay only this diff file (section inferred
            from its name).
        limit: If set, replay at most N diff files (truncating the chain).
        dry_run: If True, replay but do not write to the database.

    Returns a dict with ``inserted``, ``skipped``, ``errors``, ``files_processed``.
    """
    totals: dict[str, int] = {"inserted": 0, "skipped": 0, "errors": 0, "files_processed": 0}

    work, unassigned = _build_work(section, single_file, limit)
    totals["errors"] += unassigned

    for section_type, files in work:
        result = replay_diff_chain(files, section_type)
        totals["files_processed"] += len(files) - result.stats["read_errors"]
        totals["errors"] += result.stats["read_errors"]

        if dry_run:
            totals["inserted"] += len(result.records)
            continue

        await _ingest_replay_result(engine, result, files, totals)

    logger.info(
        "Diff backfill complete: files=%d inserted=%d skipped=%d errors=%d",
        totals["files_processed"],
        totals["inserted"],
        totals["skipped"],
        totals["errors"],
    )
    return totals
