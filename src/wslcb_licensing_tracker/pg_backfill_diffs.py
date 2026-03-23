"""Async backfill of records from unified-diff archives into PostgreSQL.

Async port of backfill_diffs.py. The two-pass diff parsing and CSV export
logic are pure Python and re-used from backfill_diffs unchanged.

Safe to re-run — duplicates are detected by the UNIQUE constraint and skipped.
Address validation is deferred; run ``wslcb backfill-addresses`` afterward.
"""

import logging
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine

from .database import get_db
from .parser import SECTION_DIR_MAP, extract_records_from_diff
from .pg_db import DATA_DIR, SOURCE_TYPE_CO_ARCHIVE, WSLCB_SOURCE_URL, get_or_create_source
from .pg_pipeline import IngestOptions, ingest_batch

logger = logging.getLogger(__name__)


def _diff_section_dirs(data_dir: Path, section: str | None = None) -> list[tuple[str, Path]]:
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


async def backfill_diffs(
    engine: AsyncEngine,
    *,
    section: str | None = None,
    single_file: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest records from CO diff archives into PostgreSQL.

    Args:
        engine: AsyncEngine connected to the PostgreSQL database.
        section: If set, only process diffs for this section (e.g. "notifications").
        single_file: If set, process only this single diff file path.
        limit: If set, process at most N diff files.
        dry_run: If True, parse but do not write to the database.

    Returns a dict with ``inserted``, ``skipped``, ``errors``, ``files_processed``.
    """
    totals: dict[str, int] = {"inserted": 0, "skipped": 0, "errors": 0, "files_processed": 0}

    if single_file:
        diff_files: list[tuple[str | None, Path]] = [(None, Path(single_file))]
    else:
        diff_files = []
        for section_type, section_dir in _diff_section_dirs(DATA_DIR, section):
            for f in sorted(section_dir.glob("*.txt")):
                diff_files.append((section_type, f))

    if limit:
        diff_files = diff_files[:limit]

    if dry_run:
        for section_type, diff_path in diff_files:
            try:
                records = extract_records_from_diff(diff_path, section_type)
                totals["inserted"] += len(records)
                totals["files_processed"] += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error parsing %s: %s", diff_path, exc)
                totals["errors"] += 1
        return totals

    async with get_db(engine) as conn:
        for section_type, diff_path in diff_files:
            try:
                records = extract_records_from_diff(diff_path, section_type)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error parsing %s: %s", diff_path, exc)
                totals["errors"] += 1
                continue

            rel_path = str(diff_path.relative_to(DATA_DIR))
            source_id = await get_or_create_source(
                conn,
                SOURCE_TYPE_CO_ARCHIVE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=None,
            )

            opts = IngestOptions(
                link_outcomes=False,
                source_id=source_id,
            )
            batch_result = await ingest_batch(conn, records, opts)
            await conn.commit()

            totals["inserted"] += batch_result.inserted
            totals["skipped"] += batch_result.skipped
            totals["errors"] += batch_result.errors
            totals["files_processed"] += 1

            logger.debug(
                "%s: inserted=%d skipped=%d",
                diff_path.name,
                batch_result.inserted,
                batch_result.skipped,
            )

    logger.info(
        "Diff backfill complete: files=%d inserted=%d skipped=%d errors=%d",
        totals["files_processed"],
        totals["inserted"],
        totals["skipped"],
        totals["errors"],
    )
    return totals
