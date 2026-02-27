"""One-time backfill: populate source provenance for existing records.

Creates ``sources`` rows from ``scrape_log`` entries and CO archive
snapshots, then re-processes every snapshot to build ``record_sources``
junction links.  Safe to re-run (all operations are idempotent).

Usage::

    python backfill_provenance.py
"""
import logging
import re
from pathlib import Path

from database import (
    DATA_DIR, get_db, init_db,
    get_or_create_source, link_record_source,
    SOURCE_TYPE_LIVE_SCRAPE, SOURCE_TYPE_CO_ARCHIVE,
    SOURCE_TYPE_CO_DIFF_ARCHIVE, WSLCB_SOURCE_URL,
)
from scraper import URL
from backfill_snapshots import _parse_snapshot
from backfill_diffs import _discover_diff_files, _parse_diff_timestamp
from log_config import setup_logging

logger = logging.getLogger(__name__)


def _find_record_id(conn, rec: dict) -> int | None:
    """Look up an existing record by its unique key.  Returns id or None."""
    row = conn.execute(
        """SELECT id FROM license_records
           WHERE section_type = :section_type
             AND record_date = :record_date
             AND license_number = :license_number
             AND application_type = :application_type""",
        rec,
    ).fetchone()
    return row["id"] if row else None


def _link_snapshot_records(
    conn, snap_path: Path, source_id: int,
) -> tuple[int, int]:
    """Parse a snapshot and link all its records to the given source.

    Returns (linked, missed) counts.
    """
    try:
        records = _parse_snapshot(snap_path)
    except Exception as e:
        logger.warning("Failed to parse %s: %s", snap_path.name, e)
        return 0, 0

    linked = 0
    missed = 0
    for rec in records:
        rid = _find_record_id(conn, rec)
        if rid is not None:
            # Use 'confirmed' — we're retroactively linking, not
            # establishing first-seen (the record predates tracking).
            link_record_source(conn, rid, source_id, "confirmed")
            linked += 1
        else:
            missed += 1
    return linked, missed


def backfill_provenance():
    """Populate source provenance for all existing records."""
    init_db()

    with get_db() as conn:
        # ── Phase 1: scrape_log → sources (live scrapes) ──────────
        log_rows = conn.execute(
            """SELECT id, started_at, snapshot_path
               FROM scrape_log ORDER BY id"""
        ).fetchall()
        logger.info(
            "Phase 1: processing %d scrape_log entries", len(log_rows),
        )

        for row in log_rows:
            snap_rel = row["snapshot_path"]
            source_id = get_or_create_source(
                conn,
                SOURCE_TYPE_LIVE_SCRAPE,
                snapshot_path=snap_rel,
                url=URL,
                captured_at=row["started_at"],
                scrape_log_id=row["id"],
            )
            if snap_rel:
                snap_abs = DATA_DIR / snap_rel
                if snap_abs.exists():
                    linked, missed = _link_snapshot_records(
                        conn, snap_abs, source_id,
                    )
                    logger.debug(
                        "  scrape_log %d (%s): linked=%d missed=%d",
                        row["id"], snap_rel, linked, missed,
                    )
                else:
                    logger.warning(
                        "  scrape_log %d: snapshot not found: %s",
                        row["id"], snap_abs,
                    )
            conn.commit()

        # ── Phase 2: CO archive snapshots ────────────────────────
        archive_dir = DATA_DIR / "wslcb" / "licensinginfo"
        co_snapshots = sorted(archive_dir.glob("**/*.html"))
        # Exclude snapshots already handled via scrape_log to avoid
        # re-registering them under a different source type.
        live_paths = {
            row["snapshot_path"]
            for row in log_rows
            if row["snapshot_path"]
        }
        co_snapshots = [
            p for p in co_snapshots
            if str(p.relative_to(DATA_DIR)) not in live_paths
        ]
        logger.info(
            "Phase 2: processing %d CO archive snapshots", len(co_snapshots),
        )

        total_linked = 0
        total_missed = 0
        for snap_path in co_snapshots:
            rel_path = str(snap_path.relative_to(DATA_DIR))
            # Extract date from filename for captured_at
            m = re.search(r'(\d{4})_(\d{2})_(\d{2})', snap_path.name)
            captured_at = (
                f"{m.group(1)}-{m.group(2)}-{m.group(3)}T00:00:00+00:00"
                if m else None
            )
            source_id = get_or_create_source(
                conn,
                SOURCE_TYPE_CO_ARCHIVE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=captured_at,
            )
            linked, missed = _link_snapshot_records(
                conn, snap_path, source_id,
            )
            total_linked += linked
            total_missed += missed
            if linked:
                logger.debug(
                    "  %s: linked=%d missed=%d", rel_path, linked, missed,
                )
            conn.commit()

        logger.info(
            "Phase 2 done: linked %d records (%d unmatched)",
            total_linked, total_missed,
        )

        # ── Phase 3: CO diff archives ──────────────────────────
        # Strategy: instead of re-parsing 4K+ diff files (slow — some
        # are 45 MB), match orphan records to diff files via scraped_at
        # timestamps.  Each diff has --- (old) and +++ (new) header
        # timestamps that backfill_diffs.py used as scraped_at.
        diff_files = _discover_diff_files()
        logger.info(
            "Phase 3: processing %d CO diff archive files",
            len(diff_files),
        )

        # Build ts → [(source_id, path)] mapping from diff headers
        ts_to_source: dict[str, int] = {}
        for diff_path, section_type in diff_files:
            try:
                old_ts = new_ts = None
                with open(diff_path, encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("--- "):
                            old_ts = _parse_diff_timestamp(line.rstrip("\n"))
                        elif line.startswith("+++ "):
                            new_ts = _parse_diff_timestamp(line.rstrip("\n"))
                            break
            except Exception as e:
                logger.warning("Failed to read %s: %s", diff_path.name, e)
                continue

            rel_path = str(diff_path.relative_to(DATA_DIR))
            source_id = get_or_create_source(
                conn,
                SOURCE_TYPE_CO_DIFF_ARCHIVE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=new_ts,
            )
            # Map both timestamps to this source
            if old_ts:
                ts_to_source.setdefault(old_ts, source_id)
            if new_ts:
                ts_to_source.setdefault(new_ts, source_id)

        conn.commit()
        logger.info(
            "  Registered %d diff sources, %d unique timestamps",
            len(diff_files), len(ts_to_source),
        )

        # Find orphan records and link them via scraped_at
        orphans = conn.execute(
            """SELECT id, scraped_at FROM license_records
               WHERE id NOT IN (SELECT record_id FROM record_sources)"""
        ).fetchall()
        logger.info("  Matching %d orphan records to diff sources", len(orphans))

        diff_linked = 0
        diff_missed = 0
        batch = 0
        for row in orphans:
            source_id = ts_to_source.get(row["scraped_at"])
            if source_id is not None:
                link_record_source(conn, row["id"], source_id, "confirmed")
                diff_linked += 1
            else:
                diff_missed += 1
            batch += 1
            if batch % 10000 == 0:
                conn.commit()
                logger.debug("    ...%d/%d", batch, len(orphans))
        conn.commit()

        logger.info(
            "Phase 3 done: linked %d records from diffs (%d unmatched)",
            diff_linked, diff_missed,
        )

        # ── Summary ──────────────────────────────────────────────
        stats = conn.execute(
            """SELECT
                 COUNT(DISTINCT rs.record_id) AS records_with_source,
                 COUNT(*) AS total_links,
                 (SELECT COUNT(*) FROM license_records) AS total_records,
                 (SELECT COUNT(*) FROM sources) AS total_sources
               FROM record_sources rs"""
        ).fetchone()
        logger.info(
            "Summary: %d/%d records have provenance (%d links across %d sources)",
            stats["records_with_source"],
            stats["total_records"],
            stats["total_links"],
            stats["total_sources"],
        )
        orphan_count = conn.execute(
            """SELECT COUNT(*) FROM license_records
               WHERE id NOT IN (SELECT record_id FROM record_sources)"""
        ).fetchone()[0]
        if orphan_count:
            logger.warning(
                "%d records have no source attribution", orphan_count,
            )


if __name__ == "__main__":
    setup_logging()
    backfill_provenance()
