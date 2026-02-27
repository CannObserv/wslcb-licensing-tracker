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

from bs4 import BeautifulSoup

from database import (
    DATA_DIR, get_db, init_db,
    get_or_create_source, link_record_source,
    SOURCE_TYPE_LIVE_SCRAPE, SOURCE_TYPE_CO_ARCHIVE,
    WSLCB_SOURCE_URL,
)
from scraper import parse_records_from_table, SECTION_MAP, URL
from log_config import setup_logging

logger = logging.getLogger(__name__)


def _parse_snapshot(path: Path) -> list[dict]:
    """Parse a snapshot file and return a list of record dicts."""
    html = path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "lxml")
    records = []
    for table in soup.find_all("table"):
        th = table.find("th")
        if not th:
            continue
        header = th.get_text(strip=True).replace('\xa0', ' ')
        if header not in SECTION_MAP:
            continue
        section_type = SECTION_MAP[header]
        records.extend(parse_records_from_table(table, section_type))
    return records


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
                f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None
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
