"""Scraper for WSLCB licensing activity page.

Fetches the live WSLCB licensing page, archives the HTML, parses records,
and inserts them into the database via the unified ingestion pipeline.

When the page content hasn't changed since the last successful scrape
(detected via SHA-256 hash comparison), the scrape is short-circuited
and logged as ``status='unchanged'``.
"""
import hashlib
import logging
import sqlite3
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from database import (
    DATA_DIR, get_db, init_db, get_or_create_source,
    SOURCE_TYPE_LIVE_SCRAPE, WSLCB_SOURCE_URL,
)
from parser import SECTION_MAP, parse_records_from_table
from endorsements import seed_endorsements, discover_code_mappings, repair_code_name_endorsements
from address_validator import TIMEOUT as _AV_TIMEOUT
from pipeline import ingest_batch, IngestOptions

logger = logging.getLogger(__name__)


def compute_content_hash(html: str) -> str:
    """Return the SHA-256 hex digest of *html* (UTF-8 encoded)."""
    return hashlib.sha256(html.encode("utf-8")).hexdigest()


def get_last_content_hash(conn: sqlite3.Connection) -> str | None:
    """Return the content_hash from the most recent successful or unchanged scrape.

    Returns ``None`` if there are no prior scrapes or none have a hash
    (legacy rows pre-dating the content_hash column).
    """
    row = conn.execute(
        "SELECT content_hash FROM scrape_log "
        "WHERE status IN ('success', 'unchanged') "
        "  AND content_hash IS NOT NULL "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def save_html_snapshot(html: str, scrape_date: datetime) -> Path:
    """Save raw HTML to data/wslcb/licensinginfo/[yyyy]/[yyyy_mm_dd]/[yyyy_mm_dd]-licensinginfo.lcb.wa.gov-v[x].html

    Saves the HTML exactly as received from the server (no transformation).
    Increments the version number if a snapshot for the same date already exists.
    Returns the path to the saved file.
    """
    date_str = scrape_date.strftime("%Y_%m_%d")
    year_str = scrape_date.strftime("%Y")
    date_dir = DATA_DIR / "wslcb" / "licensinginfo" / year_str / date_str

    # Determine next version number for this date
    version = 1
    while list(date_dir.glob(f"{date_str}-licensinginfo.lcb.wa.gov-v{version}.html")):
        version += 1

    date_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{date_str}-licensinginfo.lcb.wa.gov-v{version}.html"
    filepath = date_dir / filename
    filepath.write_text(html, encoding="utf-8")
    return filepath


def scrape():
    """Main scrape function."""
    init_db()

    logger.info("Starting scrape of %s", WSLCB_SOURCE_URL)

    with get_db() as conn:
        # Ensure seed code→endorsement mappings exist (idempotent; needed
        # because the scraper runs standalone, not through FastAPI lifespan).
        seed_endorsements(conn)
        repair_code_name_endorsements(conn)

        # Log the scrape start
        cursor = conn.execute(
            "INSERT INTO scrape_log (started_at, status) VALUES (?, 'running')",
            (datetime.now(timezone.utc).isoformat(),),
        )
        log_id = cursor.lastrowid
        conn.commit()

        try:
            # Fetch page
            logger.debug("Fetching page...")
            resp = httpx.get(WSLCB_SOURCE_URL, timeout=120, follow_redirects=True)
            resp.raise_for_status()
            html = resp.text
            logger.debug("Fetched %s bytes", f"{len(html):,}")

            # Compute content hash and check for unchanged page
            content_hash = compute_content_hash(html)
            last_hash = get_last_content_hash(conn)

            if content_hash == last_hash:
                logger.info(
                    "Page unchanged (hash %s…); skipping parse/ingest",
                    content_hash[:12],
                )
                conn.execute(
                    """UPDATE scrape_log SET
                       finished_at = ?, status = 'unchanged',
                       content_hash = ?
                       WHERE id = ?""",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        content_hash,
                        log_id,
                    ),
                )
                conn.commit()
                return

            # Archive the raw HTML
            scrape_time = datetime.now(timezone.utc)
            snapshot_path = None
            try:
                snapshot_path = save_html_snapshot(html, scrape_time)
                logger.debug("Saved snapshot to %s", snapshot_path)
            except Exception as snap_err:
                logger.warning("Failed to save HTML snapshot: %s", snap_err)

            # Register provenance source
            rel_path = (
                str(snapshot_path.relative_to(DATA_DIR))
                if snapshot_path else None
            )
            source_id = get_or_create_source(
                conn,
                SOURCE_TYPE_LIVE_SCRAPE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=scrape_time.isoformat(),
                scrape_log_id=log_id,
            )

            # Parse HTML
            soup = BeautifulSoup(html, "lxml")

            # Find the 3 data tables (skip header/contact tables)
            all_tables = soup.find_all("table")
            data_tables = []
            for t in all_tables:
                th = t.find("th")
                if th and th.get_text(strip=True).replace('\xa0', ' ') in SECTION_MAP:
                    header_text = th.get_text(strip=True).replace('\xa0', ' ')
                    data_tables.append((SECTION_MAP[header_text], t))

            if not data_tables:
                raise ValueError("Could not find data tables in page")

            logger.debug("Found %d data sections", len(data_tables))

            counts = {"new": 0, "approved": 0, "discontinued": 0, "skipped": 0}

            with httpx.Client(timeout=_AV_TIMEOUT) as av_client:
                for section_type, table in data_tables:
                    records = parse_records_from_table(table, section_type)
                    logger.debug("  %s: parsed %d records", section_type, len(records))

                    opts = IngestOptions(
                        validate_addresses=True,
                        link_outcomes=True,
                        source_id=source_id,
                        av_client=av_client,
                    )
                    batch_result = ingest_batch(conn, records, opts)

                    key = section_type.split("_")[0] if "_" in section_type else section_type
                    if key == "new":
                        counts["new"] = batch_result.inserted
                    elif key == "approved":
                        counts["approved"] = batch_result.inserted
                    elif key == "discontinued":
                        counts["discontinued"] = batch_result.inserted
                    counts["skipped"] += batch_result.skipped

            # Update log
            conn.execute(
                """UPDATE scrape_log SET
                   finished_at = ?, status = 'success',
                   records_new = ?, records_approved = ?,
                   records_discontinued = ?, records_skipped = ?,
                   snapshot_path = ?, content_hash = ?
                   WHERE id = ?""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    counts["new"],
                    counts["approved"],
                    counts["discontinued"],
                    counts["skipped"],
                    str(snapshot_path.relative_to(DATA_DIR)) if snapshot_path else None,
                    content_hash,
                    log_id,
                ),
            )
            conn.commit()

            total = counts["new"] + counts["approved"] + counts["discontinued"]
            logger.info(
                "Done! Inserted %d new records "
                "(new=%d, approved=%d, discontinued=%d, skipped=%d)",
                total, counts["new"], counts["approved"],
                counts["discontinued"], counts["skipped"],
            )

            # Discover any new code→endorsement mappings from cross-references
            learned = discover_code_mappings(conn)
            if learned:
                logger.info("Discovered %d new code mapping(s): %s", len(learned), list(learned.keys()))

        except Exception as e:
            conn.execute(
                "UPDATE scrape_log SET finished_at = ?, status = 'error', error_message = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), str(e), log_id),
            )
            conn.commit()
            logger.error("Scrape failed: %s", e)
            raise


def cleanup_redundant_scrapes(
    conn: sqlite3.Connection,
    *,
    delete_files: bool = True,
) -> dict:
    """Remove data generated by scrapes that found zero new records.

    These scrapes produced only ``confirmed`` provenance rows and
    duplicate snapshot files that add no archival value.

    Steps:
      1. Identify scrape_log entries with ``status='success'`` and
         zero new+approved+discontinued records.
      2. Delete ``record_sources`` rows linked to those sources
         (all should be ``role='confirmed'``).
      3. Delete the ``sources`` rows themselves.
      4. Retro-stamp those scrape_log rows as ``status='unchanged'``.
      5. Optionally delete the duplicate snapshot files from disk.

    Returns a dict with counts of cleaned items.
    """
    # Find redundant scrape log IDs
    redundant = conn.execute(
        "SELECT id, snapshot_path FROM scrape_log "
        "WHERE status = 'success' "
        "  AND (records_new + records_approved + records_discontinued) = 0"
    ).fetchall()

    if not redundant:
        logger.info("No redundant scrapes found")
        return {"scrape_logs": 0, "sources": 0, "record_sources": 0, "files": 0}

    log_ids = [r[0] for r in redundant]
    snapshot_paths = [r[1] for r in redundant if r[1]]

    # Find the source IDs tied to these scrape_log entries
    placeholders = ",".join("?" * len(log_ids))
    source_rows = conn.execute(
        f"SELECT id FROM sources WHERE scrape_log_id IN ({placeholders})",
        log_ids,
    ).fetchall()
    source_ids = [r[0] for r in source_rows]

    rs_deleted = 0
    if source_ids:
        s_placeholders = ",".join("?" * len(source_ids))
        # Delete record_sources (confirmed provenance noise)
        cur = conn.execute(
            f"DELETE FROM record_sources WHERE source_id IN ({s_placeholders})",
            source_ids,
        )
        rs_deleted = cur.rowcount

        # Delete the sources rows
        conn.execute(
            f"DELETE FROM sources WHERE id IN ({s_placeholders})",
            source_ids,
        )

    # Re-stamp scrape_log rows as 'unchanged'
    conn.execute(
        f"UPDATE scrape_log SET status = 'unchanged' "
        f"WHERE id IN ({placeholders})",
        log_ids,
    )
    conn.commit()

    # Delete duplicate snapshot files
    files_deleted = 0
    if delete_files:
        for rel_path in snapshot_paths:
            filepath = DATA_DIR / rel_path
            if filepath.exists():
                filepath.unlink()
                files_deleted += 1
                logger.debug("Deleted %s", filepath)

    result = {
        "scrape_logs": len(log_ids),
        "sources": len(source_ids),
        "record_sources": rs_deleted,
        "files": files_deleted,
    }
    logger.info(
        "Cleanup complete: %d scrape logs re-stamped, %d sources removed, "
        "%d record_sources removed, %d files deleted",
        result["scrape_logs"], result["sources"],
        result["record_sources"], result["files"],
    )
    return result
