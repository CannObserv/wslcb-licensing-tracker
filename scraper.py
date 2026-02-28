"""Scraper for WSLCB licensing activity page.

Fetches the live WSLCB licensing page, archives the HTML, parses records,
and inserts them into the database with endorsement processing, address
validation, and application→outcome linking.
"""
import logging
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from database import (
    DATA_DIR, get_db, init_db, get_or_create_source, link_record_source,
    SOURCE_TYPE_LIVE_SCRAPE, WSLCB_SOURCE_URL,
)
from parser import SECTION_MAP, parse_records_from_table
from queries import insert_record
from endorsements import process_record, seed_endorsements, discover_code_mappings, repair_code_name_endorsements
from address_validator import validate_record, validate_previous_location, TIMEOUT as _AV_TIMEOUT
from link_records import link_new_record
from log_config import setup_logging

logger = logging.getLogger(__name__)

URL = WSLCB_SOURCE_URL


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

    logger.info("Starting scrape of %s", URL)

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
            resp = httpx.get(URL, timeout=120, follow_redirects=True)
            resp.raise_for_status()
            html = resp.text
            logger.debug("Fetched %s bytes", f"{len(html):,}")

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
                url=URL,
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

                    inserted = 0
                    for rec in records:
                        result = insert_record(conn, rec)
                        if result is None:
                            counts["skipped"] += 1
                            continue
                        rid, is_new = result
                        if is_new:
                            process_record(conn, rid, rec["license_type"])
                            link_record_source(conn, rid, source_id, "first_seen")
                            link_new_record(conn, rid)
                            validate_record(conn, rid, client=av_client)
                            if rec.get("previous_business_location"):
                                validate_previous_location(conn, rid, client=av_client)
                            inserted += 1
                        else:
                            link_record_source(
                                conn, rid, source_id, "confirmed",
                            )
                            counts["skipped"] += 1

                    key = section_type.split("_")[0] if "_" in section_type else section_type
                    if key == "new":
                        counts["new"] = inserted
                    elif key == "approved":
                        counts["approved"] = inserted
                    elif key == "discontinued":
                        counts["discontinued"] = inserted

                    conn.commit()

            # Update log
            conn.execute(
                """UPDATE scrape_log SET
                   finished_at = ?, status = 'success',
                   records_new = ?, records_approved = ?,
                   records_discontinued = ?, records_skipped = ?,
                   snapshot_path = ?
                   WHERE id = ?""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    counts["new"],
                    counts["approved"],
                    counts["discontinued"],
                    counts["skipped"],
                    str(snapshot_path.relative_to(DATA_DIR)) if snapshot_path else None,
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
