"""Backfill records from archived HTML snapshots into the database.

Single-pass over each snapshot, two-phase processing:
  1. **Ingest** — INSERT new records (dupes skipped).
  2. **Repair** — fix broken records from pre-fix scrapes:
     - ASSUMPTION records with empty business names
     - CHANGE OF LOCATION records with missing locations

Safe to re-run at any time.  Address validation is deferred to a
separate ``scraper.py --backfill-addresses`` pass.
"""
import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup

from database import DATA_DIR, get_db, init_db, get_or_create_location
from queries import insert_record
from entities import (
    parse_and_link_entities, clean_applicants_string, clean_entity_name,
)
from endorsements import process_record, seed_endorsements, discover_code_mappings, repair_code_name_endorsements
from log_config import setup_logging
from scraper import parse_records_from_table, SECTION_MAP

logger = logging.getLogger(__name__)


def _snapshot_paths() -> list[Path]:
    """Return all archived snapshot paths, sorted chronologically."""
    return sorted(DATA_DIR.glob("wslcb/licensinginfo/**/*.html"))


def _extract_snapshot_date(path: Path) -> str | None:
    """Extract date string from snapshot filename (e.g. '2025_12_16')."""
    m = re.search(r'(\d{4}_\d{2}_\d{2})', path.name)
    return m.group(1) if m else None


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


# ── Phase 1: Ingest ──────────────────────────────────────────────────

def _ingest_records(conn, records: list[dict]) -> tuple[int, int]:
    """Insert new records into the database.  Returns (inserted, skipped)."""
    inserted = 0
    skipped = 0
    for rec in records:
        rid = insert_record(conn, rec)
        if rid is not None:
            process_record(conn, rid, rec["license_type"])
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped


# ── Phase 2: Repair ──────────────────────────────────────────────────

def _repair_assumptions(conn, records: list[dict]) -> int:
    """Fix ASSUMPTION records that have empty or NULL business names.

    Normalizes business names and applicant strings (uppercase, strip
    trailing punctuation) before writing.  After updating, re-links
    entities so the ``record_entities`` junction table reflects the
    corrected data.
    """
    updated = 0
    for rec in records:
        if rec["application_type"] != "ASSUMPTION":
            continue
        if not rec["business_name"] and not rec["previous_business_name"]:
            continue
        cleaned_biz = clean_entity_name(rec["business_name"] or "")
        cleaned_prev_biz = clean_entity_name(
            rec["previous_business_name"] or ""
        )
        cleaned_applicants = clean_applicants_string(rec["applicants"])
        cleaned_prev_applicants = clean_applicants_string(
            rec["previous_applicants"]
        )
        cursor = conn.execute(
            """UPDATE license_records
               SET business_name = ?,
                   applicants = ?,
                   previous_business_name = ?,
                   previous_applicants = ?
               WHERE section_type = ?
                 AND record_date = ?
                 AND license_number = ?
                 AND application_type = 'ASSUMPTION'
                 AND (business_name = '' OR business_name IS NULL)""",
            (
                cleaned_biz,
                cleaned_applicants,
                cleaned_prev_biz,
                cleaned_prev_applicants,
                rec["section_type"],
                rec["record_date"],
                rec["license_number"],
            ),
        )
        if cursor.rowcount > 0:
            # Find the actual record id to re-link entities
            row = conn.execute(
                """SELECT id FROM license_records
                   WHERE section_type = ?
                     AND record_date = ?
                     AND license_number = ?
                     AND application_type = 'ASSUMPTION'""",
                (rec["section_type"], rec["record_date"], rec["license_number"]),
            ).fetchone()
            if row:
                rid = row["id"]
                # Clear stale entity links and re-create from updated data
                conn.execute(
                    "DELETE FROM record_entities WHERE record_id = ?", (rid,)
                )
                parse_and_link_entities(
                    conn, rid, cleaned_applicants, "applicant"
                )
                if cleaned_prev_applicants:
                    parse_and_link_entities(
                        conn, rid, cleaned_prev_applicants,
                        "previous_applicant",
                    )
            updated += cursor.rowcount
    return updated


def _repair_change_of_location(conn, records: list[dict]) -> int:
    """Fix CHANGE OF LOCATION records with missing locations."""
    updated = 0
    for rec in records:
        if rec["application_type"] != "CHANGE OF LOCATION":
            continue
        if not rec["business_location"]:
            continue

        loc_id = get_or_create_location(
            conn, rec["business_location"],
            city=rec["city"], state=rec["state"],
            zip_code=rec["zip_code"],
        )
        prev_loc_id = get_or_create_location(
            conn, rec["previous_business_location"],
            city=rec["previous_city"],
            state=rec["previous_state"],
            zip_code=rec["previous_zip_code"],
        )

        # If a correct record already exists, delete the broken one
        # instead of updating (avoids UNIQUE constraint violation).
        existing = conn.execute(
            """SELECT 1 FROM license_records
               WHERE section_type = ?
                 AND record_date = ?
                 AND license_number = ?
                 AND application_type = 'CHANGE OF LOCATION'""",
            (rec["section_type"], rec["record_date"], rec["license_number"]),
        ).fetchone()
        if existing:
            conn.execute(
                """DELETE FROM license_records
                   WHERE section_type = ?
                     AND record_date = ?
                     AND license_number = ?
                     AND (application_type = '' OR application_type IS NULL)""",
                (rec["section_type"], rec["record_date"], rec["license_number"]),
            )
        else:
            cursor = conn.execute(
                """UPDATE license_records
                   SET location_id = ?,
                       previous_location_id = ?,
                       application_type = 'CHANGE OF LOCATION'
                   WHERE section_type = ?
                     AND record_date = ?
                     AND license_number = ?
                     AND location_id IS NULL
                     AND (application_type = '' OR application_type IS NULL)""",
                (
                    loc_id,
                    prev_loc_id,
                    rec["section_type"],
                    rec["record_date"],
                    rec["license_number"],
                ),
            )
            if cursor.rowcount > 0:
                updated += cursor.rowcount
                continue

        # Backfill previous_location_id on records that have a location
        # but are missing the previous address.  Skip when the snapshot
        # doesn't supply one either (prev_loc_id is None) — there's
        # nothing to repair.
        if prev_loc_id is not None:
            cursor = conn.execute(
                """UPDATE license_records
                   SET previous_location_id = ?
                   WHERE section_type = ?
                     AND record_date = ?
                     AND license_number = ?
                     AND application_type = 'CHANGE OF LOCATION'
                     AND previous_location_id IS NULL""",
                (
                    prev_loc_id,
                    rec["section_type"],
                    rec["record_date"],
                    rec["license_number"],
                ),
            )
            updated += cursor.rowcount
    return updated


# ── Entry point ──────────────────────────────────────────────────────

def backfill_from_snapshots():
    """Ingest records from archived snapshots, then repair broken records."""
    init_db()

    snapshots = _snapshot_paths()
    if not snapshots:
        logger.info("No archived snapshots found.")
        return

    logger.info("Found %d snapshot(s) to process", len(snapshots))

    total_inserted = 0
    total_skipped = 0
    assumption_fixed = 0
    col_fixed = 0

    with get_db() as conn:
        seed_endorsements(conn)
        repair_code_name_endorsements(conn)

        for snap_path in snapshots:
            snap_date = _extract_snapshot_date(snap_path)
            records = _parse_snapshot(snap_path)

            # Phase 1: insert new records
            inserted, skipped = _ingest_records(conn, records)
            conn.commit()

            # Phase 2: repair broken records
            assumption_fixed += _repair_assumptions(conn, records)
            col_fixed += _repair_change_of_location(conn, records)
            conn.commit()

            total_inserted += inserted
            total_skipped += skipped
            logger.debug("  %s: +%d new, %d skipped", snap_date, inserted, skipped)

        # Discover any new code→endorsement mappings
        learned = discover_code_mappings(conn)
        if learned:
            logger.info("Discovered %d new code mapping(s): %s", len(learned), list(learned.keys()))

    logger.info("Done! Inserted %d new records (%d duplicates skipped).", total_inserted, total_skipped)
    if assumption_fixed or col_fixed:
        logger.info("Repaired %d ASSUMPTION + %d CHANGE OF LOCATION record(s).", assumption_fixed, col_fixed)


if __name__ == "__main__":
    setup_logging()
    backfill_from_snapshots()
