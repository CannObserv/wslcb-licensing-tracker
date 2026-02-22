"""Backfill records from archived HTML snapshots into the database.

Parses every archived snapshot and inserts any records not already in the DB.
Duplicates are safely skipped via the UNIQUE constraint in insert_record().
Address validation is deferred to a separate --backfill-addresses pass.
"""
import re
import sys
from pathlib import Path
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from database import DATA_DIR, get_db, init_db, insert_record
from endorsements import process_record, seed_endorsements, discover_code_mappings
from scraper import parse_records_from_table, SECTION_MAP


def extract_snapshot_date(path: Path) -> str | None:
    """Extract date string from snapshot filename (e.g. '2025_12_16')."""
    m = re.search(r'(\d{4}_\d{2}_\d{2})', path.name)
    return m.group(1) if m else None


def backfill_from_snapshots():
    """Parse all archived snapshots and insert records into the database."""
    init_db()

    snapshots = sorted(DATA_DIR.glob("wslcb/licensinginfo/**/*.html"))
    if not snapshots:
        print("No archived snapshots found.")
        return

    print(f"Found {len(snapshots)} snapshot(s) to process")

    total_inserted = 0
    total_skipped = 0

    with get_db() as conn:
        seed_endorsements(conn)

        for snap_path in snapshots:
            snap_date = extract_snapshot_date(snap_path)
            html = snap_path.read_text(encoding="utf-8")
            soup = BeautifulSoup(html, "lxml")

            all_tables = soup.find_all("table")
            snap_inserted = 0
            snap_skipped = 0

            for t in all_tables:
                th = t.find("th")
                if not th:
                    continue
                header = th.get_text(strip=True).replace('\xa0', ' ')
                if header not in SECTION_MAP:
                    continue
                section_type = SECTION_MAP[header]
                records = parse_records_from_table(t, section_type)

                for rec in records:
                    rid = insert_record(conn, rec)
                    if rid is not None:
                        process_record(conn, rid, rec["license_type"], rec["section_type"])
                        snap_inserted += 1
                    else:
                        snap_skipped += 1

            conn.commit()
            total_inserted += snap_inserted
            total_skipped += snap_skipped
            print(f"  {snap_date}: +{snap_inserted} new, {snap_skipped} skipped")

        # Discover any new code→endorsement mappings
        learned = discover_code_mappings(conn)
        if learned:
            print(f"Discovered {len(learned)} new code mapping(s): {list(learned.keys())}")

    print(f"\nDone! Inserted {total_inserted} new records, {total_skipped} duplicates skipped.")


if __name__ == "__main__":
    backfill_from_snapshots()
