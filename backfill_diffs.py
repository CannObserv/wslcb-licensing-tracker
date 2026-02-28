"""Backfill historical records from unified-diff archives.

Parses diff files in ``data/wslcb/licensinginfo-diffs/{notifications,approvals,discontinued}/``
and inserts recovered records into the database via the standard ``insert_record()``
pipeline.  Both added (+) and removed (-) lines are harvested — removals represent
records that aged off the WSLCB rolling window and are equally valid historical data.

Two-pass extraction handles the diff boundary problem:
  1. **Primary (no-context):** parse only +/- lines — clean, no chimeras.
  2. **Supplement (with-context):** include context lines to recover boundary
     records that straddle diff hunks; only used when the primary pass missed
     a record AND the supplemental version has a valid ``application_type``.

Records missing ``application_type`` after both passes are dropped (typically
~1 % of total, caused by irrecoverable hunk splits at the end of the last diff).

At the end of a run the script writes a CSV export of every record that was
successfully inserted, to ``data/wslcb/licensinginfo-diffs/``.

Usage::

    # Dry-run (parse and report, no DB writes):
    python backfill_diffs.py --dry-run

    # Process only a specific subdirectory:
    python backfill_diffs.py --section notifications

    # Process a single diff file:
    python backfill_diffs.py --file data/wslcb/licensinginfo-diffs/notifications/2022_09_07-06_15_00-notifications-diff.txt

    # Full run across all sections:
    python backfill_diffs.py

    # Limit to N diff files (useful for validation):
    python backfill_diffs.py --limit 5

Safe to re-run — duplicates are detected by the UNIQUE constraint and skipped.
Address validation is deferred; run ``python scraper.py --backfill-addresses``
afterward.
"""

import argparse
import csv
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from database import (
    DATA_DIR, get_db, init_db, get_or_create_source, link_record_source,
    SOURCE_TYPE_CO_DIFF_ARCHIVE, WSLCB_SOURCE_URL,
)
from endorsements import discover_code_mappings, process_record, seed_endorsements, repair_code_name_endorsements
from log_config import setup_logging
from parser import (
    discover_diff_files, extract_records_from_diff, SECTION_DIR_MAP,
)
from queries import insert_record, hydrate_records, RECORD_COLUMNS, RECORD_JOINS

logger = logging.getLogger(__name__)


# ── CSV export ───────────────────────────────────────────────────────

# Matches the field list used by the /export endpoint in app.py.
CSV_FIELDS = [
    "section_type", "record_date", "business_name", "business_location",
    "address_line_1", "address_line_2", "applicants", "license_type",
    "endorsements", "application_type", "license_number", "contact_phone",
    "city", "state", "zip_code", "std_city", "std_state", "std_zip",
    "previous_business_name", "previous_applicants",
    "previous_business_location",
    "prev_address_line_1", "prev_address_line_2",
    "prev_std_city", "prev_std_state", "prev_std_zip",
]

# Lightweight field list for dry-run exports (no DB to query).
_DRY_RUN_FIELDS = [
    "section_type", "record_date", "business_name", "business_location",
    "applicants", "license_type", "application_type", "license_number",
    "contact_phone", "city", "state", "zip_code",
    "previous_business_name", "previous_applicants",
    "previous_business_location", "previous_city", "previous_state",
    "previous_zip_code",
]

CSV_DIR = DATA_DIR / "wslcb" / "licensinginfo-diffs"


def _csv_export_path() -> Path:
    """Return the export path for the current UTC date and time."""
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y_%m_%d")
    time_str = now.strftime("%H_%M_%S")
    return CSV_DIR / f"{date_str}-{time_str}-licensinginfo.lcb.wa.gov-diffs.csv"


def _write_dry_run_csv(records: list[dict], path: Path) -> None:
    """Write raw parsed *records* to CSV (dry-run, no DB available)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=_DRY_RUN_FIELDS, extrasaction="ignore",
        )
        writer.writeheader()
        for rec in sorted(
            records, key=lambda r: (r["record_date"], r["section_type"]),
        ):
            writer.writerow({k: rec.get(k, "") for k in _DRY_RUN_FIELDS})
    logger.info("CSV export (dry run): %d records → %s", len(records), path)


def _write_csv_from_db(
    conn, record_ids: list[int], path: Path,
) -> None:
    """Export inserted records from the DB, with cleaned names and endorsements.

    Reuses the same query infrastructure as the ``/export`` endpoint so
    the CSV reflects the normalised data actually stored in the database.
    """
    if not record_ids:
        return
    path.parent.mkdir(parents=True, exist_ok=True)

    # Fetch in batches to keep memory bounded.
    BATCH = 5000
    total_written = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=CSV_FIELDS, extrasaction="ignore",
        )
        writer.writeheader()
        for start in range(0, len(record_ids), BATCH):
            batch_ids = record_ids[start : start + BATCH]
            placeholders = ",".join("?" * len(batch_ids))
            rows = conn.execute(
                f"""SELECT {RECORD_COLUMNS} {RECORD_JOINS}
                    WHERE lr.id IN ({placeholders})
                    ORDER BY lr.record_date, lr.section_type""",
                batch_ids,
            ).fetchall()
            hydrated = hydrate_records(conn, rows)
            for r in hydrated:
                row = {k: r.get(k, "") for k in CSV_FIELDS}
                row["endorsements"] = "; ".join(r.get("endorsements", []))
                writer.writerow(row)
            total_written += len(hydrated)

    logger.info("CSV export: %d records → %s", total_written, path)


# ── Main entry point ─────────────────────────────────────────────────

COMMIT_BATCH_SIZE = 200


def backfill_diffs(
    *,
    section: str | None = None,
    single_file: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> None:
    """Parse diff files and insert recovered records into the database."""
    init_db()

    diff_files = discover_diff_files(section=section, single_file=single_file)
    if limit is not None:
        diff_files = diff_files[:limit]

    if not diff_files:
        logger.info("No diff files found.")
        return

    logger.info(
        "Processing %d diff file(s) (%s)",
        len(diff_files),
        "DRY RUN" if dry_run else "LIVE",
    )

    # Phase 1: extract all unique records across every diff file.
    # Also build ts→path mapping for provenance linking.
    all_records: dict[tuple, dict] = {}
    ts_to_diff_path: dict[str, str] = {}  # scraped_at → diff file path
    files_processed = 0
    for fp, sec_type in diff_files:
        try:
            recs = extract_records_from_diff(fp, sec_type)
        except Exception:
            logger.exception("Failed to parse %s", fp)
            continue
        for rec in recs:
            key = (
                rec["section_type"],
                rec["record_date"],
                rec["license_number"],
                rec["application_type"],
            )
            all_records.setdefault(key, rec)
            # Track which diff file contributed this scraped_at
            ts = rec.get("scraped_at", "")
            if ts and ts not in ts_to_diff_path:
                ts_to_diff_path[ts] = str(fp.relative_to(DATA_DIR))
        files_processed += 1
        if files_processed % 100 == 0:
            logger.debug(
                "  parsed %d / %d files (%d unique records so far)",
                files_processed,
                len(diff_files),
                len(all_records),
            )

    records = list(all_records.values())
    logger.info(
        "Extraction complete: %d unique records from %d files",
        len(records),
        files_processed,
    )

    if not records:
        return

    # Summary before inserting.
    by_section = Counter(r["section_type"] for r in records)
    date_min = min(r["record_date"] for r in records)
    date_max = max(r["record_date"] for r in records)
    logger.info(
        "  date range: %s to %s | new_application=%d, approved=%d, discontinued=%d",
        date_min,
        date_max,
        by_section.get("new_application", 0),
        by_section.get("approved", 0),
        by_section.get("discontinued", 0),
    )

    if dry_run:
        csv_path = _csv_export_path()
        _write_dry_run_csv(records, csv_path)
        logger.info("Dry run complete — no database changes made.")
        return

    # Phase 2: insert into database in batches.
    inserted_ids: list[int] = []
    skipped = 0
    errors = 0

    with get_db() as conn:
        seed_endorsements(conn)
        repair_code_name_endorsements(conn)

        # Pre-register provenance sources for all diff files.
        # Cache source_id by diff path to avoid repeated lookups.
        _source_cache: dict[str, int] = {}

        def _get_source_id(scraped_at: str) -> int | None:
            diff_path = ts_to_diff_path.get(scraped_at)
            if not diff_path:
                return None
            if diff_path not in _source_cache:
                _source_cache[diff_path] = get_or_create_source(
                    conn,
                    SOURCE_TYPE_CO_DIFF_ARCHIVE,
                    snapshot_path=diff_path,  # already DATA_DIR-relative
                    url=WSLCB_SOURCE_URL,
                    captured_at=scraped_at,
                )
            return _source_cache[diff_path]

        # Sort by date for deterministic insertion order.
        records.sort(key=lambda r: (r["record_date"], r["section_type"]))

        for i, rec in enumerate(records):
            try:
                result = insert_record(conn, rec)
                if result is None:
                    skipped += 1
                else:
                    rid, is_new = result
                    if is_new:
                        process_record(conn, rid, rec["license_type"])
                        sid = _get_source_id(rec.get("scraped_at", ""))
                        if sid is not None:
                            link_record_source(conn, rid, sid, "first_seen")
                        inserted_ids.append(rid)
                    else:
                        skipped += 1
            except Exception:
                logger.exception(
                    "Error inserting record: %s/%s/#%s",
                    rec.get("section_type"),
                    rec.get("record_date"),
                    rec.get("license_number"),
                )
                errors += 1

            # Commit in batches to allow recovery from interruption.
            if (i + 1) % COMMIT_BATCH_SIZE == 0:
                conn.commit()
                logger.debug(
                    "  progress: %d / %d (inserted=%d, skipped=%d)",
                    i + 1,
                    len(records),
                    len(inserted_ids),
                    skipped,
                )

        # Final commit and endorsement discovery.
        conn.commit()
        learned = discover_code_mappings(conn)
        if learned:
            logger.info(
                "Discovered %d new code mapping(s): %s",
                len(learned),
                list(learned.keys()),
            )
        conn.commit()

    logger.info(
        "Done! inserted=%d, skipped=%d, errors=%d (of %d total)",
        len(inserted_ids),
        skipped,
        errors,
        len(records),
    )

    # Phase 3: CSV export of successfully inserted records from the DB,
    # so the CSV reflects cleaned/normalised values and endorsements.
    if inserted_ids:
        csv_path = _csv_export_path()
        with get_db() as export_conn:
            _write_csv_from_db(export_conn, inserted_ids, csv_path)


# ── CLI ──────────────────────────────────────────────────────────────


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Backfill historical records from unified-diff archives.",
    )
    parser.add_argument(
        "--section",
        choices=list(SECTION_DIR_MAP.keys()),
        help="Process only this section subdirectory.",
    )
    parser.add_argument(
        "--file",
        dest="single_file",
        help="Process a single diff file instead of scanning directories.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process at most N diff files (for validation runs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and export CSV without writing to the database.",
    )
    args = parser.parse_args()

    backfill_diffs(
        section=args.section,
        single_file=args.single_file,
        limit=args.limit,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
