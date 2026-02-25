"""Backfill historical records from unified-diff archives.

Parses diff files in ``research/historical_data/{notifications,approvals,discontinued}/``
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
successfully inserted, to ``data/backfill_diffs_<timestamp>.csv``.

Usage::

    # Dry-run (parse and report, no DB writes):
    python backfill_diffs.py --dry-run

    # Process only a specific subdirectory:
    python backfill_diffs.py --section notifications

    # Process a single diff file:
    python backfill_diffs.py --file research/historical_data/notifications/2022_09_07-06_15_00-notifications-diff.txt

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
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup

from database import DATA_DIR, get_db, init_db
from endorsements import discover_code_mappings, process_record, seed_endorsements
from log_config import setup_logging
from queries import insert_record
from scraper import parse_records_from_table

logger = logging.getLogger(__name__)

RESEARCH_DIR = Path("research/historical_data")

# Maps subdirectory names to the section_type values used in the DB.
SECTION_DIR_MAP = {
    "notifications": "new_application",
    "approvals": "approved",
    "discontinued": "discontinued",
}

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ── Diff parsing ─────────────────────────────────────────────────────


def _split_diff_lines(content: str):
    """Split a unified diff into added, removed, and context line lists.

    Returns ``(added, removed, new_with_ctx, old_with_ctx)`` where:
    - *added* / *removed* contain only ``+`` / ``-`` lines (prefix stripped).
    - *new_with_ctx* / *old_with_ctx* include context lines on both sides.
    """
    added: list[str] = []
    removed: list[str] = []
    new_ctx: list[str] = []
    old_ctx: list[str] = []

    for line in content.split("\n"):
        if line.startswith(("---", "+++", "@@")):
            continue
        if line.startswith("+"):
            stripped = line[1:]
            added.append(stripped)
            new_ctx.append(stripped)
        elif line.startswith("-"):
            stripped = line[1:]
            removed.append(stripped)
            old_ctx.append(stripped)
        else:
            # Context line — belongs to both sides.
            new_ctx.append(line)
            old_ctx.append(line)

    return added, removed, new_ctx, old_ctx


def _parse_html_lines(lines: list[str], section_type: str) -> list[dict]:
    """Wrap *lines* in a ``<table>`` and parse via the scraper's parser."""
    if not lines:
        return []
    html = "<table>" + "\n".join(lines) + "</table>"
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    return parse_records_from_table(table, section_type)


def _is_valid(record: dict) -> bool:
    """Return True if a record has the minimum required fields."""
    return bool(
        record.get("section_type")
        and record.get("record_date")
        and _ISO_DATE_RE.match(record.get("record_date", ""))
        and record.get("license_number")
        and record.get("application_type")
    )


def extract_records_from_diff(
    filepath: Path, section_type: str
) -> list[dict]:
    """Extract deduplicated, validated records from a single diff file.

    Uses the two-pass strategy described in the module docstring.
    The supplemental (with-context) pass is only run when the primary
    pass produced incomplete records at hunk boundaries, keeping
    overall parse time low.
    """
    content = filepath.read_text(encoding="utf-8")
    added, removed, new_ctx, old_ctx = _split_diff_lines(content)

    # ── Primary pass (no context) ──
    primary: dict[tuple, dict] = {}
    has_incomplete = False
    for lines in (added, removed):
        for rec in _parse_html_lines(lines, section_type):
            if _is_valid(rec):
                key = (
                    rec["section_type"],
                    rec["record_date"],
                    rec["license_number"],
                    rec["application_type"],
                )
                primary.setdefault(key, rec)
            elif rec.get("license_number"):
                # Partial record — boundary artifact.
                has_incomplete = True

    # Fast path: skip the expensive supplemental parse when nothing
    # was incomplete in the primary pass.
    if not has_incomplete:
        return list(primary.values())

    # Track which (section, date, license) tuples the primary pass covers.
    primary_ids = {(k[0], k[1], k[2]) for k in primary}

    # ── Supplemental pass (with context) ──
    supplemental: dict[tuple, dict] = {}
    for lines in (new_ctx, old_ctx):
        for rec in _parse_html_lines(lines, section_type):
            if _is_valid(rec):
                short_id = (
                    rec["section_type"],
                    rec["record_date"],
                    rec["license_number"],
                )
                if short_id not in primary_ids:
                    key = (
                        rec["section_type"],
                        rec["record_date"],
                        rec["license_number"],
                        rec["application_type"],
                    )
                    supplemental.setdefault(key, rec)

    merged = {**primary, **{k: v for k, v in supplemental.items() if k not in primary}}
    return list(merged.values())


# ── Diff file discovery ──────────────────────────────────────────────


def _discover_diff_files(
    section: str | None = None,
    single_file: str | None = None,
) -> list[tuple[Path, str]]:
    """Return ``[(path, section_type), ...]`` sorted by filename.

    *section* limits to a single subdirectory (e.g. ``"notifications"``).
    *single_file* overrides everything and processes just one file.
    """
    if single_file:
        p = Path(single_file)
        if not p.exists():
            logger.error("File not found: %s", p)
            return []
        # Infer section from parent directory name.
        dir_name = p.parent.name
        if dir_name not in SECTION_DIR_MAP:
            logger.error(
                "Cannot infer section from directory '%s'. "
                "Expected one of: %s",
                dir_name,
                list(SECTION_DIR_MAP.keys()),
            )
            return []
        return [(p, SECTION_DIR_MAP[dir_name])]

    dirs = (
        {section: SECTION_DIR_MAP[section]}
        if section and section in SECTION_DIR_MAP
        else SECTION_DIR_MAP
    )

    result: list[tuple[Path, str]] = []
    for dir_name, sec_type in dirs.items():
        dir_path = RESEARCH_DIR / dir_name
        if not dir_path.is_dir():
            logger.warning("Directory not found: %s", dir_path)
            continue
        for fp in sorted(dir_path.glob("*.txt")):
            result.append((fp, sec_type))

    return result


# ── CSV export ───────────────────────────────────────────────────────

CSV_FIELDS = [
    "section_type",
    "record_date",
    "business_name",
    "business_location",
    "applicants",
    "license_type",
    "application_type",
    "license_number",
    "contact_phone",
    "city",
    "state",
    "zip_code",
    "previous_business_name",
    "previous_applicants",
    "previous_business_location",
    "previous_city",
    "previous_state",
    "previous_zip_code",
]


def _write_csv(records: list[dict], path: Path) -> None:
    """Write *records* to a CSV file at *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for rec in sorted(
            records, key=lambda r: (r["record_date"], r["section_type"])
        ):
            writer.writerow({k: rec.get(k, "") for k in CSV_FIELDS})
    logger.info("CSV export: %d records → %s", len(records), path)


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

    diff_files = _discover_diff_files(section=section, single_file=single_file)
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
    all_records: dict[tuple, dict] = {}
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
    from collections import Counter

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
        csv_path = DATA_DIR / f"backfill_diffs_dry_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
        _write_csv(records, csv_path)
        logger.info("Dry run complete — no database changes made.")
        return

    # Phase 2: insert into database in batches.
    inserted_records: list[dict] = []
    skipped = 0
    errors = 0

    with get_db() as conn:
        seed_endorsements(conn)

        # Sort by date for deterministic insertion order.
        records.sort(key=lambda r: (r["record_date"], r["section_type"]))

        for i, rec in enumerate(records):
            try:
                rid = insert_record(conn, rec)
                if rid is not None:
                    process_record(
                        conn, rid, rec["license_type"], rec["section_type"]
                    )
                    inserted_records.append(rec)
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
                    len(inserted_records),
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
        len(inserted_records),
        skipped,
        errors,
        len(records),
    )

    # Phase 3: CSV export of successfully inserted records.
    if inserted_records:
        csv_path = DATA_DIR / f"backfill_diffs_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
        _write_csv(inserted_records, csv_path)


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
