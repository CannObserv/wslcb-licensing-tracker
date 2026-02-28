"""Pure parsing functions for WSLCB HTML pages and diff archives.

This module contains all HTML/diff parsing functions and file-discovery
helpers.  It has **no database access, no project imports, and no side
effects** (beyond reading files from disk).  Dependencies are limited
to stdlib + bs4/lxml.  Callers pass ``data_dir`` where filesystem paths
are needed.

Extracted from ``scraper.py``, ``backfill_snapshots.py``, and
``backfill_diffs.py`` as part of the Phase 1 architecture refactor (#16).
"""
import logging
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────

SECTION_MAP = {
    "STATEWIDE NEW LICENSE APPLICATIONS": "new_application",
    "STATEWIDE RECENTLY APPROVED LICENSES": "approved",
    "STATEWIDE DISCONTINUED LICENSES": "discontinued",
}

DATE_FIELD_MAP = {
    "new_application": "Notification Date:",
    "approved": "Approved Date:",
    "discontinued": "Discontinued Date:",
}

# Maps diff-archive subdirectory names to section_type values.
SECTION_DIR_MAP = {
    "notifications": "new_application",
    "approvals": "approved",
    "discontinued": "discontinued",
}

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ── Location / date helpers ──────────────────────────────────────────


def parse_location(location: str) -> tuple[str, str, str]:
    """Extract city, state, zip from a location string like '123 MAIN ST, SEATTLE, WA 98101'."""
    if not location:
        return "", "WA", ""
    # Try to match: ..., CITY, ST ZIP
    m = re.search(r',\s*([A-Z][A-Z .]+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', location)
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    # Fallback: try ..., CITY, ST
    m = re.search(r',\s*([A-Z][A-Z .]+?),\s*([A-Z]{2})', location)
    if m:
        return m.group(1).strip(), m.group(2).strip(), ""
    return "", "WA", ""


def normalize_date(date_str: str) -> str:
    """Convert M/D/YYYY to YYYY-MM-DD for proper sorting."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str.strip(), "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str.strip()


# ── HTML table parsing ───────────────────────────────────────────────


def parse_records_from_table(table, section_type: str) -> list[dict]:
    """Parse all records from a section table."""
    records = []
    rows = table.find_all("tr")
    current = {}
    date_field = DATE_FIELD_MAP[section_type]
    scraped_at = datetime.now(timezone.utc).isoformat()

    for row in rows:
        cells = row.find_all("td")
        if len(cells) != 2:
            # If we have a partially built record, save it before skipping
            continue

        label = cells[0].get_text(strip=True)
        value = cells[1].get_text(strip=True)

        if label == date_field:
            # Start of a new record — save previous if complete
            if current.get("license_number"):
                records.append(current)
            current = {
                "section_type": section_type,
                "record_date": normalize_date(value),
                "business_name": "",
                "business_location": "",
                "applicants": "",
                "license_type": "",
                "application_type": "",
                "license_number": "",
                "contact_phone": "",
                "city": "",
                "state": "WA",
                "zip_code": "",
                "previous_business_name": "",
                "previous_applicants": "",
                "previous_business_location": "",
                "previous_city": "",
                "previous_state": "",
                "previous_zip_code": "",
                "scraped_at": scraped_at,
            }
        elif label == "Business Name:":
            current["business_name"] = value
        elif label == "New Business Name:":
            # ASSUMPTION records: buyer's business name
            current["business_name"] = value
        elif label == "Current Business Name:":
            # ASSUMPTION records: seller's business name
            current["previous_business_name"] = value
        elif label == "Business Location:":
            current["business_location"] = value
            city, state, zip_code = parse_location(value)
            current["city"] = city
            current["state"] = state
            current["zip_code"] = zip_code
        elif label == "New Business Location:":
            # CHANGE OF LOCATION records: new (destination) address
            current["business_location"] = value
            city, state, zip_code = parse_location(value)
            current["city"] = city
            current["state"] = state
            current["zip_code"] = zip_code
        elif label == "Current Business Location:":
            # CHANGE OF LOCATION records: previous (origin) address
            current["previous_business_location"] = value
            city, state, zip_code = parse_location(value)
            current["previous_city"] = city
            current["previous_state"] = state
            current["previous_zip_code"] = zip_code
        elif label == "Applicant(s):":
            current["applicants"] = value
        elif label == "New Applicant(s):":
            # ASSUMPTION records: buyer's applicants
            current["applicants"] = value
        elif label == "Current Applicant(s):":
            # ASSUMPTION records: seller's applicants
            current["previous_applicants"] = value
        elif label == "License Type:":
            current["license_type"] = value
        elif label in ("Application Type:", "\\Application Type:"):
            current["application_type"] = value
        elif label == "License Number:":
            current["license_number"] = value
        elif label == "Contact Phone:":
            current["contact_phone"] = value

    # Don't forget the last record
    if current.get("license_number"):
        records.append(current)

    return records


# ── Full-page snapshot parsing ───────────────────────────────────────


def snapshot_paths(data_dir: Path) -> list[Path]:
    """Return all archived snapshot paths, sorted chronologically."""
    return sorted(data_dir.glob("wslcb/licensinginfo/**/*.html"))


def extract_snapshot_date(path: Path) -> str | None:
    """Extract date string from snapshot filename (e.g. '2025_12_16')."""
    m = re.search(r'(\d{4}_\d{2}_\d{2})', path.name)
    return m.group(1) if m else None


def parse_snapshot(path: Path) -> list[dict]:
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


# ── Diff parsing ─────────────────────────────────────────────────────


def parse_diff_timestamp(header_line: str) -> str:
    """Extract an ISO 8601 timestamp from a ``---`` or ``+++`` diff header.

    Expected format: ``--- @\tWed, 07 Sep 2022 06:15:05 -0700``
    Returns the current UTC time as fallback if parsing fails.
    """
    try:
        # Strip the "--- @\t" / "+++ @\t" prefix.
        raw = header_line.split("\t", 1)[1]
        return parsedate_to_datetime(raw).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def split_diff_lines(content: str):
    """Split a unified diff into added, removed, and context line lists.

    Returns ``(added, removed, new_with_ctx, old_with_ctx, old_ts, new_ts)``
    where:

    - *added* / *removed* contain only ``+`` / ``-`` lines (prefix stripped).
    - *new_with_ctx* / *old_with_ctx* include context lines on both sides.
    - *old_ts* / *new_ts* are ISO 8601 timestamps extracted from the
      ``---`` / ``+++`` headers (used as ``scraped_at`` for recovered records).
    """
    added: list[str] = []
    removed: list[str] = []
    new_ctx: list[str] = []
    old_ctx: list[str] = []
    fallback_ts = datetime.now(timezone.utc).isoformat()
    old_ts = fallback_ts
    new_ts = fallback_ts

    for line in content.split("\n"):
        if line.startswith("--- "):
            old_ts = parse_diff_timestamp(line)
            continue
        if line.startswith("+++ "):
            new_ts = parse_diff_timestamp(line)
            continue
        if line.startswith("@@"):
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

    return added, removed, new_ctx, old_ctx, old_ts, new_ts


def parse_html_lines(lines: list[str], section_type: str) -> list[dict]:
    """Wrap *lines* in a ``<table>`` and parse via the standard parser."""
    if not lines:
        return []
    html = "<table>" + "\n".join(lines) + "</table>"
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    return parse_records_from_table(table, section_type)


def is_valid_record(record: dict) -> bool:
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

    Uses the two-pass strategy described in the ``backfill_diffs`` module
    docstring.  The supplemental (with-context) pass is only run when the
    primary pass produced incomplete records at hunk boundaries, keeping
    overall parse time low.
    """
    content = filepath.read_text(encoding="utf-8")
    added, removed, new_ctx, old_ctx, old_ts, new_ts = split_diff_lines(content)

    # ── Primary pass (no context) ──
    primary: dict[tuple, dict] = {}
    has_incomplete = False
    for lines, ts in ((added, new_ts), (removed, old_ts)):
        for rec in parse_html_lines(lines, section_type):
            if is_valid_record(rec):
                rec["scraped_at"] = ts
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

    # ── Supplemental pass (with context) ──
    # Only recover records whose full 4-tuple key is absent from the
    # primary results.
    for lines, ts in ((new_ctx, new_ts), (old_ctx, old_ts)):
        for rec in parse_html_lines(lines, section_type):
            if is_valid_record(rec):
                key = (
                    rec["section_type"],
                    rec["record_date"],
                    rec["license_number"],
                    rec["application_type"],
                )
                if key not in primary:
                    rec["scraped_at"] = ts
                    primary.setdefault(key, rec)

    return list(primary.values())


# ── Diff file discovery ──────────────────────────────────────────────


def discover_diff_files(
    data_dir: Path,
    section: str | None = None,
    single_file: str | None = None,
) -> list[tuple[Path, str]]:
    """Return ``[(path, section_type), ...]`` sorted by filename.

    *section* limits to a single subdirectory (e.g. ``"notifications"``).
    *single_file* overrides everything and processes just one file.
    """
    if single_file:
        p = Path(single_file).resolve()
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
        dir_path = data_dir / "wslcb" / "licensinginfo-diffs" / dir_name
        if not dir_path.is_dir():
            logger.warning("Directory not found: %s", dir_path)
            continue
        for fp in sorted(dir_path.glob("*.txt")):
            result.append((fp, sec_type))

    return result
