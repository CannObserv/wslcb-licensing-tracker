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
from datetime import UTC, datetime
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
    m = re.search(r",\s*([A-Z][A-Z .]+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)", location)
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    # Fallback: try ..., CITY, ST
    m = re.search(r",\s*([A-Z][A-Z .]+?),\s*([A-Z]{2})", location)
    if m:
        return m.group(1).strip(), m.group(2).strip(), ""
    return "", "WA", ""


def normalize_date(date_str: str) -> str:
    """Convert M/D/YYYY to YYYY-MM-DD for proper sorting."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str.strip(), "%m/%d/%Y").replace(
            tzinfo=UTC,
        )
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str.strip()


# ── HTML table parsing ───────────────────────────────────────────────


_CELL_COUNT = 2


def parse_records_from_table(  # noqa: C901, PLR0912, PLR0915  # WSLCB field dispatch; not worth splitting
    table: "BeautifulSoup",
    section_type: str,
) -> list[dict]:
    """Parse all records from a section table."""
    records = []
    rows = table.find_all("tr")
    current = {}
    date_field = DATE_FIELD_MAP[section_type]
    scraped_at = datetime.now(UTC)

    for row in rows:
        cells = row.find_all("td")
        if len(cells) != _CELL_COUNT:
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


def extract_snapshot_date(path: Path) -> datetime | None:
    """Extract a UTC midnight datetime from a snapshot filename (e.g. '2025_12_16')."""
    m = re.search(r"(\d{4})_(\d{2})_(\d{2})", path.name)
    if not m:
        return None
    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=UTC)


def parse_snapshot(path: Path) -> list[dict]:
    """Parse a snapshot file and return a list of record dicts."""
    html = path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "lxml")
    records = []
    for table in soup.find_all("table"):
        th = table.find("th")
        if not th:
            continue
        header = th.get_text(strip=True).replace("\xa0", " ")
        if header not in SECTION_MAP:
            continue
        section_type = SECTION_MAP[header]
        records.extend(parse_records_from_table(table, section_type))
    return records


# ── Diff parsing ─────────────────────────────────────────────────────


def parse_diff_timestamp(header_line: str) -> datetime:
    r"""Extract a timestamp from a ``---`` or ``+++`` diff header.

    Expected format: ``--- @\tWed, 07 Sep 2022 06:15:05 -0700``
    Returns the current UTC time as fallback if parsing fails.
    """
    try:
        # Strip the "--- @\t" / "+++ @\t" prefix.
        raw = header_line.split("\t", 1)[1]
        return parsedate_to_datetime(raw)
    except Exception:  # noqa: BLE001  # malformed diff headers vary widely; fall back to current time
        return datetime.now(UTC)


def split_diff_lines(
    content: str,
) -> tuple[list[str], list[str], list[str], list[str], datetime, datetime]:
    """Split a unified diff into added, removed, and context line lists.

    Returns ``(added, removed, new_with_ctx, old_with_ctx, old_ts, new_ts)``
    where:

    - *added* / *removed* contain only ``+`` / ``-`` lines (prefix stripped).
    - *new_with_ctx* / *old_with_ctx* include context lines on both sides.
    - *old_ts* / *new_ts* are timestamps extracted from the
      ``---`` / ``+++`` headers (used as ``scraped_at`` for recovered records).
    """
    added: list[str] = []
    removed: list[str] = []
    new_ctx: list[str] = []
    old_ctx: list[str] = []
    fallback_ts = datetime.now(UTC)
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


def extract_records_from_diff(filepath: Path, section_type: str) -> list[dict]:
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
                "Cannot infer section from directory '%s'. Expected one of: %s",
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
        result.extend((fp, sec_type) for fp in sorted(dir_path.glob("*.txt")))

    return result


# ── Source viewer: raw <tbody> extraction ──────────────────────────


def _match_key(
    tbody: "BeautifulSoup",
    section_type: str,
    license_number: str,
    record_date: str,
    application_type: str,
) -> bool:
    """Return True if a BeautifulSoup <tbody> element matches the natural key.

    Scans the two-cell <tr> rows to extract the date, license number, and
    application type fields, then compares against the provided key.
    Normalises the date via ``normalize_date`` so M/D/Y and ISO formats
    both match.
    """
    date_field = DATE_FIELD_MAP.get(section_type)
    if date_field is None:
        return False

    found_date = ""
    found_license = ""
    found_app_type = ""

    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != _CELL_COUNT:
            continue
        label = cells[0].get_text(strip=True)
        value = cells[1].get_text(strip=True)
        if label == date_field:
            found_date = normalize_date(value)
        elif label == "License Number:":
            found_license = value.strip()
        elif label in ("Application Type:", "\\Application Type:"):
            found_app_type = value.strip()

    return (
        found_date == record_date
        and found_license == license_number
        and found_app_type == application_type
    )


def strip_anchor_tags(html: str) -> str:
    """Remove all <a> elements from *html* while preserving their text content.

    WSLCB snapshot pages sometimes wrap cell values in anchor tags (e.g.
    ``<a href="tel:...">206-555-1234</a>``).  These links are meaningless and
    potentially broken inside the sandboxed source-viewer iframe, so we unwrap
    them before embedding the ``<tbody>`` HTML in ``srcdoc``.

    Uses BeautifulSoup ``unwrap()`` so that any child elements inside the
    ``<a>`` tag (e.g. ``<b>``) are also preserved.
    """
    if not html:
        return html
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all("a"):
        tag.unwrap()
    # lxml wraps the fragment in <html><body>; strip that wrapper by
    # returning only the inner content of <body>.
    body = soup.find("body")
    if body is not None:
        return "".join(str(c) for c in body.contents)
    return str(soup)


def extract_tbody_from_snapshot(
    path: Path,
    section_type: str,
    license_number: str,
    record_date: str,
    application_type: str,
) -> str | None:
    """Extract the raw outer HTML of the <tbody> for a single record.

    Searches the full-page HTML snapshot at *path* for the section table
    matching *section_type*, then locates the ``<tbody>`` whose natural key
    ``(section_type, license_number, record_date, application_type)`` matches.

    Returns the raw ``str(tbody)`` HTML string (inline styles intact) or
    ``None`` if the record is not present in this snapshot.
    """
    try:
        html = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        html = path.read_text(encoding="latin-1")

    soup = BeautifulSoup(html, "lxml")

    for table in soup.find_all("table"):
        th = table.find("th")
        if not th:
            continue
        header = th.get_text(strip=True).replace("\xa0", " ")
        if SECTION_MAP.get(header) != section_type:
            continue
        # Found the right section table — scan its <tbody> elements.
        for tbody in table.find_all("tbody"):
            if _match_key(tbody, section_type, license_number, record_date, application_type):
                return str(tbody)

    return None


# Date field labels used as record-start sentinels in the tbody-less diff fallback.
_DATE_LABELS = tuple(DATE_FIELD_MAP.values())  # e.g. "Notification Date:"


def _extract_tbody_lines(lines: list[str]) -> list[list[str]]:  # noqa: C901  # two-format detection + stateful parsing
    """Split a flat list of HTML lines into per-record ``<tbody>`` line groups.

    Two formats are handled:

    1. **``<tbody>``-wrapped** (most diffs): a new group starts when a line
       contains ``<tbody`` and ends when a line contains ``</tbody>``.
    2. **Bare ``<tr>`` rows** (some older diffs lack ``<tbody>`` wrappers): a
       new group starts when a label cell contains one of the date field
       labels (e.g. ``Notification Date``) and ends when the next date-label
       line is encountered (or the list is exhausted).  The resulting group
       is wrapped in synthetic ``<tbody>...</tbody>`` tags so downstream
       BeautifulSoup parsing works uniformly.

    Returns a list of line-lists, one per record ``<tbody>`` block.
    """
    # Detect format by checking for any <tbody> tag.
    has_tbody = any("<tbody" in ln.lower() for ln in lines)

    if has_tbody:
        # Format 1: <tbody>-wrapped records.
        groups: list[list[str]] = []
        current: list[str] | None = None
        for line in lines:
            stripped = line.strip()
            if "<tbody" in stripped.lower() and current is None:
                current = [line]
                if "</tbody>" in stripped.lower():
                    groups.append(current)
                    current = None
            elif current is not None:
                current.append(line)
                if "</tbody>" in stripped.lower():
                    groups.append(current)
                    current = None
        return groups

    # Format 2: bare <tr> rows — split on date field labels.
    def _is_date_label(line: str) -> bool:
        """Return True when *line* contains a date-field label as the visible text of a ``<td>``.

        Checks for the label inside a cell context (possibly wrapped in ``<b>``)
        to prevent false matches on business names that incidentally contain
        the label text as a substring.
        """
        for label in _DATE_LABELS:
            if re.search(
                r"<td[^>]*>\s*(?:<[^>]+>)*\s*" + re.escape(label) + r"\s*(?:</[^>]+>)*\s*</td>",
                line,
                re.IGNORECASE,
            ):
                return True
        return False

    groups2: list[list[str]] = []
    current2: list[str] | None = None
    for line in lines:
        if _is_date_label(line):
            if current2:
                groups2.append(["<tbody>", *current2, "</tbody>"])
            current2 = [line]
        elif current2 is not None:
            current2.append(line)
    if current2:
        groups2.append(["<tbody>", *current2, "</tbody>"])
    return groups2


def extract_tbody_from_diff(
    path: Path,
    section_type: str,
    license_number: str,
    record_date: str,
    application_type: str,
) -> str | None:
    """Extract the reconstructed <tbody> HTML for a single record from a diff.

    Parses the unified diff at *path*, scanning the added lines first (the
    "after" state) and the removed lines as a fallback.  For each contiguous
    ``<tbody>…</tbody>`` block found in those lines, checks whether the
    natural key matches.  Returns the raw HTML string or ``None``.
    """
    content = path.read_text(encoding="utf-8")
    added, removed, _new_ctx, _old_ctx, _old_ts, _new_ts = split_diff_lines(content)

    for lines in (added, removed):
        for group in _extract_tbody_lines(lines):
            html = "\n".join(group)
            soup = BeautifulSoup(html, "lxml")
            tbody = soup.find("tbody")
            if tbody is None:
                continue
            if _match_key(tbody, section_type, license_number, record_date, application_type):
                return str(tbody)

    return None
