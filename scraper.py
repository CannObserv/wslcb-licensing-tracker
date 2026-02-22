"""Scraper for WSLCB licensing activity page."""
import re
import sys
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from database import DATA_DIR, get_db, init_db, insert_record, get_or_create_location
from endorsements import process_record, seed_endorsements, discover_code_mappings
from address_validator import validate_record, validate_previous_location, backfill_addresses, refresh_addresses, TIMEOUT as _AV_TIMEOUT

URL = "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp"

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


def save_html_snapshot(html: str, scrape_date: datetime) -> Path:
    """Save raw HTML to data/[yyyy]/[yyyy_mm_dd]-v[x]/licensing info.lcb.wa.gov-[yyyy_mm_dd]-v[x].html

    Saves the HTML exactly as received from the server (no transformation).
    Increments the version number if a snapshot for the same date already exists.
    Returns the path to the saved file.
    """
    date_str = scrape_date.strftime("%Y_%m_%d")
    year_str = scrape_date.strftime("%Y")
    year_dir = DATA_DIR / year_str

    # Determine next version number for this date
    version = 1
    while (year_dir / f"{date_str}-v{version}").exists():
        version += 1

    snapshot_dir = year_dir / f"{date_str}-v{version}"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    filename = f"licensing info.lcb.wa.gov-{date_str}-v{version}.html"
    filepath = snapshot_dir / filename
    filepath.write_text(html, encoding="utf-8")
    return filepath


def scrape():
    """Main scrape function."""
    init_db()

    print(f"[{datetime.now().isoformat()}] Starting scrape of {URL}")

    with get_db() as conn:
        # Ensure seed code→endorsement mappings exist (idempotent; needed
        # because the scraper runs standalone, not through FastAPI lifespan).
        seed_endorsements(conn)

        # Log the scrape start
        cursor = conn.execute(
            "INSERT INTO scrape_log (started_at, status) VALUES (?, 'running')",
            (datetime.now(timezone.utc).isoformat(),),
        )
        log_id = cursor.lastrowid
        conn.commit()

        try:
            # Fetch page
            print("Fetching page...")
            resp = httpx.get(URL, timeout=120, follow_redirects=True)
            resp.raise_for_status()
            html = resp.text
            print(f"Fetched {len(html):,} bytes")

            # Archive the raw HTML
            snapshot_path = None
            try:
                snapshot_path = save_html_snapshot(html, datetime.now(timezone.utc))
                print(f"Saved snapshot to {snapshot_path}")
            except Exception as snap_err:
                print(f"WARNING: Failed to save HTML snapshot: {snap_err}", file=sys.stderr)

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

            print(f"Found {len(data_tables)} data sections")

            counts = {"new": 0, "approved": 0, "discontinued": 0, "skipped": 0}

            with httpx.Client(timeout=_AV_TIMEOUT) as av_client:
                for section_type, table in data_tables:
                    records = parse_records_from_table(table, section_type)
                    print(f"  {section_type}: parsed {len(records)} records")

                    inserted = 0
                    for rec in records:
                        rid = insert_record(conn, rec)
                        if rid is not None:
                            process_record(conn, rid, rec["license_type"], rec["section_type"])
                            validate_record(conn, rid, client=av_client)
                            if rec.get("previous_business_location"):
                                validate_previous_location(conn, rid, client=av_client)
                            inserted += 1
                        else:
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
            print(
                f"Done! Inserted {total} new records "
                f"(new={counts['new']}, approved={counts['approved']}, "
                f"discontinued={counts['discontinued']}, skipped={counts['skipped']})"
            )

            # Discover any new code→endorsement mappings from cross-references
            learned = discover_code_mappings(conn)
            if learned:
                print(f"Discovered {len(learned)} new code mapping(s): {list(learned.keys())}")

        except Exception as e:
            conn.execute(
                "UPDATE scrape_log SET finished_at = ?, status = 'error', error_message = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), str(e), log_id),
            )
            conn.commit()
            print(f"ERROR: {e}", file=sys.stderr)
            raise


def backfill_from_snapshots():
    """Backfill records from archived HTML snapshots.

    Parses every archived snapshot and fixes two categories of records:

    1. ASSUMPTION records with empty business_name (pre-fix scrape)
    2. CHANGE OF LOCATION records with empty business_location or
       empty previous_business_location (pre-fix scrape)

    Safe to re-run — only updates records that still have empty fields.
    """
    init_db()
    snapshots = sorted(DATA_DIR.glob("**/*.html"))
    if not snapshots:
        print("No archived snapshots found.")
        return

    print(f"Found {len(snapshots)} snapshot(s) to scan")
    assumption_updated = 0
    col_updated = 0

    with get_db() as conn:
        for snap_path in snapshots:
            html = snap_path.read_text(encoding="utf-8")
            soup = BeautifulSoup(html, "lxml")

            all_tables = soup.find_all("table")
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
                    # Backfill ASSUMPTION records
                    if rec["application_type"] == "ASSUMPTION":
                        if not rec["business_name"] and not rec["previous_business_name"]:
                            continue  # Still empty even after new parsing — skip

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
                                rec["business_name"],
                                rec["applicants"],
                                rec["previous_business_name"],
                                rec["previous_applicants"],
                                rec["section_type"],
                                rec["record_date"],
                                rec["license_number"],
                            ),
                        )
                        if cursor.rowcount > 0:
                            assumption_updated += cursor.rowcount

                    # Backfill CHANGE OF LOCATION records
                    if rec["application_type"] == "CHANGE OF LOCATION":
                        if not rec["business_location"]:
                            continue  # Still empty even after new parsing — skip

                        # Resolve location IDs for the parsed addresses
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

                        # Fix records that had empty location/application_type
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
                            col_updated += cursor.rowcount
                            continue

                        # Also fix records that have the location but are
                        # missing previous_location_id
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
                        if cursor.rowcount > 0:
                            col_updated += cursor.rowcount

            conn.commit()

        print(f"Updated {assumption_updated} ASSUMPTION record(s) from archived snapshots.")
        print(f"Updated {col_updated} CHANGE OF LOCATION record(s) from archived snapshots.")


if __name__ == "__main__":
    if "--refresh-addresses" in sys.argv:
        init_db()
        with get_db() as conn:
            refresh_addresses(conn)
    elif "--backfill-addresses" in sys.argv:
        init_db()
        with get_db() as conn:
            backfill_addresses(conn)
    elif "--backfill-assumptions" in sys.argv or "--backfill-from-snapshots" in sys.argv:
        backfill_from_snapshots()
    else:
        # "scrape" is accepted as an explicit positional arg (used by
        # the wslcb-task@ systemd template) but is not required.
        scrape()
