"""Scraper for WSLCB licensing activity page."""
import re
import sys
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from database import get_db, init_db, insert_record, learn_license_type_mappings

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
                "scraped_at": scraped_at,
            }
        elif label == "Business Name:":
            current["business_name"] = value
        elif label == "Business Location:":
            current["business_location"] = value
            city, state, zip_code = parse_location(value)
            current["city"] = city
            current["state"] = state
            current["zip_code"] = zip_code
        elif label == "Applicant(s):":
            current["applicants"] = value
        elif label == "License Type:":
            current["license_type"] = value
        elif label == "Application Type:":
            current["application_type"] = value
        elif label == "License Number:":
            current["license_number"] = value
        elif label == "Contact Phone:":
            current["contact_phone"] = value

    # Don't forget the last record
    if current.get("license_number"):
        records.append(current)

    return records


def scrape():
    """Main scrape function."""
    init_db()

    print(f"[{datetime.now().isoformat()}] Starting scrape of {URL}")

    with get_db() as conn:
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

            for section_type, table in data_tables:
                records = parse_records_from_table(table, section_type)
                print(f"  {section_type}: parsed {len(records)} records")

                inserted = 0
                for rec in records:
                    if insert_record(conn, rec):
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
                   records_discontinued = ?, records_skipped = ?
                   WHERE id = ?""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    counts["new"],
                    counts["approved"],
                    counts["discontinued"],
                    counts["skipped"],
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

            # Learn / update license-type code-to-label mappings
            learned = learn_license_type_mappings(conn)
            if learned:
                print(f"Learned {len(learned)} license-type mapping(s): {learned}")

        except Exception as e:
            conn.execute(
                "UPDATE scrape_log SET finished_at = ?, status = 'error', error_message = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), str(e), log_id),
            )
            conn.commit()
            print(f"ERROR: {e}", file=sys.stderr)
            raise


if __name__ == "__main__":
    scrape()
