"""Query and record-manipulation functions for WSLCB licensing tracker.

Contains search, filter, stats, and record CRUD operations that
combine data from multiple tables (records, locations, endorsements,
entities).  Thin read/write layer on top of the core schema in
``database.py``.
"""
import logging
import sqlite3
import time

from endorsements import get_endorsement_options, get_record_endorsements
from entities import _parse_and_link_entities, get_record_entities

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Shared column list and JOINs for all record queries
# ------------------------------------------------------------------

_RECORD_COLUMNS = """
    lr.id, lr.section_type, lr.record_date, lr.business_name,
    lr.applicants, lr.license_type, lr.application_type,
    lr.license_number, lr.contact_phone,
    lr.previous_business_name, lr.previous_applicants,
    lr.location_id, lr.previous_location_id,
    lr.scraped_at, lr.created_at,
    COALESCE(loc.raw_address, '') AS business_location,
    COALESCE(loc.city, '') AS city,
    COALESCE(loc.state, 'WA') AS state,
    COALESCE(loc.zip_code, '') AS zip_code,
    COALESCE(loc.address_line_1, '') AS address_line_1,
    COALESCE(loc.address_line_2, '') AS address_line_2,
    COALESCE(loc.std_city, '') AS std_city,
    COALESCE(loc.std_state, '') AS std_state,
    COALESCE(loc.std_zip, '') AS std_zip,
    loc.address_validated_at,
    COALESCE(ploc.raw_address, '') AS previous_business_location,
    COALESCE(ploc.city, '') AS previous_city,
    COALESCE(ploc.state, '') AS previous_state,
    COALESCE(ploc.zip_code, '') AS previous_zip_code,
    COALESCE(ploc.address_line_1, '') AS prev_address_line_1,
    COALESCE(ploc.address_line_2, '') AS prev_address_line_2,
    COALESCE(ploc.std_city, '') AS prev_std_city,
    COALESCE(ploc.std_state, '') AS prev_std_state,
    COALESCE(ploc.std_zip, '') AS prev_std_zip,
    ploc.address_validated_at AS prev_address_validated_at"""

_RECORD_JOINS = """
    FROM license_records lr
    LEFT JOIN locations loc ON loc.id = lr.location_id
    LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id"""

_RECORD_SELECT = f"SELECT {_RECORD_COLUMNS} {_RECORD_JOINS}"


# ------------------------------------------------------------------
# Record enrichment helpers
# ------------------------------------------------------------------

def enrich_record(record: dict) -> dict:
    """Add display-ready fields with standardized-first fallback.

    Works with joined query results that include location columns
    aliased as business_location, city, std_city, etc.
    """
    record["display_city"] = record.get("std_city") or record.get("city") or ""
    record["display_zip"] = record.get("std_zip") or record.get("zip_code") or ""
    record["display_previous_city"] = record.get("prev_std_city") or record.get("previous_city") or ""
    record["display_previous_zip"] = record.get("prev_std_zip") or record.get("previous_zip_code") or ""
    return record


def _hydrate_records(
    conn: sqlite3.Connection, rows: list,
) -> list[dict]:
    """Enrich DB rows/dicts with endorsements, entities, and display fields.

    Accepts sqlite3.Row objects or plain dicts.  Shared by
    search_records(), get_entity_records(), and app.py record_detail().
    """
    if not rows:
        return []
    record_ids = [r["id"] for r in rows]
    endorsement_map = get_record_endorsements(conn, record_ids)
    entity_map = get_record_entities(conn, record_ids)
    results = []
    for r in rows:
        d = enrich_record(r if isinstance(r, dict) else dict(r))
        d["endorsements"] = endorsement_map.get(d["id"], [])
        d["entities"] = entity_map.get(
            d["id"], {"applicant": [], "previous_applicant": []}
        )
        results.append(d)
    return results


# ------------------------------------------------------------------
# Record CRUD
# ------------------------------------------------------------------

def insert_record(conn: sqlite3.Connection, record: dict) -> int | None:
    """Insert a record, returning the new row id or None if duplicate.

    Automatically resolves (or creates) location rows for the primary
    and previous business addresses.  Checks for duplicates *before*
    creating locations to avoid orphaned location rows.
    """
    from database import get_or_create_location

    existing = conn.execute(
        """SELECT 1 FROM license_records
           WHERE section_type = :section_type
             AND record_date = :record_date
             AND license_number = :license_number
             AND application_type = :application_type
           LIMIT 1""",
        record,
    ).fetchone()
    if existing:
        return None

    location_id = get_or_create_location(
        conn,
        record.get("business_location", ""),
        city=record.get("city", ""),
        state=record.get("state", "WA"),
        zip_code=record.get("zip_code", ""),
    )
    previous_location_id = get_or_create_location(
        conn,
        record.get("previous_business_location", ""),
        city=record.get("previous_city", ""),
        state=record.get("previous_state", ""),
        zip_code=record.get("previous_zip_code", ""),
    )
    try:
        cursor = conn.execute(
            """INSERT INTO license_records
               (section_type, record_date, business_name, location_id,
                applicants, license_type, application_type, license_number,
                contact_phone, previous_business_name, previous_applicants,
                previous_location_id, scraped_at)
               VALUES (:section_type, :record_date, :business_name, :location_id,
                       :applicants, :license_type, :application_type, :license_number,
                       :contact_phone, :previous_business_name, :previous_applicants,
                       :previous_location_id, :scraped_at)""",
            {
                **record,
                "location_id": location_id,
                "previous_location_id": previous_location_id,
            },
        )
        record_id = cursor.lastrowid
        _parse_and_link_entities(
            conn, record_id, record.get("applicants", ""), "applicant"
        )
        if record.get("previous_applicants"):
            _parse_and_link_entities(
                conn, record_id, record["previous_applicants"], "previous_applicant"
            )
        return record_id
    except sqlite3.IntegrityError:
        return None


# ------------------------------------------------------------------
# Search and filter queries
# ------------------------------------------------------------------

def search_records(
    conn: sqlite3.Connection,
    query: str = "",
    section_type: str = "",
    application_type: str = "",
    endorsement: str = "",
    city: str = "",
    date_from: str = "",
    date_to: str = "",
    page: int = 1,
    per_page: int = 50,
) -> tuple[list[dict], int]:
    """Search records with filters.  Returns (records, total_count)."""
    conditions = []
    params: list = []

    if query:
        safe_query = query.replace('"', '').replace("'", "")
        terms = safe_query.split()
        fts_query = " AND ".join(f'"{ t }"*' for t in terms if t)
        if fts_query:
            conditions.append(
                "lr.id IN (SELECT rowid FROM license_records_fts WHERE license_records_fts MATCH ?)"
            )
            params.append(fts_query)

    if section_type:
        conditions.append("lr.section_type = ?")
        params.append(section_type)

    if application_type:
        conditions.append("lr.application_type = ?")
        params.append(application_type)

    if endorsement:
        conditions.append("""
            lr.id IN (
                SELECT re.record_id FROM record_endorsements re
                JOIN license_endorsements le ON le.id = re.endorsement_id
                WHERE le.name = ?
            )""")
        params.append(endorsement)

    if city:
        conditions.append(
            "(COALESCE(NULLIF(loc.std_city, ''), loc.city) = ?"
            " OR COALESCE(NULLIF(ploc.std_city, ''), ploc.city) = ?)"
        )
        params.extend([city, city])

    if date_from:
        conditions.append("lr.record_date >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("lr.record_date <= ?")
        params.append(date_to)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Only JOIN locations in the count query when needed (city filter).
    if city:
        count_sql = f"""
            SELECT COUNT(*) FROM license_records lr
            LEFT JOIN locations loc ON loc.id = lr.location_id
            LEFT JOIN locations ploc ON ploc.id = lr.previous_location_id
            {where}
        """
    else:
        count_sql = f"SELECT COUNT(*) FROM license_records lr {where}"
    total = conn.execute(count_sql, params).fetchone()[0]

    offset = (page - 1) * per_page
    rows = conn.execute(
        f"""{_RECORD_SELECT}
            {where}
            ORDER BY lr.record_date DESC, lr.id DESC
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    return _hydrate_records(conn, rows), total


# In-process cache for filter dropdown options.  The underlying data
# changes at most twice daily (scraper runs), so a short TTL avoids
# running the ~10 ms city-list query on every search page load.
_filter_cache: dict = {}  # {"data": ..., "ts": float}
_FILTER_CACHE_TTL = 300  # seconds (5 minutes)


def get_filter_options(conn: sqlite3.Connection) -> dict:
    """Get distinct values for filter dropdowns (cached, 5-min TTL)."""
    now = time.monotonic()
    if _filter_cache and now - _filter_cache["ts"] < _FILTER_CACHE_TTL:
        return _filter_cache["data"]
    options: dict = {}
    for col in ["section_type", "application_type"]:
        rows = conn.execute(
            f"SELECT DISTINCT {col} FROM license_records "
            f"WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col}"
        ).fetchall()
        options[col] = [r[0] for r in rows]

    rows = conn.execute(
        "SELECT DISTINCT display_city FROM ("
        "  SELECT COALESCE(NULLIF(l.std_city, ''), l.city) AS display_city"
        "  FROM locations l"
        "  WHERE l.id IN ("
        "    SELECT location_id FROM license_records WHERE location_id IS NOT NULL"
        "    UNION"
        "    SELECT previous_location_id FROM license_records WHERE previous_location_id IS NOT NULL"
        "  )"
        ") WHERE display_city IS NOT NULL AND display_city != '' ORDER BY display_city"
    ).fetchall()
    options["city"] = [r[0] for r in rows]

    options["endorsement"] = get_endorsement_options(conn)
    _filter_cache["data"] = options
    _filter_cache["ts"] = now
    return options


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get summary statistics.

    Cheap aggregates (COUNT, SUM, MIN, MAX) are combined into a single
    query.  The two COUNT(DISTINCT ...) calls remain separate because
    combining them forces a slower full-table scan in SQLite.
    """
    agg = conn.execute("""
        SELECT
            COUNT(*) AS total_records,
            SUM(CASE WHEN section_type = 'new_application' THEN 1 ELSE 0 END) AS new_application_count,
            SUM(CASE WHEN section_type = 'approved' THEN 1 ELSE 0 END) AS approved_count,
            SUM(CASE WHEN section_type = 'discontinued' THEN 1 ELSE 0 END) AS discontinued_count,
            MIN(record_date) AS min_date,
            MAX(record_date) AS max_date
        FROM license_records
    """).fetchone()
    return {
        "total_records": agg["total_records"],
        "new_application_count": agg["new_application_count"],
        "approved_count": agg["approved_count"],
        "discontinued_count": agg["discontinued_count"],
        "date_range": (agg["min_date"], agg["max_date"]),
        "unique_businesses": conn.execute(
            "SELECT COUNT(DISTINCT business_name) FROM license_records"
        ).fetchone()[0],
        "unique_licenses": conn.execute(
            "SELECT COUNT(DISTINCT license_number) FROM license_records"
        ).fetchone()[0],
        "unique_entities": conn.execute(
            "SELECT COUNT(*) FROM entities"
        ).fetchone()[0],
        "last_scrape": conn.execute(
            "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
        ).fetchone(),
    }


# ------------------------------------------------------------------
# Single-record lookups
# ------------------------------------------------------------------

def get_record_by_id(conn: sqlite3.Connection, record_id: int) -> dict | None:
    """Fetch a single record with location data joined."""
    row = conn.execute(
        f"{_RECORD_SELECT} WHERE lr.id = ?", (record_id,)
    ).fetchone()
    return dict(row) if row else None


def get_related_records(
    conn: sqlite3.Connection, license_number: str, exclude_id: int
) -> list[dict]:
    """Fetch other records for the same license number."""
    rows = conn.execute(
        f"{_RECORD_SELECT} WHERE lr.license_number = ? AND lr.id != ? ORDER BY lr.record_date DESC",
        (license_number, exclude_id),
    ).fetchall()
    return [dict(r) for r in rows]


def get_entity_records(
    conn: sqlite3.Connection, entity_id: int
) -> list[dict]:
    """Fetch all records associated with an entity, with location data."""
    rows = conn.execute(
        f"""SELECT DISTINCT {_RECORD_COLUMNS} {_RECORD_JOINS}
            JOIN record_entities re ON re.record_id = lr.id
            WHERE re.entity_id = ?
            ORDER BY lr.record_date DESC, lr.id DESC""",
        (entity_id,),
    ).fetchall()
    return _hydrate_records(conn, rows)
