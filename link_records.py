"""Application→outcome record linking for WSLCB licensing tracker.

Links new_application records to their corresponding approved or
discontinued outcome records using a bidirectional nearest-neighbor
algorithm with a configurable date tolerance window.

The core insight: the WSLCB publishes "Notification Date" for new
applications and "Approved/Discontinued Date" for outcomes — these are
different events.  Outcome dates routinely precede notification dates
by 1–3 days (weekend offsets), so a tolerance window is essential.
"""
import logging
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DATE_TOLERANCE_DAYS = 7

# Application types that get linked to approved records (same type)
_APPROVAL_LINK_TYPES = {
    "RENEWAL", "NEW APPLICATION", "ASSUMPTION",
    "ADDED/CHANGE OF CLASS", "CHANGE OF CORPORATE OFFICER",
    "CHANGE OF LOCATION", "RESUME BUSINESS", "IN LIEU",
}

# DISC. LIQUOR SALES links to discontinued/DISCONTINUED
_DISC_LINK_TYPE = "DISC. LIQUOR SALES"

# Cutoff: applications older than this with no outcome → 'unknown'
PENDING_CUTOFF_DAYS = 180

# Date after which NEW APPLICATION approvals stopped being published
_DATA_GAP_CUTOFF = "2025-05-12"

# All application types eligible for outcome linking
LINKABLE_TYPES = _APPROVAL_LINK_TYPES | {_DISC_LINK_TYPE}


def outcome_filter_sql(
    status: str,
    *,
    record_alias: str = "lr",
) -> list[str]:
    """Return SQL WHERE-clause fragments for an outcome_status filter.

    Each element is a standalone condition to be ANDed into the query.
    The *record_alias* must be the table alias for ``license_records``.

    Valid *status* values: ``'approved'``, ``'discontinued'``,
    ``'pending'``, ``'data_gap'``, ``'unknown'``.
    Returns an empty list for unrecognised values.
    """
    r = record_alias
    linkable = ", ".join(f"'{t}'" for t in LINKABLE_TYPES)
    not_linked = f"{r}.id NOT IN (SELECT rl.new_app_id FROM record_links rl)"
    not_data_gap = (
        f"NOT ({r}.application_type = 'NEW APPLICATION'"
        f" AND {r}.record_date > '{_DATA_GAP_CUTOFF}')"
    )

    if status == "approved":
        return [
            f"{r}.id IN (SELECT rl.new_app_id FROM record_links rl "
            "JOIN license_records o ON o.id = rl.outcome_id "
            "WHERE o.section_type = 'approved')",
        ]
    if status == "discontinued":
        return [
            f"{r}.id IN (SELECT rl.new_app_id FROM record_links rl "
            "JOIN license_records o ON o.id = rl.outcome_id "
            "WHERE o.section_type = 'discontinued')",
        ]
    if status == "pending":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type IN ({linkable})",
            not_linked,
            f"{r}.record_date >= date('now', '-{PENDING_CUTOFF_DAYS} days')",
            not_data_gap,
        ]
    if status == "data_gap":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type = 'NEW APPLICATION'",
            f"{r}.record_date > '{_DATA_GAP_CUTOFF}'",
            not_linked,
        ]
    if status == "unknown":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type IN ({linkable})",
            not_linked,
            f"{r}.record_date < date('now', '-{PENDING_CUTOFF_DAYS} days')",
            not_data_gap,
        ]
    return []


def _date_add(date_str: str, days: int) -> str:
    """Add *days* to an ISO date string.  Returns ISO date string."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return (dt + timedelta(days=days)).strftime("%Y-%m-%d")


def build_all_links(conn: sqlite3.Connection) -> dict:
    """Run the full bidirectional linking algorithm.

    Clears ``record_links`` and rebuilds from scratch.
    Returns a summary dict with counts.
    """
    conn.execute("DELETE FROM record_links")

    high = 0
    medium = 0

    # --- Phase A: approval linking (same application_type) ---
    high_a, med_a = _link_approvals(conn)
    high += high_a
    medium += med_a

    # --- Phase B: DISC. LIQUOR SALES → discontinued ---
    high_b, med_b = _link_discontinuances(conn)
    high += high_b
    medium += med_b

    conn.commit()
    logger.info(
        "Record linking complete: %d high-confidence + %d medium-confidence = %d total",
        high, medium, high + medium,
    )
    return {"high": high, "medium": medium, "total": high + medium}


def _link_approvals(conn: sqlite3.Connection) -> tuple[int, int]:
    """Link new_application → approved records (same application_type).

    Returns (high_count, medium_count).
    """
    type_list = ", ".join(f"'{t}'" for t in _APPROVAL_LINK_TYPES)

    # Forward pass: for each new_app, find earliest approved record
    # for the same license_number + application_type within tolerance.
    forward = conn.execute(f"""
        SELECT na.id AS new_app_id, (
            SELECT ap.id FROM license_records ap
            WHERE ap.section_type = 'approved'
              AND ap.license_number = na.license_number
              AND ap.application_type = na.application_type
              AND ap.record_date >= date(na.record_date, '-{DATE_TOLERANCE_DAYS} days')
            ORDER BY ap.record_date ASC, ap.id ASC
            LIMIT 1
        ) AS outcome_id
        FROM license_records na
        WHERE na.section_type = 'new_application'
          AND na.application_type IN ({type_list})
    """).fetchall()

    # Build forward map: new_app_id → outcome_id
    fwd_map: dict[int, int] = {}
    for row in forward:
        if row["outcome_id"] is not None:
            fwd_map[row["new_app_id"]] = row["outcome_id"]

    if not fwd_map:
        return 0, 0

    # Backward pass: for each outcome that was claimed, find the latest
    # new_app that points to it.
    outcome_ids = set(fwd_map.values())
    backward = conn.execute(f"""
        SELECT ap.id AS outcome_id, (
            SELECT na.id FROM license_records na
            WHERE na.section_type = 'new_application'
              AND na.license_number = ap.license_number
              AND na.application_type = ap.application_type
              AND na.record_date <= date(ap.record_date, '+{DATE_TOLERANCE_DAYS} days')
              AND na.application_type IN ({type_list})
            ORDER BY na.record_date DESC, na.id DESC
            LIMIT 1
        ) AS new_app_id
        FROM license_records ap
        WHERE ap.section_type = 'approved'
          AND ap.id IN ({','.join('?' for _ in outcome_ids)})
    """, list(outcome_ids)).fetchall()

    # Build backward map: outcome_id → new_app_id
    bwd_map: dict[int, int] = {}
    for row in backward:
        if row["new_app_id"] is not None:
            bwd_map[row["outcome_id"]] = row["new_app_id"]

    # Mutual matches (high confidence): both passes agree
    high = 0
    medium = 0

    for new_app_id, outcome_id in fwd_map.items():
        if bwd_map.get(outcome_id) == new_app_id:
            _insert_link(conn, new_app_id, outcome_id, "high")
            high += 1

    # Forward-only matches (medium confidence): new_app points to an
    # outcome, but that outcome's backward pass prefers a different new_app.
    linked_new_apps = {na for na, oid in fwd_map.items() if bwd_map.get(oid) == na}
    for new_app_id, outcome_id in fwd_map.items():
        if new_app_id not in linked_new_apps:
            _insert_link(conn, new_app_id, outcome_id, "medium")
            medium += 1

    return high, medium


def _link_discontinuances(conn: sqlite3.Connection) -> tuple[int, int]:
    """Link DISC. LIQUOR SALES → discontinued/DISCONTINUED.

    Returns (high_count, medium_count).
    """
    # Forward pass
    forward = conn.execute(f"""
        SELECT na.id AS new_app_id, (
            SELECT dc.id FROM license_records dc
            WHERE dc.section_type = 'discontinued'
              AND dc.license_number = na.license_number
              AND dc.application_type = 'DISCONTINUED'
              AND dc.record_date >= date(na.record_date, '-{DATE_TOLERANCE_DAYS} days')
            ORDER BY dc.record_date ASC, dc.id ASC
            LIMIT 1
        ) AS outcome_id
        FROM license_records na
        WHERE na.section_type = 'new_application'
          AND na.application_type = '{_DISC_LINK_TYPE}'
    """).fetchall()

    fwd_map: dict[int, int] = {}
    for row in forward:
        if row["outcome_id"] is not None:
            fwd_map[row["new_app_id"]] = row["outcome_id"]

    if not fwd_map:
        return 0, 0

    # Backward pass
    outcome_ids = set(fwd_map.values())
    backward = conn.execute(f"""
        SELECT dc.id AS outcome_id, (
            SELECT na.id FROM license_records na
            WHERE na.section_type = 'new_application'
              AND na.license_number = dc.license_number
              AND na.application_type = '{_DISC_LINK_TYPE}'
              AND na.record_date <= date(dc.record_date, '+{DATE_TOLERANCE_DAYS} days')
            ORDER BY na.record_date DESC, na.id DESC
            LIMIT 1
        ) AS new_app_id
        FROM license_records dc
        WHERE dc.section_type = 'discontinued'
          AND dc.application_type = 'DISCONTINUED'
          AND dc.id IN ({','.join('?' for _ in outcome_ids)})
    """, list(outcome_ids)).fetchall()

    bwd_map: dict[int, int] = {}
    for row in backward:
        if row["new_app_id"] is not None:
            bwd_map[row["outcome_id"]] = row["new_app_id"]

    high = 0
    medium = 0

    for new_app_id, outcome_id in fwd_map.items():
        if bwd_map.get(outcome_id) == new_app_id:
            _insert_link(conn, new_app_id, outcome_id, "high")
            high += 1

    linked_new_apps = {na for na, oid in fwd_map.items() if bwd_map.get(oid) == na}
    for new_app_id, outcome_id in fwd_map.items():
        if new_app_id not in linked_new_apps:
            _insert_link(conn, new_app_id, outcome_id, "medium")
            medium += 1

    return high, medium


def _insert_link(
    conn: sqlite3.Connection,
    new_app_id: int,
    outcome_id: int,
    confidence: str,
) -> None:
    """Insert a record_links row, computing days_gap from DB dates."""
    dates = conn.execute(
        """SELECT
               (SELECT record_date FROM license_records WHERE id = ?) AS new_date,
               (SELECT record_date FROM license_records WHERE id = ?) AS out_date""",
        (new_app_id, outcome_id),
    ).fetchone()
    days_gap = None
    if dates["new_date"] and dates["out_date"]:
        d1 = datetime.strptime(dates["new_date"], "%Y-%m-%d")
        d2 = datetime.strptime(dates["out_date"], "%Y-%m-%d")
        days_gap = (d2 - d1).days

    conn.execute(
        """INSERT OR IGNORE INTO record_links
           (new_app_id, outcome_id, confidence, days_gap)
           VALUES (?, ?, ?, ?)""",
        (new_app_id, outcome_id, confidence, days_gap),
    )


def link_new_record(
    conn: sqlite3.Connection, record_id: int,
) -> int | None:
    """Incrementally link a single newly-inserted record.

    Called after ``insert_record()`` during scraping.  Attempts to find
    a match in both directions (the new record could be either a
    new_application or an outcome).

    Returns the linked record's id if a link was created, else None.
    """
    rec = conn.execute(
        """SELECT id, section_type, application_type, license_number,
                  record_date
           FROM license_records WHERE id = ?""",
        (record_id,),
    ).fetchone()
    if not rec:
        return None

    section = rec["section_type"]
    app_type = rec["application_type"]
    lic_num = rec["license_number"]
    rec_date = rec["record_date"]

    if section == "new_application":
        return _link_new_app(conn, record_id, app_type, lic_num, rec_date)
    elif section in ("approved", "discontinued"):
        return _link_outcome(conn, record_id, section, app_type, lic_num, rec_date)
    return None


def _link_new_app(
    conn: sqlite3.Connection,
    new_app_id: int,
    app_type: str,
    lic_num: str,
    new_date: str,
) -> int | None:
    """Try to link a new_application record to an existing outcome."""
    if app_type == _DISC_LINK_TYPE:
        outcome = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'discontinued'
              AND license_number = ?
              AND application_type = 'DISCONTINUED'
              AND record_date >= date(?, '-{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date ASC, id ASC
            LIMIT 1
        """, (lic_num, new_date)).fetchone()
    elif app_type in _APPROVAL_LINK_TYPES:
        outcome = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'approved'
              AND license_number = ?
              AND application_type = ?
              AND record_date >= date(?, '-{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date ASC, id ASC
            LIMIT 1
        """, (lic_num, app_type, new_date)).fetchone()
    else:
        return None

    if not outcome:
        return None

    # Verify backward pass: is this new_app the best match for that outcome?
    outcome_id = outcome["id"]
    out_rec = conn.execute(
        "SELECT record_date FROM license_records WHERE id = ?",
        (outcome_id,),
    ).fetchone()
    if not out_rec:
        return None

    if app_type == _DISC_LINK_TYPE:
        best_new = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'new_application'
              AND license_number = ?
              AND application_type = '{_DISC_LINK_TYPE}'
              AND record_date <= date(?, '+{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date DESC, id DESC
            LIMIT 1
        """, (lic_num, out_rec["record_date"])).fetchone()
    else:
        best_new = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'new_application'
              AND license_number = ?
              AND application_type = ?
              AND record_date <= date(?, '+{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date DESC, id DESC
            LIMIT 1
        """, (lic_num, app_type, out_rec["record_date"])).fetchone()

    if best_new and best_new["id"] == new_app_id:
        confidence = "high"
    else:
        confidence = "medium"

    _insert_link(conn, new_app_id, outcome_id, confidence)
    return outcome_id


def _link_outcome(
    conn: sqlite3.Connection,
    outcome_id: int,
    section: str,
    app_type: str,
    lic_num: str,
    out_date: str,
) -> int | None:
    """Try to link an outcome record to an existing new_application."""
    if section == "discontinued" and app_type == "DISCONTINUED":
        best_new = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'new_application'
              AND license_number = ?
              AND application_type = '{_DISC_LINK_TYPE}'
              AND record_date <= date(?, '+{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date DESC, id DESC
            LIMIT 1
        """, (lic_num, out_date)).fetchone()
    elif section == "approved" and app_type in _APPROVAL_LINK_TYPES:
        best_new = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'new_application'
              AND license_number = ?
              AND application_type = ?
              AND record_date <= date(?, '+{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date DESC, id DESC
            LIMIT 1
        """, (lic_num, app_type, out_date)).fetchone()
    else:
        return None

    if not best_new:
        return None

    new_app_id = best_new["id"]
    new_rec = conn.execute(
        "SELECT record_date FROM license_records WHERE id = ?",
        (new_app_id,),
    ).fetchone()
    if not new_rec:
        return None

    # Verify forward pass
    if section == "discontinued":
        best_out = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'discontinued'
              AND license_number = ?
              AND application_type = 'DISCONTINUED'
              AND record_date >= date(?, '-{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date ASC, id ASC
            LIMIT 1
        """, (lic_num, new_rec["record_date"])).fetchone()
    else:
        best_out = conn.execute(f"""
            SELECT id FROM license_records
            WHERE section_type = 'approved'
              AND license_number = ?
              AND application_type = ?
              AND record_date >= date(?, '-{DATE_TOLERANCE_DAYS} days')
            ORDER BY record_date ASC, id ASC
            LIMIT 1
        """, (lic_num, app_type, new_rec["record_date"])).fetchone()

    if best_out and best_out["id"] == outcome_id:
        confidence = "high"
    else:
        confidence = "medium"

    _insert_link(conn, new_app_id, outcome_id, confidence)
    return new_app_id


def get_outcome_status(record: dict, link: dict | None) -> dict:
    """Compute the outcome status for a record.

    Returns a dict with keys: status, label, css_class, icon, detail,
    linked_record_id, confidence.
    """
    if record["section_type"] != "new_application":
        return {"status": None}

    app_type = record["application_type"]

    # Not a linkable type
    if app_type not in _APPROVAL_LINK_TYPES and app_type != _DISC_LINK_TYPE:
        return {"status": None}

    if link:
        # Determine if this is an approval or discontinuance link
        outcome_section = link.get("outcome_section_type", "")
        days = link.get("days_gap")
        days_label = f"{abs(days)} day{'s' if abs(days) != 1 else ''}" if days is not None else ""

        if outcome_section == "approved":
            return {
                "status": "approved",
                "label": "Approved",
                "icon": "✅",
                "css_bg": "bg-green-50",
                "css_border": "border-green-200",
                "css_text": "text-green-800",
                "css_accent": "text-green-600",
                "detail": f"Approved on {link['outcome_date']}" + (f" ({days_label} after application)" if days_label else ""),
                "linked_record_id": link["outcome_id"],
                "confidence": link["confidence"],
                "outcome_date": link["outcome_date"],
                "days_gap": days,
            }
        elif outcome_section == "discontinued":
            return {
                "status": "discontinued",
                "label": "Discontinued",
                "icon": "🚫",
                "css_bg": "bg-red-50",
                "css_border": "border-red-200",
                "css_text": "text-red-800",
                "css_accent": "text-red-600",
                "detail": f"Discontinued on {link['outcome_date']}" + (f" ({days_label} after filing)" if days_label else ""),
                "linked_record_id": link["outcome_id"],
                "confidence": link["confidence"],
                "outcome_date": link["outcome_date"],
                "days_gap": days,
            }

    # No link — determine why
    rec_date = record.get("record_date", "")

    # Data gap: post-May 2025 NEW APPLICATION records
    if app_type == "NEW APPLICATION" and rec_date > _DATA_GAP_CUTOFF:
        return {
            "status": "data_gap",
            "label": "Data Unavailable",
            "icon": "📁",
            "css_bg": "bg-slate-50",
            "css_border": "border-slate-200",
            "css_text": "text-slate-700",
            "css_accent": "text-slate-500",
            "detail": "The WSLCB stopped publishing NEW APPLICATION approvals after May 2025 due to a data transfer issue.",
            "linked_record_id": None,
            "confidence": None,
        }

    # Pending or unknown based on age
    if rec_date:
        try:
            filed_date = datetime.strptime(rec_date, "%Y-%m-%d")
            age_days = (datetime.now() - filed_date).days
            if age_days <= PENDING_CUTOFF_DAYS:
                return {
                    "status": "pending",
                    "label": "Pending",
                    "icon": "⏳",
                    "css_bg": "bg-amber-50",
                    "css_border": "border-amber-200",
                    "css_text": "text-amber-800",
                    "css_accent": "text-amber-600",
                    "detail": f"Filed {age_days} day{'s' if age_days != 1 else ''} ago. Typical time to approval: 50–90 days.",
                    "linked_record_id": None,
                    "confidence": None,
                }
        except ValueError:
            pass

    return {
        "status": "unknown",
        "label": "No Outcome Recorded",
        "icon": "❓",
        "css_bg": "bg-gray-50",
        "css_border": "border-gray-200",
        "css_text": "text-gray-600",
        "css_accent": "text-gray-400",
        "detail": "No matching approved or discontinued record was found.",
        "linked_record_id": None,
        "confidence": None,
    }


def get_reverse_link_info(
    conn: sqlite3.Connection, record: dict,
) -> dict | None:
    """For an outcome record, get info about the originating application.

    Returns a dict with new_app_id, confidence, days_gap, or None.
    """
    if record["section_type"] not in ("approved", "discontinued"):
        return None

    row = conn.execute(
        """SELECT rl.new_app_id, rl.confidence, rl.days_gap,
                  lr.record_date AS new_app_date,
                  lr.business_name AS new_app_business_name,
                  lr.application_type AS new_app_type
           FROM record_links rl
           JOIN license_records lr ON lr.id = rl.new_app_id
           WHERE rl.outcome_id = ?
           LIMIT 1""",
        (record["id"],),
    ).fetchone()
    return dict(row) if row else None
