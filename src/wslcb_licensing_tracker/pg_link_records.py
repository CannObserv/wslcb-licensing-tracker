"""Async PostgreSQL application-outcome record linking for WSLCB licensing tracker.

Async equivalent of link_records.py. Uses SQLAlchemy Core expressions and
the table objects from models.py.

Links new_application records to their corresponding approved or discontinued
outcome records using a bidirectional nearest-neighbor algorithm with a
configurable date tolerance window.
"""

import logging
from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import license_records, record_links

logger = logging.getLogger(__name__)

DATE_TOLERANCE_DAYS = 7

# Application types that get linked to approved records (same type)
_APPROVAL_LINK_TYPES = {
    "RENEWAL",
    "NEW APPLICATION",
    "ASSUMPTION",
    "ADDED/CHANGE OF CLASS",
    "CHANGE OF CORPORATE OFFICER",
    "CHANGE OF LOCATION",
    "RESUME BUSINESS",
    "IN LIEU",
}

# DISC. LIQUOR SALES links to discontinued/DISCONTINUED
_DISC_LINK_TYPE = "DISC. LIQUOR SALES"

# Cutoff: applications older than this with no outcome -> 'unknown'
PENDING_CUTOFF_DAYS = 180

# Date after which NEW APPLICATION approvals stopped being published
DATA_GAP_CUTOFF = "2025-05-12"

# All application types eligible for outcome linking
LINKABLE_TYPES = _APPROVAL_LINK_TYPES | {_DISC_LINK_TYPE}


def outcome_filter_sql(
    status: str,
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
    not_linked = f"NOT EXISTS (SELECT 1 FROM record_links rl WHERE rl.new_app_id = {r}.id)"
    not_data_gap = (
        f"NOT ({r}.application_type = 'NEW APPLICATION' AND {r}.record_date > '{DATA_GAP_CUTOFF}')"
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
            f"{r}.record_date::date >= CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'",
            not_data_gap,
        ]
    if status == "data_gap":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type = 'NEW APPLICATION'",
            f"{r}.record_date > '{DATA_GAP_CUTOFF}'",
            not_linked,
        ]
    if status == "unknown":
        return [
            f"{r}.section_type = 'new_application'",
            f"{r}.application_type IN ({linkable})",
            not_linked,
            f"{r}.record_date::date < CURRENT_DATE - interval '{PENDING_CUTOFF_DAYS} days'",
            not_data_gap,
        ]
    return []


def get_outcome_status(record: dict, link: dict | None) -> dict:  # noqa: C901, PLR0911
    """Compute the semantic outcome status for a record.

    Returns a dict with keys: status, label, detail, linked_record_id,
    confidence.  Does NOT include presentation properties (CSS, icons);
    call ``display.format_outcome()`` to add those.
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
            detail = f"Approved on {link['outcome_date']}"
            if days_label:
                detail += f" ({days_label} after application)"
            return {
                "status": "approved",
                "label": "Approved",
                "detail": detail,
                "linked_record_id": link["outcome_id"],
                "confidence": link["confidence"],
                "outcome_date": link["outcome_date"],
                "days_gap": days,
            }
        if outcome_section == "discontinued":
            detail = f"Discontinued on {link['outcome_date']}"
            if days_label:
                detail += f" ({days_label} after filing)"
            return {
                "status": "discontinued",
                "label": "Discontinued",
                "detail": detail,
                "linked_record_id": link["outcome_id"],
                "confidence": link["confidence"],
                "outcome_date": link["outcome_date"],
                "days_gap": days,
            }

    # No link - determine why
    rec_date = record.get("record_date", "")

    # Data gap: post-May 2025 NEW APPLICATION records
    if app_type == "NEW APPLICATION" and rec_date > DATA_GAP_CUTOFF:
        return {
            "status": "data_gap",
            "label": "Data Unavailable",
            "detail": (
                "The WSLCB stopped publishing NEW APPLICATION approvals "
                "after May 2025 due to a data transfer issue."
            ),
            "linked_record_id": None,
            "confidence": None,
        }

    # Pending or unknown based on age
    if rec_date:
        try:
            filed_date = datetime.strptime(rec_date, "%Y-%m-%d").replace(tzinfo=UTC)
            age_days = (datetime.now(UTC) - filed_date).days
            if age_days <= PENDING_CUTOFF_DAYS:
                return {
                    "status": "pending",
                    "label": "Pending",
                    "detail": (
                        f"Filed {age_days} day{'s' if age_days != 1 else ''} ago. "
                        "Typical time to approval: 50-90 days."
                    ),
                    "linked_record_id": None,
                    "confidence": None,
                }
        except ValueError:
            pass

    return {
        "status": "unknown",
        "label": "No Outcome Recorded",
        "detail": "No matching approved or discontinued record was found.",
        "linked_record_id": None,
        "confidence": None,
    }


async def build_all_links(conn: AsyncConnection) -> dict:
    """Run the full bidirectional linking algorithm.

    TRUNCATEs ``record_links`` and rebuilds from scratch.
    Returns a summary dict with counts.
    """
    await conn.execute(text("TRUNCATE record_links"))

    high, medium = await _link_section(conn, mode="approval")
    disc_high, disc_medium = await _link_section(conn, mode="discontinuance")

    total_high = high + disc_high
    total_medium = medium + disc_medium

    logger.info(
        "Record linking complete: %d high-confidence + %d medium-confidence = %d total",
        total_high,
        total_medium,
        total_high + total_medium,
    )
    return {"high": total_high, "medium": total_medium, "total": total_high + total_medium}


async def _link_section(  # noqa: C901
    conn: AsyncConnection,
    *,
    mode: str,
) -> tuple[int, int]:
    """Bulk bidirectional linking for one mode.

    *mode* is ``'approval'`` or ``'discontinuance'``.

    Returns (high_count, medium_count).
    """
    if mode == "approval":
        na_type_filter = ", ".join(f"'{t}'" for t in _APPROVAL_LINK_TYPES)
        na_where = f"na.application_type IN ({na_type_filter})"
        out_section = "approved"
        fwd_type_match = "out.application_type = na.application_type"
        bwd_type_match = "na.application_type = out.application_type"
    elif mode == "discontinuance":
        na_where = f"na.application_type = '{_DISC_LINK_TYPE}'"
        out_section = "discontinued"
        fwd_type_match = "out.application_type = 'DISCONTINUED'"
        bwd_type_match = f"na.application_type = '{_DISC_LINK_TYPE}'"
    else:
        msg = f"Unknown mode: {mode!r}"
        raise ValueError(msg)

    # Forward pass: for each new_app, find earliest outcome within tolerance.
    tol = f"interval '{DATE_TOLERANCE_DAYS} days'"
    fwd_result = await conn.execute(
        text(f"""
        SELECT na.id AS new_app_id, (
            SELECT out.id FROM license_records out
            WHERE out.section_type = '{out_section}'
              AND out.license_number = na.license_number
              AND {fwd_type_match}
              AND out.record_date::date >= na.record_date::date - {tol}
            ORDER BY out.record_date ASC, out.id ASC
            LIMIT 1
        ) AS outcome_id
        FROM license_records na
        WHERE na.section_type = 'new_application'
          AND {na_where}
    """)
    )

    fwd_map: dict[int, int] = {}
    for row in fwd_result.mappings():
        if row["outcome_id"] is not None:
            fwd_map[row["new_app_id"]] = row["outcome_id"]

    if not fwd_map:
        return 0, 0

    # Backward pass: for each claimed outcome, find the best new_app.
    outcome_ids = list(set(fwd_map.values()))
    # Build a parameterised IN list
    placeholders = ", ".join(str(oid) for oid in outcome_ids)
    bwd_result = await conn.execute(
        text(f"""
        SELECT out.id AS outcome_id, (
            SELECT na.id FROM license_records na
            WHERE na.section_type = 'new_application'
              AND na.license_number = out.license_number
              AND {bwd_type_match}
              AND na.record_date::date <= out.record_date::date + {tol}
            ORDER BY na.record_date DESC, na.id DESC
            LIMIT 1
        ) AS new_app_id
        FROM license_records out
        WHERE out.section_type = '{out_section}'
          AND out.id IN ({placeholders})
    """)
    )

    bwd_map: dict[int, int] = {}
    for row in bwd_result.mappings():
        if row["new_app_id"] is not None:
            bwd_map[row["outcome_id"]] = row["new_app_id"]

    # Mutual matches = high confidence; forward-only = medium.
    high = 0
    medium = 0

    for new_app_id, outcome_id in fwd_map.items():
        if bwd_map.get(outcome_id) == new_app_id:
            await _insert_link(conn, new_app_id, outcome_id, "high")
            high += 1

    linked_new_apps = {na for na, oid in fwd_map.items() if bwd_map.get(oid) == na}
    for new_app_id, outcome_id in fwd_map.items():
        if new_app_id not in linked_new_apps:
            await _insert_link(conn, new_app_id, outcome_id, "medium")
            medium += 1

    return high, medium


async def _insert_link(
    conn: AsyncConnection,
    new_app_id: int,
    outcome_id: int,
    confidence: str,
) -> None:
    """Insert a record_links row, computing days_gap from DB dates.

    Also backfills ``previous_location_id`` on the outcome record when it is
    a CHANGE OF LOCATION approved record with no prior value and the
    new-application record carries one.
    """
    dates_result = await conn.execute(
        text("""
            SELECT
                (SELECT record_date FROM license_records WHERE id = :new_id) AS new_date,
                (SELECT record_date FROM license_records WHERE id = :out_id) AS out_date
        """),
        {"new_id": new_app_id, "out_id": outcome_id},
    )
    dates = dates_result.mappings().fetchone()

    days_gap = None
    if dates and dates["new_date"] and dates["out_date"]:
        d1 = datetime.strptime(dates["new_date"], "%Y-%m-%d").replace(tzinfo=UTC)
        d2 = datetime.strptime(dates["out_date"], "%Y-%m-%d").replace(tzinfo=UTC)
        days_gap = (d2 - d1).days

    stmt = (
        pg_insert(record_links)
        .values(
            new_app_id=new_app_id,
            outcome_id=outcome_id,
            confidence=confidence,
            days_gap=days_gap,
        )
        .on_conflict_do_nothing(constraint="uq_record_links")
    )
    await conn.execute(stmt)

    # For CHANGE OF LOCATION links: copy previous_location_id from the
    # new_application to the approved outcome when the outcome lacks it.
    await conn.execute(
        text("""
        UPDATE license_records
        SET previous_location_id = (
            SELECT previous_location_id FROM license_records WHERE id = :new_id
        )
        WHERE id = :out_id
          AND application_type = 'CHANGE OF LOCATION'
          AND previous_location_id IS NULL
          AND (SELECT previous_location_id FROM license_records WHERE id = :new_id) IS NOT NULL
        """),
        {"new_id": new_app_id, "out_id": outcome_id},
    )


async def link_new_record(
    conn: AsyncConnection,
    record_id: int,
) -> int | None:
    """Incrementally link a single newly-inserted record.

    Called after ``insert_record()`` during scraping.  Attempts to find
    a match in both directions (the new record could be either a
    new_application or an outcome).

    Returns the linked record's id if a link was created, else None.
    """
    result = await conn.execute(
        select(
            license_records.c.id,
            license_records.c.section_type,
            license_records.c.application_type,
            license_records.c.license_number,
            license_records.c.record_date,
        ).where(license_records.c.id == record_id)
    )
    rec = result.mappings().fetchone()
    if not rec:
        return None

    section = rec["section_type"]
    app_type = rec["application_type"]
    lic_num = rec["license_number"]
    rec_date = rec["record_date"]

    if section == "new_application":
        return await _link_incremental(
            conn,
            direction="forward",
            record_id=record_id,
            app_type=app_type,
            lic_num=lic_num,
            record_date=rec_date,
        )
    if section in ("approved", "discontinued"):
        return await _link_incremental(
            conn,
            direction="backward",
            record_id=record_id,
            app_type=app_type,
            lic_num=lic_num,
            record_date=rec_date,
            outcome_section=section,
        )
    return None


async def _link_incremental(  # noqa: PLR0911, PLR0913
    conn: AsyncConnection,
    *,
    direction: str,
    record_id: int,
    app_type: str,
    lic_num: str,
    record_date: str,
    outcome_section: str | None = None,
) -> int | None:
    """Incremental bidirectional linking for a single record.

    *direction* is ``'forward'`` (new_app seeking outcome) or
    ``'backward'`` (outcome seeking new_app).
    """
    if direction == "forward":
        # new_application seeking an outcome
        if app_type == _DISC_LINK_TYPE:
            out_section = "discontinued"
            out_type_val = "DISCONTINUED"
            na_type_val = _DISC_LINK_TYPE
        elif app_type in _APPROVAL_LINK_TYPES:
            out_section = "approved"
            out_type_val = app_type
            na_type_val = app_type
        else:
            return None

        # Forward: find earliest outcome for this new_app.
        outcome_result = await conn.execute(
            text(f"""
            SELECT id FROM license_records
            WHERE section_type = :out_section
              AND license_number = :lic_num
              AND application_type = :out_type_val
              AND record_date::date >= :record_date::date - interval '{DATE_TOLERANCE_DAYS} days'
            ORDER BY record_date ASC, id ASC
            LIMIT 1
            """),
            {
                "out_section": out_section,
                "lic_num": lic_num,
                "out_type_val": out_type_val,
                "record_date": record_date,
            },
        )
        outcome = outcome_result.mappings().fetchone()
        if not outcome:
            return None

        outcome_id = outcome["id"]
        out_date_result = await conn.execute(
            select(license_records.c.record_date).where(license_records.c.id == outcome_id)
        )
        out_date = out_date_result.scalar_one()

        # Backward verification: is this new_app the best for that outcome?
        best_new_result = await conn.execute(
            text(f"""
            SELECT id FROM license_records
            WHERE section_type = 'new_application'
              AND license_number = :lic_num
              AND application_type = :na_type_val
              AND record_date::date <= :out_date::date + interval '{DATE_TOLERANCE_DAYS} days'
            ORDER BY record_date DESC, id DESC
            LIMIT 1
            """),
            {"lic_num": lic_num, "na_type_val": na_type_val, "out_date": out_date},
        )
        best_new = best_new_result.mappings().fetchone()

        confidence = "high" if (best_new and best_new["id"] == record_id) else "medium"
        await _insert_link(conn, record_id, outcome_id, confidence)
        return outcome_id

    if direction == "backward":
        # outcome seeking a new_application
        if outcome_section is None:
            return None
        if outcome_section == "discontinued" and app_type == "DISCONTINUED":
            na_type_val = _DISC_LINK_TYPE
            out_type_val = "DISCONTINUED"
        elif outcome_section == "approved" and app_type in _APPROVAL_LINK_TYPES:
            na_type_val = app_type
            out_type_val = app_type
        else:
            return None

        # Backward: find latest new_app for this outcome.
        best_new_result = await conn.execute(
            text(f"""
            SELECT id FROM license_records
            WHERE section_type = 'new_application'
              AND license_number = :lic_num
              AND application_type = :na_type_val
              AND record_date::date <= :record_date::date + interval '{DATE_TOLERANCE_DAYS} days'
            ORDER BY record_date DESC, id DESC
            LIMIT 1
            """),
            {"lic_num": lic_num, "na_type_val": na_type_val, "record_date": record_date},
        )
        best_new = best_new_result.mappings().fetchone()
        if not best_new:
            return None

        new_app_id = best_new["id"]
        new_date_result = await conn.execute(
            select(license_records.c.record_date).where(license_records.c.id == new_app_id)
        )
        new_date = new_date_result.scalar_one()

        # Forward verification: is this outcome the best for that new_app?
        best_out_result = await conn.execute(
            text(f"""
            SELECT id FROM license_records
            WHERE section_type = :outcome_section
              AND license_number = :lic_num
              AND application_type = :out_type_val
              AND record_date::date >= :new_date::date - interval '{DATE_TOLERANCE_DAYS} days'
            ORDER BY record_date ASC, id ASC
            LIMIT 1
            """),
            {
                "outcome_section": outcome_section,
                "lic_num": lic_num,
                "out_type_val": out_type_val,
                "new_date": new_date,
            },
        )
        best_out = best_out_result.mappings().fetchone()

        confidence = "high" if (best_out and best_out["id"] == record_id) else "medium"
        await _insert_link(conn, new_app_id, record_id, confidence)
        return new_app_id

    return None


async def get_reverse_link_info(
    conn: AsyncConnection,
    record: dict,
) -> dict | None:
    """For an outcome record, get info about the originating application.

    Returns a dict with new_app_id, confidence, days_gap, or None.
    """
    if record["section_type"] not in ("approved", "discontinued"):
        return None

    lr_new = license_records.alias("lr_new")
    stmt = (
        select(
            record_links.c.new_app_id,
            record_links.c.confidence,
            record_links.c.days_gap,
            lr_new.c.record_date.label("new_app_date"),
            lr_new.c.business_name.label("new_app_business_name"),
            lr_new.c.application_type.label("new_app_type"),
        )
        .join(lr_new, lr_new.c.id == record_links.c.new_app_id)
        .where(record_links.c.outcome_id == record["id"])
        .limit(1)
    )
    result = await conn.execute(stmt)
    row = result.mappings().fetchone()
    return dict(row) if row else None


async def get_record_links_bulk(
    conn: AsyncConnection,
    new_app_ids: list[int],
) -> dict[int, dict]:
    """Batch fetch record_links for a list of new_app_ids.

    Returns a dict keyed by new_app_id. Each value is a mapping with
    outcome_id, confidence, days_gap, outcome_section_type, outcome_date,
    outcome_application_type.
    """
    if not new_app_ids:
        return {}

    outcome_lr = license_records.alias("outcome_lr")
    stmt = (
        select(
            record_links.c.new_app_id,
            record_links.c.outcome_id,
            record_links.c.confidence,
            record_links.c.days_gap,
            outcome_lr.c.section_type.label("outcome_section_type"),
            outcome_lr.c.record_date.label("outcome_date"),
            outcome_lr.c.application_type.label("outcome_application_type"),
        )
        .join(outcome_lr, outcome_lr.c.id == record_links.c.outcome_id)
        .where(record_links.c.new_app_id.in_(new_app_ids))
    )
    rows = (await conn.execute(stmt)).mappings().all()
    result: dict[int, dict] = {}
    for r in rows:
        nid = r["new_app_id"]
        if nid not in result or r["confidence"] == "high":
            result[nid] = dict(r)
    return result
