"""Async PostgreSQL database helpers for the WSLCB licensing tracker.

Async equivalents of the location, source, and provenance helpers in db.py.
Uses SQLAlchemy Core expressions against the table objects in models.py.

Also re-exports shared constants (DATA_DIR, source type IDs, SOURCE_ROLE_PRIORITY,
US_STATES) that were previously defined in db.py.

Also contains pipeline constants (DATA_GAP_CUTOFF, LINKABLE_TYPES,
PENDING_CUTOFF_DAYS) and outcome_filter_sql() shared by pg_link_records
and the pg_queries_* modules.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import locations, record_sources, sources
from .text_utils import _normalize_raw_address

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared constants (previously in db.py)
# ---------------------------------------------------------------------------

# Source type constants (fixed IDs — must match seed data in pg_schema.py)
SOURCE_TYPE_LIVE_SCRAPE = 1
SOURCE_TYPE_CO_ARCHIVE = 2
SOURCE_TYPE_INTERNET_ARCHIVE = 3
SOURCE_TYPE_CO_DIFF_ARCHIVE = 4
SOURCE_TYPE_MANUAL = 5

WSLCB_SOURCE_URL = "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp"

# All persistent data (HTML snapshots etc.) lives under DATA_DIR.
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data"))

# Source-role priority used when selecting the "best" source for display.
# Lower value = higher priority.  Imported by display.py to avoid circular imports.
SOURCE_ROLE_PRIORITY: dict[str, int] = {"first_seen": 0, "repaired": 1, "confirmed": 2}

# US state code → full name mapping.  Used by the state filter dropdown and
# the address validation layer.
US_STATES: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

# ---------------------------------------------------------------------------
# Application pipeline constants (shared by pg_link_records, pg_queries_*)
# ---------------------------------------------------------------------------

# Cutoff: applications older than this with no outcome -> 'unknown'
PENDING_CUTOFF_DAYS = 180

# Date after which NEW APPLICATION approvals stopped being published
DATA_GAP_CUTOFF = "2025-05-12"

# All application types eligible for outcome linking
LINKABLE_TYPES: frozenset[str] = frozenset(
    {
        "RENEWAL",
        "NEW APPLICATION",
        "ASSUMPTION",
        "ADDED/CHANGE OF CLASS",
        "CHANGE OF CORPORATE OFFICER",
        "CHANGE OF LOCATION",
        "RESUME BUSINESS",
        "IN LIEU",
        "DISC. LIQUOR SALES",
    }
)


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


# ------------------------------------------------------------------
# Location helpers
# ------------------------------------------------------------------


async def get_or_create_location(
    conn: AsyncConnection,
    raw_address: str | None,
    city: str = "",
    state: str = "WA",
    zip_code: str = "",
) -> int | None:
    """Return the location id for *raw_address*, creating if needed.

    Returns None if raw_address is empty/None/whitespace-only.
    Uses INSERT ... ON CONFLICT DO NOTHING + RETURNING for atomicity.
    """
    if not raw_address or not raw_address.strip():
        return None

    normalized = _normalize_raw_address(raw_address)

    # Try insert; ON CONFLICT returns no row
    stmt = (
        pg_insert(locations)
        .values(raw_address=normalized, city=city, state=state, zip_code=zip_code)
        .on_conflict_do_nothing(index_elements=["raw_address"])
        .returning(locations.c.id)
    )
    result = await conn.execute(stmt)
    row = result.first()
    if row:
        return row[0]

    # Conflict — fetch existing
    result = await conn.execute(select(locations.c.id).where(locations.c.raw_address == normalized))
    return result.scalar_one()


# ------------------------------------------------------------------
# Source helpers
# ------------------------------------------------------------------


async def get_or_create_source(  # noqa: PLR0913
    conn: AsyncConnection,
    source_type_id: int,
    snapshot_path: str | None = None,
    url: str | None = None,
    captured_at: datetime | None = None,
    scrape_log_id: int | None = None,
    metadata: dict | None = None,
) -> int:
    """Return the source id for the given type + snapshot_path, creating if needed.

    When snapshot_path is not None, uses ON CONFLICT DO NOTHING on the
    (source_type_id, snapshot_path) unique constraint.

    When snapshot_path is None, PostgreSQL treats NULLs as distinct in
    UNIQUE constraints, so we use manual lookup-before-insert logic
    (same as the SQLite version in db.py).
    """
    meta_json = json.dumps(metadata) if metadata else "{}"

    if snapshot_path is not None:
        stmt = (
            pg_insert(sources)
            .values(
                source_type_id=source_type_id,
                snapshot_path=snapshot_path,
                url=url,
                captured_at=captured_at,
                scrape_log_id=scrape_log_id,
                metadata=meta_json,
            )
            .on_conflict_do_nothing(constraint="uq_sources_type_path")
            .returning(sources.c.id)
        )
        result = await conn.execute(stmt)
        row = result.first()
        if row:
            return row[0]
        # Conflict — fetch existing
        result = await conn.execute(
            select(sources.c.id).where(
                sources.c.source_type_id == source_type_id,
                sources.c.snapshot_path == snapshot_path,
            )
        )
        row = result.first()
        if row is None:
            msg = f"Source row vanished for type={source_type_id}, path={snapshot_path!r}"
            raise RuntimeError(msg)
        return row[0]

    # NULL snapshot_path — PostgreSQL NULLs are distinct in UNIQUE constraints,
    # so ON CONFLICT DO NOTHING won't fire. Use SAVEPOINT to make the insert
    # atomic: attempt insert; on IntegrityError roll back to the savepoint and
    # fetch the row that won the race.
    async def _select_null_path() -> int:
        if scrape_log_id is not None:
            r = await conn.execute(
                select(sources.c.id).where(
                    sources.c.source_type_id == source_type_id,
                    sources.c.snapshot_path.is_(None),
                    sources.c.scrape_log_id == scrape_log_id,
                )
            )
        else:
            r = await conn.execute(
                select(sources.c.id).where(
                    sources.c.source_type_id == source_type_id,
                    sources.c.snapshot_path.is_(None),
                    sources.c.scrape_log_id.is_(None),
                )
            )
        row = r.scalar_one_or_none()
        if row is None:
            msg = (
                f"Source row vanished for type={source_type_id},"
                f" scrape_log_id={scrape_log_id!r} (NULL snapshot_path)"
            )
            raise RuntimeError(msg)
        return row

    await conn.execute(text("SAVEPOINT get_or_create_source_null"))
    try:
        stmt = (
            pg_insert(sources)
            .values(
                source_type_id=source_type_id,
                snapshot_path=None,
                url=url,
                captured_at=captured_at,
                scrape_log_id=scrape_log_id,
                metadata=meta_json,
            )
            .returning(sources.c.id)
        )
        result = await conn.execute(stmt)
        await conn.execute(text("RELEASE SAVEPOINT get_or_create_source_null"))
        return result.scalar_one()
    except IntegrityError:
        await conn.execute(text("ROLLBACK TO SAVEPOINT get_or_create_source_null"))
        return await _select_null_path()


async def link_record_source(
    conn: AsyncConnection,
    record_id: int,
    source_id: int,
    role: str = "first_seen",
) -> None:
    """Link a license record to a source (idempotent).

    Does nothing if the (record_id, source_id, role) combination already exists.
    """
    stmt = (
        pg_insert(record_sources)
        .values(record_id=record_id, source_id=source_id, role=role)
        .on_conflict_do_nothing()
    )
    await conn.execute(stmt)


# ------------------------------------------------------------------
# Provenance query helpers
# ------------------------------------------------------------------


async def get_primary_source(
    conn: AsyncConnection,
    record_id: int,
) -> dict | None:
    """Return the single most-relevant source for a record, or None.

    Priority order:
    1. Role: first_seen > repaired > confirmed
    2. Within a role: sources with a non-NULL snapshot_path first
    3. Newest captured_at as tiebreaker
    """
    stmt = text("""
        SELECT s.id, st.slug AS source_type, st.label AS source_label,
               s.snapshot_path, s.url, s.captured_at, s.ingested_at,
               s.metadata, rs.role
        FROM record_sources rs
        JOIN sources s ON s.id = rs.source_id
        JOIN source_types st ON st.id = s.source_type_id
        WHERE rs.record_id = :record_id
        ORDER BY s.captured_at DESC
    """)
    result = await conn.execute(stmt, {"record_id": record_id})
    rows = result.mappings().all()

    if not rows:
        return None

    best = None
    best_priority = (999, 999)
    for r in rows:
        d = dict(r)
        role_rank = SOURCE_ROLE_PRIORITY.get(d["role"], 2)
        no_snap = 0 if d["snapshot_path"] else 1
        priority = (role_rank, no_snap)
        if best is None or priority < best_priority:
            best = d
            best_priority = priority

    if best is not None:
        raw = best.get("metadata")
        best["metadata"] = json.loads(raw) if raw else {}
    return best


async def get_record_sources(
    conn: AsyncConnection,
    record_id: int,
) -> list[dict]:
    """Return provenance sources for a record, newest first."""
    stmt = text("""
        SELECT s.id, st.slug AS source_type, st.label AS source_label,
               s.snapshot_path, s.url, s.captured_at, s.ingested_at,
               s.metadata, rs.role
        FROM record_sources rs
        JOIN sources s ON s.id = rs.source_id
        JOIN source_types st ON st.id = s.source_type_id
        WHERE rs.record_id = :record_id
        ORDER BY s.captured_at DESC
    """)
    result = await conn.execute(stmt, {"record_id": record_id})
    results = []
    for r in result.mappings().all():
        d = dict(r)
        raw = d.get("metadata")
        d["metadata"] = json.loads(raw) if raw else {}
        results.append(d)
    return results
