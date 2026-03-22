"""Async PostgreSQL database integrity checks for the WSLCB licensing tracker.

Provides async check functions, fix functions, and a full orchestrated suite
for use by ``cli.py check``. Also used by ``admin_routes.admin_dashboard``
for the three lightweight check functions.
"""

import logging

from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import locations as locations_table

logger = logging.getLogger(__name__)


async def check_orphaned_locations(conn: AsyncConnection) -> dict:
    """Find locations not referenced by any license_records row.

    Returns a dict with ``count`` (int) and ``details`` (list of dicts
    with ``id`` and ``raw_address``).
    """
    result = await conn.execute(
        text("""
        SELECT l.id, l.raw_address
        FROM locations l
        WHERE l.id NOT IN (
            SELECT DISTINCT location_id FROM license_records WHERE location_id IS NOT NULL
            UNION
            SELECT DISTINCT previous_location_id FROM license_records
            WHERE previous_location_id IS NOT NULL
        )
    """)
    )
    rows = result.mappings().all()
    details = [dict(r) for r in rows]
    return {"count": len(details), "details": details}


async def check_unenriched_records(conn: AsyncConnection) -> dict:
    """Count records missing key enrichment artifacts.

    Returns a dict with counts for each category:
    - ``no_endorsements``: records with non-empty license_type but no record_endorsements rows
    - ``no_entities``: records with applicants containing ';' but no record_entities rows
    - ``no_provenance``: records with no record_sources rows
    - ``no_enrichment_tracking``: records missing from record_enrichments for step 'endorsements'
    """
    result = await conn.execute(
        text("""
        SELECT COUNT(*) FROM license_records lr
        WHERE lr.license_type IS NOT NULL AND lr.license_type != ''
          AND NOT EXISTS (
              SELECT 1 FROM record_endorsements re WHERE re.record_id = lr.id
          )
    """)
    )
    no_endorsements = result.scalar_one()

    result = await conn.execute(
        text("""
        SELECT COUNT(*) FROM license_records lr
        WHERE lr.applicants LIKE '%;%'
          AND NOT EXISTS (
              SELECT 1 FROM record_entities re WHERE re.record_id = lr.id
          )
    """)
    )
    no_entities = result.scalar_one()

    result = await conn.execute(
        text("""
        SELECT COUNT(*) FROM license_records lr
        WHERE NOT EXISTS (
            SELECT 1 FROM record_sources rs WHERE rs.record_id = lr.id
        )
    """)
    )
    no_provenance = result.scalar_one()

    result = await conn.execute(
        text("""
        SELECT COUNT(*) FROM license_records lr
        WHERE NOT EXISTS (
            SELECT 1 FROM record_enrichments re
            WHERE re.record_id = lr.id AND re.step = 'endorsements'
        )
    """)
    )
    no_enrichment = result.scalar_one()

    return {
        "no_endorsements": no_endorsements,
        "no_entities": no_entities,
        "no_provenance": no_provenance,
        "no_enrichment_tracking": no_enrichment,
    }


async def check_endorsement_anomalies(conn: AsyncConnection) -> dict:
    """Detect endorsement-related data issues.

    Returns a dict with:
    - ``unresolved_codes``: records with numeric license_type that have no endorsement links
    - ``placeholder_endorsements``: endorsements whose name is purely numeric
    """
    result = await conn.execute(
        text(r"""
        SELECT COUNT(*) FROM license_records lr
        WHERE lr.license_type ~ '^\d'
          AND lr.id NOT IN (SELECT record_id FROM record_endorsements)
    """)
    )
    unresolved = result.scalar_one()

    result = await conn.execute(
        text(r"""
        SELECT COUNT(*) FROM license_endorsements
        WHERE name ~ '^\d+$'
    """)
    )
    placeholders = result.scalar_one()

    return {
        "unresolved_codes": unresolved,
        "placeholder_endorsements": placeholders,
    }


async def fix_orphaned_locations(conn: AsyncConnection) -> int:
    """Delete orphaned locations. Returns the number removed.

    Caller-commits convention: caller must call ``await conn.commit()`` after this.
    """
    result = await check_orphaned_locations(conn)
    if not result["count"]:
        return 0
    ids = [o["id"] for o in result["details"]]
    await conn.execute(delete(locations_table).where(locations_table.c.id.in_(ids)))
    logger.info("Removed %d orphaned location(s).", len(ids))
    return len(ids)


async def check_broken_fks(conn: AsyncConnection) -> list[dict]:
    """Find license_records rows with location_id or previous_location_id pointing nowhere.

    Returns a list of dicts with ``record_id``, ``column``, ``bad_id``.
    PostgreSQL enforces FK constraints, so this should always return empty
    in a healthy database. Included for parity with the SQLite integrity suite.
    """
    results = []
    for col in ("location_id", "previous_location_id"):
        result = await conn.execute(
            text(f"""
            SELECT lr.id AS record_id, lr.{col} AS bad_id
            FROM license_records lr
            WHERE lr.{col} IS NOT NULL
              AND lr.{col} NOT IN (SELECT id FROM locations)
            """)
        )
        rows = result.mappings().all()
        results.extend(
            {"record_id": r["record_id"], "column": col, "bad_id": r["bad_id"]} for r in rows
        )
    return results


async def check_entity_duplicates(conn: AsyncConnection) -> list[dict]:
    """Find entities that would merge under current cleaning rules (case-only differences).

    Returns a list of dicts with ``upper_name``, ``cnt``, ``names``.
    """
    result = await conn.execute(
        text("""
        SELECT UPPER(name) AS upper_name, COUNT(*) AS cnt,
               STRING_AGG(name, ' | ') AS names
        FROM entities
        GROUP BY UPPER(name)
        HAVING COUNT(*) > 1
        """)
    )
    return [dict(r) for r in result.mappings().all()]


async def run_all_checks(conn: AsyncConnection, *, fix: bool = False) -> dict:
    """Run all integrity checks and optionally auto-fix safe issues.

    Returns a structured report dict (same shape as SQLite integrity.py).
    When ``fix=True``, commits internally after auto-fixes.
    """
    report: dict = {}

    orphaned = await check_orphaned_locations(conn)
    entry: dict = {"count": orphaned["count"]}
    if fix and orphaned["count"]:
        entry["fixed"] = await fix_orphaned_locations(conn)
        await conn.commit()  # fix_orphaned_locations follows caller-commits
    report["orphaned_locations"] = entry

    broken = await check_broken_fks(conn)
    report["broken_fks"] = {"count": len(broken), "details": broken}

    unenriched = await check_unenriched_records(conn)
    report["unenriched"] = unenriched

    endorsement = await check_endorsement_anomalies(conn)
    report["endorsement_anomalies"] = endorsement

    dupes = await check_entity_duplicates(conn)
    report["entity_duplicates"] = {"count": len(dupes), "details": dupes}

    return report


_DETAIL_PREVIEW_LIMIT = 5


def print_report(report: dict) -> int:  # noqa: C901, PLR0912
    """Print a human-readable integrity report to stdout.

    Returns the total number of issues found (0 = clean).
    Pure Python — no DB access.
    """
    total_issues = 0

    print("\n=== WSLCB Database Integrity Report ===")
    print()

    n = report["orphaned_locations"]["count"]
    fixed = report["orphaned_locations"].get("fixed", 0)
    if n:
        total_issues += n
        status = f"  FIXED {fixed}" if fixed else "  (use --fix to remove)"
        print(f"\u274c Orphaned locations: {n}{status}")
    else:
        print("\u2705 No orphaned locations")

    n = report["broken_fks"]["count"]
    if n:
        total_issues += n
        print(f"\u274c Broken foreign keys: {n}")
        for d in report["broken_fks"]["details"][:_DETAIL_PREVIEW_LIMIT]:
            print(f"     record {d['record_id']}: {d['column']} = {d['bad_id']}")
        if n > _DETAIL_PREVIEW_LIMIT:
            print(f"     ... and {n - _DETAIL_PREVIEW_LIMIT} more")
    else:
        print("\u2705 No broken foreign keys")

    ue = report["unenriched"]
    for key, label in [
        ("no_endorsements", "Records missing endorsement links"),
        ("no_entities", "Records missing entity links"),
        ("no_provenance", "Records missing provenance"),
        ("no_enrichment_tracking", "Records missing enrichment tracking"),
    ]:
        n = ue[key]
        if n:
            total_issues += n
            print(f"\u274c {label}: {n}")
        else:
            print(f"\u2705 {label}: 0")

    ea = report["endorsement_anomalies"]
    for key, label in [
        ("unresolved_codes", "Unresolved numeric codes"),
        ("placeholder_endorsements", "Placeholder endorsements"),
    ]:
        n = ea[key]
        if n:
            total_issues += n
            print(f"\u274c {label}: {n}")
        else:
            print(f"\u2705 {label}: 0")

    n = report["entity_duplicates"]["count"]
    if n:
        total_issues += n
        print(f"\u274c Entity duplicate groups: {n}")
        for d in report["entity_duplicates"]["details"][:_DETAIL_PREVIEW_LIMIT]:
            print(f"     {d['names']}")
        if n > _DETAIL_PREVIEW_LIMIT:
            print(f"     ... and {n - _DETAIL_PREVIEW_LIMIT} more")
    else:
        print("\u2705 No entity duplicates")

    print()
    if total_issues:
        print(f"Total issues: {total_issues}")
    else:
        print("\u2705 All checks passed!")
    print()

    return total_issues
