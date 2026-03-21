"""Async PostgreSQL database integrity checks for the WSLCB licensing tracker.

Async port of the check functions used by admin_routes.admin_dashboard.
Full integrity suite (fix_orphaned_locations, run_all_checks) is Phase 6 (CLI).
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

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
    no_endorsements = result.scalar()

    result = await conn.execute(
        text("""
        SELECT COUNT(*) FROM license_records lr
        WHERE lr.applicants LIKE '%;%'
          AND NOT EXISTS (
              SELECT 1 FROM record_entities re WHERE re.record_id = lr.id
          )
    """)
    )
    no_entities = result.scalar()

    result = await conn.execute(
        text("""
        SELECT COUNT(*) FROM license_records lr
        WHERE NOT EXISTS (
            SELECT 1 FROM record_sources rs WHERE rs.record_id = lr.id
        )
    """)
    )
    no_provenance = result.scalar()

    result = await conn.execute(
        text("""
        SELECT COUNT(*) FROM license_records lr
        WHERE NOT EXISTS (
            SELECT 1 FROM record_enrichments re
            WHERE re.record_id = lr.id AND re.step = 'endorsements'
        )
    """)
    )
    no_enrichment = result.scalar()

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
    unresolved = result.scalar()

    result = await conn.execute(
        text(r"""
        SELECT COUNT(*) FROM license_endorsements
        WHERE name ~ '^\d+$'
    """)
    )
    placeholders = result.scalar()

    return {
        "unresolved_codes": unresolved,
        "placeholder_endorsements": placeholders,
    }
