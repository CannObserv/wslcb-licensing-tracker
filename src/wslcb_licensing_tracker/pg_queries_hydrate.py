"""Record enrichment and hydration for WSLCB licensing tracker.

Integration layer: assembles composite record dicts from endorsement,
entity, and link-records domain modules. Intentionally crosses module
boundaries — this is the single acknowledged fan-in point for the
query layer.

Contains:
- enrich_record() — pure Python display-field helper
- hydrate_records() / _hydrate_records() — bulk record enrichment
"""

import logging

from sqlalchemy.ext.asyncio import AsyncConnection

from .display import format_outcome
from .pg_endorsements import get_record_endorsements
from .pg_entities import get_record_entities
from .pg_link_records import get_outcome_status, get_record_links_bulk

logger = logging.getLogger(__name__)


def enrich_record(record: dict) -> dict:
    """Add display-ready fields with standardized-first fallback.

    Works with joined query results that include location columns
    aliased as business_location, city, std_city, std_region, std_postal_code, etc.
    """
    record["display_city"] = record.get("std_city") or record.get("city") or ""
    record["display_zip"] = record.get("std_postal_code") or record.get("zip_code") or ""
    record["display_previous_city"] = (
        record.get("prev_std_city") or record.get("previous_city") or ""
    )
    record["display_previous_zip"] = (
        record.get("prev_std_postal_code") or record.get("previous_zip_code") or ""
    )
    return record


async def _hydrate_records(conn: AsyncConnection, rows: list) -> list[dict]:
    """Enrich DB rows/dicts with endorsements, entities, and display fields.

    Accepts dicts or mapping objects. Shared by search_records(),
    get_entity_records(), and get_record_by_id().
    Also attaches outcome_status for new_application records.
    """
    if not rows:
        return []
    record_ids = [r["id"] for r in rows]
    endorsement_map = await get_record_endorsements(conn, record_ids)
    entity_map = await get_record_entities(conn, record_ids)

    # Bulk-fetch outcome links for new_application records
    new_app_ids = [r["id"] for r in rows if r["section_type"] == "new_application"]
    link_map = await get_record_links_bulk(conn, new_app_ids) if new_app_ids else {}

    results = []
    for r in rows:
        d = enrich_record(r if isinstance(r, dict) else dict(r))
        d["endorsements"] = endorsement_map.get(d["id"], [])
        d["entities"] = entity_map.get(d["id"], {"applicant": [], "previous_applicant": []})
        d["outcome_status"] = format_outcome(get_outcome_status(d, link_map.get(d["id"])))
        results.append(d)
    return results


# Public alias for external callers (e.g. app.py record_detail route).
hydrate_records = _hydrate_records
