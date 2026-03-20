"""Async PostgreSQL database helpers for the WSLCB licensing tracker.

Async equivalents of the location, source, and provenance helpers in db.py.
Uses SQLAlchemy Core expressions against the table objects in models.py.

Pure-string helpers (_normalize_raw_address, clean_entity_name, etc.) are
re-imported from db.py — they have no DB dependency.
"""

import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .db import _normalize_raw_address
from .models import locations

logger = logging.getLogger(__name__)


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
