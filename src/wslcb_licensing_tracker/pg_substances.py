"""Async PostgreSQL regulated substance CRUD.

Async equivalent of substances.py. No admin_audit dependency — caller logs.
"""

import logging

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import (
    license_endorsements,
    regulated_substance_endorsements,
    regulated_substances,
)

logger = logging.getLogger(__name__)


async def get_regulated_substances(conn: AsyncConnection) -> list[dict]:
    """All substances ordered by display_order, each with endorsement names list."""
    rows = (
        (
            await conn.execute(
                select(
                    regulated_substances.c.id,
                    regulated_substances.c.name,
                    regulated_substances.c.display_order,
                ).order_by(regulated_substances.c.display_order, regulated_substances.c.name)
            )
        )
        .mappings()
        .all()
    )

    results = []
    for row in rows:
        enames = (
            (
                await conn.execute(
                    select(license_endorsements.c.name)
                    .join(
                        regulated_substance_endorsements,
                        regulated_substance_endorsements.c.endorsement_id
                        == license_endorsements.c.id,
                    )
                    .where(regulated_substance_endorsements.c.substance_id == row["id"])
                    .order_by(license_endorsements.c.name)
                )
            )
            .scalars()
            .all()
        )
        results.append(
            {
                "id": row["id"],
                "name": row["name"],
                "display_order": row["display_order"],
                "endorsements": list(enames),
            }
        )
    return results


async def get_substance_endorsement_ids(conn: AsyncConnection, substance_id: int) -> list[int]:
    """Endorsement IDs linked to substance_id."""
    rows = (
        (
            await conn.execute(
                select(regulated_substance_endorsements.c.endorsement_id).where(
                    regulated_substance_endorsements.c.substance_id == substance_id
                )
            )
        )
        .scalars()
        .all()
    )
    return list(rows)


async def set_substance_endorsements(
    conn: AsyncConnection,
    substance_id: int,
    endorsement_ids: list[int],
) -> None:
    """Replace full endorsement list for substance_id. Caller commits + audits."""
    await conn.execute(
        delete(regulated_substance_endorsements).where(
            regulated_substance_endorsements.c.substance_id == substance_id
        )
    )
    for eid in endorsement_ids:
        await conn.execute(
            pg_insert(regulated_substance_endorsements)
            .values(substance_id=substance_id, endorsement_id=eid)
            .on_conflict_do_nothing()
        )


async def add_substance(conn: AsyncConnection, name: str, display_order: int) -> int:
    """Insert a new regulated substance and return its id. Caller commits + audits."""
    return (
        await conn.execute(
            pg_insert(regulated_substances)
            .values(name=name, display_order=display_order)
            .returning(regulated_substances.c.id)
        )
    ).scalar_one()


async def remove_substance(conn: AsyncConnection, substance_id: int) -> str | None:
    """Delete a regulated substance. Returns name for audit logging, or None if not found."""
    row = (
        await conn.execute(
            select(regulated_substances.c.name).where(regulated_substances.c.id == substance_id)
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    # Delete junction rows first (in case no CASCADE is configured)
    await conn.execute(
        delete(regulated_substance_endorsements).where(
            regulated_substance_endorsements.c.substance_id == substance_id
        )
    )
    await conn.execute(
        delete(regulated_substances).where(regulated_substances.c.id == substance_id)
    )
    return row
