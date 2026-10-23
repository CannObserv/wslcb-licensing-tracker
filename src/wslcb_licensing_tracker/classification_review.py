"""Operator classification actions and review-queue queries (#208).

Every action is audited: it calls ``admin_audit.log_action`` with the
entity's resolved ``(entity_type, entity_subtype, classification_source)``
before and after.  Set, accept and not-a-name resolve the entity's open
review flags; clearing an override leaves them open.  Caller commits.

Raises ``ValueError`` for bad input and ``LookupError`` for a missing entity
or answer, before anything is written.
"""

import logging
from typing import Literal

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .admin_audit import log_action
from .entity_classification import (
    NOT_A_NAME,
    Triple,
    clear_manual,
    current_answers,
    record_manual,
)
from .models import entities, entity_review_flags

logger = logging.getLogger(__name__)

ACCEPTABLE_SOURCES = ("jev", "regex")


async def _triple(conn: AsyncConnection, entity_id: int) -> Triple:
    """Return the entity's resolved triple; ``LookupError`` when it does not exist."""
    row = (
        await conn.execute(
            select(
                entities.c.entity_type,
                entities.c.entity_subtype,
                entities.c.classification_source,
            ).where(entities.c.id == entity_id)
        )
    ).one_or_none()
    if row is None:
        msg = f"entity {entity_id} not found"
        raise LookupError(msg)
    return tuple(row)


async def _resolve_flags(
    conn: AsyncConnection, entity_id: int, *, admin_email: str, resolution: str
) -> int:
    """Close the entity's open review flags; return how many."""
    erf = entity_review_flags
    result = await conn.execute(
        update(erf)
        .where((erf.c.entity_id == entity_id) & erf.c.resolved_at.is_(None))
        .values(resolved_at=func.now(), resolved_by=admin_email, resolution=resolution)
    )
    return result.rowcount


async def _audit(  # noqa: PLR0913
    conn: AsyncConnection,
    entity_id: int,
    *,
    admin_email: str,
    action: str,
    old: Triple,
    details: dict,
) -> None:
    new = await _triple(conn, entity_id)
    await log_action(
        conn,
        admin_email,
        action,
        "entity",
        entity_id,
        {"old": list(old), "new": list(new), **details},
    )


async def _record_and_resolve(  # noqa: PLR0913
    conn: AsyncConnection,
    entity_id: int,
    *,
    action: str,
    entity_type: str,
    entity_subtype: str | None,
    note: str | None,
    admin_email: str,
    details: dict | None = None,
) -> int:
    """Write a manual row, close open flags, audit; return the manual row id."""
    old = await _triple(conn, entity_id)
    row_id = await record_manual(
        conn,
        entity_id,
        entity_type=entity_type,
        entity_subtype=entity_subtype,
        created_by=admin_email,
        note=note,
    )
    flags = await _resolve_flags(conn, entity_id, admin_email=admin_email, resolution=action)
    await _audit(
        conn,
        entity_id,
        admin_email=admin_email,
        action=action,
        old=old,
        details={
            "manual_id": row_id,
            "entity_type": entity_type,
            "entity_subtype": entity_subtype,
            "note": note,
            "flags_resolved": flags,
            **(details or {}),
        },
    )
    return row_id


async def set_classification(  # noqa: PLR0913
    conn: AsyncConnection,
    entity_id: int,
    *,
    entity_type: str,
    entity_subtype: str | None,
    note: str | None,
    admin_email: str,
) -> int:
    """Store an operator's classification (``entity.classify_set``); return the manual row id."""
    return await _record_and_resolve(
        conn,
        entity_id,
        action="entity.classify_set",
        entity_type=entity_type,
        entity_subtype=entity_subtype,
        note=note,
        admin_email=admin_email,
    )


async def mark_not_a_name(
    conn: AsyncConnection, entity_id: int, *, admin_email: str, note: str | None = None
) -> int:
    """Confirm the entity is not a name (``entity.classify_not_a_name``); return the row id.

    The next ``reclassify-entities`` purges the entity with all its rows,
    this one included; the audit row is the lasting record.
    """
    return await _record_and_resolve(
        conn,
        entity_id,
        action="entity.classify_not_a_name",
        entity_type=NOT_A_NAME,
        entity_subtype=None,
        note=note,
        admin_email=admin_email,
    )


async def accept_answer(
    conn: AsyncConnection,
    entity_id: int,
    *,
    source: Literal["jev", "regex"],
    admin_email: str,
) -> int:
    """Copy *source*'s current answer into a manual row; return its id.

    Actions ``entity.classify_accept_jev`` / ``entity.classify_accept_regex``.
    A Jev ``not_a_name`` answer becomes :func:`mark_not_a_name`.  An
    organization answer with no subtype is stored as ``unknown``.
    ``LookupError`` when *source* has no current answer.
    """
    if source not in ACCEPTABLE_SOURCES:
        msg = f"source must be one of {ACCEPTABLE_SOURCES} (got {source!r})"
        raise ValueError(msg)
    await _triple(conn, entity_id)
    answer = (await current_answers(conn, [entity_id])).get(entity_id, {}).get(source)
    if answer is None:
        msg = f"entity {entity_id} has no current {source} answer"
        raise LookupError(msg)
    conf = "" if answer.confidence is None else f", {answer.confidence:.2f}"
    note = f"Accepted {source} answer ({answer.model}{conf})"
    if answer.entity_type == NOT_A_NAME:
        return await mark_not_a_name(conn, entity_id, admin_email=admin_email, note=note)
    subtype = answer.entity_subtype
    if answer.entity_type == "organization" and subtype is None:
        subtype = "unknown"
    return await _record_and_resolve(
        conn,
        entity_id,
        action=f"entity.classify_accept_{source}",
        entity_type=answer.entity_type,
        entity_subtype=subtype,
        note=note,
        admin_email=admin_email,
        details={
            "accepted": {
                "source": source,
                "entity_type": answer.entity_type,
                "entity_subtype": answer.entity_subtype,
                "confidence": answer.confidence,
                "model": answer.model,
            }
        },
    )


async def clear_override(conn: AsyncConnection, entity_id: int, *, admin_email: str) -> int:
    """Ignore the entity's manual rows (``entity.classify_clear``); return how many.

    Open flags stay open.
    """
    old = await _triple(conn, entity_id)
    cleared = await clear_manual(conn, entity_id)
    await _audit(
        conn,
        entity_id,
        admin_email=admin_email,
        action="entity.classify_clear",
        old=old,
        details={"cleared": cleared},
    )
    return cleared


async def flag_entity(
    conn: AsyncConnection, entity_id: int, *, reason: str, admin_email: str
) -> int:
    """Open a review flag (``entity.flag``); return its id.

    ``ValueError`` on a blank reason.
    """
    reason = reason.strip()
    if not reason:
        msg = "a flag needs a reason"
        raise ValueError(msg)
    old = await _triple(conn, entity_id)
    flag_id = (
        await conn.execute(
            insert(entity_review_flags)
            .values(entity_id=entity_id, flagged_by=admin_email, reason=reason)
            .returning(entity_review_flags.c.id)
        )
    ).scalar_one()
    await _audit(
        conn,
        entity_id,
        admin_email=admin_email,
        action="entity.flag",
        old=old,
        details={"flag_id": flag_id, "reason": reason},
    )
    logger.info("Entity %d flagged for review by %s", entity_id, admin_email)
    return flag_id
