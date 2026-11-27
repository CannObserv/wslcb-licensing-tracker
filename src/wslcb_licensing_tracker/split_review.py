"""Operator couple verdicts with audit rows (#269, spec #246 §3, §7).

Each write runs in the caller's transaction:

1. validate (nothing is written on a refusal);
2. replace the verdict and its members;
3. write one ``admin_audit_log`` row whose details carry the combined
   entity's id **and name**, the members, the previous verdict and the note;
4. reprocess the combined entity's printed records, which rebuilds their
   member rows and relinks their alias groups.

| Action | Audit action |
|---|---|
| :func:`set_split` | ``entity.split_set`` |
| :func:`keep_whole` | ``entity.split_keep`` |
| :func:`clear_verdict` | ``entity.split_clear`` |

Raises ``ValueError`` for a refused write and ``LookupError`` for a missing
entity or verdict.  Caller commits.
"""

import logging
from collections.abc import Sequence

from sqlalchemy import delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .admin_audit import log_action
from .classification_review import MAX_TEXT_CHARS
from .entities import reprocess_entities
from .entity_aliases import VERDICT_SPLIT, in_manual_alias
from .entity_classification import tombstone_ids
from .entity_splits import VERDICT_KEEP, has_couple_token
from .models import entities, entity_split_members, entity_splits, record_entities
from .text_utils import clean_entity_name

logger = logging.getLogger(__name__)

ACTION_SET = "entity.split_set"
ACTION_KEEP = "entity.split_keep"
ACTION_CLEAR = "entity.split_clear"


def _check_text(field: str, value: str | None) -> None:
    if value is not None and len(value) > MAX_TEXT_CHARS:
        msg = f"{field} is too long (over {MAX_TEXT_CHARS} characters)"
        raise ValueError(msg)


async def _entity(conn: AsyncConnection, entity_id: int) -> tuple[str, str]:
    """``(name, resolved type)`` of *entity_id*; ``LookupError`` when missing."""
    row = (
        await conn.execute(
            select(entities.c.name, entities.c.entity_type).where(entities.c.id == entity_id)
        )
    ).one_or_none()
    if row is None:
        msg = f"entity {entity_id} not found"
        raise LookupError(msg)
    return row[0], row[1]


async def _require_person(conn: AsyncConnection, entity_id: int) -> str:
    name, etype = await _entity(conn, entity_id)
    if etype != "person":
        msg = f"entity {entity_id} resolves to {etype}; only a person can be split or kept"
        raise ValueError(msg)
    return name


async def current_verdict(conn: AsyncConnection, entity_id: int) -> dict | None:
    """``{"verdict", "members", "created_by", "created_at", "note"}``, or None."""
    row = (
        (await conn.execute(select(entity_splits).where(entity_splits.c.combined_id == entity_id)))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    members = (
        await conn.execute(
            select(entity_split_members.c.member_name)
            .where(entity_split_members.c.combined_id == entity_id)
            .order_by(entity_split_members.c.ordinal)
        )
    ).scalars()
    return {
        "verdict": row["verdict"],
        "members": list(members),
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "note": row["note"],
    }


def _previous(verdict: dict | None) -> dict | None:
    return None if verdict is None else {k: verdict[k] for k in ("verdict", "members")}


async def _clean_members(
    conn: AsyncConnection, combined_name: str, members: Sequence[str]
) -> list[str]:
    """Clean, check and order the member names; blank fields are dropped."""
    out: list[str] = []
    for raw in members:
        if not raw.strip():
            continue
        _check_text("a member name", raw)
        name = clean_entity_name(raw)
        if not name:
            msg = f"member name {raw!r} is empty once cleaned"
            raise ValueError(msg)
        if name == combined_name:
            msg = f"member {name!r} is the combined name itself"
            raise ValueError(msg)
        if has_couple_token(name):
            msg = f"member {name!r} has an AND/& token; name one person per field"
            raise ValueError(msg)
        if name in out:
            msg = f"member {name!r} is named twice"
            raise ValueError(msg)
        out.append(name)
    if len(out) < 2:  # noqa: PLR2004
        msg = "a split names at least 2 people (one is an alias: use Same as…)"
        raise ValueError(msg)
    existing = dict(
        (
            await conn.execute(
                select(entities.c.name, entities.c.id).where(
                    func.lower(entities.c.name).in_([n.lower() for n in out])
                )
            )
        ).fetchall()
    )
    if tombs := await tombstone_ids(conn, list(existing.values())):
        names = sorted(n for n, i in existing.items() if i in tombs)
        msg = f"member {names[0]!r} is marked not-a-name"
        raise ValueError(msg)
    return out


async def _write(  # noqa: PLR0913
    conn: AsyncConnection,
    combined_id: int,
    verdict: str,
    members: list[str],
    *,
    admin_email: str,
    note: str | None,
) -> None:
    await conn.execute(
        delete(entity_split_members).where(entity_split_members.c.combined_id == combined_id)
    )
    stmt = pg_insert(entity_splits).values(
        combined_id=combined_id, verdict=verdict, created_by=admin_email, note=note
    )
    await conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["combined_id"],
            set_={
                "verdict": stmt.excluded.verdict,
                "created_by": stmt.excluded.created_by,
                "created_at": func.now(),
                "note": stmt.excluded.note,
            },
        )
    )
    if members:
        await conn.execute(
            insert(entity_split_members).values(
                [
                    {"combined_id": combined_id, "ordinal": i, "member_name": m}
                    for i, m in enumerate(members)
                ]
            )
        )


async def printed_records(conn: AsyncConnection, combined_id: int) -> list[int]:
    """Records that print *combined_id* (its printed rows, not member rows)."""
    re_ = record_entities
    stmt = (
        select(re_.c.record_id)
        .where((re_.c.entity_id == combined_id) & re_.c.via_entity_id.is_(None))
        .distinct()
        .order_by(re_.c.record_id)
    )
    return list((await conn.execute(stmt)).scalars())


async def _apply(conn: AsyncConnection, combined_id: int) -> int:
    """Reprocess the combined entity's printed records (and relink them); return the count."""
    records = await printed_records(conn, combined_id)
    if records:
        await reprocess_entities(conn, record_ids=records)
    return len(records)


async def set_split(
    conn: AsyncConnection,
    combined_id: int,
    members: Sequence[str],
    *,
    admin_email: str,
    note: str | None = None,
) -> dict:
    """Record that *combined_id* names the people in *members*; return ``{"members", "records"}``.

    Refused for a non-person, an entity in an operator alias link (unlink it
    first, spec §5a), or bad members (§3 step 1).
    """
    _check_text("note", note)
    name = await _require_person(conn, combined_id)
    if await in_manual_alias(conn, combined_id):
        msg = f"entity {combined_id} is in an operator alias link; unlink it before splitting"
        raise ValueError(msg)
    cleaned = await _clean_members(conn, name, members)
    before = await current_verdict(conn, combined_id)
    await _write(conn, combined_id, VERDICT_SPLIT, cleaned, admin_email=admin_email, note=note)
    await log_action(
        conn,
        admin_email,
        ACTION_SET,
        "entity",
        combined_id,
        {"name": name, "members": cleaned, "previous": _previous(before), "note": note},
    )
    records = await _apply(conn, combined_id)
    logger.info(
        "Split entity %d into %d member(s); %d record(s)", combined_id, len(cleaned), records
    )
    return {"members": cleaned, "records": records}


async def keep_whole(
    conn: AsyncConnection, combined_id: int, *, admin_email: str, note: str | None = None
) -> dict:
    """Record that *combined_id* is not a split (off the Couples queue); return ``{"records"}``."""
    _check_text("note", note)
    name = await _require_person(conn, combined_id)
    before = await current_verdict(conn, combined_id)
    await _write(conn, combined_id, VERDICT_KEEP, [], admin_email=admin_email, note=note)
    await log_action(
        conn,
        admin_email,
        ACTION_KEEP,
        "entity",
        combined_id,
        {"name": name, "members": [], "previous": _previous(before), "note": note},
    )
    records = await _apply(conn, combined_id) if before and before["members"] else 0
    logger.info("Kept entity %d whole", combined_id)
    return {"records": records}


async def clear_verdict(conn: AsyncConnection, combined_id: int, *, admin_email: str) -> dict:
    """Delete *combined_id*'s verdict and its member rows; return ``{"records"}``."""
    name, _ = await _entity(conn, combined_id)
    before = await current_verdict(conn, combined_id)
    if before is None:
        msg = f"entity {combined_id} has no verdict"
        raise LookupError(msg)
    await conn.execute(delete(entity_splits).where(entity_splits.c.combined_id == combined_id))
    await log_action(
        conn,
        admin_email,
        ACTION_CLEAR,
        "entity",
        combined_id,
        {"name": name, "previous": _previous(before)},
    )
    records = await _apply(conn, combined_id) if before["members"] else 0
    logger.info("Cleared the verdict on entity %d", combined_id)
    return {"records": records}
