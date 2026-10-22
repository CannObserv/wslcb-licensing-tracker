"""Entity classification resolution (#206).

``entities.entity_type`` / ``entity_subtype`` / ``classification_source`` are
*resolved* columns, computed here from the append-only answers in
``entity_classifications``: the current manual row, else a non-ignored Jev
row at or above the confidence threshold with a real type, else the regex
row.  A source's current answer is its latest non-ignored row.

Also holds the operator-input helpers (manual rows, protected / confirmed-noise
sets) and the reversible Jev rollback.  Must not import ``entities.py``, which
imports this module.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import bindparam, exists, func, insert, select, union, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.expression import ColumnElement, Exists
from sqlalchemy.sql.selectable import Subquery

from .config import jev_confidence_threshold
from .models import entities, entity_classifications, entity_review_flags

logger = logging.getLogger(__name__)

SOURCES = ("regex", "jev", "manual")
ENTITY_TYPES = ("person", "organization")
ENTITY_SUBTYPES = ("business", "nonprofit", "trust", "estate", "tribe", "government", "unknown")
# Subtypes Jev's org question can answer (trust/estate come from its kind question).
JEV_ORG_SUBTYPES = ("business", "nonprofit", "tribe", "government")
NOT_A_NAME = "not_a_name"
# Org subtypes whose Jev answer met the go-live bar; set in #210 (Task 5.2).
JEV_TRUSTED_ORG_SUBTYPES: frozenset[str] = frozenset()

_CHUNK = 1000

Triple = tuple[str, str | None, str]


@dataclass(frozen=True)
class SourceAnswer:
    """One source's current answer for an entity."""

    entity_type: str
    entity_subtype: str | None
    confidence: float | None = None
    model: str | None = None


@dataclass(frozen=True)
class ResolutionChange:
    """A resolved ``(entity_type, entity_subtype, classification_source)`` change."""

    entity_id: int
    old: Triple
    new: Triple


@dataclass(frozen=True)
class ResolveResult:
    """Outcome of ``resolve_entities``."""

    examined: int
    changes: list[ResolutionChange]


def choose_classification(
    manual: SourceAnswer | None,
    jev: SourceAnswer | None,
    regex: SourceAnswer | None,
    *,
    threshold: float,
    trusted_org_subtypes: frozenset[str] = JEV_TRUSTED_ORG_SUBTYPES,
) -> Triple | None:
    """Return the resolved triple for one entity's current answers, or None if it has none.

    A Jev org subtype that is ``unknown`` or not yet trusted falls back to the
    regex's org subtype when it has one, else ``unknown``.
    """
    if manual and manual.entity_type != NOT_A_NAME:
        return manual.entity_type, manual.entity_subtype, "manual"
    if jev and jev.entity_type in ENTITY_TYPES and (jev.confidence or 0) >= threshold:
        sub = jev.entity_subtype
        if jev.entity_type == "organization" and (
            sub == "unknown" or (sub in JEV_ORG_SUBTYPES and sub not in trusted_org_subtypes)
        ):
            sub = (
                regex.entity_subtype
                if regex and regex.entity_type == "organization" and regex.entity_subtype
                else "unknown"
            )
        return jev.entity_type, sub, "jev"
    if regex:
        return regex.entity_type, regex.entity_subtype, "regex"
    return None


def _chunks(ids: Sequence[int]) -> list[Sequence[int]]:
    return [ids[i : i + _CHUNK] for i in range(0, len(ids), _CHUNK)]


async def current_answers(
    conn: AsyncConnection, entity_ids: Sequence[int] | None = None
) -> dict[int, dict[str, SourceAnswer]]:
    """Return ``{entity_id: {source: SourceAnswer}}`` from each source's latest non-ignored row."""
    ec = entity_classifications
    base = (
        select(
            ec.c.entity_id,
            ec.c.source,
            ec.c.entity_type,
            ec.c.entity_subtype,
            ec.c.confidence,
            ec.c.model,
        )
        .where(ec.c.ignored_at.is_(None))
        .distinct(ec.c.entity_id, ec.c.source)
        .order_by(ec.c.entity_id, ec.c.source, ec.c.id.desc())
    )
    stmts = (
        [base]
        if entity_ids is None
        else [base.where(ec.c.entity_id.in_(chunk)) for chunk in _chunks(entity_ids)]
    )
    out: dict[int, dict[str, SourceAnswer]] = {}
    for stmt in stmts:
        for eid, source, etype, sub, conf, model in (await conn.execute(stmt)).fetchall():
            out.setdefault(eid, {})[source] = SourceAnswer(etype, sub, conf, model)
    return out


async def resolve_entities(
    conn: AsyncConnection, entity_ids: Sequence[int] | None = None
) -> ResolveResult:
    """Recompute the resolved columns for *entity_ids* (all entities when None).

    Updates only rows whose triple changed.  An entity with no current answer
    at all is left as it is.  Caller commits.
    """
    cols = (
        entities.c.id,
        entities.c.entity_type,
        entities.c.entity_subtype,
        entities.c.classification_source,
    )
    if entity_ids is None:
        rows = (await conn.execute(select(*cols))).fetchall()
    else:
        ids = list(dict.fromkeys(entity_ids))
        rows = []
        for chunk in _chunks(ids):
            rows += (await conn.execute(select(*cols).where(entities.c.id.in_(chunk)))).fetchall()
    answers = await current_answers(conn, None if entity_ids is None else [r[0] for r in rows])
    threshold = jev_confidence_threshold()

    changes: list[ResolutionChange] = []
    for eid, etype, sub, source in rows:
        a = answers.get(eid, {})
        new = choose_classification(
            a.get("manual"),
            a.get("jev"),
            a.get("regex"),
            threshold=threshold,
            trusted_org_subtypes=JEV_TRUSTED_ORG_SUBTYPES,
        )
        old = (etype, sub, source)
        if new is not None and new != old:
            changes.append(ResolutionChange(eid, old, new))

    if changes:
        stmt = (
            update(entities)
            .where(entities.c.id == bindparam("_id"))
            .values(
                entity_type=bindparam("_type"),
                entity_subtype=bindparam("_subtype"),
                classification_source=bindparam("_source"),
            )
        )
        params = [
            {"_id": c.entity_id, "_type": c.new[0], "_subtype": c.new[1], "_source": c.new[2]}
            for c in changes
        ]
        for i in range(0, len(params), _CHUNK):
            await conn.execute(stmt, params[i : i + _CHUNK])
    return ResolveResult(examined=len(rows), changes=changes)


def _validate_manual(entity_type: str, entity_subtype: str | None) -> None:
    if entity_type in ("person", NOT_A_NAME):
        if entity_subtype is not None:
            msg = f"{entity_type} takes no subtype (got {entity_subtype!r})"
            raise ValueError(msg)
    elif entity_type == "organization":
        if entity_subtype not in ENTITY_SUBTYPES:
            msg = f"organization needs a subtype in {ENTITY_SUBTYPES} (got {entity_subtype!r})"
            raise ValueError(msg)
    else:
        msg = f"unknown entity_type {entity_type!r}"
        raise ValueError(msg)


async def record_manual(  # noqa: PLR0913
    conn: AsyncConnection,
    entity_id: int,
    *,
    entity_type: str,
    entity_subtype: str | None,
    created_by: str,
    note: str | None = None,
) -> int:
    """Store an operator's classification for *entity_id*, re-resolve it, and return the row id.

    Raises ``ValueError`` for a bad type/subtype combination and
    ``LookupError`` when the entity does not exist; nothing is written then.
    """
    _validate_manual(entity_type, entity_subtype)
    found = (
        await conn.execute(select(entities.c.id).where(entities.c.id == entity_id))
    ).scalar_one_or_none()
    if found is None:
        msg = f"entity {entity_id} not found"
        raise LookupError(msg)
    row_id = (
        await conn.execute(
            insert(entity_classifications)
            .values(
                entity_id=entity_id,
                source="manual",
                entity_type=entity_type,
                entity_subtype=entity_subtype,
                created_by=created_by,
                note=note,
            )
            .returning(entity_classifications.c.id)
        )
    ).scalar_one()
    await resolve_entities(conn, [entity_id])
    logger.info(
        "Manual classification for entity %d: %s/%s by %s",
        entity_id,
        entity_type,
        entity_subtype,
        created_by,
    )
    return row_id


async def clear_manual(conn: AsyncConnection, entity_id: int) -> int:
    """Ignore *entity_id*'s manual rows (kept as history), re-resolve, and return the count."""
    ec = entity_classifications
    result = await conn.execute(
        update(ec)
        .where(
            (ec.c.entity_id == entity_id) & (ec.c.source == "manual") & ec.c.ignored_at.is_(None)
        )
        .values(ignored_at=func.now())
    )
    if result.rowcount:
        await resolve_entities(conn, [entity_id])
        logger.info("Cleared %d manual classification(s) for entity %d", result.rowcount, entity_id)
    return result.rowcount


def _current_manual() -> Subquery:
    """Subquery: each entity's latest non-ignored manual row (``entity_id``, ``entity_type``)."""
    ec = entity_classifications
    return (
        select(ec.c.entity_id, ec.c.entity_type)
        .where((ec.c.source == "manual") & ec.c.ignored_at.is_(None))
        .distinct(ec.c.entity_id)
        .order_by(ec.c.entity_id, ec.c.id.desc())
        .subquery()
    )


def _open_flag(entity_id_col: ColumnElement[int]) -> Exists:
    """EXISTS an unresolved review flag for *entity_id_col*."""
    erf = entity_review_flags
    return exists().where((erf.c.entity_id == entity_id_col) & erf.c.resolved_at.is_(None))


async def protected_ids(conn: AsyncConnection) -> set[int]:
    """Entities an automated purge must keep.

    Those with a current manual row of a real type, or an open review flag.
    """
    m = _current_manual()
    erf = entity_review_flags
    stmt = union(
        select(m.c.entity_id).where(m.c.entity_type != NOT_A_NAME),
        select(erf.c.entity_id).where(erf.c.resolved_at.is_(None)),
    )
    return set((await conn.execute(stmt)).scalars().all())


async def confirmed_noise_ids(conn: AsyncConnection) -> set[int]:
    """Entities an operator confirmed are not names.

    Their current manual row is ``not_a_name`` and they have no open flag.
    """
    m = _current_manual()
    stmt = select(m.c.entity_id).where((m.c.entity_type == NOT_A_NAME) & ~_open_flag(m.c.entity_id))
    return set((await conn.execute(stmt)).scalars().all())


async def set_jev_ignored(
    conn: AsyncConnection, *, ignored: bool, model: str | None = None
) -> dict[str, int]:
    """Ignore (or restore) Jev rows, optionally one model's, and re-resolve the affected entities.

    The reversible rollback for Jev answers: rows are marked, never deleted.
    Regex and manual rows are not touched.  Caller commits.

    Returns ``{"rows": int, "entities": int, "changed": int}`` — rows marked,
    distinct entities they belong to, and resolved triples that changed.
    """
    ec = entity_classifications
    where = (ec.c.source == "jev") & (
        ec.c.ignored_at.is_(None) if ignored else ec.c.ignored_at.is_not(None)
    )
    if model is not None:
        where &= ec.c.model == model
    stmt = (
        update(ec)
        .where(where)
        .values(ignored_at=func.now() if ignored else None)
        .returning(ec.c.entity_id)
    )
    entity_ids = (await conn.execute(stmt)).scalars().all()
    affected = sorted(set(entity_ids))
    changed = len((await resolve_entities(conn, affected)).changes) if affected else 0
    logger.info(
        "%s %d jev row(s)%s across %d entities; %d resolved classification(s) changed",
        "Ignored" if ignored else "Restored",
        len(entity_ids),
        f" (model {model})" if model else "",
        len(affected),
        changed,
    )
    return {"rows": len(entity_ids), "entities": len(affected), "changed": changed}
