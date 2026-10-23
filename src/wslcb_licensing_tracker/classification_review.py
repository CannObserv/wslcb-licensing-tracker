"""Operator classification actions and review-queue queries (#208).

Every action is audited: it calls ``admin_audit.log_action`` with the
entity's resolved ``(entity_type, entity_subtype, classification_source)``
before and after.  Set, accept and not-a-name resolve the entity's open
review flags; clearing an override leaves them open.  Caller commits.

Raises ``ValueError`` for bad input and ``LookupError`` for a missing entity
or answer, before anything is written.

The review queue's main tab holds entities with an open flag, plus those with
no current manual row whose current Jev answer is below the confidence
threshold or ``not_a_name``.  Its "unknown" tab holds the remaining
organizations whose resolved subtype is ``unknown`` and that have no manual row.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import ColumnElement, FromClause, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.selectable import Subquery

from .admin_audit import log_action
from .config import JEV_USD_PER_MILLION_INPUT_TOKENS, jev_confidence_threshold
from .entity_classification import (
    NOT_A_NAME,
    Triple,
    clear_manual,
    current_answers,
    record_manual,
)
from .jev_classifier import awaiting_count
from .jev_client import get_api_key, get_model, is_classification_enabled
from .models import classification_runs, entities, entity_classifications, entity_review_flags

logger = logging.getLogger(__name__)

ACCEPTABLE_SOURCES = ("jev", "regex")
Tab = Literal["main", "unknown"]
TABS: tuple[Tab, ...] = ("main", "unknown")


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


# ---------------------------------------------------------------------------
# Review queue
# ---------------------------------------------------------------------------


def _current_jev() -> Subquery:
    """Subquery: each entity's latest non-ignored Jev row."""
    ec = entity_classifications
    return (
        select(
            ec.c.entity_id,
            ec.c.entity_type,
            ec.c.entity_subtype,
            ec.c.confidence,
            ec.c.model,
        )
        .where((ec.c.source == "jev") & ec.c.ignored_at.is_(None))
        .distinct(ec.c.entity_id)
        .order_by(ec.c.entity_id, ec.c.id.desc())
        .subquery()
    )


@dataclass(frozen=True)
class _QueueSource:
    """Entities left-joined to their current Jev answer, open flags and manual rows.

    Joins, not correlated EXISTS: the subplans inflated the cost estimate past
    ``jit_above_cost``, and JIT compilation then took most of the query time.
    """

    jev: Subquery
    flagged: ColumnElement[bool]
    has_manual: ColumnElement[bool]
    from_: FromClause


def _queue_source() -> _QueueSource:
    ec = entity_classifications
    erf = entity_review_flags
    jev = _current_jev()
    flagged = select(erf.c.entity_id).where(erf.c.resolved_at.is_(None)).distinct().subquery()
    manual = (
        select(ec.c.entity_id)
        .where((ec.c.source == "manual") & ec.c.ignored_at.is_(None))
        .distinct()
        .subquery()
    )
    from_ = (
        entities.outerjoin(jev, jev.c.entity_id == entities.c.id)
        .outerjoin(flagged, flagged.c.entity_id == entities.c.id)
        .outerjoin(manual, manual.c.entity_id == entities.c.id)
    )
    return _QueueSource(
        jev, flagged.c.entity_id.is_not(None), manual.c.entity_id.is_not(None), from_
    )


def _queue_filter(q: _QueueSource, tab: Tab, threshold: float) -> ColumnElement[bool]:
    jev = q.jev
    jev_needs_review = (
        jev.c.entity_id.is_not(None)
        & ~q.has_manual
        & ((func.coalesce(jev.c.confidence, 0) < threshold) | (jev.c.entity_type == NOT_A_NAME))
    )
    main = q.flagged | jev_needs_review
    if tab == "main":
        return main
    if tab == "unknown":
        return (entities.c.entity_subtype == "unknown") & ~q.has_manual & ~main
    msg = f"tab must be one of {TABS} (got {tab!r})"
    raise ValueError(msg)


def _reasons(item: dict, *, flagged: bool, has_manual: bool, threshold: float) -> list[str]:
    reasons = ["flag"] if flagged else []
    jev = item["jev"]
    if jev and not has_manual:
        if jev["entity_type"] == NOT_A_NAME:
            reasons.append("not_a_name")
        elif (jev["confidence"] or 0) < threshold:
            reasons.append("low_confidence")
    if item["entity_subtype"] == "unknown" and not has_manual:
        reasons.append("unknown_subtype")
    return reasons


async def _open_flags(conn: AsyncConnection, entity_ids: list[int]) -> dict[int, list[dict]]:
    if not entity_ids:
        return {}
    erf = entity_review_flags
    rows = (
        await conn.execute(
            select(erf.c.entity_id, erf.c.id, erf.c.reason, erf.c.flagged_by, erf.c.created_at)
            .where(erf.c.entity_id.in_(entity_ids) & erf.c.resolved_at.is_(None))
            .order_by(erf.c.id)
        )
    ).fetchall()
    out: dict[int, list[dict]] = {}
    for eid, flag_id, reason, by, created in rows:
        out.setdefault(eid, []).append(
            {"id": flag_id, "reason": reason, "flagged_by": by, "created_at": created}
        )
    return out


async def review_queue(conn: AsyncConnection, *, tab: Tab, limit: int = 200) -> list[dict]:
    """Return one item per entity in *tab*, at most *limit*.

    Sorted by open flag first, then Jev confidence ascending (no Jev answer
    last), then id.  Each item carries the entity's resolved columns,
    ``reasons`` (``flag``, ``low_confidence``, ``not_a_name``,
    ``unknown_subtype``), its open ``flags``, and the current ``jev`` and
    ``regex`` answers (or None).
    """
    threshold = jev_confidence_threshold()
    q = _queue_source()
    jev = q.jev
    stmt = (
        select(
            entities.c.id,
            entities.c.name,
            entities.c.entity_type,
            entities.c.entity_subtype,
            entities.c.classification_source,
            jev.c.entity_id.label("jev_entity_id"),
            jev.c.entity_type.label("jev_type"),
            jev.c.entity_subtype.label("jev_subtype"),
            jev.c.confidence.label("jev_confidence"),
            jev.c.model.label("jev_model"),
            q.flagged.label("flagged"),
            q.has_manual.label("has_manual"),
        )
        .select_from(q.from_)
        .where(_queue_filter(q, tab, threshold))
        .order_by(q.flagged.desc(), jev.c.confidence.asc().nulls_last(), entities.c.id)
        .limit(limit)
    )
    rows = (await conn.execute(stmt)).mappings().all()
    ids = [r["id"] for r in rows]
    flags = await _open_flags(conn, ids)
    answers = await current_answers(conn, ids) if ids else {}
    items = []
    for r in rows:
        regex = answers.get(r["id"], {}).get("regex")
        item = {
            "entity_id": r["id"],
            "name": r["name"],
            "entity_type": r["entity_type"],
            "entity_subtype": r["entity_subtype"],
            "classification_source": r["classification_source"],
            "flags": flags.get(r["id"], []),
            "jev": None
            if r["jev_entity_id"] is None
            else {
                "entity_type": r["jev_type"],
                "entity_subtype": r["jev_subtype"],
                "confidence": r["jev_confidence"],
                "model": r["jev_model"],
            },
            "regex": None
            if regex is None
            else {"entity_type": regex.entity_type, "entity_subtype": regex.entity_subtype},
        }
        item["reasons"] = _reasons(
            item, flagged=r["flagged"], has_manual=r["has_manual"], threshold=threshold
        )
        items.append(item)
    return items


async def review_counts(conn: AsyncConnection) -> dict[str, int]:
    """Return ``{"main": n, "unknown": n}``: each tab's full size."""
    threshold = jev_confidence_threshold()
    out = {}
    for tab in TABS:
        q = _queue_source()
        out[tab] = (
            await conn.execute(
                select(func.count()).select_from(q.from_).where(_queue_filter(q, tab, threshold))
            )
        ).scalar_one()
    return out


# ---------------------------------------------------------------------------
# Entity detail and health
# ---------------------------------------------------------------------------


async def classification_detail(conn: AsyncConnection, entity_id: int) -> dict:
    """Return the entity's answers for the admin panel.

    ``current``: ``{source: row}``, each source's latest non-ignored row;
    ``history``: every row, newest first, ignored ones included (with
    ``ignored_at``); ``open_flags``.
    """
    ec = entity_classifications
    rows = (
        (
            await conn.execute(
                select(
                    ec.c.id,
                    ec.c.source,
                    ec.c.entity_type,
                    ec.c.entity_subtype,
                    ec.c.confidence,
                    ec.c.model,
                    ec.c.created_at,
                    ec.c.created_by,
                    ec.c.note,
                    ec.c.ignored_at,
                )
                .where(ec.c.entity_id == entity_id)
                .order_by(ec.c.id.desc())
            )
        )
        .mappings()
        .all()
    )
    history = [dict(r) for r in rows]
    current: dict[str, dict] = {}
    for row in history:
        if row["ignored_at"] is None:
            current.setdefault(row["source"], row)
    flags = await _open_flags(conn, [entity_id])
    return {"current": current, "history": history, "open_flags": flags.get(entity_id, [])}


def _spend(tokens: int) -> float:
    return tokens * JEV_USD_PER_MILLION_INPUT_TOKENS / 1_000_000


async def classification_health(conn: AsyncConnection, *, now: datetime) -> dict:
    """Return the dashboard's Jev health and spend numbers as of *now*.

    ``failures_7d`` counts failed batches plus errored runs started in the
    last 7 days, so a run stopped by failed batches counts both.  The month
    is *now*'s UTC calendar month.  ``unfinished_runs`` are runs with no
    ``finished_at``: still going, or died.  ``problems`` lists
    ``missing_api_key`` (enabled without a key) and ``recent_failures``.
    """
    cr = classification_runs
    now = now.astimezone(UTC)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    week_ago = now - timedelta(days=7)
    in_week = cr.c.started_at >= week_ago
    row = (
        await conn.execute(
            select(
                func.max(cr.c.finished_at).filter(cr.c.batches_ok > 0),
                func.coalesce(func.sum(cr.c.batches_failed).filter(in_week), 0),
                func.count().filter(in_week & cr.c.error.is_not(None)),
                func.coalesce(
                    func.sum(cr.c.input_tokens).filter(cr.c.started_at >= month_start), 0
                ),
                func.coalesce(func.sum(cr.c.input_tokens), 0),
                func.count().filter(cr.c.finished_at.is_(None)),
            )
        )
    ).one()
    last_success, failed_batches, errored_runs, tokens_month, tokens_all, unfinished = row
    enabled = is_classification_enabled()
    key_present = bool(get_api_key())
    failures = int(failed_batches) + int(errored_runs)
    counts = await review_counts(conn)
    problems = []
    if enabled and not key_present:
        problems.append("missing_api_key")
    if failures:
        problems.append("recent_failures")
    return {
        "enabled": enabled,
        "key_present": key_present,
        "model": get_model(),
        "threshold": jev_confidence_threshold(),
        "last_success_at": last_success,
        "awaiting": await awaiting_count(conn),
        "failures_7d": failures,
        "unfinished_runs": unfinished,
        "tokens_month": int(tokens_month),
        "tokens_all": int(tokens_all),
        "spend_month_usd": _spend(int(tokens_month)),
        "spend_all_usd": _spend(int(tokens_all)),
        "queue_main": counts["main"],
        "queue_unknown": counts["unknown"],
        "problems": problems,
    }
