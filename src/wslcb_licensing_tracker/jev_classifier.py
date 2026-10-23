"""Jev classification orchestration: candidates, batching, storing, run records (#207).

Classify once: an entity with any stored Jev row (ignored ones included) is
never re-sent by the default mode; only the explicit refresh modes and the
admin single-entity action (``mode="ids", include_manual=True``) re-query.
Every bulk mode skips entities with a current manual row, so no automated pass
spends money second-guessing an operator.

Answers are stored as ``entity_classifications`` rows and resolved through
``entity_classification.resolve_entities``; nothing here writes the resolved
columns on ``entities``.

One run at a time: a session-level Postgres advisory lock, taken and released
on the caller's connection.  The HTTP runs concurrently (``concurrency``
requests in flight); the DB work stays serial on that one connection — asyncpg
rejects concurrent operations on a connection.  Each good batch is stored,
resolved and committed with the run row's running totals, so a killed run
leaves accurate spend behind.

The enable flag gates calls only.  Transport lives in jev_client.py.
"""

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import httpx
from sqlalchemy import Select, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .config import JEV_USD_PER_MILLION_INPUT_TOKENS, jev_confidence_threshold
from .entities import is_noise_name
from .entity_classification import (
    NOT_A_NAME,
    confirmed_noise_ids,
    protected_ids,
    resolve_entities,
)
from .jev_client import (
    BATCH_SIZE,
    CONCURRENCY,
    TIMEOUT,
    JevAnswer,
    JevBatch,
    JevError,
    classify_batch,
    get_api_key,
    get_model,
    is_classification_enabled,
    max_names_per_run,
)
from .models import classification_runs, entities, entity_classifications

logger = logging.getLogger(__name__)

Mode = Literal["unclassified", "refresh_all", "refresh_model", "ids"]
Origin = Literal["post_scrape", "post_import", "cli", "admin"]

# Measured for the kind question alone (research §5.1); #210 (task 5.2)
# re-measures with the org question added.
EST_INPUT_TOKENS_PER_NAME = 238
MAX_CONSECUTIVE_FAILURES = 3
ADVISORY_LOCK_KEY = 0x6A6576  # "jev"

_CHUNK = 1000


@dataclass
class RunSummary:
    """What one ``classify_entities`` call did; ``run_id`` is its ``classification_runs`` row."""

    origin: str
    enabled: bool
    key_present: bool
    model: str
    candidates: int = 0
    names_sent: int = 0
    batches_ok: int = 0
    batches_failed: int = 0
    input_tokens: int = 0
    dry_run: bool = False
    skipped_locked: bool = False
    error: str | None = None
    run_id: int | None = None

    @property
    def estimated_cost_usd(self) -> float:
        """Spend at list price: measured tokens, or the per-name estimate on a dry-run."""
        tokens = self.candidates * EST_INPUT_TOKENS_PER_NAME if self.dry_run else self.input_tokens
        return tokens * JEV_USD_PER_MILLION_INPUT_TOKENS / 1_000_000


def map_answer(answer: JevAnswer, *, threshold: float) -> tuple[str, str | None]:
    """Return the ``(entity_type, entity_subtype)`` a Jev answer is stored as.

    ``not_a_name`` is stored as-is (the resolver never picks it).  An
    organization whose ``org`` answer is below *threshold* gets ``unknown``.
    """
    if answer.kind in ("trust", "estate"):
        return "organization", answer.kind
    if answer.kind == "organization":
        return "organization", answer.org if answer.org_confidence >= threshold else "unknown"
    if answer.kind == "person":
        return "person", None
    return NOT_A_NAME, None


def _has_jev(**match: object):  # noqa: ANN202 — an EXISTS clause
    ec = entity_classifications
    where = (ec.c.entity_id == entities.c.id) & (ec.c.source == "jev")
    for column, value in match.items():
        where &= ec.c[column] == value
    return select(ec.c.id).where(where).exists()


def _has_current_manual():  # noqa: ANN202 — an EXISTS clause
    ec = entity_classifications
    return (
        select(ec.c.id)
        .where(
            (ec.c.entity_id == entities.c.id)
            & (ec.c.source == "manual")
            & ec.c.ignored_at.is_(None)
        )
        .exists()
    )


def _candidate_stmt(
    mode: Mode, model: str | None, ids: Sequence[int] | None, *, include_manual: bool
) -> Select:
    stmt = select(entities.c.id, entities.c.name).order_by(entities.c.id)
    if mode == "unclassified":
        stmt = stmt.where(~_has_jev())
    elif mode == "refresh_model":
        if not model:
            msg = "mode 'refresh_model' needs a model"
            raise ValueError(msg)
        stmt = stmt.where(_has_jev(model=model))
    elif mode == "ids":
        if ids is None:
            msg = "mode 'ids' needs ids"
            raise ValueError(msg)
    elif mode != "refresh_all":
        msg = f"unknown mode {mode!r}"
        raise ValueError(msg)
    if not include_manual:
        stmt = stmt.where(~_has_current_manual())
    return stmt


async def _purgeable_noise(
    conn: AsyncConnection, rows: Sequence[tuple[int, str]], *, include_manual: bool
) -> set[int]:
    """Ids among *rows* the reclassify purge would delete: ``(noise - protected) | confirmed``."""
    noise = {eid for eid, name in rows if is_noise_name(name)}
    if noise:
        noise -= await protected_ids(conn)
    if include_manual:  # otherwise confirmed noise (a manual row) is already excluded
        noise |= await confirmed_noise_ids(conn)
    return noise


async def select_candidates(  # noqa: PLR0913 — one keyword per selection rule
    conn: AsyncConnection,
    *,
    mode: Mode,
    model: str | None = None,
    ids: Sequence[int] | None = None,
    include_manual: bool = False,
    limit: int | None = None,
) -> list[tuple[int, str]]:
    """Return ``(entity_id, name)`` pairs to send, ordered by id, at most *limit*.

    - ``unclassified``: no Jev row at all (ignored rows count as classified);
    - ``refresh_all``: every entity;
    - ``refresh_model``: has a Jev row from *model*;
    - ``ids``: those entities.

    Every mode skips an entity with a current manual row unless
    *include_manual*.  Noise names (``entities.is_noise_name``) are skipped
    unless protected (a manual decision or an open flag): the reclassify purge
    would delete them, and their paid answers with them.
    """
    stmt = _candidate_stmt(mode, model, ids, include_manual=include_manual)
    if mode == "ids":
        wanted = sorted(set(ids or ()))
        rows = []
        for i in range(0, len(wanted), _CHUNK):
            chunk = wanted[i : i + _CHUNK]
            rows += (await conn.execute(stmt.where(entities.c.id.in_(chunk)))).fetchall()
    else:
        rows = (await conn.execute(stmt)).fetchall()
    purgeable = await _purgeable_noise(conn, rows, include_manual=include_manual)
    picked = [(eid, name) for eid, name in rows if eid not in purgeable]
    return picked if limit is None else picked[:limit]


async def awaiting_count(conn: AsyncConnection) -> int:
    """Entities the default mode would send, ignoring the per-run cap."""
    return len(await select_candidates(conn, mode="unclassified"))


async def store_batch(
    conn: AsyncConnection,
    entities_sent: Sequence[tuple[int, str]],
    batch: JevBatch,
    *,
    threshold: float,
) -> list[int]:
    """Insert one Jev row per entity that still exists; return the stored ids.

    An entity merged or purged since it was selected is skipped, not an FK
    violation: the survivors are locked ``FOR KEY SHARE`` first, so none can be
    deleted before the insert commits.  Each row's ``input_tokens`` is its
    share of the batch (total ÷ names sent).  Caller commits.
    """
    sent_ids = [eid for eid, _ in entities_sent]
    alive = set(
        (
            await conn.execute(
                select(entities.c.id)
                .where(entities.c.id.in_(sent_ids))
                .with_for_update(key_share=True)
            )
        )
        .scalars()
        .all()
    )
    share = batch.input_tokens / len(entities_sent)
    rows = []
    for (eid, _), answer in zip(entities_sent, batch.answers, strict=True):
        if eid not in alive:
            continue
        entity_type, entity_subtype = map_answer(answer, threshold=threshold)
        rows.append(
            {
                "entity_id": eid,
                "source": "jev",
                "entity_type": entity_type,
                "entity_subtype": entity_subtype,
                "confidence": answer.kind_confidence,
                "probabilities": answer.raw,
                "model": batch.model,
                "input_tokens": share,
            }
        )
    if rows:
        await conn.execute(insert(entity_classifications), rows)
    skipped = len(sent_ids) - len(rows)
    if skipped:
        logger.info("Jev: %d entit(ies) gone since selection; not stored", skipped)
    return [r["entity_id"] for r in rows]


async def _try_lock(conn: AsyncConnection) -> bool:
    got = (await conn.execute(select(func.pg_try_advisory_lock(ADVISORY_LOCK_KEY)))).scalar_one()
    await conn.commit()  # the lock is session-level; end the transaction that took it
    return bool(got)


async def _unlock(conn: AsyncConnection) -> bool:
    released = (await conn.execute(select(func.pg_advisory_unlock(ADVISORY_LOCK_KEY)))).scalar_one()
    await conn.commit()
    return bool(released)


def _run_totals(summary: RunSummary) -> dict:
    return {
        "names_sent": summary.names_sent,
        "batches_ok": summary.batches_ok,
        "batches_failed": summary.batches_failed,
        "input_tokens": summary.input_tokens,
    }


async def _save_progress(conn: AsyncConnection, summary: RunSummary) -> None:
    """Write the run's running totals and commit (with any batch just stored)."""
    await conn.execute(
        update(classification_runs)
        .where(classification_runs.c.id == summary.run_id)
        .values(**_run_totals(summary))
    )
    await conn.commit()


async def _finish(conn: AsyncConnection, summary: RunSummary) -> None:
    """Close the run row and release the lock, on the connection that took it.

    Rolls back first: after an exception the transaction may be aborted, and
    an aborted transaction rejects the unlock.  If the lock can't be confirmed
    released, the connection is invalidated — closing the session releases
    it — rather than returned to the pool still holding it.
    """
    try:
        await conn.rollback()
        if summary.run_id is not None:
            await conn.execute(
                update(classification_runs)
                .where(classification_runs.c.id == summary.run_id)
                .values(finished_at=func.now(), error=summary.error, **_run_totals(summary))
            )
            await conn.commit()
        if await _unlock(conn):
            return
        logger.error("Jev: advisory lock was not held at release; invalidating the connection")
    except Exception:
        logger.exception("Jev: could not close the run cleanly; invalidating the connection")
    await conn.invalidate()


async def _post(
    chunk: list[tuple[int, str]],
    slots: asyncio.Semaphore,
    *,
    api_key: str,
    model: str,
    client: httpx.AsyncClient,
) -> tuple[list[tuple[int, str]], JevBatch | JevError]:
    """One batch's request; a JevError is returned, not raised, so the run decides.

    Takes a slot and keeps it: the run releases it once it has handled the
    result, so no request goes out between a failure and the decision to stop.
    """
    await slots.acquire()
    try:
        return chunk, await classify_batch(
            [name for _, name in chunk], api_key=api_key, model=model, client=client
        )
    except JevError as e:
        return chunk, e
    except BaseException:
        slots.release()
        raise


async def _handle(
    conn: AsyncConnection,
    summary: RunSummary,
    chunk: list[tuple[int, str]],
    outcome: JevBatch | JevError,
    *,
    threshold: float,
) -> bool:
    """Store a good batch or count a failed one; return whether it failed."""
    summary.names_sent += len(chunk)
    if isinstance(outcome, JevError):
        summary.batches_failed += 1
        logger.warning("Jev batch of %d name(s) failed: %s", len(chunk), outcome)
        await _save_progress(conn, summary)
        return True
    stored = await store_batch(conn, chunk, outcome, threshold=threshold)
    if stored:
        await resolve_entities(conn, stored)
    summary.batches_ok += 1
    summary.input_tokens += outcome.input_tokens
    await _save_progress(conn, summary)
    return False


async def _run_batches(  # noqa: PLR0913 — the run's settings, passed through
    conn: AsyncConnection,
    summary: RunSummary,
    candidates: list[tuple[int, str]],
    *,
    api_key: str,
    client: httpx.AsyncClient,
    concurrency: int,
) -> None:
    """Post every batch, ``concurrency`` in flight; handle results serially on *conn*."""
    threshold = jev_confidence_threshold()
    slots = asyncio.Semaphore(concurrency)
    tasks = [
        asyncio.create_task(
            _post(
                candidates[i : i + BATCH_SIZE],
                slots,
                api_key=api_key,
                model=summary.model,
                client=client,
            )
        )
        for i in range(0, len(candidates), BATCH_SIZE)
    ]
    consecutive = 0
    try:
        for next_done in asyncio.as_completed(tasks):
            chunk, outcome = await next_done
            try:
                failed = await _handle(conn, summary, chunk, outcome, threshold=threshold)
            finally:
                slots.release()
            if not failed:
                consecutive = 0
                continue
            consecutive += 1
            if isinstance(outcome, JevError) and outcome.auth:
                summary.error = "auth_failed"
                logger.error("Jev rejected the API key (HTTP %s); stopping", outcome.status)
                return
            if consecutive >= MAX_CONSECUTIVE_FAILURES:
                summary.error = "consecutive_failures"
                logger.error("Jev: %d consecutive failed batches; stopping", consecutive)
                return
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def classify_entities(  # noqa: PLR0913 — one keyword per CLI/admin option
    conn: AsyncConnection,
    *,
    origin: Origin,
    mode: Mode = "unclassified",
    model_filter: str | None = None,
    ids: Sequence[int] | None = None,
    include_manual: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
    client: httpx.AsyncClient | None = None,
    concurrency: int = CONCURRENCY,
) -> RunSummary:
    """Send candidates to Jev in batches of 25, store and resolve the answers.

    In order: a dry-run only counts (whatever the flag says; no calls, no run
    row).  With the flag off, nothing happens.  With no API key, a finished run
    row records ``missing_api_key``.  If another run holds the advisory lock,
    ``skipped_locked``.  Otherwise a run row, then batches; an auth failure or
    ``MAX_CONSECUTIVE_FAILURES`` in a row stops the run.  ``limit=None`` means
    ``max_names_per_run()``.  Commits as it goes.
    """
    api_key = get_api_key()
    summary = RunSummary(
        origin=origin,
        enabled=is_classification_enabled(),
        key_present=bool(api_key),
        model=get_model(),
        dry_run=dry_run,
    )
    cap = max_names_per_run() if limit is None else limit
    select_kw = {"mode": mode, "model": model_filter, "ids": ids, "include_manual": include_manual}

    if dry_run:
        summary.candidates = len(await select_candidates(conn, **select_kw, limit=cap))
        return summary
    if not summary.enabled:
        logger.debug("Jev classification disabled; nothing sent (%s)", origin)
        return summary
    if not api_key:
        summary.error = "missing_api_key"
        summary.run_id = (
            await conn.execute(
                insert(classification_runs)
                .values(
                    origin=origin,
                    model=summary.model,
                    finished_at=func.now(),
                    error=summary.error,
                )
                .returning(classification_runs.c.id)
            )
        ).scalar_one()
        await conn.commit()
        logger.error("Jev classification is enabled but the TypeSafe API key is unset")
        return summary
    if not await _try_lock(conn):
        summary.skipped_locked = True
        logger.info("Jev: another classification run holds the lock; skipping (%s)", origin)
        return summary

    try:
        candidates = await select_candidates(conn, **select_kw, limit=cap)
        summary.candidates = len(candidates)
        summary.run_id = (
            await conn.execute(
                insert(classification_runs)
                .values(origin=origin, model=summary.model)
                .returning(classification_runs.c.id)
            )
        ).scalar_one()
        await conn.commit()
        if candidates:
            if client is None:
                async with httpx.AsyncClient(timeout=TIMEOUT) as own:
                    await _run_batches(
                        conn,
                        summary,
                        candidates,
                        api_key=api_key,
                        client=own,
                        concurrency=concurrency,
                    )
            else:
                await _run_batches(
                    conn,
                    summary,
                    candidates,
                    api_key=api_key,
                    client=client,
                    concurrency=concurrency,
                )
    except BaseException as e:
        summary.error = summary.error or type(e).__name__
        raise
    finally:
        await _finish(conn, summary)
    logger.info(
        "Jev classification (%s): %d candidate(s), %d sent, %d ok / %d failed batch(es), "
        "%d token(s), model %s%s",
        origin,
        summary.candidates,
        summary.names_sent,
        summary.batches_ok,
        summary.batches_failed,
        summary.input_tokens,
        summary.model,
        f", error {summary.error}" if summary.error else "",
    )
    return summary
