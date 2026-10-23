"""FastAPI APIRouter for /admin/entities/* — classification review (#208).

The review queue page, and the POST actions behind it and the entity-page
admin panel: set, accept, not-a-name, clear override, flag, re-classify with
Jev.  Each POST commits and then 303-redirects to its ``next`` field (a local
path only).  ``ValueError`` → 400 and ``LookupError`` → 404, with nothing
committed.

No CSRF token: the posture of the other admin routes (exe.dev proxy login).
"""

import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from .admin_audit import log_action
from .admin_auth import require_admin
from .admin_routes import render_admin
from .classification_review import (
    TABS,
    accept_answer,
    clear_override,
    flag_entity,
    mark_not_a_name,
    review_counts,
    review_queue,
    set_classification,
)
from .config import jev_confidence_threshold
from .engine import get_db
from .entity_classification import ENTITY_SUBTYPES, ENTITY_TYPES
from .jev_classifier import RunSummary, classify_entities
from .jev_client import is_classification_enabled
from .models import entities

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

Admin = Annotated[dict[str, Any], Depends(require_admin)]
Next = Annotated[str | None, Form(alias="next")]

BUSY_RETRY_AFTER_SECONDS = 60


def _safe_next(next_: str | None, entity_id: int) -> str:
    r"""Return *next_* when it is a local path, else ``/entity/{entity_id}``.

    Rejects ``//host`` and ``/\host`` (browsers read both as another host),
    and control characters, which browsers strip before parsing.
    """
    if (
        next_
        and next_.startswith("/")
        and next_[1:2] not in ("/", "\\")
        and "\\" not in next_
        and all(ch >= " " for ch in next_)
    ):
        return next_
    return f"/entity/{entity_id}"


def _blank_to_none(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


async def _act(
    request: Request,
    entity_id: int,
    next_: str | None,
    action: Callable[[AsyncConnection], Awaitable[object]],
) -> RedirectResponse:
    """Run *action*, commit, redirect; map ValueError → 400 and LookupError → 404."""
    async with get_db(request.app.state.engine) as conn:
        try:
            await action(conn)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except LookupError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        await conn.commit()
    return RedirectResponse(_safe_next(next_, entity_id), status_code=303)


@router.get("/entities/review", response_class=HTMLResponse)
async def admin_entity_review(request: Request, admin: Admin, tab: str = "main") -> HTMLResponse:
    """The classification review queue: a main tab and a *Subtype unknown* tab."""
    if tab not in TABS:
        tab = "main"
    async with get_db(request.app.state.engine) as conn:
        items = await review_queue(conn, tab=tab)
        counts = await review_counts(conn)
    return await render_admin(
        request,
        "admin/entity_review.html",
        {
            "request": request,
            "admin": admin,
            "active_section": "entity-review",
            "tab": tab,
            "items": items,
            "counts": counts,
            "threshold": jev_confidence_threshold(),
            "entity_types": ENTITY_TYPES,
            "entity_subtypes": ENTITY_SUBTYPES,
            "next_url": f"/admin/entities/review?tab={tab}",
        },
    )


@router.post("/entities/{entity_id}/classify")
async def admin_entity_classify(  # noqa: PLR0913
    request: Request,
    admin: Admin,
    entity_id: int,
    entity_type: Annotated[str, Form()],
    entity_subtype: Annotated[str, Form()] = "",
    note: Annotated[str, Form()] = "",
    next_url: Next = None,
) -> RedirectResponse:
    """Set an operator classification."""
    return await _act(
        request,
        entity_id,
        next_url,
        lambda conn: set_classification(
            conn,
            entity_id,
            entity_type=entity_type.strip(),
            entity_subtype=_blank_to_none(entity_subtype),
            note=_blank_to_none(note),
            admin_email=admin["email"],
        ),
    )


@router.post("/entities/{entity_id}/accept")
async def admin_entity_accept(
    request: Request,
    admin: Admin,
    entity_id: int,
    source: Annotated[str, Form()],
    next_url: Next = None,
) -> RedirectResponse:
    """Accept Jev's or the rules' current answer."""
    return await _act(
        request,
        entity_id,
        next_url,
        lambda conn: accept_answer(conn, entity_id, source=source, admin_email=admin["email"]),
    )


@router.post("/entities/{entity_id}/not-a-name")
async def admin_entity_not_a_name(
    request: Request,
    admin: Admin,
    entity_id: int,
    note: Annotated[str, Form()] = "",
    next_url: Next = None,
) -> RedirectResponse:
    """Confirm the entity is not a name; the next reclassify purges it."""
    return await _act(
        request,
        entity_id,
        next_url,
        lambda conn: mark_not_a_name(
            conn, entity_id, admin_email=admin["email"], note=_blank_to_none(note)
        ),
    )


@router.post("/entities/{entity_id}/clear-manual")
async def admin_entity_clear_manual(
    request: Request,
    admin: Admin,
    entity_id: int,
    next_url: Next = None,
) -> RedirectResponse:
    """Return the entity to automatic resolution."""
    return await _act(
        request,
        entity_id,
        next_url,
        lambda conn: clear_override(conn, entity_id, admin_email=admin["email"]),
    )


@router.post("/entities/{entity_id}/flag")
async def admin_entity_flag(
    request: Request,
    admin: Admin,
    entity_id: int,
    reason: Annotated[str, Form()] = "",
    next_url: Next = None,
) -> RedirectResponse:
    """Flag the entity for review."""
    return await _act(
        request,
        entity_id,
        next_url,
        lambda conn: flag_entity(conn, entity_id, reason=reason, admin_email=admin["email"]),
    )


def _jev_audit_details(summary: RunSummary | None, error: str | None) -> dict:
    if summary is None:
        return {"error": error}
    return {
        "run_id": summary.run_id,
        "model": summary.model,
        "names_sent": summary.names_sent,
        "batches_ok": summary.batches_ok,
        "batches_failed": summary.batches_failed,
        "input_tokens": summary.input_tokens,
        "skipped_locked": summary.skipped_locked,
        "error": error,
    }


@router.post("/entities/{entity_id}/jev")
async def admin_entity_jev(
    request: Request,
    admin: Admin,
    entity_id: int,
    next_url: Next = None,
) -> RedirectResponse:
    """Re-classify one entity with Jev, manual override or not.  A paid call.

    ``classify_entities`` owns its connection's transactions, so the audit row
    is written afterwards on a connection of its own.  An exception is still
    audited (money may have been spent), then answered with a 502.
    """
    if not is_classification_enabled():
        raise HTTPException(status_code=409, detail="Jev classification is disabled")
    engine = request.app.state.engine
    async with get_db(engine) as conn:
        found = (
            await conn.execute(select(entities.c.id).where(entities.c.id == entity_id))
        ).scalar_one_or_none()
    if found is None:
        raise HTTPException(status_code=404, detail=f"entity {entity_id} not found")

    summary: RunSummary | None = None
    try:
        async with get_db(engine) as conn:
            summary = await classify_entities(
                conn, origin="admin", mode="ids", ids=[entity_id], include_manual=True, limit=1
            )
        error = summary.error
    except Exception as e:
        logger.exception("Jev re-classify of entity %d raised", entity_id)
        error = type(e).__name__

    if summary is not None and not summary.enabled:
        raise HTTPException(status_code=409, detail="Jev classification is disabled")
    async with get_db(engine) as conn:
        await log_action(
            conn,
            admin["email"],
            "entity.reclassify_jev",
            "entity",
            entity_id,
            _jev_audit_details(summary, error),
        )
        await conn.commit()

    if summary is not None and summary.skipped_locked:
        raise HTTPException(
            status_code=503,
            detail="Jev is busy with another classification run; try again shortly.",
            headers={"Retry-After": str(BUSY_RETRY_AFTER_SECONDS)},
        )
    if error:
        raise HTTPException(status_code=502, detail=f"Jev classification failed: {error}")
    if summary is not None and summary.batches_failed:
        raise HTTPException(
            status_code=502,
            detail="Jev classification failed: the request failed; see the web service log.",
        )
    return RedirectResponse(_safe_next(next_url, entity_id), status_code=303)
