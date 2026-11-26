"""Operator alias actions with audit rows, and the entity page's Links panel (#264).

Wraps the write API of :mod:`entity_aliases` (#262), which writes no audit
rows, the way :mod:`classification_review` wraps the classification writes.
Each action writes one ``admin_audit_log`` row in the caller's transaction,
whose details carry **both names** as well as both ids, the scope, the
source and the note (spec §4: an audit row with no name cannot be replayed).

| Action | Audit action |
|---|---|
| :func:`link` | ``entity.alias_set`` |
| :func:`unlink` of a manual row | ``entity.alias_remove`` |
| :func:`unlink` of a rule row, :func:`dismiss` | ``entity.alias_dismiss`` |
| :func:`undismiss` | ``entity.alias_undismiss`` |

Raises :class:`~entity_aliases.AliasError` (a ``ValueError``) for a refused
write and ``LookupError`` for a missing entity or row; nothing is written
then.  Caller commits.
"""

import logging
import re

from sqlalchemy import delete, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from .admin_audit import log_action
from .classification_review import MAX_TEXT_CHARS
from .entity_aliases import (
    AliasError,
    add_manual_alias,
    dismiss_pair,
    in_manual_alias,
    relink,
    remove_manual_alias,
)
from .models import entities, entity_alias_dismissals, entity_aliases

logger = logging.getLogger(__name__)

_ea = entity_aliases
_ead = entity_alias_dismissals
# A target typed as an id, or picked from the lookup list ("41265 · NAME").
# Anything else is a name, even with leading digits (``7-ELEVEN``, ``13 COINS``).
_LEADING_ID = re.compile(r"^\s*#?(\d+)\s*(?:·.*)?$")


def _check_note(note: str | None) -> None:
    if note is not None and len(note) > MAX_TEXT_CHARS:
        msg = f"note is {len(note)} characters; the limit is {MAX_TEXT_CHARS}"
        raise ValueError(msg)


async def _names(conn: AsyncConnection, *ids: int) -> dict[int, str]:
    stmt = select(entities.c.id, entities.c.name).where(entities.c.id.in_(ids))
    names = dict((await conn.execute(stmt)).fetchall())
    for eid in ids:
        if eid not in names:
            msg = f"entity {eid} not found"
            raise LookupError(msg)
    return names


async def entity_licences(conn: AsyncConnection, *entity_ids: int) -> list[str]:
    """Sorted licence numbers of the records the entities' own links are on."""
    rows = await conn.execute(
        text(
            "SELECT DISTINCT lr.license_number FROM record_entities re"
            " JOIN license_records lr ON lr.id = re.record_id"
            " WHERE re.entity_id = ANY(:ids) ORDER BY 1"
        ),
        {"ids": list(entity_ids)},
    )
    return list(rows.scalars())


async def _check_scope(conn: AsyncConnection, scope_license: str | None, *entity_ids: int) -> None:
    """A licence scope must be one of the entities' own licences."""
    if scope_license is None:
        return
    if scope_license not in await entity_licences(conn, *entity_ids):
        msg = f"licence {scope_license} is not one of this entity's licences"
        raise AliasError(msg)


async def parse_target(conn: AsyncConnection, target: str) -> int:
    """Return the entity id an operator typed: an id (``41265``, ``#41265 · NAME``) or a name.

    A name must match exactly one entity, case ignored; otherwise
    :class:`AliasError` names up to five candidates.
    """
    target = (target or "").strip()
    if not target:
        msg = "give a target entity: its id, or pick a name from the list"
        raise AliasError(msg)
    m = _LEADING_ID.match(target)
    if m:
        return int(m.group(1))
    rows = (
        await conn.execute(
            select(entities.c.id, entities.c.name)
            .where(func.upper(entities.c.name) == target.upper())
            .order_by(entities.c.id)
            .limit(6)
        )
    ).fetchall()
    if len(rows) == 1:
        return rows[0].id
    if not rows:
        msg = f"no entity is named {target!r}; give its id, or pick a name from the list"
        raise AliasError(msg)
    listed = ", ".join(f"{r.name} ({r.id})" for r in rows[:5])
    msg = f"{target!r} names more than one entity ({listed}); give the id"
    raise AliasError(msg)


async def lookup(conn: AsyncConnection, q: str, *, limit: int = 20) -> list[dict]:
    """Entities whose name contains *q* (2+ characters), prefix matches first."""
    q = (q or "").strip()
    if len(q) < 2:  # noqa: PLR2004 — a one-letter search matches most of the table
        return []
    rows = await conn.execute(
        select(entities.c.id, entities.c.name, entities.c.entity_type)
        # autoescape: a typed % or _ is a character, not a wildcard.
        .where(entities.c.name.icontains(q, autoescape=True))
        .order_by(
            (~entities.c.name.istartswith(q, autoescape=True)),
            func.length(entities.c.name),
            entities.c.name,
            entities.c.id,
        )
        .limit(limit)
    )
    return [dict(r) for r in rows.mappings()]


async def link(  # noqa: PLR0913
    conn: AsyncConnection,
    variant_id: int,
    canonical_id: int,
    *,
    scope_license: str | None,
    note: str | None,
    admin_email: str,
    suggestion: str | None = None,
) -> int:
    """Link *variant_id* to *canonical_id* (``entity.alias_set``); return the row id.

    *scope_license* must be one of the variant's licences.  *suggestion* is
    the Links-tab kind the action came from, if any.
    """
    _check_note(note)
    names = await _names(conn, variant_id, canonical_id)
    await _check_scope(conn, scope_license, variant_id)
    alias_id = await add_manual_alias(
        conn,
        variant_id,
        canonical_id,
        scope_license=scope_license,
        created_by=admin_email,
        note=note,
    )
    await log_action(
        conn,
        admin_email,
        "entity.alias_set",
        "entity",
        variant_id,
        {
            "alias_id": alias_id,
            "variant_id": variant_id,
            "variant_name": names[variant_id],
            "canonical_id": canonical_id,
            "canonical_name": names[canonical_id],
            "scope_license": scope_license,
            "source": "manual",
            "rule": None,
            "note": note,
            "suggestion": suggestion,
        },
    )
    return alias_id


async def unlink(conn: AsyncConnection, alias_id: int, *, admin_email: str) -> str:
    """Undo alias row *alias_id*; return ``"removed"`` or ``"dismissed"``.

    A manual row is deleted (``entity.alias_remove``).  A rule row is derived,
    so it gets a dismissal in its own scope instead (``entity.alias_dismiss``,
    source ``rule``), which keeps ``relink`` from writing it again.
    """
    row = (
        (
            await conn.execute(
                select(
                    _ea.c.variant_id,
                    _ea.c.canonical_id,
                    _ea.c.scope_license,
                    _ea.c.source,
                    _ea.c.rule,
                    _ea.c.note,
                    _ea.c.created_by,
                ).where(_ea.c.id == alias_id)
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        msg = f"alias {alias_id} not found"
        raise LookupError(msg)
    names = await _names(conn, row["variant_id"], row["canonical_id"])
    if row["source"] == "manual":
        await remove_manual_alias(conn, alias_id)
        await log_action(
            conn,
            admin_email,
            "entity.alias_remove",
            "entity",
            row["variant_id"],
            {
                "alias_id": alias_id,
                "variant_id": row["variant_id"],
                "variant_name": names[row["variant_id"]],
                "canonical_id": row["canonical_id"],
                "canonical_name": names[row["canonical_id"]],
                "scope_license": row["scope_license"],
                "source": "manual",
                "rule": None,
                "note": row["note"],
                "created_by": row["created_by"],
            },
        )
        return "removed"
    await _dismiss(
        conn,
        row["variant_id"],
        row["canonical_id"],
        names,
        scope_license=row["scope_license"],
        note=None,
        admin_email=admin_email,
        source="rule",
        rule=row["rule"],
    )
    return "dismissed"


async def _dismiss(  # noqa: PLR0913
    conn: AsyncConnection,
    entity_id: int,
    other_id: int,
    names: dict[int, str],
    *,
    scope_license: str | None,
    note: str | None,
    admin_email: str,
    source: str,
    rule: str | None = None,
    suggestion: str | None = None,
) -> int:
    written = await dismiss_pair(
        conn,
        entity_id,
        other_id,
        scope_license=scope_license,
        dismissed_by=admin_email,
        note=note,
    )
    if not written:
        msg = (
            f"{names[entity_id]} and {names[other_id]} are already dismissed "
            f"{'on licence ' + scope_license if scope_license else 'everywhere'}"
        )
        raise AliasError(msg)
    a, b = sorted((entity_id, other_id))
    scope = (
        _ead.c.scope_license.is_(None)
        if scope_license is None
        else _ead.c.scope_license == scope_license
    )
    dismissal_id = (
        await conn.execute(
            select(_ead.c.id).where((_ead.c.entity_id_a == a) & (_ead.c.entity_id_b == b) & scope)
        )
    ).scalar_one()
    await log_action(
        conn,
        admin_email,
        "entity.alias_dismiss",
        "entity",
        entity_id,
        {
            "dismissal_id": dismissal_id,
            "entity_id": entity_id,
            "entity_name": names[entity_id],
            "other_id": other_id,
            "other_name": names[other_id],
            "scope_license": scope_license,
            "source": source,
            "rule": rule,
            "note": note,
            "suggestion": suggestion,
        },
    )
    return dismissal_id


async def dismiss(  # noqa: PLR0913
    conn: AsyncConnection,
    entity_id: int,
    other_id: int,
    *,
    scope_license: str | None,
    note: str | None,
    admin_email: str,
    suggestion: str | None = None,
) -> int:
    """Record that the two are not one party (``entity.alias_dismiss``); return its id.

    *scope_license* must be a licence of one of the two.  A pair already
    dismissed in that scope is refused.
    """
    _check_note(note)
    names = await _names(conn, entity_id, other_id)
    await _check_scope(conn, scope_license, entity_id, other_id)
    return await _dismiss(
        conn,
        entity_id,
        other_id,
        names,
        scope_license=scope_license,
        note=note,
        admin_email=admin_email,
        source="manual",
        suggestion=suggestion,
    )


async def undismiss(conn: AsyncConnection, dismissal_id: int, *, admin_email: str) -> None:
    """Delete dismissal *dismissal_id*, relink the pair (``entity.alias_undismiss``)."""
    row = (
        (
            await conn.execute(
                delete(_ead)
                .where(_ead.c.id == dismissal_id)
                .returning(
                    _ead.c.entity_id_a,
                    _ead.c.entity_id_b,
                    _ead.c.scope_license,
                    _ead.c.dismissed_by,
                    _ead.c.note,
                )
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        msg = f"dismissal {dismissal_id} not found"
        raise LookupError(msg)
    a, b = row["entity_id_a"], row["entity_id_b"]
    names = await _names(conn, a, b)
    await relink(conn, [a, b])
    await log_action(
        conn,
        admin_email,
        "entity.alias_undismiss",
        "entity",
        a,
        {
            "dismissal_id": dismissal_id,
            "entity_id": a,
            "entity_name": names[a],
            "other_id": b,
            "other_name": names[b],
            "scope_license": row["scope_license"],
            "dismissed_by": row["dismissed_by"],
            "note": row["note"],
        },
    )


async def links_detail(conn: AsyncConnection, entity_id: int) -> dict:
    """The Links panel's data for *entity_id*.

    ``outgoing`` (rows whose variant it is) and ``incoming`` (rows whose
    canonical it is): ``{id, other_id, other_name, scope, source, rule,
    created_at, created_by, note}``.  ``dismissals``: ``{id, other_id,
    other_name, scope, dismissed_by, dismissed_at, note}``.  ``licences``:
    its own licences (the scopes it can link on).  ``in_manual``: whether a
    manual row holds it, which bars not-a-name.
    """
    other = entities.alias("other")

    async def rows(own, far) -> list[dict]:  # noqa: ANN001 — two columns of _ea
        stmt = (
            select(
                _ea.c.id,
                far.label("other_id"),
                other.c.name.label("other_name"),
                _ea.c.scope_license.label("scope"),
                _ea.c.source,
                _ea.c.rule,
                _ea.c.created_at,
                _ea.c.created_by,
                _ea.c.note,
            )
            .join(other, other.c.id == far)
            .where(own == entity_id)
            .order_by(other.c.name, _ea.c.scope_license.nulls_first(), _ea.c.id)
        )
        return [dict(r) for r in (await conn.execute(stmt)).mappings()]

    # The pair is stored ordered (a < b); the other end is the one that isn't this entity.
    far = (_ead.c.entity_id_a + _ead.c.entity_id_b) - entity_id
    dismissals = [
        dict(r)
        for r in (
            await conn.execute(
                select(
                    _ead.c.id,
                    far.label("other_id"),
                    other.c.name.label("other_name"),
                    _ead.c.scope_license.label("scope"),
                    _ead.c.dismissed_by,
                    _ead.c.dismissed_at,
                    _ead.c.note,
                )
                .join(other, other.c.id == far)
                .where(or_(_ead.c.entity_id_a == entity_id, _ead.c.entity_id_b == entity_id))
                .order_by(other.c.name, _ead.c.id)
            )
        ).mappings()
    ]
    return {
        "outgoing": await rows(_ea.c.variant_id, _ea.c.canonical_id),
        "incoming": await rows(_ea.c.canonical_id, _ea.c.variant_id),
        "dismissals": dismissals,
        "licences": await entity_licences(conn, entity_id),
        "in_manual": await in_manual_alias(conn, entity_id),
    }
