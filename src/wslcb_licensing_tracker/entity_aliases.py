"""Entity alias layer: variant → canonical links over kept entity rows (#262).

Design: ``docs/specs/2026-11-23-247-entity-name-linking-design.md`` §1-§3.

An entity stays one cleaned string.  ``entity_aliases`` maps a variant
entity to a canonical one, globally (``scope_license`` NULL) or on one
licence.  Rows have a source:

- ``rule`` rows are **derived**: :func:`relink` rebuilds them (the
  ``format_key`` rule here; #265 adds ``middle_names``).
- ``manual`` rows and every ``entity_alias_dismissals`` row are **operator
  data**: nothing automated rewrites or deletes them.

Rows store what was decided; reads resolve (:func:`resolve`, :func:`eff_many`).
The writers keep the graph free of cycles, and resolution is capped at
:data:`MAX_STEPS` anyway.

This module holds the pure functions (``link_key``, ``elect_canonical``,
``resolve``, ``find_cycle``) and the database side (write API, ``relink``).
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import bindparam, delete, func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .entity_classification import tombstone_ids
from .models import (
    entities,
    entity_alias_dismissals,
    entity_aliases,
    entity_review_flags,
    record_entities,
)

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Sequence

    from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)

MAX_STEPS = 4
RULE_FORMAT_KEY = "format_key"
KEYED_TYPES = frozenset({"person", "organization"})

_GENERATIONAL = frozenset({"JR", "SR", "II", "III", "IV"})
# A trailing FRANCHISOR/FRANCHISEE role annotation, with its separator (spec §2).
_ROLE_ANNOTATION = re.compile(r"[\s,(\-]*\(?\s*FRANCHIS(?:OR|EE)\s*\)?$")
_PUNCT = re.compile(r"[,.]")


@dataclass(frozen=True)
class AliasRow:
    """One ``entity_aliases`` row, as the pure functions see it."""

    variant: int
    canonical: int
    scope: str | None
    source: str
    rule: str | None = None


@dataclass(frozen=True)
class Member:
    """A link-key group member: id, cleaned name, and its record-link count."""

    id: int
    name: str
    links: int


# ── link_key (spec §2) ───────────────────────────────────────────────


def _squash(s: str) -> str:
    return " ".join(_PUNCT.sub(" ", s).split())


def _is_generational(tail: str) -> bool:
    return tail.strip().rstrip(".").strip() in _GENERATIONAL


def has_reorder_comma(name: str) -> bool:
    """True when *name* is ``LAST, FIRST…``: one comma, not before a generational suffix."""
    parts = name.split(",")
    return len(parts) == 2 and bool(parts[1].strip()) and not _is_generational(parts[1])  # noqa: PLR2004


def has_role_annotation(name: str) -> bool:
    """True when *name* ends in a FRANCHISOR/FRANCHISEE annotation."""
    return bool(_ROLE_ANNOTATION.search(name))


def link_key(name: str, entity_type: str | None) -> str | None:
    """Return the format key for *name* of resolved *entity_type*, or None for other types.

    - person: ``LAST, FIRST [MIDDLE…]`` → ``FIRST [MIDDLE…] LAST`` (unless the
      tail is ``JR SR II III IV``), then ``,`` and ``.`` → space;
    - organization: strip a trailing role annotation, then ``,`` and ``.`` → space.

    The key never drops a reorder comma without reordering, so ``ZHAO QIAN``
    and ``ZHAO, QIAN`` never share a key.
    """
    if entity_type == "person":
        if has_reorder_comma(name):
            last, first = name.split(",")
            name = f"{first.strip()} {last.strip()}"
        return _squash(name) or None
    if entity_type == "organization":
        return _squash(_ROLE_ANNOTATION.sub("", name)) or None
    return None


# ── canonical election (spec §1) ─────────────────────────────────────


def _preference(kind: str, name: str) -> int:
    if kind == "person":
        return 1 if has_reorder_comma(name) else 0
    return 1 if has_role_annotation(name) else 0


def elect_canonical(
    kind: str,
    members: Sequence[Member],
    *,
    current: int | None = None,
    manual_targets: Collection[int] = frozenset(),
    manual_variants: Collection[int] = frozenset(),
) -> int | None:
    """Return the canonical id of a format-key group, or None if no member is eligible.

    A *manual_variants* member (the caller passes global manual variants) is
    never elected.  The *current* canonical sticks while it
    is still an eligible member.  Otherwise: a manual target first, then the
    natural-order (person) or annotation-free (organization) spelling, then
    the most links, then the lowest id.
    """
    eligible = [m for m in members if m.id not in manual_variants]
    if not eligible:
        return None
    if current is not None and any(m.id == current for m in eligible):
        return current
    best = min(
        eligible,
        key=lambda m: (m.id not in manual_targets, _preference(kind, m.name), -m.links, m.id),
    )
    return best.id


# ── resolution (spec §1) ─────────────────────────────────────────────


def _index(rows: Iterable[AliasRow]) -> dict[int, list[AliasRow]]:
    by_variant: dict[int, list[AliasRow]] = defaultdict(list)
    for r in rows:
        by_variant[r.variant].append(r)
    return by_variant


def _step(candidates: list[AliasRow], licence: str | None) -> AliasRow | None:
    scoped = [r for r in candidates if licence is not None and r.scope == licence]
    pool = scoped or [r for r in candidates if r.scope is None]
    if not pool:
        return None
    return min(pool, key=lambda r: r.source != "manual")


def _resolve_indexed(
    entity_id: int, licence: str | None, by_variant: dict[int, list[AliasRow]]
) -> int:
    x = entity_id
    for _ in range(MAX_STEPS):
        row = _step(by_variant.get(x, []), licence)
        if row is None:
            break
        x = row.canonical
    return x


def resolve(entity_id: int, licence: str | None, rows: Iterable[AliasRow]) -> int:
    """Return ``eff(entity_id, licence)``: follow scoped, else global rows; manual beats rule.

    *licence* None means "no licence context": only global rows apply.
    """
    return _resolve_indexed(entity_id, licence, _index(rows))


# ── cycles ───────────────────────────────────────────────────────────


_State = tuple[int, str | None]


def _find_cycle_indexed(
    variant: int, canonical: int, scope: str | None, by_variant: dict[int, list[AliasRow]]
) -> list[AliasRow] | None:
    """Breadth-first search over ``(entity, licence committed to)`` states.

    A global row keeps the state's licence; a scoped row is usable when the
    state has none yet (committing to it) or the same one.  So one search
    covers every licence without combining two different licences' rows.
    """
    if variant == canonical:
        return []
    start: _State = (canonical, scope)
    prev: dict[_State, tuple[_State, AliasRow] | None] = {start: None}
    frontier = [start]
    while frontier:
        nxt: list[_State] = []
        for node, lic in frontier:
            for r in by_variant.get(node, []):
                if r.scope is not None and lic is not None and r.scope != lic:
                    continue
                state: _State = (r.canonical, lic if r.scope is None else r.scope)
                if state in prev:
                    continue
                prev[state] = ((node, lic), r)
                if r.canonical == variant:
                    path: list[AliasRow] = []
                    cur: _State = state
                    while (step := prev[cur]) is not None:
                        cur, row = step
                        path.append(row)
                    return path[::-1]
                nxt.append(state)
        frontier = nxt
    return None


def find_cycle(
    variant: int, canonical: int, scope: str | None, rows: Iterable[AliasRow]
) -> list[AliasRow] | None:
    """Return the rows that would close a cycle if ``variant → canonical`` were added, else None.

    A self-link returns ``[]``.  A row applies on licence ``L`` when its scope
    is NULL or ``L``, so the closing path may use global rows plus the rows of
    one licence: *scope*'s own, or any single licence's for a new global row.
    Scoped rows on two different licences never combine.
    """
    return _find_cycle_indexed(variant, canonical, scope, _index(rows))


# ── database: loading and resolution ─────────────────────────────────

SYSTEM_FLAGGER = "system:entity-aliases"
TYPE_DISAGREE_REASON = "members disagree on type"
_CHUNK = 5000

_ea = entity_aliases
_ead = entity_alias_dismissals


def _alias_row(r) -> AliasRow:  # noqa: ANN001 — a Row from _ROW_COLS
    return AliasRow(r.variant_id, r.canonical_id, r.scope_license, r.source, r.rule)


_ROW_COLS = (_ea.c.variant_id, _ea.c.canonical_id, _ea.c.scope_license, _ea.c.source, _ea.c.rule)


def _chunks(ids: Sequence[int]) -> list[Sequence[int]]:
    return [ids[i : i + _CHUNK] for i in range(0, len(ids), _CHUNK)]


async def _rows_from(conn: AsyncConnection, variant_ids: Collection[int]) -> list[AliasRow]:
    out: list[AliasRow] = []
    for chunk in _chunks(sorted(variant_ids)):
        stmt = select(*_ROW_COLS).where(_ea.c.variant_id.in_(chunk))
        out += [_alias_row(r) for r in (await conn.execute(stmt)).fetchall()]
    return out


async def eff_many(
    conn: AsyncConnection, pairs: Iterable[tuple[int, str | None]]
) -> dict[tuple[int, str | None], int]:
    """Return ``eff(entity, licence)`` for each pair: one fetch per resolution step.

    An entity with no alias row resolves to itself.
    """
    pairs = list(dict.fromkeys(pairs))
    rows: list[AliasRow] = []
    seen: set[int] = set()
    frontier = {e for e, _ in pairs}
    for _ in range(MAX_STEPS):
        frontier -= seen
        if not frontier:
            break
        seen |= frontier
        fetched = await _rows_from(conn, frontier)
        rows += fetched
        frontier = {r.canonical for r in fetched}
    by_variant = _index(rows)
    return {(e, lic): _resolve_indexed(e, lic, by_variant) for e, lic in pairs}


# ── database: relink (rule rows) ─────────────────────────────────────


@dataclass
class RelinkResult:
    """What :func:`relink` did (or, with ``dry_run``, would do)."""

    groups: dict[str, int] = field(default_factory=dict)
    variants: dict[str, int] = field(default_factory=dict)
    kept: int = 0
    inserted: int = 0
    deleted: int = 0
    skipped_cycle: int = 0
    skipped_dismissed: int = 0
    keys_updated: int = 0

    @property
    def changed(self) -> int:
        """Rows inserted or deleted plus link keys rewritten; 0 on a no-op run."""
        return self.inserted + self.deleted + self.keys_updated


async def _expand(conn: AsyncConnection, ids: set[int]) -> dict[int, tuple[str, str, str | None]]:
    """Load the entities a targeted relink must look at: *ids*, their rule partners, key-mates.

    Returns ``{id: (name, entity_type, stored link_key)}``.
    """
    fk = (_ea.c.source == "rule") & (_ea.c.rule == RULE_FORMAT_KEY)
    touched = set(ids)
    for chunk in _chunks(sorted(ids)):
        for v, c in (
            await conn.execute(
                select(_ea.c.variant_id, _ea.c.canonical_id).where(
                    fk & or_(_ea.c.variant_id.in_(chunk), _ea.c.canonical_id.in_(chunk))
                )
            )
        ).fetchall():
            touched |= {v, c}
    # Partners of a canonical: every variant pointing at it.
    cans = sorted(touched)
    for chunk in _chunks(cans):
        touched |= set(
            (
                await conn.execute(
                    select(_ea.c.variant_id).where(fk & _ea.c.canonical_id.in_(chunk))
                )
            ).scalars()
        )
    cols = (entities.c.id, entities.c.name, entities.c.entity_type, entities.c.link_key)
    loaded: dict[int, tuple[str, str, str | None]] = {}
    for chunk in _chunks(sorted(touched)):
        for eid, name, etype, key in (
            await conn.execute(select(*cols).where(entities.c.id.in_(chunk)))
        ).fetchall():
            loaded[eid] = (name, etype, key)
    keys = {k for _, _, k in loaded.values() if k} | {
        k for name, etype, _ in loaded.values() if (k := link_key(name, etype))
    }
    for chunk in _chunks(sorted(keys)):
        for eid, name, etype, key in (
            await conn.execute(select(*cols).where(entities.c.link_key.in_(chunk)))
        ).fetchall():
            loaded[eid] = (name, etype, key)
    return loaded


async def _load_all(conn: AsyncConnection) -> dict[int, tuple[str, str, str | None]]:
    cols = (entities.c.id, entities.c.name, entities.c.entity_type, entities.c.link_key)
    return {
        eid: (name, etype, key)
        for eid, name, etype, key in (await conn.execute(select(*cols))).fetchall()
    }


async def _link_counts(conn: AsyncConnection, ids: Collection[int]) -> dict[int, int]:
    out: dict[int, int] = {}
    for chunk in _chunks(sorted(ids)):
        stmt = (
            select(record_entities.c.entity_id, func.count())
            .where(record_entities.c.entity_id.in_(chunk))
            .group_by(record_entities.c.entity_id)
        )
        out.update(dict((await conn.execute(stmt)).fetchall()))
    return out


async def _dismissed_pairs(conn: AsyncConnection) -> set[tuple[int, int]]:
    """Every dismissed pair ``(a, b)``, a < b, in any scope.

    A rule row is global, so a dismissal in any scope blocks it: a global
    row would otherwise link the pair on the dismissed licence too.
    """
    stmt = select(_ead.c.entity_id_a, _ead.c.entity_id_b)
    return {(a, b) for a, b in (await conn.execute(stmt)).fetchall()}


def _format_key_groups(
    loaded: dict[int, tuple[str, str, str | None]],
    tombstones: Collection[int],
    links: dict[int, int],
) -> dict[tuple[str, str], list[Member]]:
    """Group the loaded non-tombstone entities by ``(resolved type, link_key)``; 2+ members only."""
    groups: dict[tuple[str, str], list[Member]] = defaultdict(list)
    for eid, (name, etype, _) in loaded.items():
        if eid not in tombstones and (k := link_key(name, etype)) is not None:
            groups[(etype, k)].append(Member(eid, name, links.get(eid, 0)))
    return {g: ms for g, ms in groups.items() if len(ms) > 1}


@dataclass
class _Plan:
    """What the format-key planner needs besides the groups."""

    existing: dict[int, int]
    other_rows: list[AliasRow]
    dismissed: set[tuple[int, int]]
    # Format-key rows of entities this run did not load: not replanned, but a
    # loop can still run through them (only fetched when one could).
    outside: list[AliasRow] = field(default_factory=list)


def _plan_format_key(
    groups: dict[tuple[str, str], list[Member]], plan: _Plan, result: RelinkResult
) -> dict[int, int]:
    """Return the desired ``{variant: canonical}`` format-key rows for *groups*."""
    manual = [r for r in plan.other_rows if r.source == "manual"]
    manual_targets = {r.canonical for r in manual}
    # Only a *global* manual variant is barred (spec §1, read with its
    # Pendergraft worked case): a scoped one is still its group's party
    # everywhere else, and on its licence resolution follows the chain.
    manual_variants = {r.variant for r in manual if r.scope is None}
    by_variant = _index(plan.other_rows + plan.outside)  # grows as rows are accepted

    def manual_into(ids: set[int]) -> set[int]:
        return {r.variant for r in manual if r.canonical in ids and r.variant in ids}

    desired: dict[int, int] = {}
    for (kind, _key), members in sorted(groups.items()):
        ids = {m.id for m in members}
        currents = [plan.existing[i] for i in ids if plan.existing.get(i) in ids]
        current = max(set(currents), key=currents.count) if currents else None
        if current in manual_into(ids):
            # An operator pointed the current canonical at another member, in
            # some scope: re-elect, so that member (a manual target) can lead.
            current = None
        canon = elect_canonical(
            kind,
            members,
            current=current,
            manual_targets=manual_targets & ids,
            manual_variants=manual_variants & ids,
        )
        if canon is None:
            continue
        result.groups[kind] = result.groups.get(kind, 0) + 1
        for v in sorted(ids - {canon}):
            if (min(v, canon), max(v, canon)) in plan.dismissed:
                result.skipped_dismissed += 1
            elif _find_cycle_indexed(v, canon, None, by_variant) is not None:
                result.skipped_cycle += 1
            else:
                desired[v] = canon
                by_variant[v].append(AliasRow(v, canon, None, "rule", RULE_FORMAT_KEY))
                result.variants[kind] = result.variants.get(kind, 0) + 1
    return desired


_FK = (_ea.c.source == "rule") & (_ea.c.rule == RULE_FORMAT_KEY)


async def _existing_format_rows(conn: AsyncConnection, ids: Collection[int]) -> dict[int, int]:
    """``{variant: canonical}`` of the format-key rows touching *ids* at either end."""
    existing: dict[int, int] = {}
    for chunk in _chunks(sorted(ids)):
        stmt = select(_ea.c.variant_id, _ea.c.canonical_id).where(
            _FK & or_(_ea.c.variant_id.in_(chunk), _ea.c.canonical_id.in_(chunk))
        )
        existing.update(dict((await conn.execute(stmt)).fetchall()))
    return existing


async def _outside_format_rows(conn: AsyncConnection, loaded: Collection[int]) -> list[AliasRow]:
    """Format-key rows whose variant is not in *loaded*."""
    rows = (await conn.execute(select(*_ROW_COLS).where(_FK))).fetchall()
    return [_alias_row(r) for r in rows if r.variant_id not in loaded]


async def _write_relink(
    conn: AsyncConnection,
    key_updates: list[dict],
    stale: list[int],
    fresh: list[tuple[int, int]],
) -> None:
    if key_updates:
        stmt = (
            update(entities)
            .where(entities.c.id == bindparam("_id"))
            .values(link_key=bindparam("_key"))
        )
        for i in range(0, len(key_updates), _CHUNK):
            await conn.execute(stmt, key_updates[i : i + _CHUNK])
    for chunk in _chunks(stale):
        await conn.execute(delete(_ea).where(_FK & _ea.c.variant_id.in_(chunk)))
    if fresh:
        ins = pg_insert(_ea)
        stmt = ins.on_conflict_do_update(
            constraint="uq_entity_aliases_variant_scope_source",
            set_={"canonical_id": ins.excluded.canonical_id, "rule": ins.excluded.rule},
        )
        params = [
            {
                "variant_id": v,
                "canonical_id": c,
                "scope_license": None,
                "source": "rule",
                "rule": RULE_FORMAT_KEY,
            }
            for v, c in fresh
        ]
        for i in range(0, len(params), _CHUNK):
            await conn.execute(stmt, params[i : i + _CHUNK])


async def relink(
    conn: AsyncConnection, entity_ids: Iterable[int] | None = None, *, dry_run: bool = False
) -> RelinkResult:
    """Rebuild the derived ``rule='format_key'`` alias rows; return what changed.

    With *entity_ids*, only the groups those entities belong to now or
    belonged to before are rebuilt (ingest, a type change, a merge); with
    None, every group (``wslcb db relink-entities``).  ``entities.link_key``
    is refreshed for every entity looked at.  Rows are diffed, not rewritten,
    so a second run changes nothing.  Manual rows and dismissals are never
    touched.  Caller commits.
    """
    result = RelinkResult()
    ids = None if entity_ids is None else set(entity_ids)
    if ids is not None and not ids:
        return result
    loaded = await _load_all(conn) if ids is None else await _expand(conn, ids)

    key_updates = [
        {"_id": eid, "_key": k}
        for eid, (name, etype, stored) in loaded.items()
        if (k := link_key(name, etype)) != stored
    ]
    plan = _Plan(
        existing=await _existing_format_rows(conn, loaded),
        other_rows=[
            _alias_row(r) for r in (await conn.execute(select(*_ROW_COLS).where(~_FK))).fetchall()
        ],
        dismissed=await _dismissed_pairs(conn),
    )
    tombstones = await tombstone_ids(conn, None if ids is None else sorted(loaded))
    members = [m.id for ms in _format_key_groups(loaded, tombstones, {}).values() for m in ms]
    groups = _format_key_groups(loaded, tombstones, await _link_counts(conn, members))
    if ids is not None and {r.variant for r in plan.other_rows} & set(members):
        # A loop needs a non-format-key row out of a group member; only then
        # can it pass through format-key rows this targeted run did not load.
        plan.outside = await _outside_format_rows(conn, loaded)
    desired = _plan_format_key(groups, plan, result)

    stale = [v for v, c in plan.existing.items() if desired.get(v) != c]
    fresh = [(v, c) for v, c in desired.items() if plan.existing.get(v) != c]
    result.keys_updated = len(key_updates)
    result.deleted = len(stale)
    result.inserted = len(fresh)
    result.kept = len(desired) - len(fresh)
    if not dry_run:
        await _write_relink(conn, key_updates, stale, fresh)
    return result


# ── database: write API (manual rows, dismissals) ────────────────────


class AliasError(ValueError):
    """A manual alias or dismissal write that the layer refuses; nothing was written."""


async def _types(conn: AsyncConnection, ids: Collection[int]) -> dict[int, str]:
    stmt = select(entities.c.id, entities.c.entity_type).where(entities.c.id.in_(list(ids)))
    return dict((await conn.execute(stmt)).fetchall())


async def flag_type_disagreements(conn: AsyncConnection, entity_ids: Collection[int]) -> int:
    """Open a review flag on the canonical of each manual row whose two ends differ in type.

    Looks at manual rows touching *entity_ids*; skips a canonical that already
    has an open flag with that reason.  Returns the number of flags opened.
    """
    if not entity_ids:
        return 0
    ids = list(entity_ids)
    rows = (
        await conn.execute(
            select(_ea.c.variant_id, _ea.c.canonical_id).where(
                (_ea.c.source == "manual")
                & or_(_ea.c.variant_id.in_(ids), _ea.c.canonical_id.in_(ids))
            )
        )
    ).fetchall()
    if not rows:
        return 0
    types = await _types(conn, {e for r in rows for e in r})
    erf = entity_review_flags
    opened = 0
    for canon in sorted({c for v, c in rows if types.get(v) != types.get(c)}):
        already = (
            await conn.execute(
                select(erf.c.id).where(
                    (erf.c.entity_id == canon)
                    & (erf.c.reason == TYPE_DISAGREE_REASON)
                    & erf.c.resolved_at.is_(None)
                )
            )
        ).first()
        if already is None:
            await conn.execute(
                insert(erf).values(
                    entity_id=canon, flagged_by=SYSTEM_FLAGGER, reason=TYPE_DISAGREE_REASON
                )
            )
            opened += 1
    return opened


def _describe(path: list[AliasRow]) -> str:
    return " → ".join(
        [str(path[0].variant)]
        + [f"{r.canonical} ({r.source}, {r.scope or 'everywhere'})" for r in path]
    )


async def add_manual_alias(  # noqa: PLR0913
    conn: AsyncConnection,
    variant_id: int,
    canonical_id: int,
    *,
    scope_license: str | None = None,
    created_by: str,
    note: str | None = None,
) -> int:
    """Link *variant_id* to *canonical_id* as an operator decision; return the row id.

    Raises ``LookupError`` if either entity is missing, and :class:`AliasError`
    for a self-link, a tombstone at either end, an existing manual row for the
    variant in that scope, or a link that would close a cycle (the message
    names the rows that close it).  A link across resolved types is allowed
    and opens a review flag on the canonical.  The pair's rule groups are
    relinked in the same transaction.  Caller commits.
    """
    if variant_id == canonical_id:
        msg = "an entity cannot be linked to itself"
        raise AliasError(msg)
    types = await _types(conn, {variant_id, canonical_id})
    for eid in (variant_id, canonical_id):
        if eid not in types:
            msg = f"entity {eid} not found"
            raise LookupError(msg)
    tombs = await tombstone_ids(conn, [variant_id, canonical_id])
    if tombs:
        msg = f"entity {min(tombs)} is marked not-a-name; clear that first"
        raise AliasError(msg)
    same = (
        await conn.execute(
            select(_ea.c.canonical_id).where(
                (_ea.c.variant_id == variant_id)
                & (_ea.c.source == "manual")
                & (
                    _ea.c.scope_license.is_(None)
                    if scope_license is None
                    else _ea.c.scope_license == scope_license
                )
            )
        )
    ).scalar_one_or_none()
    if same is not None:
        msg = f"entity {variant_id} is already linked to {same} in this scope; unlink it first"
        raise AliasError(msg)
    # Format-key rows are left out: the relink below replans them in this
    # transaction, and the planner skips any rule row that would loop with a
    # manual one, so the operator's link wins (spec §1 Writes).
    rows = [_alias_row(r) for r in (await conn.execute(select(*_ROW_COLS).where(~_FK))).fetchall()]
    path = find_cycle(variant_id, canonical_id, scope_license, rows)
    if path is not None:
        msg = f"linking {variant_id} → {canonical_id} would close a cycle: {_describe(path)}"
        raise AliasError(msg)

    row_id = (
        await conn.execute(
            insert(_ea)
            .values(
                variant_id=variant_id,
                canonical_id=canonical_id,
                scope_license=scope_license,
                source="manual",
                rule=None,
                created_by=created_by,
                note=note,
            )
            .returning(_ea.c.id)
        )
    ).scalar_one()
    await flag_type_disagreements(conn, [variant_id])
    await relink(conn, [variant_id, canonical_id])
    logger.info(
        "Manual alias %d → %d (%s) by %s",
        variant_id,
        canonical_id,
        scope_license or "everywhere",
        created_by,
    )
    return row_id


async def remove_manual_alias(conn: AsyncConnection, alias_id: int) -> AliasRow:
    """Delete manual row *alias_id*, relink its pair, and return what was removed.

    Raises ``LookupError`` if no manual row has that id (rule rows are
    derived: ``relink`` owns them).
    """
    row = (
        await conn.execute(
            delete(_ea)
            .where((_ea.c.id == alias_id) & (_ea.c.source == "manual"))
            .returning(*_ROW_COLS)
        )
    ).first()
    if row is None:
        msg = f"manual alias {alias_id} not found"
        raise LookupError(msg)
    removed = _alias_row(row)
    await relink(conn, [removed.variant, removed.canonical])
    return removed


def _pair(a: int, b: int) -> tuple[int, int]:
    if a == b:
        msg = "an entity cannot be dismissed against itself"
        raise AliasError(msg)
    return (a, b) if a < b else (b, a)


async def dismiss_pair(  # noqa: PLR0913
    conn: AsyncConnection,
    entity_a: int,
    entity_b: int,
    *,
    scope_license: str | None = None,
    dismissed_by: str,
    note: str | None = None,
) -> bool:
    """Record that *entity_a* and *entity_b* are not one party; return False if already so.

    Stops every rule from linking the pair, then relinks their groups.
    Manual rows are not touched.  Caller commits.
    """
    a, b = _pair(entity_a, entity_b)
    result = await conn.execute(
        pg_insert(_ead)
        .values(
            entity_id_a=a,
            entity_id_b=b,
            scope_license=scope_license,
            dismissed_by=dismissed_by,
            note=note,
        )
        .on_conflict_do_nothing(constraint="uq_entity_alias_dismissals_pair_scope")
    )
    if result.rowcount:
        await relink(conn, [a, b])
    return bool(result.rowcount)


async def undismiss_pair(
    conn: AsyncConnection, entity_a: int, entity_b: int, *, scope_license: str | None = None
) -> bool:
    """Remove the pair's dismissal in *scope_license*, relink, and return whether one existed."""
    a, b = _pair(entity_a, entity_b)
    scope = (
        _ead.c.scope_license.is_(None)
        if scope_license is None
        else _ead.c.scope_license == scope_license
    )
    result = await conn.execute(
        delete(_ead).where((_ead.c.entity_id_a == a) & (_ead.c.entity_id_b == b) & scope)
    )
    if result.rowcount:
        await relink(conn, [a, b])
    return bool(result.rowcount)


async def in_manual_alias(conn: AsyncConnection, entity_id: int) -> bool:
    """True when *entity_id* is the variant or the canonical of a manual row."""
    stmt = select(_ea.c.id).where(
        (_ea.c.source == "manual")
        & or_(_ea.c.variant_id == entity_id, _ea.c.canonical_id == entity_id)
    )
    return (await conn.execute(stmt.limit(1))).first() is not None


# ── database: hooks for destructive entity paths ─────────────────────


async def repoint_alias_rows(conn: AsyncConnection, dirty_id: int, canon_id: int) -> set[int]:
    """Move *dirty_id*'s operator rows to *canon_id* before *dirty_id* is deleted.

    Manual rows are re-pointed; one that would become a self-link, or
    duplicate a manual row *canon_id* already has in that scope, is dropped.
    Dismissals are re-pointed (re-ordered a < b) or dropped the same way.
    *dirty_id*'s rule rows are left to cascade: they are derived.  Returns the
    ids whose groups the caller should relink once *dirty_id* is gone.
    """
    touched = {canon_id}
    rows = (
        await conn.execute(
            select(_ea.c.id, *_ROW_COLS).where(
                or_(_ea.c.variant_id == dirty_id, _ea.c.canonical_id == dirty_id)
            )
        )
    ).fetchall()
    for r in rows:
        touched |= {r.variant_id, r.canonical_id}
        if r.source != "manual":
            continue
        v = canon_id if r.variant_id == dirty_id else r.variant_id
        c = canon_id if r.canonical_id == dirty_id else r.canonical_id
        scope = (
            _ea.c.scope_license.is_(None)
            if r.scope_license is None
            else _ea.c.scope_license == r.scope_license
        )
        clash = (
            await conn.execute(
                select(_ea.c.id).where(
                    (_ea.c.variant_id == v)
                    & (_ea.c.source == "manual")
                    & scope
                    & (_ea.c.id != r.id)
                )
            )
        ).first()
        if (
            v == c
            or clash is not None
            or await _closes_cycle_after_merge(
                conn, r.id, v, c, r.scope_license, dirty_id, canon_id
            )
        ):
            # Dropping loses nothing: dirty *is* canon, so the rows left
            # already state the same equivalence.
            logger.info(
                "Merge %d → %d: dropped manual alias %d (%d → %d, %s)",
                dirty_id,
                canon_id,
                r.id,
                r.variant_id,
                r.canonical_id,
                r.scope_license or "everywhere",
            )
            await conn.execute(delete(_ea).where(_ea.c.id == r.id))
        else:
            await conn.execute(
                update(_ea).where(_ea.c.id == r.id).values(variant_id=v, canonical_id=c)
            )

    dis = (
        await conn.execute(
            select(_ead.c.id, _ead.c.entity_id_a, _ead.c.entity_id_b, _ead.c.scope_license).where(
                or_(_ead.c.entity_id_a == dirty_id, _ead.c.entity_id_b == dirty_id)
            )
        )
    ).fetchall()
    for rid, a, b, scope_license in dis:
        other = b if a == dirty_id else a
        touched.add(other)
        if other == canon_id:
            await conn.execute(delete(_ead).where(_ead.c.id == rid))
            continue
        lo, hi = sorted((other, canon_id))
        scope = (
            _ead.c.scope_license.is_(None)
            if scope_license is None
            else _ead.c.scope_license == scope_license
        )
        clash = (
            await conn.execute(
                select(_ead.c.id).where(
                    (_ead.c.entity_id_a == lo) & (_ead.c.entity_id_b == hi) & scope
                )
            )
        ).first()
        if clash is not None:
            await conn.execute(delete(_ead).where(_ead.c.id == rid))
        else:
            await conn.execute(
                update(_ead).where(_ead.c.id == rid).values(entity_id_a=lo, entity_id_b=hi)
            )
    touched.discard(dirty_id)
    return touched


async def _closes_cycle_after_merge(  # noqa: PLR0913
    conn: AsyncConnection,
    row_id: int,
    variant: int,
    canonical: int,
    scope: str | None,
    dirty_id: int,
    canon_id: int,
) -> bool:
    """True when re-pointed row ``variant → canonical`` would close a cycle once dirty is canon."""

    def _m(e: int) -> int:
        return canon_id if e == dirty_id else e

    rows = (await conn.execute(select(_ea.c.id, *_ROW_COLS))).fetchall()
    graph = [
        AliasRow(_m(r.variant_id), _m(r.canonical_id), r.scope_license, r.source, r.rule)
        for r in rows
        if r.id != row_id
        and not (r.source == "rule" and dirty_id in (r.variant_id, r.canonical_id))
        and _m(r.variant_id) != _m(r.canonical_id)
    ]
    return find_cycle(variant, canonical, scope, graph) is not None


async def key_mates(conn: AsyncConnection, entity_ids: Collection[int]) -> set[int]:
    """Entities sharing a stored ``link_key`` with any of *entity_ids*, those excluded.

    Call before deleting *entity_ids*; relink the result afterwards so their
    groups re-elect.
    """
    ids = list(entity_ids)
    if not ids:
        return set()
    keys = select(entities.c.link_key).where(
        entities.c.id.in_(ids) & entities.c.link_key.is_not(None)
    )
    stmt = select(entities.c.id).where(entities.c.link_key.in_(keys) & ~entities.c.id.in_(ids))
    return set((await conn.execute(stmt)).scalars())
