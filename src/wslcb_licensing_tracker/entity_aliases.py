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

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Sequence

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

    A manual variant is never elected.  The *current* canonical sticks while it
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


def _path(start: int, goal: int, by_variant: dict[int, list[AliasRow]]) -> list[AliasRow] | None:
    """Breadth-first path of rows from *start* to *goal* over *by_variant*, or None."""
    prev: dict[int, AliasRow | None] = {start: None}
    frontier = [start]
    while frontier:
        nxt: list[int] = []
        for x in frontier:
            for r in by_variant.get(x, []):
                if r.canonical in prev:
                    continue
                prev[r.canonical] = r
                if r.canonical == goal:
                    path: list[AliasRow] = []
                    node = goal
                    while (row := prev[node]) is not None:
                        path.append(row)
                        node = row.variant
                    return path[::-1]
                nxt.append(r.canonical)
        frontier = nxt
    return None


def find_cycle(
    variant: int, canonical: int, scope: str | None, rows: Iterable[AliasRow]
) -> list[AliasRow] | None:
    """Return the rows that would close a cycle if ``variant → canonical`` were added, else None.

    A self-link returns ``[]``.  A row applies on licence ``L`` when its scope
    is NULL or ``L``, so a new global row is checked against the global rows
    alone and against each licence's rows plus the global ones; a new scoped
    row against its own licence's rows plus the global ones.  Scoped rows on
    two different licences never combine.
    """
    if variant == canonical:
        return []
    rows = list(rows)
    scopes: list[str | None] = (
        [None, *sorted({r.scope for r in rows if r.scope is not None})]
        if scope is None
        else [scope]
    )
    for s in scopes:
        usable = [r for r in rows if r.scope is None or (s is not None and r.scope == s)]
        found = _path(canonical, variant, _index(usable))
        if found is not None:
            return found
    return None


@dataclass
class _Group:
    """A format-key group while :func:`relink` plans it."""

    kind: str
    key: str
    members: list[Member] = field(default_factory=list)
