"""Couple names: operator verdicts that split one printed name into its people (#269).

Design: ``docs/specs/2026-11-24-246-couple-name-splitting-design.md`` (#246).
An operator decides, per combined entity, that its name stands for two or
more people (verdict ``split``, with their names) or for one (``keep``).
Extraction (``entities.parse_and_link_entities``) then links each named
person to every record that prints the combined string, as a *member row*
(``record_entities.via_entity_id``).  Verdicts are operator data; member rows
are derived.

Contains the pure prefill (§3: shape, truncation completion, fuller form)
and, below it, the verdict reads and the audited writes.
"""

import logging
import re
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from .entity_aliases import (
    COUPLE_TOKENS,
    VERDICT_SPLIT,
    eff_many,
    link_key,
    middles_compatible,
    parse_person_key,
)
from .models import entities, entity_split_members, entity_splits

logger = logging.getLogger(__name__)

VERDICT_KEEP = "keep"

# WSLCB cuts a name at 30 characters (entities._TRUNCATION_MIN_LEN's comment).
# A raw part this close to the cut may have lost its tail; a longer one was
# not cut there (GARTH GEHLEN AND NATHAN DOIDGE PER WAC 314-55 is 45).
_TRUNCATION_MIN = 28
_TRUNCATION_CUT = 30
_TOKEN_SPLIT = re.compile(r"[\s,.;()]+")


# ── pure: shape and prefill (spec §3) ────────────────────────────────


def has_couple_token(name: str) -> bool:
    """True when *name* has a standalone ``AND`` or ``&`` token (a glued ``B&P`` is not one)."""
    return bool(COUPLE_TOKENS & set(name.split()))


def truncation_suspect(raw_part: str) -> bool:
    """True when the raw printed part is long enough to have been cut at 30 characters.

    Judged on the raw part, before ``strip_wac_annotation``.
    """
    return _TRUNCATION_MIN <= len(raw_part.strip()) <= _TRUNCATION_CUT


def shape_split(name: str) -> list[str] | None:
    """Split a couple name by its shape, or None when the shape is not one of the two.

    - ``F (AND|&) G L`` → ``F L``, ``G L`` (the surname is shared);
    - ``F [M] L (AND|&) G [N] K`` → the two halves as printed.

    One conjunction only, with names on both sides; two bare tokens
    (``LOPEZ & LOPEZ``) get nothing.
    """
    toks = name.split()
    conj = [i for i, t in enumerate(toks) if t in COUPLE_TOKENS]
    if len(conj) != 1:
        return None
    left, right = toks[: conj[0]], toks[conj[0] + 1 :]
    if len(left) == 1 and len(right) == 2:  # noqa: PLR2004
        return [f"{left[0]} {right[-1]}", " ".join(right)]
    if 2 <= len(left) <= 3 and 2 <= len(right) <= 3:  # noqa: PLR2004
        return [" ".join(left), " ".join(right)]
    return None


def _complete_last_token(parts: list[str], record_names: Iterable[str]) -> list[str]:
    """Complete the last part's last token if it starts exactly one longer printed token."""
    *head, last = parts
    toks = last.split()
    stub = toks[-1]
    printed = {t for n in record_names for t in _TOKEN_SPLIT.split(n.upper()) if t}
    longer = {t for t in printed if len(t) > len(stub) and t.startswith(stub)}
    if len(longer) != 1:
        return parts
    return [*head, " ".join([*toks[:-1], longer.pop()])]


def _fuller_form(part: str, persons: Iterable[str]) -> str:
    """The unique longest printed person name middle-compatible with *part*, else *part*.

    Compatible: the same first and last tokens and suffix, and middles as in
    #265 (``entity_aliases.middles_compatible``).  Nicknames are not matched.
    """
    want = parse_person_key(link_key(part, "person"))
    if want is None:
        return part
    ranked: list[tuple[tuple[int, int], str]] = []
    for name in dict.fromkeys(persons):
        key = link_key(name, "person")
        got = parse_person_key(key)
        if (
            got is not None
            and (got.first, got.last, got.suffix) == (want.first, want.last, want.suffix)
            and middles_compatible(got.middles, want.middles)
        ):
            ranked.append(((len(got.middles), len(key or "")), name))
    if not ranked:
        return part
    ranked.sort(reverse=True)
    if len(ranked) > 1 and ranked[0][0] == ranked[1][0]:
        return part
    return ranked[0][1]


@dataclass(frozen=True)
class Prefill:
    """The split panel's suggestion: member names (None: no suggestion) and the badge."""

    members: list[str] | None
    truncated: bool


def suggest_split(
    name: str,
    *,
    raw_part: str,
    record_names: Sequence[str],
    persons: Sequence[str],
) -> Prefill:
    """Suggest the people a couple name stands for (spec #246 §3); a suggestion only.

    *raw_part* is the printed part the name was cleaned from (the longest,
    when it is printed several ways); *record_names* every name printed on the
    combined entity's records (step 2 reads their tokens); *persons* the
    persons printed in the same role on those records (step 3).

    1. Shape (:func:`shape_split`).
    2. With the truncation badge, complete the last token from *record_names*.
    3. Replace each part by its unique longest middle-compatible form in *persons*.
    """
    truncated = truncation_suspect(raw_part)
    parts = shape_split(name)
    if parts is None:
        return Prefill(None, truncated)
    if truncated:
        parts = _complete_last_token(parts, record_names)
    return Prefill([_fuller_form(p, persons) for p in parts], truncated)


# ── database: verdict reads ──────────────────────────────────────────


async def split_members(
    conn: AsyncConnection, combined_ids: Collection[int] | None = None
) -> dict[int, tuple[str, ...]]:
    """``{combined_id: member names in order}`` per ``split`` verdict (of *combined_ids*)."""
    s, m = entity_splits, entity_split_members
    stmt = (
        select(m.c.combined_id, m.c.member_name)
        .join(s, s.c.combined_id == m.c.combined_id)
        .where(s.c.verdict == VERDICT_SPLIT)
        .order_by(m.c.combined_id, m.c.ordinal)
    )
    if combined_ids is not None:
        if not combined_ids:
            return {}
        stmt = stmt.where(m.c.combined_id.in_(list(combined_ids)))
    out: dict[int, list[str]] = {}
    for cid, name in (await conn.execute(stmt)).fetchall():
        out.setdefault(cid, []).append(name)
    return {cid: tuple(names) for cid, names in out.items()}


async def member_ids(conn: AsyncConnection, names: Collection[str]) -> dict[str, int]:
    """``{member name: entity id}`` for the names that exist (names are stored cleaned)."""
    if not names:
        return {}
    stmt = select(entities.c.name, entities.c.id).where(
        func.lower(entities.c.name).in_(sorted({n.lower() for n in names}))
    )
    found = {name.lower(): eid for name, eid in (await conn.execute(stmt)).fetchall()}
    return {n: found[n.lower()] for n in names if n.lower() in found}


async def split_banner(conn: AsyncConnection, entity_id: int) -> list[dict] | None:
    """The people a ``split`` entity names, as ``{"id", "name"}`` in order; None without one.

    ``id`` is the member's party everywhere (``eff``), or None for a member no
    record has linked yet.
    """
    members = (await split_members(conn, [entity_id])).get(entity_id)
    if not members:
        return None
    ids = await member_ids(conn, members)
    parties = await eff_many(conn, [(i, None) for i in ids.values()])
    return [{"id": parties[(ids[m], None)] if m in ids else None, "name": m} for m in members]
