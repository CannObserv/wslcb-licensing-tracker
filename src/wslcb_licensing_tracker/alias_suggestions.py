"""Link suggestions for the review queue's Links tab (#264; spec §2 and §4).

Pairs the format key deliberately does not link, computed when the tab loads
and never stored as links.  Each is a variant → canonical pair with a scope:

- ``comma_drop`` (everywhere): a ``LAST, FIRST`` person and the comma-dropped
  spelling ``LAST FIRST``; a *trap* when ``FIRST LAST`` also exists.
- ``legal_designator`` (everywhere): organizations whose keys differ only in
  trailing designators (``LLC``, ``INC``, …).
- ``single_token`` (that licence): a one-token person and the one other
  person party on a licence whose spelling has that token.
- ``middle_names`` (that licence, #265): each pair, shorter → longer, of a
  group the middle-name rule left alone because its middles conflict.

A pair already one party in that scope, a pair with a dismissal in any scope,
and a pair with a not-a-name end are left out, so linking or dismissing a
suggestion removes it.
"""

import logging
import re
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from .entity_aliases import (
    Party,
    conflict_pairs,
    eff_many,
    has_reorder_comma,
    parse_person_key,
    plan_licence,
)
from .entity_classification import tombstone_ids
from .models import entities, entity_alias_dismissals

logger = logging.getLogger(__name__)

_DESIGNATOR = (
    r"(?:L L C|LLC|L L P|LLP|INCORPORATED|INC|CORPORATION|CORP|COMPANY|CO"
    r"|LIMITED|LTD|LP|PLLC|P S|PS|P C|PC)"
)
_TRAILING_DESIGNATORS = re.compile(rf"(?:\s+{_DESIGNATOR})+$")
KIND_ORDER = ("comma_drop", "legal_designator", "single_token", "middle_names")


@dataclass(frozen=True)
class _Pair:
    kind: str
    label: str
    variant: int
    canonical: int
    scope: str | None = None


def designator_base(key: str) -> str:
    """*key* without its trailing legal designators; *key* itself if nothing else is left."""
    return _TRAILING_DESIGNATORS.sub("", key) or key


async def _global_parties(conn: AsyncConnection, ids: set[int]) -> dict[int, int]:
    resolved = await eff_many(conn, [(e, None) for e in ids])
    return {e: resolved[(e, None)] for e in ids}


# Each comma-form person joined to the comma-free persons keyed as its name
# with the punctuation dropped and no reorder (``_squash``), in SQL: pulling the
# 12.5k comma names into Python and back as an IN list took 700 ms on prod.
_COMMA_DROP_SQL = text(
    """
    WITH c AS (
        SELECT id, name, link_key,
               btrim(regexp_replace(regexp_replace(name, '[,.]', ' ', 'g'), '\\s+', ' ', 'g'))
                   AS dropped_key
        FROM entities
        WHERE entity_type = 'person' AND name LIKE '%,%' AND link_key IS NOT NULL
    )
    SELECT c.id AS comma_id, c.name AS comma_name, d.id AS dropped_id,
           (SELECT count(*) FROM entities k
             WHERE k.entity_type = 'person' AND k.link_key = c.link_key) AS key_size,
           (SELECT o.name FROM entities o
             WHERE o.entity_type = 'person' AND o.link_key = d.link_key AND o.name LIKE '%,%'
             ORDER BY o.id LIMIT 1) AS other_comma
    FROM c JOIN entities d
      ON d.entity_type = 'person' AND d.link_key = c.dropped_key AND d.name NOT LIKE '%,%'
    ORDER BY c.id, d.id
    """
)


async def _comma_drop(conn: AsyncConnection) -> list[_Pair]:
    # The scalar subqueries' estimate trips JIT, whose compile cost is most of
    # the query (85 ms with it, 47 ms without, prod copy 2026-11-26).  LOCAL:
    # this transaction only; the review page never commits.
    await conn.execute(text("SET LOCAL jit = off"))
    rows = [
        r
        for r in (await conn.execute(_COMMA_DROP_SQL)).fetchall()
        if has_reorder_comma(r.comma_name)
    ]
    if not rows:
        return []
    party = await _global_parties(conn, {e for r in rows for e in (r.comma_id, r.dropped_id)})
    pairs = []
    for r in rows:
        if party[r.dropped_id] == party[r.comma_id]:
            continue
        if r.other_comma:
            label = f"also the natural order of {r.other_comma}"
        elif r.key_size > 1:
            label = "comma-drop trap"
        else:
            label = "comma-dropped only"
        pairs.append(_Pair("comma_drop", label, party[r.dropped_id], party[r.comma_id]))
    return pairs


async def _record_counts(conn: AsyncConnection, party_ids: set[int]) -> dict[int, int]:
    rows = await conn.execute(
        text(
            "SELECT party_id, COUNT(DISTINCT record_id) FROM record_entities"
            " WHERE party_id = ANY(:ids) GROUP BY party_id"
        ),
        {"ids": list(party_ids)},
    )
    return dict(rows.fetchall())


async def _legal_designator(conn: AsyncConnection) -> list[_Pair]:
    orgs = (
        await conn.execute(
            select(entities.c.id, entities.c.link_key).where(
                (entities.c.entity_type == "organization") & entities.c.link_key.is_not(None)
            )
        )
    ).fetchall()
    party = await _global_parties(conn, {r.id for r in orgs})
    by_base: dict[str, set[int]] = defaultdict(set)
    for r in orgs:
        by_base[designator_base(r.link_key)].add(party[r.id])
    groups = [ps for ps in by_base.values() if len(ps) > 1]
    if not groups:
        return []
    in_groups = {p for ps in groups for p in ps}
    keys = dict(
        (
            await conn.execute(
                select(entities.c.id, entities.c.link_key).where(entities.c.id.in_(list(in_groups)))
            )
        ).fetchall()
    )
    counts = await _record_counts(conn, in_groups)

    def preference(p: int) -> tuple[bool, int, int]:
        key = keys.get(p) or ""
        return (designator_base(key) == key, -counts.get(p, 0), p)

    pairs = []
    for ps in groups:
        target, *rest = sorted(ps, key=preference)
        pairs.extend(_Pair("legal_designator", "legal designator", p, target) for p in rest)
    return pairs


async def _single_token(conn: AsyncConnection) -> list[_Pair]:
    singles = dict(
        (
            await conn.execute(
                select(entities.c.id, entities.c.link_key).where(
                    (entities.c.entity_type == "person")
                    & entities.c.link_key.is_not(None)
                    & ~entities.c.link_key.contains(" ")
                )
            )
        ).fetchall()
    )
    if not singles:
        return []
    own = (
        await conn.execute(
            text(
                "SELECT DISTINCT re.entity_id, re.party_id, lr.license_number"
                " FROM record_entities re JOIN license_records lr ON lr.id = re.record_id"
                " WHERE re.entity_id = ANY(:ids)"
            ),
            {"ids": list(singles)},
        )
    ).fetchall()
    licences = sorted({r.license_number for r in own})
    on_licence: dict[str, set[tuple[int, int, str]]] = defaultdict(set)
    for r in (
        await conn.execute(
            text(
                "SELECT DISTINCT lr.license_number, re.entity_id, re.party_id, e.link_key"
                " FROM record_entities re JOIN license_records lr ON lr.id = re.record_id"
                " JOIN entities e ON e.id = re.entity_id"
                " WHERE lr.license_number = ANY(:lics) AND e.entity_type = 'person'"
                " AND e.link_key IS NOT NULL"
            ),
            {"lics": licences},
        )
    ).fetchall():
        on_licence[r.license_number].add((r.entity_id, r.party_id, r.link_key))

    found: list[tuple[int, str, int, list[int]]] = []
    for single, own_party, lic in own:
        if own_party != single:
            continue  # already linked on this licence
        token = singles[single]
        matches = [(e, p) for e, p, key in on_licence[lic] if p != single and token in key.split()]
        parties = {p for _, p in matches}
        if len(parties) == 1:
            [p] = parties
            found.append((single, lic, p, sorted({e for e, _ in matches})))
    if not found:
        return []
    party = await _global_parties(conn, {e for *_, ms in found for e in ms})
    pairs = []
    for single, lic, p, members in found:
        # The party itself if its own spelling has the token; else the
        # matching spelling that is not a global variant, lowest id.
        target = p if p in members else min(members, key=lambda e: (party[e] != e, e))
        pairs.append(_Pair("single_token", "single-token name", single, target, lic))
    return pairs


# Each licence's person parties with a two-token or longer key.  The stored
# party_id is the rule's own input for a conflicting group: the rule wrote
# no row there, and a compatible group folds into its canonical.
_MIDDLE_SQL = text(
    """
    SELECT DISTINCT lr.license_number, re.party_id, re.role, p.name, p.link_key
    FROM record_entities re
    JOIN license_records lr ON lr.id = re.record_id
    JOIN entities p ON p.id = re.party_id
    WHERE p.entity_type = 'person' AND p.link_key LIKE '% %'
    """
)


def _middles_label(a: str | None, b: str | None) -> str:
    def mids(key: str | None) -> str:
        parsed = parse_person_key(key)
        return " ".join(parsed.middles) if parsed and parsed.middles else "none"

    return f"middle names: {mids(a)} / {mids(b)}"


async def _middle_names(conn: AsyncConnection) -> list[_Pair]:
    by_licence: dict[str, dict[int, tuple[str, str, set[str]]]] = defaultdict(dict)
    for lic, pid, role, name, key in (await conn.execute(_MIDDLE_SQL)).fetchall():
        by_licence[lic].setdefault(pid, (name, key, set()))[2].add(role)
    tombs = await tombstone_ids(conn)
    groups: list[tuple[str, list[Party]]] = []
    for lic, members in sorted(by_licence.items()):
        if len(members) < 2:  # noqa: PLR2004
            continue
        parties = [
            Party(pid, name, key, frozenset(roles))
            for pid, (name, key, roles) in members.items()
            if pid not in tombs
        ]
        groups += [(lic, g) for g in plan_licence(parties).conflicts]
    if not groups:
        return []
    counts = await _record_counts(conn, {p.id for _, g in groups for p in g})
    keys = {p.id: p.key for _, g in groups for p in g}
    pairs = []
    for lic, group in groups:
        ranked = [Party(p.id, p.name, p.key, p.roles, counts.get(p.id, 0)) for p in group]
        pairs += [
            _Pair("middle_names", _middles_label(keys[v], keys[c]), v, c, lic)
            for v, c in conflict_pairs(ranked)
        ]
    return pairs


GENERATORS: list[Callable[[AsyncConnection], Awaitable[list[_Pair]]]] = [
    _comma_drop,
    _legal_designator,
    _single_token,
    _middle_names,
]


async def _evidence(
    conn: AsyncConnection, pairs: list[_Pair]
) -> dict[tuple[int, int], tuple[list[str], int]]:
    """Licences and records the two ends' parties share, per pair."""
    ids = {e for pr in pairs for e in (pr.variant, pr.canonical)}
    licences: dict[int, set[str]] = defaultdict(set)
    records: dict[int, set[int]] = defaultdict(set)
    for pid, lic, rid in (
        await conn.execute(
            text(
                "SELECT re.party_id, lr.license_number, re.record_id FROM record_entities re"
                " JOIN license_records lr ON lr.id = re.record_id WHERE re.party_id = ANY(:ids)"
            ),
            {"ids": list(ids)},
        )
    ).fetchall():
        licences[pid].add(lic)
        records[pid].add(rid)
    return {
        (pr.variant, pr.canonical): (
            sorted(licences[pr.variant] & licences[pr.canonical]),
            len(records[pr.variant] & records[pr.canonical]),
        )
        for pr in pairs
    }


async def suggestions(conn: AsyncConnection) -> list[dict]:
    """Every current suggestion, by kind then variant name.

    Each is ``{kind, label, variant_id, variant_name, canonical_id,
    canonical_name, scope_license, shared_licences, shared_records}``.
    """
    pairs: list[_Pair] = []
    seen: set[tuple[int, int, str | None]] = set()
    for generate in GENERATORS:
        for pr in await generate(conn):
            if (pr.variant, pr.canonical, pr.scope) not in seen:
                seen.add((pr.variant, pr.canonical, pr.scope))
                pairs.append(pr)
    if not pairs:
        return []
    ead = entity_alias_dismissals
    dismissed = {
        tuple(r) for r in (await conn.execute(select(ead.c.entity_id_a, ead.c.entity_id_b)))
    }
    ids = {e for pr in pairs for e in (pr.variant, pr.canonical)}
    tombs = await tombstone_ids(conn, list(ids))
    pairs = [
        pr
        for pr in pairs
        if tuple(sorted((pr.variant, pr.canonical))) not in dismissed
        and not {pr.variant, pr.canonical} & tombs
    ]
    if not pairs:
        return []
    names = dict(
        (
            await conn.execute(
                select(entities.c.id, entities.c.name).where(entities.c.id.in_(list(ids)))
            )
        ).fetchall()
    )
    evidence = await _evidence(conn, pairs)
    out = [
        {
            "kind": pr.kind,
            "label": pr.label,
            "variant_id": pr.variant,
            "variant_name": names[pr.variant],
            "canonical_id": pr.canonical,
            "canonical_name": names[pr.canonical],
            "scope_license": pr.scope,
            "shared_licences": evidence[(pr.variant, pr.canonical)][0],
            "shared_records": evidence[(pr.variant, pr.canonical)][1],
        }
        for pr in pairs
    ]
    return sorted(
        out, key=lambda s: (KIND_ORDER.index(s["kind"]), s["variant_name"], s["variant_id"])
    )
