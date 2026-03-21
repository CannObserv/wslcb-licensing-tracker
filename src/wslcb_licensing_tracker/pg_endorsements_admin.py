"""Async PostgreSQL endorsement admin helpers for WSLCB licensing tracker.

Async equivalent of endorsements_admin.py. Ports all public functions to
async SQLAlchemy Core. Uses table objects from models.py.

Functions:
- ``endorsement_similarity`` — pure Python, not async
- ``get_endorsement_list``
- ``suggest_duplicate_endorsements``
- ``dismiss_suggestion``
- ``get_code_mappings``
- ``add_code_mapping``
- ``remove_code_mapping``
- ``create_code``

No dependency on admin_audit — caller logs.
"""

import logging
import re
from itertools import combinations

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import (
    endorsement_aliases,
    endorsement_codes,
    endorsement_dismissed_suggestions,
    license_endorsements,
    record_endorsements,
)
from .pg_endorsements import ensure_endorsement

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Similarity algorithm (pure Python — not async)
# ---------------------------------------------------------------------------

_SIM_STOP_WORDS: frozenset[str] = frozenset(
    {
        "ENDORSEMENT",
        "THE",
        "AND",
        "OF",
        "FOR",
        "A",
    }
)

_SIM_SPELLING: list[tuple[str, str]] = [
    ("TAKE OUT", "TAKEOUT"),
]

_SIM_THRESHOLD: float = 0.70


def _sim_normalize(name: str) -> str:
    """Apply phrase-level spelling normalisations before tokenising."""
    upper = name.upper()
    for old, new in _SIM_SPELLING:
        upper = upper.replace(old, new)
    return upper


def _sim_tokenize(name: str) -> list[str]:
    """Split *name* into uppercase tokens, strip punctuation & stop-words."""
    normalized = _sim_normalize(name)
    sentinel = re.sub(r"\+", " PLUS ", normalized)
    sentinel = re.sub(r"(?<![a-zA-Z0-9])\-(?![a-zA-Z0-9])", " MINUS ", sentinel)
    cleaned = re.sub(r"[<>,.;:!?/\\()[\]{}\"'`@#$%^&*=|~]", " ", sentinel)
    return [t for t in cleaned.split() if t and t not in _SIM_STOP_WORDS]


def _sim_features(tokens: list[str]) -> frozenset[str]:
    """Return unigrams + adjacent bigrams for *tokens*."""
    unigrams: list[str] = tokens
    bigrams: list[str] = [f"{tokens[i]} {tokens[i + 1]}" for i in range(len(tokens) - 1)]
    return frozenset(unigrams + bigrams)


_SIM_POLAR_TOKENS: frozenset[str] = frozenset({"PLUS", "MINUS"})


def endorsement_similarity(name_a: str, name_b: str) -> float:
    """Blended similarity between two endorsement name strings.

    Score = 0.70 x Jaccard(token+bigram features) + 0.30 x containment.

    Containment = max(|A∩B|/|A|, |A∩B|/|B|) on unigram sets, which catches
    cases where one name is a proper subset of the other.

    Hard rule: if the symmetric difference of the unigram sets is exactly
    ``{PLUS, MINUS}`` (i.e. the two names are identical except that one has
    ``+`` and the other has ``-``), return 0.0.  These are semantically
    distinct licence categories, not spelling variants.

    Returns a float in [0.0, 1.0].
    """
    tok_a = _sim_tokenize(name_a)
    tok_b = _sim_tokenize(name_b)
    if not tok_a or not tok_b:
        return 0.0

    uni_a = frozenset(tok_a)
    uni_b = frozenset(tok_b)

    # Hard exclusion: sole difference is + vs - polarity token.
    if (uni_a | uni_b) - (uni_a & uni_b) == _SIM_POLAR_TOKENS:
        return 0.0

    feat_a = _sim_features(tok_a)
    feat_b = _sim_features(tok_b)

    intersection = feat_a & feat_b
    union = feat_a | feat_b
    jaccard = len(intersection) / len(union) if union else 0.0

    uni_inter = uni_a & uni_b
    containment = max(
        len(uni_inter) / len(uni_a) if uni_a else 0.0,
        len(uni_inter) / len(uni_b) if uni_b else 0.0,
    )

    return 0.70 * jaccard + 0.30 * containment


# ---------------------------------------------------------------------------
# Endorsement list
# ---------------------------------------------------------------------------


async def get_endorsement_list(conn: AsyncConnection) -> list[dict]:
    """Return all endorsements as a flat list for the revised admin UI.

    Each dict has:
    - ``id``, ``name``, ``record_count``
    - ``is_canonical`` (True if at least one alias points to this endorsement)
    - ``is_variant`` (True if this endorsement has an alias pointing elsewhere)
    - ``canonical_id``, ``canonical_name`` (None if not a variant)
    - ``codes`` — list of numeric code strings this endorsement is mapped to
    """
    stmt = (
        select(
            license_endorsements.c.id,
            license_endorsements.c.name,
            func.count(record_endorsements.c.record_id).label("record_count"),
        )
        .select_from(license_endorsements)
        .outerjoin(
            record_endorsements,
            record_endorsements.c.endorsement_id == license_endorsements.c.id,
        )
        .group_by(license_endorsements.c.id, license_endorsements.c.name)
        .order_by(func.lower(license_endorsements.c.name))
    )
    rows = (await conn.execute(stmt)).fetchall()

    alias_rows = (
        await conn.execute(
            select(
                endorsement_aliases.c.endorsement_id,
                endorsement_aliases.c.canonical_endorsement_id,
            )
        )
    ).fetchall()
    alias_map: dict[int, int] = {r[0]: r[1] for r in alias_rows}
    canonical_ids: set[int] = set(alias_map.values())

    name_by_id: dict[int, str] = {r[0]: r[1] for r in rows}

    code_rows = (
        await conn.execute(select(endorsement_codes.c.endorsement_id, endorsement_codes.c.code))
    ).fetchall()
    eid_to_codes: dict[int, list[str]] = {}
    for eid, code in code_rows:
        eid_to_codes.setdefault(eid, []).append(code)

    result = []
    for eid, name, count in rows:
        canonical_id = alias_map.get(eid)
        result.append(
            {
                "id": eid,
                "name": name,
                "record_count": count,
                "is_canonical": eid in canonical_ids,
                "is_variant": eid in alias_map,
                "canonical_id": canonical_id,
                "canonical_name": name_by_id.get(canonical_id) if canonical_id else None,
                "codes": sorted(
                    eid_to_codes.get(eid, []),
                    key=lambda c: (0, int(c)) if c.isdigit() else (1, c),
                ),
            }
        )
    return result


# ---------------------------------------------------------------------------
# Duplicate suggestion helpers
# ---------------------------------------------------------------------------


async def suggest_duplicate_endorsements(
    conn: AsyncConnection,
    threshold: float = _SIM_THRESHOLD,
) -> list[dict]:
    """Return suggested duplicate endorsement pairs above *threshold*.

    Pairs that are already aliased or have been dismissed are excluded.
    Each dict has:
    - ``id_a``, ``name_a``, ``count_a``
    - ``id_b``, ``name_b``, ``count_b``
    - ``score`` (float)

    Ordered by score descending.
    """
    # Fetch all non-variant endorsements (variants already resolved)
    variant_subq = select(endorsement_aliases.c.endorsement_id).scalar_subquery()
    stmt = (
        select(
            license_endorsements.c.id,
            license_endorsements.c.name,
            func.count(record_endorsements.c.record_id).label("cnt"),
        )
        .select_from(license_endorsements)
        .outerjoin(
            record_endorsements,
            record_endorsements.c.endorsement_id == license_endorsements.c.id,
        )
        .where(license_endorsements.c.id.not_in(variant_subq))
        .group_by(license_endorsements.c.id, license_endorsements.c.name)
    )
    rows = (await conn.execute(stmt)).fetchall()

    alias_rows = (
        await conn.execute(
            select(
                endorsement_aliases.c.endorsement_id,
                endorsement_aliases.c.canonical_endorsement_id,
            )
        )
    ).fetchall()
    aliased_pairs: set[tuple[int, int]] = set()
    for a, b in alias_rows:
        aliased_pairs.add((min(a, b), max(a, b)))

    dismissed_rows = (
        await conn.execute(
            select(
                endorsement_dismissed_suggestions.c.endorsement_id_a,
                endorsement_dismissed_suggestions.c.endorsement_id_b,
            )
        )
    ).fetchall()
    dismissed_pairs: set[tuple[int, int]] = {(r[0], r[1]) for r in dismissed_rows}

    suggestions: list[dict] = []
    for (id_a, name_a, cnt_a), (id_b, name_b, cnt_b) in combinations(rows, 2):
        pair = (min(id_a, id_b), max(id_a, id_b))
        if pair in aliased_pairs or pair in dismissed_pairs:
            continue
        score = endorsement_similarity(name_a, name_b)
        if score >= threshold:
            if id_a > id_b:
                out_a_id, out_a_name, out_a_cnt = id_b, name_b, cnt_b
                out_b_id, out_b_name, out_b_cnt = id_a, name_a, cnt_a
            else:
                out_a_id, out_a_name, out_a_cnt = id_a, name_a, cnt_a
                out_b_id, out_b_name, out_b_cnt = id_b, name_b, cnt_b
            suggestions.append(
                {
                    "id_a": out_a_id,
                    "name_a": out_a_name,
                    "count_a": out_a_cnt,
                    "id_b": out_b_id,
                    "name_b": out_b_name,
                    "count_b": out_b_cnt,
                    "score": round(score, 3),
                }
            )

    suggestions.sort(key=lambda s: s["score"], reverse=True)
    return suggestions


async def dismiss_suggestion(
    conn: AsyncConnection,
    id_a: int,
    id_b: int,
    dismissed_by: str,
) -> None:
    """Permanently suppress a duplicate suggestion pair.

    *id_a* and *id_b* are normalised (smaller first) before insert.
    Idempotent — re-dismissing the same pair is a no-op.
    """
    a, b = min(id_a, id_b), max(id_a, id_b)
    stmt = (
        pg_insert(endorsement_dismissed_suggestions)
        .values(endorsement_id_a=a, endorsement_id_b=b, dismissed_by=dismissed_by)
        .on_conflict_do_nothing()
    )
    await conn.execute(stmt)


# ---------------------------------------------------------------------------
# Code-mapping CRUD
# ---------------------------------------------------------------------------


async def get_code_mappings(conn: AsyncConnection) -> list[dict]:
    """Return all WSLCB numeric codes with their endorsement mappings.

    Each dict has:
    - ``code`` — the numeric string (e.g. ``'450'``)
    - ``endorsements`` — list of ``{id, name, record_count, is_variant,
      canonical_id, canonical_name}``
    - ``record_count`` — total records linked via this code's endorsement(s)

    Ordered numerically by code.
    """
    code_rows = (
        await conn.execute(
            select(endorsement_codes.c.code, endorsement_codes.c.endorsement_id).order_by(
                endorsement_codes.c.code
            )
        )
    ).fetchall()

    code_to_eids: dict[str, list[int]] = {}
    for code, eid in code_rows:
        code_to_eids.setdefault(code, []).append(eid)

    eid_meta_rows = (
        await conn.execute(
            select(
                license_endorsements.c.id,
                license_endorsements.c.name,
                func.count(record_endorsements.c.record_id).label("record_count"),
            )
            .select_from(license_endorsements)
            .outerjoin(
                record_endorsements,
                record_endorsements.c.endorsement_id == license_endorsements.c.id,
            )
            .group_by(license_endorsements.c.id, license_endorsements.c.name)
        )
    ).fetchall()
    eid_meta: dict[int, dict] = {}
    for eid, name, cnt in eid_meta_rows:
        eid_meta[eid] = {"id": eid, "name": name, "record_count": cnt}

    alias_rows = (
        await conn.execute(
            select(
                endorsement_aliases.c.endorsement_id,
                endorsement_aliases.c.canonical_endorsement_id,
            )
        )
    ).fetchall()
    alias_map: dict[int, int] = {r[0]: r[1] for r in alias_rows}
    name_by_id = {m["id"]: m["name"] for m in eid_meta.values()}

    # record count per code via endorsement links
    code_record_rows = (
        await conn.execute(
            select(
                endorsement_codes.c.code,
                func.count(func.distinct(record_endorsements.c.record_id)).label("cnt"),
            )
            .select_from(endorsement_codes)
            .join(
                record_endorsements,
                record_endorsements.c.endorsement_id == endorsement_codes.c.endorsement_id,
            )
            .group_by(endorsement_codes.c.code)
        )
    ).fetchall()
    code_record_counts: dict[str, int] = {r[0]: r[1] for r in code_record_rows}

    def _sort_key(code: str) -> tuple:
        try:
            return (0, int(code))
        except ValueError:
            return (1, code)

    result = []
    for code in sorted(code_to_eids.keys(), key=_sort_key):
        eids = code_to_eids[code]
        endorsements = []
        for eid in eids:
            meta = eid_meta.get(eid, {"id": eid, "name": str(eid), "record_count": 0})
            canonical_id = alias_map.get(eid)
            endorsements.append(
                {
                    **meta,
                    "is_variant": eid in alias_map,
                    "canonical_id": canonical_id,
                    "canonical_name": name_by_id.get(canonical_id) if canonical_id else None,
                }
            )
        endorsements.sort(key=lambda e: e["name"])
        result.append(
            {
                "code": code,
                "endorsements": endorsements,
                "record_count": code_record_counts.get(code, 0),
            }
        )
    return result


async def add_code_mapping(
    conn: AsyncConnection,
    code: str,
    endorsement_id: int,
) -> bool:
    """Add *endorsement_id* to the expansion of *code*.

    Returns True if a new row was inserted, False if it already existed.
    """
    stmt = (
        pg_insert(endorsement_codes)
        .values(code=code, endorsement_id=endorsement_id)
        .on_conflict_do_nothing()
    )
    result = await conn.execute(stmt)
    return result.rowcount > 0


async def remove_code_mapping(
    conn: AsyncConnection,
    code: str,
    endorsement_id: int | None = None,
) -> bool:
    """Remove a code mapping row.

    If *endorsement_id* is given, removes only that specific (code,
    endorsement_id) row. Otherwise removes all rows for *code*.

    Returns True if at least one row was deleted.
    """
    if endorsement_id is not None:
        stmt = delete(endorsement_codes).where(
            (endorsement_codes.c.code == code)
            & (endorsement_codes.c.endorsement_id == endorsement_id)
        )
    else:
        stmt = delete(endorsement_codes).where(endorsement_codes.c.code == code)
    result = await conn.execute(stmt)
    return result.rowcount > 0


async def create_code(
    conn: AsyncConnection,
    code: str,
    endorsement_name: str,
) -> int:
    """Create a new code entry mapping *code* to *endorsement_name*.

    Calls ``ensure_endorsement`` then ``add_code_mapping``.

    Returns the endorsement id.
    """
    eid = await ensure_endorsement(conn, endorsement_name)
    await add_code_mapping(conn, code, eid)
    return eid
