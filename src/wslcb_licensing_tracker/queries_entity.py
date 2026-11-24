"""Entity-centric record queries for WSLCB licensing tracker.

Contains:
- get_entity_records() — hydrated records of an entity's party (#263)
- global_party() — where a global alias variant's page redirects (#263)
- get_party_members() — the "Also listed as" spellings of a party (#263)
- get_entities() — paginated, searchable entity list

The alias layer (#262) resolves each link to a party, stored as
``record_entities.party_id`` (#263); these read it.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .entity_aliases import eff_many, reverse_closure
from .entity_classification import ENTITY_SUBTYPES, with_jev_answer
from .queries_hydrate import _hydrate_records
from .queries_search import RECORD_COLUMNS, RECORD_JOINS

logger = logging.getLogger(__name__)

# Above this many matching ids, /entities counts every party (presorted) instead
# of filtering the count on the id array (measured on a prod copy, #263 CR 3).
_NARROW_SEARCH = 5000


async def get_entity_records(conn: AsyncConnection, entity_id: int) -> list[dict]:
    """Fetch the records of *entity_id*'s party, newest first, with location data.

    The union of every link that resolves to the entity (its own and its
    members', ``record_entities.party_id``) and the entity's own links.  An
    own link that resolves elsewhere (a scoped variant on its licence) sets
    the record's ``party_note`` to ``{"id", "name"}`` of that party; it is
    None on every other record.
    """
    result = await conn.execute(
        text(
            f"SELECT DISTINCT {RECORD_COLUMNS} {RECORD_JOINS}"
            " JOIN record_entities re ON re.record_id = lr.id"
            " WHERE re.party_id = :entity_id OR re.entity_id = :entity_id"
            " ORDER BY lr.record_date DESC, lr.id DESC"
        ),
        {"entity_id": entity_id},
    )
    rows = [dict(r) for r in result.mappings().all()]
    notes = {
        rid: {"id": pid, "name": name}
        for rid, pid, name in (
            await conn.execute(
                text(
                    "SELECT re.record_id, re.party_id, p.name FROM record_entities re"
                    " JOIN entities p ON p.id = re.party_id"
                    " WHERE re.entity_id = :entity_id AND re.party_id <> :entity_id"
                    " AND NOT EXISTS (SELECT 1 FROM record_entities o"
                    "   WHERE o.record_id = re.record_id AND o.party_id = :entity_id)"
                ),
                {"entity_id": entity_id},
            )
        ).fetchall()
    }
    records = await _hydrate_records(conn, rows)
    for r in records:
        r["party_note"] = notes.get(r["id"])
    return records


async def global_party(conn: AsyncConnection, entity_id: int) -> int | None:
    """Return ``eff(entity_id)`` everywhere if the entity is a global alias variant, else None."""
    party = (await eff_many(conn, [(entity_id, None)]))[(entity_id, None)]
    return None if party == entity_id else party


async def get_party_members(conn: AsyncConnection, entity_id: int) -> list[dict]:
    """Return the other spellings that resolve to *entity_id*, sorted by name.

    Each is ``{"id", "name", "licences"}``: ``licences`` is None when the
    member resolves to the entity everywhere, else the sorted licence numbers
    on which its links do.  A member that resolves here nowhere is left out.
    """
    candidates = sorted(await reverse_closure(conn, [entity_id]) - {entity_id})
    if not candidates:
        return []
    everywhere = await eff_many(conn, [(m, None) for m in candidates])
    scoped: dict[int, list[str]] = {}
    for m, lic in (
        await conn.execute(
            text(
                "SELECT DISTINCT re.entity_id, lr.license_number FROM record_entities re"
                " JOIN license_records lr ON lr.id = re.record_id"
                " WHERE re.party_id = :entity_id AND re.entity_id = ANY(:candidates)"
                " ORDER BY 1, 2"
            ),
            {"entity_id": entity_id, "candidates": candidates},
        )
    ).fetchall():
        scoped.setdefault(m, []).append(lic)
    names = dict(
        (
            await conn.execute(
                text("SELECT id, name FROM entities WHERE id = ANY(:ids)"), {"ids": candidates}
            )
        ).fetchall()
    )
    members = [
        {
            "id": m,
            "name": names[m],
            "licences": None if everywhere[(m, None)] == entity_id else scoped[m],
        }
        for m in candidates
        if m in names and (everywhere[(m, None)] == entity_id or m in scoped)
    ]
    return sorted(members, key=lambda d: (d["name"], d["id"]))


async def get_entities(  # noqa: PLR0913
    conn: AsyncConnection,
    *,
    q: str | None = None,
    entity_type: str | None = None,
    entity_subtype: str | None = None,
    sort: str = "count",
    page: int = 1,
    per_page: int = 50,
) -> dict:
    """Return a paginated, searchable list of all entities.

    Parameters
    ----------
    q:
        Optional name substring (case-insensitive ILIKE search).
    entity_type:
        Optional filter: 'person', 'organization', or None for all.
    entity_subtype:
        Optional filter on the resolved subtype; a value not in
        ``ENTITY_SUBTYPES`` is ignored.  Only organizations have one.
    sort:
        'count' (default) — most active first (record_count DESC, name ASC);
        'name' — alphabetical (name ASC).

    Global alias variants are not listed; ``record_count`` counts the distinct
    records of the entity's party, and ``q`` also matches the spellings linked
    under a party (#263).
    page:
        1-based page number.
    per_page:
        Rows per page (default 50).

    Returns a dict with keys ``entities`` (list of row dicts) and ``total`` (int).
    Rows carry ``id``, ``name``, ``entity_type``, ``entity_subtype``,
    ``classification_source``, ``record_count``, ``jev_confidence``, ``jev_subtype``.
    """
    # A global alias variant is listed under its party, not on its own (#263).
    where_parts = [
        "NOT EXISTS (SELECT 1 FROM entity_aliases a"
        " WHERE a.variant_id = e.id AND a.scope_license IS NULL)"
    ]
    params: dict = {}
    if q:
        # The party's own name, or any member's spelling linked under it: one
        # ILIKE scan, then the parties those spellings resolve to.
        matched = list(
            (
                await conn.execute(
                    text("SELECT id FROM entities WHERE name ILIKE :q_like"),
                    {"q_like": f"%{q}%"},
                )
            ).scalars()
        )
        parties = (
            await conn.execute(
                text(
                    "SELECT DISTINCT party_id FROM record_entities"
                    " WHERE entity_id = ANY(:ids) AND party_id <> entity_id"
                ),
                {"ids": matched},
            )
        ).scalars()
        where_parts.append("e.id = ANY(:q_ids)")
        params["q_ids"] = sorted(set(matched) | set(parties))
    if entity_type is not None:
        where_parts.append("e.entity_type = :entity_type")
        params["entity_type"] = entity_type
    if entity_subtype in ENTITY_SUBTYPES:
        where_parts.append("e.entity_subtype = :entity_subtype")
        params["entity_subtype"] = entity_subtype
    where_clause = "WHERE " + " AND ".join(where_parts)
    cols = "e.id, e.name, e.entity_type, e.entity_subtype, e.classification_source"

    total = (
        await conn.execute(text(f"SELECT COUNT(*) FROM entities e {where_clause}"), params)
    ).scalar_one()

    page = max(1, page)
    page_params = {**params, "limit": per_page, "offset": (page - 1) * per_page}
    # Records are counted per party over record_entities.party_id, distinct
    # (two spellings on one record count once); the (party_id, record_id)
    # index makes the distinct count presorted.
    if sort != "name":
        # A narrow search counts only its matches; a broad one is cheaper as
        # the full presorted count than as a filter on a huge id array.
        narrow = q and len(params["q_ids"]) <= _NARROW_SEARCH
        only = " WHERE party_id = ANY(:q_ids)" if narrow else ""
        sql = f"""
            WITH counts AS (
                SELECT party_id, COUNT(DISTINCT record_id) AS n
                FROM record_entities{only} GROUP BY party_id
            )
            SELECT {cols}, COALESCE(c.n, 0) AS record_count
            FROM entities e LEFT JOIN counts c ON c.party_id = e.id
            {where_clause}
            ORDER BY record_count DESC, e.name ASC LIMIT :limit OFFSET :offset
        """
        rows = [dict(r) for r in (await conn.execute(text(sql), page_params)).mappings().all()]
    else:
        sql = (
            f"SELECT {cols} FROM entities e {where_clause}"
            " ORDER BY e.name ASC LIMIT :limit OFFSET :offset"
        )
        rows = [dict(r) for r in (await conn.execute(text(sql), page_params)).mappings().all()]
        counts = dict(
            (
                await conn.execute(
                    text(
                        "SELECT party_id, COUNT(DISTINCT record_id) FROM record_entities"
                        " WHERE party_id = ANY(:ids) GROUP BY party_id"
                    ),
                    {"ids": [r["id"] for r in rows]},
                )
            ).fetchall()
        )
        for r in rows:
            r["record_count"] = counts.get(r["id"], 0)
    return {"entities": await with_jev_answer(conn, rows), "total": total}
