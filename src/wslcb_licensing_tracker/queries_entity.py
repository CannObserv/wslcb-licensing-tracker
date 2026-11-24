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
    page:
        1-based page number.
    per_page:
        Rows per page (default 50).

    Returns a dict with keys ``entities`` (list of row dicts) and ``total`` (int).
    Rows carry ``id``, ``name``, ``entity_type``, ``entity_subtype``,
    ``classification_source``, ``record_count``, ``jev_confidence``, ``jev_subtype``.
    """
    where_parts = []
    params: dict = {}
    if q:
        where_parts.append("e.name ILIKE :q_like")
        params["q_like"] = f"%{q}%"
    if entity_type is not None:
        where_parts.append("e.entity_type = :entity_type")
        params["entity_type"] = entity_type
    if entity_subtype in ENTITY_SUBTYPES:
        where_parts.append("e.entity_subtype = :entity_subtype")
        params["entity_subtype"] = entity_subtype

    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    order_clause = (
        "ORDER BY record_count DESC, e.name ASC" if sort != "name" else "ORDER BY e.name ASC"
    )

    base_sql = f"""
        SELECT e.id, e.name, e.entity_type, e.entity_subtype, e.classification_source,
               COUNT(re.record_id) AS record_count
        FROM entities e
        LEFT JOIN record_entities re ON re.entity_id = e.id
        {where_clause}
        GROUP BY e.id
    """

    total_result = await conn.execute(
        text(f"SELECT COUNT(*) FROM ({base_sql}) sub"),
        params,
    )
    total = total_result.scalar_one()

    page = max(1, page)
    offset = (page - 1) * per_page
    rows_result = await conn.execute(
        text(f"{base_sql} {order_clause} LIMIT :limit OFFSET :offset"),
        {**params, "limit": per_page, "offset": offset},
    )
    rows = [dict(r) for r in rows_result.mappings().all()]
    return {"entities": await with_jev_answer(conn, rows), "total": total}
