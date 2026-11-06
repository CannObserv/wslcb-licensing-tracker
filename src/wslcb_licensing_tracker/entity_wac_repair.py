"""One-off repair of entity names that carry an appended WSLCB WAC annotation (#211).

Ingest now strips ``PER WAC 314-…`` from applicant names at extraction
(``text_utils.strip_wac_annotation``), but entities created before the fix
keep the suffix: ``7-ELEVEN, INC. PER WAC 314-07-035`` beside ``7-ELEVEN, INC.``.
``reprocess_entities`` can't fix them in place.  It would create fresh,
unclassified entities and leave the suffixed ones orphaned with their Jev and
manual rows.  This pass renames or merges them:

- a suffixed entity whose cleaned name already exists merges into that entity
  (#206 semantics: manual rows and review flags move to it; the merged-away
  entity's regex/Jev rows cascade away; the survivor keeps its own Jev answer);
- otherwise the lowest-id entity of the group sharing the cleaned name is
  renamed in place (keeping its id and every classification row) and the rest
  merge into it.

A renamed entity whose regex answer changed gets a fresh regex row; every
survivor is re-resolved.  A suffixed name that strips to nothing or to noise
is left for ``reclassify-entities``.  Frozen data (``license_records``) is not
touched.  The report re-extracts each affected record's applicant strings and
lists any whose links don't match (expected: none).
"""

import logging
from collections import defaultdict

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from .entities import (
    extract_entity_names,
    is_noise_name,
    merge_entity_into,
    plan_regex_rows,
    renumber_link_positions,
)
from .entity_classification import current_answers, resolve_entities
from .models import entities, entity_classifications, license_records, record_entities
from .text_utils import clean_entity_name, strip_wac_annotation

logger = logging.getLogger(__name__)

_ROLE_COLUMNS = {
    "applicant": license_records.c.applicants,
    "previous_applicant": license_records.c.previous_applicants,
}


def _target(name: str) -> str | None:
    """Return the cleaned name *name* should carry, or None if no annotation is appended."""
    stripped = strip_wac_annotation(name)
    return None if stripped == name else clean_entity_name(stripped)


async def strip_wac_suffixes(conn: AsyncConnection, *, dry_run: bool = False) -> dict:
    """Rename or merge every entity whose name carries an appended WAC annotation.

    ``dry_run`` runs the same steps inside a savepoint and rolls it back, so
    the report (including its verification) is what an apply would produce.
    Idempotent.  Caller commits.

    Returns:
    -------
    dict
        ``examined``, ``renamed`` (``id``/``old``/``new``), ``merged``
        (``id``/``name``/``into``/``into_name``), ``skipped_noise``
        (``id``/``name``), ``links``, ``regex_rows_written``,
        ``resolved_changes``, ``records_checked``, ``unmatched_records``,
        ``remaining``.
    """
    if not dry_run:
        return await _strip(conn, dry_run=False)
    savepoint = await conn.begin_nested()
    try:
        return await _strip(conn, dry_run=True)
    finally:
        await savepoint.rollback()


def _plan(rows: list[tuple[int, str]]) -> tuple[dict[str, list[tuple[int, str]]], list[dict]]:
    """Group suffixed ``(id, name)`` rows by cleaned target name; list those that strip to noise."""
    groups: dict[str, list[tuple[int, str]]] = defaultdict(list)
    skipped: list[dict] = []
    for eid, name in rows:
        target = _target(name)
        if target is None:
            continue
        if not target or is_noise_name(target):
            skipped.append({"id": eid, "name": name})
        else:
            groups[target].append((eid, name))
    return groups, skipped


async def _affected_links(
    conn: AsyncConnection, entity_ids: list[int]
) -> tuple[list[tuple[int, str]], int]:
    """Return the distinct ``(record_id, role)`` pairs linking *entity_ids*, and the link count."""
    if not entity_ids:
        return [], 0
    where = record_entities.c.entity_id.in_(entity_ids)
    pairs = [
        (rid, role)
        for rid, role in (
            await conn.execute(
                select(record_entities.c.record_id, record_entities.c.role)
                .where(where)
                .distinct()
                .order_by(record_entities.c.record_id, record_entities.c.role)
            )
        ).fetchall()
    ]
    links = (await conn.execute(select(func.count()).where(where))).scalar_one()
    return pairs, links


async def _apply(
    conn: AsyncConnection, groups: dict[str, list[tuple[int, str]]], by_lower: dict[str, int]
) -> tuple[list[dict], list[dict], list[int]]:
    """Rename or merge each group; return ``(renamed, merged, survivor ids)``."""
    suffixed = {eid for members in groups.values() for eid, _ in members}
    renamed: list[dict] = []
    merged: list[dict] = []
    survivors: list[int] = []
    for target, members in sorted(groups.items()):
        twin = by_lower.get(target.lower())
        if twin is not None and twin not in suffixed:
            survivor, dups = twin, members
        else:
            (survivor, old), dups = members[0], members[1:]
            await conn.execute(
                update(entities).where(entities.c.id == survivor).values(name=target)
            )
            renamed.append({"id": survivor, "old": old, "new": target})
        for dup_id, dup_name in dups:
            await merge_entity_into(conn, dup_id, survivor)
            merged.append({"id": dup_id, "name": dup_name, "into": survivor, "into_name": target})
        survivors.append(survivor)
    return renamed, merged, survivors


async def _strip(conn: AsyncConnection, *, dry_run: bool) -> dict:
    rows = (
        await conn.execute(select(entities.c.id, entities.c.name).order_by(entities.c.id))
    ).fetchall()
    by_lower: dict[str, int] = {}
    for eid, name in rows:
        by_lower.setdefault(name.lower(), eid)

    groups, skipped = _plan(rows)
    suffixed = sorted(eid for members in groups.values() for eid, _ in members)
    pairs, links = await _affected_links(conn, suffixed)
    renamed, merged, survivors = await _apply(conn, groups, by_lower)
    await renumber_link_positions(conn, pairs)

    regex_rows: list[dict] = []
    if renamed:
        ids = [r["id"] for r in renamed]
        regex_rows = plan_regex_rows(
            [(r["id"], r["new"]) for r in renamed], await current_answers(conn, ids)
        )
        if regex_rows:
            await conn.execute(insert(entity_classifications), regex_rows)
    resolved = await resolve_entities(conn, survivors) if survivors else None

    unmatched = await _unmatched_records(conn, pairs)
    remaining = sum(
        1
        for (name,) in (await conn.execute(select(entities.c.name))).fetchall()
        if (t := _target(name)) and not is_noise_name(t)
    )

    tag = " (dry-run)" if dry_run else ""
    if not dry_run:
        for r in renamed:
            logger.info("Renamed entity %d %r -> %r", r["id"], r["old"], r["new"])
        for m in merged:
            logger.info(
                "Merged entity %d %r -> %d %r", m["id"], m["name"], m["into"], m["into_name"]
            )
    logger.info(
        "strip_wac_suffixes%s: %d suffixed entit(ies) (%d link(s)), %d renamed, %d merged, "
        "%d skipped as noise, %d regex row(s), %d record(s) checked, %d unmatched, %d remaining",
        tag,
        len(suffixed),
        links,
        len(renamed),
        len(merged),
        len(skipped),
        len(regex_rows),
        len({rid for rid, _ in pairs}),
        len(unmatched),
        remaining,
    )
    return {
        "examined": len(rows),
        "renamed": renamed,
        "merged": merged,
        "skipped_noise": skipped,
        "links": links,
        "regex_rows_written": len(regex_rows),
        "resolved_changes": len(resolved.changes) if resolved else 0,
        "records_checked": len({rid for rid, _ in pairs}),
        "unmatched_records": unmatched,
        "remaining": remaining,
    }


async def _unmatched_records(conn: AsyncConnection, pairs: list[tuple[int, str]]) -> list[dict]:
    """Return the ``(record, role)`` pairs whose links differ from a fresh extraction."""
    out: list[dict] = []
    for record_id, role in pairs:
        source = (
            await conn.execute(select(_ROLE_COLUMNS[role]).where(license_records.c.id == record_id))
        ).scalar_one()
        expected = extract_entity_names(source)
        rows = (
            await conn.execute(
                select(entities.c.name, record_entities.c.position)
                .join(record_entities, record_entities.c.entity_id == entities.c.id)
                .where(
                    (record_entities.c.record_id == record_id) & (record_entities.c.role == role)
                )
                .order_by(record_entities.c.position)
            )
        ).fetchall()
        linked = [name for name, _ in rows]
        positions = [pos for _, pos in rows]
        if linked != expected or positions != list(range(len(positions))):
            out.append(
                {"record_id": record_id, "role": role, "expected": expected, "linked": linked}
            )
    return out
