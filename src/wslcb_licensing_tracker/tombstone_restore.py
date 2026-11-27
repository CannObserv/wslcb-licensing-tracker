"""Restore tombstones a pre-#261 ``reclassify-entities`` purged, from its backup (#261).

Before #261, ``reclassify-entities`` deleted an operator-confirmed non-name
together with its manual ``not_a_name`` row, and a later reprocess brought
the string back as a fresh entity.  #214's purge of ``IL`` and ``IL IL`` was
backed up first (``data/remediation-backups/2026-11-06-214/``); this pass
reads that backup and recreates each such entity as a tombstone (#247 §5):

- the entity under its **original id**, name and ``created_at``, so the
  audit rows already written about it point at it again;
- its manual ``not_a_name`` row with the original ``created_by`` and
  ``created_at``, and a note citing #247 §5, #261 and the backup;
- a fresh regex row and the resolved columns, as a new entity gets;
- one ``entity.tombstone_restore`` audit row.

It links nothing: extraction never links a tombstone.  It refuses (nothing
written) when a backup id now holds another name or the name is held by
another id; an entity already restored is reported, not rewritten.  The
operator's email comes from the backup file, never from this code.
``wslcb db restore-tombstones``.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from .admin_audit import log_action
from .entities import REGEX_MODEL, regex_classification
from .entity_aliases import relink
from .entity_classification import NOT_A_NAME, resolve_entities, tombstone_ids
from .models import entities, entity_classifications

logger = logging.getLogger(__name__)

ENTITIES_FILE = "entities.jsonl"
CLASSIFICATIONS_FILE = "entity_classifications.jsonl"
AUDIT_ACTION = "entity.tombstone_restore"


class RestoreRefused(Exception):  # noqa: N818 — a refusal, as SeedRefused is
    """The backup or the database is not what the pass expects; nothing was written."""


@dataclass(frozen=True)
class BackupTombstone:
    """One purged non-name as the backup holds it."""

    id: int
    name: str
    created_at: datetime
    manual_created_by: str | None
    manual_created_at: datetime
    manual_note: str | None


@dataclass
class RestoreResult:
    """What the pass did (or, under ``dry_run``, would do)."""

    restored: list[tuple[int, str]] = field(default_factory=list)
    already: list[tuple[int, str]] = field(default_factory=list)
    dry_run: bool = False


def _jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        msg = f"backup file not found: {path}"
        raise RestoreRefused(msg)
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def read_backup(backup_dir: Path) -> list[BackupTombstone]:
    """The backed-up entities whose latest non-ignored manual row is ``not_a_name``, by id."""
    rows = _jsonl(backup_dir / ENTITIES_FILE)
    manual: dict[int, dict] = {}
    for r in _jsonl(backup_dir / CLASSIFICATIONS_FILE):
        if r.get("source") != "manual" or r.get("ignored_at") is not None:
            continue
        eid = r["entity_id"]
        if eid not in manual or r["id"] > manual[eid]["id"]:
            manual[eid] = r
    out = [
        BackupTombstone(
            id=e["id"],
            name=e["name"],
            created_at=datetime.fromisoformat(e["created_at"]),
            manual_created_by=m.get("created_by"),
            manual_created_at=datetime.fromisoformat(m["created_at"]),
            manual_note=m.get("note"),
        )
        for e in rows
        if (m := manual.get(e["id"])) is not None and m["entity_type"] == NOT_A_NAME
    ]
    return sorted(out, key=lambda t: t.id)


async def _plan(
    conn: AsyncConnection, wanted: list[BackupTombstone]
) -> tuple[list[BackupTombstone], list[BackupTombstone]]:
    """Split *wanted* into (to restore, already restored); refuse any conflict."""
    ids = [t.id for t in wanted]
    by_id = dict(
        (await conn.execute(select(entities.c.id, entities.c.name).where(entities.c.id.in_(ids))))
        .tuples()
        .all()
    )
    names = {t.name.lower() for t in wanted}
    by_name = dict(
        (
            await conn.execute(
                select(func.lower(entities.c.name), entities.c.id).where(
                    func.lower(entities.c.name).in_(names)
                )
            )
        )
        .tuples()
        .all()
    )
    tombs = await tombstone_ids(conn, ids)
    problems: list[str] = []
    todo: list[BackupTombstone] = []
    done: list[BackupTombstone] = []
    for t in wanted:
        held = by_name.get(t.name.lower())
        if t.id in by_id:
            if by_id[t.id] != t.name:
                problems.append(f"id {t.id} now holds {by_id[t.id]!r}, not {t.name!r}")
            elif t.id not in tombs:
                problems.append(f"{t.id} {t.name!r} exists but is not a tombstone")
            else:
                done.append(t)
        elif held is not None:
            problems.append(f"{t.name!r} is held by entity {held}; merge or mark it instead")
        else:
            todo.append(t)
    if problems:
        raise RestoreRefused("; ".join(problems))
    return todo, done


def _note(t: BackupTombstone, backup_dir: Path) -> str:
    note = (
        f"Restored as a tombstone from {backup_dir} (#261, #247 §5): "
        "purged by reclassify-entities before tombstones were kept"
    )
    return f"{note}. Original note: {t.manual_note}" if t.manual_note else note


async def _restore_one(
    conn: AsyncConnection, t: BackupTombstone, *, backup_dir: Path, admin_email: str
) -> None:
    entity_type, entity_subtype = regex_classification(t.name)
    await conn.execute(
        insert(entities).values(
            id=t.id,
            name=t.name,
            entity_type=entity_type,
            entity_subtype=entity_subtype,
            classification_source="regex",
            created_at=t.created_at,
        )
    )
    await conn.execute(
        insert(entity_classifications).values(
            entity_id=t.id,
            source="regex",
            entity_type=entity_type,
            entity_subtype=entity_subtype,
            model=REGEX_MODEL,
        )
    )
    await conn.execute(
        insert(entity_classifications).values(
            entity_id=t.id,
            source="manual",
            entity_type=NOT_A_NAME,
            entity_subtype=None,
            created_by=t.manual_created_by,
            created_at=t.manual_created_at,
            note=_note(t, backup_dir),
        )
    )
    await resolve_entities(conn, [t.id])
    await relink(conn, [t.id])
    await log_action(
        conn,
        admin_email,
        AUDIT_ACTION,
        "entity",
        t.id,
        {
            "name": t.name,
            "backup": str(backup_dir),
            "manual_created_at": t.manual_created_at.isoformat(),
        },
    )
    logger.info("Restored tombstone %d %r from %s", t.id, t.name, backup_dir)


async def restore_tombstones(
    conn: AsyncConnection, backup_dir: Path, *, admin_email: str, dry_run: bool = False
) -> RestoreResult:
    """Recreate each backed-up non-name as a tombstone; see the module docstring.

    Raises :class:`RestoreRefused` (nothing written) for an unreadable backup,
    one with no non-name in it, or a conflict.  ``dry_run`` runs the writes
    in a savepoint and rolls them back.  Caller commits.
    """
    wanted = read_backup(backup_dir)
    if not wanted:
        msg = f"no operator-confirmed non-name in {backup_dir}"
        raise RestoreRefused(msg)
    todo, done = await _plan(conn, wanted)
    result = RestoreResult(
        restored=[(t.id, t.name) for t in todo],
        already=[(t.id, t.name) for t in done],
        dry_run=dry_run,
    )
    if not todo:
        return result
    savepoint = await conn.begin_nested()
    try:
        for t in todo:
            await _restore_one(conn, t, backup_dir=backup_dir, admin_email=admin_email)
    except BaseException:
        await savepoint.rollback()
        raise
    if dry_run:
        await savepoint.rollback()
    else:
        await savepoint.commit()
    return result
