"""Tests for tombstone_restore.py — recreating #214's purged non-names as tombstones (#261)."""

import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

from wslcb_licensing_tracker.entities import get_or_create_entity, parse_and_link_entities
from wslcb_licensing_tracker.entities import reprocess_entities as full_reprocess
from wslcb_licensing_tracker.entity_classification import tombstone_ids
from wslcb_licensing_tracker.models import (
    admin_audit_log,
    entities,
    entity_classifications,
    record_entities,
)
from wslcb_licensing_tracker.pipeline import insert_record
from wslcb_licensing_tracker.tombstone_restore import (
    AUDIT_ACTION,
    RestoreRefused,
    read_backup,
    restore_tombstones,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")

ADMIN = "admin@example.com"
OPERATOR = "operator@example.com"
# Far above any id the test database's sequence hands out.
IL, IL_IL = 990_261_001, 990_261_002


def _ent(eid: int, name: str, created_at: str = "2026-02-26T00:55:11+00:00") -> dict:
    return {"id": eid, "name": name, "entity_type": "person", "created_at": created_at}


def _row(  # noqa: PLR0913
    rid: int, eid: int, source: str, etype: str, at: str, by: str | None = None, note=None
) -> dict:
    return {
        "id": rid,
        "entity_id": eid,
        "source": source,
        "entity_type": etype,
        "created_at": at,
        "created_by": by,
        "note": note,
        "ignored_at": None,
    }


def _write_backup(tmp_path: Path, *, extra_entities=(), extra_rows=()) -> Path:
    """A backup shaped like #214's: the two purged entities and their classification rows."""
    ents = [_ent(IL, "T261 IL"), _ent(IL_IL, "T261 IL IL"), *extra_entities]
    rows = [
        _row(1, IL, "regex", "person", "2026-10-22T23:55:05+00:00"),
        _row(2, IL_IL, "manual", "not_a_name", "2026-11-06T13:55:24.403835+00:00", OPERATOR),
        _row(
            3, IL, "manual", "not_a_name", "2026-11-06T13:56:40.337021+00:00", OPERATOR, "fragment"
        ),
        *extra_rows,
    ]
    for name, data in (("entities.jsonl", ents), ("entity_classifications.jsonl", rows)):
        (tmp_path / name).write_text("".join(json.dumps(r) + "\n" for r in data))
    return tmp_path


async def _manual(conn, eid: int) -> list[tuple]:
    ec = entity_classifications
    rows = await conn.execute(
        select(ec.c.entity_type, ec.c.created_by, ec.c.created_at, ec.c.note)
        .where((ec.c.entity_id == eid) & (ec.c.source == "manual"))
        .order_by(ec.c.id)
    )
    return [tuple(r) for r in rows]


async def test_read_backup_picks_latest_manual_not_a_name(tmp_path):
    backup = _write_backup(
        tmp_path,
        extra_entities=[_ent(7, "KEPT", "2026-01-01T00:00:00+00:00")],
        extra_rows=[
            # A later manual answer of a real type: not a non-name any more.
            _row(4, 7, "manual", "not_a_name", "2026-11-01T00:00:00+00:00", OPERATOR),
            _row(5, 7, "manual", "person", "2026-11-02T00:00:00+00:00", OPERATOR),
        ],
    )
    got = read_backup(backup)
    assert [(t.id, t.name, t.manual_note) for t in got] == [
        (IL, "T261 IL", "fragment"),
        (IL_IL, "T261 IL IL", None),
    ]


async def test_restore_recreates_tombstones_and_audits(pg_conn, tmp_path):
    backup = _write_backup(tmp_path)

    result = await restore_tombstones(pg_conn, backup, admin_email=ADMIN)

    assert result.restored == [(IL, "T261 IL"), (IL_IL, "T261 IL IL")]
    assert await tombstone_ids(pg_conn, [IL, IL_IL]) == {IL, IL_IL}
    created = (
        await pg_conn.execute(select(entities.c.created_at).where(entities.c.id == IL))
    ).scalar_one()
    assert created.isoformat() == "2026-02-26T00:55:11+00:00"
    [(etype, by, at, note)] = await _manual(pg_conn, IL)
    assert (etype, by) == ("not_a_name", OPERATOR)
    assert at.isoformat() == "2026-11-06T13:56:40.337021+00:00"
    assert "#261" in note
    assert str(backup) in note
    assert note.endswith("Original note: fragment")
    regex = (
        await pg_conn.execute(
            select(func.count()).where(
                (entity_classifications.c.entity_id == IL)
                & (entity_classifications.c.source == "regex")
            )
        )
    ).scalar_one()
    assert regex == 1
    audits = (
        await pg_conn.execute(
            select(admin_audit_log.c.admin_email, admin_audit_log.c.details).where(
                (admin_audit_log.c.action == AUDIT_ACTION)
                & admin_audit_log.c.target_id.in_([IL, IL_IL])
            )
        )
    ).fetchall()
    assert len(audits) == 2
    assert {json.loads(d)["name"] for _, d in audits} == {"T261 IL", "T261 IL IL"}
    assert {e for e, _ in audits} == {ADMIN}
    links = (
        await pg_conn.execute(
            select(func.count()).where(record_entities.c.entity_id.in_([IL, IL_IL]))
        )
    ).scalar_one()
    assert links == 0


async def test_rerun_reports_already_restored(pg_conn, tmp_path):
    backup = _write_backup(tmp_path)
    await restore_tombstones(pg_conn, backup, admin_email=ADMIN)
    again = await restore_tombstones(pg_conn, backup, admin_email=ADMIN)
    assert again.restored == []
    assert again.already == [(IL, "T261 IL"), (IL_IL, "T261 IL IL")]
    assert len(await _manual(pg_conn, IL)) == 1


async def test_dry_run_writes_nothing(pg_conn, tmp_path):
    result = await restore_tombstones(
        pg_conn, _write_backup(tmp_path), admin_email=ADMIN, dry_run=True
    )
    assert result.restored == [(IL, "T261 IL"), (IL_IL, "T261 IL IL")]
    n = (
        await pg_conn.execute(select(func.count()).where(entities.c.id.in_([IL, IL_IL])))
    ).scalar_one()
    assert n == 0


async def test_refuses_name_held_by_another_entity(pg_conn, tmp_path):
    """A fresh entity under the name (a reprocess recreated it) is the operator's to resolve."""
    await get_or_create_entity(pg_conn, "T261 IL IL")
    with pytest.raises(RestoreRefused, match="held by entity"):
        await restore_tombstones(pg_conn, _write_backup(tmp_path), admin_email=ADMIN)
    n = (await pg_conn.execute(select(func.count()).where(entities.c.id == IL))).scalar_one()
    assert n == 0


async def test_refuses_missing_or_empty_backup(pg_conn, tmp_path):
    with pytest.raises(RestoreRefused, match="not found"):
        await restore_tombstones(pg_conn, tmp_path, admin_email=ADMIN)
    (tmp_path / "entities.jsonl").write_text("")
    (tmp_path / "entity_classifications.jsonl").write_text("")
    with pytest.raises(RestoreRefused, match="no operator-confirmed"):
        await restore_tombstones(pg_conn, tmp_path, admin_email=ADMIN)


async def test_full_reprocess_after_restore_links_nothing(
    pg_conn, tmp_path, standard_new_application
):
    """The issue's Done-when, in small: after the backfill a full reprocess links no IL."""
    applicants = "NEW LUCK TOY; T261 IL; T261 IL IL; T261 JACOB MIHOULIDES"
    rid = (
        await insert_record(
            pg_conn,
            dict(standard_new_application, license_number="tomb_261_r", applicants=applicants),
        )
    )[0]
    await parse_and_link_entities(pg_conn, rid, "NEW LUCK TOY; T261 JACOB MIHOULIDES")
    await restore_tombstones(pg_conn, _write_backup(tmp_path), admin_email=ADMIN)

    await full_reprocess(pg_conn)

    names = (
        await pg_conn.execute(
            select(entities.c.name)
            .join(record_entities, record_entities.c.entity_id == entities.c.id)
            .where(record_entities.c.record_id == rid)
        )
    ).scalars()
    assert list(names) == ["T261 JACOB MIHOULIDES"]
    n = (
        await pg_conn.execute(
            select(func.count()).where(func.lower(entities.c.name).in_(["t261 il", "t261 il il"]))
        )
    ).scalar_one()
    assert n == 2
