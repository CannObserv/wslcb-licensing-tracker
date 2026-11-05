"""Tests for entity_wac_repair.py — the one-off WAC-suffix rename-or-merge pass (#211)."""

import pytest
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.entity_classification import record_manual
from wslcb_licensing_tracker.entity_wac_repair import strip_wac_suffixes
from wslcb_licensing_tracker.models import (
    entities,
    entity_classifications,
    entity_review_flags,
    record_entities,
)
from wslcb_licensing_tracker.pipeline import insert_record

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def _entity(conn, name: str, *, regex=("organization", "business"), jev=None) -> int:
    """Insert an entity as prod has it: resolved columns plus a regex (and optional Jev) row."""
    etype, sub = jev or regex
    eid = (
        await conn.execute(
            pg_insert(entities)
            .values(
                name=name,
                entity_type=etype,
                entity_subtype=sub,
                classification_source="jev" if jev else "regex",
            )
            .returning(entities.c.id)
        )
    ).scalar_one()
    await conn.execute(
        pg_insert(entity_classifications).values(
            entity_id=eid,
            source="regex",
            entity_type=regex[0],
            entity_subtype=regex[1],
            model="regex-1",
        )
    )
    if jev:
        await conn.execute(
            pg_insert(entity_classifications).values(
                entity_id=eid,
                source="jev",
                entity_type=jev[0],
                entity_subtype=jev[1],
                confidence=0.95,
                model="jev-1.14.0",
            )
        )
    return eid


async def _record(conn, fixture: dict, license_number: str, applicants: str) -> int:
    rec = dict(fixture, license_number=license_number, applicants=applicants)
    return (await insert_record(conn, rec))[0]


async def _link(conn, record_id: int, entity_id: int, position: int, role="applicant") -> None:
    await conn.execute(
        pg_insert(record_entities).values(
            record_id=record_id, entity_id=entity_id, role=role, position=position
        )
    )


async def _name(conn, eid: int) -> str | None:
    return (
        await conn.execute(select(entities.c.name).where(entities.c.id == eid))
    ).scalar_one_or_none()


async def _links(conn, record_id: int) -> list[tuple[int, int]]:
    rows = (
        await conn.execute(
            select(record_entities.c.entity_id, record_entities.c.position)
            .where(record_entities.c.record_id == record_id)
            .order_by(record_entities.c.position)
        )
    ).fetchall()
    return [tuple(r) for r in rows]


async def _sources(conn, eid: int) -> list[str]:
    rows = (
        await conn.execute(
            select(entity_classifications.c.source)
            .where(entity_classifications.c.entity_id == eid)
            .order_by(entity_classifications.c.id)
        )
    ).fetchall()
    return [r[0] for r in rows]


async def _triple(conn, eid: int) -> tuple:
    row = (
        await conn.execute(
            select(
                entities.c.entity_type,
                entities.c.entity_subtype,
                entities.c.classification_source,
            ).where(entities.c.id == eid)
        )
    ).one()
    return tuple(row)


class TestRename:
    async def test_singleton_renamed_in_place_keeps_rows(self, pg_conn, standard_new_application):
        """27107's shape: a rename keeps the id, the Jev row and the manual row."""
        eid = await _entity(
            pg_conn, "T211 HTLV TRS LLC PER WAC 314-07-035", jev=("organization", "trust")
        )
        await record_manual(
            pg_conn,
            eid,
            entity_type="organization",
            entity_subtype="business",
            created_by="op@example.com",
        )
        rid = await _record(
            pg_conn,
            standard_new_application,
            "t211_r1",
            "BIZ; T211 HTLV TRS LLC PER WAC 314-07-035",
        )
        await _link(pg_conn, rid, eid, 0)

        report = await strip_wac_suffixes(pg_conn)

        assert await _name(pg_conn, eid) == "T211 HTLV TRS LLC"
        assert {
            "id": eid,
            "old": "T211 HTLV TRS LLC PER WAC 314-07-035",
            "new": "T211 HTLV TRS LLC",
        } in report["renamed"]
        assert {"jev", "manual", "regex"} <= set(await _sources(pg_conn, eid))
        assert await _triple(pg_conn, eid) == ("organization", "business", "manual")
        assert await _links(pg_conn, rid) == [(eid, 0)]

    async def test_rename_refreshes_a_stale_regex_row(self, pg_conn, standard_new_application):
        eid = await _entity(
            pg_conn, "T211 JOHN DOE PER WAC 314-55-035", regex=("organization", "business")
        )
        rid = await _record(
            pg_conn, standard_new_application, "t211_r2", "BIZ; T211 JOHN DOE PER WAC 314-55-035"
        )
        await _link(pg_conn, rid, eid, 0)

        report = await strip_wac_suffixes(pg_conn)

        assert report["regex_rows_written"] >= 1
        assert await _triple(pg_conn, eid) == ("person", None, "regex")


class TestMerge:
    async def test_merges_into_existing_clean_twin(self, pg_conn, standard_new_application):
        """Operator input carries to the twin (#206); the twin's own Jev answer wins."""
        twin = await _entity(pg_conn, "T211 SEVEN, INC.", jev=("organization", "business"))
        dup = await _entity(
            pg_conn, "T211 SEVEN, INC. (PER WAC 314-07-035)", jev=("organization", "nonprofit")
        )
        await record_manual(
            pg_conn,
            dup,
            entity_type="organization",
            entity_subtype="business",
            created_by="op@example.com",
        )
        await pg_conn.execute(
            pg_insert(entity_review_flags).values(
                entity_id=dup, flagged_by="op@example.com", reason="?"
            )
        )
        rid = await _record(
            pg_conn,
            standard_new_application,
            "t211_m1",
            "BIZ; T211 SEVEN, INC. (PER WAC 314-07-035); T211 JANE ROE",
        )
        jane = await _entity(pg_conn, "T211 JANE ROE", regex=("person", None))
        await _link(pg_conn, rid, dup, 0)
        await _link(pg_conn, rid, jane, 1)

        report = await strip_wac_suffixes(pg_conn)

        assert await _name(pg_conn, dup) is None
        assert {
            "id": dup,
            "name": "T211 SEVEN, INC. (PER WAC 314-07-035)",
            "into": twin,
            "into_name": "T211 SEVEN, INC.",
        } in report["merged"]
        assert await _links(pg_conn, rid) == [(twin, 0), (jane, 1)]
        assert await _sources(pg_conn, twin) == ["regex", "jev", "manual"]
        flags = (
            await pg_conn.execute(
                select(func.count()).where(entity_review_flags.c.entity_id == twin)
            )
        ).scalar_one()
        assert flags == 1
        assert await _triple(pg_conn, twin) == ("organization", "business", "manual")
        # The merged-away Jev row went with its entity; the twin's own is the only one left.
        jev_rows = (
            await pg_conn.execute(
                select(entity_classifications.c.entity_subtype).where(
                    (entity_classifications.c.source == "jev")
                    & entity_classifications.c.entity_id.in_([twin, dup])
                )
            )
        ).fetchall()
        assert [r[0] for r in jev_rows] == ["business"]

    async def test_suffixed_only_group_lowest_id_renamed(self, pg_conn, standard_new_application):
        low = await _entity(pg_conn, "T211 TUKWILA GROUP, LLC PER WAC 314-07-03")
        high = await _entity(pg_conn, "T211 TUKWILA GROUP, LLC PER WAC 314-07-035")
        assert low < high
        r1 = await _record(
            pg_conn,
            standard_new_application,
            "t211_g1",
            "BIZ; T211 TUKWILA GROUP, LLC PER WAC 314-07-03",
        )
        r2 = await _record(
            pg_conn,
            standard_new_application,
            "t211_g2",
            "BIZ; T211 TUKWILA GROUP, LLC PER WAC 314-07-035",
        )
        await _link(pg_conn, r1, low, 0)
        await _link(pg_conn, r2, high, 0)

        report = await strip_wac_suffixes(pg_conn)

        assert await _name(pg_conn, low) == "T211 TUKWILA GROUP, LLC"
        assert await _name(pg_conn, high) is None
        assert any(m["id"] == high and m["into"] == low for m in report["merged"])
        assert await _links(pg_conn, r1) == [(low, 0)]
        assert await _links(pg_conn, r2) == [(low, 0)]

    async def test_link_conflict_renumbers_positions(self, pg_conn, standard_new_application):
        """A record listing both the suffixed and the clean name keeps contiguous positions."""
        twin = await _entity(pg_conn, "T211 X LLC")
        dup = await _entity(pg_conn, "T211 X LLC PER WAC 314-07-035")
        y = await _entity(pg_conn, "T211 Y PERSON", regex=("person", None))
        rid = await _record(
            pg_conn,
            standard_new_application,
            "t211_c1",
            "BIZ; T211 X LLC PER WAC 314-07-035; T211 X LLC; T211 Y PERSON",
        )
        await _link(pg_conn, rid, dup, 0)
        await _link(pg_conn, rid, twin, 1)
        await _link(pg_conn, rid, y, 2)

        report = await strip_wac_suffixes(pg_conn)

        assert await _links(pg_conn, rid) == [(twin, 0), (y, 1)]
        assert report["unmatched_records"] == []


class TestScopeAndSafety:
    async def test_lookalikes_and_pure_annotations_untouched(self, pg_conn):
        look = await _entity(pg_conn, "T211 WACHAREE BOWERS", regex=("person", None))
        pure = await _entity(pg_conn, "PER WAC 314-07-035", regex=("person", None))

        report = await strip_wac_suffixes(pg_conn)

        assert await _name(pg_conn, look) == "T211 WACHAREE BOWERS"
        assert await _name(pg_conn, pure) == "PER WAC 314-07-035"
        assert {"id": pure, "name": "PER WAC 314-07-035"} in report["skipped_noise"]
        assert all(r["id"] != look for r in report["renamed"])

    async def test_dry_run_changes_nothing(self, pg_conn, standard_new_application):
        twin = await _entity(pg_conn, "T211 DRY LLC")
        dup = await _entity(pg_conn, "T211 DRY LLC PER WAC 314-07-035")
        solo = await _entity(pg_conn, "T211 SOLO LLC (PER WAC 314-07-035)")
        rid = await _record(
            pg_conn,
            standard_new_application,
            "t211_d1",
            "BIZ; T211 DRY LLC PER WAC 314-07-035; T211 SOLO LLC (PER WAC 314-07-035)",
        )
        await _link(pg_conn, rid, dup, 0)
        await _link(pg_conn, rid, solo, 1)

        report = await strip_wac_suffixes(pg_conn, dry_run=True)

        assert any(m["id"] == dup and m["into"] == twin for m in report["merged"])
        assert any(r["id"] == solo for r in report["renamed"])
        assert report["remaining"] == 0
        assert report["unmatched_records"] == []
        assert await _name(pg_conn, dup) == "T211 DRY LLC PER WAC 314-07-035"
        assert await _name(pg_conn, solo) == "T211 SOLO LLC (PER WAC 314-07-035)"
        assert await _links(pg_conn, rid) == [(dup, 0), (solo, 1)]

    async def test_idempotent_and_verified(self, pg_conn, standard_new_application):
        eid = await _entity(pg_conn, "T211 AGAIN LLC, LICENSE PER WAC 314.07.035")
        rid = await _record(
            pg_conn,
            standard_new_application,
            "t211_i1",
            "BIZ; T211 AGAIN LLC, LICENSE PER WAC 314.07.035",
        )
        await _link(pg_conn, rid, eid, 0)

        first = await strip_wac_suffixes(pg_conn)
        second = await strip_wac_suffixes(pg_conn)

        assert first["remaining"] == 0
        assert first["unmatched_records"] == []
        assert first["records_checked"] >= 1
        assert second["renamed"] == [] and second["merged"] == []


class TestVerification:
    async def test_previous_applicant_links_renamed_and_verified(
        self, pg_conn, standard_new_application
    ):
        """CR 1: most prod links (49 of 61) are previous_applicant."""
        eid = await _entity(pg_conn, "T211 PREV LLC PER WAC")
        rec = dict(
            standard_new_application,
            license_number="t211_p1",
            previous_applicants="PREV BIZ; T211 PREV LLC PER WAC; 314-07-035",
        )
        rid = (await insert_record(pg_conn, rec))[0]
        await _link(pg_conn, rid, eid, 0, role="previous_applicant")

        report = await strip_wac_suffixes(pg_conn)

        assert await _name(pg_conn, eid) == "T211 PREV LLC"
        assert report["records_checked"] == 1
        assert report["unmatched_records"] == []

    async def test_mismatch_between_links_and_applicants_is_reported(
        self, pg_conn, standard_new_application
    ):
        """CR 2: the check that gates the CLI commit detects a real mismatch."""
        eid = await _entity(pg_conn, "T211 MIS LLC PER WAC 314-07-035")
        stray = await _entity(pg_conn, "T211 STRAY PERSON", regex=("person", None))
        rid = await _record(
            pg_conn, standard_new_application, "t211_x1", "BIZ; T211 MIS LLC PER WAC 314-07-035"
        )
        await _link(pg_conn, rid, eid, 0)
        await _link(pg_conn, rid, stray, 1)

        report = await strip_wac_suffixes(pg_conn, dry_run=True)

        assert report["unmatched_records"] == [
            {
                "record_id": rid,
                "role": "applicant",
                "expected": ["T211 MIS LLC"],
                "linked": ["T211 MIS LLC", "T211 STRAY PERSON"],
            }
        ]
