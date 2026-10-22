"""Tests for entities.py — async entity normalization."""

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.entities import (
    get_or_create_entity,
    get_record_entities,
    merge_duplicate_entities,
    parse_and_link_entities,
    reclassify_entities,
)
from wslcb_licensing_tracker.models import entities, entity_classifications, record_entities
from wslcb_licensing_tracker.pipeline import insert_record


class TestGetOrCreateEntity:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_and_returns_id(self, pg_conn):
        eid = await get_or_create_entity(pg_conn, "john doe")
        assert isinstance(eid, int)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn):
        eid1 = await get_or_create_entity(pg_conn, "JANE SMITH")
        eid2 = await get_or_create_entity(pg_conn, "jane smith")
        assert eid1 == eid2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_raises_on_empty_name(self, pg_conn):
        with pytest.raises(ValueError):
            await get_or_create_entity(pg_conn, "")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_writes_regex_row_and_resolved_columns(self, pg_conn):
        """#206: a new entity gets its resolved columns and one regex classification row."""
        eid = await get_or_create_entity(pg_conn, "T206 LOWER ELWHA KLALLAM TRIBE")
        row = (
            await pg_conn.execute(
                select(
                    entities.c.entity_type,
                    entities.c.entity_subtype,
                    entities.c.classification_source,
                ).where(entities.c.id == eid)
            )
        ).one()
        assert tuple(row) == ("organization", "tribe", "regex")

        async def _rows():
            return (
                await pg_conn.execute(
                    select(
                        entity_classifications.c.source,
                        entity_classifications.c.entity_type,
                        entity_classifications.c.entity_subtype,
                        entity_classifications.c.model,
                        entity_classifications.c.confidence,
                    ).where(entity_classifications.c.entity_id == eid)
                )
            ).fetchall()

        assert [tuple(r) for r in await _rows()] == [
            ("regex", "organization", "tribe", "regex-1", None)
        ]
        # The fallback-SELECT path writes nothing.
        assert await get_or_create_entity(pg_conn, "t206 lower elwha klallam tribe") == eid
        assert len(await _rows()) == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_person_has_null_subtype(self, pg_conn):
        eid = await get_or_create_entity(pg_conn, "T206 JOHN Q DOE")
        row = (
            await pg_conn.execute(
                select(entities.c.entity_type, entities.c.entity_subtype).where(
                    entities.c.id == eid
                )
            )
        ).one()
        assert tuple(row) == ("person", None)


class TestParseAndLinkEntities:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_links_applicants(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_001"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        count = await parse_and_link_entities(
            pg_conn,
            record_id,
            "ACME CANNABIS CO; JOHN DOE; JANE SMITH",
            role="applicant",
        )
        # Skips first element (business name), links JOHN DOE + JANE SMITH
        assert count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_record_entities(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_002"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await parse_and_link_entities(
            pg_conn,
            record_id,
            "ACME CANNABIS CO; JOHN DOE",
            role="applicant",
        )
        entity_map = await get_record_entities(pg_conn, [record_id])
        assert record_id in entity_map
        applicants = entity_map[record_id].get("applicant", [])
        assert len(applicants) >= 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_003"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        await parse_and_link_entities(pg_conn, record_id, "JOHN DOE; JANE SMITH", role="applicant")
        await parse_and_link_entities(
            pg_conn,
            record_id,
            "JOHN DOE; JANE SMITH",
            role="applicant",
            delete_existing=True,
        )
        entity_map = await get_record_entities(pg_conn, [record_id])
        applicants = entity_map[record_id].get("applicant", [])
        # First element ("JOHN DOE") is the business name and is skipped;
        # only JANE SMITH is linked. Idempotency: should be exactly 1, not 2.
        assert len(applicants) == 1


class TestMergeDuplicateEntities:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_int(self, pg_conn):
        count = await merge_duplicate_entities(pg_conn)
        assert isinstance(count, int)
        assert count >= 0


class TestNoiseSkipped:
    """#203: non-name strings never become entities."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_noise_not_linked(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_noise_001"
        record_id = (await insert_record(pg_conn, standard_new_application))[0]
        count = await parse_and_link_entities(
            pg_conn,
            record_id,
            "ACME LLC; PER WAC 314-07-035; 035; JOHN DOE; 17 SHAREHOLDERS W/10% OR LESS",
            role="applicant",
            delete_existing=True,
        )
        assert count == 1
        linked = (await get_record_entities(pg_conn, [record_id]))[record_id]["applicant"]
        assert [e["name"] for e in linked] == ["JOHN DOE"]
        n = (
            await pg_conn.execute(
                select(func.count()).where(entities.c.name.in_(["PER WAC 314-07-035", "035"]))
            )
        ).scalar_one()
        assert n == 0


async def _seed_entity(conn, name: str, entity_type: str) -> int:
    """Insert an entity row directly with a (possibly stale) type."""
    return (
        await conn.execute(
            pg_insert(entities)
            .values(name=name, entity_type=entity_type)
            .on_conflict_do_update(index_elements=["name"], set_={"entity_type": entity_type})
            .returning(entities.c.id)
        )
    ).scalar_one()


class TestReclassifyEntities:
    """#203: explicit pass that re-applies the classifier and purges noise."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_dry_run_reports_without_writing(self, pg_conn):
        eid = await _seed_entity(pg_conn, "SEABROOK MANAGEMENT CO", "person")
        result = await reclassify_entities(pg_conn, dry_run=True)
        assert result["transitions"].get("person->organization", 0) >= 1
        assert any(s["id"] == eid for s in result["reclassified_sample"]) or (
            result["reclassified"] > len(result["reclassified_sample"])
        )
        row = (
            await pg_conn.execute(select(entities.c.entity_type).where(entities.c.id == eid))
        ).scalar_one()
        assert row == "person"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_updates_type_and_is_idempotent(self, pg_conn):
        org = await _seed_entity(pg_conn, "GLEASON & KOATZ LLP", "person")
        person = await _seed_entity(pg_conn, "ROBERT J PIZZA", "person")
        first = await reclassify_entities(pg_conn)
        assert first["reclassified"] >= 1
        types = dict(
            (
                await pg_conn.execute(
                    select(entities.c.id, entities.c.entity_type).where(
                        entities.c.id.in_([org, person])
                    )
                )
            ).fetchall()
        )
        assert types == {org: "organization", person: "person"}
        second = await reclassify_entities(pg_conn)
        assert second["reclassified"] == 0
        assert second["noise_entities"] == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_purges_noise_and_renumbers_positions(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_noise_002"
        record_id = (await insert_record(pg_conn, standard_new_application))[0]
        await parse_and_link_entities(
            pg_conn, record_id, "ACME LLC; JANE ROE; JOHN DOE", delete_existing=True
        )
        # Simulate a pre-#203 link: noise entity at position 0.
        noise_id = await _seed_entity(pg_conn, "PER WAC 314-07-035", "person")
        await pg_conn.execute(
            update(record_entities)
            .where(record_entities.c.record_id == record_id)
            .values(position=record_entities.c.position + 1)
        )
        await pg_conn.execute(
            pg_insert(record_entities).values(
                record_id=record_id, entity_id=noise_id, role="applicant", position=0
            )
        )

        dry = await reclassify_entities(pg_conn, dry_run=True)
        assert dry["noise_entities"] >= 1
        assert dry["noise_links"] >= 1
        assert (
            await pg_conn.execute(select(func.count()).where(entities.c.id == noise_id))
        ).scalar_one() == 1

        result = await reclassify_entities(pg_conn)
        assert result["noise_entities"] >= 1
        assert (
            await pg_conn.execute(select(func.count()).where(entities.c.id == noise_id))
        ).scalar_one() == 0
        rows = (
            await pg_conn.execute(
                select(entities.c.name, record_entities.c.position)
                .join(entities, entities.c.id == record_entities.c.entity_id)
                .where(record_entities.c.record_id == record_id)
                .order_by(record_entities.c.position)
            )
        ).fetchall()
        assert [tuple(r) for r in rows] == [("JANE ROE", 0), ("JOHN DOE", 1)]
