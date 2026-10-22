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
from wslcb_licensing_tracker.models import (
    entities,
    entity_classifications,
    entity_review_flags,
    record_entities,
)
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


async def _manual(conn, eid: int, entity_type: str, subtype: str | None = None) -> None:
    from wslcb_licensing_tracker.entity_classification import record_manual

    await record_manual(
        conn, eid, entity_type=entity_type, entity_subtype=subtype, created_by="op@example.com"
    )


async def _flag(conn, eid: int) -> None:
    await conn.execute(
        pg_insert(entity_review_flags).values(
            entity_id=eid, flagged_by="op@example.com", reason="?"
        )
    )


async def _triple(conn, eid: int) -> tuple:
    return tuple(
        (
            await conn.execute(
                select(
                    entities.c.entity_type,
                    entities.c.entity_subtype,
                    entities.c.classification_source,
                ).where(entities.c.id == eid)
            )
        ).one()
    )


async def _exists(conn, eid: int) -> bool:
    n = (await conn.execute(select(func.count()).where(entities.c.id == eid))).scalar_one()
    return n == 1


class TestReclassifyThroughResolver:
    """#206: reclassify writes regex rows and resolves; operator input is protected."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reclassify_writes_regex_rows_and_resolves(self, pg_conn):
        eid = await _seed_entity(pg_conn, "T206 GLEASON & KOATZ LLP", "person")
        first = await reclassify_entities(pg_conn)
        assert first["regex_rows_written"] >= 1
        assert first["subtype_changes"] >= 1
        assert await _triple(pg_conn, eid) == ("organization", "business", "regex")
        regex_rows = (
            await pg_conn.execute(
                select(entity_classifications.c.model).where(
                    (entity_classifications.c.entity_id == eid)
                    & (entity_classifications.c.source == "regex")
                )
            )
        ).fetchall()
        assert [r[0] for r in regex_rows] == ["regex-1"]
        second = await reclassify_entities(pg_conn)
        assert second["regex_rows_written"] == 0
        assert second["reclassified"] == 0
        assert second["subtype_changes"] == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reclassify_never_overrides_manual(self, pg_conn):
        eid = await _seed_entity(pg_conn, "T206 KOATZ & GLEASON LLP", "person")
        await _manual(pg_conn, eid, "person")
        await reclassify_entities(pg_conn)
        assert await _triple(pg_conn, eid) == ("person", None, "manual")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_noise_purge_skips_manual_and_flagged(self, pg_conn):
        trustee = await _seed_entity(pg_conn, "TRUSTEE", "person")
        digits = await _seed_entity(pg_conn, "035", "person")
        await _manual(pg_conn, trustee, "person")
        await _flag(pg_conn, digits)
        result = await reclassify_entities(pg_conn)
        assert await _exists(pg_conn, trustee)
        assert await _exists(pg_conn, digits)
        assert result["protected_noise_skipped"] >= 2
        assert await _triple(pg_conn, trustee) == ("person", None, "manual")
        # the kept noise-named entity still gets a regex answer
        n = (
            await pg_conn.execute(
                select(func.count()).where(
                    (entity_classifications.c.entity_id == digits)
                    & (entity_classifications.c.source == "regex")
                )
            )
        ).scalar_one()
        assert n == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_confirmed_noise_is_purged(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "entity_noise_206"
        record_id = (await insert_record(pg_conn, standard_new_application))[0]
        await parse_and_link_entities(
            pg_conn,
            record_id,
            "ACME LLC; T206 JOHN DOE XYZ; T206 JANE ROE",
            delete_existing=True,
        )
        noise = (
            await pg_conn.execute(
                select(entities.c.id).where(entities.c.name == "T206 JOHN DOE XYZ")
            )
        ).scalar_one()
        await _manual(pg_conn, noise, "not_a_name")
        result = await reclassify_entities(pg_conn)
        assert result["confirmed_noise"] >= 1
        assert not await _exists(pg_conn, noise)
        rows = (
            await pg_conn.execute(
                select(entities.c.name, record_entities.c.position)
                .join(entities, entities.c.id == record_entities.c.entity_id)
                .where(record_entities.c.record_id == record_id)
            )
        ).fetchall()
        assert [tuple(r) for r in rows] == [("T206 JANE ROE", 0)]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_flag_keeps_confirmed_noise(self, pg_conn):
        eid = await _seed_entity(pg_conn, "T206 NOT A NAME BUT FLAGGED", "person")
        await _manual(pg_conn, eid, "not_a_name")
        await _flag(pg_conn, eid)
        await reclassify_entities(pg_conn)
        assert await _exists(pg_conn, eid)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reclassify_dry_run_writes_nothing(self, pg_conn):
        eid = await _seed_entity(pg_conn, "T206 DRY RUN HOLDINGS", "person")
        noise = await _seed_entity(pg_conn, "PER WAC 314-07-035", "person")

        async def _count():
            return (
                await pg_conn.execute(select(func.count()).select_from(entity_classifications))
            ).scalar_one()

        before = await _count()
        result = await reclassify_entities(pg_conn, dry_run=True)
        assert result["regex_rows_written"] >= 1
        assert result["reclassified"] >= 1
        assert result["noise_entities"] >= 1
        assert await _count() == before
        assert await _triple(pg_conn, eid) == ("person", None, "regex")
        assert await _exists(pg_conn, noise)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reprocess_entities_leaves_classification_untouched(
        self, pg_conn, standard_new_application
    ):
        from wslcb_licensing_tracker.entities import reprocess_entities

        standard_new_application["license_number"] = "entity_reprocess_206"
        standard_new_application["applicants"] = "ACME LLC; T206 REPROCESS PERSON"
        record_id = (await insert_record(pg_conn, standard_new_application))[0]
        await parse_and_link_entities(
            pg_conn, record_id, standard_new_application["applicants"], delete_existing=True
        )
        eid = (
            await pg_conn.execute(
                select(entities.c.id).where(entities.c.name == "T206 REPROCESS PERSON")
            )
        ).scalar_one()
        await _manual(pg_conn, eid, "organization", "nonprofit")

        async def _rows():
            return (
                await pg_conn.execute(
                    select(entity_classifications).where(entity_classifications.c.entity_id == eid)
                )
            ).fetchall()

        before = (await _rows(), await _triple(pg_conn, eid))
        await reprocess_entities(pg_conn, record_id=record_id)
        assert (await _rows(), await _triple(pg_conn, eid)) == before
        assert before[1] == ("organization", "nonprofit", "manual")


class TestMergeMovesOperatorInput:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_merge_moves_manual_rows_and_flags(self, pg_conn):
        canon = await _seed_entity(pg_conn, "T206 MERGE TARGET", "person")
        dup = await _seed_entity(pg_conn, "t206 merge target", "person")
        assert canon < dup
        await _manual(pg_conn, dup, "organization", "nonprofit")
        await _flag(pg_conn, dup)
        merged = await merge_duplicate_entities(pg_conn)
        assert merged >= 1
        assert not await _exists(pg_conn, dup)
        assert await _triple(pg_conn, canon) == ("organization", "nonprofit", "manual")
        flags = (
            await pg_conn.execute(
                select(func.count()).where(entity_review_flags.c.entity_id == canon)
            )
        ).scalar_one()
        assert flags == 1
