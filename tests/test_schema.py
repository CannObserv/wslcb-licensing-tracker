"""Tests for PostgreSQL schema initialization.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
init_db is also exercised by the pg_engine fixture itself, which calls it
to migrate the test database. Also pins the entity-classification constraints,
defaults and cascades from migration 0009 (#206), and 0010's type/subtype
pairing on both entity tables (#212).
"""

import pytest
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import IntegrityError

from wslcb_licensing_tracker.models import (
    classification_runs,
    entities,
    entity_classifications,
    entity_review_flags,
)
from wslcb_licensing_tracker.schema import init_db


@pytest.mark.asyncio(loop_scope="session")
async def test_init_db_creates_tables(pg_engine):
    """init_db runs Alembic migrations, is idempotent, and creates all expected tables."""
    # pg_engine already ran init_db once — calling again must be a no-op.
    await init_db(pg_engine)

    async with pg_engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            )
        )
        tables = {row[0] for row in result}

    assert "license_records" in tables
    assert "locations" in tables
    assert "sources" in tables
    assert "record_sources" in tables


async def _entity(conn, name: str, entity_type: str = "person", **values) -> int:
    return (
        await conn.execute(
            insert(entities)
            .values(name=name, entity_type=entity_type, **values)
            .returning(entities.c.id)
        )
    ).scalar_one()


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_subtype_check_rejects_unknown_value(pg_conn):
    with pytest.raises(IntegrityError):
        async with pg_conn.begin_nested():
            await _entity(
                pg_conn, "SCHEMA TEST BOGUS SUBTYPE", "organization", entity_subtype="bogus"
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_classification_source_defaults_to_regex(pg_conn):
    eid = await _entity(pg_conn, "SCHEMA TEST DEFAULT SOURCE")
    row = (
        await pg_conn.execute(
            select(entities.c.entity_subtype, entities.c.classification_source).where(
                entities.c.id == eid
            )
        )
    ).one()
    assert tuple(row) == (None, "regex")


@pytest.mark.asyncio(loop_scope="session")
async def test_classification_source_check(pg_conn):
    with pytest.raises(IntegrityError):
        async with pg_conn.begin_nested():
            await _entity(pg_conn, "SCHEMA TEST BAD SOURCE", classification_source="llm")


@pytest.mark.asyncio(loop_scope="session")
async def test_ec_checks_reject_bad_values(pg_conn):
    eid = await _entity(pg_conn, "SCHEMA TEST EC CHECKS")
    for bad in (
        {"source": "llm", "entity_type": "person"},
        {"source": "jev", "entity_type": "household"},
        {"source": "jev", "entity_type": "organization", "entity_subtype": "club"},
    ):
        with pytest.raises(IntegrityError):
            async with pg_conn.begin_nested():
                await pg_conn.execute(insert(entity_classifications).values(entity_id=eid, **bad))
    # not_a_name is a valid stored answer
    await pg_conn.execute(
        insert(entity_classifications).values(entity_id=eid, source="jev", entity_type="not_a_name")
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_delete_cascades_classifications_and_flags(pg_conn):
    eid = await _entity(pg_conn, "SCHEMA TEST CASCADE")
    await pg_conn.execute(
        insert(entity_classifications).values(
            entity_id=eid, source="regex", entity_type="organization", entity_subtype="business"
        )
    )
    await pg_conn.execute(
        insert(entity_review_flags).values(entity_id=eid, flagged_by="a@example.com", reason="x")
    )
    await pg_conn.execute(entities.delete().where(entities.c.id == eid))
    for table in (entity_classifications, entity_review_flags):
        n = (
            await pg_conn.execute(select(func.count()).where(table.c.entity_id == eid))
        ).scalar_one()
        assert n == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_runs_origin_check(pg_conn):
    with pytest.raises(IntegrityError):
        async with pg_conn.begin_nested():
            await pg_conn.execute(
                insert(classification_runs).values(origin="weekly", model="jev-1.14.0")
            )
    rid = (
        await pg_conn.execute(
            insert(classification_runs)
            .values(origin="cli", model="jev-1.14.0")
            .returning(classification_runs.c.id)
        )
    ).scalar_one()
    row = (
        await pg_conn.execute(
            select(
                classification_runs.c.batches_ok,
                classification_runs.c.batches_failed,
                classification_runs.c.input_tokens,
                classification_runs.c.started_at,
            ).where(classification_runs.c.id == rid)
        )
    ).one()
    assert row[:3] == (0, 0, 0)
    assert row[3] is not None


# An organization carries a subtype; a person, the legacy '' and not_a_name do not (#212).
_ENTITY_PAIRS_REJECTED = [("organization", None), ("person", "business"), ("", "trust")]
_ENTITY_PAIRS_ACCEPTED = [("organization", "unknown"), ("person", None), ("", None)]
_EC_PAIRS_REJECTED = [("organization", None), ("person", "business"), ("not_a_name", "unknown")]
_EC_PAIRS_ACCEPTED = [("organization", "business"), ("person", None), ("not_a_name", None)]


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(("entity_type", "subtype"), _ENTITY_PAIRS_REJECTED)
async def test_entity_subtype_must_match_type(pg_conn, entity_type, subtype):
    with pytest.raises(IntegrityError, match="ck_entities_subtype_matches_type"):
        async with pg_conn.begin_nested():
            await _entity(
                pg_conn, f"SCHEMA TEST PAIR {entity_type!r}", entity_type, entity_subtype=subtype
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_consistent_pairs_accepted(pg_conn):
    for i, (entity_type, subtype) in enumerate(_ENTITY_PAIRS_ACCEPTED):
        await _entity(pg_conn, f"SCHEMA TEST GOOD PAIR {i}", entity_type, entity_subtype=subtype)


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_type_change_must_move_subtype_in_same_statement(pg_conn):
    """A writer that sets type and subtype in two statements trips the CHECK in between."""
    eid = await _entity(pg_conn, "SCHEMA TEST SPLIT WRITE", "organization", entity_subtype="trust")
    with pytest.raises(IntegrityError, match="ck_entities_subtype_matches_type"):
        async with pg_conn.begin_nested():
            await pg_conn.execute(
                entities.update().where(entities.c.id == eid).values(entity_type="person")
            )
    await pg_conn.execute(
        entities.update()
        .where(entities.c.id == eid)
        .values(entity_type="person", entity_subtype=None)
    )


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(("entity_type", "subtype"), _EC_PAIRS_REJECTED)
async def test_ec_subtype_must_match_type(pg_conn, entity_type, subtype):
    eid = await _entity(pg_conn, f"SCHEMA TEST EC PAIR {entity_type}")
    with pytest.raises(IntegrityError, match="ck_ec_subtype_matches_type"):
        async with pg_conn.begin_nested():
            await pg_conn.execute(
                insert(entity_classifications).values(
                    entity_id=eid, source="jev", entity_type=entity_type, entity_subtype=subtype
                )
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_ec_consistent_pairs_accepted(pg_conn):
    eid = await _entity(pg_conn, "SCHEMA TEST EC GOOD PAIRS")
    await pg_conn.execute(
        insert(entity_classifications),
        [
            {"entity_id": eid, "source": "jev", "entity_type": t, "entity_subtype": s}
            for t, s in _EC_PAIRS_ACCEPTED
        ],
    )
