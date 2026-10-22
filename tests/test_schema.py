"""Tests for PostgreSQL schema initialization.

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
init_db is also exercised by the pg_engine fixture itself, which calls it
to migrate the test database. Also pins the entity-classification constraints,
defaults and cascades from migration 0009 (#206).
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


async def _entity(conn, name: str, **values) -> int:
    return (
        await conn.execute(
            insert(entities)
            .values(name=name, entity_type="organization", **values)
            .returning(entities.c.id)
        )
    ).scalar_one()


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_subtype_check_rejects_unknown_value(pg_conn):
    with pytest.raises(IntegrityError):
        async with pg_conn.begin_nested():
            await _entity(pg_conn, "SCHEMA TEST BOGUS SUBTYPE", entity_subtype="bogus")


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
