"""Tests for PostgreSQL schema initialization.

Requires ``config.TEST_DATABASE_URL_ENV`` pointing at a running PostgreSQL instance.
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
    entity_alias_dismissals,
    entity_aliases,
    entity_classifications,
    entity_review_flags,
    record_entities,
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


# ── 0011 entity alias layer (#262) ──────────────────────────────────


async def _alias(conn, variant, canonical, **values):
    row = {"scope_license": None, "source": "rule", "rule": "format_key"} | values
    await conn.execute(
        insert(entity_aliases).values(variant_id=variant, canonical_id=canonical, **row)
    )


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    ("kwargs", "constraint"),
    [
        ({"same": True}, "ck_entity_aliases_not_self"),
        ({"source": "rule", "rule": None}, "ck_entity_aliases_rule_iff_rule_source"),
        ({"source": "manual", "rule": "format_key"}, "ck_entity_aliases_rule_iff_rule_source"),
        ({"source": "jev", "rule": None}, "ck_entity_aliases_source"),
    ],
)
async def test_entity_alias_checks(pg_conn, kwargs, constraint):
    a = await _entity(pg_conn, "SCHEMA TEST ALIAS A")
    b = await _entity(pg_conn, "SCHEMA TEST ALIAS B")
    same = kwargs.pop("same", False)
    with pytest.raises(IntegrityError, match=constraint):
        async with pg_conn.begin_nested():
            await _alias(pg_conn, a, a if same else b, **kwargs)


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_alias_unique_treats_null_scope_as_equal(pg_conn):
    a = await _entity(pg_conn, "SCHEMA TEST ALIAS U A")
    b = await _entity(pg_conn, "SCHEMA TEST ALIAS U B")
    c = await _entity(pg_conn, "SCHEMA TEST ALIAS U C")
    await _alias(pg_conn, a, b)
    with pytest.raises(IntegrityError, match="uq_entity_aliases_variant_scope_source"):
        async with pg_conn.begin_nested():
            await _alias(pg_conn, a, c)
    # a scoped row and a manual row for the same variant are separate keys
    await _alias(pg_conn, a, c, scope_license="355693")
    await _alias(pg_conn, a, c, source="manual", rule=None)


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_alias_dismissal_checks_and_unique(pg_conn):
    a = await _entity(pg_conn, "SCHEMA TEST DISMISS A")
    b = await _entity(pg_conn, "SCHEMA TEST DISMISS B")
    lo, hi = sorted((a, b))
    with pytest.raises(IntegrityError, match="ck_entity_alias_dismissals_ordered"):
        async with pg_conn.begin_nested():
            await pg_conn.execute(
                insert(entity_alias_dismissals).values(
                    entity_id_a=hi, entity_id_b=lo, dismissed_by="a@example.com"
                )
            )
    row = {"entity_id_a": lo, "entity_id_b": hi, "dismissed_by": "a@example.com"}
    await pg_conn.execute(insert(entity_alias_dismissals).values(**row))
    with pytest.raises(IntegrityError, match="uq_entity_alias_dismissals_pair_scope"):
        async with pg_conn.begin_nested():
            await pg_conn.execute(insert(entity_alias_dismissals).values(**row))


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_delete_cascades_alias_rows(pg_conn):
    a = await _entity(pg_conn, "SCHEMA TEST ALIAS CASCADE A")
    b = await _entity(pg_conn, "SCHEMA TEST ALIAS CASCADE B")
    lo, hi = sorted((a, b))
    await _alias(pg_conn, a, b)
    await pg_conn.execute(
        insert(entity_alias_dismissals).values(
            entity_id_a=lo, entity_id_b=hi, dismissed_by="a@example.com"
        )
    )
    await pg_conn.execute(entities.delete().where(entities.c.id == b))
    assert (
        await pg_conn.execute(
            select(func.count()).select_from(entity_aliases).where(entity_aliases.c.variant_id == a)
        )
    ).scalar_one() == 0
    assert (
        await pg_conn.execute(
            select(func.count())
            .select_from(entity_alias_dismissals)
            .where(entity_alias_dismissals.c.entity_id_a == lo)
        )
    ).scalar_one() == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_entities_link_key_column_and_index(pg_conn):
    cols = (
        await pg_conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'entities' AND column_name = 'link_key'"
            )
        )
    ).scalar_one_or_none()
    assert cols == "link_key"
    idx = (
        await pg_conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_entities_link_key'")
        )
    ).scalar_one()
    assert "(link_key)" in idx


# ── 0012 record_entities.party_id (#263) ────────────────────────────


async def _record(conn, standard_new_application, licence):
    from wslcb_licensing_tracker.pipeline import insert_record

    rec = standard_new_application | {"license_number": licence}
    return (await insert_record(conn, rec))[0]


async def _party(conn, rid, eid):
    return (
        await conn.execute(
            select(record_entities.c.party_id).where(
                (record_entities.c.record_id == rid) & (record_entities.c.entity_id == eid)
            )
        )
    ).scalar_one()


@pytest.mark.asyncio(loop_scope="session")
async def test_party_id_defaults_to_entity_id_on_insert(pg_conn, standard_new_application):
    rid = await _record(pg_conn, standard_new_application, "zq_0012_a")
    eid = await _entity(pg_conn, "SCHEMA TEST PARTY A")
    await pg_conn.execute(insert(record_entities).values(record_id=rid, entity_id=eid))
    assert await _party(pg_conn, rid, eid) == eid


@pytest.mark.asyncio(loop_scope="session")
async def test_party_id_kept_when_given_and_follows_entity_repoint(
    pg_conn, standard_new_application
):
    rid = await _record(pg_conn, standard_new_application, "zq_0012_b")
    a = await _entity(pg_conn, "SCHEMA TEST PARTY B1")
    b = await _entity(pg_conn, "SCHEMA TEST PARTY B2")
    c = await _entity(pg_conn, "SCHEMA TEST PARTY B3")
    await pg_conn.execute(insert(record_entities).values(record_id=rid, entity_id=a, party_id=c))
    assert await _party(pg_conn, rid, a) == c
    # a merge re-points entity_id: the stale party follows the new entity
    await pg_conn.execute(
        record_entities.update().where(record_entities.c.entity_id == a).values(entity_id=b)
    )
    assert await _party(pg_conn, rid, b) == b
    # an explicit party write alongside wins
    await pg_conn.execute(
        record_entities.update()
        .where(record_entities.c.entity_id == b)
        .values(entity_id=a, party_id=c)
    )
    assert await _party(pg_conn, rid, a) == c


@pytest.mark.asyncio(loop_scope="session")
async def test_party_id_not_null_and_indexed(pg_conn):
    nullable = (
        await pg_conn.execute(
            text(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_name = 'record_entities' AND column_name = 'party_id'"
            )
        )
    ).scalar_one()
    assert nullable == "NO"
    idx = (
        await pg_conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_re_party_record'")
        )
    ).scalar_one()
    assert "(party_id, record_id)" in idx


# ── 0013 entity splits (#269, spec #246 §1) ─────────────────────────


@pytest.mark.asyncio(loop_scope="session")
async def test_split_verdict_and_members_cascade_with_the_entity(pg_conn):
    from wslcb_licensing_tracker.models import entity_split_members, entity_splits

    eid = await _entity(pg_conn, "SCHEMA TEST A & B SPLIT")
    await pg_conn.execute(
        insert(entity_splits).values(combined_id=eid, verdict="split", created_by="a@example.com")
    )
    await pg_conn.execute(
        insert(entity_split_members).values(
            [
                {"combined_id": eid, "ordinal": 0, "member_name": "A SPLIT"},
                {"combined_id": eid, "ordinal": 1, "member_name": "B SPLIT"},
            ]
        )
    )
    await pg_conn.execute(entities.delete().where(entities.c.id == eid))
    left = (
        await pg_conn.execute(
            select(func.count())
            .select_from(entity_split_members)
            .where(entity_split_members.c.combined_id == eid)
        )
    ).scalar_one()
    assert left == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_split_verdict_is_split_or_keep(pg_conn):
    from wslcb_licensing_tracker.models import entity_splits

    eid = await _entity(pg_conn, "SCHEMA TEST VERDICT CHECK")
    with pytest.raises(IntegrityError):
        async with pg_conn.begin_nested():
            await pg_conn.execute(
                insert(entity_splits).values(combined_id=eid, verdict="maybe", created_by="x")
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_split_member_names_unique_per_verdict(pg_conn):
    from wslcb_licensing_tracker.models import entity_split_members, entity_splits

    eid = await _entity(pg_conn, "SCHEMA TEST DUP MEMBERS")
    await pg_conn.execute(
        insert(entity_splits).values(combined_id=eid, verdict="split", created_by="x")
    )
    await pg_conn.execute(
        insert(entity_split_members).values(combined_id=eid, ordinal=0, member_name="SAME")
    )
    with pytest.raises(IntegrityError):
        async with pg_conn.begin_nested():
            await pg_conn.execute(
                insert(entity_split_members).values(combined_id=eid, ordinal=1, member_name="SAME")
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_via_entity_id_never_the_linked_entity_and_indexed(pg_conn, standard_new_application):
    rid = await _record(pg_conn, standard_new_application, "zq_0013_a")
    eid = await _entity(pg_conn, "SCHEMA TEST VIA SELF")
    with pytest.raises(IntegrityError):
        async with pg_conn.begin_nested():
            await pg_conn.execute(
                insert(record_entities).values(record_id=rid, entity_id=eid, via_entity_id=eid)
            )
    idx = (
        await pg_conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_re_via'")
        )
    ).scalar_one()
    assert "(via_entity_id)" in idx and "IS NOT NULL" in idx


@pytest.mark.asyncio(loop_scope="session")
async def test_member_rows_cascade_with_the_combined_entity(pg_conn, standard_new_application):
    rid = await _record(pg_conn, standard_new_application, "zq_0013_b")
    combined = await _entity(pg_conn, "SCHEMA TEST C & D VIA")
    member = await _entity(pg_conn, "SCHEMA TEST C VIA")
    await pg_conn.execute(insert(record_entities).values(record_id=rid, entity_id=combined))
    await pg_conn.execute(
        insert(record_entities).values(
            record_id=rid, entity_id=member, via_entity_id=combined, position=1
        )
    )
    await pg_conn.execute(entities.delete().where(entities.c.id == combined))
    left = (
        await pg_conn.execute(
            select(func.count())
            .select_from(record_entities)
            .where(record_entities.c.record_id == rid)
        )
    ).scalar_one()
    assert left == 0
