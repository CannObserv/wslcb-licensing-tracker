"""Tests for enrichment.py — one logic version per enrichment step (#267).

Ingest and the reprocess commands must stamp ``record_enrichments.version``
from the same constant, so a stamp says which logic produced a record's
derived rows. Requires ``config.TEST_DATABASE_URL_ENV`` for the PG tests.
"""

import pytest
from sqlalchemy import select

from wslcb_licensing_tracker.endorsements import reprocess_endorsements
from wslcb_licensing_tracker.enrichment import (
    RETIRED_STEPS,
    STEP_ADDRESS,
    STEP_ENDORSEMENTS,
    STEP_ENTITIES,
    STEP_OUTCOME_LINK,
    STEP_VERSIONS,
    stamp_enrichment,
)
from wslcb_licensing_tracker.entities import reprocess_entities
from wslcb_licensing_tracker.models import record_enrichments
from wslcb_licensing_tracker.pipeline import IngestOptions, ingest_record


def test_live_steps_have_versions():
    assert set(STEP_VERSIONS) == {STEP_ENDORSEMENTS, STEP_ENTITIES, STEP_OUTCOME_LINK}
    assert all(isinstance(v, int) and v >= 1 for v in STEP_VERSIONS.values())


def test_address_step_is_retired():
    """No code writes the address step since the SQLite stack went (#97)."""
    assert STEP_ADDRESS in RETIRED_STEPS
    assert STEP_ADDRESS not in STEP_VERSIONS


def test_entities_version_counts_entity_source():
    """#266 changed what entity extraction reads: that is v6 (#267)."""
    assert STEP_VERSIONS[STEP_ENTITIES] == 6


def test_step_modules_hold_no_version_of_their_own():
    """One source of truth: the old per-module constants are gone."""
    from wslcb_licensing_tracker import endorsements, entities

    assert not hasattr(entities, "_ENTITY_REPROCESS_VERSION")
    assert not hasattr(endorsements, "_ENDORSEMENT_REPROCESS_VERSION")


@pytest.mark.asyncio(loop_scope="session")
async def test_stamp_refuses_retired_or_unknown_step(pg_conn):
    with pytest.raises(ValueError, match="address"):
        await stamp_enrichment(pg_conn, 1, STEP_ADDRESS)
    with pytest.raises(ValueError, match="bogus"):
        await stamp_enrichment(pg_conn, 1, "bogus")


async def _versions(conn, record_id: int) -> dict[str, str]:
    rows = await conn.execute(
        select(record_enrichments.c.step, record_enrichments.c.version).where(
            record_enrichments.c.record_id == record_id
        )
    )
    return {row.step: row.version for row in rows}


@pytest.mark.asyncio(loop_scope="session")
async def test_ingest_stamps_current_version_per_step(pg_conn, standard_new_application):
    standard_new_application["license_number"] = "ENR267_INGEST"
    r = await ingest_record(pg_conn, standard_new_application, IngestOptions(link_outcomes=True))
    assert r is not None
    assert r.is_new is True

    assert await _versions(pg_conn, r.record_id) == {
        step: str(v) for step, v in STEP_VERSIONS.items()
    }


@pytest.mark.asyncio(loop_scope="session")
async def test_ingest_and_reprocess_stamp_the_same_version(pg_conn, standard_new_application):
    """Reprocessing a freshly ingested record leaves its stamps' versions unchanged."""
    standard_new_application["license_number"] = "ENR267_AGREE"
    r = await ingest_record(pg_conn, standard_new_application, IngestOptions(link_outcomes=False))
    assert r is not None
    after_ingest = await _versions(pg_conn, r.record_id)

    # Start from a stale stamp so the assertion proves reprocess writes, not keeps.
    await pg_conn.execute(
        record_enrichments.update()
        .where(record_enrichments.c.record_id == r.record_id)
        .values(version="0")
    )
    await reprocess_entities(pg_conn, record_id=r.record_id)
    await reprocess_endorsements(pg_conn, record_id=r.record_id)

    after_reprocess = await _versions(pg_conn, r.record_id)
    for step in (STEP_ENTITIES, STEP_ENDORSEMENTS):
        assert after_reprocess[step] == after_ingest[step] == str(STEP_VERSIONS[step])


@pytest.mark.asyncio(loop_scope="session")
async def test_reprocess_stamps_empty_license_type_like_ingest(pg_conn, standard_new_application):
    """Ingest stamps a record with no license_type; reprocess must too, or it stays behind."""
    standard_new_application["license_number"] = "ENR267_EMPTY_LT"
    standard_new_application["license_type"] = ""
    r = await ingest_record(pg_conn, standard_new_application, IngestOptions(link_outcomes=False))
    assert r is not None
    assert (await _versions(pg_conn, r.record_id))[STEP_ENDORSEMENTS] == str(
        STEP_VERSIONS[STEP_ENDORSEMENTS]
    )

    await pg_conn.execute(
        record_enrichments.update()
        .where(record_enrichments.c.record_id == r.record_id)
        .values(version="0")
    )
    result = await reprocess_endorsements(pg_conn, record_id=r.record_id)

    assert result["records_processed"] == 1
    assert (await _versions(pg_conn, r.record_id))[STEP_ENDORSEMENTS] == str(
        STEP_VERSIONS[STEP_ENDORSEMENTS]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_endorsement_backfill_stamps_current_version(pg_conn, standard_new_application):
    """Data migration 0004's backfill runs current logic, so it stamps the current version."""
    from wslcb_licensing_tracker.endorsements_seed import backfill
    from wslcb_licensing_tracker.pipeline import insert_record

    standard_new_application["license_number"] = "ENR267_BF_END"
    record_id, is_new = await insert_record(pg_conn, standard_new_application)
    assert is_new is True
    assert await _versions(pg_conn, record_id) == {}

    await backfill(pg_conn)

    assert (await _versions(pg_conn, record_id))[STEP_ENDORSEMENTS] == str(
        STEP_VERSIONS[STEP_ENDORSEMENTS]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_entity_backfill_stamps_current_version(pg_conn, standard_new_application):
    """Data migration 0005's backfill_entities runs current logic, so it stamps too."""
    from wslcb_licensing_tracker.entities import backfill_entities
    from wslcb_licensing_tracker.pipeline import insert_record

    standard_new_application["license_number"] = "ENR267_BF_ENT"
    record_id, is_new = await insert_record(pg_conn, standard_new_application)
    assert is_new is True

    await backfill_entities(pg_conn)

    assert (await _versions(pg_conn, record_id))[STEP_ENTITIES] == str(STEP_VERSIONS[STEP_ENTITIES])
