"""Tests for the async PostgreSQL ingestion pipeline (pg_pipeline.py).

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
"""

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.models import license_records, locations, record_enrichments, record_sources, source_types, sources
from wslcb_licensing_tracker.pg_pipeline import (
    BatchResult,
    IngestOptions,
    IngestResult,
    ingest_batch,
    ingest_record,
    insert_record,
)


class TestPgInsertRecord:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_inserts_new_record(self, pg_conn, standard_new_application):
        """Inserts a new record and returns (id, True)."""
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id, is_new = result
        assert is_new is True
        assert isinstance(record_id, int)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_duplicate_returns_false(self, pg_conn, standard_new_application):
        """Inserting the same record twice returns (id, False)."""
        result1 = await insert_record(pg_conn, standard_new_application)
        result2 = await insert_record(pg_conn, standard_new_application)
        assert result1 is not None
        assert result2 is not None
        assert result1[0] == result2[0]  # same ID
        assert result2[1] is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_name_cleaning(self, pg_conn, standard_new_application):
        """Business name is uppercased and cleaned."""
        # Use a name that ends with a stray dot (not a legitimate abbreviation like CO.)
        standard_new_application["business_name"] = "  acme cannabis shop.  "
        standard_new_application["license_number"] = "078002"
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(license_records.c.business_name)
            .where(license_records.c.id == record_id)
        )).one()
        assert row.business_name == "ACME CANNABIS SHOP"  # trailing dot stripped, uppercased

    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_location(self, pg_conn, standard_new_application):
        """insert_record creates a location row and links it."""
        standard_new_application["license_number"] = "078003"
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(license_records.c.location_id)
            .where(license_records.c.id == record_id)
        )).one()
        assert row.location_id is not None

        loc = (await pg_conn.execute(
            select(locations.c.raw_address)
            .where(locations.c.id == row.location_id)
        )).one()
        assert loc.raw_address == "123 MAIN ST, SEATTLE, WA 98101"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_preserves_raw_values(self, pg_conn, standard_new_application):
        """Raw shadow columns preserve the original (pre-cleaned) values."""
        # Use a name with a stray trailing dot (not a legit abbreviation) to verify cleaning
        standard_new_application["business_name"] = "acme cannabis shop."
        standard_new_application["license_number"] = "078004"
        result = await insert_record(pg_conn, standard_new_application)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(
                license_records.c.business_name,
                license_records.c.raw_business_name,
            ).where(license_records.c.id == record_id)
        )).one()
        assert row.business_name == "ACME CANNABIS SHOP"
        assert row.raw_business_name == "acme cannabis shop."

    @pytest.mark.asyncio(loop_scope="session")
    async def test_has_additional_names_flag(self, pg_conn):
        """has_additional_names is set to 1 when applicants contain the marker."""
        record = {
            "section_type": "new_application",
            "record_date": "2025-07-01",
            "business_name": "TEST BIZ",
            "business_location": "100 TEST ST",
            "applicants": "TEST BIZ; ADDITIONAL NAMES ON FILE",
            "license_type": "TEST",
            "application_type": "NEW APPLICATION",
            "license_number": "999001",
            "contact_phone": "",
            "city": "",
            "state": "WA",
            "zip_code": "",
            "previous_business_name": "",
            "previous_applicants": "",
            "previous_business_location": "",
            "previous_city": "",
            "previous_state": "",
            "previous_zip_code": "",
            "scraped_at": "2025-07-01T12:00:00+00:00",
        }
        result = await insert_record(pg_conn, record)
        assert result is not None
        record_id = result[0]

        row = (await pg_conn.execute(
            select(license_records.c.has_additional_names)
            .where(license_records.c.id == record_id)
        )).one()
        assert row.has_additional_names == 1


class TestPgIngestRecord:
    async def _seed_source(self, pg_conn):
        """Create source type + source row, return source_id."""
        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        result = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/ingest.html")
            .on_conflict_do_nothing()
            .returning(sources.c.id)
        )
        row = result.first()
        if row:
            return row[0]
        r = await pg_conn.execute(
            select(sources.c.id).where(sources.c.snapshot_path == "test/ingest.html")
        )
        return r.scalar_one()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_inserts_new_record(self, pg_conn, standard_new_application):
        """ingest_record inserts a new record."""
        options = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = await ingest_record(pg_conn, standard_new_application, options)
        assert result is not None
        assert result.is_new is True
        assert result.record_id is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_duplicate_confirmed_provenance(self, pg_conn, standard_new_application):
        """Duplicate record gets provenance linked as 'confirmed'."""
        source_id = await self._seed_source(pg_conn)
        options = IngestOptions(
            validate_addresses=False,
            link_outcomes=False,
            source_id=source_id,
        )
        r1 = await ingest_record(pg_conn, standard_new_application, options)
        r2 = await ingest_record(pg_conn, standard_new_application, options)
        assert r1.is_new is True
        assert r2.is_new is False
        assert r1.record_id == r2.record_id

        result = await pg_conn.execute(
            select(record_sources.c.role).where(
                record_sources.c.record_id == r2.record_id,
                record_sources.c.role == "confirmed",
            )
        )
        assert result.first() is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_links_provenance(self, pg_conn, standard_new_application):
        """New record gets provenance linked as 'first_seen'."""
        source_id = await self._seed_source(pg_conn)
        options = IngestOptions(
            validate_addresses=False,
            link_outcomes=False,
            source_id=source_id,
            source_role="first_seen",
        )
        r = await ingest_record(pg_conn, standard_new_application, options)
        assert r.is_new is True

        result = await pg_conn.execute(
            select(record_sources.c.role).where(
                record_sources.c.record_id == r.record_id,
                record_sources.c.role == "first_seen",
            )
        )
        assert result.first() is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_enrichment_tracked(self, pg_conn, standard_new_application):
        """Entity enrichment step is tracked for new records."""
        options = IngestOptions(validate_addresses=False, link_outcomes=False)
        r = await ingest_record(pg_conn, standard_new_application, options)
        assert r.is_new is True

        result = await pg_conn.execute(
            select(record_enrichments.c.step).where(
                record_enrichments.c.record_id == r.record_id,
            )
        )
        steps = [row[0] for row in result]
        assert "entities" in steps


@pytest.mark.asyncio(loop_scope="session")
async def test_pg_ingest_batch(pg_engine, standard_new_application):
    """ingest_batch processes multiple records and commits."""
    # ingest_batch commits internally, so we use pg_engine directly
    async with pg_engine.connect() as conn:
        await conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        await conn.commit()

    records = []
    for i in range(5):
        rec = dict(standard_new_application)
        rec["license_number"] = f"BATCH{i:04d}"
        records.append(rec)

    options = IngestOptions(
        validate_addresses=False,
        link_outcomes=False,
        batch_size=2,
    )

    async with pg_engine.connect() as conn:
        try:
            result = await ingest_batch(conn, records, options)
            assert result.inserted == 5
            assert result.skipped == 0
            assert result.errors == 0
            assert len(result.record_ids) == 5
        finally:
            # Clean up committed data
            await conn.execute(
                license_records.delete().where(
                    license_records.c.license_number.like("BATCH%")
                )
            )
            await conn.commit()
