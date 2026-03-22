"""Tests for PostgreSQL database helpers (pg_db.py).

Requires TEST_DATABASE_URL env var pointing at a running PostgreSQL instance.
"""

import pytest
from datetime import UTC, datetime
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from wslcb_licensing_tracker.models import license_records, locations, record_sources, scrape_log, source_types, sources
from wslcb_licensing_tracker.pg_db import (
    get_or_create_location,
    get_or_create_source,
    get_primary_source,
    get_record_sources,
    link_record_source,
)


class TestPgGetOrCreateLocation:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_new_location(self, pg_conn):
        """Creates a new location and returns its ID."""
        loc_id = await get_or_create_location(pg_conn, "123 MAIN ST, SEATTLE, WA 98101")
        assert loc_id is not None
        assert isinstance(loc_id, int)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_same_id_for_duplicate(self, pg_conn):
        """Returns existing ID when same raw_address is inserted again."""
        id1 = await get_or_create_location(pg_conn, "456 ELM AVE, TACOMA, WA 98402")
        id2 = await get_or_create_location(pg_conn, "456 ELM AVE, TACOMA, WA 98402")
        assert id1 == id2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_for_empty(self, pg_conn):
        """Returns None for empty/None/whitespace-only addresses."""
        assert await get_or_create_location(pg_conn, "") is None
        assert await get_or_create_location(pg_conn, None) is None
        assert await get_or_create_location(pg_conn, "   ") is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_nbsp_normalization(self, pg_conn):
        """Non-breaking spaces are normalized to regular spaces."""
        id1 = await get_or_create_location(pg_conn, "100\xa0MAIN\xa0ST")
        id2 = await get_or_create_location(pg_conn, "100 MAIN ST")
        assert id1 == id2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stores_city_state_zip(self, pg_conn):
        """City, state, and zip_code are stored correctly."""
        loc_id = await get_or_create_location(
            pg_conn, "789 OAK DR, OLYMPIA, WA 98501",
            city="OLYMPIA", state="WA", zip_code="98501",
        )
        result = await pg_conn.execute(
            select(locations.c.city, locations.c.state, locations.c.zip_code)
            .where(locations.c.id == loc_id)
        )
        row = result.one()
        assert row.city == "OLYMPIA"
        assert row.state == "WA"
        assert row.zip_code == "98501"


class TestPgGetOrCreateSource:
    async def _seed_source_type(self, pg_conn):
        """Insert the live_scrape source type (id=1) and scrape_log rows for tests."""
        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        # Seed scrape_log rows needed by scrape_log_id FK
        for sl_id in (10, 20):
            await pg_conn.execute(
                pg_insert(scrape_log)
                .values(id=sl_id, started_at=datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC))
                .on_conflict_do_nothing()
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_creates_source(self, pg_conn):
        """Creates a source row and returns its ID."""
        await self._seed_source_type(pg_conn)
        src_id = await get_or_create_source(
            pg_conn, source_type_id=1,
            snapshot_path="data/wslcb/2025/2025-06-15/page.html",
        )
        assert isinstance(src_id, int)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_idempotent_with_path(self, pg_conn):
        """Same (type, path) returns same ID."""
        await self._seed_source_type(pg_conn)
        path = "data/wslcb/2025/2025-06-15/idempotent.html"
        id1 = await get_or_create_source(pg_conn, source_type_id=1, snapshot_path=path)
        id2 = await get_or_create_source(pg_conn, source_type_id=1, snapshot_path=path)
        assert id1 == id2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_null_path_with_scrape_log_id(self, pg_conn):
        """NULL snapshot_path with different scrape_log_ids → distinct rows."""
        await self._seed_source_type(pg_conn)
        id1 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=10)
        id2 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=20)
        assert id1 != id2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_null_path_same_scrape_log_id(self, pg_conn):
        """NULL snapshot_path with same scrape_log_id → same row."""
        await self._seed_source_type(pg_conn)
        id1 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=10)
        id2 = await get_or_create_source(pg_conn, source_type_id=1, scrape_log_id=10)
        assert id1 == id2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_null_path_null_scrape_log(self, pg_conn):
        """NULL snapshot_path + NULL scrape_log_id → same row on repeat."""
        await self._seed_source_type(pg_conn)
        id1 = await get_or_create_source(pg_conn, source_type_id=1)
        id2 = await get_or_create_source(pg_conn, source_type_id=1)
        assert id1 == id2


class TestPgLinkRecordSource:
    async def _seed_data(self, pg_conn):
        """Create a source type, source, and minimal license record for linking tests."""
        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        result = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/link-seed.html")
            .on_conflict_do_nothing()
            .returning(sources.c.id)
        )
        row = result.first()
        if row:
            source_id = row[0]
        else:
            r = await pg_conn.execute(
                select(sources.c.id).where(sources.c.snapshot_path == "test/link-seed.html")
            )
            source_id = r.scalar_one()

        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="999991",
                application_type="NEW APPLICATION",
                scraped_at=datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC),
            )
            .on_conflict_do_nothing()
            .returning(license_records.c.id)
        )
        row = result.first()
        if row:
            record_id = row[0]
        else:
            r = await pg_conn.execute(
                select(license_records.c.id)
                .where(license_records.c.license_number == "999991")
            )
            record_id = r.scalar_one()

        return record_id, source_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_link_and_idempotent(self, pg_conn):
        """Linking is idempotent — second call does not raise."""
        record_id, source_id = await self._seed_data(pg_conn)
        await link_record_source(pg_conn, record_id, source_id, "first_seen")
        await link_record_source(pg_conn, record_id, source_id, "first_seen")

        result = await pg_conn.execute(
            select(record_sources)
            .where(record_sources.c.record_id == record_id)
        )
        assert len(result.all()) == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_multiple_roles(self, pg_conn):
        """Same record+source with different roles creates distinct rows."""
        record_id, source_id = await self._seed_data(pg_conn)
        await link_record_source(pg_conn, record_id, source_id, "first_seen")
        await link_record_source(pg_conn, record_id, source_id, "confirmed")

        result = await pg_conn.execute(
            select(record_sources)
            .where(record_sources.c.record_id == record_id)
        )
        assert len(result.all()) == 2


class TestPgGetPrimarySource:
    async def _seed_record(self, pg_conn):
        """Create a minimal license record."""
        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        await pg_conn.execute(
            pg_insert(source_types)
            .values(id=2, slug="co_archive", label="CO Archive")
            .on_conflict_do_nothing()
        )
        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="888881",
                application_type="NEW APPLICATION",
                scraped_at=datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC),
            )
            .on_conflict_do_nothing()
            .returning(license_records.c.id)
        )
        row = result.first()
        if row:
            return row[0]
        r = await pg_conn.execute(
            select(license_records.c.id).where(license_records.c.license_number == "888881")
        )
        return r.scalar_one()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_for_no_sources(self, pg_conn):
        """Returns None when record has no linked sources."""
        record_id = await self._seed_record(pg_conn)
        result = await get_primary_source(pg_conn, record_id)
        assert result is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_best_source(self, pg_conn):
        """Returns first_seen source over confirmed source."""
        record_id = await self._seed_record(pg_conn)

        r1 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/pri-first.html", captured_at=datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC))
            .returning(sources.c.id)
        )
        first_id = r1.scalar_one()

        r2 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=2, snapshot_path="test/pri-confirmed.html", captured_at=datetime(2025, 6, 15, 11, 0, 0, tzinfo=UTC))
            .returning(sources.c.id)
        )
        confirmed_id = r2.scalar_one()

        await link_record_source(pg_conn, record_id, confirmed_id, "confirmed")
        await link_record_source(pg_conn, record_id, first_id, "first_seen")

        best = await get_primary_source(pg_conn, record_id)
        assert best is not None
        assert best["role"] == "first_seen"
        assert best["id"] == first_id


class TestPgGetRecordSources:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_empty_list(self, pg_conn):
        """Returns empty list for record with no sources."""
        await pg_conn.execute(
            pg_insert(source_types).values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="777771",
                application_type="NEW APPLICATION",
                scraped_at=datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC),
            )
            .returning(license_records.c.id)
        )
        record_id = result.scalar_one()

        sources_list = await get_record_sources(pg_conn, record_id)
        assert sources_list == []

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_all_sources(self, pg_conn):
        """Returns all linked sources newest-first."""
        await pg_conn.execute(
            pg_insert(source_types).values(id=1, slug="live_scrape", label="Live Scrape")
            .on_conflict_do_nothing()
        )
        result = await pg_conn.execute(
            pg_insert(license_records)
            .values(
                section_type="new_application",
                record_date="2025-06-15",
                license_number="666661",
                application_type="NEW APPLICATION",
                scraped_at=datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC),
            )
            .returning(license_records.c.id)
        )
        record_id = result.scalar_one()

        r1 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/rs-old.html", captured_at=datetime(2025, 6, 14, 10, 0, 0, tzinfo=UTC))
            .returning(sources.c.id)
        )
        old_id = r1.scalar_one()

        r2 = await pg_conn.execute(
            pg_insert(sources)
            .values(source_type_id=1, snapshot_path="test/rs-new.html", captured_at=datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC))
            .returning(sources.c.id)
        )
        new_id = r2.scalar_one()

        await link_record_source(pg_conn, record_id, old_id, "first_seen")
        await link_record_source(pg_conn, record_id, new_id, "confirmed")

        results = await get_record_sources(pg_conn, record_id)
        assert len(results) == 2
        assert results[0]["id"] == new_id
        assert results[1]["id"] == old_id
        assert isinstance(results[0]["metadata"], dict)
