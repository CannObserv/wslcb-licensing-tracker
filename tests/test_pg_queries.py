"""Tests for pg_queries.py — async search and read queries."""
import pytest
from wslcb_licensing_tracker.pg_queries import (
    search_records,
    export_records,
    export_records_cursor,
    get_filter_options,
    get_cities_for_state,
    get_stats,
    enrich_record,
    get_record_by_id,
    get_related_records,
    get_entity_records,
    get_entities,
    get_source_by_id,
    get_record_source_link,
    invalidate_filter_cache,
)
from wslcb_licensing_tracker.pg_db import (
    SOURCE_TYPE_LIVE_SCRAPE,
    get_or_create_source,
    link_record_source,
)
from wslcb_licensing_tracker.pg_pipeline import insert_record


class TestEnrichRecord:
    """Pure Python — no DB."""
    def test_adds_display_fields(self):
        record = {"std_city": "SEATTLE", "city": "seattle",
                  "std_postal_code": "98101", "zip_code": "98101",
                  "prev_std_city": "", "previous_city": "TACOMA",
                  "prev_std_postal_code": "", "previous_zip_code": "98402"}
        enriched = enrich_record(record)
        assert enriched["display_city"] == "SEATTLE"
        assert enriched["display_previous_city"] == "TACOMA"


class TestSearchRecords:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_tuple_list_int(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_001"
        await insert_record(pg_conn, standard_new_application)
        records, total = await search_records(pg_conn)
        assert isinstance(records, list)
        assert isinstance(total, int)
        assert total >= 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_query_filter_fts(self, pg_conn, standard_new_application):
        """Text query finds records via tsvector full-text search."""
        standard_new_application["license_number"] = "query_002"
        standard_new_application["business_name"] = "UNIQUE FTS SEARCH CORP"
        await insert_record(pg_conn, standard_new_application)
        records, total = await search_records(pg_conn, query="UNIQUE FTS SEARCH")
        assert total >= 1
        assert any("UNIQUE FTS SEARCH" in r["business_name"] for r in records)
        # FTS result should appear first (ts_rank ordering)
        assert "UNIQUE FTS SEARCH" in records[0]["business_name"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_query_filter_trgm_fuzzy(self, pg_conn, standard_new_application):
        """Partial/non-English business names are found via pg_trgm similarity."""
        standard_new_application["license_number"] = "query_002b"
        standard_new_application["business_name"] = "XYZNOTAWORD HOLDINGS LLC"
        await insert_record(pg_conn, standard_new_application)
        # Partial query: plainto_tsquery('english', 'XYZNOTAWO') produces lexeme
        # 'xyznotawo', which won't match the stored 'xyznotaword' lexeme in
        # search_vector — so only the pg_trgm % path can hit this record.
        records, total = await search_records(pg_conn, query="XYZNOTAWO")
        assert total >= 1
        assert any("XYZNOTAWORD" in r["business_name"] for r in records)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_section_type_filter(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_003"
        await insert_record(pg_conn, standard_new_application)
        records, total = await search_records(pg_conn, section_type="new_application")
        assert all(r["section_type"] == "new_application" for r in records)


class TestExportRecords:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_list_of_dicts(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_004"
        await insert_record(pg_conn, standard_new_application)
        results = await export_records(pg_conn)
        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)
        assert all("business_name" in r for r in results)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_export_cursor_is_async_generator(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_005"
        await insert_record(pg_conn, standard_new_application)
        rows = []
        async for row in export_records_cursor(pg_conn):
            rows.append(row)
        assert len(rows) >= 1


class TestGetStats:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_stats_dict(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_006"
        await insert_record(pg_conn, standard_new_application)
        stats = await get_stats(pg_conn)
        assert "total_records" in stats
        assert stats["total_records"] >= 1

class TestGetRecordById:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetches_existing(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_007"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        record = await get_record_by_id(pg_conn, record_id)
        assert record is not None
        assert record["id"] == record_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_for_missing(self, pg_conn):
        record = await get_record_by_id(pg_conn, 999999999)
        assert record is None


class TestGetRelatedRecords:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_list(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_008"
        result = await insert_record(pg_conn, standard_new_application)
        record = await get_record_by_id(pg_conn, result[0])
        related = await get_related_records(pg_conn, record)
        assert isinstance(related, list)


class TestGetEntityRecords:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_list(self, pg_conn):
        # entity_id 999999999 simply returns empty list
        records = await get_entity_records(pg_conn, 999999999)
        assert isinstance(records, list)
        assert records == []


class TestGetSourceById:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetches_existing(self, pg_conn):
        source_id = await get_or_create_source(
            pg_conn,
            source_type_id=SOURCE_TYPE_LIVE_SCRAPE,
            url="https://example.com/test-source-by-id",
        )
        row = await get_source_by_id(pg_conn, source_id)
        assert row is not None
        assert row["id"] == source_id
        assert row["source_type"] == "live_scrape"
        assert "source_label" in row
        assert "snapshot_path" in row
        assert "url" in row
        assert "captured_at" in row
        assert "metadata" in row

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_none_for_missing(self, pg_conn):
        row = await get_source_by_id(pg_conn, 999999999)
        assert row is None


class TestGetRecordSourceLink:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_true_when_linked(self, pg_conn, standard_new_application):
        standard_new_application["license_number"] = "query_rsl_001"
        result = await insert_record(pg_conn, standard_new_application)
        record_id = result[0]
        source_id = await get_or_create_source(
            pg_conn,
            source_type_id=SOURCE_TYPE_LIVE_SCRAPE,
            url="https://example.com/test-record-source-link",
        )
        await link_record_source(pg_conn, record_id, source_id)
        assert await get_record_source_link(pg_conn, record_id, source_id) is True

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_when_not_linked(self, pg_conn):
        assert await get_record_source_link(pg_conn, 999999999, 999999999) is False
