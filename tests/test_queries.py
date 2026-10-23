"""Tests for queries_* modules — async search and read queries."""

import json
from datetime import UTC, datetime

import pytest
from fastapi.encoders import jsonable_encoder
from sqlalchemy import insert

from wslcb_licensing_tracker.db import (
    SOURCE_TYPE_LIVE_SCRAPE,
    get_or_create_source,
    link_record_source,
)
from wslcb_licensing_tracker.entities import get_or_create_entity
from wslcb_licensing_tracker.entity_classification import resolve_entities
from wslcb_licensing_tracker.models import entity_classifications, scrape_log
from wslcb_licensing_tracker.pipeline import insert_record
from wslcb_licensing_tracker.queries_entity import get_entities, get_entity_records
from wslcb_licensing_tracker.queries_export import export_records, export_records_cursor
from wslcb_licensing_tracker.queries_hydrate import enrich_record
from wslcb_licensing_tracker.queries_search import (
    get_record_by_id,
    get_record_source_link,
    get_related_records,
    get_source_by_id,
    search_records,
)
from wslcb_licensing_tracker.queries_stats import get_stats


class TestEnrichRecord:
    """Pure Python — no DB."""

    def test_adds_display_fields(self):
        record = {
            "std_city": "SEATTLE",
            "city": "seattle",
            "std_postal_code": "98101",
            "zip_code": "98101",
            "prev_std_city": "",
            "previous_city": "TACOMA",
            "prev_std_postal_code": "",
            "previous_zip_code": "98402",
        }
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

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stats_survive_the_api_encoder(self, pg_conn, standard_new_application):
        """Every value get_stats returns must reach JSON through /api/v1/stats (#170).

        Bound to the real schema rather than a mocked stats dict: last_scrape is
        SELECT * over scrape_log, so a column added there — or a Decimal from a
        future aggregate — reintroduces the 500 that route-level mocks cannot see.
        """
        started = datetime(2026, 8, 25, 13, 33, 28, tzinfo=UTC)
        finished = datetime(2026, 8, 25, 13, 33, 52, tzinfo=UTC)
        standard_new_application["license_number"] = "query_006b"
        await insert_record(pg_conn, standard_new_application)
        await pg_conn.execute(
            scrape_log.insert().values(
                started_at=started,
                finished_at=finished,
                status="success",
                records_new=191,
            )
        )

        stats = await get_stats(pg_conn)

        assert stats["last_scrape"]["records_new"] == 191
        encoded = json.dumps(jsonable_encoder(stats))
        assert "2026-08-25T13:33:28" in encoded


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
        assert row["url"] == "https://example.com/test-source-by-id"
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


async def _seed_jev(
    conn, eid: int, entity_type: str, subtype: str | None, conf: float, **kw
) -> None:
    await conn.execute(
        insert(entity_classifications).values(
            entity_id=eid,
            source="jev",
            entity_type=entity_type,
            entity_subtype=subtype,
            confidence=conf,
            model="jev-1.14.0",
            **kw,
        )
    )


class TestGetEntities:
    """Subtype filter and provenance fields (#209). Scoped by a ``T209`` name prefix."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_entities_subtype_filter(self, pg_conn):
        trust = await get_or_create_entity(pg_conn, "T209 SMITH FAMILY TRUST")
        await get_or_create_entity(pg_conn, "T209 ACME LLC")
        await get_or_create_entity(pg_conn, "T209 JOHN DOE")
        result = await get_entities(pg_conn, q="T209", entity_subtype="trust")
        assert result["total"] == 1
        assert [e["id"] for e in result["entities"]] == [trust]
        assert result["entities"][0]["entity_subtype"] == "trust"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_entities_bogus_subtype_ignored(self, pg_conn):
        for name in ("T209 SMITH FAMILY TRUST", "T209 ACME LLC", "T209 JOHN DOE"):
            await get_or_create_entity(pg_conn, name)
        unfiltered = await get_entities(pg_conn, q="T209")
        bogus = await get_entities(pg_conn, q="T209", entity_subtype="bogus")
        assert unfiltered["total"] == 3
        assert bogus["total"] == unfiltered["total"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_person_with_business_subtype_is_empty(self, pg_conn):
        await get_or_create_entity(pg_conn, "T209 ACME LLC")
        await get_or_create_entity(pg_conn, "T209 JOHN DOE")
        result = await get_entities(
            pg_conn, q="T209", entity_type="person", entity_subtype="business"
        )
        assert result == {"entities": [], "total": 0}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_rows_carry_source_and_jev_confidence(self, pg_conn, monkeypatch):
        monkeypatch.delenv("CO_WSLCB_LICENSING_TRACKER_JEV_CONFIDENCE_THRESHOLD", raising=False)
        person = await get_or_create_entity(pg_conn, "T209 JOHN DOE")
        tribe = await get_or_create_entity(pg_conn, "T209 LOWER ELWHA TRIBE")
        acme = await get_or_create_entity(pg_conn, "T209 ACME LLC")
        # Older and ignored rows never surface; the latest non-ignored one does.
        await _seed_jev(pg_conn, tribe, "organization", "business", 0.5)
        await _seed_jev(pg_conn, tribe, "organization", "government", 0.92)
        await _seed_jev(pg_conn, tribe, "person", None, 0.99, ignored_at=datetime.now(UTC))
        await _seed_jev(pg_conn, acme, "organization", "business", 0.4)  # below threshold
        await resolve_entities(pg_conn, [person, tribe, acme])

        rows = {e["id"]: e for e in (await get_entities(pg_conn, q="T209"))["entities"]}
        assert rows[person]["entity_subtype"] is None
        assert rows[person]["classification_source"] == "regex"
        assert rows[person]["jev_confidence"] is None
        assert rows[person]["jev_subtype"] is None
        # Jev wins the type; its untrusted subtype falls back to the regex's.
        assert rows[tribe]["classification_source"] == "jev"
        assert rows[tribe]["entity_subtype"] == "tribe"
        assert rows[tribe]["jev_confidence"] == pytest.approx(0.92)
        assert rows[tribe]["jev_subtype"] == "government"
        # A losing Jev answer still reports its confidence.
        assert rows[acme]["classification_source"] == "regex"
        assert rows[acme]["jev_confidence"] == pytest.approx(0.4)
