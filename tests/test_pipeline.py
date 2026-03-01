"""Tests for pipeline.py — unified ingestion pipeline.

All tests use in-memory SQLite via the ``db`` fixture.
"""
import pytest

from endorsements import seed_endorsements


# ── ingest_record ──────────────────────────────────────────────────


class TestIngestRecord:
    def test_inserts_new_record(self, db, standard_new_application):
        """ingest_record should insert a new record and return IngestResult."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, standard_new_application, opts)

        assert result is not None
        assert result.record_id is not None
        assert result.is_new is True

    def test_duplicate_returns_existing(self, db, standard_new_application):
        """Duplicate records should return is_new=False."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        r1 = ingest_record(db, standard_new_application, opts)
        r2 = ingest_record(db, standard_new_application, opts)

        assert r1.record_id == r2.record_id
        assert r1.is_new is True
        assert r2.is_new is False

    def test_processes_endorsements(self, db, standard_new_application):
        """New records should get endorsement links."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, standard_new_application, opts)

        endorsements = db.execute(
            "SELECT le.name FROM record_endorsements re "
            "JOIN license_endorsements le ON le.id = re.endorsement_id "
            "WHERE re.record_id = ?",
            (result.record_id,),
        ).fetchall()
        assert len(endorsements) > 0
        assert endorsements[0]["name"] == "CANNABIS RETAILER"

    def test_links_provenance(self, db, standard_new_application):
        """When source_id is provided, provenance should be linked."""
        from pipeline import ingest_record, IngestOptions
        from database import get_or_create_source, SOURCE_TYPE_LIVE_SCRAPE

        seed_endorsements(db)
        source_id = get_or_create_source(
            db, SOURCE_TYPE_LIVE_SCRAPE,
            snapshot_path="test/path.html",
            url="https://example.com",
            captured_at="2025-06-15T12:00:00+00:00",
        )
        opts = IngestOptions(
            validate_addresses=False, link_outcomes=False,
            source_id=source_id,
        )
        result = ingest_record(db, standard_new_application, opts)

        link = db.execute(
            "SELECT role FROM record_sources WHERE record_id = ? AND source_id = ?",
            (result.record_id, source_id),
        ).fetchone()
        assert link is not None
        assert link["role"] == "first_seen"

    def test_duplicate_gets_confirmed_provenance(self, db, standard_new_application):
        """Duplicate records should get 'confirmed' provenance role."""
        from pipeline import ingest_record, IngestOptions
        from database import get_or_create_source, SOURCE_TYPE_LIVE_SCRAPE

        seed_endorsements(db)
        source1 = get_or_create_source(
            db, SOURCE_TYPE_LIVE_SCRAPE,
            snapshot_path="test/path1.html",
            url="https://example.com",
            captured_at="2025-06-15T12:00:00+00:00",
        )
        source2 = get_or_create_source(
            db, SOURCE_TYPE_LIVE_SCRAPE,
            snapshot_path="test/path2.html",
            url="https://example.com",
            captured_at="2025-06-16T12:00:00+00:00",
        )
        opts1 = IngestOptions(
            validate_addresses=False, link_outcomes=False,
            source_id=source1,
        )
        opts2 = IngestOptions(
            validate_addresses=False, link_outcomes=False,
            source_id=source2,
        )
        ingest_record(db, standard_new_application, opts1)
        result2 = ingest_record(db, standard_new_application, opts2)

        link = db.execute(
            "SELECT role FROM record_sources WHERE record_id = ? AND source_id = ?",
            (result2.record_id, source2),
        ).fetchone()
        assert link is not None
        assert link["role"] == "confirmed"

    def test_links_entities(self, db, standard_new_application):
        """Entities should always be linked via insert_record."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, standard_new_application, opts)

        entities = db.execute(
            "SELECT e.name FROM record_entities re "
            "JOIN entities e ON e.id = re.entity_id "
            "WHERE re.record_id = ? ORDER BY re.position",
            (result.record_id,),
        ).fetchall()
        names = [e["name"] for e in entities]
        assert "JOHN DOE" in names
        assert "JANE SMITH" in names

    def test_outcome_linking(self, db, standard_new_application, approved_numeric_code):
        """When link_outcomes=True, outcomes should be linked."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        # Insert the approved record first
        approved = {
            **approved_numeric_code,
            "license_number": standard_new_application["license_number"],
            "application_type": "NEW APPLICATION",
            "record_date": "2025-06-17",  # 2 days after application
        }
        opts_no_link = IngestOptions(
            validate_addresses=False, link_outcomes=False,
        )
        ingest_record(db, approved, opts_no_link)

        # Now insert the application with linking enabled
        opts_link = IngestOptions(
            validate_addresses=False, link_outcomes=True,
        )
        result = ingest_record(db, standard_new_application, opts_link)

        link = db.execute(
            "SELECT outcome_id FROM record_links WHERE new_app_id = ?",
            (result.record_id,),
        ).fetchone()
        assert link is not None

    def test_none_on_error(self, db):
        """A completely empty/invalid record should return None."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, {}, opts)
        assert result is None


# ── ingest_batch ─────────────────────────────────────────────────────


class TestIngestBatch:
    def test_batch_inserts_multiple(self, db, standard_new_application, assumption_record):
        """ingest_batch should insert multiple records."""
        from pipeline import ingest_batch, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_batch(db, [standard_new_application, assumption_record], opts)

        assert result.inserted == 2
        assert result.skipped == 0
        assert result.errors == 0

    def test_batch_counts_duplicates(self, db, standard_new_application):
        """Duplicates in batch should be counted as skipped."""
        from pipeline import ingest_batch, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_batch(
            db,
            [standard_new_application, standard_new_application],
            opts,
        )

        assert result.inserted == 1
        assert result.skipped == 1

    def test_batch_returns_record_ids(self, db, standard_new_application, assumption_record):
        """ingest_batch should return ids of newly inserted records."""
        from pipeline import ingest_batch, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_batch(db, [standard_new_application, assumption_record], opts)

        assert len(result.record_ids) == 2
        assert all(isinstance(rid, int) for rid in result.record_ids)

    def test_batch_commits_periodically(self, db, standard_new_application):
        """Batch should commit at batch_size intervals."""
        from pipeline import ingest_batch, IngestOptions

        seed_endorsements(db)
        # Create 5 unique records
        records = []
        for i in range(5):
            rec = {**standard_new_application, "license_number": f"BATCH{i:02d}"}
            records.append(rec)

        opts = IngestOptions(
            validate_addresses=False, link_outcomes=False,
            batch_size=2,
        )
        result = ingest_batch(db, records, opts)
        assert result.inserted == 5
