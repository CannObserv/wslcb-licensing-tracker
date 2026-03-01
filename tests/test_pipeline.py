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


# ── Enrichment tracking ────────────────────────────────────────────


class TestEnrichmentTracking:
    def test_new_record_gets_endorsement_enrichment(self, db, standard_new_application):
        """After ingest, the endorsements step should be tracked."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, standard_new_application, opts)

        row = db.execute(
            "SELECT step, version FROM record_enrichments "
            "WHERE record_id = ? AND step = 'endorsements'",
            (result.record_id,),
        ).fetchone()
        assert row is not None
        assert row["step"] == "endorsements"
        assert row["version"] == "1"

    def test_new_record_gets_entities_enrichment(self, db, standard_new_application):
        """After ingest, the entities step should be tracked."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, standard_new_application, opts)

        row = db.execute(
            "SELECT step FROM record_enrichments "
            "WHERE record_id = ? AND step = 'entities'",
            (result.record_id,),
        ).fetchone()
        assert row is not None

    def test_outcome_link_tracked_when_enabled(self, db, standard_new_application, approved_numeric_code):
        """When link_outcomes=True and linking succeeds, it should be tracked."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        # Insert approved first
        approved = {
            **approved_numeric_code,
            "license_number": standard_new_application["license_number"],
            "application_type": "NEW APPLICATION",
            "record_date": "2025-06-17",
        }
        opts_no_link = IngestOptions(validate_addresses=False, link_outcomes=False)
        ingest_record(db, approved, opts_no_link)

        opts_link = IngestOptions(validate_addresses=False, link_outcomes=True)
        result = ingest_record(db, standard_new_application, opts_link)

        row = db.execute(
            "SELECT step FROM record_enrichments "
            "WHERE record_id = ? AND step = 'outcome_link'",
            (result.record_id,),
        ).fetchone()
        assert row is not None

    def test_duplicate_record_no_enrichment_tracking(self, db, standard_new_application):
        """Duplicate records should not add new enrichment rows."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        r1 = ingest_record(db, standard_new_application, opts)
        r2 = ingest_record(db, standard_new_application, opts)

        count = db.execute(
            "SELECT count(*) FROM record_enrichments WHERE record_id = ?",
            (r1.record_id,),
        ).fetchone()[0]
        # Exactly endorsements + entities from the first insert, not doubled
        assert count == 2
        # Verify no duplicates
        all_steps = db.execute(
            "SELECT step FROM record_enrichments WHERE record_id = ?",
            (r1.record_id,),
        ).fetchall()
        step_names = [r["step"] for r in all_steps]
        assert len(step_names) == len(set(step_names)), "Duplicate enrichment steps found"

    def test_record_enrichment_idempotent(self, db, standard_new_application):
        """Calling _record_enrichment twice for the same step should upsert, not duplicate."""
        from pipeline import ingest_record, IngestOptions, _record_enrichment

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, standard_new_application, opts)

        # Call again for a step that already exists
        _record_enrichment(db, result.record_id, "endorsements", "2")

        rows = db.execute(
            "SELECT version FROM record_enrichments "
            "WHERE record_id = ? AND step = 'endorsements'",
            (result.record_id,),
        ).fetchall()
        assert len(rows) == 1, "Should have exactly one row, not a duplicate"
        assert rows[0]["version"] == "2", "Version should be updated by the re-run"

    def test_find_unenriched_records(self, db, standard_new_application, assumption_record):
        """Query to find records missing a specific enrichment step."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        r1 = ingest_record(db, standard_new_application, opts)
        r2 = ingest_record(db, assumption_record, opts)

        # Delete one record's endorsement tracking to simulate partial enrichment
        db.execute(
            "DELETE FROM record_enrichments WHERE record_id = ? AND step = 'endorsements'",
            (r1.record_id,),
        )

        unenriched = db.execute(
            "SELECT id FROM license_records "
            "WHERE id NOT IN ("
            "  SELECT record_id FROM record_enrichments WHERE step = 'endorsements'"
            ")",
        ).fetchall()
        unenriched_ids = [r["id"] for r in unenriched]
        assert r1.record_id in unenriched_ids
        assert r2.record_id not in unenriched_ids


# ── Raw value preservation ─────────────────────────────────────────


class TestRawValuePreservation:
    def test_new_record_preserves_raw_values(self, db, standard_new_application):
        """Newly ingested records should store raw values before cleaning."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, standard_new_application, opts)

        row = db.execute(
            "SELECT raw_business_name, raw_applicants, "
            "       raw_previous_business_name, raw_previous_applicants "
            "FROM license_records WHERE id = ?",
            (result.record_id,),
        ).fetchone()
        # Raw values should match what was passed in
        assert row["raw_business_name"] == standard_new_application["business_name"]
        assert row["raw_applicants"] == standard_new_application["applicants"]

    def test_raw_differs_from_cleaned(self, db):
        """When cleaning modifies a value, raw and cleaned should differ."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        record = {
            "section_type": "new_application",
            "record_date": "2025-06-15",
            "business_name": "dirty name.",  # trailing dot gets cleaned
            "business_location": "123 MAIN ST, SEATTLE, WA 98101",
            "applicants": "dirty name.; JOHN DOE.",
            "license_type": "CANNABIS RETAILER",
            "application_type": "NEW APPLICATION",
            "license_number": "RAW001",
            "contact_phone": "",
            "city": "SEATTLE",
            "state": "WA",
            "zip_code": "98101",
            "previous_business_name": "",
            "previous_applicants": "",
            "previous_business_location": "",
            "previous_city": "",
            "previous_state": "",
            "previous_zip_code": "",
            "scraped_at": "2025-06-15T12:00:00+00:00",
        }
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, record, opts)

        row = db.execute(
            "SELECT business_name, raw_business_name, "
            "       applicants, raw_applicants "
            "FROM license_records WHERE id = ?",
            (result.record_id,),
        ).fetchone()
        # Raw preserves the original
        assert row["raw_business_name"] == "dirty name."
        # Cleaned is uppercased, dot stripped
        assert row["business_name"] == "DIRTY NAME"
        # Raw applicants preserves original
        assert row["raw_applicants"] == "dirty name.; JOHN DOE."

    def test_assumption_preserves_previous_raw(self, db, assumption_record):
        """ASSUMPTION records should preserve raw previous_* values."""
        from pipeline import ingest_record, IngestOptions

        seed_endorsements(db)
        opts = IngestOptions(validate_addresses=False, link_outcomes=False)
        result = ingest_record(db, assumption_record, opts)

        row = db.execute(
            "SELECT raw_previous_business_name, raw_previous_applicants "
            "FROM license_records WHERE id = ?",
            (result.record_id,),
        ).fetchone()
        assert row["raw_previous_business_name"] == assumption_record["previous_business_name"]
        assert row["raw_previous_applicants"] == assumption_record["previous_applicants"]
