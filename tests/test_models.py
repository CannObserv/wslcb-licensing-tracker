"""Tests for SQLAlchemy table metadata definitions."""

from wslcb_licensing_tracker import models


EXPECTED_TABLES = {
    "locations",
    "license_endorsements",
    "endorsement_codes",
    "scrape_log",
    "source_types",
    "sources",
    "license_records",
    "record_endorsements",
    "entities",
    "record_entities",
    "record_links",
    "record_enrichments",
    "record_sources",
    "admin_users",
    "admin_audit_log",
    "endorsement_aliases",
    "endorsement_dismissed_suggestions",
    "regulated_substances",
    "regulated_substance_endorsements",
    "data_migrations",
}


def test_all_tables_present():
    """metadata contains all expected table names."""
    assert set(models.metadata.tables.keys()) == EXPECTED_TABLES


def test_locations_columns():
    """locations table has all expected columns."""
    cols = {c.name for c in models.locations.c}
    assert "id" in cols
    assert "raw_address" in cols
    assert "city" in cols
    assert "std_region" in cols
    assert "std_postal_code" in cols
    assert "latitude" in cols


def test_license_records_columns():
    """license_records has unique constraint columns."""
    cols = {c.name for c in models.license_records.c}
    assert {"section_type", "record_date", "license_number", "application_type"} <= cols
    assert "resolved_endorsements" in cols
    assert "has_additional_names" in cols


def test_sources_metadata_column():
    """sources.metadata column is present."""
    cols = {c.name for c in models.sources.c}
    assert "metadata" in cols


def test_data_migrations_columns():
    """data_migrations table has id, name, applied_at."""
    cols = {c.name for c in models.data_migrations.c}
    assert cols == {"id", "name", "applied_at"}
