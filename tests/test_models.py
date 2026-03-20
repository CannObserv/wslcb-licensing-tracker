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


def test_record_endorsements_fk_cascade():
    """record_endorsements FKs have ON DELETE CASCADE."""
    for col_name in ("record_id", "endorsement_id"):
        col = models.record_endorsements.c[col_name]
        fk = list(col.foreign_keys)[0]
        assert fk.ondelete.upper() == "CASCADE", f"{col_name} FK missing CASCADE"


def test_record_sources_fk_cascade():
    """record_sources FKs have ON DELETE CASCADE."""
    for col_name in ("record_id", "source_id"):
        col = models.record_sources.c[col_name]
        fk = list(col.foreign_keys)[0]
        assert fk.ondelete.upper() == "CASCADE", f"{col_name} FK missing CASCADE"


def test_license_records_unique_constraint_name():
    """license_records natural key unique constraint has the expected name."""
    constraint_names = {c.name for c in models.license_records.constraints}
    assert "uq_license_records_natural_key" in constraint_names
