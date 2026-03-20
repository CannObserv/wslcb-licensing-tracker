"""SQLAlchemy Core table definitions for the WSLCB licensing tracker (PostgreSQL).

All table metadata lives here. Modules that need table objects import them
directly: ``from wslcb_licensing_tracker.models import license_records``.

No ORM mappers — all Table objects, no declarative_base(). FTS columns
(search_vector) are added in Phase 5.
"""

from sqlalchemy import (
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
)

metadata = MetaData()

locations = Table(
    "locations",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("raw_address", Text, nullable=False),
    Column("city", Text, server_default="''"),
    Column("state", Text, server_default="'WA'"),
    Column("zip_code", Text, server_default="''"),
    Column("std_address_line_1", Text, server_default="''"),
    Column("std_address_line_2", Text, server_default="''"),
    Column("std_city", Text, server_default="''"),
    Column("std_region", Text, server_default="''"),
    Column("std_postal_code", Text, server_default="''"),
    Column("std_country", Text, server_default="''"),
    Column("std_address_string", Text),
    Column("validation_status", Text),
    Column("dpv_match_code", Text),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("address_standardized_at", Text),
    Column("address_validated_at", Text),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("raw_address", name="uq_locations_raw_address"),
    Index("idx_locations_city", "city"),
    Index("idx_locations_zip", "zip_code"),
    Index("idx_locations_std_city", "std_city"),
    Index("idx_locations_std_postal_code", "std_postal_code"),
)

license_endorsements = Table(
    "license_endorsements",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("name", name="uq_license_endorsements_name"),
)

endorsement_codes = Table(
    "endorsement_codes",
    metadata,
    Column("code", Text, nullable=False, primary_key=True),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
)

scrape_log = Table(
    "scrape_log",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("started_at", Text, nullable=False),
    Column("finished_at", Text),
    Column("status", Text, nullable=False, server_default="'running'"),
    Column("records_new", Integer, server_default="0"),
    Column("records_approved", Integer, server_default="0"),
    Column("records_discontinued", Integer, server_default="0"),
    Column("records_skipped", Integer, server_default="0"),
    Column("error_message", Text),
    Column("snapshot_path", Text),
    Column("content_hash", Text),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
)

source_types = Table(
    "source_types",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("slug", Text, nullable=False),
    Column("label", Text, nullable=False),
    Column("description", Text, server_default="''"),
    UniqueConstraint("slug", name="uq_source_types_slug"),
)

sources = Table(
    "sources",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column(
        "source_type_id",
        Integer,
        ForeignKey("source_types.id"),
        nullable=False,
    ),
    Column("snapshot_path", Text),
    Column("url", Text),
    Column("captured_at", Text),
    Column("ingested_at", Text, nullable=False, server_default="now()::text"),
    Column("scrape_log_id", Integer, ForeignKey("scrape_log.id")),
    Column("metadata", Text, nullable=False, server_default="'{}'"),
    UniqueConstraint("source_type_id", "snapshot_path", name="uq_sources_type_path"),
)

license_records = Table(
    "license_records",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("section_type", Text, nullable=False),
    Column("record_date", Text, nullable=False),
    Column("business_name", Text),
    Column("location_id", Integer, ForeignKey("locations.id")),
    Column("applicants", Text),
    Column("license_type", Text),
    Column("application_type", Text),
    Column("license_number", Text),
    Column("contact_phone", Text),
    Column("previous_business_name", Text, server_default="''"),
    Column("previous_applicants", Text, server_default="''"),
    Column("previous_location_id", Integer, ForeignKey("locations.id")),
    Column("raw_business_name", Text),
    Column("raw_previous_business_name", Text),
    Column("raw_applicants", Text),
    Column("raw_previous_applicants", Text),
    Column("has_additional_names", Integer, nullable=False, server_default="0"),
    Column("resolved_endorsements", Text, nullable=False, server_default="''"),
    Column("scraped_at", Text, nullable=False),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint(
        "section_type",
        "record_date",
        "license_number",
        "application_type",
        name="uq_license_records_natural_key",
    ),
    Index("idx_records_section", "section_type"),
    Index("idx_records_date", "record_date"),
    Index("idx_records_business", "business_name"),
    Index("idx_records_license_num", "license_number"),
    Index("idx_records_app_type", "application_type"),
    Index("idx_records_location", "location_id"),
    Index("idx_records_prev_location", "previous_location_id"),
)

record_endorsements = Table(
    "record_endorsements",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Index("idx_re_endorsement", "endorsement_id"),
)

entities = Table(
    "entities",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("entity_type", Text, nullable=False, server_default="''"),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("name", name="uq_entities_name"),
    # NOTE: migration creates this as a functional index: lower(name).
    # SQLAlchemy Core can't express functional indexes inline on Table objects,
    # so this plain Index intentionally differs from the migration DDL.
    # Do NOT "fix" this to match — alembic autogenerate will flag the
    # discrepancy but it is harmless and expected.
    Index("idx_entities_name_lower", "name"),
)

record_entities = Table(
    "record_entities",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "entity_id",
        Integer,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("role", Text, nullable=False, server_default="'applicant'", primary_key=True),
    Column("position", Integer, nullable=False, server_default="0"),
    Index("idx_re_entity", "entity_id"),
    Index("idx_re_role", "role"),
)

record_links = Table(
    "record_links",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column(
        "new_app_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "outcome_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "confidence",
        Text,
        nullable=False,
    ),
    Column("days_gap", Integer),
    Column("linked_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("new_app_id", "outcome_id", name="uq_record_links"),
    CheckConstraint("confidence IN ('high', 'medium', 'low')", name="ck_record_links_confidence"),
    Index("idx_record_links_new", "new_app_id"),
    Index("idx_record_links_outcome", "outcome_id"),
)

record_enrichments = Table(
    "record_enrichments",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("step", Text, nullable=False, primary_key=True),
    Column("completed_at", Text, nullable=False),
    Column("version", Text, nullable=False, server_default="'1'"),
)

record_sources = Table(
    "record_sources",
    metadata,
    Column(
        "record_id",
        Integer,
        ForeignKey("license_records.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "source_id",
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "role",
        Text,
        nullable=False,
        server_default="'first_seen'",
        primary_key=True,
    ),
    CheckConstraint(
        "role IN ('first_seen', 'confirmed', 'repaired')", name="ck_record_sources_role"
    ),
    Index("idx_rs_source", "source_id"),
)

admin_users = Table(
    "admin_users",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("email", Text, nullable=False),
    Column("role", Text, nullable=False, server_default="'admin'"),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    Column("created_by", Text, nullable=False, server_default="'system'"),
    UniqueConstraint("email", name="uq_admin_users_email"),
)

admin_audit_log = Table(
    "admin_audit_log",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("admin_email", Text, nullable=False),
    Column("action", Text, nullable=False),
    Column("target_type", Text, nullable=False),
    Column("target_id", Integer),
    Column("details", Text),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
)

endorsement_aliases = Table(
    "endorsement_aliases",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "canonical_endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("created_at", Text, nullable=False, server_default="now()::text"),
    Column("created_by", Text),
    UniqueConstraint("endorsement_id", name="uq_endorsement_aliases_endorsement"),
)

endorsement_dismissed_suggestions = Table(
    "endorsement_dismissed_suggestions",
    metadata,
    Column(
        "endorsement_id_a",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "endorsement_id_b",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column("dismissed_by", Text, nullable=False),
    Column("dismissed_at", Text, nullable=False, server_default="now()::text"),
    CheckConstraint(
        "endorsement_id_a < endorsement_id_b",
        name="ck_dismissed_suggestions_order",
    ),
)

regulated_substances = Table(
    "regulated_substances",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("display_order", Integer, nullable=False, server_default="0"),
    UniqueConstraint("name", name="uq_regulated_substances_name"),
)

regulated_substance_endorsements = Table(
    "regulated_substance_endorsements",
    metadata,
    Column(
        "substance_id",
        Integer,
        ForeignKey("regulated_substances.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "endorsement_id",
        Integer,
        ForeignKey("license_endorsements.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Index("idx_rse_endorsement", "endorsement_id"),
)

# Tracks completed data-only migrations (distinct from Alembic schema revisions).
# Replaces the unconditional startup repair calls — see design doc Phase 4 / issue #85.
data_migrations = Table(
    "data_migrations",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("applied_at", Text, nullable=False, server_default="now()::text"),
    UniqueConstraint("name", name="uq_data_migrations_name"),
)
