"""Baseline PostgreSQL schema — equivalent to all 15 SQLite migrations.

Revision ID: 0001
Revises:
Create Date: 2026-03-20
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE locations (
            id          SERIAL PRIMARY KEY,
            raw_address TEXT NOT NULL,
            city        TEXT NOT NULL DEFAULT '',
            state       TEXT NOT NULL DEFAULT 'WA',
            zip_code    TEXT NOT NULL DEFAULT '',
            std_address_line_1      TEXT NOT NULL DEFAULT '',
            std_address_line_2      TEXT NOT NULL DEFAULT '',
            std_city                TEXT NOT NULL DEFAULT '',
            std_region              TEXT NOT NULL DEFAULT '',
            std_postal_code         TEXT NOT NULL DEFAULT '',
            std_country             TEXT NOT NULL DEFAULT '',
            std_address_string      TEXT,
            validation_status       TEXT,
            dpv_match_code          TEXT,
            latitude                DOUBLE PRECISION,
            longitude               DOUBLE PRECISION,
            address_standardized_at TEXT,
            address_validated_at    TEXT,
            created_at              TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_locations_raw_address UNIQUE (raw_address)
        )
    """)
    op.execute("CREATE INDEX idx_locations_city ON locations(city)")
    op.execute("CREATE INDEX idx_locations_zip ON locations(zip_code)")
    op.execute("CREATE INDEX idx_locations_std_city ON locations(std_city)")
    op.execute("CREATE INDEX idx_locations_std_postal_code ON locations(std_postal_code)")

    op.execute("""
        CREATE TABLE license_endorsements (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_license_endorsements_name UNIQUE (name)
        )
    """)

    op.execute("""
        CREATE TABLE endorsement_codes (
            code           TEXT    NOT NULL,
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            created_at     TEXT NOT NULL DEFAULT now()::text,
            PRIMARY KEY (code, endorsement_id)
        )
    """)

    op.execute("""
        CREATE TABLE scrape_log (
            id                    SERIAL PRIMARY KEY,
            started_at            TEXT NOT NULL,
            finished_at           TEXT,
            status                TEXT NOT NULL DEFAULT 'running',
            records_new           INTEGER DEFAULT 0,
            records_approved      INTEGER DEFAULT 0,
            records_discontinued  INTEGER DEFAULT 0,
            records_skipped       INTEGER DEFAULT 0,
            error_message         TEXT,
            snapshot_path         TEXT,
            content_hash          TEXT,
            created_at            TEXT NOT NULL DEFAULT now()::text
        )
    """)

    op.execute("""
        CREATE TABLE source_types (
            id          INTEGER PRIMARY KEY,
            slug        TEXT NOT NULL,
            label       TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            CONSTRAINT uq_source_types_slug UNIQUE (slug)
        )
    """)

    op.execute("""
        INSERT INTO source_types (id, slug, label, description) VALUES
            (1, 'live_scrape',     'Live Scrape',       'Direct scrape of the WSLCB licensing page'),
            (2, 'co_archive',      'CO Page Archive',   'Cannabis Observer archived HTML snapshots'),
            (3, 'internet_archive','Internet Archive',  'Wayback Machine snapshots'),
            (4, 'co_diff_archive', 'CO Diff Archive',   'Cannabis Observer diff-detected change snapshots'),
            (5, 'manual',          'Manual Entry',      'Manually entered or corrected records')
    """)

    op.execute("""
        CREATE TABLE sources (
            id             SERIAL PRIMARY KEY,
            source_type_id INTEGER NOT NULL REFERENCES source_types(id),
            snapshot_path  TEXT,
            url            TEXT,
            captured_at    TEXT,
            ingested_at    TEXT NOT NULL DEFAULT now()::text,
            scrape_log_id  INTEGER REFERENCES scrape_log(id),
            metadata       TEXT NOT NULL DEFAULT '{}',
            CONSTRAINT uq_sources_type_path UNIQUE (source_type_id, snapshot_path)
        )
    """)

    op.execute("""
        CREATE TABLE license_records (
            id                       SERIAL PRIMARY KEY,
            section_type             TEXT NOT NULL,
            record_date              TEXT NOT NULL,
            business_name            TEXT,
            location_id              INTEGER REFERENCES locations(id),
            applicants               TEXT,
            license_type             TEXT,
            application_type         TEXT,
            license_number           TEXT,
            contact_phone            TEXT,
            previous_business_name   TEXT NOT NULL DEFAULT '',
            previous_applicants      TEXT NOT NULL DEFAULT '',
            previous_location_id     INTEGER REFERENCES locations(id),
            raw_business_name        TEXT,
            raw_previous_business_name TEXT,
            raw_applicants           TEXT,
            raw_previous_applicants  TEXT,
            has_additional_names     INTEGER NOT NULL DEFAULT 0,
            resolved_endorsements    TEXT NOT NULL DEFAULT '',
            scraped_at               TEXT NOT NULL,
            created_at               TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_license_records_natural_key
                UNIQUE (section_type, record_date, license_number, application_type)
        )
    """)
    op.execute("CREATE INDEX idx_records_section ON license_records(section_type)")
    op.execute("CREATE INDEX idx_records_date ON license_records(record_date)")
    op.execute("CREATE INDEX idx_records_business ON license_records(business_name)")
    op.execute("CREATE INDEX idx_records_license_num ON license_records(license_number)")
    op.execute("CREATE INDEX idx_records_app_type ON license_records(application_type)")
    op.execute("CREATE INDEX idx_records_location ON license_records(location_id)")
    op.execute("CREATE INDEX idx_records_prev_location ON license_records(previous_location_id)")

    op.execute("""
        CREATE TABLE record_endorsements (
            record_id      INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            PRIMARY KEY (record_id, endorsement_id)
        )
    """)
    op.execute("CREATE INDEX idx_re_endorsement ON record_endorsements(endorsement_id)")

    op.execute("""
        CREATE TABLE entities (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            entity_type TEXT NOT NULL DEFAULT '',
            created_at  TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_entities_name UNIQUE (name)
        )
    """)
    op.execute("CREATE INDEX idx_entities_name_lower ON entities(lower(name))")

    op.execute("""
        CREATE TABLE record_entities (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            entity_id INTEGER NOT NULL
                REFERENCES entities(id) ON DELETE CASCADE,
            role      TEXT NOT NULL DEFAULT 'applicant',
            position  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (record_id, entity_id, role)
        )
    """)
    op.execute("CREATE INDEX idx_re_entity ON record_entities(entity_id)")
    op.execute("CREATE INDEX idx_re_role ON record_entities(role)")

    op.execute("""
        CREATE TABLE record_links (
            id         SERIAL PRIMARY KEY,
            new_app_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            outcome_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            confidence TEXT NOT NULL
                CHECK (confidence IN ('high', 'medium', 'low')),
            days_gap   INTEGER,
            linked_at  TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_record_links UNIQUE (new_app_id, outcome_id)
        )
    """)
    op.execute("CREATE INDEX idx_record_links_new ON record_links(new_app_id)")
    op.execute("CREATE INDEX idx_record_links_outcome ON record_links(outcome_id)")

    op.execute("""
        CREATE TABLE record_enrichments (
            record_id    INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            step         TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            version      TEXT NOT NULL DEFAULT '1',
            PRIMARY KEY (record_id, step)
        )
    """)

    op.execute("""
        CREATE TABLE record_sources (
            record_id INTEGER NOT NULL
                REFERENCES license_records(id) ON DELETE CASCADE,
            source_id INTEGER NOT NULL
                REFERENCES sources(id) ON DELETE CASCADE,
            role      TEXT NOT NULL DEFAULT 'first_seen'
                CHECK (role IN ('first_seen', 'confirmed', 'repaired')),
            PRIMARY KEY (record_id, source_id, role)
        )
    """)
    op.execute("CREATE INDEX idx_rs_source ON record_sources(source_id)")

    op.execute("""
        CREATE TABLE admin_users (
            id         SERIAL PRIMARY KEY,
            email      TEXT NOT NULL,
            role       TEXT NOT NULL DEFAULT 'admin',
            created_at TEXT NOT NULL DEFAULT now()::text,
            created_by TEXT NOT NULL DEFAULT 'system'
        )
    """)
    op.execute("CREATE UNIQUE INDEX uq_admin_users_email ON admin_users(lower(email))")

    op.execute("""
        CREATE TABLE admin_audit_log (
            id          SERIAL PRIMARY KEY,
            admin_email TEXT NOT NULL,
            action      TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id   INTEGER,
            details     TEXT,
            created_at  TEXT NOT NULL DEFAULT now()::text
        )
    """)

    op.execute("""
        CREATE TABLE endorsement_aliases (
            id                      SERIAL PRIMARY KEY,
            endorsement_id          INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            canonical_endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            created_at              TEXT NOT NULL DEFAULT now()::text,
            created_by              TEXT,
            CONSTRAINT uq_endorsement_aliases_endorsement UNIQUE (endorsement_id)
        )
    """)

    op.execute("""
        CREATE TABLE endorsement_dismissed_suggestions (
            endorsement_id_a INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            endorsement_id_b INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            dismissed_by     TEXT NOT NULL,
            dismissed_at     TEXT NOT NULL DEFAULT now()::text,
            PRIMARY KEY (endorsement_id_a, endorsement_id_b),
            CHECK (endorsement_id_a < endorsement_id_b)
        )
    """)

    op.execute("""
        CREATE TABLE regulated_substances (
            id            SERIAL PRIMARY KEY,
            name          TEXT NOT NULL,
            display_order INTEGER NOT NULL DEFAULT 0
        )
    """)
    op.execute("CREATE UNIQUE INDEX uq_regulated_substances_name ON regulated_substances(lower(name))")

    op.execute("""
        CREATE TABLE regulated_substance_endorsements (
            substance_id   INTEGER NOT NULL
                REFERENCES regulated_substances(id) ON DELETE CASCADE,
            endorsement_id INTEGER NOT NULL
                REFERENCES license_endorsements(id) ON DELETE CASCADE,
            PRIMARY KEY (substance_id, endorsement_id)
        )
    """)
    op.execute(
        "CREATE INDEX idx_rse_endorsement ON regulated_substance_endorsements(endorsement_id)"
    )

    # Tracks completed data-only migrations (issue #85).
    op.execute("""
        CREATE TABLE data_migrations (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT now()::text,
            CONSTRAINT uq_data_migrations_name UNIQUE (name)
        )
    """)


def downgrade() -> None:
    tables = [
        "data_migrations",
        "regulated_substance_endorsements",
        "regulated_substances",
        "endorsement_dismissed_suggestions",
        "endorsement_aliases",
        "admin_audit_log",
        "admin_users",
        "record_sources",
        "record_enrichments",
        "record_links",
        "record_entities",
        "entities",
        "record_endorsements",
        "license_records",
        "sources",
        "source_types",
        "scrape_log",
        "endorsement_codes",
        "license_endorsements",
        "locations",
    ]
    for t in tables:
        op.execute(f"DROP TABLE IF EXISTS {t} CASCADE")  # noqa: S608
