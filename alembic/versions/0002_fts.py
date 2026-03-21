"""Phase 5: add pg_trgm extension, search_vector column, and FTS trigger.

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-21
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. pg_trgm needed for the % similarity operator and GIN trgm indexes
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # 2. Add search_vector column — nullable; trigger populates it
    op.execute(
        "ALTER TABLE license_records ADD COLUMN IF NOT EXISTS search_vector tsvector"
    )

    # 3. GIN index for tsvector @@ plainto_tsquery() lookups
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_license_records_search_vector"
        " ON license_records USING GIN(search_vector)"
    )

    # 4. GIN trgm indexes for fuzzy % matching on name fields
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_lr_business_name_trgm"
        " ON license_records USING GIN(business_name gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_lr_applicants_trgm"
        " ON license_records USING GIN(applicants gin_trgm_ops)"
    )

    # 5. Trigger function: weighted tsvector from license_records columns +
    #    subqueries to pull city and address from the linked location row.
    #    Weights per design doc: business_name (A), city (B),
    #    resolved_endorsements + license_type (C), applicants + address (D).
    op.execute("""
        CREATE OR REPLACE FUNCTION license_records_search_vector_update()
        RETURNS trigger AS $$
        DECLARE
            loc_city    TEXT := '';
            loc_address TEXT := '';
        BEGIN
            SELECT coalesce(city, ''), coalesce(raw_address, '')
            INTO loc_city, loc_address
            FROM locations
            WHERE id = NEW.location_id;

            NEW.search_vector :=
                setweight(to_tsvector('english', coalesce(NEW.business_name, '')), 'A') ||
                setweight(to_tsvector('english', coalesce(loc_city, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(NEW.resolved_endorsements, '')), 'C') ||
                setweight(to_tsvector('english', coalesce(NEW.license_type, '')), 'C') ||
                setweight(to_tsvector('english',
                    coalesce(NEW.applicants, '') || ' ' || coalesce(loc_address, '')), 'D');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # 6. Trigger — fires BEFORE INSERT and on relevant UPDATE columns
    op.execute("""
        CREATE OR REPLACE TRIGGER license_records_search_vector_trigger
        BEFORE INSERT OR UPDATE OF
            business_name, applicants, license_type,
            resolved_endorsements, location_id
        ON license_records
        FOR EACH ROW EXECUTE FUNCTION license_records_search_vector_update()
    """)

    # 7. Locations sync trigger — when city or raw_address changes on a location
    #    row, touch location_id on all linked license_records to re-fire the
    #    license_records_search_vector_trigger (keeps weight logic in one place).
    op.execute("""
        CREATE OR REPLACE FUNCTION locations_search_vector_sync()
        RETURNS trigger AS $$
        BEGIN
            UPDATE license_records
            SET location_id = location_id
            WHERE location_id = NEW.id;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    op.execute("""
        CREATE OR REPLACE TRIGGER locations_search_vector_sync_trigger
        AFTER UPDATE OF city, raw_address
        ON locations
        FOR EACH ROW
        WHEN (
            OLD.city IS DISTINCT FROM NEW.city
            OR OLD.raw_address IS DISTINCT FROM NEW.raw_address
        )
        EXECUTE FUNCTION locations_search_vector_sync()
    """)

    # 8. Backfill all existing rows — correlated subqueries pull city (B) and
    #    raw_address (D) separately so weights match the design doc spec.
    #    NULL location_id → subquery returns NULL → coalesce → empty string.
    op.execute("""
        UPDATE license_records lr
        SET search_vector =
            setweight(to_tsvector('english', coalesce(lr.business_name, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(
                (SELECT city FROM locations WHERE id = lr.location_id), ''
            )), 'B') ||
            setweight(to_tsvector('english', coalesce(lr.resolved_endorsements, '')), 'C') ||
            setweight(to_tsvector('english', coalesce(lr.license_type, '')), 'C') ||
            setweight(to_tsvector('english',
                coalesce(lr.applicants, '') || ' ' || coalesce(
                    (SELECT raw_address FROM locations WHERE id = lr.location_id), ''
                )
            ), 'D')
    """)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS locations_search_vector_sync_trigger"
        " ON locations"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS locations_search_vector_sync()"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS license_records_search_vector_trigger"
        " ON license_records"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS license_records_search_vector_update()"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_lr_applicants_trgm"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_lr_business_name_trgm"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_license_records_search_vector"
    )
    op.execute(
        "ALTER TABLE license_records DROP COLUMN IF EXISTS search_vector"
    )
    # Note: does NOT drop pg_trgm extension — other parts of the DB may use it.
