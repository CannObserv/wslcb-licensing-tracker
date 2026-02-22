"""One-time migration: inline address columns → locations table.

Extracts address data from 20 legacy columns on license_records into
a normalized locations table, sets FK references, and rebuilds
license_records without the legacy columns.

This module is imported lazily by database.init_db() and only runs
when the old schema is detected (business_location column exists on
license_records).  After the migration completes, this module is
never invoked again.
"""
import sqlite3


def migrate_to_locations(conn: sqlite3.Connection) -> None:
    """Populate locations table from legacy columns on license_records.

    Detects the old schema by checking for the 'business_location' column.
    If absent, returns immediately (already migrated or fresh DB).
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(license_records)").fetchall()}
    if "business_location" not in cols:
        return  # Already migrated or fresh DB

    # Ensure FK columns exist (ALTER is a no-op error if they already do)
    for col in ("location_id", "previous_location_id"):
        if col not in cols:
            try:
                conn.execute(f"ALTER TABLE license_records ADD COLUMN {col} INTEGER")
            except sqlite3.OperationalError:
                pass

    print("Migrating address data into locations table...")

    # 1. Insert distinct primary addresses
    conn.execute("""
        INSERT OR IGNORE INTO locations (raw_address, city, state, zip_code,
            address_line_1, address_line_2, std_city, std_state, std_zip,
            address_validated_at)
        SELECT DISTINCT
            REPLACE(REPLACE(business_location, X'C2A0', ' '), X'C2A0', ' '),
            city, state, zip_code,
            address_line_1, address_line_2, std_city, std_state, std_zip,
            address_validated_at
        FROM license_records
        WHERE business_location IS NOT NULL AND business_location != ''
    """)

    # 2. Insert distinct previous addresses (CHANGE OF LOCATION)
    conn.execute("""
        INSERT OR IGNORE INTO locations (raw_address, city, state, zip_code,
            address_line_1, address_line_2, std_city, std_state, std_zip,
            address_validated_at)
        SELECT DISTINCT
            REPLACE(REPLACE(previous_business_location, X'C2A0', ' '), X'C2A0', ' '),
            previous_city, previous_state, previous_zip_code,
            prev_address_line_1, prev_address_line_2,
            prev_std_city, prev_std_state, prev_std_zip,
            prev_address_validated_at
        FROM license_records
        WHERE previous_business_location IS NOT NULL
          AND previous_business_location != ''
    """)

    # 3. Set location_id FK
    conn.execute("""
        UPDATE license_records
        SET location_id = (
            SELECT l.id FROM locations l
            WHERE l.raw_address = REPLACE(REPLACE(
                license_records.business_location, X'C2A0', ' '), X'C2A0', ' ')
        )
        WHERE business_location IS NOT NULL AND business_location != ''
          AND location_id IS NULL
    """)

    # 4. Set previous_location_id FK
    conn.execute("""
        UPDATE license_records
        SET previous_location_id = (
            SELECT l.id FROM locations l
            WHERE l.raw_address = REPLACE(REPLACE(
                license_records.previous_business_location, X'C2A0', ' '), X'C2A0', ' ')
        )
        WHERE previous_business_location IS NOT NULL
          AND previous_business_location != ''
          AND previous_location_id IS NULL
    """)

    conn.commit()

    # 5. Rebuild license_records without legacy columns
    _rebuild_records_table(conn)

    print("Migration complete.")


def _rebuild_records_table(conn: sqlite3.Connection) -> None:
    """Rebuild license_records dropping all legacy address columns.

    Disables foreign keys during the rebuild to prevent CASCADE deletes
    on record_endorsements when the old table is dropped.
    """
    # Drop FTS triggers first to avoid errors during rebuild
    conn.executescript("""
        DROP TRIGGER IF EXISTS license_records_ai;
        DROP TRIGGER IF EXISTS license_records_ad;
        DROP TRIGGER IF EXISTS license_records_au;
        DROP TRIGGER IF EXISTS license_records_bd;
    """)

    # Disable FK enforcement so DROP TABLE doesn't cascade
    conn.execute("PRAGMA foreign_keys=OFF")

    conn.executescript("""
        CREATE TABLE license_records_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_type TEXT NOT NULL,
            record_date TEXT NOT NULL,
            business_name TEXT,
            location_id INTEGER REFERENCES locations(id),
            applicants TEXT,
            license_type TEXT,
            application_type TEXT,
            license_number TEXT,
            contact_phone TEXT,
            previous_business_name TEXT DEFAULT '',
            previous_applicants TEXT DEFAULT '',
            previous_location_id INTEGER REFERENCES locations(id),
            scraped_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(section_type, record_date, license_number, application_type)
        );

        INSERT INTO license_records_new
            (id, section_type, record_date, business_name, location_id,
             applicants, license_type, application_type, license_number,
             contact_phone, previous_business_name, previous_applicants,
             previous_location_id, scraped_at, created_at)
        SELECT id, section_type, record_date, business_name, location_id,
               applicants, license_type, application_type, license_number,
               contact_phone,
               COALESCE(previous_business_name, ''),
               COALESCE(previous_applicants, ''),
               previous_location_id, scraped_at, created_at
        FROM license_records;

        DROP TABLE license_records;
        ALTER TABLE license_records_new RENAME TO license_records;
    """)

    # Re-enable FK enforcement
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()
