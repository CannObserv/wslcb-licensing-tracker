"""Migrate timestamp columns from TEXT to TIMESTAMPTZ.

Converts all audit/operational timestamp columns that were created as TEXT
(a SQLite migration artifact) to native PostgreSQL TIMESTAMPTZ.

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-22
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

# (table, column, nullable, has_default)
# has_default=True means the column has DEFAULT now()::text that must be updated.
_COLUMNS: list[tuple[str, str, bool, bool]] = [
    ("locations", "address_standardized_at", True, False),
    ("locations", "address_validated_at", True, False),
    ("locations", "created_at", False, True),
    ("license_endorsements", "created_at", False, True),
    ("endorsement_codes", "created_at", False, True),
    ("scrape_log", "started_at", False, False),
    ("scrape_log", "finished_at", True, False),
    ("scrape_log", "created_at", False, True),
    ("sources", "captured_at", True, False),
    ("sources", "ingested_at", False, True),
    ("license_records", "scraped_at", False, False),
    ("license_records", "created_at", False, True),
    ("entities", "created_at", False, True),
    ("record_links", "linked_at", False, True),
    ("record_enrichments", "completed_at", False, False),
    ("admin_users", "created_at", False, True),
    ("admin_audit_log", "created_at", False, True),
    ("endorsement_aliases", "created_at", False, True),
    ("endorsement_dismissed_suggestions", "dismissed_at", False, True),
    ("data_migrations", "applied_at", False, True),
]


def upgrade() -> None:
    for table, column, nullable, has_default in _COLUMNS:
        # Drop the old DEFAULT (now()::text) before changing the type so
        # the USING clause doesn't conflict with the existing default expression.
        if has_default:
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT"  # noqa: S608
            )

        # Cast existing TEXT values to TIMESTAMPTZ.
        # NULL-safe: NULL TEXT → NULL TIMESTAMPTZ (fine for nullable columns).
        # Non-null ISO 8601 strings (e.g. "2026-01-15T12:00:00+00:00") cast cleanly.
        op.execute(
            f"ALTER TABLE {table} ALTER COLUMN {column} "  # noqa: S608
            f"TYPE TIMESTAMPTZ USING {column}::timestamptz"
        )

        # Restore a sensible server default for audit columns.
        if has_default:
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT now()"  # noqa: S608
            )


def downgrade() -> None:
    for table, column, nullable, has_default in _COLUMNS:
        if has_default:
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT"  # noqa: S608
            )

        op.execute(
            f"ALTER TABLE {table} ALTER COLUMN {column} "  # noqa: S608
            f"TYPE TEXT USING {column}::text"
        )

        if has_default:
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT now()::text"  # noqa: S608
            )
