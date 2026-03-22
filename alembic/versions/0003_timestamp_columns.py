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

# (table, column, has_default)
# has_default=True means the column has DEFAULT now()::text that must be updated.
_COLUMNS: list[tuple[str, str, bool]] = [
    ("locations", "address_standardized_at", False),
    ("locations", "address_validated_at", False),
    ("locations", "created_at", True),
    ("license_endorsements", "created_at", True),
    ("endorsement_codes", "created_at", True),
    ("scrape_log", "started_at", False),
    ("scrape_log", "finished_at", False),
    ("scrape_log", "created_at", True),
    ("sources", "captured_at", False),
    ("sources", "ingested_at", True),
    ("license_records", "scraped_at", False),
    ("license_records", "created_at", True),
    ("entities", "created_at", True),
    ("record_links", "linked_at", True),
    ("record_enrichments", "completed_at", False),
    ("admin_users", "created_at", True),
    ("admin_audit_log", "created_at", True),
    ("endorsement_aliases", "created_at", True),
    ("endorsement_dismissed_suggestions", "dismissed_at", True),
    ("data_migrations", "applied_at", True),
]

# Per-column USING expressions that override the default ``column::timestamptz``.
# Required when existing text values don't match ISO 8601 format.
#
# sources.captured_at: old backfill_from_snapshots() stored dates as "YYYY_MM_DD"
# (underscore-delimited). PostgreSQL's ::timestamptz rejects that format, so we
# replace underscores with hyphens before casting.
_USING_OVERRIDES: dict[tuple[str, str], str] = {
    ("sources", "captured_at"): (
        "CASE WHEN captured_at ~ '^\\d{4}_\\d{2}_\\d{2}$' "
        "THEN replace(captured_at, '_', '-')::timestamptz "
        "ELSE captured_at::timestamptz END"
    ),
}


def upgrade() -> None:
    for table, column, has_default in _COLUMNS:
        # Drop the old DEFAULT (now()::text) before changing the type so
        # the USING clause doesn't conflict with the existing default expression.
        if has_default:
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT"  # noqa: S608
            )

        # Cast existing TEXT values to TIMESTAMPTZ.
        # NULL-safe: NULL TEXT → NULL TIMESTAMPTZ (fine for nullable columns).
        # Non-null ISO 8601 strings (e.g. "2026-01-15T12:00:00+00:00") cast cleanly.
        using = _USING_OVERRIDES.get((table, column), f"{column}::timestamptz")
        op.execute(
            f"ALTER TABLE {table} ALTER COLUMN {column} "  # noqa: S608
            f"TYPE TIMESTAMPTZ USING {using}"
        )

        # Restore a sensible server default for audit columns.
        if has_default:
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT now()"  # noqa: S608
            )


def downgrade() -> None:
    for table, column, has_default in _COLUMNS:
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
