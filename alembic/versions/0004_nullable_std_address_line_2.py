"""Make locations.std_address_line_2 nullable.

Most addresses do not have a second address line.  The address validator API
returns null for std_address_line_2 in those cases, which triggered a
NotNullViolationError on every such UPDATE, cascading into
InFailedSQLTransactionError for all subsequent rows in the same batch.

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-01
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("locations", "std_address_line_2", nullable=True)


def downgrade() -> None:
    op.execute("UPDATE locations SET std_address_line_2 = '' WHERE std_address_line_2 IS NULL")
    op.alter_column("locations", "std_address_line_2", nullable=False)
