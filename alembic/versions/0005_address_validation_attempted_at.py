"""Add locations.address_validation_attempted_at (renewal scheduling).

Separates renewal *scheduling* from validation *provenance* (#150).
``address_validation_attempted_at`` records the last time a /validate call was
made for a row, regardless of outcome; ``address_validated_at`` reverts to
meaning "last time a /validate call returned a confirmed result".

Backfills ``attempted_at := validated_at`` for already-validated rows so the TTL
renewal wave tracks the original validation distribution rather than making the
whole table eligible the instant the column ships.

Revision ID: 0005
Revises: 0004
Create Date: 2026-07-25
"""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "locations",
        sa.Column("address_validation_attempted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE locations "
        "SET address_validation_attempted_at = address_validated_at "
        "WHERE address_validated_at IS NOT NULL"
    )
    # Renewal selector filters/orders on this column and the daily-ceiling count
    # scans it, all twice daily; index it since the table grows unboundedly.
    op.create_index(
        "idx_locations_attempted_at",
        "locations",
        ["address_validation_attempted_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_locations_attempted_at", table_name="locations")
    op.drop_column("locations", "address_validation_attempted_at")
