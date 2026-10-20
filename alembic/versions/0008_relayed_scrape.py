"""Relayed scrape: source type and scrape_log origin (#201).

While WSLCB blocks this host (#198), the operator captures the page
off-host and relays it in. Those captures get their own source type, and
``scrape_log`` records which path produced each run plus, for a relay, the
capture time. Source-type row per the ``SOURCE_TYPE_ROWS`` pattern in
``db.py`` — the canonical list there is the source of truth; this migration
freezes the same values as history.

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-20
"""

import sqlalchemy as sa
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "INSERT INTO source_types (id, slug, label, description) VALUES "
        "(7, 'relayed_scrape', 'Relayed Capture', "
        "'WSLCB page captured off-host by the operator during an IP block') "
        "ON CONFLICT (id) DO NOTHING"
    )
    op.add_column(
        "scrape_log",
        sa.Column("origin", sa.Text(), nullable=False, server_default="server"),
    )
    op.add_column("scrape_log", sa.Column("captured_at", sa.DateTime(timezone=True)))
    op.create_check_constraint(
        "ck_scrape_log_origin", "scrape_log", "origin IN ('server', 'relay')"
    )


def downgrade() -> None:
    op.drop_constraint("ck_scrape_log_origin", "scrape_log", type_="check")
    op.drop_column("scrape_log", "captured_at")
    op.drop_column("scrape_log", "origin")
    op.execute("DELETE FROM source_types WHERE id = 7 AND slug = 'relayed_scrape'")
