"""Seed the co_replay source type (#154).

Replay-generated provenance extracts get their own source type: the
reconstructed ``<tbody>`` a record was first evidenced by in the diff-chain
replay (#151). Additive data migration per the ``SOURCE_TYPE_ROWS`` pattern
in ``db.py`` — the canonical row list there is the source of truth; this
migration freezes the same values as history.

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-30
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "INSERT INTO source_types (id, slug, label, description) VALUES "
        "(6, 'co_replay', 'CO Replay Extract', "
        "'Reconstructed from CO diff chain replay') "
        "ON CONFLICT (id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DELETE FROM source_types WHERE id = 6 AND slug = 'co_replay'")
