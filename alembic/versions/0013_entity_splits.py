"""Entity splits: operator verdicts on couple names, and member links (#269, spec #246 §1).

``entity_splits`` holds one operator verdict per combined entity (``split`` or
``keep``); ``entity_split_members`` the people a ``split`` names, as cleaned
names in display order.  Both are operator data: nothing automated writes or
deletes them.

``record_entities.via_entity_id`` marks a member link: the record prints the
combined entity, and extraction also links each member through it.  Member
rows are derived (``reprocess-entities`` rebuilds them); printed rows have
``via_entity_id`` NULL.

Empty tables and a nullable column: no backfill, under ``lock_timeout``.
Deploy order: ``alembic upgrade head``, restart, then the seeded pass
(``wslcb db seed-couple-splits``).

Revision ID: 0013
Revises: 0012
Create Date: 2026-11-27
"""

import sqlalchemy as sa
from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def _fail_fast_on_lock() -> None:
    # ADD COLUMN takes ACCESS EXCLUSIVE on record_entities; fail and roll back
    # rather than queue every web read behind an ingest transaction (as 0012).
    op.execute("SET LOCAL lock_timeout = '5s'")


def upgrade() -> None:
    _fail_fast_on_lock()
    op.create_table(
        "entity_splits",
        sa.Column(
            "combined_id",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("verdict", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column("note", sa.Text()),
        sa.CheckConstraint("verdict IN ('split', 'keep')", name="ck_entity_splits_verdict"),
    )
    op.create_table(
        "entity_split_members",
        sa.Column(
            "combined_id",
            sa.Integer(),
            sa.ForeignKey("entity_splits.combined_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("ordinal", sa.SmallInteger(), primary_key=True),
        sa.Column("member_name", sa.Text(), nullable=False),
        sa.UniqueConstraint("combined_id", "member_name", name="uq_entity_split_members_name"),
    )
    op.add_column(
        "record_entities",
        sa.Column(
            "via_entity_id",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "ck_record_entities_via_not_self",
        "record_entities",
        "via_entity_id IS NULL OR via_entity_id <> entity_id",
    )
    op.create_index(
        "idx_re_via",
        "record_entities",
        ["via_entity_id"],
        postgresql_where=sa.text("via_entity_id IS NOT NULL"),
    )


def downgrade() -> None:
    _fail_fast_on_lock()
    op.drop_index("idx_re_via", table_name="record_entities")
    op.drop_constraint("ck_record_entities_via_not_self", "record_entities", type_="check")
    op.drop_column("record_entities", "via_entity_id")
    op.drop_table("entity_split_members")
    op.drop_table("entity_splits")
