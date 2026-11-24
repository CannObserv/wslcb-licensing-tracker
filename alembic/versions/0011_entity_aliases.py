"""Entity alias layer: entity_aliases, entity_alias_dismissals, entities.link_key (#262).

Spec: ``docs/specs/2026-11-23-247-entity-name-linking-design.md`` §1.
Purely additive.  ``entities.link_key`` starts NULL: it depends on each
entity's resolved type, so ``wslcb db relink-entities`` fills it along with
the ``source='rule'`` alias rows.  Deploy order: ``alembic upgrade head``,
restart, ``relink-entities --dry-run``, then ``relink-entities``.

``UNIQUE NULLS NOT DISTINCT`` needs PostgreSQL 15+ (prod runs 16).

Revision ID: 0011
Revises: 0010
Create Date: 2026-11-24
"""

import sqlalchemy as sa
from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def _fail_fast_on_lock() -> None:
    # ADD COLUMN takes ACCESS EXCLUSIVE on entities; fail and roll back rather
    # than queue every web read behind an ingest or Jev transaction (as 0010).
    op.execute("SET LOCAL lock_timeout = '5s'")


def upgrade() -> None:
    _fail_fast_on_lock()
    op.add_column("entities", sa.Column("link_key", sa.Text(), nullable=True))
    op.create_index("idx_entities_link_key", "entities", ["link_key"])

    op.create_table(
        "entity_aliases",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column(
            "variant_id",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "canonical_id",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("scope_license", sa.Text()),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("rule", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("created_by", sa.Text()),
        sa.Column("note", sa.Text()),
        sa.UniqueConstraint(
            "variant_id",
            "scope_license",
            "source",
            name="uq_entity_aliases_variant_scope_source",
            postgresql_nulls_not_distinct=True,
        ),
        sa.CheckConstraint("variant_id <> canonical_id", name="ck_entity_aliases_not_self"),
        sa.CheckConstraint("source IN ('rule', 'manual')", name="ck_entity_aliases_source"),
        sa.CheckConstraint(
            "(source = 'rule') = (rule IS NOT NULL)",
            name="ck_entity_aliases_rule_iff_rule_source",
        ),
    )
    op.create_index("idx_entity_aliases_canonical", "entity_aliases", ["canonical_id"])

    op.create_table(
        "entity_alias_dismissals",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column(
            "entity_id_a",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "entity_id_b",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("scope_license", sa.Text()),
        sa.Column("dismissed_by", sa.Text(), nullable=False),
        sa.Column(
            "dismissed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("note", sa.Text()),
        sa.UniqueConstraint(
            "entity_id_a",
            "entity_id_b",
            "scope_license",
            name="uq_entity_alias_dismissals_pair_scope",
            postgresql_nulls_not_distinct=True,
        ),
        sa.CheckConstraint("entity_id_a < entity_id_b", name="ck_entity_alias_dismissals_ordered"),
    )
    op.create_index("idx_entity_alias_dismissals_b", "entity_alias_dismissals", ["entity_id_b"])


def downgrade() -> None:
    _fail_fast_on_lock()
    op.drop_table("entity_alias_dismissals")
    op.drop_table("entity_aliases")
    op.drop_index("idx_entities_link_key", table_name="entities")
    op.drop_column("entities", "link_key")
