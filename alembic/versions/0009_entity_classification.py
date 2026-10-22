"""Entity classification: subtypes, per-source answers, review flags, run records (#206).

``entities`` gains the resolved ``entity_subtype`` and ``classification_source``
columns. ``entity_classifications`` keeps every source's answer (regex, Jev,
manual) as append-only history; ``entity_review_flags`` holds operator flags;
``classification_runs`` records each Jev run. The constant lists in
``entity_classification.py`` are the source of truth; this migration freezes
the same values as history. Purely additive: existing rows default to
``classification_source = 'regex'`` with a NULL subtype until
``wslcb db reclassify-entities`` writes the regex rows and resolves.

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-22
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None

_SUBTYPE_CHECK = (
    "entity_subtype IS NULL OR entity_subtype IN "
    "('business', 'nonprofit', 'trust', 'estate', 'tribe', 'government', 'unknown')"
)
_SOURCE_VALUES = "('regex', 'jev', 'manual')"


def upgrade() -> None:
    op.add_column("entities", sa.Column("entity_subtype", sa.Text()))
    op.add_column(
        "entities",
        sa.Column("classification_source", sa.Text(), nullable=False, server_default="regex"),
    )
    op.create_check_constraint("ck_entities_subtype", "entities", _SUBTYPE_CHECK)
    op.create_check_constraint(
        "ck_entities_classification_source",
        "entities",
        f"classification_source IN {_SOURCE_VALUES}",
    )
    op.create_index("idx_entities_subtype", "entities", ["entity_subtype"])

    op.create_table(
        "entity_classifications",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column(
            "entity_id",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_subtype", sa.Text()),
        sa.Column("confidence", sa.Float()),
        sa.Column("probabilities", JSONB()),
        sa.Column("model", sa.Text()),
        sa.Column("input_tokens", sa.Float()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("created_by", sa.Text()),
        sa.Column("note", sa.Text()),
        sa.Column("ignored_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(f"source IN {_SOURCE_VALUES}", name="ck_ec_source"),
        sa.CheckConstraint(
            "entity_type IN ('person', 'organization', 'not_a_name')", name="ck_ec_entity_type"
        ),
        sa.CheckConstraint(_SUBTYPE_CHECK, name="ck_ec_subtype"),
    )
    op.create_index("idx_ec_entity_source", "entity_classifications", ["entity_id", "source", "id"])
    op.create_index("idx_ec_source_model", "entity_classifications", ["source", "model"])

    op.create_table(
        "entity_review_flags",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column(
            "entity_id",
            sa.Integer(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("flagged_by", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("resolved_by", sa.Text()),
        sa.Column("resolution", sa.Text()),
    )
    op.create_index(
        "idx_erf_open",
        "entity_review_flags",
        ["entity_id"],
        postgresql_where=sa.text("resolved_at IS NULL"),
    )

    op.create_table(
        "classification_runs",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("origin", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("names_sent", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("batches_ok", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("batches_failed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("input_tokens", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text()),
        sa.CheckConstraint(
            "origin IN ('post_scrape', 'post_import', 'cli', 'admin')",
            name="ck_classification_runs_origin",
        ),
    )
    op.create_index("idx_classification_runs_started", "classification_runs", ["started_at"])


def downgrade() -> None:
    op.drop_index("idx_classification_runs_started", table_name="classification_runs")
    op.drop_table("classification_runs")
    op.drop_index("idx_erf_open", table_name="entity_review_flags")
    op.drop_table("entity_review_flags")
    op.drop_index("idx_ec_source_model", table_name="entity_classifications")
    op.drop_index("idx_ec_entity_source", table_name="entity_classifications")
    op.drop_table("entity_classifications")
    op.drop_index("idx_entities_subtype", table_name="entities")
    op.drop_constraint("ck_entities_classification_source", "entities", type_="check")
    op.drop_constraint("ck_entities_subtype", "entities", type_="check")
    op.drop_column("entities", "classification_source")
    op.drop_column("entities", "entity_subtype")
