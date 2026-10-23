"""Tie entity_subtype to entity_type on entities and entity_classifications (#212).

An organization carries a subtype; anything else (``person``, the legacy
``''`` on ``entities``, ``not_a_name`` on ``entity_classifications``) carries
NULL.  0009 could not add this: every organization had a NULL subtype until
#206's ``reclassify-entities`` backfill.  Precheck before applying on prod
(must return no ``organization``/NULL row and no non-organization row with a
subtype)::

    SELECT entity_type, entity_subtype IS NULL, count(*) FROM entities GROUP BY 1, 2;
    SELECT entity_type, entity_subtype IS NULL, count(*)
      FROM entity_classifications GROUP BY 1, 2;

Revision ID: 0010
Revises: 0009
Create Date: 2026-10-23
"""

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None

_PAIR_CHECK = (
    "(entity_type = 'organization' AND entity_subtype IS NOT NULL) "
    "OR (entity_type <> 'organization' AND entity_subtype IS NULL)"
)


def upgrade() -> None:
    op.create_check_constraint("ck_entities_subtype_matches_type", "entities", _PAIR_CHECK)
    op.create_check_constraint(
        "ck_ec_subtype_matches_type", "entity_classifications", _PAIR_CHECK
    )


def downgrade() -> None:
    op.drop_constraint("ck_ec_subtype_matches_type", "entity_classifications", type_="check")
    op.drop_constraint("ck_entities_subtype_matches_type", "entities", type_="check")
