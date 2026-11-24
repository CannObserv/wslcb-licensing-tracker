"""record_entities.party_id: each link's resolved alias party (#263).

``party_id`` is derived: ``eff(entity_id, the record's licence)`` from the
alias layer (#262).  A trigger defaults it to ``entity_id`` on insert and
when an update re-points ``entity_id`` without setting ``party_id``, so it is
never NULL and no insert site has to know about it; ``entity_aliases.relink``
writes the real party.  The index serves the per-party record counts on
``/entities`` (presorted distinct count) and the entity-page union.

Backfills ``party_id = entity_id`` (~210k rows on prod, seconds, under
``lock_timeout``).  Deploy order: ``alembic upgrade head``, restart,
``wslcb db relink-entities``.

Revision ID: 0012
Revises: 0011
Create Date: 2026-11-24
"""

import sqlalchemy as sa
from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None

_FUNCTION = """
CREATE OR REPLACE FUNCTION record_entities_party_default() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.party_id IS NULL THEN
            NEW.party_id := NEW.entity_id;
        END IF;
    ELSIF NEW.entity_id IS DISTINCT FROM OLD.entity_id
          AND NEW.party_id IS NOT DISTINCT FROM OLD.party_id THEN
        NEW.party_id := NEW.entity_id;
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql
"""


def _fail_fast_on_lock() -> None:
    # ADD COLUMN takes ACCESS EXCLUSIVE on record_entities; fail and roll back
    # rather than queue every web read behind an ingest transaction (as 0010).
    op.execute("SET LOCAL lock_timeout = '5s'")


def upgrade() -> None:
    _fail_fast_on_lock()
    op.add_column("record_entities", sa.Column("party_id", sa.Integer(), nullable=True))
    op.execute("UPDATE record_entities SET party_id = entity_id")
    op.alter_column("record_entities", "party_id", nullable=False)
    op.create_index("idx_re_party_record", "record_entities", ["party_id", "record_id"])
    op.execute(_FUNCTION)
    op.execute(
        "CREATE TRIGGER trg_record_entities_party_default "
        "BEFORE INSERT OR UPDATE OF entity_id ON record_entities "
        "FOR EACH ROW EXECUTE FUNCTION record_entities_party_default()"
    )


def downgrade() -> None:
    _fail_fast_on_lock()
    op.execute("DROP TRIGGER IF EXISTS trg_record_entities_party_default ON record_entities")
    op.execute("DROP FUNCTION IF EXISTS record_entities_party_default()")
    op.drop_index("idx_re_party_record", table_name="record_entities")
    op.drop_column("record_entities", "party_id")
