"""Allow the replay_extract record_sources role (#154).

Replay-generated provenance extracts link to their record with role
``replay_extract``; widen the role CHECK constraint to admit it. The
baseline named the constraint ``record_sources_role_check`` (inline
CHECK); ``models.py`` names it ``ck_record_sources_role`` — drop
whichever exists before re-adding.

Revision ID: 0007
Revises: 0006
Create Date: 2026-07-30
"""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None

_ROLES_NEW = "('first_seen', 'confirmed', 'repaired', 'replay_extract')"
_ROLES_OLD = "('first_seen', 'confirmed', 'repaired')"


def upgrade() -> None:
    op.execute(
        "ALTER TABLE record_sources "
        "DROP CONSTRAINT IF EXISTS record_sources_role_check, "
        "DROP CONSTRAINT IF EXISTS ck_record_sources_role"
    )
    op.execute(
        "ALTER TABLE record_sources "
        f"ADD CONSTRAINT ck_record_sources_role CHECK (role IN {_ROLES_NEW})"
    )


def downgrade() -> None:
    op.execute("DELETE FROM record_sources WHERE role = 'replay_extract'")
    op.execute("ALTER TABLE record_sources DROP CONSTRAINT IF EXISTS ck_record_sources_role")
    op.execute(
        "ALTER TABLE record_sources "
        f"ADD CONSTRAINT ck_record_sources_role CHECK (role IN {_ROLES_OLD})"
    )
