"""Enrichment step names and logic versions for ``record_enrichments`` (#267).

One version per step, shared by ingest (``pipeline._enrich_new_record``) and
the reprocess commands, so a stamp says which logic produced a record's
derived rows. A stamp below its step's current version is *behind*: the
record was processed by older logic (``wslcb db check`` counts these).

Bump a step's version in this module, and only here, whenever a change
alters what that step produces from the same input. Version history:

- ``endorsements`` 2: async pipeline (#94). The code has not changed since.
  Mapping edits (``endorsement_codes`` / aliases) are data, not logic.
- ``entities`` 2: async pipeline (#94); 3: categorised classifier
  and noise filter (#203); 4: regex subtypes (#206); 5: WAC annotations
  stripped at extraction (#211); 6: extraction reads ``raw_*`` when it
  cleans to the stored value (#266); 7: extraction never links a tombstone
  (current manual ``not_a_name``, #261).
  Prod's ``entities`` stamps of "1" came from ingest before #267, which
  always wrote "1" whatever the logic; treat them as behind.
- ``outcome_link`` 1: ``link_records.link_new_record`` at ingest. Only
  ingest stamps it; ``rebuild-links`` rebuilds ``record_links`` without
  stamping.
- ``address`` (retired): written only by the SQLite pipeline, removed in
  #97. Address validation now lives on ``locations``. Its "1" rows are
  frozen history and nothing stamps the step.
"""

from datetime import UTC, datetime

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import record_enrichments

STEP_ENDORSEMENTS = "endorsements"
STEP_ENTITIES = "entities"
STEP_OUTCOME_LINK = "outcome_link"
STEP_ADDRESS = "address"

STEP_VERSIONS: dict[str, int] = {
    STEP_ENDORSEMENTS: 2,
    STEP_ENTITIES: 7,
    STEP_OUTCOME_LINK: 1,
}

RETIRED_STEPS: frozenset[str] = frozenset({STEP_ADDRESS})


def current_version(step: str) -> str:
    """Return *step*'s current version as stored in ``record_enrichments.version``.

    Raises ValueError for a retired or unknown step.
    """
    if step not in STEP_VERSIONS:
        state = "retired" if step in RETIRED_STEPS else "unknown"
        msg = f"{state} enrichment step: {step!r}"
        raise ValueError(msg)
    return str(STEP_VERSIONS[step])


async def stamp_enrichment(
    conn: AsyncConnection,
    record_id: int,
    step: str,
    *,
    completed_at: datetime | None = None,
) -> None:
    """Record that *step* ran on *record_id* with the current logic version.

    Upserts on ``(record_id, step)`` so a re-run moves the timestamp and
    version forward. Caller commits.
    """
    version = current_version(step)
    now = completed_at or datetime.now(UTC)
    stmt = (
        pg_insert(record_enrichments)
        .values(record_id=record_id, step=step, completed_at=now, version=version)
        .on_conflict_do_update(
            index_elements=["record_id", "step"],
            set_={"completed_at": now, "version": version},
        )
    )
    await conn.execute(stmt)
