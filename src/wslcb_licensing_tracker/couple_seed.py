"""The #246 seeded pass: the operator-approved verdicts on the 14 couple names (#269).

Spec ``docs/specs/2026-11-24-246-couple-name-splitting-design.md``, *Test
set* and *The seeded pass*.  The operator approved that table on 2026-11-24
as their decision on these 14 entities, so this pass writes it through the
same audited paths an operator's save uses:

- 9 ``split`` verdicts (:func:`split_review.set_split`, which reprocesses and
  relinks each one's records);
- 3 ``keep`` verdicts (:func:`split_review.keep_whole`);
- manual ``organization``/``trust`` rows for 17210 and 45444
  (:func:`classification_review.set_classification`).

It refuses to run if any of the 14 ids is missing, renamed, or already has a
verdict (the table was approved against these names).  Once fully applied, a
second run reports that and writes nothing.  ``wslcb db seed-couple-splits``.
"""

import logging
from dataclasses import dataclass, field

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from .classification_review import set_classification
from .entity_splits import member_ids
from .models import admin_users, entities, record_entities
from .split_review import current_verdict, keep_whole, printed_records, set_split

logger = logging.getLogger(__name__)

_NOTE = (
    "#246 seeded pass: the operator-approved table "
    "(docs/specs/2026-11-24-246-couple-name-splitting-design.md)"
)


@dataclass(frozen=True)
class SeedTable:
    """Verdicts by entity id: ``(printed name, members, reason)``, ``(name, reason)``."""

    splits: dict[int, tuple[str, tuple[str, ...], str]]
    keeps: dict[int, tuple[str, str]]
    trusts: dict[int, tuple[str, str]]

    def names(self) -> dict[int, str]:
        """Every id with the name the table was approved against."""
        return (
            {i: n for i, (n, _, _) in self.splits.items()}
            | {i: n for i, (n, _) in self.keeps.items()}
            | {i: n for i, (n, _) in self.trusts.items()}
        )


SEED_246 = SeedTable(
    splits={
        13439: (
            "TREVOR & JAYNE BLACKWELL",
            ("TREVOR EUGENE BLACKWELL", "JAYNE MARIE BLACKWELL"),
            "fuller forms printed on the same records",
        ),
        15477: ("JASON AND WENDI MINAMI", ("JASON MINAMI", "WENDI MINAMI"), "as prefilled"),
        16331: (
            "CHRISTINA & RAYMUND AND RIVAS",
            ("CHRISTINA RIVAS", "RAYMUNDO PALOMAREZ RIVAS"),
            "both printed on both records; RAYMUND is the cut-off form",
        ),
        21867: (
            "GARTH GEHLEN AND NATHAN DOIDGE",
            ("GARTH GEHLEN", "NATHAN DOIDGE"),
            "as prefilled",
        ),
        30292: ("JAMES AND WENDY HULL", ("JAMES HULL", "WENDY HULL"), "as prefilled"),
        45609: (
            "RENEE AND TIM OCHS",
            ("RENEE ELIZABETH OCHS", "TIMOTHY FLOYD OCHS"),
            "both printed on both records; TIM is TIMOTHY FLOYD OCHS",
        ),
        49945: (
            "DONALD P CLARK AND SALLY A CLA",
            ("DONALD P CLARK", "SALLY A CLARK"),
            "cut-off CLA completed from SALLY A CLARK, TTEE OF THE CLA",
        ),
        51098: (
            "WASSEF & RACHA HAROUN",
            ("WASSEF HAROUN", "RACHA BARAZI HAROUN"),
            "both printed",
        ),
        57311: ("COLIN & MARTHA MOSELEY", ("COLIN MOSELEY", "MARTHA MOSELEY"), "as prefilled"),
    },
    keeps={
        49087: ("LOPEZ & LOPEZ", "three LOPEZes are printed; nothing says which two"),
        57314: ("MERRITT KEVIN & PATR AULD", "PATR is cut off and the token order is unclear"),
        66317: (
            "FRANCISCO & VILLA VILLA",
            "VILLA VILLA is not a credible given name; FRANCISCO VILLA (9454) is a collision",
        ),
    },
    trusts={
        17210: ("HEATHER TWIST PHILLI THE ROBERT PHILLIPS AND", "trust wording, cut off"),
        45444: (
            "KEVIN AND ANNA LANGE CHILDREN",
            "beneficiary wording; JPD 2019 GIFT TRUST co-listed",
        ),
    },
)


class SeedRefused(ValueError):  # noqa: N818 — a refusal, raised before anything is written
    """The table no longer matches the database; nothing was written."""


@dataclass
class SeedResult:
    """What the pass did (or, on a dry run, would do)."""

    already_applied: bool = False
    splits: int = 0
    keeps: int = 0
    trusts: int = 0
    records: int = 0
    new_entities: list[str] = field(default_factory=list)
    member_links: int = 0
    printed_unchanged: bool = True


async def resolve_admin(conn: AsyncConnection, email: str | None) -> str:
    """The attributing admin: *email* if it is an admin, else the only admin there is.

    Raises :class:`SeedRefused` otherwise.  Never logs the address.
    """
    rows = (await conn.execute(select(admin_users.c.email))).scalars().all()
    if email is not None:
        match = [e for e in rows if e.lower() == email.strip().lower()]
        if not match:
            msg = "--admin-email is not an admin account"
            raise SeedRefused(msg)
        return match[0]
    if len(rows) != 1:
        msg = f"{len(rows)} admin accounts exist; pass --admin-email"
        raise SeedRefused(msg)
    return rows[0]


async def _check(conn: AsyncConnection, table: SeedTable) -> bool:
    """Refuse a drifted table; return True when it is already fully applied."""
    expected = table.names()
    found = dict(
        (
            await conn.execute(
                select(entities.c.id, entities.c.name).where(entities.c.id.in_(expected))
            )
        )
        .tuples()
        .all()
    )
    problems = [
        f"{i}: {'missing' if i not in found else f'renamed to {found[i]!r}'} (table: {n!r})"
        for i, n in sorted(expected.items())
        if found.get(i) != n
    ]
    verdicts = {i: await current_verdict(conn, i) for i in sorted(expected)}
    want = {i: ("split", list(m)) for i, (_, m, _) in table.splits.items()} | {
        i: ("keep", []) for i in table.keeps
    }
    applied = all(
        (v := verdicts[i]) is not None and (v["verdict"], v["members"]) == w
        for i, w in want.items()
    ) and not any(verdicts[i] for i in table.trusts)
    if applied and not problems:
        return True
    problems += [f"{i}: already has a {v['verdict']!r} verdict" for i, v in verdicts.items() if v]
    if problems:
        msg = "the approved table no longer matches; nothing written:\n  " + "\n  ".join(problems)
        raise SeedRefused(msg)
    return False


async def _printed_rows(conn: AsyncConnection, record_ids: list[int]) -> set[tuple]:
    re_ = record_entities
    stmt = select(re_.c.record_id, re_.c.entity_id, re_.c.role, re_.c.position).where(
        re_.c.record_id.in_(record_ids) & re_.c.via_entity_id.is_(None)
    )
    return set((await conn.execute(stmt)).tuples().all())


async def _apply(
    conn: AsyncConnection, table: SeedTable, admin_email: str, result: SeedResult
) -> None:
    record_ids = sorted({r for i in table.names() for r in await printed_records(conn, i)})
    before = await _printed_rows(conn, record_ids)
    members = {m for _, ms, _ in table.splits.values() for m in ms}
    result.new_entities = sorted(members - set(await member_ids(conn, members)))
    for eid, (_, ms, why) in sorted(table.splits.items()):
        await set_split(conn, eid, list(ms), admin_email=admin_email, note=f"{_NOTE}: {why}")
        result.splits += 1
    for eid, (_, why) in sorted(table.keeps.items()):
        await keep_whole(conn, eid, admin_email=admin_email, note=f"{_NOTE}: {why}")
        result.keeps += 1
    for eid, (_, why) in sorted(table.trusts.items()):
        await set_classification(
            conn,
            eid,
            entity_type="organization",
            entity_subtype="trust",
            note=f"{_NOTE}: {why}",
            admin_email=admin_email,
        )
        result.trusts += 1
    result.records = len(record_ids)
    result.printed_unchanged = await _printed_rows(conn, record_ids) == before
    result.member_links = (
        await conn.execute(
            select(func.count())
            .select_from(record_entities)
            .where(record_entities.c.via_entity_id.in_(sorted(table.splits)))
        )
    ).scalar_one()


async def seed_couples(
    conn: AsyncConnection,
    *,
    admin_email: str,
    dry_run: bool = False,
    table: SeedTable = SEED_246,
) -> SeedResult:
    """Apply *table* in the caller's transaction (in a rolled-back savepoint for a dry run).

    Raises :class:`SeedRefused` (nothing written) when the table no longer
    matches.  Caller commits.
    """
    result = SeedResult()
    if await _check(conn, table):
        result.already_applied = True
        return result
    if not dry_run:
        await _apply(conn, table, admin_email, result)
        if not result.printed_unchanged:
            msg = "a printed link changed on a reprocessed record; rolled back"
            raise SeedRefused(msg)
        logger.info(
            "Seeded %d split and %d keep verdicts, %d trust rows; %d member links on %d records",
            result.splits,
            result.keeps,
            result.trusts,
            result.member_links,
            result.records,
        )
        return result
    savepoint = await conn.begin_nested()
    try:
        await _apply(conn, table, admin_email, result)
    finally:
        await savepoint.rollback()
    return result
