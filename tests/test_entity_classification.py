"""Tests for entity_classification.py — the classification resolver (#206).

``choose_classification`` is pure; the rest run against PostgreSQL
(``pg_conn``).  Jev rows are seeded by inserting into
``entity_classifications`` directly.
"""

import pytest
from sqlalchemy import func, insert, select

from wslcb_licensing_tracker.entities import get_or_create_entity
from wslcb_licensing_tracker.entity_classification import (
    ENTITY_SUBTYPES,
    JEV_ORG_SUBTYPES,
    SourceAnswer,
    choose_classification,
    clear_manual,
    confirmed_noise_ids,
    current_answers,
    protected_ids,
    record_manual,
    resolve_entities,
    set_jev_ignored,
)
from wslcb_licensing_tracker.models import entities, entity_classifications, entity_review_flags

THRESHOLD_ENV = "CO_WSLCB_LICENSING_TRACKER_JEV_CONFIDENCE_THRESHOLD"

R_PERSON = SourceAnswer("person", None)
R_BIZ = SourceAnswer("organization", "business")


@pytest.mark.parametrize(
    ("manual", "jev", "regex", "trusted", "expected"),
    [
        # manual wins over everything
        (
            SourceAnswer("person", None),
            SourceAnswer("organization", "business", 0.99),
            R_BIZ,
            frozenset(),
            ("person", None, "manual"),
        ),
        # manual not_a_name is never resolved onto entities
        (SourceAnswer("not_a_name", None), None, R_PERSON, frozenset(), ("person", None, "regex")),
        # jev at exactly the threshold wins; trust is not an org-question subtype
        (
            None,
            SourceAnswer("organization", "trust", 0.70),
            R_PERSON,
            frozenset(),
            ("organization", "trust", "jev"),
        ),
        # below threshold -> regex
        (
            None,
            SourceAnswer("organization", "business", 0.69),
            R_PERSON,
            frozenset(),
            ("person", None, "regex"),
        ),
        # jev not_a_name -> regex
        (
            None,
            SourceAnswer("not_a_name", None, 0.99),
            R_PERSON,
            frozenset(),
            ("person", None, "regex"),
        ),
        # untrusted subtype falls back to the regex's org subtype ...
        (
            None,
            SourceAnswer("organization", "nonprofit", 0.95),
            R_BIZ,
            frozenset(),
            ("organization", "business", "jev"),
        ),
        # ... or unknown when the regex says person
        (
            None,
            SourceAnswer("organization", "nonprofit", 0.95),
            R_PERSON,
            frozenset(),
            ("organization", "unknown", "jev"),
        ),
        # trusted subtype is kept
        (
            None,
            SourceAnswer("organization", "nonprofit", 0.95),
            R_BIZ,
            frozenset({"nonprofit"}),
            ("organization", "nonprofit", "jev"),
        ),
        # jev unknown falls back uniformly
        (
            None,
            SourceAnswer("organization", "unknown", 0.95),
            R_BIZ,
            frozenset(),
            ("organization", "business", "jev"),
        ),
        (
            None,
            SourceAnswer("organization", "unknown", 0.95),
            R_PERSON,
            frozenset(),
            ("organization", "unknown", "jev"),
        ),
        (None, SourceAnswer("person", None, 0.9), R_BIZ, frozenset(), ("person", None, "jev")),
        # jev with no confidence never clears the threshold
        (
            None,
            SourceAnswer("person", None, None),
            R_BIZ,
            frozenset(),
            ("organization", "business", "regex"),
        ),
        # a NULL Jev org subtype falls back like unknown
        (
            None,
            SourceAnswer("organization", None, 0.95),
            R_BIZ,
            frozenset(),
            ("organization", "business", "jev"),
        ),
        (None, None, None, frozenset(), None),
    ],
)
def test_choose_classification(manual, jev, regex, trusted, expected):
    got = choose_classification(manual, jev, regex, threshold=0.7, trusted_org_subtypes=trusted)
    assert got == expected


def test_constants():
    assert set(JEV_ORG_SUBTYPES) < set(ENTITY_SUBTYPES)
    assert len(ENTITY_SUBTYPES) == 7


# ── PostgreSQL ───────────────────────────────────────────────────────


async def _jev(conn, eid: int, entity_type: str, subtype: str | None, conf: float, **kw) -> int:
    return (
        await conn.execute(
            insert(entity_classifications)
            .values(
                entity_id=eid,
                source="jev",
                entity_type=entity_type,
                entity_subtype=subtype,
                confidence=conf,
                model=kw.pop("model", "jev-1.14.0"),
                **kw,
            )
            .returning(entity_classifications.c.id)
        )
    ).scalar_one()


async def _resolved(conn, eid: int) -> tuple:
    row = (
        await conn.execute(
            select(
                entities.c.entity_type,
                entities.c.entity_subtype,
                entities.c.classification_source,
            ).where(entities.c.id == eid)
        )
    ).one()
    return tuple(row)


async def _row_count(conn, eid: int) -> int:
    return (
        await conn.execute(select(func.count()).where(entity_classifications.c.entity_id == eid))
    ).scalar_one()


@pytest.mark.asyncio(loop_scope="session")
async def test_resolve_entities_applies_jev_and_reports_changes(pg_conn, monkeypatch):
    monkeypatch.delenv(THRESHOLD_ENV, raising=False)
    eid = await get_or_create_entity(pg_conn, "T206 FAMILY DOLLAR")
    assert await _resolved(pg_conn, eid) == ("person", None, "regex")
    await _jev(pg_conn, eid, "organization", "business", 0.92)

    result = await resolve_entities(pg_conn, [eid])
    assert result.examined == 1
    assert [(c.entity_id, c.old, c.new) for c in result.changes] == [
        (eid, ("person", None, "regex"), ("organization", "unknown", "jev"))
    ]
    assert await _resolved(pg_conn, eid) == ("organization", "unknown", "jev")
    # idempotent
    assert (await resolve_entities(pg_conn, [eid])).changes == []


@pytest.mark.asyncio(loop_scope="session")
async def test_current_answers_latest_non_ignored_per_source(pg_conn):
    eid = await get_or_create_entity(pg_conn, "T206 LIBERTY BAR")
    await _jev(pg_conn, eid, "organization", "business", 0.8, model="jev-old")
    await _jev(pg_conn, eid, "person", None, 0.9, model="jev-new")
    answers = (await current_answers(pg_conn, [eid]))[eid]
    assert set(answers) == {"regex", "jev"}
    assert answers["jev"] == SourceAnswer("person", None, 0.9, "jev-new")
    assert answers["regex"].model == "regex-1"


@pytest.mark.asyncio(loop_scope="session")
async def test_ignored_jev_row_is_skipped(pg_conn, monkeypatch):
    monkeypatch.delenv(THRESHOLD_ENV, raising=False)
    eid = await get_or_create_entity(pg_conn, "T206 SUNSHINE MART")
    await _jev(pg_conn, eid, "organization", "business", 0.95, ignored_at=func.now())
    await resolve_entities(pg_conn, [eid])
    assert await _resolved(pg_conn, eid) == ("person", None, "regex")


@pytest.mark.asyncio(loop_scope="session")
async def test_latest_manual_row_wins_and_history_kept(pg_conn):
    eid = await get_or_create_entity(pg_conn, "T206 GREEN ACRES")
    await record_manual(
        pg_conn, eid, entity_type="organization", entity_subtype="nonprofit", created_by="a@x"
    )
    assert await _resolved(pg_conn, eid) == ("organization", "nonprofit", "manual")
    await record_manual(
        pg_conn, eid, entity_type="person", entity_subtype=None, created_by="b@x", note="oops"
    )
    assert await _resolved(pg_conn, eid) == ("person", None, "manual")
    manual_rows = (
        await pg_conn.execute(
            select(entity_classifications.c.created_by, entity_classifications.c.note)
            .where(
                (entity_classifications.c.entity_id == eid)
                & (entity_classifications.c.source == "manual")
            )
            .order_by(entity_classifications.c.id)
        )
    ).fetchall()
    assert [tuple(r) for r in manual_rows] == [("a@x", None), ("b@x", "oops")]


@pytest.mark.asyncio(loop_scope="session")
async def test_clear_manual_returns_to_automatic_and_keeps_rows(pg_conn):
    eid = await get_or_create_entity(pg_conn, "T206 ROSE GARDEN")
    await record_manual(
        pg_conn, eid, entity_type="organization", entity_subtype="business", created_by="a@x"
    )
    await record_manual(
        pg_conn, eid, entity_type="organization", entity_subtype="nonprofit", created_by="a@x"
    )
    assert await clear_manual(pg_conn, eid) == 2
    assert await _resolved(pg_conn, eid) == ("person", None, "regex")
    ignored = (
        await pg_conn.execute(
            select(func.count()).where(
                (entity_classifications.c.entity_id == eid)
                & (entity_classifications.c.source == "manual")
                & entity_classifications.c.ignored_at.is_not(None)
            )
        )
    ).scalar_one()
    assert ignored == 2
    # the regex row is untouched
    assert (await current_answers(pg_conn, [eid]))[eid]["regex"].entity_type == "person"
    assert await clear_manual(pg_conn, eid) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_record_manual_rejects_bad_combinations(pg_conn):
    eid = await get_or_create_entity(pg_conn, "T206 BAD INPUT")
    before = await _row_count(pg_conn, eid)
    for entity_type, subtype in (
        ("person", "business"),
        ("organization", None),
        ("organization", "club"),
        ("not_a_name", "business"),
        ("household", None),
    ):
        with pytest.raises(ValueError):
            await record_manual(
                pg_conn, eid, entity_type=entity_type, entity_subtype=subtype, created_by="a@x"
            )
    missing = (await pg_conn.execute(select(func.max(entities.c.id)))).scalar_one() + 1000
    with pytest.raises(LookupError):
        await record_manual(
            pg_conn, missing, entity_type="person", entity_subtype=None, created_by="a@x"
        )
    assert await _row_count(pg_conn, eid) == before
    assert await _resolved(pg_conn, eid) == ("person", None, "regex")


@pytest.mark.asyncio(loop_scope="session")
async def test_threshold_env_changes_resolution(pg_conn, monkeypatch):
    eid = await get_or_create_entity(pg_conn, "T206 MAPLE LEAF")
    await _jev(pg_conn, eid, "organization", "trust", 0.8)
    monkeypatch.setenv(THRESHOLD_ENV, "0.9")
    await resolve_entities(pg_conn, [eid])
    assert await _resolved(pg_conn, eid) == ("person", None, "regex")
    monkeypatch.setenv(THRESHOLD_ENV, "0.75")
    await resolve_entities(pg_conn, [eid])
    assert await _resolved(pg_conn, eid) == ("organization", "trust", "jev")


@pytest.mark.asyncio(loop_scope="session")
async def test_protected_and_confirmed_noise_ids(pg_conn):
    manual_person = await get_or_create_entity(pg_conn, "T206 PROTECTED PERSON")
    flagged = await get_or_create_entity(pg_conn, "T206 FLAGGED ONE")
    noise = await get_or_create_entity(pg_conn, "T206 CONFIRMED NOISE")
    noise_flagged = await get_or_create_entity(pg_conn, "T206 NOISE BUT FLAGGED")
    resolved_flag = await get_or_create_entity(pg_conn, "T206 RESOLVED FLAG")
    await record_manual(
        pg_conn, manual_person, entity_type="person", entity_subtype=None, created_by="a@x"
    )
    for eid in (noise, noise_flagged):
        await record_manual(
            pg_conn, eid, entity_type="not_a_name", entity_subtype=None, created_by="a@x"
        )
    for eid in (flagged, noise_flagged):
        await pg_conn.execute(
            insert(entity_review_flags).values(entity_id=eid, flagged_by="a@x", reason="check")
        )
    await pg_conn.execute(
        insert(entity_review_flags).values(
            entity_id=resolved_flag, flagged_by="a@x", reason="x", resolved_at=func.now()
        )
    )
    # not_a_name never resolves onto entities: the regex answer stands
    assert await _resolved(pg_conn, noise) == ("person", None, "regex")

    prot = await protected_ids(pg_conn)
    conf = await confirmed_noise_ids(pg_conn)
    ours = {manual_person, flagged, noise, noise_flagged, resolved_flag}
    assert prot & ours == {manual_person, flagged, noise_flagged}
    assert conf & ours == {noise}


# ── Jev rollback (Task 1.5) ──────────────────────────────────────────


async def _ignored_by_source(conn, eid: int) -> dict[str, list[bool]]:
    rows = (
        await conn.execute(
            select(entity_classifications.c.source, entity_classifications.c.ignored_at)
            .where(entity_classifications.c.entity_id == eid)
            .order_by(entity_classifications.c.id)
        )
    ).fetchall()
    out: dict[str, list[bool]] = {}
    for source, ignored_at in rows:
        out.setdefault(source, []).append(ignored_at is not None)
    return out


@pytest.mark.asyncio(loop_scope="session")
async def test_ignore_and_restore_jev_rows(pg_conn, monkeypatch):
    monkeypatch.delenv(THRESHOLD_ENV, raising=False)
    jev_eid = await get_or_create_entity(pg_conn, "T206 IGNORE ME MART")
    manual_eid = await get_or_create_entity(pg_conn, "T206 IGNORE MANUAL")
    await _jev(pg_conn, jev_eid, "organization", "trust", 0.9)
    await _jev(pg_conn, manual_eid, "organization", "trust", 0.9)
    await record_manual(
        pg_conn, manual_eid, entity_type="person", entity_subtype=None, created_by="a@x"
    )
    await resolve_entities(pg_conn, [jev_eid])
    assert await _resolved(pg_conn, jev_eid) == ("organization", "trust", "jev")

    result = await set_jev_ignored(pg_conn, ignored=True)
    assert result["rows"] >= 2
    assert result["entities"] >= 2
    assert result["changed"] >= 1
    assert await _resolved(pg_conn, jev_eid) == ("person", None, "regex")
    assert await _resolved(pg_conn, manual_eid) == ("person", None, "manual")
    assert await _ignored_by_source(pg_conn, manual_eid) == {
        "regex": [False],
        "jev": [True],
        "manual": [False],
    }
    # a second ignore touches nothing
    assert (await set_jev_ignored(pg_conn, ignored=True))["rows"] == 0

    restored = await set_jev_ignored(pg_conn, ignored=False)
    assert restored["rows"] >= 2
    assert await _resolved(pg_conn, jev_eid) == ("organization", "trust", "jev")
    assert await _ignored_by_source(pg_conn, jev_eid) == {"regex": [False], "jev": [False]}


@pytest.mark.asyncio(loop_scope="session")
async def test_ignore_by_model_only_touches_that_model(pg_conn):
    a = await get_or_create_entity(pg_conn, "T206 MODEL A")
    b = await get_or_create_entity(pg_conn, "T206 MODEL B")
    await _jev(pg_conn, a, "organization", "trust", 0.9, model="jev-t206-old")
    await _jev(pg_conn, b, "organization", "trust", 0.9, model="jev-t206-new")
    result = await set_jev_ignored(pg_conn, ignored=True, model="jev-t206-old")
    assert (result["rows"], result["entities"]) == (1, 1)
    assert (await _ignored_by_source(pg_conn, a))["jev"] == [True]
    assert (await _ignored_by_source(pg_conn, b))["jev"] == [False]


@pytest.mark.asyncio(loop_scope="session")
async def test_resolve_reads_trusted_subtypes_at_call_time(pg_conn, monkeypatch):
    from wslcb_licensing_tracker import entity_classification

    monkeypatch.delenv(THRESHOLD_ENV, raising=False)
    eid = await get_or_create_entity(pg_conn, "T206 TRUSTED NONPROFIT LLC")
    await _jev(pg_conn, eid, "organization", "nonprofit", 0.95)
    await resolve_entities(pg_conn, [eid])
    assert await _resolved(pg_conn, eid) == ("organization", "business", "jev")
    monkeypatch.setattr(entity_classification, "JEV_TRUSTED_ORG_SUBTYPES", frozenset({"nonprofit"}))
    await resolve_entities(pg_conn, [eid])
    assert await _resolved(pg_conn, eid) == ("organization", "nonprofit", "jev")
