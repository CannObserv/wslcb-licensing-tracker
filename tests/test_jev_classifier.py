"""Tests for jev_classifier.py — classify-once orchestration (#207).

``classify_entities`` commits per batch, so these use ``pg_engine`` and clean
up after themselves: test entities carry the ``JEVTEST`` prefix, and entities
other tests left behind are marked classified (an ignored Jev row) for the
duration, so "unclassified" means this test's entities only.

Jev itself is an ``httpx.MockTransport`` that answers by name:
``LLC`` → organization .95 / business .9; ``LOWCONF`` → person .4;
``NOISEWORD`` → not_a_name .9; ``TRUSTY`` → trust .9; otherwise person .97.
"""

import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, func, insert, select

from wslcb_licensing_tracker import jev_classifier as jc
from wslcb_licensing_tracker.config import ENV_PREFIX
from wslcb_licensing_tracker.entities import get_or_create_entity
from wslcb_licensing_tracker.entity_classification import record_manual
from wslcb_licensing_tracker.jev_client import JevAnswer
from wslcb_licensing_tracker.models import (
    classification_runs,
    entities,
    entity_classifications,
    entity_review_flags,
)

# The engine lives on the session loop; so must every test and fixture using it.
pytestmark = pytest.mark.asyncio(loop_scope="session")

PREFIX = "JEVTEST "
TOKENS_PER_NAME = 10


def _answers_for(name: str) -> tuple[tuple[str, float], tuple[str, float]]:
    if "LLC" in name:
        return ("organization", 0.95), ("business", 0.9)
    if "LOWCONF" in name:
        return ("person", 0.4), ("business", 0.1)
    if "NOISEWORD" in name:
        return ("not_a_name", 0.9), ("business", 0.1)
    if "TRUSTY" in name:
        return ("trust", 0.9), ("business", 0.2)
    return ("person", 0.97), ("business", 0.2)


class FakeJev:
    """A MockTransport handler; ``statuses`` (popped per call) override the 200."""

    def __init__(self, *statuses: int) -> None:
        self.statuses = list(statuses)
        self.batches: list[list[str]] = []

    @property
    def names(self) -> list[str]:
        return [n for b in self.batches for n in b]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        names = [body["state"]["names"][f"n{i}"] for i in range(len(body["state"]["names"]))]
        self.batches.append(names)
        status = self.statuses.pop(0) if self.statuses else 200
        if status != 200:
            return httpx.Response(status, json={})
        answers = {}
        for i, name in enumerate(names):
            (kind, kc), (org, oc) = _answers_for(name)
            answers[f"kind_{i}"] = {"choice": kind, "confidence": kc}
            answers[f"org_{i}"] = {"choice": org, "confidence": oc}
        return httpx.Response(
            200,
            json={
                "model": body["model"],
                "usage": {"input_tokens": TOKENS_PER_NAME * len(names)},
                "answers": answers,
            },
        )

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


@pytest.fixture
def jev_on(monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION", "true")
    monkeypatch.setenv(f"{ENV_PREFIX}TYPESAFE_API_KEY", "k")
    monkeypatch.delenv(f"{ENV_PREFIX}JEV_MODEL", raising=False)
    monkeypatch.delenv(f"{ENV_PREFIX}JEV_MAX_NAMES_PER_RUN", raising=False)
    monkeypatch.delenv(f"{ENV_PREFIX}JEV_CONFIDENCE_THRESHOLD", raising=False)


@pytest_asyncio.fixture(loop_scope="session")
async def isolated(pg_engine):
    """Hide other tests' entities from "unclassified"; clean up ours afterwards."""
    ec = entity_classifications
    async with pg_engine.connect() as conn:
        foreign = (
            (
                await conn.execute(
                    select(entities.c.id).where(
                        ~entities.c.name.startswith(PREFIX)
                        & ~select(ec.c.id)
                        .where((ec.c.entity_id == entities.c.id) & (ec.c.source == "jev"))
                        .exists()
                    )
                )
            )
            .scalars()
            .all()
        )
        marker_ids = []
        if foreign:
            marker_ids = (
                (
                    await conn.execute(
                        insert(ec).returning(ec.c.id),
                        [
                            {
                                "entity_id": eid,
                                "source": "jev",
                                "entity_type": "person",
                                "model": "jevtest-marker",
                                "ignored_at": datetime.now(UTC),
                            }
                            for eid in foreign
                        ],
                    )
                )
                .scalars()
                .all()
            )
        await conn.commit()
    yield pg_engine
    async with pg_engine.connect() as conn:
        await conn.execute(delete(entities).where(entities.c.name.startswith(PREFIX)))
        if marker_ids:
            await conn.execute(delete(ec).where(ec.c.id.in_(marker_ids)))
        await conn.execute(delete(classification_runs))
        await conn.commit()


async def _seed(engine, *names: str) -> list[int]:
    async with engine.connect() as conn:
        ids = [await get_or_create_entity(conn, PREFIX + n) for n in names]
        await conn.commit()
    return ids


async def _run(engine, fake: FakeJev, **kw) -> jc.RunSummary:
    kw.setdefault("origin", "cli")
    async with fake.client() as client, engine.connect() as conn:
        return await jc.classify_entities(conn, client=client, **kw)


async def _jev_rows(engine, eid: int) -> list:
    ec = entity_classifications
    async with engine.connect() as conn:
        return (
            await conn.execute(
                select(ec).where((ec.c.entity_id == eid) & (ec.c.source == "jev")).order_by(ec.c.id)
            )
        ).fetchall()


async def _resolved(engine, eid: int) -> tuple:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                select(
                    entities.c.entity_type,
                    entities.c.entity_subtype,
                    entities.c.classification_source,
                ).where(entities.c.id == eid)
            )
        ).one()


async def _runs(engine) -> list:
    async with engine.connect() as conn:
        return (
            await conn.execute(select(classification_runs).order_by(classification_runs.c.id))
        ).fetchall()


def _ours(names: list[str]) -> list[str]:
    return [n for n in names if n.startswith(PREFIX)]


# ── map_answer ───────────────────────────────────────────────────────


def _a(kind: str, kc: float = 0.9, org: str = "business", oc: float = 0.9) -> JevAnswer:
    return JevAnswer(kind, kc, org, oc, {})


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (_a("trust", org="government"), ("organization", "trust")),
        (_a("estate"), ("organization", "estate")),
        (_a("organization", org="nonprofit", oc=0.8), ("organization", "nonprofit")),
        (_a("organization", org="tribe", oc=0.7), ("organization", "tribe")),
        (_a("organization", org="tribe", oc=0.5), ("organization", "unknown")),
        (_a("person", org="business"), ("person", None)),
        (_a("not_a_name"), ("not_a_name", None)),
    ],
)
async def test_map_answer_table(answer, expected):
    assert jc.map_answer(answer, threshold=0.7) == expected


async def test_estimated_cost():
    dry = jc.RunSummary("cli", enabled=False, key_present=False, model="m", candidates=1000)
    dry.dry_run = True
    assert dry.estimated_cost_usd == pytest.approx(1000 * 238 * 0.042 / 1e6)
    real = jc.RunSummary("cli", enabled=True, key_present=True, model="m", input_tokens=2_000_000)
    assert real.estimated_cost_usd == pytest.approx(0.084)


# ── gates: dry-run, flag, key ────────────────────────────────────────


async def test_dry_run_counts_without_calls_even_when_disabled(isolated, monkeypatch):
    monkeypatch.delenv(f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION", raising=False)
    await _seed(isolated, "DRY ONE", "DRY TWO", "DRY THREE")
    fake = FakeJev()
    s = await _run(isolated, fake, dry_run=True)
    assert fake.batches == []
    assert s.dry_run
    assert s.candidates == 3
    assert not s.enabled
    assert s.estimated_cost_usd == pytest.approx(3 * 238 * 0.042 / 1e6)
    assert await _runs(isolated) == []


async def test_dry_run_respects_limit(isolated, jev_on):
    await _seed(isolated, "LIM ONE", "LIM TWO", "LIM THREE")
    s = await _run(isolated, FakeJev(), dry_run=True, limit=2)
    assert s.candidates == 2


async def test_disabled_makes_no_calls_and_no_run_row(isolated, monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION", "0")
    monkeypatch.setenv(f"{ENV_PREFIX}TYPESAFE_API_KEY", "k")
    await _seed(isolated, "OFF ONE")
    fake = FakeJev()
    s = await _run(isolated, fake)
    assert not s.enabled
    assert fake.batches == []
    assert await _runs(isolated) == []


async def test_missing_key_records_failed_run(isolated, jev_on, monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}TYPESAFE_API_KEY", "")
    await _seed(isolated, "NOKEY ONE")
    fake = FakeJev()
    s = await _run(isolated, fake, origin="post_scrape")
    assert fake.batches == []
    assert s.error == "missing_api_key"
    assert not s.key_present
    (run,) = await _runs(isolated)
    assert run.error == "missing_api_key"
    assert run.origin == "post_scrape"
    assert run.finished_at is not None
    assert s.run_id == run.id


# ── classify once ────────────────────────────────────────────────────


async def test_classifies_once(isolated, jev_on):
    acme, john = await _seed(isolated, "ACME LLC", "JOHN DOE")
    fake = FakeJev()
    first = await _run(isolated, fake)
    assert sorted(_ours(fake.names)) == [PREFIX + "ACME LLC", PREFIX + "JOHN DOE"]
    assert first.names_sent == 2
    assert first.batches_ok == 1
    (row,) = await _jev_rows(isolated, acme)
    assert row.entity_type == "organization"
    assert row.entity_subtype == "business"
    assert row.confidence == pytest.approx(0.95)
    assert row.probabilities["kind"] == {"choice": "organization", "confidence": 0.95}
    assert row.probabilities["org"]["choice"] == "business"
    assert row.model == "jev-1.14.0"
    assert row.input_tokens == pytest.approx(TOKENS_PER_NAME)

    second = await _run(isolated, fake)
    assert second.names_sent == 0
    assert len(fake.batches) == 1
    assert len(await _jev_rows(isolated, john)) == 1


async def test_bulk_modes_skip_manual_but_admin_may_query(isolated, jev_on):
    (eid,) = await _seed(isolated, "MANUAL LLC")
    async with isolated.connect() as conn:
        await record_manual(
            conn, eid, entity_type="person", entity_subtype=None, created_by="t@example.com"
        )
        await conn.commit()
    for kw in (
        {"mode": "unclassified"},
        {"mode": "refresh_all"},
        {"mode": "refresh_model", "model_filter": "jev-1.14.0"},
        {"mode": "ids", "ids": [eid]},
    ):
        fake = FakeJev()
        await _run(isolated, fake, **kw)
        assert PREFIX + "MANUAL LLC" not in fake.names, kw

    fake = FakeJev()
    s = await _run(isolated, fake, origin="admin", mode="ids", ids=[eid], include_manual=True)
    assert fake.names == [PREFIX + "MANUAL LLC"]
    assert s.batches_ok == 1
    assert len(await _jev_rows(isolated, eid)) == 1
    assert await _resolved(isolated, eid) == ("person", None, "manual")


async def test_refresh_model_selects_only_that_model(isolated, jev_on, monkeypatch):
    old, new = await _seed(isolated, "OLD MODEL", "NEW MODEL")
    monkeypatch.setenv(f"{ENV_PREFIX}JEV_MODEL", "jev-old")
    await _run(isolated, FakeJev(), mode="ids", ids=[old])
    monkeypatch.setenv(f"{ENV_PREFIX}JEV_MODEL", "jev-new")
    await _run(isolated, FakeJev(), mode="ids", ids=[new])

    fake = FakeJev()
    await _run(isolated, fake, mode="refresh_model", model_filter="jev-old")
    assert fake.names == [PREFIX + "OLD MODEL"]
    assert [r.model for r in await _jev_rows(isolated, old)] == ["jev-old", "jev-new"]


async def test_mode_arguments_are_required(isolated, jev_on):
    async with isolated.connect() as conn:
        with pytest.raises(ValueError, match="model"):
            await jc.select_candidates(conn, mode="refresh_model")
        with pytest.raises(ValueError, match="ids"):
            await jc.select_candidates(conn, mode="ids")


async def test_ignored_jev_rows_are_not_resent(isolated, jev_on):
    (eid,) = await _seed(isolated, "IGNORED ONE")
    await _run(isolated, FakeJev())
    async with isolated.connect() as conn:
        await conn.execute(
            entity_classifications.update()
            .where(entity_classifications.c.entity_id == eid)
            .where(entity_classifications.c.source == "jev")
            .values(ignored_at=func.now())
        )
        await conn.commit()
    fake = FakeJev()
    await _run(isolated, fake)
    assert fake.batches == []


async def test_noise_names_are_not_candidates_unless_protected(isolated, jev_on):
    """The reclassify purge deletes unprotected noise names, and their paid
    answers with them (#206's note for #207)."""
    (real,) = await _seed(isolated, "REAL NAME")
    async with isolated.connect() as conn:  # no letters: noise, outside the JEVTEST prefix
        noise, flagged = (
            (
                await conn.execute(
                    insert(entities).returning(entities.c.id),
                    [
                        {"name": "9999901", "entity_type": "person"},
                        {"name": "9999902", "entity_type": "person"},
                    ],
                )
            )
            .scalars()
            .all()
        )
        await conn.execute(
            insert(entity_review_flags).values(entity_id=flagged, flagged_by="t@example.com")
        )
        await conn.commit()
    try:
        async with isolated.connect() as conn:
            picked = await jc.select_candidates(conn, mode="unclassified")
            ids = [eid for eid, _ in picked]
            assert real in ids
            assert noise not in ids
            assert flagged in ids
            assert await jc.awaiting_count(conn) == len(picked)
    finally:
        async with isolated.connect() as conn:
            await conn.execute(delete(entities).where(entities.c.id.in_([noise, flagged])))
            await conn.commit()


async def test_limit_and_default_cap(isolated, jev_on, monkeypatch):
    await _seed(isolated, *(f"CAP {i:02d}" for i in range(5)))
    fake = FakeJev()
    s = await _run(isolated, fake, limit=2)
    assert s.names_sent == 2
    assert _ours(fake.names) == [PREFIX + "CAP 00", PREFIX + "CAP 01"]  # ordered by id

    monkeypatch.setenv(f"{ENV_PREFIX}JEV_MAX_NAMES_PER_RUN", "1")
    fake = FakeJev()
    s = await _run(isolated, fake)
    assert s.names_sent == 1
    assert _ours(fake.names) == [PREFIX + "CAP 02"]


# ── failures ─────────────────────────────────────────────────────────


async def test_failed_batch_stores_nothing_and_is_retried(isolated, jev_on):
    (eid,) = await _seed(isolated, "RETRY ME")
    s = await _run(isolated, FakeJev(500))
    assert s.batches_failed == 1
    assert s.batches_ok == 0
    assert await _jev_rows(isolated, eid) == []
    s = await _run(isolated, FakeJev())
    assert s.batches_ok == 1
    assert len(await _jev_rows(isolated, eid)) == 1


async def test_auth_failure_stops_run(isolated, jev_on):
    await _seed(isolated, *(f"AUTH {i:02d}" for i in range(60)))  # 3 batches
    fake = FakeJev(401, 401, 401)
    s = await _run(isolated, fake, concurrency=1)
    assert len(fake.batches) == 1
    assert s.error == "auth_failed"
    assert s.batches_failed == 1
    (run,) = await _runs(isolated)
    assert run.error == "auth_failed"
    assert run.finished_at is not None


async def test_consecutive_failures_stop_run(isolated, jev_on):
    await _seed(isolated, *(f"FAIL {i:03d}" for i in range(110)))  # 5 batches
    fake = FakeJev(500, 500, 500, 500, 500)
    s = await _run(isolated, fake, concurrency=1)
    assert len(fake.batches) == jc.MAX_CONSECUTIVE_FAILURES == 3
    assert s.error == "consecutive_failures"
    assert s.batches_failed == 3


async def test_a_success_resets_the_failure_count(isolated, jev_on):
    await _seed(isolated, *(f"MIXED {i:03d}" for i in range(110)))  # 5 batches
    fake = FakeJev(500, 500, 200, 500, 500)
    s = await _run(isolated, fake, concurrency=1)
    assert len(fake.batches) == 5
    assert s.error is None
    assert (s.batches_ok, s.batches_failed) == (1, 4)


async def test_concurrent_requests_store_every_batch(isolated, jev_on):
    """HTTP runs concurrently; the DB writes stay serial on the one connection."""
    ids = await _seed(isolated, *(f"PAR {i:03d}" for i in range(110)))
    s = await _run(isolated, FakeJev(), concurrency=8)
    assert (s.batches_ok, s.names_sent) == (5, 110)
    ec = entity_classifications
    async with isolated.connect() as conn:
        stored = (
            await conn.execute(
                select(func.count()).where((ec.c.source == "jev") & ec.c.entity_id.in_(ids))
            )
        ).scalar_one()
    assert stored == 110


# ── the advisory lock ────────────────────────────────────────────────


async def test_overlapping_run_is_skipped(isolated, jev_on):
    await _seed(isolated, "LOCKED ONE")
    async with isolated.connect() as holder:
        await holder.execute(select(func.pg_advisory_lock(jc.ADVISORY_LOCK_KEY)))
        await holder.commit()
        try:
            fake = FakeJev()
            s = await _run(isolated, fake)
            assert s.skipped_locked
            assert fake.batches == []
            assert await _runs(isolated) == []
        finally:
            await holder.execute(select(func.pg_advisory_unlock(jc.ADVISORY_LOCK_KEY)))
            await holder.commit()


async def _lock_is_free(engine) -> bool:
    async with engine.connect() as conn:
        got = (
            await conn.execute(select(func.pg_try_advisory_lock(jc.ADVISORY_LOCK_KEY)))
        ).scalar_one()
        if got:
            await conn.execute(select(func.pg_advisory_unlock(jc.ADVISORY_LOCK_KEY)))
        await conn.commit()
        return got


async def test_lock_is_released_after_a_finished_run(isolated, jev_on):
    """A leaked session lock would make every later run skip, silently."""
    await _seed(isolated, "AFTER ONE", "AFTER TWO")
    await _run(isolated, FakeJev(), limit=1)
    assert await _lock_is_free(isolated)
    s = await _run(isolated, FakeJev())
    assert not s.skipped_locked
    assert s.names_sent == 1


async def test_lock_is_released_after_a_run_that_raises(isolated, jev_on, monkeypatch):
    await _seed(isolated, "BOOM ONE")

    async def boom(*_a, **_kw):
        msg = "db went away"
        raise RuntimeError(msg)

    monkeypatch.setattr(jc, "store_batch", boom)
    with pytest.raises(RuntimeError, match="db went away"):
        await _run(isolated, FakeJev())
    assert await _lock_is_free(isolated)
    (run,) = await _runs(isolated)
    assert run.error == "RuntimeError"
    assert run.finished_at is not None


async def test_unreleasable_lock_invalidates_the_connection(isolated, jev_on, monkeypatch):
    """If the unlock can't be confirmed, the session ends rather than return to the pool."""
    await _seed(isolated, "STUCK ONE")
    real_unlock = jc._unlock

    async def failing_unlock(conn):
        await real_unlock(conn)  # really release, so the test leaves no lock behind
        return False

    monkeypatch.setattr(jc, "_unlock", failing_unlock)
    async with FakeJev().client() as client, isolated.connect() as conn:
        await jc.classify_entities(conn, origin="cli", client=client)
        assert conn.invalidated


# ── storing and resolution ───────────────────────────────────────────


async def test_store_batch_skips_deleted_entity(isolated, jev_on):
    keep, gone = await _seed(isolated, "KEEP ME", "GONE SOON")
    fake = FakeJev()
    async with fake.client() as client:
        from wslcb_licensing_tracker.jev_client import classify_batch

        batch = await classify_batch(
            [PREFIX + "KEEP ME", PREFIX + "GONE SOON"],
            api_key="k",
            model="jev-1.14.0",
            client=client,
        )
    async with isolated.connect() as conn:
        await conn.execute(delete(entities).where(entities.c.id == gone))
        stored = await jc.store_batch(
            conn, [(keep, PREFIX + "KEEP ME"), (gone, PREFIX + "GONE SOON")], batch, threshold=0.7
        )
        await conn.commit()
    assert stored == [keep]
    (row,) = await _jev_rows(isolated, keep)
    assert row.input_tokens == pytest.approx(TOKENS_PER_NAME)  # the batch's share per name sent


async def test_resolution_and_not_a_name(isolated, jev_on):
    llc, noise, low, trusty = await _seed(isolated, "BIG LLC", "NOISEWORD", "LOWCONF", "TRUSTY BOB")
    await _run(isolated, FakeJev())
    assert await _resolved(isolated, llc) == ("organization", "business", "jev")
    (row,) = await _jev_rows(isolated, noise)
    assert row.entity_type == "not_a_name"
    assert (await _resolved(isolated, noise))[2] == "regex"
    assert await _resolved(isolated, low) == ("person", None, "regex")
    assert await _resolved(isolated, trusty) == ("organization", "trust", "jev")


async def test_run_row_totals(isolated, jev_on):
    await _seed(isolated, *(f"TOTAL {i:02d}" for i in range(30)))
    s = await _run(isolated, FakeJev(), origin="post_import")
    (run,) = await _runs(isolated)
    assert run.id == s.run_id
    assert run.origin == "post_import"
    assert run.model == "jev-1.14.0"
    assert (run.names_sent, run.batches_ok, run.batches_failed) == (30, 2, 0)
    assert run.input_tokens == 30 * TOKENS_PER_NAME == s.input_tokens
    assert run.finished_at is not None
    assert run.error is None


async def test_no_candidates_still_records_a_run(isolated, jev_on):
    s = await _run(isolated, FakeJev())
    assert s.candidates == 0
    (run,) = await _runs(isolated)
    assert run.names_sent == 0
    assert run.finished_at is not None


async def test_runs_never_overlap_in_one_process(isolated, jev_on):
    """Two runs started together: one sends, the other skips; nothing is sent twice."""
    await _seed(isolated, *(f"RACE {i:02d}" for i in range(30)))
    fake = FakeJev()
    a, b = await asyncio.gather(_run(isolated, fake), _run(isolated, fake))
    assert sorted([a.skipped_locked, b.skipped_locked]) in ([False, True], [False, False])
    ours = _ours(fake.names)
    assert len(ours) == len(set(ours)) == 30


# ── check-in payload ─────────────────────────────────────────────────

_VARIABLES = {
    "outcome",
    "source_host",
    "enabled",
    "model",
    "names_sent",
    "batches_ok",
    "batches_failed",
    "backlog",
    "error",
}


def _s(**kw) -> jc.RunSummary:
    base = {"origin": "cli", "enabled": True, "key_present": True, "model": "jev-1.14.0"}
    return jc.RunSummary(**{**base, **kw})


@pytest.mark.parametrize(
    ("case", "summary", "backlog", "status", "fault"),
    [
        ("disabled_ok", _s(enabled=False, error="ignored"), 70_000, "ok", None),
        (
            "missing_key",
            _s(key_present=False, error="missing_api_key"),
            0,
            "alert",
            "missing_api_key",
        ),
        ("auth_failed", _s(error="auth_failed", batches_failed=1), 0, "alert", "auth_failed"),
        ("batches_failed", _s(batches_ok=3, batches_failed=1), 0, "alert", "batches_failed"),
        ("backlog", _s(batches_ok=200), jc.BACKLOG_ALERT_THRESHOLD + 1, "alert", "backlog"),
        ("backlog_at_threshold", _s(), jc.BACKLOG_ALERT_THRESHOLD, "ok", None),
        ("clean_ok", _s(names_sent=30, batches_ok=2), 0, "ok", None),
        ("weekly_locked_alerts", _s(skipped_locked=True), 0, "alert", "locked"),
        ("post_scrape_locked_ok", _s(origin="post_scrape", skipped_locked=True), 0, "ok", None),
    ],
)
async def test_checkin_payload(case, summary, backlog, status, fault):
    got_status, variables, metadata = jc.checkin_payload(summary, backlog=backlog, host="h1")
    assert got_status == status, case
    assert set(variables) == _VARIABLES
    assert variables["source_host"] == "h1"
    assert variables["backlog"] == backlog
    assert variables["enabled"] == ("true" if summary.enabled else "false")
    if fault is None:
        assert metadata is None
        assert variables["outcome"] == "ok"
        assert variables["error"] == ""
    else:
        assert metadata == {"fault": fault}
        assert variables["outcome"] == "failed"
        assert variables["error"] == fault


async def test_exception_payload(monkeypatch):
    monkeypatch.setenv(f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION", "1")
    status, variables, metadata = jc.exception_payload(
        RuntimeError("db went away\nsecond line"), host="h1"
    )
    assert status == "alert"
    assert metadata == {"fault": "RuntimeError"}
    assert set(variables) == _VARIABLES
    assert variables["error"] == "RuntimeError: db went away"
    assert variables["enabled"] == "true"
    assert variables["outcome"] == "failed"
