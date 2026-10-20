"""Tests for scraper.py — pure helper logic and behavioral DB tests."""

import gzip
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from sqlalchemy import text

from wslcb_licensing_tracker.scraper import (
    STALE_AFTER,
    ScrapeHealth,
    ScrapeOutcome,
    alert_variables,
    cleanup_redundant_scrapes,
    compute_content_hash,
    fault_key,
    get_last_content_hash,
    ok_variables,
    save_html_snapshot,
    scrape,
    scrape_freshness,
    scrape_health,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)


def test_compute_content_hash_is_deterministic():
    """compute_content_hash returns the same hash for the same input."""
    assert compute_content_hash("<html>test</html>") == compute_content_hash("<html>test</html>")


def test_get_last_content_hash_is_importable():
    """scraper.get_last_content_hash is an async function."""
    import inspect

    assert inspect.iscoroutinefunction(get_last_content_hash)


@pytest.mark.asyncio
async def test_get_last_content_hash_returns_none_when_no_eligible_rows():
    """get_last_content_hash returns None when no eligible rows exist."""
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_result
    assert await get_last_content_hash(mock_conn) is None


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_cleanup_redundant_scrapes_removes_unchanged_rows(pg_engine):
    """cleanup_redundant_scrapes deletes unchanged scrape_log rows with no linked sources,
    once a later success supersedes them."""
    async with pg_engine.connect() as conn:
        row = await conn.execute(
            text(
                "INSERT INTO scrape_log (started_at, status) "
                "VALUES (:started_at, 'unchanged') RETURNING id"
            ),
            {"started_at": datetime.now(UTC)},
        )
        inserted_id = row.scalar_one()
        success = await conn.execute(
            text(
                "INSERT INTO scrape_log (started_at, status) "
                "VALUES (:started_at, 'success') RETURNING id"
            ),
            {"started_at": datetime.now(UTC)},
        )
        success_id = success.scalar_one()
        await conn.commit()

    result = await cleanup_redundant_scrapes(pg_engine, delete_files=False)

    assert result["scrape_logs"] >= 1
    assert result["files"] == 0

    # Verify the specific row was removed
    async with pg_engine.connect() as conn:
        remaining = await conn.execute(
            text("SELECT id FROM scrape_log WHERE id = :id"),
            {"id": inserted_id},
        )
        assert remaining.fetchone() is None
        await conn.execute(text("DELETE FROM scrape_log WHERE id = :id"), {"id": success_id})
        await conn.commit()


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_cleanup_redundant_scrapes_keeps_unchanged_rows_since_the_last_success(pg_engine):
    """Those rows are the current state: deleting them would move scrape_health's
    last_success_at back, and could flip /api/v1/health's ``stale`` (#197)."""
    async with pg_engine.connect() as conn:
        ids = []
        for status in ("success", "unchanged", "error", "unchanged"):
            row = await conn.execute(
                text(
                    "INSERT INTO scrape_log (started_at, finished_at, status) "
                    "VALUES (now(), now(), :status) RETURNING id"
                ),
                {"status": status},
            )
            ids.append(row.scalar_one())
        await conn.commit()
        before = await scrape_health(conn)

    try:
        await cleanup_redundant_scrapes(pg_engine, delete_files=False)
        async with pg_engine.connect() as conn:
            kept = await conn.execute(
                text("SELECT id FROM scrape_log WHERE id = ANY(:ids) ORDER BY id"),
                {"ids": ids},
            )
            assert [r[0] for r in kept] == ids
            assert await scrape_health(conn) == before
    finally:
        async with pg_engine.connect() as conn:
            await conn.execute(text("DELETE FROM scrape_log WHERE id = ANY(:ids)"), {"ids": ids})
            await conn.commit()


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_cleanup_keeps_the_servers_last_success_past_a_relay(pg_engine):
    """A relay success (#201) must not supersede the server's own unchanged row:
    it anchors scrape_health's server error count."""
    rows = [
        ("success", "server"),
        ("error", "server"),
        ("unchanged", "server"),  # the server's last success
        ("success", "relay"),
        ("error", "server"),
    ]
    async with pg_engine.connect() as conn:
        ids = []
        for status, origin in rows:
            row = await conn.execute(
                text(
                    "INSERT INTO scrape_log (started_at, finished_at, status, origin, captured_at)"
                    " VALUES (now(), now(), :status, :origin,"
                    " CASE WHEN :origin = 'relay' THEN now() END) RETURNING id"
                ),
                {"status": status, "origin": origin},
            )
            ids.append(row.scalar_one())
        await conn.commit()
        before = await scrape_health(conn)

    try:
        await cleanup_redundant_scrapes(pg_engine, delete_files=False)
        async with pg_engine.connect() as conn:
            kept = await conn.execute(
                text("SELECT id FROM scrape_log WHERE id = ANY(:ids) ORDER BY id"),
                {"ids": ids},
            )
            assert ids[2] in [r[0] for r in kept]
            assert await scrape_health(conn) == before
    finally:
        async with pg_engine.connect() as conn:
            await conn.execute(text("DELETE FROM scrape_log WHERE id = ANY(:ids)"), {"ids": ids})
            await conn.commit()


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_fail_log_recovers_an_aborted_transaction(pg_engine):
    """A DB error mid-run leaves the transaction aborted; the error row still lands."""
    from wslcb_licensing_tracker.scraper import fail_log, start_log

    async with pg_engine.connect() as conn:
        log_id = await start_log(conn)
        with pytest.raises(Exception):  # noqa: B017 — any DB error aborts the txn
            await conn.execute(text("SELECT 1/0"))
        await fail_log(conn, log_id, RuntimeError("boom"))
    async with pg_engine.connect() as conn:
        status = (
            await conn.execute(text("SELECT status FROM scrape_log WHERE id = :id"), {"id": log_id})
        ).scalar_one()
        await conn.execute(text("DELETE FROM scrape_log WHERE id = :id"), {"id": log_id})
        await conn.commit()
    assert status == "error"


# ── save_html_snapshot ───────────────────────────────────────────────


def test_save_html_snapshot_creates_gz(tmp_path, monkeypatch):
    """save_html_snapshot writes a .html.gz file, not .html."""
    import wslcb_licensing_tracker.scraper as mod

    monkeypatch.setattr(mod, "DATA_DIR", tmp_path)
    html = "<html><body>test</body></html>"
    scrape_time = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

    path = save_html_snapshot(html, scrape_time)

    assert path.name.endswith(".html.gz"), f"Expected .html.gz, got {path.name}"
    assert path.exists()
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        assert fh.read() == html


def test_save_html_snapshot_increments_version(tmp_path, monkeypatch):
    """save_html_snapshot increments version when same-date file exists."""
    import wslcb_licensing_tracker.scraper as mod

    monkeypatch.setattr(mod, "DATA_DIR", tmp_path)
    scrape_time = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

    p1 = save_html_snapshot("v1", scrape_time)
    p2 = save_html_snapshot("v2", scrape_time)

    assert "v1" in p1.name
    assert "v2" in p2.name


# ── scrape outcome + health, for the co-status check-in (#196) ───────


async def _fresh_scrape_log(conn, *rows):
    """Empty scrape_log inside the test's transaction, then insert *rows*.

    Each row is ``(status, started_at)`` for a server run, or
    ``(status, started_at, captured_at)`` for a relay (#201); ids ascend in
    the order given.
    """
    await conn.execute(text("UPDATE sources SET scrape_log_id = NULL"))
    await conn.execute(text("DELETE FROM scrape_log"))
    for status, started_at, *relay in rows:
        await conn.execute(
            text(
                "INSERT INTO scrape_log (started_at, finished_at, status, origin, captured_at) "
                "VALUES (:at, :at, :status, :origin, :captured)"
            ),
            {
                "at": started_at,
                "status": status,
                "origin": "relay" if relay else "server",
                "captured": relay[0] if relay else None,
            },
        )


_T0 = datetime(2026, 10, 12, 7, 31, tzinfo=UTC)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
class TestScrapeHealth:
    async def test_no_history(self, pg_conn):
        await _fresh_scrape_log(pg_conn)
        assert await scrape_health(pg_conn) == ScrapeHealth(None, 0)

    async def test_errors_since_the_last_success(self, pg_conn):
        """unchanged is a success: the page was fetched. Earlier errors don't count."""
        await _fresh_scrape_log(
            pg_conn,
            ("error", _T0 - timedelta(days=2)),
            ("success", _T0),
            ("unchanged", _T0 + timedelta(hours=6)),
            ("error", _T0 + timedelta(hours=24)),
            ("error", _T0 + timedelta(hours=30)),
        )
        assert await scrape_health(pg_conn) == ScrapeHealth(_T0 + timedelta(hours=6), 2)

    async def test_running_rows_are_not_errors(self, pg_conn):
        """A run killed mid-scrape leaves 'running'; it is no evidence either way."""
        await _fresh_scrape_log(
            pg_conn,
            ("success", _T0),
            ("running", _T0 + timedelta(hours=6)),
            ("error", _T0 + timedelta(hours=24)),
        )
        assert await scrape_health(pg_conn) == ScrapeHealth(_T0, 1)

    async def test_never_succeeded(self, pg_conn):
        await _fresh_scrape_log(pg_conn, ("error", _T0), ("error", _T0 + timedelta(hours=6)))
        assert await scrape_health(pg_conn) == ScrapeHealth(None, 2)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
class TestScrapeHealthWithRelays:
    """Relays (#201) refresh the data but say nothing about the server's own fetch."""

    async def test_relay_success_is_fresh_data_dated_by_capture(self, pg_conn):
        captured = _T0 + timedelta(hours=23, minutes=40)
        await _fresh_scrape_log(
            pg_conn,
            ("success", _T0),
            ("error", _T0 + timedelta(hours=23)),
            ("success", _T0 + timedelta(hours=24), captured),  # imported 20 min later
        )
        assert (await scrape_health(pg_conn)).last_success_at == captured

    async def test_server_errors_keep_counting_across_relays(self, pg_conn):
        await _fresh_scrape_log(
            pg_conn,
            ("success", _T0),
            ("error", _T0 + timedelta(hours=23)),
            ("success", _T0 + timedelta(hours=23, minutes=10), _T0 + timedelta(hours=23)),
            ("error", _T0 + timedelta(hours=29)),
            ("unchanged", _T0 + timedelta(hours=29, minutes=10), _T0 + timedelta(hours=29)),
        )
        assert (await scrape_health(pg_conn)).consecutive_errors == 2

    async def test_relay_rejections_are_not_server_errors(self, pg_conn):
        await _fresh_scrape_log(
            pg_conn,
            ("success", _T0),
            ("error", _T0 + timedelta(hours=6), _T0 + timedelta(hours=6)),
        )
        assert await scrape_health(pg_conn) == ScrapeHealth(_T0, 0)

    async def test_latest_capture_wins_not_latest_row(self, pg_conn):
        """A relay imported after a server success, but captured before it, doesn't
        pull freshness backwards."""
        await _fresh_scrape_log(
            pg_conn,
            ("success", _T0 + timedelta(hours=6)),
            ("unchanged", _T0 + timedelta(hours=6, minutes=5), _T0 + timedelta(hours=1)),
        )
        assert (await scrape_health(pg_conn)).last_success_at == _T0 + timedelta(hours=6)

    async def test_freshness_last_attempt_is_the_servers(self, pg_conn):
        await _fresh_scrape_log(
            pg_conn,
            ("error", _T0),
            ("success", _T0 + timedelta(minutes=10), _T0 + timedelta(minutes=9)),
        )
        freshness = await scrape_freshness(pg_conn, now=_T0 + timedelta(hours=1))
        assert freshness["last_attempt_status"] == "error"
        assert freshness["consecutive_errors"] == 1
        assert freshness["stale"] is False
        assert freshness["last_success_at"] == _T0 + timedelta(minutes=9)


# ── scrape freshness, for /api/v1/health (#197) ───────────────────────


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
class TestScrapeFreshness:
    async def test_fresh(self, pg_conn):
        await _fresh_scrape_log(pg_conn, ("success", _T0), ("error", _T0 + timedelta(hours=6)))
        assert await scrape_freshness(pg_conn, now=_T0 + timedelta(hours=7)) == {
            "last_success_at": _T0,
            "last_attempt_status": "error",
            "consecutive_errors": 1,
            "stale": False,
            "stale_after_hours": 26,
        }

    async def test_stale(self, pg_conn):
        """Down since 2026-10-13: two days of 403s behind the last good fetch."""
        await _fresh_scrape_log(
            pg_conn,
            ("unchanged", _T0),
            *(("error", _T0 + timedelta(hours=h)) for h in (6, 24, 30, 48)),
        )
        freshness = await scrape_freshness(pg_conn, now=_T0 + timedelta(hours=49))
        assert freshness["stale"] is True
        assert freshness["last_success_at"] == _T0
        assert freshness["consecutive_errors"] == 4

    async def test_stale_only_past_the_threshold(self, pg_conn):
        """At exactly STALE_AFTER the monitor hasn't alerted yet; a second later it has."""
        await _fresh_scrape_log(pg_conn, ("success", _T0))
        at_edge = await scrape_freshness(pg_conn, now=_T0 + STALE_AFTER)
        past_edge = await scrape_freshness(pg_conn, now=_T0 + STALE_AFTER + timedelta(seconds=1))
        assert at_edge["stale"] is False
        assert past_edge["stale"] is True

    async def test_in_progress_run_is_the_last_attempt(self, pg_conn):
        await _fresh_scrape_log(pg_conn, ("success", _T0), ("running", _T0 + timedelta(hours=6)))
        freshness = await scrape_freshness(pg_conn, now=_T0 + timedelta(hours=6))
        assert freshness["last_attempt_status"] == "running"
        assert freshness["stale"] is False

    async def test_no_history(self, pg_conn):
        """Nothing ever fetched is nothing fresh."""
        await _fresh_scrape_log(pg_conn)
        assert await scrape_freshness(pg_conn, now=_T0) == {
            "last_success_at": None,
            "last_attempt_status": None,
            "consecutive_errors": 0,
            "stale": True,
            "stale_after_hours": 26,
        }


class _FakeResponse:
    def __init__(self, status_code, body=""):
        self.status_code = status_code
        self.text = body
        self._request = httpx.Request("GET", "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp")

    def raise_for_status(self):
        if self.status_code >= 400:
            response = httpx.Response(self.status_code, request=self._request)
            raise httpx.HTTPStatusError("Client error", request=self._request, response=response)


def _fake_client(response):
    class _Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

        async def get(self, _url):
            return response

    return _Client


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
class TestScrapeOutcome:
    async def test_unchanged_page_returns_its_outcome(self, pg_engine, monkeypatch):
        import wslcb_licensing_tracker.scraper as mod

        body = "<html>same</html>"
        monkeypatch.setattr(mod.httpx, "AsyncClient", _fake_client(_FakeResponse(200, body)))
        monkeypatch.setattr(
            mod, "get_last_content_hash", AsyncMock(return_value=compute_content_hash(body))
        )

        outcome = await scrape(pg_engine)

        assert outcome.status == "unchanged"
        assert (outcome.new, outcome.approved, outcome.discontinued, outcome.skipped) == (
            0,
            0,
            0,
            0,
        )
        async with pg_engine.connect() as conn:
            status = (
                await conn.execute(
                    text("SELECT status FROM scrape_log WHERE id = :id"),
                    {"id": outcome.scrape_log_id},
                )
            ).scalar_one()
        assert status == "unchanged"

    async def test_changed_page_ingests_under_a_live_scrape_source(
        self, pg_engine, monkeypatch, tmp_path
    ):
        """A changed page is archived, ingested, and attributed to a live_scrape source."""
        import wslcb_licensing_tracker.scraper as mod

        body = (FIXTURES_DIR / "full_snapshot.html").read_text()
        monkeypatch.setattr(mod, "DATA_DIR", tmp_path)
        monkeypatch.setattr(mod.httpx, "AsyncClient", _fake_client(_FakeResponse(200, body)))
        monkeypatch.setattr(mod, "get_last_content_hash", AsyncMock(return_value=None))

        outcome = await scrape(pg_engine)

        assert outcome.status == "success"
        assert outcome.new + outcome.approved + outcome.discontinued + outcome.skipped == 3
        async with pg_engine.connect() as conn:
            log = (
                await conn.execute(
                    text(
                        "SELECT status, origin, captured_at, content_hash FROM scrape_log"
                        " WHERE id = :id"
                    ),
                    {"id": outcome.scrape_log_id},
                )
            ).one()
            src = (
                await conn.execute(
                    text(
                        "SELECT st.slug, s.url, s.snapshot_path, s.captured_at, s.metadata"
                        " FROM sources s JOIN source_types st ON st.id = s.source_type_id"
                        " WHERE s.scrape_log_id = :id"
                    ),
                    {"id": outcome.scrape_log_id},
                )
            ).one()
        assert (log.status, log.origin, log.captured_at) == ("success", "server", None)
        assert log.content_hash == compute_content_hash(body)
        assert src.slug == "live_scrape"
        assert src.url == "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp"
        assert src.captured_at is not None
        assert src.metadata == "{}"
        assert (tmp_path / src.snapshot_path).exists()

    async def test_http_error_still_raises_and_logs(self, pg_engine, monkeypatch):
        """The exit status is the unit's failure signal; the check-in doesn't replace it."""
        import wslcb_licensing_tracker.scraper as mod

        monkeypatch.setattr(mod.httpx, "AsyncClient", _fake_client(_FakeResponse(403)))
        with pytest.raises(httpx.HTTPStatusError):
            await scrape(pg_engine)
        async with pg_engine.connect() as conn:
            status = (
                await conn.execute(text("SELECT status FROM scrape_log ORDER BY id DESC LIMIT 1"))
            ).scalar_one()
        assert status == "error"


def _http_error(code):
    request = httpx.Request("GET", "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp")
    response = httpx.Response(code, request=request)
    message = (
        f"Client error '{code} Forbidden' for url '{request.url}'\n"
        "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403"
    )
    return httpx.HTTPStatusError(message, request=request, response=response)


class TestFaultKey:
    """``metadata.fault``: the stable part of a failure, so a new kind reports at once."""

    def test_http_status_carries_the_code(self):
        assert fault_key(_http_error(403)) == "HTTPStatusError 403"
        assert fault_key(_http_error(503)) == "HTTPStatusError 503"

    def test_other_errors_are_their_type(self):
        assert fault_key(httpx.ConnectError("[Errno -3] Temporary failure")) == "ConnectError"
        assert fault_key(ValueError("Could not find data tables in page")) == "ValueError"


class TestCheckinVariables:
    def test_ok(self):
        outcome = ScrapeOutcome(
            scrape_log_id=565, status="success", new=3, approved=1, discontinued=2, skipped=40
        )
        assert ok_variables(outcome, "host") == {
            "outcome": "ok",
            "source_host": "host",
            "scrape_status": "success",
            "records_new": 3,
            "records_approved": 1,
            "records_discontinued": 2,
            "records_skipped": 40,
            "scrape_log_id": 565,
        }

    def test_alert_carries_the_deadline(self):
        """window_expires: when the 30-day source window starts dropping the gap."""
        health = ScrapeHealth(datetime(2026, 10, 12, 13, 34, 1, 160977, tzinfo=UTC), 4)
        assert alert_variables(_http_error(403), health, "host") == {
            "outcome": "failed",
            "source_host": "host",
            "error": "HTTPStatusError: Client error '403 Forbidden' for url "
            "'https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp'",
            "consecutive_errors": 4,
            "last_success_at": "2026-10-12T13:34:01Z",
            "window_expires": "2026-11-11",
        }

    def test_alert_without_history_keeps_every_key(self):
        """The monitor's template names every key; a missing one could break the render."""
        variables = alert_variables(ValueError(""), None, "host")
        assert variables["error"] == "ValueError"
        assert (
            variables["consecutive_errors"],
            variables["last_success_at"],
            variables["window_expires"],
        ) == ("", "", "")

    def test_alert_never_succeeded(self):
        variables = alert_variables(ValueError("x"), ScrapeHealth(None, 2), "host")
        assert (
            variables["consecutive_errors"],
            variables["last_success_at"],
            variables["window_expires"],
        ) == (2, "", "")

    def test_alert_survives_an_exception_that_cannot_be_printed(self):
        """str(exc) is the one foreign call here; if it raises, the alert still goes."""

        class UnprintableError(Exception):
            def __str__(self):
                raise RuntimeError("no")

        variables = alert_variables(UnprintableError(), ScrapeHealth(None, 1), "host")
        assert variables["error"] == "UnprintableError"
        assert variables["consecutive_errors"] == 1
