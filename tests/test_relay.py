"""Tests for relay.py — importing operator-relayed captures of the WSLCB page (#201)."""

import gzip
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text

from wslcb_licensing_tracker import relay
from wslcb_licensing_tracker.relay import (
    RelayRejectedError,
    capture_name,
    import_inbox,
    inbox_dir,
    parse_capture_name,
    validate_report,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 10, 20, 14, 0, tzinfo=UTC)

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)


def _page(nonce: str) -> str:
    """The three-section fixture page, made unique so its hash is new."""
    body = (FIXTURES_DIR / "full_snapshot.html").read_text()
    return body.replace("<html>", f"<html><!-- {nonce} -->", 1)


def _drop(inbox: Path, captured_at: datetime, html: str) -> Path:
    inbox.mkdir(parents=True, exist_ok=True)
    path = inbox / capture_name(captured_at)
    path.write_bytes(gzip.compress(html.encode()))
    return path


class TestCaptureName:
    def test_round_trip(self):
        when = datetime(2026, 10, 20, 13, 40, 5, tzinfo=UTC)
        assert capture_name(when) == "20261020T134005Z.html.gz"
        assert parse_capture_name("20261020T134005Z.html.gz") == when

    @pytest.mark.parametrize("name", ["capture.html.gz", "20261020T134005Z.html", "2026.html.gz"])
    def test_other_names_rejected(self, name):
        with pytest.raises(RelayRejectedError, match="not a capture name"):
            parse_capture_name(name)


class TestInboxDir:
    def test_default_is_outside_data(self):
        assert inbox_dir({}) == Path.home() / "wslcb-relay-inbox"

    def test_env_override(self, tmp_path):
        assert inbox_dir({"WSLCB_RELAY_INBOX": str(tmp_path)}) == tmp_path


class TestValidateReport:
    """A capture must look like the real report before anything is ingested."""

    def test_real_shape_passes(self, monkeypatch):
        monkeypatch.setattr(relay, "MIN_BYTES", 0)
        validate_report(_page("ok"))

    def test_block_page_rejected(self):
        page = "<html><head><title>403 Forbidden</title></head><body>403</body></html>"
        with pytest.raises(RelayRejectedError, match="bytes"):
            validate_report(page)

    def test_truncated_rejected(self, monkeypatch):
        monkeypatch.setattr(relay, "MIN_BYTES", 0)
        page = _page("cut")
        with pytest.raises(RelayRejectedError, match="truncated"):
            validate_report(page[: len(page) // 2])

    def test_missing_section_rejected(self, monkeypatch):
        monkeypatch.setattr(relay, "MIN_BYTES", 0)
        page = _page("two").replace("STATEWIDE DISCONTINUED LICENSES", "SOMETHING ELSE")
        with pytest.raises(RelayRejectedError, match="STATEWIDE DISCONTINUED LICENSES"):
            validate_report(page)

    def test_no_records_rejected(self, monkeypatch):
        monkeypatch.setattr(relay, "MIN_BYTES", 0)
        page = (
            "<html><body>"
            + "".join(f"<table><tr><th>{h}</th></tr></table>" for h in relay.SECTION_MAP)
            + "</body></html>"
        )
        with pytest.raises(RelayRejectedError, match="no records"):
            validate_report(page)


class TestPending:
    def test_an_upload_renamed_mid_scan_is_skipped(self, tmp_path):
        """A .part that is listed, then renamed by a concurrent upload, isn't fatal."""
        from pathlib import PosixPath

        class _Racy(PosixPath):
            def iterdir(self):
                yield from super().iterdir()
                yield self / "gone.html.gz.part"

            def is_file(self):
                return self.name == "gone.html.gz.part" or super().is_file()

        assert relay._pending(_Racy(tmp_path), NOW) == []


async def _log(engine, log_id):
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    "SELECT status, origin, captured_at, error_message FROM scrape_log"
                    " WHERE id = :id"
                ),
                {"id": log_id},
            )
        ).one()


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
class TestImportInbox:
    @pytest.fixture(autouse=True)
    def _isolate(self, monkeypatch, tmp_path):
        from wslcb_licensing_tracker import scraper

        monkeypatch.setattr(scraper, "DATA_DIR", tmp_path / "data")
        monkeypatch.setattr(relay, "MIN_BYTES", 0)

    async def test_valid_capture_ingests_as_relayed(self, pg_engine, tmp_path):
        inbox = tmp_path / "inbox"
        captured = NOW - timedelta(minutes=5)
        path = _drop(inbox, captured, _page("valid"))

        results = await import_inbox(pg_engine, inbox, now=NOW)

        assert [r.status for r in results] == ["success"]
        assert not path.exists()  # archived by the ingest, so the inbox copy goes
        log = await _log(pg_engine, results[0].scrape_log_id)
        assert (log.status, log.origin, log.captured_at) == ("success", "relay", captured)
        async with pg_engine.connect() as conn:
            src = (
                await conn.execute(
                    text(
                        "SELECT st.slug, s.captured_at, s.metadata FROM sources s"
                        " JOIN source_types st ON st.id = s.source_type_id"
                        " WHERE s.scrape_log_id = :id"
                    ),
                    {"id": results[0].scrape_log_id},
                )
            ).one()
        assert src.slug == "relayed_scrape"
        assert src.captured_at == captured
        assert json.loads(src.metadata) == {
            "relay": "owner-laptop",
            "captured_at": "2026-10-20T13:55:00Z",
        }

    async def test_oldest_first_and_repeat_is_unchanged(self, pg_engine, tmp_path):
        inbox = tmp_path / "inbox"
        page = _page("twice")
        later = _drop(inbox, NOW - timedelta(minutes=10), page)
        earlier = _drop(inbox, NOW - timedelta(hours=1), page)

        results = await import_inbox(pg_engine, inbox, now=NOW)

        assert [(r.name, r.status) for r in results] == [
            (earlier.name, "success"),
            (later.name, "unchanged"),
        ]
        assert list(inbox.glob("*.html.gz")) == []

    async def test_block_page_is_rejected_and_logged(self, pg_engine, tmp_path, monkeypatch):
        monkeypatch.setattr(relay, "MIN_BYTES", 1_000_000)
        inbox = tmp_path / "inbox"
        path = _drop(inbox, NOW - timedelta(minutes=5), "<html>403 Forbidden</html>")

        results = await import_inbox(pg_engine, inbox, now=NOW)

        assert [r.status for r in results] == ["rejected"]
        assert not path.exists()
        assert (inbox / "rejected" / path.name).exists()
        log = await _log(pg_engine, results[0].scrape_log_id)
        assert (log.status, log.origin) == ("error", "relay")
        assert "bytes" in log.error_message

    async def test_stale_capture_is_rejected(self, pg_engine, tmp_path):
        inbox = tmp_path / "inbox"
        path = _drop(inbox, NOW - timedelta(hours=7), _page("stale"))

        results = await import_inbox(pg_engine, inbox, now=NOW)

        assert [r.status for r in results] == ["rejected"]
        assert "old" in results[0].reason
        assert (inbox / "rejected" / path.name).exists()

    async def test_future_capture_is_rejected(self, pg_engine, tmp_path):
        inbox = tmp_path / "inbox"
        _drop(inbox, NOW + timedelta(hours=1), _page("future"))

        results = await import_inbox(pg_engine, inbox, now=NOW)

        assert [r.status for r in results] == ["rejected"]
        assert "future" in results[0].reason

    async def test_unreadable_gzip_is_rejected(self, pg_engine, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        bad = inbox / capture_name(NOW - timedelta(minutes=5))
        bad.write_bytes(b"not gzip")

        results = await import_inbox(pg_engine, inbox, now=NOW)

        assert [r.status for r in results] == ["rejected"]
        assert (inbox / "rejected" / bad.name).exists()

    async def test_stray_names_rejected_and_partials_left(self, pg_engine, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        (inbox / "notes.txt").write_text("hi")
        partial = inbox / (capture_name(NOW) + ".part")
        partial.write_bytes(b"in flight")
        os.utime(partial, (NOW.timestamp(), NOW.timestamp()))

        results = await import_inbox(pg_engine, inbox, now=NOW)

        assert [(r.name, r.status) for r in results] == [("notes.txt", "rejected")]
        assert partial.exists()  # an upload still in flight is not ours yet

    async def test_empty_or_missing_inbox_is_a_noop(self, pg_engine, tmp_path):
        assert await import_inbox(pg_engine, tmp_path / "absent", now=NOW) == []
        (tmp_path / "empty").mkdir()
        assert await import_inbox(pg_engine, tmp_path / "empty", now=NOW) == []

    async def test_unexpected_error_keeps_the_capture_and_fails_the_run(
        self, pg_engine, tmp_path, monkeypatch
    ):
        """A DB fault isn't the capture's fault: it stays queued for a retry."""

        async def broken(conn, *_a, **_kw):
            await conn.execute(text("SELECT 1/0"))  # leaves the transaction aborted

        monkeypatch.setattr(relay, "ingest_page", broken)
        inbox = tmp_path / "inbox"
        path = _drop(inbox, NOW - timedelta(minutes=5), _page("dbfault"))

        with pytest.raises(Exception) as caught:
            await import_inbox(pg_engine, inbox, now=NOW)

        assert not isinstance(caught.value, RelayRejectedError)
        assert path.exists()
        assert not (inbox / "rejected").exists()
        async with pg_engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT status, origin FROM scrape_log"
                        " WHERE origin = 'relay' ORDER BY id DESC LIMIT 1"
                    )
                )
            ).one()
        assert tuple(row) == ("error", "relay")

    async def test_abandoned_partials_are_pruned(self, pg_engine, tmp_path, caplog):
        """An upload whose rename never came (ssh failed after scp) is cleared once stale."""
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        stale = inbox / (capture_name(NOW - timedelta(hours=7)) + ".part")
        stale.write_bytes(b"orphan")
        then = (NOW - relay.MAX_AGE - timedelta(minutes=1)).timestamp()
        os.utime(stale, (then, then))

        assert await import_inbox(pg_engine, inbox, now=NOW) == []
        assert not stale.exists()
        assert "abandoned upload" in caplog.text
