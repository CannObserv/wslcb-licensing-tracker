"""Tests for scrape command post-scrape address backfill."""

from unittest.mock import AsyncMock, patch

from cli_helpers import mock_async_engine, mock_get_db
from click.testing import CliRunner

from wslcb_licensing_tracker.cli import main


class TestCmdScrapeBackfill:
    """cmd_scrape calls backfill_addresses after scraping."""

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.run_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_called_after_scrape(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """backfill_addresses is called after a successful scrape."""
        mock_engine_factory.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0
        mock_scrape.assert_called_once()
        mock_backfill.assert_called_once()
        mock_engine_factory.return_value.dispose.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.run_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_hook_neither_renews_early_nor_reserves(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """The reserve is kept *for* the hooks: they spend it, and never pull renewals
        forward, which is the daily run's job (#195)."""
        mock_engine_factory.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0
        kwargs = mock_backfill.call_args.kwargs
        assert kwargs.get("early_level") is None
        assert kwargs.get("reserve", 0) == 0

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.run_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_uses_rate_limit(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """backfill_addresses receives the rate_limit from args."""
        mock_engine_factory.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "scrape", "--rate-limit", "0.5"])
        assert result.exit_code == 0
        mock_backfill.assert_called_once()
        assert mock_backfill.call_args.kwargs["rate_limit"] == 0.5

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.run_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_failure_does_not_raise(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """If backfill_addresses raises, cmd_scrape still completes."""
        mock_engine_factory.return_value = mock_async_engine()
        mock_backfill.side_effect = Exception("API down")
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0
        mock_scrape.assert_called_once()
        mock_engine_factory.return_value.dispose.assert_called_once()


_SCRAPE = "wslcb_licensing_tracker.cli"


@patch(f"{_SCRAPE}.get_db", side_effect=mock_get_db)
@patch(f"{_SCRAPE}.create_engine_from_env", side_effect=lambda: mock_async_engine())
class TestCmdScrapeCheckin:
    """The scrape checks in to its co-status monitor on every run (#196)."""

    @patch(f"{_SCRAPE}.post_checkin")
    @patch(f"{_SCRAPE}.run_backfill_addresses", new_callable=AsyncMock)
    @patch(f"{_SCRAPE}.run_scrape", new_callable=AsyncMock)
    def test_ok_after_a_scrape_before_the_backfill(self, scrape, backfill, checkin, *_):
        """The backfill takes minutes and can't fail the scrape, so the signal
        goes first."""
        from wslcb_licensing_tracker.checkin import SCRAPE
        from wslcb_licensing_tracker.scraper import ScrapeOutcome

        order = []
        scrape.return_value = ScrapeOutcome(7, "unchanged")
        checkin.side_effect = lambda *_a, **_kw: order.append("checkin")
        backfill.side_effect = lambda *_a, **_kw: order.append("backfill")

        result = CliRunner().invoke(main, ["ingest", "scrape"])

        assert result.exit_code == 0
        assert order == ["checkin", "backfill"]
        (status, variables), kwargs = checkin.call_args
        assert status == "ok"
        assert kwargs["monitor"] is SCRAPE
        assert variables["scrape_status"] == "unchanged"
        assert variables["scrape_log_id"] == 7
        assert "metadata" not in kwargs

    @patch(f"{_SCRAPE}.scrape_health", new_callable=AsyncMock)
    @patch(f"{_SCRAPE}.post_checkin")
    @patch(f"{_SCRAPE}.run_backfill_addresses", new_callable=AsyncMock)
    @patch(f"{_SCRAPE}.run_scrape", new_callable=AsyncMock)
    def test_checkins_leave_the_event_loop(self, scrape, backfill, checkin, health, *_):
        """post_checkin is sync httpx (up to 2 x 10 s): it runs in a worker thread."""
        import threading

        from wslcb_licensing_tracker.scraper import ScrapeHealth, ScrapeOutcome

        threads = []
        health.return_value = ScrapeHealth(None, 1)
        checkin.side_effect = lambda *_a, **_kw: threads.append(threading.current_thread())
        scrape.return_value = ScrapeOutcome(7, "unchanged")
        CliRunner().invoke(main, ["ingest", "scrape"])
        scrape.side_effect = OSError("down")
        CliRunner().invoke(main, ["ingest", "scrape"])

        assert len(threads) == 2
        assert all(t is not threading.main_thread() for t in threads)

    @patch(f"{_SCRAPE}.scrape_health", new_callable=AsyncMock)
    @patch(f"{_SCRAPE}.post_checkin")
    @patch(f"{_SCRAPE}.run_backfill_addresses", new_callable=AsyncMock)
    @patch(f"{_SCRAPE}.run_scrape", new_callable=AsyncMock)
    def test_failure_alerts_and_still_fails(self, scrape, backfill, checkin, health, *_):
        """An alert with the history and a fault key; exit 1 so the unit fails."""
        from datetime import UTC, datetime

        from wslcb_licensing_tracker.checkin import SCRAPE
        from wslcb_licensing_tracker.scraper import ScrapeHealth

        scrape.side_effect = ConnectionError("no route")
        health.return_value = ScrapeHealth(datetime(2026, 10, 12, 13, 34, tzinfo=UTC), 3)

        result = CliRunner().invoke(main, ["ingest", "scrape"])

        assert result.exit_code == 1
        assert isinstance(result.exception, ConnectionError)
        backfill.assert_not_called()
        (status, variables), kwargs = checkin.call_args
        assert status == "alert"
        assert kwargs["monitor"] is SCRAPE
        assert kwargs["metadata"] == {"fault": "ConnectionError"}
        assert variables["error"] == "ConnectionError: no route"
        assert variables["consecutive_errors"] == 3
        assert variables["window_expires"] == "2026-11-11"

    @patch(f"{_SCRAPE}.scrape_health", new_callable=AsyncMock)
    @patch(f"{_SCRAPE}.post_checkin")
    @patch(f"{_SCRAPE}.run_backfill_addresses", new_callable=AsyncMock)
    @patch(f"{_SCRAPE}.run_scrape", new_callable=AsyncMock)
    def test_alert_survives_an_unreadable_history(self, scrape, backfill, checkin, health, *_):
        """The database being down is exactly when the alert matters."""
        scrape.side_effect = OSError("connection refused")
        health.side_effect = OSError("connection refused")

        result = CliRunner().invoke(main, ["ingest", "scrape"])

        assert result.exit_code == 1
        assert isinstance(result.exception, OSError)
        (status, variables), _ = checkin.call_args
        assert status == "alert"
        assert variables["consecutive_errors"] == "unknown"
        assert variables["last_success_at"] == "unknown"
        assert variables["window_expires"] == "unknown"


def _summary(**kw):
    from wslcb_licensing_tracker.jev_classifier import RunSummary

    base = {"origin": "post_scrape", "enabled": True, "key_present": True, "model": "jev-1.14.0"}
    return RunSummary(**{**base, **kw})


@patch(f"{_SCRAPE}.get_db", side_effect=mock_get_db)
@patch(f"{_SCRAPE}.create_engine_from_env", side_effect=lambda: mock_async_engine())
@patch(f"{_SCRAPE}.post_checkin")
@patch(f"{_SCRAPE}.run_backfill_addresses", new_callable=AsyncMock)
@patch(f"{_SCRAPE}.run_classify_entities", new_callable=AsyncMock)
@patch(f"{_SCRAPE}.run_scrape", new_callable=AsyncMock)
class TestCmdScrapeClassification:
    """The post-scrape Jev pass (#207): never fails the scrape; alerts only."""

    def _scrape_ok(self, scrape):
        from wslcb_licensing_tracker.scraper import ScrapeOutcome

        scrape.return_value = ScrapeOutcome(7, "success")

    def _classify_calls(self, checkin):
        from wslcb_licensing_tracker.checkin import CLASSIFY

        return [c for c in checkin.call_args_list if c.kwargs["monitor"] is CLASSIFY]

    def test_runs_before_the_address_backfill(self, scrape, classify, backfill, checkin, *_):
        self._scrape_ok(scrape)
        order = []
        classify.side_effect = lambda *_a, **_kw: order.append("classify") or _summary()
        backfill.side_effect = lambda *_a, **_kw: order.append("backfill")
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0, result.output
        assert order == ["classify", "backfill"]
        assert classify.await_args.kwargs["origin"] == "post_scrape"

    def test_scrape_runs_classification_and_survives_its_failure(
        self, scrape, classify, backfill, checkin, *_
    ):
        self._scrape_ok(scrape)
        classify.side_effect = RuntimeError("jev blew up")
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0, result.output
        backfill.assert_called_once()
        (call,) = self._classify_calls(checkin)
        assert call.args[0] == "alert"
        assert call.kwargs["metadata"] == {"fault": "RuntimeError"}

    def test_a_failed_run_alerts(self, scrape, classify, backfill, checkin, *_):
        self._scrape_ok(scrape)
        classify.return_value = _summary(error="auth_failed", batches_failed=1)
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0, result.output
        (call,) = self._classify_calls(checkin)
        assert call.args[0] == "alert"
        assert call.kwargs["metadata"] == {"fault": "auth_failed"}
        assert call.args[1]["backlog"] == "n/a"  # not counted after an ingest (#210)

    def _no_classify_checkin(self, scrape, classify, checkin, **summary):
        self._scrape_ok(scrape)
        classify.return_value = _summary(**summary)
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0, result.output
        assert self._classify_calls(checkin) == []

    def test_scrape_classification_success_sends_no_classify_checkin(
        self, scrape, classify, backfill, checkin, *_
    ):
        """Twice-daily oks would mask a dead weekly timer."""
        self._no_classify_checkin(scrape, classify, checkin, names_sent=3, batches_ok=1)

    def test_disabled_sends_no_classify_checkin(self, scrape, classify, backfill, checkin, *_):
        self._no_classify_checkin(scrape, classify, checkin, enabled=False)

    def test_losing_the_lock_sends_no_classify_checkin(
        self, scrape, classify, backfill, checkin, *_
    ):
        """A manual backfill holding the lock is no fault; the weekly run judges locks."""
        self._no_classify_checkin(scrape, classify, checkin, skipped_locked=True)

    def test_failed_scrape_never_classifies(self, scrape, classify, backfill, checkin, *_):
        """During the WSLCB block (#198) the hook never runs; the weekly timer catches up."""
        scrape.side_effect = OSError("403")
        with patch(f"{_SCRAPE}.scrape_health", new_callable=AsyncMock, return_value=None):
            result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 1
        classify.assert_not_awaited()


def test_the_real_hook_is_a_no_op_with_the_flag_off():
    """Hooks go live at merge with the flag off: no calls, no run row, no check-in."""
    from wslcb_licensing_tracker.scraper import ScrapeOutcome

    with (
        patch(f"{_SCRAPE}.get_db", side_effect=mock_get_db),
        patch(f"{_SCRAPE}.create_engine_from_env", side_effect=lambda: mock_async_engine()),
        patch(f"{_SCRAPE}.post_checkin") as checkin,
        patch(f"{_SCRAPE}.run_backfill_addresses", new_callable=AsyncMock),
        patch(f"{_SCRAPE}.run_scrape", new_callable=AsyncMock) as scrape,
        patch("httpx.AsyncClient.send", side_effect=AssertionError("no HTTP")),
    ):
        scrape.return_value = ScrapeOutcome(7, "success")
        result = CliRunner().invoke(main, ["ingest", "scrape"])
    assert result.exit_code == 0, result.output
    assert checkin.call_count == 1  # the scrape's own ok, nothing for CLASSIFY
