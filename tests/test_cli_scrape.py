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
        assert variables["consecutive_errors"] == ""
        assert variables["last_success_at"] == ""
