"""Tests for scrape command post-scrape address backfill."""

from unittest.mock import AsyncMock, patch

from cli_helpers import mock_async_engine, mock_get_db
from click.testing import CliRunner

from wslcb_licensing_tracker.cli import main


class TestCmdScrapeBackfill:
    """cmd_scrape calls backfill_addresses after scraping."""

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
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
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
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
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
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
