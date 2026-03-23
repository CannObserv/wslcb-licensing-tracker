"""Tests for cmd_scrape post-scrape address backfill."""

import argparse
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

from wslcb_licensing_tracker.cli import cmd_scrape


def _mock_get_db(*_args, **_kwargs):
    """Return an async context manager yielding a mock connection."""
    @asynccontextmanager
    async def _ctx(*_a, **_kw):
        conn = AsyncMock()
        yield conn
    return _ctx()


class TestCmdScrapeBackfill:
    """cmd_scrape calls backfill_addresses after scraping."""

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_called_after_scrape(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """backfill_addresses is called after a successful scrape."""
        mock_engine = mock_engine_factory.return_value
        args = argparse.Namespace(rate_limit=0.1)
        cmd_scrape(args)
        mock_scrape.assert_called_once()
        mock_backfill.assert_called_once()
        mock_engine.dispose.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_uses_rate_limit(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """backfill_addresses receives the rate_limit from args."""
        args = argparse.Namespace(rate_limit=0.5)
        cmd_scrape(args)
        mock_backfill.assert_called_once()
        assert mock_backfill.call_args.kwargs["rate_limit"] == 0.5

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_failure_does_not_raise(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """If backfill_addresses raises, cmd_scrape still completes."""
        mock_backfill.side_effect = Exception("API down")
        args = argparse.Namespace(rate_limit=0.1)
        # Should not raise
        cmd_scrape(args)
        mock_scrape.assert_called_once()
        mock_engine_factory.return_value.dispose.assert_called_once()
