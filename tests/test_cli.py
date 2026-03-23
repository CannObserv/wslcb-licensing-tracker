"""Tests for the click-based CLI."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner

from wslcb_licensing_tracker.cli import main


@asynccontextmanager
async def _mock_get_db_ctx(*_a, **_kw):
    conn = AsyncMock()
    yield conn


def _mock_get_db(*_args, **_kwargs):
    return _mock_get_db_ctx()


class TestTopLevel:
    """Top-level CLI behavior."""

    def test_help(self):
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "WSLCB Licensing Tracker" in result.output

    def test_no_command_shows_help(self):
        result = CliRunner().invoke(main, [])
        assert result.exit_code == 0
        assert "Usage" in result.output


class TestIngestGroup:
    """Commands under the ingest group."""

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_scrape(self, mock_engine, mock_scrape, mock_backfill, mock_gdb):
        mock_engine.return_value = MagicMock()
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0
        mock_scrape.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_scrape_rate_limit(self, mock_engine, mock_scrape, mock_backfill, mock_gdb):
        mock_engine.return_value = MagicMock()
        result = CliRunner().invoke(main, ["ingest", "scrape", "--rate-limit", "0.5"])
        assert result.exit_code == 0
        mock_backfill.assert_called_once()
        assert mock_backfill.call_args.kwargs["rate_limit"] == 0.5

    @patch("wslcb_licensing_tracker.cli.pg_backfill_snapshots", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_snapshots(self, mock_engine, mock_bf):
        mock_engine.return_value = MagicMock()
        result = CliRunner().invoke(main, ["ingest", "backfill-snapshots"])
        assert result.exit_code == 0
        mock_bf.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.pg_backfill_diffs", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_diffs_dry_run(self, mock_engine, mock_bf):
        mock_engine.return_value = MagicMock()
        mock_bf.return_value = {"inserted": 10, "files_processed": 2, "skipped": 0, "errors": 0}
        result = CliRunner().invoke(
            main, ["ingest", "backfill-diffs", "--section", "notifications", "--dry-run"]
        )
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert "10" in result.output

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_addresses(self, mock_engine, mock_ba, mock_gdb):
        mock_engine.return_value = MagicMock()
        result = CliRunner().invoke(main, ["ingest", "backfill-addresses"])
        assert result.exit_code == 0
        mock_ba.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_refresh_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_refresh_addresses(self, mock_engine, mock_ra, mock_gdb):
        mock_engine.return_value = MagicMock()
        result = CliRunner().invoke(main, ["ingest", "refresh-addresses"])
        assert result.exit_code == 0
        mock_ra.assert_called_once()


class TestDbGroup:
    """Commands under the db group."""

    @patch("wslcb_licensing_tracker.cli.print_report", return_value=0)
    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_run_all_checks", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_check(self, mock_engine, mock_checks, mock_gdb, mock_print):
        mock_engine.return_value = MagicMock()
        mock_checks.return_value = {"orphaned_locations": []}
        result = CliRunner().invoke(main, ["db", "check"])
        assert result.exit_code == 0
        mock_checks.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.print_report", return_value=0)
    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_run_all_checks", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_check_fix(self, mock_engine, mock_checks, mock_gdb, mock_print):
        mock_engine.return_value = MagicMock()
        mock_checks.return_value = {"orphaned_locations": []}
        result = CliRunner().invoke(main, ["db", "check", "--fix"])
        assert result.exit_code == 0
        call_kwargs = mock_checks.call_args.kwargs
        assert call_kwargs["fix"] is True

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_build_all_links", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_rebuild_links(self, mock_engine, mock_links, mock_gdb):
        mock_engine.return_value = MagicMock()
        result = CliRunner().invoke(main, ["db", "rebuild-links"])
        assert result.exit_code == 0
        mock_links.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.pg_cleanup_redundant", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_cleanup_redundant(self, mock_engine, mock_cleanup):
        mock_engine.return_value = MagicMock()
        mock_cleanup.return_value = {"scrape_logs": 0, "files": 0}
        result = CliRunner().invoke(main, ["db", "cleanup-redundant"])
        assert result.exit_code == 0
        assert "Nothing to clean up" in result.output

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_reprocess_endorsements", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_reprocess_endorsements_dry_run(self, mock_engine, mock_rp, mock_gdb):
        mock_engine.return_value = MagicMock()
        mock_rp.return_value = {"records_processed": 42, "endorsements_linked": 0}
        result = CliRunner().invoke(main, ["db", "reprocess-endorsements", "--dry-run"])
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert "42" in result.output

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_reprocess_entities", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_reprocess_entities(self, mock_engine, mock_rp, mock_gdb):
        mock_engine.return_value = MagicMock()
        mock_rp.return_value = {"records_processed": 10, "entities_linked": 5}
        result = CliRunner().invoke(main, ["db", "reprocess-entities"])
        assert result.exit_code == 0
        assert "10" in result.output


class TestAdminGroup:
    """Commands under the admin group."""

    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_admin_add_user(self, mock_engine):
        mock_engine.return_value = MagicMock()

        @asynccontextmanager
        async def _ctx(*_a, **_kw):
            conn = AsyncMock()
            # execute returns a result whose .fetchone() returns None (no existing user)
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            conn.execute = AsyncMock(return_value=mock_result)
            yield conn

        with patch("wslcb_licensing_tracker.cli.get_db", side_effect=lambda *a, **k: _ctx()):
            result = CliRunner().invoke(main, ["admin", "add-user", "test@example.com"])
        assert result.exit_code == 0
        assert "Added admin user" in result.output

    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_admin_list_users(self, mock_engine):
        mock_engine.return_value = MagicMock()

        @asynccontextmanager
        async def _ctx(*_a, **_kw):
            conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            conn.execute = AsyncMock(return_value=mock_result)
            yield conn

        with patch("wslcb_licensing_tracker.cli.get_db", side_effect=lambda *a, **k: _ctx()):
            result = CliRunner().invoke(main, ["admin", "list-users"])
        assert result.exit_code == 0
        assert "No admin users" in result.output

    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_admin_remove_user(self, mock_engine):
        mock_engine.return_value = MagicMock()

        call_count = 0

        @asynccontextmanager
        async def _ctx(*_a, **_kw):
            conn = AsyncMock()

            async def _execute(stmt, *a, **k):
                nonlocal call_count
                call_count += 1
                mock_result = MagicMock()
                if call_count == 1:
                    # First call: select to find user — return a row
                    mock_result.fetchone.return_value = (1,)
                elif call_count == 2:
                    # Second call: count of admin users
                    mock_result.scalar_one.return_value = 2
                return mock_result

            conn.execute = _execute
            yield conn

        with patch("wslcb_licensing_tracker.cli.get_db", side_effect=lambda *a, **k: _ctx()):
            result = CliRunner().invoke(main, ["admin", "remove-user", "test@example.com"])
        assert result.exit_code == 0
        assert "Removed admin user" in result.output

    def test_admin_help(self):
        result = CliRunner().invoke(main, ["admin", "--help"])
        assert result.exit_code == 0
        assert "add-user" in result.output
        assert "list-users" in result.output
        assert "remove-user" in result.output

    def test_db_help(self):
        result = CliRunner().invoke(main, ["db", "--help"])
        assert result.exit_code == 0
        assert "check" in result.output
        assert "rebuild-links" in result.output

    def test_ingest_help(self):
        result = CliRunner().invoke(main, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "scrape" in result.output
        assert "backfill-snapshots" in result.output
