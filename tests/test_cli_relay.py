"""Tests for the import-relayed command (#201)."""

from unittest.mock import AsyncMock, patch

import pytest
from cli_helpers import mock_async_engine, mock_get_db
from click.testing import CliRunner

from wslcb_licensing_tracker.cli import main
from wslcb_licensing_tracker.relay import RelayResult

_CLI = "wslcb_licensing_tracker.cli"


@patch(f"{_CLI}.get_db", side_effect=mock_get_db)
@patch(f"{_CLI}.create_engine_from_env", side_effect=lambda: mock_async_engine())
@patch(f"{_CLI}.post_checkin")
@patch(f"{_CLI}.run_backfill_addresses", new_callable=AsyncMock)
@patch(f"{_CLI}.run_import_inbox", new_callable=AsyncMock)
class TestCmdImportRelayed:
    def test_success_backfills_and_exits_zero(self, imp, backfill, checkin, *_):
        imp.return_value = [RelayResult("20261020T134000Z.html.gz", "success", 9)]
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output
        assert "20261020T134000Z.html.gz: success" in result.output
        backfill.assert_called_once()
        checkin.assert_not_called()  # the monitor reports the server's own fetch

    def test_unchanged_only_skips_the_backfill(self, imp, backfill, checkin, *_):
        imp.return_value = [RelayResult("20261020T134000Z.html.gz", "unchanged", 9)]
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output
        backfill.assert_not_called()

    def test_empty_inbox_is_a_quiet_success(self, imp, backfill, checkin, *_):
        imp.return_value = []
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output
        assert "nothing to import" in result.output
        backfill.assert_not_called()
        checkin.assert_not_called()

    def test_any_rejection_exits_nonzero(self, imp, backfill, checkin, *_):
        imp.return_value = [
            RelayResult("20261020T074000Z.html.gz", "success", 8),
            RelayResult("20261020T134000Z.html.gz", "rejected", 9, "capture is 118 bytes"),
        ]
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 1
        assert "rejected: capture is 118 bytes" in result.output
        backfill.assert_called_once()  # the good capture still gets its addresses
        checkin.assert_not_called()

    def test_backfill_failure_is_not_fatal(self, imp, backfill, checkin, *_):
        imp.return_value = [RelayResult("20261020T134000Z.html.gz", "success", 9)]
        backfill.side_effect = Exception("API down")
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output

    def test_inbox_comes_from_the_environment(self, imp, backfill, checkin, *_):
        imp.return_value = []
        result = CliRunner().invoke(
            main, ["ingest", "import-relayed"], env={"WSLCB_RELAY_INBOX": "/srv/inbox"}
        )
        assert result.exit_code == 0, result.output
        assert str(imp.call_args.args[1]) == "/srv/inbox"


@pytest.mark.parametrize("name", ["import-relayed"])
def test_top_level_alias_for_the_task_template(name):
    """wslcb-task@import-relayed runs ``cli import-relayed``."""
    assert name in main.commands
