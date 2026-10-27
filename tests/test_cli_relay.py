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

    def test_backfill_neither_renews_early_nor_reserves(self, imp, backfill, checkin, *_):
        """The reserve is kept *for* this hook; pulling renewals forward is the daily
        run's job (#195 CR 8)."""
        imp.return_value = [RelayResult("20261020T134000Z.html.gz", "success", 9)]
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output
        kwargs = backfill.call_args.kwargs
        assert kwargs.get("early_level") is None
        assert kwargs.get("reserve", 0) == 0

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

    def test_a_failed_capture_exits_nonzero_after_reporting_the_rest(
        self, imp, backfill, checkin, *_
    ):
        imp.return_value = [
            RelayResult("20261020T074000Z.html.gz", "success", 8),
            RelayResult("20261020T134000Z.html.gz", "failed", 9, "OperationalError: gone"),
        ]
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 1
        assert "20261020T074000Z.html.gz: success" in result.output
        assert "failed: OperationalError: gone" in result.output
        backfill.assert_called_once()

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


@patch(f"{_CLI}.get_db", side_effect=mock_get_db)
@patch(f"{_CLI}.create_engine_from_env", side_effect=lambda: mock_async_engine())
@patch(f"{_CLI}.post_checkin")
@patch(f"{_CLI}.run_backfill_addresses", new_callable=AsyncMock)
@patch(f"{_CLI}.run_classify_entities", new_callable=AsyncMock)
@patch(f"{_CLI}.run_import_inbox", new_callable=AsyncMock)
class TestImportRelayedClassification:
    """The post-import Jev pass (#207): never fails the import, never checks in."""

    OK = [RelayResult("20261020T134000Z.html.gz", "success", 9)]

    def test_classifies_before_the_backfill(self, imp, classify, backfill, checkin, *_):
        from wslcb_licensing_tracker.jev_classifier import RunSummary

        imp.return_value = self.OK
        order = []
        classify.side_effect = lambda *_a, **_kw: (
            order.append("classify")
            or RunSummary("post_import", enabled=True, key_present=True, model="m")
        )
        backfill.side_effect = lambda *_a, **_kw: order.append("backfill")
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output
        assert order == ["classify", "backfill"]
        assert classify.await_args.kwargs["origin"] == "post_import"

    def test_import_relayed_classifies_without_any_checkin(
        self, imp, classify, backfill, checkin, *_
    ):
        """Failure swallowed; the relay unit has no checkin-key, and must not get one."""
        imp.return_value = self.OK
        classify.side_effect = RuntimeError("jev blew up")
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output
        backfill.assert_called_once()
        checkin.assert_not_called()

    def test_a_failed_run_does_not_check_in_either(self, imp, classify, backfill, checkin, *_):
        from wslcb_licensing_tracker.jev_classifier import RunSummary

        imp.return_value = self.OK
        classify.return_value = RunSummary(
            "post_import", enabled=True, key_present=False, model="m", error="missing_api_key"
        )
        result = CliRunner().invoke(main, ["ingest", "import-relayed"])
        assert result.exit_code == 0, result.output
        checkin.assert_not_called()

    def test_nothing_imported_nothing_classified(self, imp, classify, backfill, checkin, *_):
        imp.return_value = [RelayResult("20261020T134000Z.html.gz", "unchanged", 9)]
        CliRunner().invoke(main, ["ingest", "import-relayed"])
        classify.assert_not_awaited()


@pytest.mark.parametrize("name", ["import-relayed"])
def test_top_level_alias_for_the_task_template(name):
    """wslcb-task@import-relayed runs ``cli import-relayed``."""
    assert name in main.commands
