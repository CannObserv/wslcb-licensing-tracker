"""Tests for the click-based CLI."""

import gzip
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cli_helpers import mock_async_engine, mock_get_db
from click.testing import CliRunner

from wslcb_licensing_tracker.address_validator import DAILY_VALIDATION_LIMIT, RENEWAL_RESERVE
from wslcb_licensing_tracker.cli import main


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

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.run_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_scrape(self, mock_engine, mock_scrape, mock_backfill, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "scrape"])
        assert result.exit_code == 0
        mock_scrape.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.run_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_scrape_rate_limit(self, mock_engine, mock_scrape, mock_backfill, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "scrape", "--rate-limit", "0.5"])
        assert result.exit_code == 0
        mock_backfill.assert_called_once()
        assert mock_backfill.call_args.kwargs["rate_limit"] == 0.5

    @patch("wslcb_licensing_tracker.cli.run_backfill_snapshots", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_snapshots(self, mock_engine, mock_bf):
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "backfill-snapshots"])
        assert result.exit_code == 0
        mock_bf.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.run_backfill_diffs", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_diffs_dry_run(self, mock_engine, mock_bf):
        mock_engine.return_value = mock_async_engine()
        mock_bf.return_value = {"inserted": 10, "files_processed": 2, "skipped": 0, "errors": 0}
        result = CliRunner().invoke(
            main, ["ingest", "backfill-diffs", "--section", "notifications", "--dry-run"]
        )
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert "10" in result.output

    @patch("wslcb_licensing_tracker.cli.run_generate_replay_extracts", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_generate_replay_extracts(self, mock_engine, mock_gen):
        mock_engine.return_value = mock_async_engine()
        mock_gen.return_value = {
            "records": 5,
            "written": 4,
            "linked": 4,
            "unmatched": 1,
            "collisions": 0,
            "errors": 0,
        }
        result = CliRunner().invoke(
            main, ["ingest", "generate-replay-extracts", "--section", "approvals"]
        )
        assert result.exit_code == 0
        mock_gen.assert_called_once()
        assert mock_gen.call_args.kwargs["section"] == "approvals"
        assert "4" in result.output

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_addresses(self, mock_engine, mock_ba, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "backfill-addresses"])
        assert result.exit_code == 0
        mock_ba.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_addresses_never_renews_early(self, mock_engine, mock_ba, mock_gdb):
        """Only renew-addresses pulls renewals forward; a manual backfill is the hooks' run."""
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "backfill-addresses"])
        assert result.exit_code == 0, result.output
        assert mock_ba.call_args.kwargs.get("early_level") is None

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_renew_addresses_defaults(self, mock_engine, mock_ba, mock_gdb):
        """The daily run (#195): fills to the limit less the hooks' reserve, which is also
        the per-due-day level it flattens to."""
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "renew-addresses"])
        assert result.exit_code == 0, result.output
        kwargs = mock_ba.call_args.kwargs
        assert kwargs["daily_limit"] == DAILY_VALIDATION_LIMIT
        assert kwargs["reserve"] == RENEWAL_RESERVE
        assert kwargs["early_level"] == DAILY_VALIDATION_LIMIT - RENEWAL_RESERVE
        assert kwargs["limit"] is None
        assert kwargs["dry_run"] is False
        assert kwargs["rate_limit"] == 1.0

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_renew_addresses_canary_flags(self, mock_engine, mock_ba, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(
            main,
            [
                "ingest",
                "renew-addresses",
                "--budget-reserve",
                "100",
                "--limit",
                "25",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        kwargs = mock_ba.call_args.kwargs
        assert kwargs["reserve"] == 100
        assert kwargs["early_level"] == DAILY_VALIDATION_LIMIT - 100
        assert kwargs["limit"] == 25
        assert kwargs["dry_run"] is True

    def test_renew_addresses_reserve_is_bounded(self):
        result = CliRunner().invoke(
            main,
            ["ingest", "renew-addresses", "--budget-reserve", str(DAILY_VALIDATION_LIMIT + 1)],
        )
        assert result.exit_code == 2

    def test_renew_addresses_is_a_timer_instance(self):
        """wslcb-task@renew-addresses runs ``cli renew-addresses``."""
        assert "renew-addresses" in main.commands

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_refresh_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_refresh_addresses(self, mock_engine, mock_ra, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["ingest", "refresh-addresses"])
        assert result.exit_code == 0
        mock_ra.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_refresh_specific_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_refresh_addresses_budget_reserve(self, mock_engine, mock_rs, mock_gdb, tmp_path):
        """--budget-reserve runs ids under the rolling daily limit (#189/#190)."""
        mock_engine.return_value = mock_async_engine()
        ids = tmp_path / "ids.txt"
        ids.write_text("7\n3\n\n5\n")
        result = CliRunner().invoke(
            main,
            ["ingest", "refresh-addresses", "--location-ids", str(ids), "--budget-reserve", "100"],
        )
        assert result.exit_code == 0, result.output
        args, kwargs = mock_rs.call_args
        assert args[1] == [7, 3, 5]
        assert kwargs["daily_limit"] == DAILY_VALIDATION_LIMIT
        assert kwargs["reserve"] == 100

    @patch("wslcb_licensing_tracker.cli.run_refresh_addresses", new_callable=AsyncMock)
    def test_refresh_addresses_budget_reserve_needs_location_ids(self, mock_ra):
        """A budget only applies to an id list; a full refresh must not pretend (CR 11)."""
        result = CliRunner().invoke(
            main, ["ingest", "refresh-addresses", "--budget-reserve", "100"]
        )
        assert result.exit_code == 2
        assert "--budget-reserve needs --location-ids" in result.output
        mock_ra.assert_not_called()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_refresh_specific_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_refresh_addresses_ids_unbudgeted_by_default(
        self, mock_engine, mock_rs, mock_gdb, tmp_path
    ):
        mock_engine.return_value = mock_async_engine()
        ids = tmp_path / "ids.txt"
        ids.write_text("7\n")
        result = CliRunner().invoke(
            main, ["ingest", "refresh-addresses", "--location-ids", str(ids)]
        )
        assert result.exit_code == 0, result.output
        assert mock_rs.call_args.kwargs.get("daily_limit") is None


class TestDbGroup:
    """Commands under the db group."""

    @patch("wslcb_licensing_tracker.cli.print_report", return_value=0)
    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_integrity_checks", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_check(self, mock_engine, mock_checks, mock_gdb, mock_print):
        mock_engine.return_value = mock_async_engine()
        mock_checks.return_value = {"orphaned_locations": []}
        result = CliRunner().invoke(main, ["db", "check"])
        assert result.exit_code == 0
        mock_checks.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.print_report", return_value=0)
    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_integrity_checks", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_check_fix(self, mock_engine, mock_checks, mock_gdb, mock_print):
        mock_engine.return_value = mock_async_engine()
        mock_checks.return_value = {"orphaned_locations": []}
        result = CliRunner().invoke(main, ["db", "check", "--fix"])
        assert result.exit_code == 0
        call_kwargs = mock_checks.call_args.kwargs
        assert call_kwargs["fix"] is True

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_build_all_links", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_rebuild_links(self, mock_engine, mock_links, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        result = CliRunner().invoke(main, ["db", "rebuild-links"])
        assert result.exit_code == 0
        mock_links.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.run_cleanup_redundant", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_cleanup_redundant(self, mock_engine, mock_cleanup):
        mock_engine.return_value = mock_async_engine()
        mock_cleanup.return_value = {"scrape_logs": 0, "files": 0}
        result = CliRunner().invoke(main, ["db", "cleanup-redundant"])
        assert result.exit_code == 0
        assert "Nothing to clean up" in result.output

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_reprocess_endorsements", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_reprocess_endorsements_dry_run(self, mock_engine, mock_rp, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        mock_rp.return_value = {"records_processed": 42, "endorsements_linked": 0}
        result = CliRunner().invoke(main, ["db", "reprocess-endorsements", "--dry-run"])
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert "42" in result.output

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_reprocess_entities", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_reprocess_entities(self, mock_engine, mock_rp, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        mock_rp.return_value = {"records_processed": 10, "entities_linked": 5}
        result = CliRunner().invoke(main, ["db", "reprocess-entities"])
        assert result.exit_code == 0
        assert "10" in result.output

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_reclassify_entities", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_reclassify_entities_dry_run(self, mock_engine, mock_rc, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        mock_rc.return_value = {
            "entities_examined": 1000,
            "reclassified": 3,
            "transitions": {"person->organization": 3},
            "reclassified_sample": [
                {
                    "id": 7,
                    "name": "GLEASON & KOATZ LLP",
                    "old": "person",
                    "new": "organization",
                    "category": "legal_suffix",
                }
            ],
            "noise_entities": 2,
            "noise_links": 5,
            "noise_sample": [{"id": 9, "name": "PER WAC 314-07-035"}],
            "regex_rows_written": 997,
            "subtype_changes": 40,
            "protected_noise_skipped": 1,
            "confirmed_noise": 1,
        }
        result = CliRunner().invoke(main, ["db", "reclassify-entities", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert mock_rc.await_args.kwargs["dry_run"] is True
        assert "dry-run" in result.output
        assert "person->organization: 3" in result.output
        assert "GLEASON & KOATZ LLP" in result.output
        assert "PER WAC 314-07-035" in result.output
        assert "997 regex row(s)" in result.output
        assert "40 subtype change(s)" in result.output
        assert "Would keep 1 noise-named entities" in result.output
        assert "protected" in result.output
        assert "1 operator-confirmed" in result.output


class TestIgnoreJev:
    """wslcb db ignore-jev (#206)."""

    RESULT = {"rows": 12, "entities": 10, "changed": 4}

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_set_jev_ignored", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_ignore_jev_passes_options(self, mock_engine, mock_ig, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        mock_ig.return_value = self.RESULT
        result = CliRunner().invoke(main, ["db", "ignore-jev", "--model", "jev-1.14.0"])
        assert result.exit_code == 0, result.output
        assert mock_ig.await_args.kwargs == {"ignored": True, "model": "jev-1.14.0"}
        assert (
            "Ignored 12 jev row(s) across 10 entities; 4 resolved classification(s) changed."
            in (result.output)
        )

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=mock_get_db)
    @patch("wslcb_licensing_tracker.cli.run_set_jev_ignored", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_undo_and_dry_run(self, mock_engine, mock_ig, mock_gdb):
        mock_engine.return_value = mock_async_engine()
        mock_ig.return_value = self.RESULT
        result = CliRunner().invoke(main, ["db", "ignore-jev", "--undo", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert mock_ig.await_args.kwargs == {"ignored": False, "model": None}
        assert "[dry-run] Restored 12 jev row(s)" in result.output


def _summary(**kw):
    from wslcb_licensing_tracker.jev_classifier import RunSummary

    base = {"origin": "cli", "enabled": True, "key_present": True, "model": "jev-1.14.0"}
    return RunSummary(**{**base, **kw})


_CLI = "wslcb_licensing_tracker.cli"


@patch(f"{_CLI}.post_checkin")
@patch(f"{_CLI}.awaiting_count", new_callable=AsyncMock, return_value=0)
@patch(f"{_CLI}.run_classify_entities", new_callable=AsyncMock)
@patch(f"{_CLI}.get_db", side_effect=mock_get_db)
@patch(f"{_CLI}.create_engine_from_env", side_effect=lambda: mock_async_engine())
class TestClassifyEntities:
    """wslcb db classify-entities (#207)."""

    def test_classify_entities_default(self, eng, gdb, run, *_):
        run.return_value = _summary(names_sent=30, batches_ok=2, input_tokens=7140)
        result = CliRunner().invoke(main, ["db", "classify-entities"])
        assert result.exit_code == 0, result.output
        kw = run.await_args.kwargs
        assert kw["mode"] == "unclassified"
        assert kw["origin"] == "cli"
        assert kw["limit"] is None
        assert kw["dry_run"] is False
        assert (
            "Sent 30 name(s): 2 ok, 0 failed batch(es); 7,140 tokens (~$0.0003); "
            "model jev-1.14.0." in result.output
        )

    def test_refresh_modes(self, eng, gdb, run, *_):
        run.return_value = _summary()
        CliRunner().invoke(main, ["db", "classify-entities", "--refresh-all"])
        assert run.await_args.kwargs["mode"] == "refresh_all"
        CliRunner().invoke(main, ["db", "classify-entities", "--refresh-model", "jev-1.13.0"])
        assert run.await_args.kwargs["mode"] == "refresh_model"
        assert run.await_args.kwargs["model_filter"] == "jev-1.13.0"

    def test_ids_file(self, eng, gdb, run, *_):
        run.return_value = _summary()
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("ids.txt", "w") as f:
                f.write("3\n\n 5 \n")
            result = runner.invoke(main, ["db", "classify-entities", "--ids-file", "ids.txt"])
        assert result.exit_code == 0, result.output
        assert run.await_args.kwargs["mode"] == "ids"
        assert run.await_args.kwargs["ids"] == [3, 5]

    def test_ids_file_rejects_a_non_integer(self, eng, gdb, run, *_):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("ids.txt", "w") as f:
                f.write("3\nfive\n")
            result = runner.invoke(main, ["db", "classify-entities", "--ids-file", "ids.txt"])
        assert result.exit_code == 2
        assert "five" in result.output
        run.assert_not_awaited()

    def test_limit_passes_through(self, eng, gdb, run, *_):
        run.return_value = _summary()
        CliRunner().invoke(main, ["db", "classify-entities", "--limit", "100000"])
        assert run.await_args.kwargs["limit"] == 100000

    def test_dry_run_output(self, eng, gdb, run, *_):
        run.return_value = _summary(enabled=False, candidates=2601, dry_run=True)
        result = CliRunner().invoke(main, ["db", "classify-entities", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert run.await_args.kwargs["dry_run"] is True
        # 2601 names -> 105 requests; 2601 * 545 tokens at $0.042/M
        assert "[dry-run] Would send 2,601 name(s) in 105 request(s); est. $0.0595" in result.output

    def test_disabled_exit_0(self, eng, gdb, run, *_):
        run.return_value = _summary(enabled=False)
        result = CliRunner().invoke(main, ["db", "classify-entities"])
        assert result.exit_code == 0, result.output
        assert (
            "Jev classification disabled (CO_WSLCB_LICENSING_TRACKER_ENABLE_JEV_CLASSIFICATION); "
            "nothing sent." in result.output
        )

    def test_locked_exit_0(self, eng, gdb, run, *_):
        run.return_value = _summary(skipped_locked=True)
        result = CliRunner().invoke(main, ["db", "classify-entities", "--refresh-all"])
        assert result.exit_code == 0, result.output
        assert "Skipped: another classification run holds the lock." in result.output

    def test_failed_batches_exit_1(self, eng, gdb, run, *_):
        run.return_value = _summary(names_sent=50, batches_ok=1, batches_failed=1)
        result = CliRunner().invoke(main, ["db", "classify-entities", "--refresh-all"])
        assert result.exit_code == 1
        assert "1 ok, 1 failed batch(es)" in result.output

    def test_error_exit_1(self, eng, gdb, run, *_):
        run.return_value = _summary(key_present=False, error="missing_api_key")
        result = CliRunner().invoke(main, ["db", "classify-entities", "--refresh-all"])
        assert result.exit_code == 1
        assert "missing_api_key" in result.output

    def test_alias_registered(self, *_):
        assert "classify-entities" in main.commands
        assert main.commands["classify-entities"].hidden


@patch(f"{_CLI}.post_checkin")
@patch(f"{_CLI}.awaiting_count", new_callable=AsyncMock, return_value=0)
@patch(f"{_CLI}.run_classify_entities", new_callable=AsyncMock)
@patch(f"{_CLI}.get_db", side_effect=mock_get_db)
@patch(f"{_CLI}.create_engine_from_env", side_effect=lambda: mock_async_engine())
class TestClassifyCheckin:
    """Only the default-mode run (the weekly timer's) checks in, ok or alert (#207)."""

    def test_default_run_checks_in_ok_once(self, eng, gdb, run, backlog, checkin):
        from wslcb_licensing_tracker.checkin import CLASSIFY

        run.return_value = _summary(names_sent=3, batches_ok=1)
        backlog.return_value = 12
        result = CliRunner().invoke(main, ["db", "classify-entities"])
        assert result.exit_code == 0, result.output
        checkin.assert_called_once()
        (status, variables), kw = checkin.call_args
        assert status == "ok"
        assert kw["monitor"] is CLASSIFY
        assert kw["metadata"] is None
        assert variables["backlog"] == 12
        assert variables["names_sent"] == 3

    def test_disabled_default_run_still_checks_in_ok(self, eng, gdb, run, backlog, checkin):
        run.return_value = _summary(enabled=False)
        backlog.return_value = 67_000
        result = CliRunner().invoke(main, ["db", "classify-entities"])
        assert result.exit_code == 0, result.output
        (status, variables), _ = checkin.call_args
        assert status == "ok"
        assert variables["enabled"] == "false"

    def test_default_run_failure_alerts_and_exits_1(self, eng, gdb, run, backlog, checkin):
        run.return_value = _summary(key_present=False, error="missing_api_key")
        result = CliRunner().invoke(main, ["db", "classify-entities"])
        assert result.exit_code == 1
        (status, _), kw = checkin.call_args
        assert status == "alert"
        assert kw["metadata"] == {"fault": "missing_api_key"}

    def test_a_limited_default_run_is_still_the_default_mode(self, eng, gdb, run, backlog, checkin):
        """The one-off backfill (--limit N) is a default-mode run; it checks in when it can."""
        run.return_value = _summary()
        CliRunner().invoke(main, ["db", "classify-entities", "--limit", "100000"])
        checkin.assert_called_once()

    def test_default_run_exception_alerts_and_fails(self, eng, gdb, run, backlog, checkin):
        run.side_effect = RuntimeError("db went away")
        result = CliRunner().invoke(main, ["db", "classify-entities"])
        assert result.exit_code == 1
        assert isinstance(result.exception, RuntimeError)
        checkin.assert_called_once()
        (status, variables), kw = checkin.call_args
        assert status == "alert"
        assert kw["metadata"] == {"fault": "RuntimeError"}
        assert variables["error"] == "RuntimeError: db went away"

    def test_backlog_failure_alerts_too(self, eng, gdb, run, backlog, checkin):
        run.return_value = _summary()
        backlog.side_effect = OSError("connection reset")
        result = CliRunner().invoke(main, ["db", "classify-entities"])
        assert result.exit_code == 1
        (status, _), kw = checkin.call_args
        assert status == "alert"
        assert kw["metadata"] == {"fault": "OSError"}


@pytest.mark.parametrize("args", [["--dry-run"], ["--refresh-all"], ["--refresh-model", "m"]])
def test_classify_dry_run_and_refresh_never_check_in(args):
    with (
        patch(f"{_CLI}.run_classify_entities", new_callable=AsyncMock) as run,
        patch(f"{_CLI}.awaiting_count", new_callable=AsyncMock, return_value=0),
        patch(f"{_CLI}.post_checkin") as checkin,
        patch(f"{_CLI}.get_db", side_effect=mock_get_db),
        patch(f"{_CLI}.create_engine_from_env", side_effect=lambda: mock_async_engine()),
    ):
        run.return_value = _summary(dry_run="--dry-run" in args)
        result = CliRunner().invoke(main, ["db", "classify-entities", *args])
    assert result.exit_code == 0, result.output
    checkin.assert_not_called()


@pytest.mark.parametrize(
    "args",
    [
        ["--refresh-all", "--refresh-model", "jev-1"],
        ["--refresh-all", "--ids-file", "ids.txt"],
        ["--refresh-model", "jev-1", "--ids-file", "ids.txt"],
    ],
)
def test_classify_refresh_options_exclusive(args):
    runner = CliRunner()
    with (
        patch(f"{_CLI}.run_classify_entities", new_callable=AsyncMock) as run,
        patch(f"{_CLI}.create_engine_from_env", side_effect=lambda: mock_async_engine()),
        runner.isolated_filesystem(),
    ):
        with open("ids.txt", "w") as f:
            f.write("1\n")
        result = runner.invoke(main, ["db", "classify-entities", *args])
    assert result.exit_code == 2
    assert "mutually exclusive" in result.output
    run.assert_not_awaited()


class TestAdminGroup:
    """Commands under the admin group."""

    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_admin_add_user(self, mock_engine):
        mock_engine.return_value = mock_async_engine()

        @asynccontextmanager
        async def _ctx(*_a, **_kw):
            conn = AsyncMock()
            # execute returns a result whose .fetchone() returns None (no existing user)
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            conn.execute = AsyncMock(return_value=mock_result)
            yield conn

        with patch("wslcb_licensing_tracker.cli.get_db", side_effect=lambda *_a, **_kw: _ctx()):
            result = CliRunner().invoke(main, ["admin", "add-user", "test@example.com"])
        assert result.exit_code == 0
        assert "Added admin user" in result.output

    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_admin_list_users(self, mock_engine):
        mock_engine.return_value = mock_async_engine()

        @asynccontextmanager
        async def _ctx(*_a, **_kw):
            conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            conn.execute = AsyncMock(return_value=mock_result)
            yield conn

        with patch("wslcb_licensing_tracker.cli.get_db", side_effect=lambda *_a, **_kw: _ctx()):
            result = CliRunner().invoke(main, ["admin", "list-users"])
        assert result.exit_code == 0
        assert "No admin users" in result.output

    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_admin_remove_user(self, mock_engine):
        mock_engine.return_value = mock_async_engine()

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

        with patch("wslcb_licensing_tracker.cli.get_db", side_effect=lambda *_a, **_kw: _ctx()):
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


class TestCompressSnapshots:
    """Tests for `wslcb ingest compress-snapshots`."""

    def _make_snapshot_dir(self, tmp_path):
        d = tmp_path / "wslcb" / "licensinginfo" / "2026" / "2026_01_01"
        d.mkdir(parents=True)
        return d

    def test_no_files_exits_cleanly(self, tmp_path):
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-snapshots"])
        assert result.exit_code == 0
        assert "No uncompressed snapshots found" in result.output

    def test_dry_run_lists_files_without_writing(self, tmp_path):
        d = self._make_snapshot_dir(tmp_path)
        html = d / "2026_01_01-test-v1.html"
        html.write_text("<html/>")
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-snapshots", "--dry-run"])
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert html.name in result.output
        assert html.exists()
        assert not (d / "2026_01_01-test-v1.html.gz").exists()

    def test_compresses_and_removes_original(self, tmp_path):
        d = self._make_snapshot_dir(tmp_path)
        html = d / "2026_01_01-test-v1.html"
        html.write_text("<html>content</html>")
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-snapshots"])
        assert result.exit_code == 0
        assert "Compressed 1" in result.output
        assert not html.exists()
        gz = d / "2026_01_01-test-v1.html.gz"
        assert gz.exists()
        with gzip.open(gz, "rt", encoding="utf-8") as fh:
            assert fh.read() == "<html>content</html>"

    def test_rerun_skips_already_compressed(self, tmp_path):
        d = self._make_snapshot_dir(tmp_path)
        (d / "2026_01_01-test-v1.html.gz").write_bytes(gzip.compress(b"x"))
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-snapshots"])
        assert result.exit_code == 0
        assert "No uncompressed snapshots found" in result.output

    def test_orphan_cleanup_removes_html_when_gz_exists(self, tmp_path):
        d = self._make_snapshot_dir(tmp_path)
        base = "2026_01_01-test-v1"
        (d / f"{base}.html").write_text("<html/>")
        (d / f"{base}.html.gz").write_bytes(gzip.compress(b"<html/>"))
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-snapshots"])
        assert result.exit_code == 0
        assert not (d / f"{base}.html").exists()
        assert (d / f"{base}.html.gz").exists()

    def test_dry_run_reports_orphaned_html(self, tmp_path):
        d = self._make_snapshot_dir(tmp_path)
        base = "2026_01_01-test-v1"
        (d / f"{base}.html").write_text("<html/>")
        (d / f"{base}.html.gz").write_bytes(gzip.compress(b"<html/>"))
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-snapshots", "--dry-run"])
        assert result.exit_code == 0
        assert "orphaned" in result.output
        assert (d / f"{base}.html").exists()


class TestCompressDiffs:
    """Tests for `wslcb ingest compress-diffs`."""

    def _make_diffs_dir(self, tmp_path):
        d = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
        d.mkdir(parents=True)
        return d

    def test_no_files_exits_cleanly(self, tmp_path):
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-diffs"])
        assert result.exit_code == 0
        assert "No uncompressed diffs found" in result.output

    def test_dry_run_lists_files_without_writing(self, tmp_path):
        d = self._make_diffs_dir(tmp_path)
        txt = d / "2025_06_15-notifications-diff.txt"
        txt.write_text("diff content")
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-diffs", "--dry-run"])
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert txt.name in result.output
        assert txt.exists()
        assert not (d / "2025_06_15-notifications-diff.txt.gz").exists()

    def test_compresses_and_removes_original(self, tmp_path):
        d = self._make_diffs_dir(tmp_path)
        txt = d / "2025_06_15-notifications-diff.txt"
        txt.write_text("diff content")
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-diffs"])
        assert result.exit_code == 0
        assert "Compressed 1" in result.output
        assert not txt.exists()
        gz = d / "2025_06_15-notifications-diff.txt.gz"
        assert gz.exists()
        with gzip.open(gz, "rt", encoding="utf-8") as fh:
            assert fh.read() == "diff content"

    def test_rerun_skips_already_compressed(self, tmp_path):
        d = self._make_diffs_dir(tmp_path)
        (d / "2025_06_15-notifications-diff.txt.gz").write_bytes(gzip.compress(b"x"))
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-diffs"])
        assert result.exit_code == 0
        assert "No uncompressed diffs found" in result.output

    def test_orphan_cleanup_removes_txt_when_gz_exists(self, tmp_path):
        d = self._make_diffs_dir(tmp_path)
        base = "2025_06_15-notifications-diff"
        (d / f"{base}.txt").write_text("diff content")
        (d / f"{base}.txt.gz").write_bytes(gzip.compress(b"diff content"))
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-diffs"])
        assert result.exit_code == 0
        assert not (d / f"{base}.txt").exists()
        assert (d / f"{base}.txt.gz").exists()

    def test_dry_run_reports_orphaned_txt(self, tmp_path):
        d = self._make_diffs_dir(tmp_path)
        base = "2025_06_15-notifications-diff"
        (d / f"{base}.txt").write_text("diff content")
        (d / f"{base}.txt.gz").write_bytes(gzip.compress(b"diff content"))
        with patch("wslcb_licensing_tracker.cli.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-diffs", "--dry-run"])
        assert result.exit_code == 0
        assert "orphaned" in result.output
        assert (d / f"{base}.txt").exists()


class TestOpsGroup:
    """`wslcb ops disk-hygiene` retired to the cohort's disk GC (#200)."""

    def test_disk_hygiene_is_gone(self):
        assert "disk-hygiene" not in CliRunner().invoke(main, ["ops", "--help"]).output
        for argv in (["ops", "disk-hygiene"], ["disk-hygiene"]):
            assert CliRunner().invoke(main, argv).exit_code != 0, argv


class TestCompressStragglers:
    """Tests for `wslcb ingest compress-stragglers`, the weekly timer's job (#200)."""

    def _stragglers(self, tmp_path):
        snap = tmp_path / "wslcb" / "licensinginfo" / "2026" / "2026_01_01"
        snap.mkdir(parents=True)
        (snap / "2026_01_01-test-v1.html").write_text("<html/>")
        diff = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
        diff.mkdir(parents=True)
        (diff / "2025_06_15-notifications-diff.txt").write_text("diff content")
        return snap / "2026_01_01-test-v1.html", diff / "2025_06_15-notifications-diff.txt"

    def test_none_found(self, tmp_path):
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-stragglers"])
        assert result.exit_code == 0
        assert "No uncompressed snapshots or diffs found." in result.output

    def test_dry_run_names_each_kind_and_writes_nothing(self, tmp_path):
        snap, diff = self._stragglers(tmp_path)
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-stragglers", "--dry-run"])
        assert result.exit_code == 0
        assert "Would compress 1 snapshot(s)" in result.output
        assert "Would compress 1 diff(s)" in result.output
        assert snap.exists() and diff.exists()

    def test_compresses_both_kinds(self, tmp_path):
        snap, diff = self._stragglers(tmp_path)
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["ingest", "compress-stragglers"])
        assert result.exit_code == 0
        assert "Compressed 1 snapshot(s)" in result.output
        assert "Compressed 1 diff(s)" in result.output
        for path in (snap, diff):
            assert not path.exists()
            assert path.with_name(path.name + ".gz").exists()

    def test_top_level_alias(self, tmp_path):
        """wslcb-task@compress-stragglers runs `cli compress-stragglers`."""
        with patch("wslcb_licensing_tracker.disk_hygiene.DATA_DIR", tmp_path):
            result = CliRunner().invoke(main, ["compress-stragglers", "--dry-run"])
        assert result.exit_code == 0
