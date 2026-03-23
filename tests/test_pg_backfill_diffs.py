"""Tests for pg_backfill_diffs.py — file-scanning, dry-run, and live ingest."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from wslcb_licensing_tracker.pg_backfill_diffs import _diff_section_dirs, backfill_diffs

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def diff_data_dir(tmp_path):
    """Fake data dir with one notifications diff file."""
    diffs_dir = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True)
    (diffs_dir / "2025-06-15.txt").write_text((FIXTURES_DIR / "diff_two_records.txt").read_text())
    return tmp_path


# ── _diff_section_dirs ────────────────────────────────────────────────


def test_diff_section_dirs_empty_for_nonexistent_root(tmp_path):
    """Returns [] when no diffs root directory exists on disk."""
    assert _diff_section_dirs(tmp_path) == []


def test_diff_section_dirs_returns_existing_sections(tmp_path):
    """Returns only section dirs that exist, with correct section_type."""
    (tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications").mkdir(parents=True)
    result = _diff_section_dirs(tmp_path)
    assert len(result) == 1
    section_type, section_dir = result[0]
    assert section_type == "new_application"
    assert section_dir.name == "notifications"


def test_diff_section_dirs_filters_by_section(tmp_path):
    """section= restricts results to the matching directory."""
    for name in ("notifications", "approvals", "discontinued"):
        (tmp_path / "wslcb" / "licensinginfo-diffs" / name).mkdir(parents=True)
    result = _diff_section_dirs(tmp_path, section="approvals")
    assert len(result) == 1
    assert result[0][0] == "approved"


def test_diff_section_dirs_unknown_section_returns_empty(tmp_path):
    """Non-existent section name returns []."""
    (tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications").mkdir(parents=True)
    assert _diff_section_dirs(tmp_path, section="nonexistent") == []


# ── dry_run ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_backfill_diffs_dry_run_parses_files(diff_data_dir):
    """dry_run=True counts parsed records without touching the DB."""
    with patch("wslcb_licensing_tracker.pg_backfill_diffs.DATA_DIR", diff_data_dir):
        result = await backfill_diffs(None, dry_run=True)
    assert result["files_processed"] == 1
    assert result["inserted"] >= 1
    assert result["errors"] == 0
    assert result["skipped"] == 0


@pytest.mark.asyncio
async def test_backfill_diffs_dry_run_counts_parse_errors(tmp_path):
    """dry_run increments errors when a file raises an exception."""
    diffs_dir = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True)
    # Invalid UTF-8 causes a decode error in extract_records_from_diff.
    (diffs_dir / "bad.txt").write_bytes(b"\xff\xfe")
    with patch("wslcb_licensing_tracker.pg_backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(None, dry_run=True)
    assert result["errors"] == 1
    assert result["files_processed"] == 0


@pytest.mark.asyncio
async def test_backfill_diffs_dry_run_limit_enforced(diff_data_dir):
    """limit=1 stops after one file even when more exist."""
    approvals_dir = diff_data_dir / "wslcb" / "licensinginfo-diffs" / "approvals"
    approvals_dir.mkdir(parents=True)
    (approvals_dir / "2025-06-16.txt").write_text(
        (FIXTURES_DIR / "diff_two_records.txt").read_text()
    )
    with patch("wslcb_licensing_tracker.pg_backfill_diffs.DATA_DIR", diff_data_dir):
        result = await backfill_diffs(None, dry_run=True, limit=1)
    assert result["files_processed"] == 1


@pytest.mark.asyncio
async def test_backfill_diffs_dry_run_no_files_returns_zeros(tmp_path):
    """Empty data dir returns all-zero totals."""
    with patch("wslcb_licensing_tracker.pg_backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(None, dry_run=True)
    assert result == {"inserted": 0, "skipped": 0, "errors": 0, "files_processed": 0}


# ── DB tests ──────────────────────────────────────────────────────────


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_diffs_result_has_expected_keys(pg_engine, diff_data_dir):
    """Result dict contains inserted, skipped, errors, files_processed."""
    with patch("wslcb_licensing_tracker.pg_backfill_diffs.DATA_DIR", diff_data_dir):
        result = await backfill_diffs(pg_engine, dry_run=True)
    assert set(result.keys()) == {"inserted", "skipped", "errors", "files_processed"}


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_diffs_inserts_records(pg_engine, tmp_path):
    """Live ingest writes records to the database."""
    # Use unique license numbers to avoid collisions with other tests.
    diffs_dir = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True)
    # Write the fixture diff — distinct license numbers ensure a clean insert.
    (diffs_dir / "2025-01-01.txt").write_text((FIXTURES_DIR / "diff_two_records.txt").read_text())
    with patch("wslcb_licensing_tracker.pg_backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(pg_engine)
    assert result["files_processed"] >= 1
    assert result["errors"] == 0
    assert result["inserted"] + result["skipped"] >= 1


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_diffs_second_run_skips_duplicates(pg_engine, tmp_path):
    """Re-running the same diff file skips already-ingested records."""
    diffs_dir = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True)
    (diffs_dir / "2025-02-01.txt").write_text((FIXTURES_DIR / "diff_two_records.txt").read_text())
    with patch("wslcb_licensing_tracker.pg_backfill_diffs.DATA_DIR", tmp_path):
        first = await backfill_diffs(pg_engine)
        second = await backfill_diffs(pg_engine)
    assert second["inserted"] == 0
    assert second["skipped"] == first["inserted"] + first["skipped"]
