"""Tests for backfill_diffs.py — file-scanning, dry-run, and live ingest."""

import difflib
import gzip
import os
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text

from wslcb_licensing_tracker.backfill_diffs import _diff_section_dirs, backfill_diffs

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)


# ── Synthetic chain builders (mirror the real archive's diff format) ──


def _notif_block(date: str, name: str, lic: str) -> list[str]:
    return [
        '                   <tbody width="100%" style="Font-size: .80em;">',
        "                      <tr>",
        '                         <td align="Right"><b>Notification Date: </b></td>',
        f"<td>{date}</td>",
        "                      </tr>",
        "                      <tr>",
        '                         <td align="Right"><b>Business Name: </b></td>',
        f"<td>{name}</td>",
        "                      </tr>",
        "                      <tr>",
        '                         <td align="Right"><b>Application Type:</b> </td>',
        "<td>NEW APPLICATION</td>",
        "                      </tr>",
        "                      <tr>",
        '                         <td align="Right"><b>License Number: </b></td>',
        f"<td>{lic}</td>",
        "                      </tr>",
        "                   </tbody>",
        "",
    ]


def _notif_page(*blocks: list[str]) -> list[str]:
    return ["<html>", "<table>", ""] + [ln for b in blocks for ln in b] + ["</table>", "</html>"]


def _write_notif_chain(data_dir: Path, states: list[list[str]]) -> list[Path]:
    diffs_dir = data_dir / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for i in range(1, len(states)):
        old_dt = datetime(2025, 6, 14 + i, 0, 15, 0, tzinfo=UTC)
        new_dt = datetime(2025, 6, 15 + i, 0, 15, 0, tzinfo=UTC)
        diff = "\n".join(
            difflib.unified_diff(
                states[i - 1],
                states[i],
                fromfile="@",
                tofile="@",
                fromfiledate=format_datetime(old_dt),
                tofiledate=format_datetime(new_dt),
                lineterm="",
            )
        )
        p = diffs_dir / f"2025_06_{15 + i:02d}-00_15_00-notifications-diff.txt"
        p.write_text(diff)
        files.append(p)
    return files


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
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", diff_data_dir):
        result = await backfill_diffs(None, dry_run=True)
    assert result["files_processed"] == 1
    assert result["inserted"] >= 1
    assert result["errors"] == 0
    assert result["skipped"] == 0


@pytest.mark.asyncio
async def test_backfill_diffs_dry_run_finds_compressed_files(tmp_path):
    """A .txt.gz diff archive is discovered and parsed like a plain .txt."""
    diffs_dir = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True)
    content = (FIXTURES_DIR / "diff_two_records.txt").read_bytes()
    (diffs_dir / "2025-06-15.txt.gz").write_bytes(gzip.compress(content))
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(None, dry_run=True)
    assert result["files_processed"] == 1
    assert result["inserted"] >= 1
    assert result["errors"] == 0


@pytest.mark.asyncio
async def test_backfill_diffs_dry_run_counts_parse_errors(tmp_path):
    """dry_run increments errors when a file raises an exception."""
    diffs_dir = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True)
    # Invalid UTF-8 causes a decode error in extract_records_from_diff.
    (diffs_dir / "bad.txt").write_bytes(b"\xff\xfe")
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
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
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", diff_data_dir):
        result = await backfill_diffs(None, dry_run=True, limit=1)
    assert result["files_processed"] == 1


@pytest.mark.asyncio
async def test_backfill_diffs_dry_run_no_files_returns_zeros(tmp_path):
    """Empty data dir returns all-zero totals."""
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(None, dry_run=True)
    assert result == {"inserted": 0, "skipped": 0, "errors": 0, "files_processed": 0}


@pytest.mark.asyncio
async def test_backfill_diffs_single_file_infers_section(tmp_path):
    """--file works standalone: the section is inferred from the filename."""
    p = tmp_path / "2025_06_16-00_15_00-notifications-diff.txt"
    p.write_text((FIXTURES_DIR / "diff_two_records.txt").read_text())
    result = await backfill_diffs(None, single_file=str(p), dry_run=True)
    assert result["errors"] == 0
    assert result["inserted"] >= 1


@pytest.mark.asyncio
async def test_backfill_diffs_single_file_unknown_section_is_error(tmp_path):
    """A single file whose name carries no section token counts as an error."""
    p = tmp_path / "2025-06-15.txt"
    p.write_text((FIXTURES_DIR / "diff_two_records.txt").read_text())
    result = await backfill_diffs(None, single_file=str(p), dry_run=True)
    assert result["errors"] == 1
    assert result["inserted"] == 0


# ── DB tests ──────────────────────────────────────────────────────────


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_diffs_result_has_expected_keys(pg_engine, diff_data_dir):
    """Result dict contains inserted, skipped, errors, files_processed."""
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", diff_data_dir):
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
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(pg_engine)
    assert result["files_processed"] >= 1
    assert result["errors"] == 0
    assert result["inserted"] + result["skipped"] >= 1


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_diffs_interleaved_replacement_ingests_no_hybrid(pg_engine, tmp_path):
    """Replacing one record's block with another must never glue the two.

    difflib matches the shared boilerplate lines and interleaves -/+ runs
    mid-block — the pathology behind #151's cross-record bleeds. Both
    licenses must land with their own business names.
    """
    s1 = _notif_page(_notif_block("6/15/2025", "REPLAY ALPHA", "955001"))
    s2 = _notif_page(_notif_block("6/16/2025", "REPLAY BRAVO", "955002"))
    s3 = _notif_page()
    _write_notif_chain(tmp_path, [s1, s2, s3])
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(pg_engine)
    assert result["errors"] == 0
    async with pg_engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "select license_number, business_name from license_records "
                    "where license_number in ('955001','955002')"
                )
            )
        ).fetchall()
    pairs = {(r[0], r[1]) for r in rows}
    assert pairs == {("955001", "REPLAY ALPHA"), ("955002", "REPLAY BRAVO")}


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_diffs_provenance_is_evidencing_file(pg_engine, tmp_path):
    """Each record's source row points at the diff file that evidenced it."""
    s1 = _notif_page(_notif_block("6/15/2025", "REPLAY GOLF", "955003"))
    s2 = _notif_page(
        _notif_block("6/15/2025", "REPLAY GOLF", "955003"),
        _notif_block("6/17/2025", "REPLAY HOTEL", "955004"),
    )
    s3 = _notif_page(
        _notif_block("6/15/2025", "REPLAY GOLF", "955003"),
        _notif_block("6/17/2025", "REPLAY HOTEL", "955004"),
        _notif_block("6/18/2025", "REPLAY INDIA", "955005"),
    )
    files = _write_notif_chain(tmp_path, [s1, s2, s3])
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        result = await backfill_diffs(pg_engine)
    assert result["errors"] == 0
    async with pg_engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "select lr.license_number, s.snapshot_path "
                    "from license_records lr "
                    "join record_sources rs on rs.record_id = lr.id "
                    "join sources s on s.id = rs.source_id "
                    "where lr.license_number in ('955004','955005')"
                )
            )
        ).fetchall()
    by_lic = {r[0]: r[1] for r in rows}
    assert by_lic["955004"].endswith(files[0].name)
    assert by_lic["955005"].endswith(files[1].name)


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_backfill_diffs_second_run_skips_duplicates(pg_engine, tmp_path):
    """Re-running the same diff file skips already-ingested records."""
    diffs_dir = tmp_path / "wslcb" / "licensinginfo-diffs" / "notifications"
    diffs_dir.mkdir(parents=True)
    (diffs_dir / "2025-02-01.txt").write_text((FIXTURES_DIR / "diff_two_records.txt").read_text())
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        first = await backfill_diffs(pg_engine)
        second = await backfill_diffs(pg_engine)
    assert second["inserted"] == 0
    assert second["skipped"] == first["inserted"] + first["skipped"]
