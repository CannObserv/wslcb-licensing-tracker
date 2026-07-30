"""Tests for replay_extracts.py — replay-generated provenance extracts (#154)."""

import difflib
import gzip
import os
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text

from wslcb_licensing_tracker.backfill_diffs import backfill_diffs
from wslcb_licensing_tracker.replay_extracts import (
    extract_rel_path,
    generate_replay_extracts,
)

_needs_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="requires TEST_DATABASE_URL",
)


# ── Synthetic approvals chain (mirrors the real archive's diff format) ─


def _block(date: str, name: str, lic: str) -> list[str]:
    return [
        '                   <tbody width="100%" style="Font-size: .80em;">',
        "                      <tr>",
        '                         <td align="Right"><b>Approved Date: </b></td>',
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


def _page(*blocks: list[str]) -> list[str]:
    return ["<html>", "<table>", ""] + [ln for b in blocks for ln in b] + ["</table>", "</html>"]


def _write_chain(data_dir: Path, states: list[list[str]]) -> list[Path]:
    diffs_dir = data_dir / "wslcb" / "licensinginfo-diffs" / "approvals"
    diffs_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for i in range(1, len(states)):
        old_dt = datetime(2025, 2, i, 0, 15, 0, tzinfo=UTC)
        new_dt = datetime(2025, 2, i + 1, 0, 15, 0, tzinfo=UTC)
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
        p = diffs_dir / f"2025_02_{i + 1:02d}-00_15_00-approvals-diff.txt"
        p.write_text(diff)
        files.append(p)
    return files


def _two_record_states() -> list[list[str]]:
    s1 = _page(_block("2/1/2025", "REPLAY ALPHA", "994001"))
    s2 = _page(
        _block("2/1/2025", "REPLAY ALPHA", "994001"),
        _block("2/2/2025", "REPLAY BRAVO", "994002"),
    )
    return [s1, s2]


# ── extract_rel_path ──────────────────────────────────────────────────


class TestExtractRelPath:
    def test_natural_key_path_with_slugged_components(self):
        rec = {
            "section_type": "approved",
            "record_date": "2025-02-02",
            "license_number": "994002",
            "application_type": "CHANGE OF LOCATION",
        }
        assert extract_rel_path(rec) == (
            "wslcb/licensinginfo-replay/approvals/2025-02-02/994002-change-of-location.html.gz"
        )

    def test_new_application_section_maps_to_notifications_dir(self):
        rec = {
            "section_type": "new_application",
            "record_date": "2025-02-02",
            "license_number": "081 234",
            "application_type": "ADDED FEES",
        }
        assert extract_rel_path(rec) == (
            "wslcb/licensinginfo-replay/notifications/2025-02-02/081-234-added-fees.html.gz"
        )


# ── generate_replay_extracts ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_counts_without_writing(tmp_path):
    """dry_run replays and counts records but writes no files."""
    _write_chain(tmp_path, _two_record_states())
    with patch("wslcb_licensing_tracker.replay_extracts.DATA_DIR", tmp_path):
        totals = await generate_replay_extracts(None, dry_run=True)
    assert totals["records"] >= 1
    assert totals["written"] == 0
    assert not (tmp_path / "wslcb" / "licensinginfo-replay").exists()


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_generate_writes_extract_source_and_link(pg_engine, tmp_path):
    """Matched records get an extract file, a co_replay source, and a link."""
    _write_chain(tmp_path, _two_record_states())
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        await backfill_diffs(pg_engine)
    with patch("wslcb_licensing_tracker.replay_extracts.DATA_DIR", tmp_path):
        totals = await generate_replay_extracts(pg_engine)

    assert totals["written"] >= 1
    assert totals["linked"] == totals["written"]

    extract = (
        tmp_path
        / "wslcb"
        / "licensinginfo-replay"
        / "approvals"
        / "2025-02-02"
        / "994002-new-application.html.gz"
    )
    assert extract.exists()
    content = gzip.decompress(extract.read_bytes()).decode()
    assert "<tbody" in content.lower()
    assert "REPLAY BRAVO" in content

    async with pg_engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT s.id, s.captured_at, s.metadata::text AS metadata "
                    "FROM sources s JOIN source_types st ON st.id = s.source_type_id "
                    "WHERE st.slug = 'co_replay' AND s.snapshot_path = :p"
                ),
                {
                    "p": "wslcb/licensinginfo-replay/approvals/2025-02-02/"
                    "994002-new-application.html.gz"
                },
            )
        ).first()
        assert row is not None
        assert "entry" in row.metadata

        link = (
            await conn.execute(
                text(
                    "SELECT rs.role FROM record_sources rs "
                    "JOIN license_records lr ON lr.id = rs.record_id "
                    "WHERE rs.source_id = :sid AND lr.license_number = '994002'"
                ),
                {"sid": row.id},
            )
        ).first()
        assert link is not None
        assert link.role == "replay_extract"


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_generate_is_idempotent(pg_engine, tmp_path):
    """Re-running regenerates files but creates no duplicate sources or links."""
    _write_chain(tmp_path, _two_record_states())
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        await backfill_diffs(pg_engine)
    with patch("wslcb_licensing_tracker.replay_extracts.DATA_DIR", tmp_path):
        first = await generate_replay_extracts(pg_engine)
        second = await generate_replay_extracts(pg_engine)
    assert second["written"] == first["written"]

    async with pg_engine.connect() as conn:
        count = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM sources s "
                    "JOIN source_types st ON st.id = s.source_type_id "
                    "WHERE st.slug = 'co_replay' AND s.snapshot_path LIKE :p"
                ),
                {"p": "wslcb/licensinginfo-replay/approvals/%994002%"},
            )
        ).scalar_one()
        assert count == 1


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_regeneration_refreshes_source_metadata(pg_engine, tmp_path):
    """A stale co_replay source metadata blob is refreshed on re-run."""
    _write_chain(tmp_path, _two_record_states())
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        await backfill_diffs(pg_engine)
    with patch("wslcb_licensing_tracker.replay_extracts.DATA_DIR", tmp_path):
        await generate_replay_extracts(pg_engine)

    path = "wslcb/licensinginfo-replay/approvals/2025-02-02/994002-new-application.html.gz"
    # Corrupt the stored metadata to simulate drift from a prior generation.
    async with pg_engine.connect() as conn:
        await conn.execute(
            text("UPDATE sources SET metadata = '{}' WHERE snapshot_path = :p"),
            {"p": path},
        )
        await conn.commit()

    with patch("wslcb_licensing_tracker.replay_extracts.DATA_DIR", tmp_path):
        await generate_replay_extracts(pg_engine)

    async with pg_engine.connect() as conn:
        meta = (
            await conn.execute(
                text("SELECT metadata::text FROM sources WHERE snapshot_path = :p"),
                {"p": path},
            )
        ).scalar_one()
    assert "origin" in meta
    assert "entry" in meta


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_path_collision_counted_and_second_skipped(pg_engine, tmp_path):
    """Two distinct natural keys slugging to one path: first wins, second flagged."""
    # '994 070' (space) and '994-070' (hyphen) both slug to '994-070'. Both
    # must enter via diffs (not sit in the unknown base) to be evidenced, so
    # seed a base record and add each colliding record in a later diff.
    seed = _block("2/1/2025", "REPLAY SEED", "990000")
    a = _block("2/1/2025", "REPLAY COLA", "994 070")
    b = _block("2/1/2025", "REPLAY COLB", "994-070")
    _write_chain(tmp_path, [_page(seed), _page(seed, a), _page(seed, a, b)])
    with patch("wslcb_licensing_tracker.backfill_diffs.DATA_DIR", tmp_path):
        await backfill_diffs(pg_engine)
    with patch("wslcb_licensing_tracker.replay_extracts.DATA_DIR", tmp_path):
        totals = await generate_replay_extracts(pg_engine)

    assert totals["collisions"] >= 1
    # Exactly one extract exists at the colliding path; only one link to it.
    collide = (
        tmp_path
        / "wslcb"
        / "licensinginfo-replay"
        / "approvals"
        / "2025-02-01"
        / "994-070-new-application.html.gz"
    )
    assert collide.exists()
    async with pg_engine.connect() as conn:
        links = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM record_sources rs "
                    "JOIN sources s ON s.id = rs.source_id "
                    "WHERE s.snapshot_path = :p AND rs.role = 'replay_extract'"
                ),
                {
                    "p": "wslcb/licensinginfo-replay/approvals/2025-02-01/"
                    "994-070-new-application.html.gz"
                },
            )
        ).scalar_one()
    assert links == 1


@_needs_db
@pytest.mark.asyncio(loop_scope="session")
async def test_unmatched_records_skipped_without_writes(pg_engine, tmp_path):
    """Replayed records absent from license_records produce no extract or source."""
    s1 = _page(_block("2/1/2025", "REPLAY GHOST", "994999"))
    s2 = _page(
        _block("2/1/2025", "REPLAY GHOST", "994999"),
        _block("2/2/2025", "REPLAY PHANTOM", "994998"),
    )
    _write_chain(tmp_path, [s1, s2])
    # No backfill — nothing in license_records matches.
    with patch("wslcb_licensing_tracker.replay_extracts.DATA_DIR", tmp_path):
        totals = await generate_replay_extracts(pg_engine)
    assert totals["unmatched"] >= 1
    assert totals["written"] == 0
    assert not (tmp_path / "wslcb" / "licensinginfo-replay").exists()
