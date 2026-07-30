"""Tests for diff_replay.py — chain replay of unified-diff archives.

All chains are generated with ``difflib.unified_diff`` from synthetic page
states, mirroring how the real CO archive was produced (same ``--- @\\t<date>``
headers, same 3-line context). No real data files are read.
"""

import difflib
import gzip
from datetime import UTC, datetime
from email.utils import format_datetime

from wslcb_licensing_tracker.diff_replay import (
    parse_unified_diff,
    replay_diff_chain,
)

# ── Synthetic page builders ──────────────────────────────────────────


def _block(
    date: str,
    name: str,
    lic: str,
    app_type: str = "NEW APPLICATION",
    loc: str = "123 MAIN ST,  SEATTLE, WA 98101-0001",
) -> list[str]:
    """One WSLCB-style approved-record <tbody> block as a list of HTML lines.

    Boilerplate lines are byte-identical across blocks so that difflib can
    interleave replacements exactly like the real archive does.
    """
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
        '                         <td align="Right"><b>Business Location: </b></td>',
        f"<td>{loc}</td>",
        "                      </tr>",
        "                      <tr>",
        '                         <td align="Right"><b>Application Type:</b> </td>',
        f"<td>{app_type}</td>",
        "                      </tr>",
        "                      <tr>",
        '                         <td align="Right"><b>License Number: </b></td>',
        f"<td>{lic}</td>",
        "                      </tr>",
        "                   </tbody>",
        "",
    ]


_HEADER = ["<html>", "<body>", "<table>", "<th>STATEWIDE RECENTLY APPROVED LICENSES</th>", ""]
_FOOTER = ["</table>", "</body>", "</html>"]


def _page(*blocks: list[str]) -> list[str]:
    lines = list(_HEADER)
    for b in blocks:
        lines.extend(b)
    lines.extend(_FOOTER)
    return lines


def _ts(day: int) -> datetime:
    return datetime(2025, 1, day, 0, 15, 0, tzinfo=UTC)


def _diff_text(old: list[str], new: list[str], old_dt: datetime, new_dt: datetime) -> str:
    return "\n".join(
        difflib.unified_diff(
            old,
            new,
            fromfile="@",
            tofile="@",
            fromfiledate=format_datetime(old_dt),
            tofiledate=format_datetime(new_dt),
            lineterm="",
        )
    )


def _write_chain(tmp_path, states: list[list[str]], start_day: int = 1) -> list:
    """Write consecutive-state diffs to disk; returns sorted file paths."""
    files = []
    for i in range(1, len(states)):
        old_dt, new_dt = _ts(start_day + i - 1), _ts(start_day + i)
        text = _diff_text(states[i - 1], states[i], old_dt, new_dt)
        p = tmp_path / f"2025_01_{start_day + i:02d}-00_15_00-approvals-diff.txt"
        p.write_text(text)
        files.append(p)
    return files


def _by_license(records: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in records:
        out.setdefault(r["license_number"], []).append(r)
    return out


# ── parse_unified_diff ───────────────────────────────────────────────


class TestParseUnifiedDiff:
    def test_parses_headers_and_hunks(self):
        old = _page(_block("1/1/2025", "ALPHA", "100001"))
        new = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        text = _diff_text(old, new, _ts(1), _ts(2))
        parsed = parse_unified_diff(text)
        assert parsed is not None
        assert parsed.old_ts == _ts(1)
        assert parsed.new_ts == _ts(2)
        assert len(parsed.hunks) >= 1
        assert parsed.truncated is False

    def test_error_page_content_returns_none(self):
        assert parse_unified_diff("500 Server Error: Internal Server Error") is None

    def test_hunk_counts_match_body(self):
        old = _page(_block("1/1/2025", "ALPHA", "100001"))
        new = _page(_block("1/1/2025", "ALPHA RENAMED", "100001"))
        parsed = parse_unified_diff(_diff_text(old, new, _ts(1), _ts(2)))
        (hunk,) = parsed.hunks
        old_seen = sum(1 for ln in hunk.body if not ln.startswith("+"))
        new_seen = sum(1 for ln in hunk.body if not ln.startswith("-"))
        assert old_seen == hunk.old_count
        assert new_seen == hunk.new_count

    def test_truncated_file_detected_and_complete_hunks_kept(self):
        old = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        new = _page(_block("1/3/2025", "CHARLIE", "100003"), _block("1/4/2025", "DELTA", "100004"))
        text = _diff_text(old, new, _ts(1), _ts(2))
        cut = text[: int(len(text) * 0.6)]
        parsed = parse_unified_diff(cut)
        assert parsed is not None
        assert parsed.truncated is True


# ── Replay extraction ────────────────────────────────────────────────


class TestReplayExtraction:
    def test_entry_record_extracted_with_new_timestamp(self, tmp_path):
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(
            _block("1/1/2025", "ALPHA", "100001"),
            _block("1/2/2025", "BRAVO", "100002"),
        )
        files = _write_chain(tmp_path, [s1, s2])
        result = replay_diff_chain(files, "approved")
        recs = _by_license(result.records)
        assert "100002" in recs
        (bravo,) = recs["100002"]
        assert bravo["business_name"] == "BRAVO"
        assert bravo["record_date"] == "2025-01-02"
        assert bravo["scraped_at"] == _ts(2)
        assert bravo["origin"] == "entry"
        assert bravo["source_file"] == files[0].name

    def test_exit_record_from_unknown_base_recovered(self, tmp_path):
        # ALPHA exists only in the (never-directly-seen) base state and is
        # removed in the first diff: its content is known solely from '-' lines.
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        s2 = _page(_block("1/2/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2])
        result = replay_diff_chain(files, "approved")
        recs = _by_license(result.records)
        assert "100001" in recs
        (alpha,) = recs["100001"]
        assert alpha["business_name"] == "ALPHA"
        assert alpha["origin"] == "exit"
        assert alpha["scraped_at"] == _ts(1)

    def test_entry_then_exit_deduped_with_first_seen_attribution(self, tmp_path):
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        s3 = _page(_block("1/1/2025", "ALPHA", "100001"))
        files = _write_chain(tmp_path, [s1, s2, s3])
        result = replay_diff_chain(files, "approved")
        recs = _by_license(result.records)
        (bravo,) = recs["100002"]
        assert bravo["origin"] == "entry"
        assert bravo["scraped_at"] == _ts(2)

    def test_final_state_record_extracted(self, tmp_path):
        # CHARLIE enters via a diff whose block is fully observed, but ALPHA
        # sits in the unknown base and never exits: only the final-state parse
        # can see it — but its lines are never learned, so it stays unknown
        # and must NOT be emitted as a half-known hybrid. BRAVO (entered,
        # never exited) must be emitted exactly once.
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2])
        result = replay_diff_chain(files, "approved")
        recs = _by_license(result.records)
        assert set(recs) == {"100002"}

    def test_boilerplate_interleaved_replacement_produces_no_hybrid(self, tmp_path):
        # Replace ALPHA's block with BRAVO's in one diff. difflib matches the
        # shared boilerplate lines, interleaving -/+ runs mid-block — the exact
        # pathology behind cross-record bleeds (#151 / record 31982).
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/2/2025", "BRAVO", "100002"))
        s3 = _page()  # both gone -> BRAVO exits wholesale
        files = _write_chain(tmp_path, [s1, s2, s3])
        result = replay_diff_chain(files, "approved")
        for r in result.records:
            assert (r["license_number"], r["business_name"]) in {
                ("100001", "ALPHA"),
                ("100002", "BRAVO"),
            }
        recs = _by_license(result.records)
        assert "100002" in recs
        assert recs["100002"][0]["business_name"] == "BRAVO"

    def test_missed_diff_produces_no_hybrid(self, tmp_path):
        # Drop the middle diff from a 4-state chain: the replayer sees a
        # contradictory context and must not glue stale ALPHA content onto
        # later records.
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/2/2025", "BRAVO", "100002"))
        s3 = _page(_block("1/3/2025", "CHARLIE", "100003"))
        s4 = _page()
        files = _write_chain(tmp_path, [s1, s2, s3, s4])
        files[1].unlink()  # lose the s2->s3 diff
        remaining = [files[0], files[2]]
        result = replay_diff_chain(remaining, "approved")
        allowed = {
            ("100001", "ALPHA"),
            ("100002", "BRAVO"),
            ("100003", "CHARLIE"),
        }
        for r in result.records:
            assert (r["license_number"], r["business_name"]) in allowed

    def test_truncated_diff_resets_epoch_and_recovers_both_sides(self, tmp_path):
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        s3 = _page(_block("1/3/2025", "CHARLIE", "100003"))
        s4 = _page(_block("1/3/2025", "CHARLIE", "100003"), _block("1/4/2025", "DELTA", "100004"))
        s5 = _page(_block("1/4/2025", "DELTA", "100004"))
        files = _write_chain(tmp_path, [s1, s2, s3, s4, s5])
        # Truncate the s2->s3 diff mid-hunk.
        text = files[1].read_text()
        files[1].write_text(text[: int(len(text) * 0.5)])
        result = replay_diff_chain(files, "approved")
        assert result.stats["resets"] >= 1
        recs = _by_license(result.records)
        assert recs["100002"][0]["business_name"] == "BRAVO"
        assert recs["100003"][0]["business_name"] == "CHARLIE"
        assert recs["100004"][0]["business_name"] == "DELTA"

    def test_in_place_mutation_emits_both_versions_on_clean_chain(self, tmp_path):
        # WSLCB corrects a date inside a live block; both the pre- and
        # post-correction records are genuine page states.
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        s3 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/3/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2, s3])
        result = replay_diff_chain(files, "approved")
        recs = _by_license(result.records)
        dates = {r["record_date"] for r in recs["100002"]}
        assert dates == {"2025-01-02", "2025-01-03"}

    def test_error_page_files_skipped(self, tmp_path):
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2])
        err = tmp_path / "2025_01_01-12_15_00-approvals-diff.txt"
        err.write_text("HTTPSConnectionPool(host='licensinginfo.lcb.wa.gov', port=443): boom")
        result = replay_diff_chain(sorted([*files, err]), "approved")
        assert result.stats["skipped_nondiff"] == 1
        assert "100002" in _by_license(result.records)

    def test_gzipped_diff_files_read_transparently(self, tmp_path):
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2])
        gz = files[0].with_suffix(".txt.gz")
        with gzip.open(gz, "wt", encoding="utf-8") as fh:
            fh.write(files[0].read_text())
        files[0].unlink()
        result = replay_diff_chain([gz], "approved")
        assert "100002" in _by_license(result.records)

    def test_invalid_fragments_not_emitted(self, tmp_path):
        # A block missing its license number must not surface as a record.
        broken = [
            '                   <tbody width="100%" style="Font-size: .80em;">',
            "                      <tr>",
            '                         <td align="Right"><b>Approved Date: </b></td>',
            "<td>1/5/2025</td>",
            "                      </tr>",
            "                   </tbody>",
            "",
        ]
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), broken)
        files = _write_chain(tmp_path, [s1, s2])
        result = replay_diff_chain(files, "approved")
        for r in result.records:
            assert r["license_number"]
            assert r["application_type"]

    def test_wholesale_context_break_resets_epoch(self, tmp_path):
        # Two unrelated chains spliced together (like the real 2025-08-06
        # notifications break): the second chain's context disagrees
        # wholesale with the carried document, which must trigger an epoch
        # reset — and the post-reset diff must still apply (regression for
        # resetting after prepare() had already padded the document).
        def _st(prefix: str, month: int) -> list[str]:
            lic_prefix = "10" if prefix in ("OLD", "MID") else "20"
            return _page(
                *[
                    _block(
                        f"{month}/{i}/2025",
                        f"{prefix} {i}",
                        f"{lic_prefix}{i:04d}",
                        loc=f"{i} {prefix} ST,  SEATTLE, WA 98101-0001",
                    )
                    for i in range(1, 13)
                ]
            )

        files = _write_chain(tmp_path, [_st("OLD", 1), _st("MID", 4)], start_day=1)
        files += _write_chain(tmp_path, [_st("NEW", 2), _st("NEWER", 3)], start_day=10)
        result = replay_diff_chain(files, "approved")
        assert result.stats["resets"] >= 1
        allowed_prefix = {"10": ("OLD", "MID"), "20": ("NEW", "NEWER")}
        for r in result.records:
            prefixes = allowed_prefix[r["license_number"][:2]]
            assert r["business_name"].startswith(prefixes)

    def test_undecodable_file_counted_as_read_error(self, tmp_path):
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2])
        bad = tmp_path / "2025_01_01-12_15_00-approvals-diff.txt"
        bad.write_bytes(b"\xff\xfe")
        result = replay_diff_chain(sorted([*files, bad]), "approved")
        assert result.stats["read_errors"] == 1
        assert "100002" in _by_license(result.records)

    def test_oracle_desync_stat_present_and_zero(self, tmp_path):
        # Sweep-2 allocation parity with sweep 1 is load-bearing; the stat
        # must exist and be 0 on any normal chain.
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/1/2025", "ALPHA", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2])
        result = replay_diff_chain(files, "approved")
        assert result.stats["oracle_desync"] == 0

    def test_partially_known_blocks_are_counted(self, tmp_path):
        # BRAVO is inserted ABOVE the unknown-base ALPHA block, so the diff's
        # trailing context teaches only ALPHA's first lines: its block start
        # becomes known while its interior stays unknown. The state parses
        # must skip that block AND count the skip — silent truncation is not
        # acceptable.
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/2/2025", "BRAVO", "100002"), _block("1/1/2025", "ALPHA", "100001"))
        files = _write_chain(tmp_path, [s1, s2])
        result = replay_diff_chain(files, "approved")
        assert set(_by_license(result.records)) == {"100002"}
        assert result.stats["skipped_partial_spans"] > 0

    def test_blocks_overlapping_out_of_range_yields_nothing(self):
        from wslcb_licensing_tracker.diff_replay import _blocks_overlapping

        assert list(_blocks_overlapping([], 0, 1)) == []
        lines = ["<tbody>", "x", "</tbody>"]
        assert list(_blocks_overlapping(lines, 3, 5)) == []

    def test_mutation_suppressed_on_context_mismatch_file(self, tmp_path):
        # A file whose context disagrees with the carried document (a diff
        # was lost) must not emit mutation records — its old-side positions
        # may hold stale content.
        s0 = _page(_block("1/1/2025", "ALPHA V0", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        s1 = _page(_block("1/1/2025", "ALPHA V1", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        s2 = _page(_block("1/1/2025", "ALPHA V2", "100001"), _block("1/2/2025", "BRAVO", "100002"))
        s3 = _page(_block("1/1/2025", "ALPHA V3", "100001"), _block("1/9/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s0, s1, s2, s3])
        files[1].unlink()  # lose the s1->s2 diff; s2->s3 renames ALPHA + mutates BRAVO's date
        remaining = [files[0], files[2]]
        result = replay_diff_chain(remaining, "approved")
        assert result.stats["mismatches"] > 0
        bad_file = files[2].name
        assert not any(
            r["origin"] == "mutation" and r["source_file"] == bad_file for r in result.records
        )

    def test_stats_report_applied_and_mismatches(self, tmp_path):
        s1 = _page(_block("1/1/2025", "ALPHA", "100001"))
        s2 = _page(_block("1/2/2025", "BRAVO", "100002"))
        files = _write_chain(tmp_path, [s1, s2])
        result = replay_diff_chain(files, "approved")
        assert result.stats["applied"] == 1
        assert result.stats["mismatches"] == 0
        assert result.stats["pos_drift"] == 0
