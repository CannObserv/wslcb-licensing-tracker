"""Chain replay of unified-diff archives — the structural fix for #151.

The CO diff archive is a continuous positional chain: difflib-style unified
diffs with exact ``@@`` line numbers, where each diff's old side is the
previous diff's new side. Instead of parsing each diff's changed-only line
stream (which breaks HTML label/value pairing and produced cross-record
"bleed" hybrids), this module *replays* the chain, maintaining the full
document as a list of line-IDs into a content store. Context and removed
lines teach the content of previously unknown positions; because IDs are
shared across document versions, knowledge propagates to every state that
contains the same physical line.

Records are extracted only from sources that cannot mix page states:

- **entry** — a block around added lines whose every line was directly
  observed in the same diff (added or context);
- **exit** — a complete ``<tbody>`` block inside a contiguous run of ``-``
  lines (verbatim diff text, no positional completion);
- **mutation** — a block edited in place by a hunk, emitted from the old
  document only when the whole file's context matched (clean chain step);
- **state** — fully-known blocks of a boundary state (chain base, the first
  state after an epoch reset, and the final state). Blocks containing any
  unknown line are skipped, never stripped.

Chain-damage policies: a context line disagreeing with known content means
one or more diffs were lost — the incoming context is authoritative for the
current state, so that position is *forked* to a fresh ID (earlier states
keep theirs). When a diff's context disagrees wholesale, the chain is broken
and the document restarts from an unknown base (*epoch reset*); truncated
diff files also reset.

Replay runs two sweeps: sweep 1 learns all content; sweep 2 replays with
sweep 1's completed store shared as an oracle and extracts records. ID
allocation is deterministic across sweeps, which is what makes the sharing
valid.
"""

import logging
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .parser import (
    _read_text_strict,
    is_valid_record,
    parse_diff_timestamp,
    parse_html_lines,
)

logger = logging.getLogger(__name__)

_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

# Epoch-reset detection: a diff whose known context disagrees this badly is a
# chain break, not a small missed update. RESET_MIN keeps tiny pages (where a
# handful of forks is normal repair) from resetting.
RESET_RATIO = 0.20
RESET_MIN = 50

_HEADER_LINES = 2  # '--- ' and '+++ '


# ── Unified-diff parsing ─────────────────────────────────────────────


@dataclass
class Hunk:
    """One ``@@ -a,b +c,d @@`` hunk with its body lines (prefixes intact)."""

    old_start: int
    old_count: int
    new_start: int
    new_count: int
    body: list[str]


@dataclass
class ParsedDiff:
    """A parsed unified-diff file."""

    old_ts: datetime
    new_ts: datetime
    hunks: list[Hunk]
    truncated: bool


def _parse_hunk_body(
    lines: list[str], i: int, old_count: int, new_count: int
) -> tuple[list[str], bool, int]:
    """Collect one hunk's body lines starting at *i*.

    Returns ``(body, complete, next_index)`` — *complete* is False when the
    body ends (EOF or next ``@@``) before both counts are satisfied.
    """
    body: list[str] = []
    old_seen = new_seen = 0
    n = len(lines)
    while i < n and (old_seen < old_count or new_seen < new_count):
        bl = lines[i]
        if bl.startswith("@@"):
            break
        if bl.startswith("\\"):  # "\ No newline at end of file"
            i += 1
            continue
        body.append(bl)
        if bl.startswith("+"):
            new_seen += 1
        elif bl.startswith("-"):
            old_seen += 1
        else:
            old_seen += 1
            new_seen += 1
        i += 1
    return body, old_seen == old_count and new_seen == new_count, i


def parse_unified_diff(content: str) -> ParsedDiff | None:
    """Parse a difflib-style unified diff; ``None`` if *content* is not one.

    The archive stores failed captures (HTTP error text) alongside real
    diffs — those have no ``---``/``+++`` headers and return ``None``.
    ``truncated`` is set when the file ends mid-hunk (cut off mid-write);
    complete hunks before the cut are kept.
    """
    lines = content.split("\n")
    if len(lines) < _HEADER_LINES:
        return None
    if not lines[0].startswith("--- ") or not lines[1].startswith("+++ "):
        return None
    old_ts = parse_diff_timestamp(lines[0])
    new_ts = parse_diff_timestamp(lines[1])

    hunks: list[Hunk] = []
    truncated = False
    i = _HEADER_LINES
    while i < len(lines):
        m = _HUNK_RE.match(lines[i])
        if not m:
            i += 1
            continue
        a = int(m.group(1))
        b = int(m.group(2)) if m.group(2) is not None else 1
        c = int(m.group(3))
        d = int(m.group(4)) if m.group(4) is not None else 1
        body, complete, i = _parse_hunk_body(lines, i + 1, b, d)
        if not complete:
            truncated = True
            break
        hunks.append(Hunk(a, b, c, d, body))
    return ParsedDiff(old_ts, new_ts, hunks, truncated)


# ── Replay engine ────────────────────────────────────────────────────


class _Replayer:
    """Positional document replay: line-ID doc + content store.

    ``store[id]`` is the line's text, or ``None`` while unknown. ``ids`` is
    the current document as a list of store indices. ID allocation order is
    deterministic for a given diff sequence, so sweep 2 can share sweep 1's
    completed store as *oracle*: IDs below the oracle's length are reused
    (their content is already the fully-learned value) instead of appended.
    """

    def __init__(self, oracle: list[str | None] | None = None) -> None:
        self.store: list[str | None] = oracle if oracle is not None else []
        self._reused = len(self.store)  # IDs below this are reused, not appended
        self._next = 0
        self.ids: list[int] = []
        self.stats = {
            "applied": 0,
            "skipped_nondiff": 0,
            "read_errors": 0,
            "resets": 0,
            "learned": 0,
            "mismatches": 0,
            "forks": 0,
            "pos_drift": 0,
        }
        self.reset_files: list[str] = []

    def _new_id(self, text: str | None) -> int:
        """Allocate the next line ID.

        In oracle mode (sweep 2), IDs below the oracle's length keep their
        already-learned content — never overwritten, so padded positions
        retain knowledge learned later in sweep 1.
        """
        nid = self._next
        self._next += 1
        if nid < self._reused:
            return nid
        self.store.append(text)
        return nid

    def prepare(self, hunks: list[Hunk]) -> None:
        """Pad the doc with unknown lines up to the highest old-side position.

        Must run before ``apply`` (and before any old-side materialisation)
        so that ID allocation order is identical in both sweeps.
        """
        need = 0
        for h in hunks:
            old_start = (h.old_start - 1) if h.old_count > 0 else h.old_start
            need = max(need, old_start + h.old_count)
        while len(self.ids) < need:
            self.ids.append(self._new_id(None))

    def reset(self, fname: str) -> None:
        """Epoch reset: restart from an unknown base document."""
        self.ids = []
        self.stats["resets"] += 1
        self.reset_files.append(fname)

    def scan_mismatch(self, hunks: list[Hunk]) -> tuple[int, int]:
        """Count (known old-side lines, mismatching) without mutating."""
        known = bad = 0
        for h in hunks:
            pos = (h.old_start - 1) if h.old_count > 0 else h.old_start
            for bl in h.body:
                if bl.startswith("+"):
                    continue
                text = bl[1:] if bl.startswith(("-", " ")) else bl
                if pos < len(self.ids):
                    cur = self.store[self.ids[pos]]
                    if cur is not None:
                        known += 1
                        if cur != text:
                            bad += 1
                pos += 1
        return known, bad

    def apply(  # noqa: C901, PLR0912, PLR0915  # stateful hunk walker; splitting obscures it
        self, hunks: list[Hunk]
    ) -> tuple[list[tuple[int, int]], set[int], int]:
        """Apply hunks to the document.

        Returns ``(added_ranges, confirmed_ids, file_mismatches)`` where
        *added_ranges* are (start, end) index ranges of added lines in the
        new document and *confirmed_ids* are the IDs whose content was
        directly observed in this diff.
        """
        new_ids: list[int] = []
        added_ranges: list[tuple[int, int]] = []
        confirmed: set[int] = set()
        file_mm = 0
        cursor = 0
        for h in hunks:
            old_start = (h.old_start - 1) if h.old_count > 0 else h.old_start
            if old_start < cursor:
                self.stats["pos_drift"] += 1
                old_start = cursor
            new_ids.extend(self.ids[cursor:old_start])
            cursor = old_start
            expected = len(new_ids) + 1 if h.new_count > 0 else len(new_ids)
            if expected != h.new_start:
                self.stats["pos_drift"] += 1
            run_start: int | None = None
            for bl in h.body:
                if bl.startswith("+"):
                    if run_start is None:
                        run_start = len(new_ids)
                    nid = self._new_id(bl[1:])
                    confirmed.add(nid)
                    new_ids.append(nid)
                    continue
                if run_start is not None:
                    added_ranges.append((run_start, len(new_ids)))
                    run_start = None
                text = bl[1:] if bl.startswith(("-", " ")) else bl
                lid = self.ids[cursor]
                cur = self.store[lid]
                if bl.startswith("-"):
                    if cur is None:
                        self.store[lid] = text
                        self.stats["learned"] += 1
                    elif cur != text:
                        self.stats["mismatches"] += 1
                        file_mm += 1
                elif cur is None:
                    self.store[lid] = text
                    self.stats["learned"] += 1
                    confirmed.add(lid)
                    new_ids.append(lid)
                elif cur != text:
                    # Fork: the incoming context is authoritative for the
                    # current state; earlier states keep the old ID.
                    self.stats["mismatches"] += 1
                    self.stats["forks"] += 1
                    file_mm += 1
                    nid = self._new_id(text)
                    confirmed.add(nid)
                    new_ids.append(nid)
                else:
                    confirmed.add(lid)
                    new_ids.append(lid)
                cursor += 1
            if run_start is not None:
                added_ranges.append((run_start, len(new_ids)))
        new_ids.extend(self.ids[cursor:])
        self.ids = new_ids
        self.stats["applied"] += 1
        return added_ranges, confirmed, file_mm

    def materialize(self) -> list[str | None]:
        st = self.store
        return [st[i] for i in self.ids]


# ── Block helpers ────────────────────────────────────────────────────


def _blocks_overlapping(  # noqa: C901, PLR0912  # boundary-guarded scan; splitting obscures it
    lines: list[str | None], start: int, end: int
) -> Iterator[tuple[int, int]]:
    """Yield complete ``<tbody>``..``</tbody>`` spans overlapping [start, end).

    A range may begin mid-block (expand upward to the block start), contain
    several whole blocks, or end mid-block (expand forward to the block end).
    Hitting an unknown line anywhere aborts, so a partially-known block can
    never be emitted.
    """
    lo: int | None = None
    i = start
    while i >= 0:
        t = lines[i]
        if t is None:
            break
        tl = t.lower()
        if "<tbody" in tl:
            lo = i
            break
        if "</tbody>" in tl and i < start:
            break
        i -= 1
    if lo is None:
        # Range begins before/between blocks: first block start inside it.
        for j in range(start, min(end, len(lines))):
            t = lines[j]
            if t is not None and "<tbody" in t.lower():
                lo = j
                break
    if lo is None:
        return
    n = len(lines)
    i = lo
    while i < n and i < end:
        t = lines[i]
        if t is None:
            return
        if "<tbody" in t.lower():
            j = i
            while j < n:
                tj = lines[j]
                if tj is None:
                    return
                if "</tbody>" in tj.lower():
                    yield i, j + 1
                    break
                j += 1
            else:
                return
            i = j + 1
        else:
            i += 1


def _iter_known_blocks(lines: list[str | None]) -> Iterator[tuple[int, int]]:
    """Yield (lo, hi) spans of fully-known ``<tbody>``..``</tbody>`` blocks."""
    n = len(lines)
    i = 0
    while i < n:
        t = lines[i]
        if t is not None and "<tbody" in t.lower():
            j = i
            ok = True
            while j < n:
                tj = lines[j]
                if tj is None:
                    ok = False
                    break
                if "</tbody>" in tj.lower():
                    break
                j += 1
            if ok and j < n:
                yield i, j + 1
                i = j + 1
                continue
        i += 1


def _complete_blocks_in_run(run_lines: list[str]) -> list[list[str]]:
    """Complete ``<tbody>``..``</tbody>`` groups fully inside *run_lines*."""
    groups: list[list[str]] = []
    current: list[str] | None = None
    for ln in run_lines:
        low = ln.lower()
        if "<tbody" in low:
            current = [ln]
            if "</tbody>" in low:
                groups.append(current)
                current = None
        elif current is not None:
            current.append(ln)
            if "</tbody>" in low:
                groups.append(current)
                current = None
    return groups


def _removed_runs(hunks: list[Hunk]) -> list[list[str]]:
    """Contiguous ``-`` line runs across all hunks (prefixes stripped)."""
    runs: list[list[str]] = []
    for h in hunks:
        cur: list[str] | None = None
        for bl in h.body:
            if bl.startswith("-"):
                if cur is None:
                    cur = []
                cur.append(bl[1:])
            else:
                if cur:
                    runs.append(cur)
                cur = None
        if cur:
            runs.append(cur)
    return runs


# ── Chain replay + extraction ────────────────────────────────────────


@dataclass
class ReplayResult:
    """Outcome of replaying one section's diff chain."""

    records: list[dict] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


def _natural_key(rec: dict) -> tuple:
    return (
        rec["section_type"],
        rec["record_date"],
        rec["license_number"],
        rec["application_type"],
    )


class _Collector:
    """Dedup records by natural key, first source wins."""

    def __init__(self, section_type: str) -> None:
        self.section_type = section_type
        self.records: dict[tuple, dict] = {}

    def add_lines(
        self,
        line_groups: Iterable[list[str]],
        ts: datetime,
        fname: str,
        origin: str,
    ) -> None:
        for group in line_groups:
            for rec in parse_html_lines(list(group), self.section_type):
                if not is_valid_record(rec):
                    continue
                key = _natural_key(rec)
                if key not in self.records:
                    rec["scraped_at"] = ts
                    rec["source_file"] = fname
                    rec["origin"] = origin
                    self.records[key] = rec

    def add_spans(
        self,
        lines: list[str | None],
        spans: Iterable[tuple[int, int]],
        ts: datetime,
        fname: str,
        origin: str,
    ) -> None:
        groups = []
        for lo, hi in spans:
            seg = lines[lo:hi]
            if any(t is None for t in seg):
                continue
            groups.append(seg)
        self.add_lines(groups, ts, fname, origin)


def _iter_parsed(files: list[Path], replayer: _Replayer) -> Iterator[tuple[Path, ParsedDiff]]:
    """Yield (path, ParsedDiff) for real diffs; count skipped non-diffs."""
    for path in files:
        try:
            content = _read_text_strict(path)
        except (OSError, UnicodeDecodeError) as exc:
            logger.warning("Unreadable diff file %s: %s", path, exc)
            replayer.stats["read_errors"] += 1
            continue
        parsed = parse_unified_diff(content)
        if parsed is None:
            replayer.stats["skipped_nondiff"] += 1
            continue
        yield path, parsed


def _learn_sweep(files: list[Path]) -> _Replayer:
    """Sweep 1: replay the chain, learning all line content.

    Returns the replayer whose store holds full-corpus knowledge and whose
    ``reset_files`` journal drives sweep 2.
    """
    rep = _Replayer()
    for path, parsed in _iter_parsed(files, rep):
        rep.prepare(parsed.hunks)
        known, bad = rep.scan_mismatch(parsed.hunks)
        if known >= RESET_MIN and bad / known >= RESET_RATIO:
            rep.reset(path.name)
            rep.prepare(parsed.hunks)  # re-pad: reset dropped the padding
        rep.apply(parsed.hunks)
        if parsed.truncated:
            rep.reset(path.name + " (truncated)")
    return rep


def replay_diff_chain(  # noqa: C901, PLR0912  # per-diff source dispatch; see module docstring
    files: list[Path], section_type: str
) -> ReplayResult:
    """Replay a section's diff chain and extract all recoverable records.

    *files* are the section's diff files (``.txt`` / ``.txt.gz``); they are
    sorted by name, which is chronological for the archive's naming scheme.
    Returns records in the standard parsed-record shape plus ``scraped_at``
    (capture timestamp evidencing the record), ``source_file`` (diff file
    name) and ``origin`` (entry/exit/mutation/state/final).
    """
    files = sorted(files, key=lambda p: p.name)

    learn = _learn_sweep(files)
    store = learn.store
    detected_resets = {n for n in learn.reset_files if not n.endswith("(truncated)")}

    # Sweep 2 shares sweep 1's store as oracle: every ID it allocates (in the
    # same deterministic order) resolves to the fully-learned content, so
    # old- and new-side states materialise fully known.
    collector = _Collector(section_type)
    rep = _Replayer(oracle=store)
    fresh_epoch = False
    first_file = True
    last_applied: tuple[str, datetime] | None = None

    for path, parsed in _iter_parsed(files, rep):
        # Mirror sweep 1's allocation order exactly (prepare → reset →
        # re-prepare) — ID parity with the oracle depends on it.
        rep.prepare(parsed.hunks)
        if path.name in detected_resets:
            rep.reset(path.name)
            fresh_epoch = True
            rep.prepare(parsed.hunks)

        old_lines = rep.materialize()
        added, confirmed, file_mm = rep.apply(parsed.hunks)
        new_lines = rep.materialize()

        # exit: complete blocks inside contiguous '-' runs (verbatim text).
        for run in _removed_runs(parsed.hunks):
            collector.add_lines(_complete_blocks_in_run(run), parsed.old_ts, path.name, "exit")

        # mutation: blocks edited in place, old versions — clean steps only.
        if file_mm == 0 and not first_file and not fresh_epoch:
            spans = []
            for h in parsed.hunks:
                if not any(bl.startswith("-") for bl in h.body):
                    continue
                old_start = (h.old_start - 1) if h.old_count > 0 else h.old_start
                spans.extend(
                    _blocks_overlapping(
                        old_lines, old_start, min(old_start + h.old_count, len(old_lines))
                    )
                )
            collector.add_spans(old_lines, spans, parsed.old_ts, path.name, "mutation")

        # state: chain base (before the first diff) — fully-known blocks.
        if first_file:
            collector.add_spans(
                old_lines, _iter_known_blocks(old_lines), parsed.old_ts, path.name, "state"
            )
            first_file = False

        # state: first state of a fresh epoch after a reset.
        if fresh_epoch:
            collector.add_spans(
                new_lines, _iter_known_blocks(new_lines), parsed.new_ts, path.name, "state"
            )
            fresh_epoch = False

        # entry: blocks around added lines, every line observed in THIS diff.
        spans = []
        for s, e in added:
            for lo, hi in _blocks_overlapping(new_lines, s, min(e, len(new_lines))):
                if any(rep.ids[i] not in confirmed for i in range(lo, hi)):
                    continue
                if spans and lo <= spans[-1][0]:
                    continue  # same block reached from an adjacent range
                spans.append((lo, hi))
        collector.add_spans(new_lines, spans, parsed.new_ts, path.name, "entry")

        last_applied = (path.name, parsed.new_ts)
        if parsed.truncated:
            rep.reset(path.name + " (truncated)")
            fresh_epoch = True

    # final: fully-known blocks of the last state.
    if last_applied is not None:
        lines = rep.materialize()
        collector.add_spans(
            lines, _iter_known_blocks(lines), last_applied[1], last_applied[0], "final"
        )

    stats = dict(learn.stats)
    stats["reset_files"] = learn.reset_files
    logger.info(
        "Replayed %d diffs (%s): %d records, %d resets, %d mismatches, %d forks",
        stats["applied"],
        section_type,
        len(collector.records),
        stats["resets"],
        stats["mismatches"],
        stats["forks"],
    )
    return ReplayResult(records=list(collector.records.values()), stats=stats)
