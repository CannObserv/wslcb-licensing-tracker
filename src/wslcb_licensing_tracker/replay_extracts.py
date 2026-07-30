"""Replay-generated source provenance extracts (#154).

Renders each diff-derived record's first-evidencing ``<tbody>`` block —
reconstructed by the chain replay (``diff_replay``) — to a per-record
gzip file under ``data/wslcb/licensinginfo-replay/``, creates a
``co_replay`` source row pointing at it, and links it to the record.

Extract files are *derived* data, regenerable at any time; paths are
named by the record's natural key so regeneration overwrites in place
and never strands ``sources.snapshot_path``. The ``co_replay`` source
rows themselves are frozen provenance once written. See
``docs/specs/2026-07-30-154-replay-extracts-design.md``.
"""

import gzip
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .backfill_diffs import diff_section_dirs
from .db import (
    DATA_DIR,
    SOURCE_TYPE_CO_REPLAY,
    WSLCB_SOURCE_URL,
    get_or_create_source,
    link_record_source,
)
from .diff_replay import replay_diff_chain
from .engine import get_db
from .models import license_records, sources
from .parser import SECTION_DIR_MAP, glob_with_gz

logger = logging.getLogger(__name__)

REPLAY_SUBDIR = "wslcb/licensinginfo-replay"
LINK_ROLE = "replay_extract"

_DIR_BY_SECTION = {section_type: dir_name for dir_name, section_type in SECTION_DIR_MAP.items()}

_COMMIT_EVERY = 500


def _slug(value: str) -> str:
    """Lowercase *value* and collapse runs of non-alphanumerics to ``-``."""
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def extract_rel_path(rec: dict) -> str:
    """DATA_DIR-relative extract path for a replayed record's natural key."""
    section_dir = _DIR_BY_SECTION[rec["section_type"]]
    fname = f"{_slug(rec['license_number'])}-{_slug(rec['application_type'])}.html.gz"
    return f"{REPLAY_SUBDIR}/{section_dir}/{rec['record_date']}/{fname}"


def _natural_key(rec: dict) -> tuple:
    """The record's identity tuple — section, date, license, application type."""
    return (
        rec["section_type"],
        rec["record_date"],
        rec["license_number"],
        rec["application_type"],
    )


def _write_extract(path: Path, raw_lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(raw_lines) + "\n"
    path.write_bytes(gzip.compress(content.encode()))


async def _lookup_record_id(conn: AsyncConnection, rec: dict) -> int | None:
    result = await conn.execute(
        select(license_records.c.id)
        .where(
            license_records.c.section_type == rec["section_type"],
            license_records.c.record_date == rec["record_date"],
            license_records.c.license_number == rec["license_number"],
            license_records.c.application_type == rec["application_type"],
        )
        .limit(1)
    )
    row = result.first()
    return row[0] if row else None


async def generate_replay_extracts(
    engine: AsyncEngine | None,
    *,
    section: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Generate provenance extracts for all replay-recoverable records.

    Replays each section's diff chain with raw-block retention, then for
    every record that exists in ``license_records`` (matched by natural
    key): writes the extract file, upserts the ``co_replay`` source row,
    and links it with role ``replay_extract``. Records absent from the
    database (post-remediation phantoms) are counted as ``unmatched`` and
    skipped entirely. Idempotent — safe to re-run; files are overwritten
    in place and source/link inserts are conflict-guarded.

    With *dry_run*, replays and counts only; no files or rows are written.

    Returns totals: ``records``, ``written``, ``linked``, ``unmatched``,
    ``collisions``, ``errors``.
    """
    totals = {
        "records": 0,
        "written": 0,
        "linked": 0,
        "unmatched": 0,
        "collisions": 0,
        "errors": 0,
    }

    for section_type, section_dir in diff_section_dirs(DATA_DIR, section):
        files = glob_with_gz(section_dir, "*.txt")
        if not files:
            continue
        result = replay_diff_chain(files, section_type, keep_raw=True)
        totals["records"] += len(result.records)
        totals["errors"] += result.stats["read_errors"]
        logger.info("%s: replayed %d records", section_type, len(result.records))

        if dry_run:
            continue

        generated_at = datetime.now(UTC).isoformat()
        # Natural-key path is unique per record by construction, but distinct
        # keys can slug to the same path (e.g. '994 070' vs '994-070'). Guard
        # against silently overwriting one record's extract with another's:
        # first key wins, later collisions are logged and skipped (absence
        # over a provenance falsehood).
        seen_paths: dict[str, tuple] = {}
        async with get_db(engine) as conn:
            pending = 0
            for rec in result.records:
                record_id = await _lookup_record_id(conn, rec)
                if record_id is None:
                    totals["unmatched"] += 1
                    continue
                rel_path = extract_rel_path(rec)
                key = _natural_key(rec)
                prior = seen_paths.get(rel_path)
                if prior is not None and prior != key:
                    logger.error(
                        "Extract path collision at %s: kept %s, skipped %s",
                        rel_path,
                        prior,
                        key,
                    )
                    totals["collisions"] += 1
                    continue
                seen_paths[rel_path] = key
                _write_extract(DATA_DIR / rel_path, rec["raw_lines"])
                totals["written"] += 1
                metadata = {
                    "origin": rec["origin"],
                    "evidencing_file": rec["source_file"],
                    "generated_at": generated_at,
                }
                source_id = await get_or_create_source(
                    conn,
                    SOURCE_TYPE_CO_REPLAY,
                    snapshot_path=rel_path,
                    url=WSLCB_SOURCE_URL,
                    captured_at=rec["scraped_at"],
                    metadata=metadata,
                )
                # get_or_create_source is ON CONFLICT DO NOTHING, so on a
                # regeneration the existing row keeps its old metadata. Refresh
                # it explicitly to track the latest replay attribution.
                await conn.execute(
                    update(sources)
                    .where(sources.c.id == source_id)
                    .values(captured_at=rec["scraped_at"], metadata=json.dumps(metadata))
                )
                await link_record_source(conn, record_id, source_id, role=LINK_ROLE)
                totals["linked"] += 1
                pending += 1
                if pending >= _COMMIT_EVERY:
                    await conn.commit()
                    pending = 0
            await conn.commit()

    logger.info(
        "Replay extracts complete: records=%d written=%d linked=%d unmatched=%d errors=%d",
        totals["records"],
        totals["written"],
        totals["linked"],
        totals["unmatched"],
        totals["errors"],
    )
    return totals
