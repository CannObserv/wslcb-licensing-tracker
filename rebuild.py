"""Rebuild the database from archived sources.

Creates a fresh SQLite database and replays all archived HTML snapshots
and diff archives through the unified ingestion pipeline.  Optionally
compares the rebuilt database against the production database to detect
discrepancies.

Usage via CLI::

    python cli.py rebuild --output data/wslcb-rebuilt.db
    python cli.py rebuild --output data/wslcb-rebuilt.db --verify
    python cli.py rebuild --output data/wslcb-rebuilt.db --force
"""
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class RebuildResult:
    """Aggregate result of a full rebuild."""
    records: int = 0
    from_snapshots: int = 0
    from_diffs: int = 0
    locations: int = 0
    entities: int = 0
    endorsement_mappings_discovered: int = 0
    outcome_links: int = 0
    elapsed_seconds: float = 0.0


@dataclass
class ComparisonResult:
    """Result of comparing production vs rebuilt databases."""
    prod_count: int = 0
    rebuilt_count: int = 0
    missing_from_rebuilt: int = 0
    extra_in_rebuilt: int = 0
    section_counts: dict = field(default_factory=dict)
    sample_missing: list = field(default_factory=list)
    sample_extra: list = field(default_factory=list)


def rebuild_from_sources(
    *,
    output_path: Path,
    data_dir: Path,
    force: bool = False,
) -> RebuildResult:
    """Build a fresh database from all archived sources.

    Args:
        output_path: Path for the new SQLite database file.
        data_dir: Root data directory containing ``wslcb/`` archives.
        force: If True, delete existing output file before starting.

    Returns:
        :class:`RebuildResult` with aggregate counts.

    Raises:
        FileExistsError: If *output_path* already exists and *force* is False.
    """
    if output_path.exists():
        if force:
            output_path.unlink()
            logger.info("Removed existing output file: %s", output_path)
        else:
            raise FileExistsError(
                f"Output file already exists: {output_path}\n"
                f"Use --force to overwrite."
            )

    start = time.monotonic()
    result = RebuildResult()

    # Deferred imports to avoid circular dependencies and keep module
    # importable even when optional deps (httpx, etc.) are missing.
    from db import get_connection
    from schema import init_db
    from endorsements import (
        seed_endorsements, discover_code_mappings,
        repair_code_name_endorsements,
    )
    from parser import (
        snapshot_paths, extract_snapshot_date, parse_snapshot,
        discover_diff_files, extract_records_from_diff,
    )
    from database import (
        get_or_create_source, link_record_source,
        SOURCE_TYPE_CO_ARCHIVE, SOURCE_TYPE_CO_DIFF_ARCHIVE,
        WSLCB_SOURCE_URL,
    )
    from pipeline import ingest_batch, ingest_record, IngestOptions
    from link_records import build_all_links

    # Create fresh database
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(output_path)
    init_db(conn)
    seed_endorsements(conn)
    conn.commit()

    try:
        # ── Phase 1: Replay diff archives (oldest data) ──────────────
        diff_files = discover_diff_files(data_dir)
        if diff_files:
            logger.info("Phase 1: Ingesting from %d diff file(s)", len(diff_files))
            result.from_diffs = _ingest_diffs(
                conn, diff_files, data_dir,
                get_or_create_source, SOURCE_TYPE_CO_DIFF_ARCHIVE,
                WSLCB_SOURCE_URL, ingest_record, IngestOptions,
            )
            logger.info("Phase 1 complete: %d records from diffs", result.from_diffs)
        else:
            logger.info("Phase 1: No diff files found, skipping")

        # ── Phase 2: Replay HTML snapshots ───────────────────────────
        snapshots = snapshot_paths(data_dir)
        if snapshots:
            logger.info("Phase 2: Ingesting from %d snapshot(s)", len(snapshots))
            result.from_snapshots = _ingest_snapshots(
                conn, snapshots, data_dir,
                get_or_create_source, SOURCE_TYPE_CO_ARCHIVE,
                WSLCB_SOURCE_URL, extract_snapshot_date, parse_snapshot,
                ingest_batch, IngestOptions,
            )
            logger.info(
                "Phase 2 complete: %d records from snapshots",
                result.from_snapshots,
            )
        else:
            logger.info("Phase 2: No snapshots found, skipping")

        # ── Phase 3: Endorsement discovery ───────────────────────────
        logger.info("Phase 3: Running endorsement discovery")
        repair_code_name_endorsements(conn)
        learned = discover_code_mappings(conn)
        result.endorsement_mappings_discovered = len(learned)
        if learned:
            logger.info(
                "Discovered %d new code mapping(s)", len(learned),
            )
        conn.commit()

        # ── Phase 4: Build outcome links ─────────────────────────────
        logger.info("Phase 4: Building outcome links")
        build_all_links(conn)
        result.outcome_links = conn.execute(
            "SELECT COUNT(*) FROM record_links"
        ).fetchone()[0]
        conn.commit()

        # ── Final counts ─────────────────────────────────────────────
        result.records = conn.execute(
            "SELECT COUNT(*) FROM license_records"
        ).fetchone()[0]
        result.locations = conn.execute(
            "SELECT COUNT(*) FROM locations"
        ).fetchone()[0]
        result.entities = conn.execute(
            "SELECT COUNT(*) FROM entities"
        ).fetchone()[0]

    finally:
        conn.close()

    result.elapsed_seconds = time.monotonic() - start
    logger.info(
        "Rebuild complete: %d records (%d from diffs, %d from snapshots) "
        "in %.1fs",
        result.records, result.from_diffs, result.from_snapshots,
        result.elapsed_seconds,
    )
    return result


def _ingest_diffs(
    conn, diff_files, data_dir,
    get_or_create_source, source_type, source_url,
    ingest_record, IngestOptions,
) -> int:
    """Ingest records from diff archives.  Returns count of new records."""
    from parser import extract_records_from_diff

    # Deduplicate across all diff files (same logic as backfill_diffs.py)
    all_records: dict[tuple, dict] = {}
    ts_to_diff_path: dict[str, str] = {}
    files_processed = 0

    for fp, sec_type in diff_files:
        try:
            recs = extract_records_from_diff(fp, sec_type)
        except Exception:
            logger.exception("Failed to parse diff %s", fp)
            continue
        for rec in recs:
            key = (
                rec["section_type"],
                rec["record_date"],
                rec["license_number"],
                rec["application_type"],
            )
            all_records.setdefault(key, rec)
            ts = rec.get("scraped_at", "")
            if ts and ts not in ts_to_diff_path:
                ts_to_diff_path[ts] = str(fp.relative_to(data_dir))
        files_processed += 1
        if files_processed % 200 == 0:
            logger.debug(
                "  parsed %d / %d diff files (%d unique records)",
                files_processed, len(diff_files), len(all_records),
            )

    records = sorted(
        all_records.values(),
        key=lambda r: (r["record_date"], r["section_type"]),
    )
    logger.info(
        "Diff extraction: %d unique records from %d files",
        len(records), files_processed,
    )

    # Insert via pipeline
    source_cache: dict[str, int] = {}
    inserted = 0
    COMMIT_BATCH = 500

    for i, rec in enumerate(records):
        # Resolve provenance source
        scraped_at = rec.get("scraped_at", "")
        diff_path = ts_to_diff_path.get(scraped_at)
        source_id = None
        if diff_path:
            if diff_path not in source_cache:
                source_cache[diff_path] = get_or_create_source(
                    conn, source_type,
                    snapshot_path=diff_path,
                    url=source_url,
                    captured_at=scraped_at,
                )
            source_id = source_cache[diff_path]

        opts = IngestOptions(
            validate_addresses=False,
            link_outcomes=False,
            source_id=source_id,
        )
        ir = ingest_record(conn, rec, opts)
        if ir is not None and ir.is_new:
            inserted += 1

        if (i + 1) % COMMIT_BATCH == 0:
            conn.commit()
            if (i + 1) % 5000 == 0:
                logger.debug(
                    "  diff progress: %d / %d (inserted=%d)",
                    i + 1, len(records), inserted,
                )

    conn.commit()
    return inserted


def _ingest_snapshots(
    conn, snapshots, data_dir,
    get_or_create_source, source_type, source_url,
    extract_snapshot_date, parse_snapshot,
    ingest_batch, IngestOptions,
) -> int:
    """Ingest records from HTML snapshots.  Returns count of new records."""
    total_inserted = 0

    for snap_path in snapshots:
        snap_date = extract_snapshot_date(snap_path)
        try:
            records = parse_snapshot(snap_path)
        except Exception:
            logger.exception("Failed to parse snapshot %s", snap_path.name)
            continue

        rel_path = str(snap_path.relative_to(data_dir))
        source_id = get_or_create_source(
            conn, source_type,
            snapshot_path=rel_path,
            url=source_url,
            captured_at=(
                snap_date.replace("_", "-") + "T00:00:00+00:00"
                if snap_date else None
            ),
        )

        opts = IngestOptions(
            validate_addresses=False,
            link_outcomes=False,
            source_id=source_id,
        )
        batch_result = ingest_batch(conn, records, opts)
        total_inserted += batch_result.inserted
        logger.debug(
            "  %s: +%d new, %d skipped",
            snap_date, batch_result.inserted, batch_result.skipped,
        )

    return total_inserted


def compare_databases(
    prod_path: Path,
    rebuilt_path: Path,
) -> ComparisonResult:
    """Compare production and rebuilt databases by record natural keys.

    Natural key: ``(section_type, record_date, license_number, application_type)``.

    Args:
        prod_path: Path to the production database.
        rebuilt_path: Path to the rebuilt database.

    Returns:
        :class:`ComparisonResult` with counts and sample discrepancies.
    """
    result = ComparisonResult()

    _KEY_QUERY = (
        "SELECT section_type, record_date, license_number, application_type "
        "FROM license_records"
    )
    _SECTION_QUERY = (
        "SELECT section_type, COUNT(*) as cnt "
        "FROM license_records GROUP BY section_type"
    )

    prod_conn = sqlite3.connect(str(prod_path))
    prod_conn.row_factory = sqlite3.Row
    rebuilt_conn = sqlite3.connect(str(rebuilt_path))
    rebuilt_conn.row_factory = sqlite3.Row

    try:
        # Total counts
        result.prod_count = prod_conn.execute(
            "SELECT COUNT(*) FROM license_records"
        ).fetchone()[0]
        result.rebuilt_count = rebuilt_conn.execute(
            "SELECT COUNT(*) FROM license_records"
        ).fetchone()[0]

        # Per-section counts
        prod_sections = {
            r["section_type"]: r["cnt"]
            for r in prod_conn.execute(_SECTION_QUERY).fetchall()
        }
        rebuilt_sections = {
            r["section_type"]: r["cnt"]
            for r in rebuilt_conn.execute(_SECTION_QUERY).fetchall()
        }
        all_sections = set(prod_sections) | set(rebuilt_sections)
        for sec in sorted(all_sections):
            result.section_counts[sec] = {
                "prod": prod_sections.get(sec, 0),
                "rebuilt": rebuilt_sections.get(sec, 0),
            }

        # Key-level diff
        prod_keys = set()
        for r in prod_conn.execute(_KEY_QUERY).fetchall():
            prod_keys.add((
                r["section_type"], r["record_date"],
                r["license_number"], r["application_type"],
            ))

        rebuilt_keys = set()
        for r in rebuilt_conn.execute(_KEY_QUERY).fetchall():
            rebuilt_keys.add((
                r["section_type"], r["record_date"],
                r["license_number"], r["application_type"],
            ))

        missing = prod_keys - rebuilt_keys
        extra = rebuilt_keys - prod_keys
        result.missing_from_rebuilt = len(missing)
        result.extra_in_rebuilt = len(extra)

        # Sample up to 10 of each for reporting
        SAMPLE = 10
        result.sample_missing = sorted(missing)[:SAMPLE]
        result.sample_extra = sorted(extra)[:SAMPLE]

    finally:
        prod_conn.close()
        rebuilt_conn.close()

    return result
