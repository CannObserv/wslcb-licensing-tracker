"""Async backfill of records from archived HTML snapshots into PostgreSQL.

Async port of backfill_snapshots.py. Two-phase:
  1. Ingest — INSERT new records (duplicates skipped by UNIQUE constraint).
  2. Repair — fix broken ASSUMPTION and CHANGE OF LOCATION records.

Safe to re-run. Address validation is deferred to ``wslcb backfill-addresses``.
"""

import logging

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .database import get_db
from .models import license_records
from .parser import extract_snapshot_date, parse_snapshot, snapshot_paths
from .pg_db import (
    DATA_DIR,
    SOURCE_TYPE_CO_ARCHIVE,
    WSLCB_SOURCE_URL,
    get_or_create_location,
    get_or_create_source,
    link_record_source,
)
from .pg_pipeline import IngestOptions, ingest_batch
from .text_utils import clean_applicants_string, clean_entity_name

logger = logging.getLogger(__name__)


async def _repair_assumptions(
    conn: AsyncConnection,
    records: list[dict],
    source_id: int,
) -> int:
    """Fix ASSUMPTION records with empty or NULL business names in PG."""
    updated = 0
    for rec in records:
        if rec["application_type"] != "ASSUMPTION":
            continue
        if not rec.get("business_name"):
            continue
        result = await conn.execute(
            select(license_records.c.id, license_records.c.business_name)
            .where(license_records.c.section_type == rec["section_type"])
            .where(license_records.c.record_date == rec["record_date"])
            .where(license_records.c.license_number == rec["license_number"])
            .where(license_records.c.application_type == "ASSUMPTION")
            .where(
                (license_records.c.business_name.is_(None))
                | (license_records.c.business_name == "")
            )
        )
        row = result.fetchone()
        if not row:
            continue
        await conn.execute(
            update(license_records)
            .where(license_records.c.id == row[0])
            .values(
                business_name=clean_entity_name(rec["business_name"]),
                applicants=clean_applicants_string(rec.get("applicants", "")),
            )
        )
        await link_record_source(conn, row[0], source_id, role="repaired")
        updated += 1
    return updated


async def _repair_change_of_location(
    conn: AsyncConnection,
    records: list[dict],
    source_id: int,
) -> int:
    """Fix CHANGE OF LOCATION records with NULL previous_location_id."""
    updated = 0
    for rec in records:
        if rec["application_type"] != "CHANGE OF LOCATION":
            continue
        if not rec.get("previous_business_location"):
            continue
        result = await conn.execute(
            select(license_records.c.id)
            .where(license_records.c.section_type == rec["section_type"])
            .where(license_records.c.record_date == rec["record_date"])
            .where(license_records.c.license_number == rec["license_number"])
            .where(license_records.c.previous_location_id.is_(None))
        )
        row = result.fetchone()
        if not row:
            continue
        prev_loc_id = await get_or_create_location(
            conn,
            rec["previous_business_location"],
            city=rec.get("previous_city", ""),
            state=rec.get("previous_state", "WA"),
            zip_code=rec.get("previous_zip_code", ""),
        )
        await conn.execute(
            update(license_records)
            .where(license_records.c.id == row[0])
            .values(previous_location_id=prev_loc_id)
        )
        await link_record_source(conn, row[0], source_id, role="repaired")
        updated += 1
    return updated


async def backfill_from_snapshots(engine: AsyncEngine) -> None:
    """Ingest records from all archived HTML snapshots, then repair broken records."""
    paths = list(snapshot_paths(DATA_DIR))
    logger.info("Found %d snapshot(s) to process", len(paths))

    total_inserted = 0
    total_skipped = 0
    total_repaired = 0

    async with get_db(engine) as conn:
        for snap_path in sorted(paths):
            try:
                snap_date = extract_snapshot_date(snap_path)
                records = parse_snapshot(snap_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping %s: %s", snap_path, exc)
                continue

            rel_path = str(snap_path.relative_to(DATA_DIR))
            source_id = await get_or_create_source(
                conn,
                SOURCE_TYPE_CO_ARCHIVE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=snap_date or None,
            )

            opts = IngestOptions(
                link_outcomes=False,
                source_id=source_id,
            )
            batch_result = await ingest_batch(conn, records, opts)
            total_inserted += batch_result.inserted
            total_skipped += batch_result.skipped

            # Phase 2: repair
            repaired = await _repair_assumptions(conn, records, source_id)
            repaired += await _repair_change_of_location(conn, records, source_id)
            total_repaired += repaired

            await conn.commit()
            logger.debug(
                "%s: inserted=%d skipped=%d repaired=%d",
                snap_path.name,
                batch_result.inserted,
                batch_result.skipped,
                repaired,
            )

    logger.info(
        "Backfill complete: inserted=%d skipped=%d repaired=%d",
        total_inserted,
        total_skipped,
        total_repaired,
    )
