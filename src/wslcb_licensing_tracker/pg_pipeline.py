"""Async PostgreSQL ingestion pipeline for WSLCB licensing tracker.

Async equivalent of pipeline.py. Uses SQLAlchemy Core expressions and
the table objects from models.py.

Entity linking is STUBBED — Phase 3 ports entities.py and endorsements.py.
Address validation and outcome linking are also STUBBED for Phase 3.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from .db import clean_applicants_string, clean_entity_name
from .entities import ADDITIONAL_NAMES_MARKERS
from .models import license_records, record_enrichments
from .pg_db import get_or_create_location, link_record_source

logger = logging.getLogger(__name__)


@dataclass
class IngestOptions:
    """Configuration for the ingestion pipeline."""

    validate_addresses: bool = True
    link_outcomes: bool = True
    source_id: int | None = None
    source_role: str = "first_seen"
    batch_size: int = 200


@dataclass
class IngestResult:
    """Result of ingesting a single record."""

    record_id: int | None = None
    is_new: bool = False


@dataclass
class BatchResult:
    """Aggregate result of a batch ingestion."""

    inserted: int = 0
    skipped: int = 0
    errors: int = 0
    record_ids: list[int] = field(default_factory=list)


# ------------------------------------------------------------------
# Enrichment step names (canonical values for record_enrichments.step)
# ------------------------------------------------------------------

STEP_ENDORSEMENTS = "endorsements"
STEP_ENTITIES = "entities"
STEP_ADDRESS = "address"
STEP_OUTCOME_LINK = "outcome_link"


def _applicants_have_additional_names(*applicant_strings: str | None) -> bool:
    """Return True if any applicant string contains an ADDITIONAL NAMES ON FILE marker."""
    for s in applicant_strings:
        if not s:
            continue
        if any(part.strip() in ADDITIONAL_NAMES_MARKERS for part in s.split(";")):
            return True
    return False


async def _record_enrichment(
    conn: AsyncConnection,
    record_id: int,
    step: str,
    version: str = "1",
) -> None:
    """Record that an enrichment step completed for a record.

    Uses ON CONFLICT DO UPDATE so re-running a step updates the timestamp.
    """
    now = datetime.now(UTC).isoformat()
    stmt = (
        pg_insert(record_enrichments)
        .values(record_id=record_id, step=step, completed_at=now, version=version)
        .on_conflict_do_update(
            index_elements=["record_id", "step"],
            set_={"completed_at": now, "version": version},
        )
    )
    await conn.execute(stmt)


async def insert_record(
    conn: AsyncConnection,
    record: dict,
) -> tuple[int, bool] | None:
    """Insert a record, returning (id, is_new) or None on error.

    Returns (new_id, True) for freshly inserted records and
    (existing_id, False) when a duplicate is detected.

    Entity linking is STUBBED — Phase 3 adds parse_and_link_entities.
    """
    # Check for existing (dedup by natural key)
    existing = await conn.execute(
        select(license_records.c.id)
        .where(
            license_records.c.section_type == record["section_type"],
            license_records.c.record_date == record["record_date"],
            license_records.c.license_number == record["license_number"],
            license_records.c.application_type == record["application_type"],
        )
        .limit(1)
    )
    row = existing.first()
    if row:
        return (row[0], False)

    # Resolve locations
    location_id = await get_or_create_location(
        conn,
        record.get("business_location", ""),
        city=record.get("city", ""),
        state=record.get("state", "WA"),
        zip_code=record.get("zip_code", ""),
    )
    previous_location_id = await get_or_create_location(
        conn,
        record.get("previous_business_location", ""),
        city=record.get("previous_city", ""),
        state=record.get("previous_state", ""),
        zip_code=record.get("previous_zip_code", ""),
    )

    # Clean names
    cleaned_biz = clean_entity_name(record.get("business_name", ""))
    cleaned_prev_biz = clean_entity_name(record.get("previous_business_name", ""))
    cleaned_applicants = clean_applicants_string(record.get("applicants", ""))
    cleaned_prev_applicants = clean_applicants_string(record.get("previous_applicants", ""))

    # Preserve raw values
    raw_biz = record.get("business_name", "")
    raw_prev_biz = record.get("previous_business_name", "")
    raw_applicants = record.get("applicants", "")
    raw_prev_applicants = record.get("previous_applicants", "")

    has_additional_names = int(
        _applicants_have_additional_names(cleaned_applicants, cleaned_prev_applicants)
    )

    try:
        stmt = (
            insert(license_records)
            .values(
                section_type=record["section_type"],
                record_date=record["record_date"],
                business_name=cleaned_biz,
                location_id=location_id,
                applicants=cleaned_applicants,
                license_type=record.get("license_type", ""),
                application_type=record["application_type"],
                license_number=record.get("license_number", ""),
                contact_phone=record.get("contact_phone", ""),
                previous_business_name=cleaned_prev_biz,
                previous_applicants=cleaned_prev_applicants,
                previous_location_id=previous_location_id,
                raw_business_name=raw_biz,
                raw_previous_business_name=raw_prev_biz,
                raw_applicants=raw_applicants,
                raw_previous_applicants=raw_prev_applicants,
                has_additional_names=has_additional_names,
                scraped_at=record["scraped_at"],
            )
            .returning(license_records.c.id)
        )
        result = await conn.execute(stmt)
        record_id = result.scalar_one()
        # Phase 3: parse_and_link_entities for applicants goes here

    except IntegrityError:
        return None
    else:
        return (record_id, True)


async def ingest_record(
    conn: AsyncConnection,
    record: dict,
    options: IngestOptions,
) -> IngestResult | None:
    """Insert a raw record and run all enrichment steps.

    Returns an IngestResult on success (both new and duplicate),
    or None on unexpected error.

    Steps 2-5 only run for newly inserted records; duplicates get
    provenance linked with role 'confirmed' and skip other steps.
    """
    # Step 1: Insert record (dedup, locations, name cleaning)
    try:
        result = await insert_record(conn, record)
    except Exception:
        logger.exception(
            "Error inserting record: %s/%s/#%s",
            record.get("section_type"),
            record.get("record_date"),
            record.get("license_number"),
        )
        return None

    if result is None:
        return None

    record_id, is_new = result

    if is_new:
        # Endorsements and entity linking are STUBBED (Phase 3 implementation)
        # Entity enrichment is tracked now so the step appears as completed
        await _record_enrichment(conn, record_id, STEP_ENTITIES)

        # Step 3: Link provenance (first_seen)
        if options.source_id is not None:
            try:
                await link_record_source(
                    conn,
                    record_id,
                    options.source_id,
                    options.source_role,
                )
            except Exception:
                logger.exception(
                    "Error linking provenance for record %d",
                    record_id,
                )

        # Address validation and outcome linking are STUBBED (Phase 3 implementation)

    # Duplicate — link provenance as 'confirmed'
    elif options.source_id is not None:
        try:
            await link_record_source(
                conn,
                record_id,
                options.source_id,
                "confirmed",
            )
        except Exception:
            logger.exception(
                "Error linking confirmed provenance for record %d",
                record_id,
            )

    return IngestResult(record_id=record_id, is_new=is_new)


async def ingest_batch(
    conn: AsyncConnection,
    records: list[dict],
    options: IngestOptions,
) -> BatchResult:
    """Ingest multiple records with progress logging and batch commits.

    Commits every options.batch_size records to allow recovery from
    interruption.
    """
    result = BatchResult()

    for i, rec in enumerate(records):
        ir = await ingest_record(conn, rec, options)
        if ir is None:
            result.errors += 1
        elif ir.is_new:
            result.inserted += 1
            result.record_ids.append(ir.record_id)
        else:
            result.skipped += 1

        if (i + 1) % options.batch_size == 0:
            await conn.commit()
            logger.debug(
                "  progress: %d / %d (inserted=%d, skipped=%d, errors=%d)",
                i + 1,
                len(records),
                result.inserted,
                result.skipped,
                result.errors,
            )

    await conn.commit()
    return result
