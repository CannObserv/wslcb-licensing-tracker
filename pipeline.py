"""Unified ingestion pipeline for WSLCB licensing tracker.

Provides a single ``ingest_record()`` function that all ingestion paths
(live scrape, snapshot backfill, diff backfill) call.  Each enrichment
step is idempotent and individually toggleable via ``IngestOptions``.

Steps (in order):
  1. Insert raw record (dedup, create locations, clean names, link entities)
  2. Process endorsements (resolve codes → names)
  3. Link provenance (if source_id provided)
  4. Validate addresses (if enabled and API available)
  5. Link outcomes (if enabled)

Entity linking is always performed by ``insert_record()`` as part of
step 1.  Failures in steps 3–5 are logged but do not abort the pipeline.
"""
import logging
import sqlite3
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class IngestOptions:
    """Configuration for the ingestion pipeline."""
    validate_addresses: bool = True
    link_outcomes: bool = True
    source_id: int | None = None
    source_role: str = "first_seen"
    batch_size: int = 200
    av_client: object | None = None  # httpx.Client for address validation


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


def ingest_record(
    conn: sqlite3.Connection,
    record: dict,
    options: IngestOptions,
) -> IngestResult | None:
    """Insert a raw record and run all enrichment steps.

    Returns an ``IngestResult`` on success (both new and duplicate),
    or ``None`` on unexpected error.

    Steps 3–6 only run for newly inserted records; duplicates get
    provenance linked with role ``'confirmed'`` and skip other steps.
    """
    from queries import insert_record
    from endorsements import process_record
    from database import link_record_source

    # Step 1: Insert record (dedup, locations, name cleaning)
    try:
        result = insert_record(conn, record)
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
        # Step 2: Process endorsements
        try:
            process_record(conn, record_id, record.get("license_type", ""))
        except Exception:
            logger.exception(
                "Error processing endorsements for record %d", record_id,
            )

        # Step 3: Link provenance (first_seen)
        if options.source_id is not None:
            try:
                link_record_source(
                    conn, record_id, options.source_id, options.source_role,
                )
            except Exception:
                logger.exception(
                    "Error linking provenance for record %d", record_id,
                )

        # Step 4: Validate addresses
        if options.validate_addresses:
            try:
                from address_validator import validate_record, validate_previous_location
                validate_record(conn, record_id, client=options.av_client)
                if record.get("previous_business_location"):
                    validate_previous_location(
                        conn, record_id, client=options.av_client,
                    )
            except Exception:
                logger.exception(
                    "Error validating address for record %d", record_id,
                )

        # Step 5: Link outcomes
        if options.link_outcomes:
            try:
                from link_records import link_new_record
                link_new_record(conn, record_id)
            except Exception:
                logger.exception(
                    "Error linking outcomes for record %d", record_id,
                )
    else:
        # Duplicate — link provenance as 'confirmed'
        if options.source_id is not None:
            try:
                link_record_source(
                    conn, record_id, options.source_id, "confirmed",
                )
            except Exception:
                logger.exception(
                    "Error linking confirmed provenance for record %d",
                    record_id,
                )

    return IngestResult(record_id=record_id, is_new=is_new)


def ingest_batch(
    conn: sqlite3.Connection,
    records: list[dict],
    options: IngestOptions,
) -> BatchResult:
    """Ingest multiple records with progress logging and batch commits.

    Commits every ``options.batch_size`` records to allow recovery from
    interruption.  Returns aggregate counts and the list of newly
    inserted record IDs.
    """
    result = BatchResult()

    for i, rec in enumerate(records):
        ir = ingest_record(conn, rec, options)
        if ir is None:
            result.errors += 1
        elif ir.is_new:
            result.inserted += 1
            result.record_ids.append(ir.record_id)
        else:
            result.skipped += 1

        if (i + 1) % options.batch_size == 0:
            conn.commit()
            logger.debug(
                "  progress: %d / %d (inserted=%d, skipped=%d, errors=%d)",
                i + 1, len(records),
                result.inserted, result.skipped, result.errors,
            )

    conn.commit()
    return result
