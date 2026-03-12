"""Unified ingestion pipeline for WSLCB licensing tracker.

Exports ``insert_record()`` (the canonical record-insertion function)
and ``ingest_record()`` / ``ingest_batch()`` (the full enrichment
pipeline called by all ingestion paths: live scrape, snapshot backfill,
diff backfill).

``insert_record()`` handles dedup, location resolution, name cleaning,
and entity linking.  ``ingest_record()`` wraps it with the enrichment
steps below, each idempotent and individually toggleable via
``IngestOptions``.

Steps (in order):
  1. Insert raw record via ``insert_record()`` (dedup, locations,
     name cleaning, entity linking)
  2. Process endorsements (resolve codes -> names)
  3. Link provenance (if source_id provided)
  4. Validate addresses (if enabled and API available)
  5. Link outcomes (if enabled)

Failures in steps 3-5 are logged but do not abort the pipeline.
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .address_validator import validate_previous_location, validate_record
from .db import get_or_create_location, link_record_source
from .endorsements import process_record
from .entities import (
    ADDITIONAL_NAMES_MARKERS,
    clean_applicants_string,
    clean_entity_name,
    parse_and_link_entities,
)
from .link_records import link_new_record

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger(__name__)


@dataclass
class IngestOptions:
    """Configuration for the ingestion pipeline."""

    validate_addresses: bool = True
    link_outcomes: bool = True
    source_id: int | None = None
    source_role: str = "first_seen"
    batch_size: int = 200
    av_client: "httpx.Client | None" = None


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


def _record_enrichment(
    conn: sqlite3.Connection,
    record_id: int,
    step: str,
    version: str = "1",
) -> None:
    """Record that an enrichment step completed for a record.

    Uses INSERT OR REPLACE so re-running a step updates the timestamp.
    """
    conn.execute(
        "INSERT OR REPLACE INTO record_enrichments "
        "(record_id, step, completed_at, version) VALUES (?, ?, ?, ?)",
        (record_id, step, datetime.now(UTC).isoformat(), version),
    )


def _applicants_have_additional_names(*applicant_strings: str | None) -> bool:
    """Return True if any applicant string contains an ADDITIONAL NAMES ON FILE marker.

    Checks exact and typo variant tokens.
    """
    for s in applicant_strings:
        if not s:
            continue
        if any(part.strip() in ADDITIONAL_NAMES_MARKERS for part in s.split(";")):
            return True
    return False


def insert_record(
    conn: sqlite3.Connection,
    record: dict,
) -> tuple[int, bool] | None:
    """Insert a record, returning ``(id, is_new)`` or *None* on error.

    Returns ``(new_id, True)`` for freshly inserted records and
    ``(existing_id, False)`` when a duplicate is detected.  *None* is
    only returned on an unexpected ``IntegrityError`` (safety net).

    Normalizes ``business_name``, ``previous_business_name``,
    ``applicants``, and ``previous_applicants`` (uppercase, strip
    trailing punctuation) before storage.  Automatically resolves (or
    creates) location rows and links entity records.  Checks for
    duplicates *before* creating locations to avoid orphaned rows.
    """
    existing = conn.execute(
        """SELECT id FROM license_records
           WHERE section_type = :section_type
             AND record_date = :record_date
             AND license_number = :license_number
             AND application_type = :application_type
           LIMIT 1""",
        record,
    ).fetchone()
    if existing:
        return (existing["id"], False)

    location_id = get_or_create_location(
        conn,
        record.get("business_location", ""),
        city=record.get("city", ""),
        state=record.get("state", "WA"),
        zip_code=record.get("zip_code", ""),
    )
    previous_location_id = get_or_create_location(
        conn,
        record.get("previous_business_location", ""),
        city=record.get("previous_city", ""),
        state=record.get("previous_state", ""),
        zip_code=record.get("previous_zip_code", ""),
    )
    # Normalize business names and applicant strings (uppercase, strip
    # trailing punctuation) so stored values are consistent throughout.
    cleaned_biz = clean_entity_name(record.get("business_name", ""))
    cleaned_prev_biz = clean_entity_name(record.get("previous_business_name", ""))
    cleaned_applicants = clean_applicants_string(record.get("applicants", ""))
    cleaned_prev_applicants = clean_applicants_string(record.get("previous_applicants", ""))
    # Preserve raw (as-parsed) values before cleaning
    raw_biz = record.get("business_name", "")
    raw_prev_biz = record.get("previous_business_name", "")
    raw_applicants = record.get("applicants", "")
    raw_prev_applicants = record.get("previous_applicants", "")
    has_additional_names = int(
        _applicants_have_additional_names(cleaned_applicants, cleaned_prev_applicants)
    )
    try:
        cursor = conn.execute(
            """INSERT INTO license_records
               (section_type, record_date, business_name, location_id,
                applicants, license_type, application_type, license_number,
                contact_phone, previous_business_name, previous_applicants,
                previous_location_id,
                raw_business_name, raw_previous_business_name,
                raw_applicants, raw_previous_applicants,
                has_additional_names,
                scraped_at)
               VALUES (:section_type, :record_date, :business_name, :location_id,
                       :applicants, :license_type, :application_type, :license_number,
                       :contact_phone, :previous_business_name, :previous_applicants,
                       :previous_location_id,
                       :raw_business_name, :raw_previous_business_name,
                       :raw_applicants, :raw_previous_applicants,
                       :has_additional_names,
                       :scraped_at)""",
            {
                **record,
                "location_id": location_id,
                "previous_location_id": previous_location_id,
                "business_name": cleaned_biz,
                "previous_business_name": cleaned_prev_biz,
                "applicants": cleaned_applicants,
                "previous_applicants": cleaned_prev_applicants,
                "raw_business_name": raw_biz,
                "raw_previous_business_name": raw_prev_biz,
                "raw_applicants": raw_applicants,
                "raw_previous_applicants": raw_prev_applicants,
                "has_additional_names": has_additional_names,
            },
        )
        record_id = cursor.lastrowid
        parse_and_link_entities(conn, record_id, cleaned_applicants, "applicant")
        if cleaned_prev_applicants:
            parse_and_link_entities(conn, record_id, cleaned_prev_applicants, "previous_applicant")
    except sqlite3.IntegrityError:
        return None
    else:
        return (record_id, True)


def ingest_record(  # noqa: C901, PLR0912
    conn: sqlite3.Connection,
    record: dict,
    options: IngestOptions,
) -> IngestResult | None:
    """Insert a raw record and run all enrichment steps.

    Returns an ``IngestResult`` on success (both new and duplicate),
    or ``None`` on unexpected error.

    Steps 2-5 only run for newly inserted records; duplicates get
    provenance linked with role ``'confirmed'`` and skip other steps.
    """
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
            _record_enrichment(conn, record_id, STEP_ENDORSEMENTS)
        except Exception:
            logger.exception(
                "Error processing endorsements for record %d",
                record_id,
            )

        # Entity linking is performed by insert_record (step 1), so
        # we track it here unconditionally for new records.
        _record_enrichment(conn, record_id, STEP_ENTITIES)

        # Step 3: Link provenance (first_seen)
        if options.source_id is not None:
            try:
                link_record_source(
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

        # Step 4: Validate addresses
        if options.validate_addresses:
            try:
                validate_record(conn, record_id, client=options.av_client)
                if record.get("previous_business_location"):
                    validate_previous_location(
                        conn,
                        record_id,
                        client=options.av_client,
                    )
                _record_enrichment(conn, record_id, STEP_ADDRESS)
            except Exception:
                logger.exception(
                    "Error validating address for record %d",
                    record_id,
                )

        # Step 5: Link outcomes
        if options.link_outcomes:
            try:
                link_new_record(conn, record_id)
                _record_enrichment(conn, record_id, STEP_OUTCOME_LINK)
            except Exception:
                logger.exception(
                    "Error linking outcomes for record %d",
                    record_id,
                )
    # Duplicate — link provenance as 'confirmed'
    elif options.source_id is not None:
        try:
            link_record_source(
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
                i + 1,
                len(records),
                result.inserted,
                result.skipped,
                result.errors,
            )

    conn.commit()
    return result
