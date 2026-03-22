"""Async scraper for WSLCB licensing activity page.

Async PostgreSQL port of scraper.py. Pure helper functions
(compute_content_hash, save_html_snapshot) are re-exported from
scraper.py — do not duplicate them here.
"""

import logging
from datetime import UTC, datetime

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import delete, exists, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .database import get_db
from .db import DATA_DIR, SOURCE_TYPE_LIVE_SCRAPE, WSLCB_SOURCE_URL
from .models import scrape_log, sources
from .parser import SECTION_MAP, parse_records_from_table
from .pg_db import get_or_create_source
from .pg_pipeline import IngestOptions, ingest_batch
from .scraper import compute_content_hash, save_html_snapshot  # pure helpers

logger = logging.getLogger(__name__)


async def get_last_content_hash(conn: AsyncConnection) -> str | None:
    """Return the content_hash from the most recent successful or unchanged scrape."""
    result = await conn.execute(
        select(scrape_log.c.content_hash)
        .where(scrape_log.c.status.in_(["success", "unchanged"]))
        .where(scrape_log.c.content_hash.isnot(None))
        .order_by(scrape_log.c.id.desc())
        .limit(1)
    )
    row = result.fetchone()
    return row[0] if row else None


async def scrape(engine: AsyncEngine) -> None:  # noqa: C901, PLR0915
    """Run a full scrape: fetch, archive, parse, ingest, and log."""
    logger.info("Starting scrape of %s", WSLCB_SOURCE_URL)

    async with get_db(engine) as conn:
        # Log scrape start
        result = await conn.execute(
            pg_insert(scrape_log)
            .values(
                started_at=datetime.now(UTC).isoformat(),
                status="running",
            )
            .returning(scrape_log.c.id)
        )
        log_id = result.scalar_one()
        await conn.commit()

        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                resp = await client.get(WSLCB_SOURCE_URL)
            resp.raise_for_status()
            html = resp.text
            logger.debug("Fetched %d bytes", len(html))

            content_hash = compute_content_hash(html)
            last_hash = await get_last_content_hash(conn)

            if content_hash == last_hash:
                logger.info(
                    "Page unchanged (hash %s...); skipping parse/ingest",
                    content_hash[:12],
                )
                await conn.execute(
                    update(scrape_log)
                    .where(scrape_log.c.id == log_id)
                    .values(
                        finished_at=datetime.now(UTC).isoformat(),
                        status="unchanged",
                        content_hash=content_hash,
                    )
                )
                await conn.commit()
                return

            scrape_time = datetime.now(UTC)
            snapshot_path = None
            try:
                snapshot_path = save_html_snapshot(html, scrape_time)
                logger.debug("Saved snapshot to %s", snapshot_path)
            except Exception as snap_err:  # noqa: BLE001
                logger.warning("Failed to save HTML snapshot: %s", snap_err)

            rel_path = str(snapshot_path.relative_to(DATA_DIR)) if snapshot_path else None
            source_id = await get_or_create_source(
                conn,
                SOURCE_TYPE_LIVE_SCRAPE,
                snapshot_path=rel_path,
                url=WSLCB_SOURCE_URL,
                captured_at=scrape_time.isoformat(),
                scrape_log_id=log_id,
            )

            soup = BeautifulSoup(html, "lxml")
            all_tables = soup.find_all("table")
            data_tables = []
            for t in all_tables:
                th = t.find("th")
                if th and th.get_text(strip=True).replace("\xa0", " ") in SECTION_MAP:
                    header_text = th.get_text(strip=True).replace("\xa0", " ")
                    data_tables.append((SECTION_MAP[header_text], t))

            if not data_tables:
                msg = "Could not find data tables in page"
                raise ValueError(msg)  # noqa: TRY301

            counts = {"new": 0, "approved": 0, "discontinued": 0, "skipped": 0}

            for section_type, table in data_tables:
                records = parse_records_from_table(table, section_type)
                logger.debug("  %s: parsed %d records", section_type, len(records))
                opts = IngestOptions(
                    validate_addresses=True,
                    link_outcomes=True,
                    source_id=source_id,
                )
                batch_result = await ingest_batch(conn, records, opts)

                key = section_type.split("_")[0] if "_" in section_type else section_type
                if key == "new":
                    counts["new"] = batch_result.inserted
                elif key == "approved":
                    counts["approved"] = batch_result.inserted
                elif key == "discontinued":
                    counts["discontinued"] = batch_result.inserted
                counts["skipped"] += batch_result.skipped

            await conn.commit()  # commit records first; log update follows in a second commit

            await conn.execute(
                update(scrape_log)
                .where(scrape_log.c.id == log_id)
                .values(
                    finished_at=datetime.now(UTC).isoformat(),
                    status="success",
                    content_hash=content_hash,
                    records_new=counts["new"],
                    records_approved=counts["approved"],
                    records_discontinued=counts["discontinued"],
                    records_skipped=counts["skipped"],
                )
            )
            await conn.commit()

            logger.info(
                "Scrape complete: new=%d approved=%d discontinued=%d skipped=%d",
                counts["new"],
                counts["approved"],
                counts["discontinued"],
                counts["skipped"],
            )

        except Exception as exc:
            logger.exception("Scrape failed")
            try:
                await conn.execute(
                    update(scrape_log)
                    .where(scrape_log.c.id == log_id)
                    .values(
                        finished_at=datetime.now(UTC).isoformat(),
                        status="error",
                        error_message=str(exc),
                    )
                )
                await conn.commit()
            except Exception:
                logger.exception("Failed to update scrape_log on error")
            raise


async def cleanup_redundant_scrapes(
    engine: AsyncEngine,
    *,
    delete_files: bool = True,
) -> dict[str, int]:
    """Remove scrape_log rows (and associated sources/files) for unchanged scrapes.

    Returns a dict with counts of removed rows: ``scrape_logs``, ``sources``,
    ``record_sources``, ``files``.
    """
    result = {"scrape_logs": 0, "sources": 0, "record_sources": 0, "files": 0}

    async with get_db(engine) as conn:
        # Find scrape_log rows with status='unchanged' that have no sources linked
        unchanged = await conn.execute(
            select(scrape_log.c.id, scrape_log.c.snapshot_path)
            .where(scrape_log.c.status == "unchanged")
            .where(~exists(select(sources.c.id).where(sources.c.scrape_log_id == scrape_log.c.id)))
        )
        rows = unchanged.mappings().all()

        for row in rows:
            if delete_files and row["snapshot_path"]:
                snap = DATA_DIR / row["snapshot_path"]
                if snap.exists():
                    snap.unlink()
                    result["files"] += 1

        if rows:
            ids = [r["id"] for r in rows]
            await conn.execute(delete(scrape_log).where(scrape_log.c.id.in_(ids)))
            await conn.commit()
            result["scrape_logs"] = len(ids)

    return result
