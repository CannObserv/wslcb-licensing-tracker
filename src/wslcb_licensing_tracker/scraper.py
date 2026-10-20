"""Async scraper for WSLCB licensing activity page.

Async PostgreSQL port of scraper.py.

Also the scrape's side of its co-status check-in (#196): ``scrape()`` returns a
``ScrapeOutcome``, ``scrape_health()`` reads the run history from
``scrape_log``, and ``ok_variables()`` / ``alert_variables()`` / ``fault_key()``
shape what the monitor's templates see. The CLI sends it. ``scrape_freshness()``
reports the same history to ``/api/v1/health`` (#197).
"""

import gzip
import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, TypedDict

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import delete, exists, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .db import DATA_DIR, SOURCE_TYPE_LIVE_SCRAPE, WSLCB_SOURCE_URL, get_or_create_source
from .engine import get_db
from .models import scrape_log, sources
from .parser import SECTION_MAP, parse_records_from_table
from .pipeline import IngestOptions, ingest_batch

logger = logging.getLogger(__name__)

#: The source page shows the last 30 days. Past this long after the last
#: successful fetch, records posted early in a gap fall off it unrecorded.
SOURCE_WINDOW = timedelta(days=30)
#: A fetched page counts, changed or not.
_SUCCESS_STATUSES = ("success", "unchanged")
#: Past this long after the last successful fetch, /api/v1/health reports the
#: data stale (#197). It is the scrape monitor's interval plus grace
#: (docs/MONITORING.md), so the endpoint flips when the dead-man alert fires.
STALE_AFTER = timedelta(hours=26)


@dataclass(frozen=True)
class ScrapeOutcome:
    """What one completed scrape did, as recorded in its ``scrape_log`` row."""

    scrape_log_id: int
    status: Literal["success", "unchanged"]
    new: int = 0
    approved: int = 0
    discontinued: int = 0
    skipped: int = 0


@dataclass(frozen=True)
class ScrapeHealth:
    """The run history a failure alert reports (#196) and /api/v1/health reads (#197).

    ``wslcb db cleanup-redundant`` keeps the ``unchanged`` rows since the newest
    ``success``, so both fields survive it exactly.
    """

    #: When the newest ``success``/``unchanged`` run finished; None if never.
    last_success_at: datetime | None
    #: ``error`` runs since then. ``running`` rows (a killed run) don't count.
    consecutive_errors: int


class ScrapeFreshness(TypedDict):
    """The ``scrape`` object /api/v1/health reports (#197); a public JSON contract."""

    #: As ``ScrapeHealth.last_success_at``.
    last_success_at: datetime | None
    #: The newest run's status, ``running`` included; None with no runs.
    last_attempt_status: str | None
    #: As ``ScrapeHealth.consecutive_errors``.
    consecutive_errors: int
    #: Past ``STALE_AFTER`` since ``last_success_at``, or never succeeded.
    stale: bool
    stale_after_hours: int


# ---------------------------------------------------------------------------
# Pure helpers (no DB dependency)
# ---------------------------------------------------------------------------


def compute_content_hash(html: str) -> str:
    """Return the SHA-256 hex digest of *html* (UTF-8 encoded)."""
    return hashlib.sha256(html.encode("utf-8")).hexdigest()


def save_html_snapshot(html: str, scrape_date: datetime) -> Path:
    """Save raw HTML to the per-date snapshot directory as gzip-compressed.

    Path pattern:
    ``data/wslcb/licensinginfo/[yyyy]/[yyyy_mm_dd]/[yyyy_mm_dd]-licensinginfo.lcb.wa.gov-v[x].html.gz``

    Saves the HTML exactly as received from the server (no transformation).
    Increments the version number if a snapshot for the same date already exists.
    Returns the path to the saved file.
    """
    date_str = scrape_date.strftime("%Y_%m_%d")
    year_str = scrape_date.strftime("%Y")
    date_dir = DATA_DIR / "wslcb" / "licensinginfo" / year_str / date_str

    version = 1
    while list(date_dir.glob(f"{date_str}-licensinginfo.lcb.wa.gov-v{version}.html.gz")):
        version += 1

    date_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{date_str}-licensinginfo.lcb.wa.gov-v{version}.html.gz"
    filepath = date_dir / filename
    with gzip.open(filepath, "wt", encoding="utf-8") as fh:
        fh.write(html)
    return filepath


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


#: When a successful run's page was fetched: a relay's capture time (#201),
#: else the run's own finish.
_FETCHED_AT = func.coalesce(
    scrape_log.c.captured_at, scrape_log.c.finished_at, scrape_log.c.started_at
)


async def scrape_health(conn: AsyncConnection) -> ScrapeHealth:
    """Freshest fetched page, and the server's errors since its own last success.

    Either path's success counts as fresh data, dated by when the page was
    fetched. The error count reads server runs only: it reports whether the
    server can reach WSLCB, which a relay (#201) says nothing about.
    """
    last_success_at = (
        await conn.execute(
            select(func.max(_FETCHED_AT)).where(scrape_log.c.status.in_(_SUCCESS_STATUSES))
        )
    ).scalar_one()
    server = scrape_log.c.origin == "server"
    last_server_success = (
        select(func.max(scrape_log.c.id))
        .where(scrape_log.c.status.in_(_SUCCESS_STATUSES), server)
        .scalar_subquery()
    )
    errors = (
        select(func.count())
        .select_from(scrape_log)
        .where(
            scrape_log.c.status == "error",
            server,
            scrape_log.c.id > func.coalesce(last_server_success, 0),
        )
    )
    count = (await conn.execute(errors)).scalar_one()
    return ScrapeHealth(last_success_at, count)


async def scrape_freshness(conn: AsyncConnection, now: datetime | None = None) -> ScrapeFreshness:
    """Read ``scrape_log`` into a ``ScrapeFreshness``; *now* defaults to UTC now.

    ``last_attempt_status`` is the server's latest run, like the error count.
    """
    health = await scrape_health(conn)
    last_status = (
        await conn.execute(
            select(scrape_log.c.status)
            .where(scrape_log.c.origin == "server")
            .order_by(scrape_log.c.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    now = now or datetime.now(UTC)
    last = health.last_success_at
    return ScrapeFreshness(
        last_success_at=last,
        last_attempt_status=last_status,
        consecutive_errors=health.consecutive_errors,
        stale=last is None or now - last > STALE_AFTER,
        stale_after_hours=STALE_AFTER // timedelta(hours=1),
    )


def fault_key(exc: BaseException) -> str:
    """The stable part of a failure, for the check-in's ``metadata.fault``.

    co-status opens a new fault, and reports at once, when this changes: a 403
    becoming a 503 or a DNS failure is news; the next 403 is not.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{type(exc).__name__} {exc.response.status_code}"
    return type(exc).__name__


def ok_variables(outcome: ScrapeOutcome, host: str) -> dict:
    """The flat variables an ``ok`` check-in carries."""
    return {
        "outcome": "ok",
        "source_host": host,
        "scrape_status": outcome.status,
        "records_new": outcome.new,
        "records_approved": outcome.approved,
        "records_discontinued": outcome.discontinued,
        "records_skipped": outcome.skipped,
        "scrape_log_id": outcome.scrape_log_id,
    }


def alert_variables(exc: BaseException, health: ScrapeHealth | None, host: str) -> dict:
    """The flat variables an ``alert`` check-in carries; every key always present.

    *health* is None when the history couldn't be read (the database being
    what failed): its three keys are then empty, and the alert still goes.
    ``error`` is the exception's first line — httpx appends an MDN link — or
    just its type when it has none, or can't be printed. Nothing here may
    raise: the CLI builds this inside its ``except``, where a new exception
    would replace the scrape's and drop the alert.
    """
    try:
        text = str(exc)
    except Exception:  # noqa: BLE001 — an alert without its message beats none
        text = ""
    message = text.splitlines()[0] if text else ""
    last = health.last_success_at if health else None
    return {
        "outcome": "failed",
        "source_host": host,
        "error": f"{type(exc).__name__}: {message}" if message else type(exc).__name__,
        "consecutive_errors": health.consecutive_errors if health else "",
        "last_success_at": last.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if last else "",
        "window_expires": (last + SOURCE_WINDOW).astimezone(UTC).date().isoformat() if last else "",
    }


async def start_log(
    conn: AsyncConnection,
    *,
    origin: Literal["server", "relay"] = "server",
    captured_at: datetime | None = None,
) -> int:
    """Open a ``running`` ``scrape_log`` row and commit it; return its id."""
    log_id = (
        await conn.execute(
            pg_insert(scrape_log)
            .values(
                started_at=datetime.now(UTC),
                status="running",
                origin=origin,
                captured_at=captured_at,
            )
            .returning(scrape_log.c.id)
        )
    ).scalar_one()
    await conn.commit()
    return log_id


async def fail_log(conn: AsyncConnection, log_id: int, exc: BaseException) -> None:
    """Mark a run ``error`` with the exception's text; never raises."""
    try:
        await conn.execute(
            update(scrape_log)
            .where(scrape_log.c.id == log_id)
            .values(
                finished_at=datetime.now(UTC),
                status="error",
                error_message=str(exc),
            )
        )
        await conn.commit()
    except Exception:
        logger.exception("Failed to update scrape_log on error")


async def ingest_page(  # noqa: PLR0913
    conn: AsyncConnection,
    html: str,
    log_id: int,
    *,
    source_type: int,
    captured_at: datetime,
    metadata: dict | None = None,
) -> ScrapeOutcome:
    """Archive, parse, and ingest one fetched copy of the page; finish its log row.

    Shared by the live scrape and the relay import (#201), so the two paths
    cannot drift. *captured_at* is when the page was fetched, which names the
    snapshot and dates the source. A page whose hash matches the last
    successful run of either path is logged ``unchanged`` and not ingested.
    Raises on a page with no data tables; the caller marks the row failed.
    """
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
                finished_at=datetime.now(UTC),
                status="unchanged",
                content_hash=content_hash,
            )
        )
        await conn.commit()
        return ScrapeOutcome(log_id, "unchanged")

    snapshot_path = None
    try:
        snapshot_path = save_html_snapshot(html, captured_at)
        logger.debug("Saved snapshot to %s", snapshot_path)
    except Exception as snap_err:  # noqa: BLE001
        logger.warning("Failed to save HTML snapshot: %s", snap_err)

    rel_path = str(snapshot_path.relative_to(DATA_DIR)) if snapshot_path else None
    source_id = await get_or_create_source(
        conn,
        source_type,
        snapshot_path=rel_path,
        url=WSLCB_SOURCE_URL,
        captured_at=captured_at,
        scrape_log_id=log_id,
        metadata=metadata,
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
        raise ValueError(msg)

    counts = {"new": 0, "approved": 0, "discontinued": 0, "skipped": 0}

    for section_type, table in data_tables:
        records = parse_records_from_table(table, section_type)
        logger.debug("  %s: parsed %d records", section_type, len(records))
        opts = IngestOptions(
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
            finished_at=datetime.now(UTC),
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
    return ScrapeOutcome(log_id, "success", **counts)


async def scrape(engine: AsyncEngine) -> ScrapeOutcome:
    """Run a full scrape: fetch, archive, parse, ingest, and log."""
    logger.info("Starting scrape of %s", WSLCB_SOURCE_URL)

    async with get_db(engine) as conn:
        log_id = await start_log(conn)
        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                resp = await client.get(WSLCB_SOURCE_URL)
            resp.raise_for_status()
            html = resp.text
            logger.debug("Fetched %d bytes", len(html))
            return await ingest_page(
                conn,
                html,
                log_id,
                source_type=SOURCE_TYPE_LIVE_SCRAPE,
                captured_at=datetime.now(UTC),
            )
        except Exception as exc:
            logger.exception("Scrape failed")
            await fail_log(conn, log_id, exc)
            raise


async def cleanup_redundant_scrapes(
    engine: AsyncEngine,
    *,
    delete_files: bool = True,
) -> dict[str, int]:
    """Remove scrape_log rows (and associated sources/files) for unchanged scrapes.

    Keeps the ``unchanged`` rows since the newest ``success``: they are the
    current state, which ``scrape_health()`` and /api/v1/health read (#197).
    A later cleanup removes them once a ``success`` supersedes them.

    Returns a dict with counts of removed rows: ``scrape_logs``, ``sources``,
    ``record_sources``, ``files``.
    """
    result = {"scrape_logs": 0, "sources": 0, "record_sources": 0, "files": 0}

    async with get_db(engine) as conn:
        newest_success = (
            select(func.max(scrape_log.c.id))
            .where(scrape_log.c.status == "success")
            .scalar_subquery()
        )
        # Find superseded scrape_log rows with status='unchanged' that have no sources linked
        unchanged = await conn.execute(
            select(scrape_log.c.id, scrape_log.c.snapshot_path)
            .where(scrape_log.c.status == "unchanged")
            .where(scrape_log.c.id < newest_success)
            .where(~exists(select(sources.c.id).where(sources.c.scrape_log_id == scrape_log.c.id)))
        )
        rows = unchanged.mappings().all()

        for row in rows:
            if delete_files and row["snapshot_path"]:
                snap = DATA_DIR / row["snapshot_path"]
                # Handle both .html (legacy) and .html.gz (current) on disk
                if not snap.exists():
                    snap = snap.parent / (snap.name + ".gz")
                if snap.exists():
                    snap.unlink()
                    result["files"] += 1

        if rows:
            ids = [r["id"] for r in rows]
            await conn.execute(delete(scrape_log).where(scrape_log.c.id.in_(ids)))
            await conn.commit()
            result["scrape_logs"] = len(ids)

    return result
