"""Import operator-relayed captures of the WSLCB page (#201).

While WSLCB blocks this host (#198), the operator's laptop fetches the page,
gzips it as ``<YYYYMMDDTHHMMSSZ>.html.gz`` (the UTC fetch time), and drops it
into an inbox here over ``scp``. ``import_inbox()`` drains that inbox oldest
first. Each capture gets its own ``scrape_log`` row (``origin = 'relay'``,
``captured_at`` = the fetch time). The import then checks its age, makes sure
it looks like the real report, and ingests it through the live scrape's own
``ingest_page()`` under the ``relayed_scrape`` source type.

Imported captures leave the inbox: ``ingest_page()`` archives a changed page,
and an unchanged one is already archived. A rejected capture moves to
``rejected/`` for inspection, and its row is marked ``error``. Any other
failure (a DB fault, say) is not the capture's: its row is marked ``error``,
the file stays queued for the next run, its result is ``failed``, and the run
stops there, since the next capture would likely fail the same way. Uploads
still in flight (``*.part``) are left alone, and removed once older than
``MAX_AGE``.

Never checks in to co-status: that monitor reports whether the *server* can
reach WSLCB (docs/MONITORING.md).
"""

import gzip
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from bs4 import BeautifulSoup
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .db import SOURCE_TYPE_RELAYED_SCRAPE
from .engine import get_db
from .parser import SECTION_MAP, parse_records_from_table
from .scraper import fail_log, ingest_page, start_log

logger = logging.getLogger(__name__)

#: Inbox location when ``WSLCB_RELAY_INBOX`` is unset. It sits outside
#: ``data/``, which the nightly backup mirrors create-only (#185).
DEFAULT_INBOX = Path.home() / "wslcb-relay-inbox"
#: A capture older than this when imported is refused: it would date the
#: freshness reading by a page that is no longer current.
MAX_AGE = timedelta(hours=6)
#: Clock skew tolerated between the laptop and this host.
MAX_FUTURE_SKEW = timedelta(minutes=5)
#: The real report is ~13 MB. A block or error page is a few hundred bytes.
MIN_BYTES = 1_000_000
#: What ``sources.metadata`` names as the relay. No IP or hostname, ever:
#: source details sit behind a public page.
RELAY_LABEL = "owner-laptop"

_NAME = re.compile(r"^(\d{8}T\d{6}Z)\.html\.gz$")
_NAME_FORMAT = "%Y%m%dT%H%M%SZ"


class RelayRejectedError(Exception):
    """A capture that must not be ingested; the message says why."""


@dataclass(frozen=True)
class RelayResult:
    """What became of one inbox file."""

    name: str
    status: Literal["success", "unchanged", "rejected", "failed"]
    scrape_log_id: int
    reason: str = ""


def inbox_dir(environ: Mapping[str, str]) -> Path:
    """The inbox: ``WSLCB_RELAY_INBOX`` if set, else ``DEFAULT_INBOX``."""
    value = environ.get("WSLCB_RELAY_INBOX")
    return Path(value) if value else DEFAULT_INBOX


def capture_name(captured_at: datetime) -> str:
    """The inbox filename for a capture fetched at *captured_at*."""
    return captured_at.astimezone(UTC).strftime(_NAME_FORMAT) + ".html.gz"


def parse_capture_name(name: str) -> datetime:
    """The UTC fetch time a capture's filename carries."""
    match = _NAME.match(name)
    if not match:
        msg = f"{name!r} is not a capture name (<YYYYMMDDTHHMMSSZ>.html.gz)"
        raise RelayRejectedError(msg)
    return datetime.strptime(match.group(1), _NAME_FORMAT).replace(tzinfo=UTC)


def validate_report(html: str) -> None:
    """Refuse anything that isn't a whole copy of the real report.

    Checks run cheapest first: size, a closing ``</html>`` (a cut-off
    download), all three section headers, and at least one record.
    """
    if len(html) < MIN_BYTES:
        msg = f"capture is {len(html):,} bytes; the real report is over {MIN_BYTES:,}"
        raise RelayRejectedError(msg)
    if not html.rstrip().lower().endswith("</html>"):
        msg = "capture is truncated: it does not end in </html>"
        raise RelayRejectedError(msg)
    soup = BeautifulSoup(html, "lxml")
    tables = {}
    for table in soup.find_all("table"):
        th = table.find("th")
        header = th.get_text(strip=True).replace("\xa0", " ") if th else ""
        if header in SECTION_MAP:
            tables[header] = table
    missing = [h for h in SECTION_MAP if h not in tables]
    if missing:
        msg = f"capture lacks section(s): {', '.join(missing)}"
        raise RelayRejectedError(msg)
    if not any(parse_records_from_table(t, SECTION_MAP[h]) for h, t in tables.items()):
        msg = "capture has the sections but no records"
        raise RelayRejectedError(msg)


def _read(path: Path) -> str:
    try:
        data = gzip.decompress(path.read_bytes())
    except (OSError, EOFError) as exc:
        msg = f"capture is not readable gzip: {exc}"
        raise RelayRejectedError(msg) from exc
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("iso-8859-1")  # the page's declared charset


def _discard(path: Path) -> None:
    path.unlink()


def _check_age(captured_at: datetime, now: datetime) -> None:
    if captured_at - now > MAX_FUTURE_SKEW:
        msg = f"capture time {captured_at:%Y-%m-%dT%H:%M:%SZ} is in the future"
        raise RelayRejectedError(msg)
    age = now - captured_at
    if age > MAX_AGE:
        hours = age / timedelta(hours=1)
        limit = MAX_AGE / timedelta(hours=1)
        msg = f"capture is {hours:.1f} h old; the limit is {limit:g} h"
        raise RelayRejectedError(msg)


def _pending(inbox: Path, now: datetime) -> list[Path]:
    """Files waiting in *inbox*, oldest capture first; uploads in flight excluded.

    A ``*.part`` older than ``MAX_AGE`` is an upload whose rename never came
    (ssh failed after scp); it could never be imported, so it is removed.
    """
    if not inbox.is_dir():
        return []
    pending = []
    for p in inbox.iterdir():
        if not p.is_file():
            continue
        if not p.name.endswith(".part"):
            pending.append(p)
            continue
        try:
            mtime = p.stat().st_mtime
        except FileNotFoundError:
            continue  # renamed into place by a concurrent upload; the next run takes it
        if now.timestamp() - mtime > MAX_AGE.total_seconds():
            logger.warning("Removing abandoned upload %s", p.name)
            p.unlink(missing_ok=True)
    return sorted(pending)


def _move_to_rejected(path: Path) -> None:
    dest = path.parent / "rejected"
    dest.mkdir(exist_ok=True)
    path.replace(dest / path.name)


async def _rejected(
    conn: AsyncConnection, path: Path, log_id: int, exc: RelayRejectedError
) -> RelayResult:
    """Mark the run failed and move the capture to ``rejected/``."""
    reason = str(exc)
    logger.warning("Rejected relayed capture %s: %s", path.name, reason)
    await fail_log(conn, log_id, exc)
    _move_to_rejected(path)
    return RelayResult(path.name, "rejected", log_id, reason)


async def _import_one(conn: AsyncConnection, path: Path, now: datetime) -> RelayResult:
    try:
        captured_at = parse_capture_name(path.name)
    except RelayRejectedError as exc:
        return await _rejected(conn, path, await start_log(conn, origin="relay"), exc)
    log_id = await start_log(conn, origin="relay", captured_at=captured_at)
    try:
        _check_age(captured_at, now)
        html = _read(path)
        validate_report(html)
        logger.info("Importing relayed capture %s", path.name)
        outcome = await ingest_page(
            conn,
            html,
            log_id,
            source_type=SOURCE_TYPE_RELAYED_SCRAPE,
            captured_at=captured_at,
            metadata={
                "relay": RELAY_LABEL,
                "captured_at": captured_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
    except RelayRejectedError as exc:
        return await _rejected(conn, path, log_id, exc)
    except Exception as exc:  # reported as ``failed``; the file stays queued
        logger.exception("Relayed capture %s failed to import; left queued", path.name)
        await fail_log(conn, log_id, exc)
        return RelayResult(path.name, "failed", log_id, f"{type(exc).__name__}: {exc}")
    _discard(path)
    return RelayResult(path.name, outcome.status, log_id)


async def import_inbox(
    engine: AsyncEngine, inbox: Path, *, now: datetime | None = None
) -> list[RelayResult]:
    """Import every capture waiting in *inbox*, oldest first.

    *now* defaults to UTC now; the age check measures against it. Returns
    one result per file attempted, stopping after the first ``failed``; a
    missing or empty inbox returns ``[]``.
    """
    now = now or datetime.now(UTC)
    files = _pending(inbox, now)
    if not files:
        logger.info("Relay inbox %s is empty or absent; nothing to import", inbox)
        return []
    results = []
    async with get_db(engine) as conn:
        for path in files:
            results.append(await _import_one(conn, path, now))
            if results[-1].status == "failed":
                break
    return results
