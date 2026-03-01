"""Backward-compatibility shim for the old database module.

Connection management now lives in ``db.py``; schema creation and
migrations in ``schema.py``.  Helper functions (locations, sources,
provenance) remain here for now and are re-exported alongside the
symbols from the new modules.

All existing ``from database import ...`` statements continue to work.
"""
import json
import logging
import sqlite3

# Re-export everything that consumers expect from ``database``.
from db import (  # noqa: F401 — re-exports
    DATA_DIR,
    DB_PATH,
    SOURCE_TYPE_CO_ARCHIVE,
    SOURCE_TYPE_CO_DIFF_ARCHIVE,
    SOURCE_TYPE_INTERNET_ARCHIVE,
    SOURCE_TYPE_LIVE_SCRAPE,
    SOURCE_TYPE_MANUAL,
    WSLCB_SOURCE_URL,
    _normalize_raw_address,
    get_connection,
    get_db,
)
from schema import init_db  # noqa: F401 — re-export

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Location helpers
# ------------------------------------------------------------------

def get_or_create_location(
    conn: sqlite3.Connection,
    raw_address: str,
    city: str = "",
    state: str = "WA",
    zip_code: str = "",
) -> int | None:
    """Return the location id for *raw_address*, creating if needed.

    Returns None if raw_address is empty/None.
    """
    if not raw_address or not raw_address.strip():
        return None
    normalized = _normalize_raw_address(raw_address)
    row = conn.execute(
        "SELECT id FROM locations WHERE raw_address = ?", (normalized,)
    ).fetchone()
    if row:
        return row[0]
    cursor = conn.execute(
        """INSERT INTO locations (raw_address, city, state, zip_code)
           VALUES (?, ?, ?, ?)""",
        (normalized, city, state, zip_code),
    )
    return cursor.lastrowid


def get_or_create_source(
    conn: sqlite3.Connection,
    source_type_id: int,
    snapshot_path: str | None = None,
    url: str | None = None,
    captured_at: str | None = None,
    scrape_log_id: int | None = None,
    metadata: dict | None = None,
) -> int:
    """Return the source id for the given type + snapshot_path, creating if needed.

    Uses INSERT OR IGNORE followed by SELECT so the call is idempotent.
    The (source_type_id, snapshot_path) pair is the uniqueness key.

    When *snapshot_path* is None and *scrape_log_id* is provided (i.e. a live
    scrape whose snapshot save failed), the lookup uses *scrape_log_id* to
    distinguish distinct scrape events that both lack a snapshot.  Without
    this, all NULL-path scrapes would conflate into the first row.

    .. note:: SQLite treats NULLs as distinct in UNIQUE constraints, so when
       *snapshot_path* is None the SELECT uses ``IS NULL`` checks to find an
       existing row before inserting a new one.
    """
    meta_json = json.dumps(metadata) if metadata else "{}"
    if snapshot_path is not None:
        conn.execute(
            """INSERT OR IGNORE INTO sources
                   (source_type_id, snapshot_path, url, captured_at,
                    scrape_log_id, metadata)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (source_type_id, snapshot_path, url, captured_at,
             scrape_log_id, meta_json),
        )
        row = conn.execute(
            """SELECT id FROM sources
               WHERE source_type_id = ? AND snapshot_path = ?""",
            (source_type_id, snapshot_path),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"Source row vanished for type={source_type_id},"
                f" path={snapshot_path!r}"
            )
    else:
        # NULL snapshot_path — match on scrape_log_id when available so
        # distinct scrape events with failed snapshot saves each get their
        # own source row.
        if scrape_log_id is not None:
            row = conn.execute(
                """SELECT id FROM sources
                   WHERE source_type_id = ? AND snapshot_path IS NULL
                     AND scrape_log_id = ?""",
                (source_type_id, scrape_log_id),
            ).fetchone()
        else:
            row = conn.execute(
                """SELECT id FROM sources
                   WHERE source_type_id = ? AND snapshot_path IS NULL
                     AND scrape_log_id IS NULL""",
                (source_type_id,),
            ).fetchone()
        if row is None:
            cursor = conn.execute(
                """INSERT INTO sources
                       (source_type_id, snapshot_path, url, captured_at,
                        scrape_log_id, metadata)
                   VALUES (?, NULL, ?, ?, ?, ?)""",
                (source_type_id, url, captured_at,
                 scrape_log_id, meta_json),
            )
            return cursor.lastrowid
    return row[0]


def link_record_source(
    conn: sqlite3.Connection,
    record_id: int,
    source_id: int,
    role: str = "first_seen",
) -> None:
    """Link a license record to a source (idempotent).

    Does nothing if the (record_id, source_id) pair already exists.
    """
    conn.execute(
        """INSERT OR IGNORE INTO record_sources (record_id, source_id, role)
           VALUES (?, ?, ?)""",
        (record_id, source_id, role),
    )


if __name__ == "__main__":
    from log_config import setup_logging
    setup_logging()
    init_db()
    logger.info("Database initialized at %s", DB_PATH)
