"""Database helper module for the WSLCB licensing tracker.

Connection management lives in ``db.py``; schema creation and migrations
in ``schema.py``.  This module owns helper functions for locations,
sources, provenance linking, and source-provenance queries
(``get_primary_source``, ``get_record_sources``).  It also re-exports
all ``db.py`` and ``schema.py`` public symbols so that existing
``from database import ...`` statements continue to work.
"""
import json
import logging
import sqlite3

# Re-export everything that consumers expect from ``database``.
from db import (  # noqa: F401 — re-exports
    DATA_DIR,
    DB_PATH,
    SOURCE_ROLE_PRIORITY,
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


# Alias for backward-compat and local use; canonical definition is in db.py.
_ROLE_PRIORITY = SOURCE_ROLE_PRIORITY


def get_primary_source(
    conn: sqlite3.Connection, record_id: int,
) -> dict | None:
    """Return the single most-relevant source for a record, or None.

    Priority order:
    1. Role: ``first_seen`` > ``repaired`` > ``confirmed``
    2. Within a role: sources with a non-NULL ``snapshot_path`` first
    3. Newest ``captured_at`` as tiebreaker
    """
    rows = conn.execute(
        """SELECT s.id, st.slug AS source_type, st.label AS source_label,
                  s.snapshot_path, s.url, s.captured_at, s.ingested_at,
                  s.metadata, rs.role
           FROM record_sources rs
           JOIN sources s ON s.id = rs.source_id
           JOIN source_types st ON st.id = s.source_type_id
           WHERE rs.record_id = ?
           ORDER BY s.captured_at DESC""",
        (record_id,),
    ).fetchall()
    if not rows:
        return None

    best = None
    best_priority = (999, 999)  # (role_rank, no_snapshot_penalty)
    for r in rows:
        d = dict(r)
        role_rank = _ROLE_PRIORITY.get(d["role"], 2)
        no_snap = 0 if d["snapshot_path"] else 1
        priority = (role_rank, no_snap)
        if best is None or priority < best_priority:
            best = d
            best_priority = priority

    if best is not None:
        raw = best.get("metadata")
        best["metadata"] = json.loads(raw) if raw else {}
    return best


def get_record_sources(
    conn: sqlite3.Connection, record_id: int,
) -> list[dict]:
    """Return provenance sources for a record, newest first."""
    rows = conn.execute(
        """SELECT s.id, st.slug AS source_type, st.label AS source_label,
                  s.snapshot_path, s.url, s.captured_at, s.ingested_at,
                  s.metadata, rs.role
           FROM record_sources rs
           JOIN sources s ON s.id = rs.source_id
           JOIN source_types st ON st.id = s.source_type_id
           WHERE rs.record_id = ?
           ORDER BY s.captured_at DESC""",
        (record_id,),
    ).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        raw = d.get("metadata")
        d["metadata"] = json.loads(raw) if raw else {}
        results.append(d)
    return results


if __name__ == "__main__":
    from log_config import setup_logging
    setup_logging()
    init_db()
    logger.info("Database initialized at %s", DB_PATH)
