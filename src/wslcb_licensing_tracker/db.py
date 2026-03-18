"""Connection management, constants, and core database helpers for the WSLCB licensing tracker.

This is the stable base layer.  Schema creation and migrations live in
``schema.py``; query helpers in ``queries.py``; record-insertion logic in
``pipeline.py``.

Helper functions for locations, sources, provenance linking, and
source-provenance queries (``get_or_create_location``,
``get_or_create_source``, ``link_record_source``, ``get_primary_source``,
``get_record_sources``) live here.
"""

import json
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

# ---------------------------------------------------------------------------
# Text normalization utilities
#
# These pure-string helpers live in the stable base layer so both schema.py
# (migrations) and entities.py (pipeline) can import them without creating a
# schema → business-logic layering violation.
# ---------------------------------------------------------------------------

# Suffixes where a trailing period is legitimate and should be kept.
# The full list: INC, LLC, L.L.C, LTD, CORP, CO, L.P, L.L.P, PTY, P.C, N.A, P.A,
# JR, SR, S.P.A, F.O.E, U.P, D.B.A, W. & S
_LEGIT_TRAILING_DOT = re.compile(
    r"(?:(?<=\s)|(?<=^))"
    r"(?:INC|LLC|L\.L\.C|L\.L\.P|LTD|CORP|CO|L\.P|PTY"
    r"|JR|SR"
    r"|S\.P\.A|F\.O\.E|U\.P|D\.B\.A|P\.C|N\.A|P\.A"
    r"|W\. & S)"
    r"\.\s*$"
)

# Regex matching WSLCB "DUPLICATE" annotation tokens embedded in applicant names.
_DUPLICATE_MARKER_RE = re.compile(
    r"\s*"
    r"(?:"
    r"\(\s*DUPLICATE(?:\s+\d+)?\s*\)"  # (DUPLICATE), (DUPLICATE 2), ... — closed
    r"|\*DUPLICATE\*"  # *DUPLICATE*
    r"|\(\s*DUPLICATE(?:\s+\d+)?"  # (DUPLICATE ... — unclosed paren
    r"|DUPLICATE"  # bare word
    r")",
    re.IGNORECASE,
)

logger = logging.getLogger(__name__)

# Source type constants (fixed IDs — must match seed data in schema.py)
SOURCE_TYPE_LIVE_SCRAPE = 1
SOURCE_TYPE_CO_ARCHIVE = 2
SOURCE_TYPE_INTERNET_ARCHIVE = 3
SOURCE_TYPE_CO_DIFF_ARCHIVE = 4
SOURCE_TYPE_MANUAL = 5

WSLCB_SOURCE_URL = "https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp"

# All persistent data (DB + HTML snapshots) lives under DATA_DIR.
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data"))
DB_PATH = DATA_DIR / "wslcb.db"


def get_connection(path: str | Path | None = None) -> sqlite3.Connection:
    """Open a connection to *path* (defaults to ``DB_PATH``).

    Passing ``":memory:"`` gives an ephemeral in-memory database —
    useful for tests.  When using the default on-disk path, the
    parent directory is created automatically.
    """
    effective = str(path) if path is not None else str(DB_PATH)
    if effective != ":memory:":
        Path(effective).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(effective)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db(path: str | Path | None = None):  # noqa: ANN201  # yields sqlite3.Connection
    """Context manager wrapping :func:`get_connection`."""
    conn = get_connection(path)
    try:
        yield conn
    finally:
        conn.close()


def _normalize_raw_address(raw: str) -> str:
    r"""Normalize whitespace variants (NBSP → space) in raw address strings.

    The WSLCB source page sometimes uses non-breaking spaces (\xa0)
    instead of regular spaces.  We normalize before lookup so that
    cosmetically-identical strings map to the same location row.
    """
    if not raw:
        return raw
    return re.sub(r"\xa0+", " ", raw)


def clean_entity_name(name: str) -> str:
    """Normalize an entity name: uppercase, strip whitespace, remove stray trailing punctuation.

    The WSLCB source occasionally appends periods or commas to names
    as data-entry artifacts (e.g., ``WOLDU ARAYA BERAKI.``).  This
    strips those while preserving legitimate endings like ``INC.`` or
    ``JR.``.
    """
    cleaned = name.strip().upper()
    cleaned = re.sub(r"\s+", " ", cleaned)
    while cleaned and cleaned[-1] in ".," and not _LEGIT_TRAILING_DOT.search(cleaned):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def strip_duplicate_marker(name: str) -> str:
    """Remove WSLCB DUPLICATE annotation token(s) from an applicant name.

    Handles all observed formats::

        ADAM (DUPLICATE) BENTON           -> ADAM BENTON
        NEALY DUPLICATE EVANS             -> NEALY EVANS
        KATIE (DUPLICATE 2) DAVIS         -> KATIE DAVIS
        PAUL *DUPLICATE* SONG             -> PAUL SONG
        DUPLICATE ITALIAN SUPPLY, LLC     -> ITALIAN SUPPLY, LLC
        JAY WON (DUPLICATE)               -> JAY WON
        ELIZABETH (DUPLICATE A MATTHEWS   -> ELIZABETH A MATTHEWS  (unclosed paren)

    Collapses any resulting runs of whitespace and strips leading/trailing
    spaces.  The caller is responsible for full normalization (e.g. uppercase)
    via ``clean_entity_name()``.
    """
    stripped = _DUPLICATE_MARKER_RE.sub("", name)
    return re.sub(r" {2,}", " ", stripped).strip()


def clean_applicants_string(applicants: str | None) -> str | None:
    """Clean each semicolon-separated part of an applicants string.

    Applies ``strip_duplicate_marker()`` then ``clean_entity_name()`` to
    every element so the stored string is consistent with entity names in
    the ``entities`` table.  After stripping, duplicate tokens are removed
    (first occurrence wins — preserving order).  Empty parts after cleaning
    are dropped.  Returns ``None`` unchanged.
    """
    if not applicants:
        return applicants
    parts = [clean_entity_name(strip_duplicate_marker(p)) for p in applicants.split(";")]
    seen: set[str] = set()
    deduped: list[str] = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            deduped.append(p)
    return "; ".join(deduped)


# Source-role priority used when selecting the "best" source for display.
# Lower value = higher priority.  Imported by display.py to avoid circular imports.
SOURCE_ROLE_PRIORITY: dict[str, int] = {"first_seen": 0, "repaired": 1, "confirmed": 2}

# US state code → full name mapping.  Used by the state filter dropdown and
# the address validation layer.  Defined here as reference data with no query
# concerns.
US_STATES: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}


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
    row = conn.execute("SELECT id FROM locations WHERE raw_address = ?", (normalized,)).fetchone()
    if row:
        return row[0]
    cursor = conn.execute(
        """INSERT INTO locations (raw_address, city, state, zip_code)
           VALUES (?, ?, ?, ?)""",
        (normalized, city, state, zip_code),
    )
    return cursor.lastrowid


# ------------------------------------------------------------------
# Source helpers
# ------------------------------------------------------------------


def get_or_create_source(  # noqa: PLR0913  # 7 args — all needed for source provenance
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
            (source_type_id, snapshot_path, url, captured_at, scrape_log_id, meta_json),
        )
        row = conn.execute(
            """SELECT id FROM sources
               WHERE source_type_id = ? AND snapshot_path = ?""",
            (source_type_id, snapshot_path),
        ).fetchone()
        if row is None:
            msg = f"Source row vanished for type={source_type_id}, path={snapshot_path!r}"
            raise RuntimeError(msg)
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
                (source_type_id, url, captured_at, scrape_log_id, meta_json),
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

    Does nothing if the (record_id, source_id, role) combination already exists.
    """
    conn.execute(
        """INSERT OR IGNORE INTO record_sources (record_id, source_id, role)
           VALUES (?, ?, ?)""",
        (record_id, source_id, role),
    )


# ------------------------------------------------------------------
# Provenance query helpers
# ------------------------------------------------------------------


def get_primary_source(
    conn: sqlite3.Connection,
    record_id: int,
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
        role_rank = SOURCE_ROLE_PRIORITY.get(d["role"], 2)
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
    conn: sqlite3.Connection,
    record_id: int,
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
    from wslcb_licensing_tracker.log_config import setup_logging
    from wslcb_licensing_tracker.schema import init_db

    setup_logging()
    init_db()
    logger.info("Database initialized at %s", DB_PATH)
