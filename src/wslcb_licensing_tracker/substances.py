"""Regulated substance CRUD for WSLCB licensing tracker.

Manages the ``regulated_substances`` table and its M:M junction
``regulated_substance_endorsements``.  Audit logging is delegated
to the caller (route handler) via ``admin_audit.log_action()`` so
this module stays free of web/admin concerns.

Functions
---------
get_regulated_substances  -- query all substances with endorsement names
get_substance_endorsement_ids -- endorsement IDs for one substance
set_substance_endorsements -- replace junction rows (caller commits + audits)
add_substance             -- insert a substance row (caller commits + audits)
remove_substance          -- delete a substance row (caller commits + audits)
"""
import sqlite3


def get_regulated_substances(conn: sqlite3.Connection) -> list[dict]:
    """Return all substances ordered by display_order, each with endorsement names.

    Each entry is a dict with keys ``id``, ``name``, ``display_order``,
    and ``endorsements`` (a sorted list of canonical endorsement name strings).
    """
    substances = conn.execute(
        "SELECT id, name, display_order FROM regulated_substances ORDER BY display_order, name"
    ).fetchall()
    results = []
    for row in substances:
        sid, name, order = row
        enames = conn.execute(
            """
            SELECT le.name FROM regulated_substance_endorsements rse
            JOIN license_endorsements le ON le.id = rse.endorsement_id
            WHERE rse.substance_id = ?
            ORDER BY le.name
            """,
            (sid,),
        ).fetchall()
        results.append({
            "id": sid,
            "name": name,
            "display_order": order,
            "endorsements": [r[0] for r in enames],
        })
    return results


def get_substance_endorsement_ids(conn: sqlite3.Connection, substance_id: int) -> list[int]:
    """Return the endorsement IDs associated with *substance_id*."""
    rows = conn.execute(
        "SELECT endorsement_id FROM regulated_substance_endorsements WHERE substance_id = ?",
        (substance_id,),
    ).fetchall()
    return [r[0] for r in rows]


def set_substance_endorsements(
    conn: sqlite3.Connection,
    substance_id: int,
    endorsement_ids: list[int],
) -> None:
    """Replace the full endorsement list for *substance_id*.

    Deletes all existing junction rows then inserts the new set.
    The caller is responsible for committing and writing the audit log entry.
    """
    conn.execute(
        "DELETE FROM regulated_substance_endorsements WHERE substance_id = ?",
        (substance_id,),
    )
    for eid in endorsement_ids:
        conn.execute(
            "INSERT OR IGNORE INTO regulated_substance_endorsements (substance_id, endorsement_id)"
            " VALUES (?, ?)",
            (substance_id, eid),
        )


def add_substance(
    conn: sqlite3.Connection,
    name: str,
    display_order: int,
) -> int:
    """Insert a new regulated substance and return its id.

    The caller is responsible for committing and writing the audit log entry.
    """
    cursor = conn.execute(
        "INSERT INTO regulated_substances (name, display_order) VALUES (?, ?)",
        (name, display_order),
    )
    return cursor.lastrowid


def remove_substance(
    conn: sqlite3.Connection,
    substance_id: int,
) -> str | None:
    """Delete a regulated substance (cascades to junction rows).

    Returns the substance name (for audit logging), or None if not found.
    The caller is responsible for committing and writing the audit log entry.
    """
    row = conn.execute(
        "SELECT name FROM regulated_substances WHERE id = ?", (substance_id,)
    ).fetchone()
    name = row[0] if row else None
    conn.execute(
        "DELETE FROM regulated_substances WHERE id = ?", (substance_id,)
    )
    return name
