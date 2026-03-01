"""Database integrity checks for the WSLCB licensing tracker.

Provides check functions that detect data quality issues and optional
fix functions for safe auto-remediation.  Used by ``cli.py check``.
"""
import logging
import sqlite3

logger = logging.getLogger(__name__)


# ── Orphaned locations ───────────────────────────────────────


def check_orphaned_locations(conn: sqlite3.Connection) -> list[dict]:
    """Find locations not referenced by any license_records row.

    Returns a list of dicts with ``id`` and ``raw_address``.
    """
    rows = conn.execute("""
        SELECT l.id, l.raw_address
        FROM locations l
        WHERE l.id NOT IN (
            SELECT DISTINCT location_id FROM license_records WHERE location_id IS NOT NULL
            UNION
            SELECT DISTINCT previous_location_id FROM license_records WHERE previous_location_id IS NOT NULL
        )
    """).fetchall()
    return [dict(r) for r in rows]


def fix_orphaned_locations(conn: sqlite3.Connection) -> int:
    """Delete orphaned locations.  Returns the number removed."""
    orphans = check_orphaned_locations(conn)
    if not orphans:
        return 0
    ids = [o["id"] for o in orphans]
    placeholders = ",".join("?" * len(ids))
    conn.execute(f"DELETE FROM locations WHERE id IN ({placeholders})", ids)
    conn.commit()
    logger.info("Removed %d orphaned location(s).", len(ids))
    return len(ids)


# ── Broken foreign keys ──────────────────────────────────────


def check_broken_fks(conn: sqlite3.Connection) -> list[dict]:
    """Find records with location_id or previous_location_id pointing nowhere.

    Returns a list of dicts with ``record_id``, ``column``, ``bad_id``.
    """
    results = []
    for col in ("location_id", "previous_location_id"):
        rows = conn.execute(f"""
            SELECT lr.id AS record_id, lr.{col} AS bad_id
            FROM license_records lr
            WHERE lr.{col} IS NOT NULL
              AND lr.{col} NOT IN (SELECT id FROM locations)
        """).fetchall()
        for r in rows:
            results.append({
                "record_id": r["record_id"],
                "column": col,
                "bad_id": r["bad_id"],
            })
    return results


# ── Un-enriched records ──────────────────────────────────────


def check_unenriched_records(conn: sqlite3.Connection) -> dict:
    """Count records missing key enrichment artifacts.

    Returns a dict with counts for each category:
    - ``no_endorsements``: records with non-empty license_type but no
      record_endorsements rows
    - ``no_entities``: records with non-empty applicants but no
      record_entities rows
    - ``no_provenance``: records with no record_sources rows
    - ``no_enrichment_tracking``: records missing from record_enrichments
      for any expected step
    """
    no_endorsements = conn.execute("""
        SELECT COUNT(*) FROM license_records lr
        WHERE lr.license_type IS NOT NULL AND lr.license_type != ''
          AND NOT EXISTS (
              SELECT 1 FROM record_endorsements re WHERE re.record_id = lr.id
          )
    """).fetchone()[0]

    # Entities are only expected for records with applicants containing
    # at least one semicolon (first element is business name, excluded).
    no_entities = conn.execute("""
        SELECT COUNT(*) FROM license_records lr
        WHERE lr.applicants LIKE '%;%'
          AND NOT EXISTS (
              SELECT 1 FROM record_entities re WHERE re.record_id = lr.id
          )
    """).fetchone()[0]

    no_provenance = conn.execute("""
        SELECT COUNT(*) FROM license_records lr
        WHERE NOT EXISTS (
            SELECT 1 FROM record_sources rs WHERE rs.record_id = lr.id
        )
    """).fetchone()[0]

    no_enrichment = conn.execute("""
        SELECT COUNT(*) FROM license_records lr
        WHERE NOT EXISTS (
            SELECT 1 FROM record_enrichments re
            WHERE re.record_id = lr.id AND re.step = 'endorsements'
        )
    """).fetchone()[0]

    return {
        "no_endorsements": no_endorsements,
        "no_entities": no_entities,
        "no_provenance": no_provenance,
        "no_enrichment_tracking": no_enrichment,
    }


# ── Endorsement anomalies ────────────────────────────────────


def check_endorsement_anomalies(conn: sqlite3.Connection) -> dict:
    """Detect endorsement-related data issues.

    Returns a dict with:
    - ``unresolved_codes``: records with numeric license_type that have
      no endorsement links
    - ``placeholder_endorsements``: endorsements whose name is purely
      numeric (placeholder from unknown codes)
    """
    unresolved = conn.execute("""
        SELECT COUNT(*) FROM license_records lr
        WHERE lr.license_type GLOB '[0-9]*'
          AND lr.id NOT IN (SELECT record_id FROM record_endorsements)
    """).fetchone()[0]

    placeholders = conn.execute("""
        SELECT COUNT(*) FROM license_endorsements
        WHERE name GLOB '[0-9]*' AND name NOT GLOB '*[a-zA-Z]*'
    """).fetchone()[0]

    return {
        "unresolved_codes": unresolved,
        "placeholder_endorsements": placeholders,
    }


# ── Entity duplicates ────────────────────────────────────────


def check_entity_duplicates(conn: sqlite3.Connection) -> list[dict]:
    """Find entities that would merge under current cleaning rules.

    Detects entities whose names differ only by case.
    Returns a list of dicts with ``upper_name``, ``cnt``, ``names``.
    """
    rows = conn.execute("""
        SELECT UPPER(name) AS upper_name, COUNT(*) AS cnt,
               GROUP_CONCAT(name, ' | ') AS names
        FROM entities
        GROUP BY UPPER(name)
        HAVING cnt > 1
    """).fetchall()
    return [dict(r) for r in rows]


# ── Aggregate runner ─────────────────────────────────────────


def run_all_checks(
    conn: sqlite3.Connection,
    *,
    fix: bool = False,
) -> dict:
    """Run all integrity checks and optionally auto-fix safe issues.

    Returns a structured report dict.
    """
    report: dict = {}

    # Orphaned locations
    orphans = check_orphaned_locations(conn)
    entry: dict = {"count": len(orphans)}
    if fix and orphans:
        entry["fixed"] = fix_orphaned_locations(conn)
    report["orphaned_locations"] = entry

    # Broken FKs
    broken = check_broken_fks(conn)
    report["broken_fks"] = {"count": len(broken), "details": broken}

    # Un-enriched records
    unenriched = check_unenriched_records(conn)
    report["unenriched"] = unenriched

    # Endorsement anomalies
    endorsement = check_endorsement_anomalies(conn)
    report["endorsement_anomalies"] = endorsement

    # Entity duplicates
    dupes = check_entity_duplicates(conn)
    report["entity_duplicates"] = {"count": len(dupes), "details": dupes}

    return report


def print_report(report: dict) -> int:
    """Print a human-readable integrity report to stdout.

    Returns the total number of issues found (0 = clean).
    """
    total_issues = 0

    print("\n=== WSLCB Database Integrity Report ===")
    print()

    # Orphaned locations
    n = report["orphaned_locations"]["count"]
    fixed = report["orphaned_locations"].get("fixed", 0)
    if n:
        total_issues += n
        status = f"  FIXED {fixed}" if fixed else "  (use --fix to remove)"
        print(f"\u274c Orphaned locations: {n}{status}")
    else:
        print("\u2705 No orphaned locations")

    # Broken FKs
    n = report["broken_fks"]["count"]
    if n:
        total_issues += n
        print(f"\u274c Broken foreign keys: {n}")
        for d in report["broken_fks"]["details"][:5]:
            print(f"     record {d['record_id']}: {d['column']} = {d['bad_id']}")
        if n > 5:
            print(f"     ... and {n - 5} more")
    else:
        print("\u2705 No broken foreign keys")

    # Un-enriched
    ue = report["unenriched"]
    for key, label in [
        ("no_endorsements", "Records missing endorsement links"),
        ("no_entities", "Records missing entity links"),
        ("no_provenance", "Records missing provenance"),
        ("no_enrichment_tracking", "Records missing enrichment tracking"),
    ]:
        n = ue[key]
        if n:
            total_issues += n
            print(f"\u274c {label}: {n}")
        else:
            print(f"\u2705 {label}: 0")

    # Endorsement anomalies
    ea = report["endorsement_anomalies"]
    for key, label in [
        ("unresolved_codes", "Unresolved numeric codes"),
        ("placeholder_endorsements", "Placeholder endorsements"),
    ]:
        n = ea[key]
        if n:
            total_issues += n
            print(f"\u274c {label}: {n}")
        else:
            print(f"\u2705 {label}: 0")

    # Entity duplicates
    n = report["entity_duplicates"]["count"]
    if n:
        total_issues += n
        print(f"\u274c Entity duplicate groups: {n}")
        for d in report["entity_duplicates"]["details"][:5]:
            print(f"     {d['names']}")
        if n > 5:
            print(f"     ... and {n - 5} more")
    else:
        print("\u2705 No entity duplicates")

    print()
    if total_issues:
        print(f"Total issues: {total_issues}")
    else:
        print("\u2705 All checks passed!")
    print()

    return total_issues
