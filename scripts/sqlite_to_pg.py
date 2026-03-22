#!/usr/bin/env python3
"""One-time SQLite → PostgreSQL data migration.

Reads data/wslcb.db via sqlite3, inserts into PostgreSQL in FK-dependency
order, resets identity sequences, and backfills search_vector.

Usage:
    DATABASE_URL=postgresql+asyncpg://... python scripts/sqlite_to_pg.py
    DATABASE_URL=postgresql+asyncpg://... python scripts/sqlite_to_pg.py --dry-run

Prerequisites:
    - Alembic migrations must be applied first: alembic upgrade head
    - PostgreSQL database must be empty (or have only system rows)
    - SQLite database must be at data/wslcb.db (or set SQLITE_PATH env var)

Safety:
    - Idempotent on empty target: ON CONFLICT DO NOTHING on UNIQUE columns
    - Reads SQLite in read-only mode (uri=True with ?mode=ro)
    - Does NOT modify or delete data/wslcb.db
"""

import asyncio
import os
import sqlite3
import sys
from pathlib import Path

import asyncpg

SQLITE_PATH = Path(os.environ.get("SQLITE_PATH", "data/wslcb.db"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
BATCH_SIZE = 500

# Tables with GENERATED ALWAYS AS IDENTITY columns — need sequence reset after copy
IDENTITY_TABLES = [
    "locations",
    "entities",
    "license_endorsements",
    "scrape_log",
    "license_records",
    "sources",
    "endorsement_aliases",
    "regulated_substances",
    "record_links",
    "admin_users",
    "admin_audit_log",
    "data_migrations",
]

# Insertion order: FK parents before FK children
TABLE_ORDER = [
    "source_types",
    "locations",
    "entities",
    "license_endorsements",
    "endorsement_codes",
    "scrape_log",
    "license_records",
    "sources",
    "record_endorsements",
    "record_sources",
    "endorsement_aliases",
    "endorsement_dismissed_suggestions",
    "regulated_substances",
    "regulated_substance_endorsements",
    "record_entities",
    "record_enrichments",
    "record_links",
    "admin_users",
    "admin_audit_log",
    "data_migrations",
]


def read_sqlite_table(conn: sqlite3.Connection, table: str) -> tuple[list[str], list[tuple]]:
    """Return (column_names, rows) for a SQLite table. Returns ([], []) if table missing."""
    try:
        cur = conn.execute(f"SELECT * FROM {table}")  # noqa: S608
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    except sqlite3.OperationalError:
        return [], []


async def copy_table(
    pg: asyncpg.Connection,
    table: str,
    cols: list[str],
    rows: list[tuple],
    *,
    dry_run: bool = False,
) -> int:
    """Insert rows into PostgreSQL table. Returns count inserted."""
    if not rows:
        return 0

    has_identity = table in IDENTITY_TABLES
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
    overriding = "OVERRIDING SYSTEM VALUE" if has_identity else ""

    sql = (
        f'INSERT INTO {table} ({col_list}) {overriding} VALUES ({placeholders})'
        f' ON CONFLICT DO NOTHING'
    )

    if dry_run:
        print(f"  [dry-run] {table}: would insert {len(rows):,} rows")
        return len(rows)

    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        # Convert sqlite3.Row tuples to list-of-tuples asyncpg expects
        await pg.executemany(sql, [tuple(r) for r in batch])
        inserted += len(batch)

    return inserted


async def reset_sequences(pg: asyncpg.Connection) -> None:
    """Reset identity sequences for all identity tables.

    ``pg_get_serial_sequence()`` does not work for GENERATED ALWAYS AS IDENTITY
    columns (returns NULL). Use ``ALTER TABLE ... ALTER COLUMN id RESTART WITH``
    instead — this is the correct approach for identity columns.
    """
    for table in IDENTITY_TABLES:
        try:
            max_id = await pg.fetchval(f"SELECT COALESCE(MAX(id), 0) FROM {table}")  # noqa: S608
            next_val = max_id + 1
            await pg.execute(
                f"ALTER TABLE {table} ALTER COLUMN id RESTART WITH {next_val}"
            )
            print(f"  Reset sequence: {table} → start at {next_val}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: Could not reset sequence for {table}: {exc}")


async def backfill_search_vector(pg: asyncpg.Connection) -> None:
    """Backfill search_vector on all license_records rows."""
    print("Backfilling search_vector...")
    await pg.execute("""
        UPDATE license_records lr
        SET search_vector =
            setweight(to_tsvector('english', coalesce(lr.business_name, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(
                (SELECT city FROM locations WHERE id = lr.location_id), ''
            )), 'B') ||
            setweight(to_tsvector('english', coalesce(lr.resolved_endorsements, '')), 'C') ||
            setweight(to_tsvector('english', coalesce(lr.license_type, '')), 'C') ||
            setweight(to_tsvector('english',
                coalesce(lr.applicants, '') || ' ' || coalesce(
                    (SELECT raw_address FROM locations WHERE id = lr.location_id), ''
                )
            ), 'D')
    """)
    print("  search_vector backfill complete.")


async def verify_counts(
    sqlite_conn: sqlite3.Connection,
    pg: asyncpg.Connection,
) -> bool:
    """Compare row counts between SQLite and PostgreSQL. Returns True if all match."""
    print("\nVerifying row counts:")
    all_ok = True
    for table in TABLE_ORDER:
        try:
            sqlite_count = sqlite_conn.execute(  # noqa: S608
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
        except sqlite3.OperationalError:
            sqlite_count = 0
        pg_count = await pg.fetchval(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
        status = "✅" if pg_count >= sqlite_count else "❌"
        print(f"  {status} {table:<40} sqlite={sqlite_count:>8,}  pg={pg_count:>8,}")
        if pg_count < sqlite_count:
            all_ok = False
    return all_ok


async def main(dry_run: bool = False) -> None:
    if not SQLITE_PATH.exists():
        print(f"ERROR: SQLite database not found: {SQLITE_PATH}", file=sys.stderr)
        sys.exit(1)

    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    # asyncpg expects postgresql:// not postgresql+asyncpg://
    pg_url = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

    # Mask credentials in output
    try:
        display_url = pg_url[:pg_url.index("@") + 1] + "..."
    except ValueError:
        display_url = pg_url

    print(f"Source: {SQLITE_PATH}")
    print(f"Target: {display_url}")
    print(f"Dry-run: {dry_run}")
    print()

    sqlite_conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True)
    sqlite_conn.row_factory = sqlite3.Row

    pg = await asyncpg.connect(pg_url)

    try:
        total_inserted = 0

        for table in TABLE_ORDER:
            cols, rows = read_sqlite_table(sqlite_conn, table)
            if not cols:
                print(f"  SKIP {table} (not in SQLite schema)")
                continue

            count = await copy_table(pg, table, cols, rows, dry_run=dry_run)
            total_inserted += count
            if not dry_run:
                print(f"  ✅ {table}: {count:,} rows")

        if not dry_run:
            print("\nResetting identity sequences...")
            await reset_sequences(pg)

            print()
            await backfill_search_vector(pg)

            ok = await verify_counts(sqlite_conn, pg)
            print()
            if ok:
                print("✅ Migration complete — all row counts match.")
            else:
                print("⚠️  Some row counts differ — review above.")
                sys.exit(1)
        else:
            print(f"\n[dry-run] Would insert ~{total_inserted:,} total rows.")

    finally:
        sqlite_conn.close()
        await pg.close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Migrate SQLite → PostgreSQL")
    p.add_argument("--dry-run", action="store_true", help="Parse and count, no writes")
    args = p.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
