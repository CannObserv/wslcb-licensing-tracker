"""Unified CLI entry point for the WSLCB Licensing Tracker.

All operational commands are exposed as argparse subcommands.  This
replaces the previous pattern of ``python scraper.py --flag`` with
explicit ``python cli.py <subcommand>`` invocations.

Usage::

    python cli.py scrape                  # live scrape
    python cli.py backfill-snapshots      # replay archived HTML
    python cli.py backfill-diffs          # replay diff archives
    python cli.py backfill-provenance     # populate source provenance
    python cli.py backfill-addresses      # validate un-validated locations
    python cli.py refresh-addresses       # re-validate all locations
    python cli.py rebuild-links           # rebuild application→outcome links
    python cli.py check                   # run integrity checks
    python cli.py check --fix             # run checks and auto-fix safe issues

Uses PostgreSQL via SQLAlchemy async engine (Phase 6 migration, #94).
"""

import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .database import create_engine_from_env, get_db
from .log_config import setup_logging
from .models import admin_users
from .parser import SECTION_DIR_MAP
from .pg_address_validator import backfill_addresses as pg_backfill_addresses
from .pg_address_validator import refresh_addresses as pg_refresh_addresses
from .pg_address_validator import refresh_specific_addresses as pg_refresh_specific_addresses
from .pg_backfill_diffs import backfill_diffs as pg_backfill_diffs
from .pg_backfill_snapshots import backfill_from_snapshots as pg_backfill_snapshots
from .pg_endorsements import reprocess_endorsements as pg_reprocess_endorsements
from .pg_entities import reprocess_entities as pg_reprocess_entities
from .pg_integrity import print_report
from .pg_integrity import run_all_checks as pg_run_all_checks
from .pg_link_records import build_all_links as pg_build_all_links
from .pg_scraper import cleanup_redundant_scrapes as pg_cleanup_redundant
from .pg_scraper import scrape as pg_scrape


def cmd_scrape(_args: argparse.Namespace) -> None:
    """Run a live scrape of the WSLCB licensing page."""
    engine = create_engine_from_env()
    asyncio.run(pg_scrape(engine))
    engine.dispose()


def cmd_backfill_snapshots(_args: argparse.Namespace) -> None:
    """Ingest records from archived HTML snapshots."""
    engine = create_engine_from_env()
    asyncio.run(pg_backfill_snapshots(engine))
    engine.dispose()


def cmd_backfill_diffs(args: argparse.Namespace) -> None:
    """Ingest records from unified-diff archives."""
    engine = create_engine_from_env()
    result = asyncio.run(
        pg_backfill_diffs(
            engine,
            section=args.section,
            single_file=args.file,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    )
    engine.dispose()
    if args.dry_run:
        print(
            f"[dry-run] Would insert {result['inserted']:,} record(s)"
            f" from {result['files_processed']:,} file(s)."
        )
    else:
        print(
            f"Processed {result['files_processed']:,} file(s): "
            f"{result['inserted']:,} inserted, {result['skipped']:,} skipped, "
            f"{result['errors']:,} errors."
        )


def cmd_backfill_provenance(_args: argparse.Namespace) -> None:
    """Populate source provenance for existing records.

    Note: provenance is populated at ingest time via pg_pipeline. This command
    is a no-op in the PostgreSQL version — kept for CLI compatibility.
    """
    print("Provenance is populated at ingest time in the PostgreSQL version. No action needed.")


def cmd_backfill_addresses(args: argparse.Namespace) -> None:
    """Validate un-validated locations via the address API."""
    engine = create_engine_from_env()

    async def _run() -> None:
        async with get_db(engine) as conn:
            await pg_backfill_addresses(conn, rate_limit=args.rate_limit)
            await conn.commit()

    asyncio.run(_run())
    engine.dispose()


def cmd_refresh_addresses(args: argparse.Namespace) -> None:
    """Re-validate locations via the address API.

    By default re-validates all locations.  Pass --location-ids to target only
    a specific set (e.g. IDs extracted from a prior run's lock-failure log).
    """
    engine = create_engine_from_env()

    # Read the IDs file synchronously before entering the async context.
    ids: list[int] | None = None
    if args.location_ids:
        with Path(args.location_ids).open() as fh:
            ids = [int(line.strip()) for line in fh if line.strip()]

    async def _run() -> None:
        async with get_db(engine) as conn:
            if ids is not None:
                await pg_refresh_specific_addresses(conn, ids, rate_limit=args.rate_limit)
            else:
                await pg_refresh_addresses(conn, rate_limit=args.rate_limit)
            await conn.commit()

    asyncio.run(_run())
    engine.dispose()


def cmd_rebuild_links(_args: argparse.Namespace) -> None:
    """Rebuild all application→outcome links from scratch."""
    engine = create_engine_from_env()

    async def _run() -> None:
        async with get_db(engine) as conn:
            await pg_build_all_links(conn)
            await conn.commit()

    asyncio.run(_run())
    engine.dispose()


def cmd_check(args: argparse.Namespace) -> None:
    """Run database integrity checks."""
    engine = create_engine_from_env()

    async def _run() -> dict:
        async with get_db(engine) as conn:
            return await pg_run_all_checks(conn, fix=args.fix)

    report = asyncio.run(_run())
    engine.dispose()
    issues = print_report(report)
    if issues:
        sys.exit(1)


def cmd_cleanup_redundant(args: argparse.Namespace) -> None:
    """Remove data from scrapes that found no new records."""
    engine = create_engine_from_env()
    result = asyncio.run(pg_cleanup_redundant(engine, delete_files=not args.keep_files))
    engine.dispose()
    if result["scrape_logs"] == 0:
        print("Nothing to clean up.")
    else:
        print(
            f"Cleaned {result['scrape_logs']} redundant scrape(s): "
            f"{result['files']} snapshot files removed."
        )


def cmd_reprocess_endorsements(args: argparse.Namespace) -> None:
    """Regenerate record_endorsements from current code mappings."""
    engine = create_engine_from_env()

    async def _run() -> dict:
        async with get_db(engine) as conn:
            result = await pg_reprocess_endorsements(
                conn,
                record_id=args.record_id,
                code=args.code,
                dry_run=args.dry_run,
            )
            if not args.dry_run:
                await conn.commit()
            return result

    result = asyncio.run(_run())
    engine.dispose()
    if args.dry_run:
        print(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        print(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['endorsements_linked']:,} endorsement link(s) written."
        )


def cmd_reprocess_entities(args: argparse.Namespace) -> None:
    """Regenerate record_entities from current applicants data."""
    engine = create_engine_from_env()

    async def _run() -> dict:
        async with get_db(engine) as conn:
            result = await pg_reprocess_entities(
                conn,
                record_id=args.record_id,
                dry_run=args.dry_run,
            )
            if not args.dry_run:
                await conn.commit()
            return result

    result = asyncio.run(_run())
    engine.dispose()
    if args.dry_run:
        print(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        print(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['entities_linked']:,} entity link(s) written."
        )


def cmd_rebuild(args: argparse.Namespace) -> None:  # noqa: ARG001
    """Rebuild database from archived sources.

    Note: rebuild.py targets SQLite. This command is not yet ported to PostgreSQL.
    Use ``wslcb backfill-snapshots`` + ``wslcb backfill-diffs`` for PostgreSQL recovery.
    """
    print(
        "ERROR: 'rebuild' is not yet ported to PostgreSQL.\n"
        "Use 'wslcb backfill-snapshots' and 'wslcb backfill-diffs' to repopulate from archives."
    )
    sys.exit(1)


# -- Admin subcommands -----------------------------------------------


def cmd_admin_add_user(args: argparse.Namespace) -> None:
    """Add an admin user by email."""
    email = args.email.strip()
    engine = create_engine_from_env()

    async def _run() -> None:
        async with get_db(engine) as conn:
            existing = (
                await conn.execute(
                    select(admin_users.c.id).where(
                        text("lower(email) = lower(:email)").bindparams(email=email)
                    )
                )
            ).fetchone()
            if existing:
                print(f"User already exists: {email}")
                return
            await conn.execute(pg_insert(admin_users).values(email=email, created_by="cli"))
            await conn.commit()
        print(f"Added admin user: {email}")

    asyncio.run(_run())
    engine.dispose()


def cmd_admin_list_users(_args: argparse.Namespace) -> None:
    """List all admin users."""
    engine = create_engine_from_env()

    async def _run() -> list:
        async with get_db(engine) as conn:
            result = await conn.execute(
                select(
                    admin_users.c.email,
                    admin_users.c.role,
                    admin_users.c.created_at,
                    admin_users.c.created_by,
                ).order_by(admin_users.c.created_at)
            )
            return result.fetchall()

    rows = asyncio.run(_run())
    engine.dispose()
    if not rows:
        print("No admin users.")
        return
    print(f"{'Email':<40} {'Role':<10} {'Created At':<20} {'Created By'}")
    print("-" * 90)
    for email, role, created_at, created_by in rows:
        print(f"{email:<40} {role:<10} {created_at:<20} {created_by}")


def cmd_admin_remove_user(args: argparse.Namespace) -> None:
    """Remove an admin user by email."""
    email = args.email.strip()
    engine = create_engine_from_env()

    async def _run() -> str | None:
        """Return error message string on failure, None on success."""
        async with get_db(engine) as conn:
            row = (
                await conn.execute(
                    select(admin_users.c.id).where(
                        text("lower(email) = lower(:email)").bindparams(email=email)
                    )
                )
            ).fetchone()
            if not row:
                return f"User not found: {email}"
            count = (await conn.execute(select(func.count()).select_from(admin_users))).scalar_one()
            if count <= 1:
                return "Cannot remove the last admin user."
            await conn.execute(
                delete(admin_users).where(
                    text("lower(email) = lower(:email)").bindparams(email=email)
                )
            )
            await conn.commit()
            return None

    error = asyncio.run(_run())
    engine.dispose()
    if error:
        print(error)
        sys.exit(1)
    print(f"Removed admin user: {email}")


def main() -> None:  # noqa: PLR0915 — arg-parser setup requires many statements; genuine refactoring not worthwhile
    """Parse CLI arguments and dispatch to the appropriate subcommand handler."""
    setup_logging()

    top = argparse.ArgumentParser(
        description="WSLCB Licensing Tracker — operational commands.",
    )
    sub = top.add_subparsers(dest="command")

    # scrape
    p = sub.add_parser("scrape", help="Run a live scrape of the WSLCB page")
    p.set_defaults(func=cmd_scrape)

    # backfill-snapshots
    p = sub.add_parser(
        "backfill-snapshots",
        help="Ingest records from archived HTML snapshots",
    )
    p.set_defaults(func=cmd_backfill_snapshots)

    # backfill-diffs
    p = sub.add_parser(
        "backfill-diffs",
        help="Ingest records from unified-diff archives",
    )
    p.add_argument(
        "--section",
        choices=list(SECTION_DIR_MAP.keys()),
        help="Process only this section subdirectory.",
    )
    p.add_argument(
        "--file",
        help="Process a single diff file instead of scanning directories.",
    )
    p.add_argument(
        "--limit",
        type=int,
        help="Process at most N diff files (for validation runs).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and count, no writes to the database.",
    )
    p.set_defaults(func=cmd_backfill_diffs)

    # backfill-provenance
    p = sub.add_parser(
        "backfill-provenance",
        help="Populate source provenance for existing records",
    )
    p.set_defaults(func=cmd_backfill_provenance)

    # backfill-addresses
    p = sub.add_parser(
        "backfill-addresses",
        help="Validate un-validated locations via the address API",
    )
    p.add_argument(
        "--rate-limit",
        type=float,
        default=0.1,
        metavar="SECONDS",
        help="Seconds to sleep between API calls (default: 0.1)",
    )
    p.set_defaults(func=cmd_backfill_addresses)

    # refresh-addresses
    p = sub.add_parser(
        "refresh-addresses",
        help="Re-validate locations via the address API (all, or a specific set)",
    )
    p.add_argument(
        "--rate-limit",
        type=float,
        default=0.1,
        metavar="SECONDS",
        help="Seconds to sleep between API calls (default: 0.1)",
    )
    p.add_argument(
        "--location-ids",
        metavar="FILE",
        default=None,
        help="Path to a file of newline-separated location IDs to re-validate "
        "(default: re-validate all locations)",
    )
    p.set_defaults(func=cmd_refresh_addresses)

    # rebuild-links
    p = sub.add_parser(
        "rebuild-links",
        help="Rebuild all application→outcome links",
    )
    p.set_defaults(func=cmd_rebuild_links)

    # check
    p = sub.add_parser(
        "check",
        help="Run database integrity checks",
    )
    p.add_argument(
        "--fix",
        action="store_true",
        help="Auto-fix safe issues (orphan cleanup, re-run enrichments)",
    )
    p.set_defaults(func=cmd_check)

    # cleanup-redundant
    p = sub.add_parser(
        "cleanup-redundant",
        help="Remove data from scrapes that found no new records",
    )
    p.add_argument(
        "--keep-files",
        action="store_true",
        help="Don't delete snapshot files from disk",
    )
    p.set_defaults(func=cmd_cleanup_redundant)

    # reprocess-endorsements
    p = sub.add_parser(
        "reprocess-endorsements",
        help="Regenerate record_endorsements from current code mappings",
    )
    p.add_argument(
        "--record-id",
        type=int,
        default=None,
        dest="record_id",
        help="Only reprocess this single record ID",
    )
    p.add_argument(
        "--code",
        default=None,
        help="Only reprocess records with this numeric license-type code (e.g. '394')",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing to the database",
    )
    p.set_defaults(func=cmd_reprocess_endorsements)

    # reprocess-entities
    p = sub.add_parser(
        "reprocess-entities",
        help="Regenerate record_entities from current applicants data",
    )
    p.add_argument(
        "--record-id",
        type=int,
        default=None,
        dest="record_id",
        help="Only reprocess this single record ID",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing to the database",
    )
    p.set_defaults(func=cmd_reprocess_entities)

    # rebuild
    p = sub.add_parser(
        "rebuild",
        help="Rebuild database from archived sources",
    )
    p.add_argument(
        "--output",
        required=True,
        help="Path for the rebuilt database file",
    )
    p.add_argument(
        "--verify",
        action="store_true",
        help="Compare rebuilt DB against production and report differences",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output file",
    )
    p.set_defaults(func=cmd_rebuild)

    # admin subcommand group
    p_admin = sub.add_parser("admin", help="Admin user management")
    admin_sub = p_admin.add_subparsers(dest="admin_command")

    p_add = admin_sub.add_parser("add-user", help="Add an admin user")
    p_add.add_argument("email", help="Email address to add")
    p_add.set_defaults(func=cmd_admin_add_user)

    p_list = admin_sub.add_parser("list-users", help="List all admin users")
    p_list.set_defaults(func=cmd_admin_list_users)

    p_rm = admin_sub.add_parser("remove-user", help="Remove an admin user")
    p_rm.add_argument("email", help="Email address to remove")
    p_rm.set_defaults(func=cmd_admin_remove_user)

    args = top.parse_args()
    if not args.command:
        top.print_help()
        sys.exit(1)

    if args.command == "admin" and not getattr(args, "admin_command", None):
        p_admin.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
