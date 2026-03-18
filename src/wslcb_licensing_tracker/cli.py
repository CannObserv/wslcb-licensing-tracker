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
    python cli.py rebuild --output data/wslcb-rebuilt.db           # rebuild from sources
    python cli.py rebuild --output data/wslcb-rebuilt.db --verify  # rebuild and compare

Extracted from ``scraper.py`` as part of the Phase 1 architecture
refactor (#17).
"""

import argparse
import logging
import sys
from pathlib import Path

from .address_validator import backfill_addresses, refresh_addresses, refresh_specific_addresses
from .backfill_diffs import backfill_diffs
from .backfill_provenance import backfill_provenance
from .backfill_snapshots import backfill_from_snapshots
from .db import DATA_DIR, DB_PATH, get_db
from .endorsements import reprocess_endorsements
from .entities import reprocess_entities
from .integrity import print_report, run_all_checks
from .link_records import build_all_links
from .log_config import setup_logging
from .parser import SECTION_DIR_MAP
from .rebuild import compare_databases, rebuild_from_sources
from .schema import init_db
from .scraper import cleanup_redundant_scrapes, scrape


def cmd_scrape(_args: argparse.Namespace) -> None:
    """Run a live scrape of the WSLCB licensing page."""
    scrape()


def cmd_backfill_snapshots(_args: argparse.Namespace) -> None:
    """Ingest records from archived HTML snapshots."""
    backfill_from_snapshots()


def cmd_backfill_diffs(args: argparse.Namespace) -> None:
    """Ingest records from unified-diff archives."""
    backfill_diffs(
        section=args.section,
        single_file=args.file,
        limit=args.limit,
        dry_run=args.dry_run,
    )


def cmd_backfill_provenance(_args: argparse.Namespace) -> None:
    """Populate source provenance for existing records."""
    backfill_provenance()


def cmd_backfill_addresses(args: argparse.Namespace) -> None:
    """Validate un-validated locations via the address API."""
    init_db()
    with get_db() as conn:
        backfill_addresses(conn, rate_limit=args.rate_limit)


def cmd_refresh_addresses(args: argparse.Namespace) -> None:
    """Re-validate locations via the address API.

    By default re-validates all locations.  Pass --location-ids to target only
    a specific set (e.g. IDs extracted from a prior run's lock-failure log).
    """
    init_db()
    with get_db() as conn:
        if args.location_ids:
            with Path(args.location_ids).open() as fh:
                ids = [int(line.strip()) for line in fh if line.strip()]
            refresh_specific_addresses(conn, ids, rate_limit=args.rate_limit)
        else:
            refresh_addresses(conn, rate_limit=args.rate_limit)


def cmd_rebuild_links(_args: argparse.Namespace) -> None:
    """Rebuild all application→outcome links from scratch."""
    init_db()
    with get_db() as conn:
        build_all_links(conn)
        conn.commit()


def cmd_check(args: argparse.Namespace) -> None:
    """Run database integrity checks."""
    init_db()
    with get_db() as conn:
        report = run_all_checks(conn, fix=args.fix)
        issues = print_report(report)
    if issues:
        sys.exit(1)


def cmd_cleanup_redundant(args: argparse.Namespace) -> None:
    """Remove data from scrapes that found no new records."""
    init_db()
    with get_db() as conn:
        result = cleanup_redundant_scrapes(
            conn,
            delete_files=not args.keep_files,
        )
    if result["scrape_logs"] == 0:
        print("Nothing to clean up.")
    else:
        print(
            f"Cleaned {result['scrape_logs']} redundant scrape(s): "
            f"{result['record_sources']} record_sources rows, "
            f"{result['sources']} source rows, "
            f"{result['files']} snapshot files removed."
        )


def cmd_reprocess_endorsements(args: argparse.Namespace) -> None:
    """Regenerate record_endorsements from current code mappings."""
    init_db()
    with get_db() as conn:
        result = reprocess_endorsements(
            conn,
            record_id=args.record_id,
            code=args.code,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            conn.commit()

    if args.dry_run:
        print(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        print(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['endorsements_linked']:,} endorsement link(s) written."
        )


def cmd_reprocess_entities(args: argparse.Namespace) -> None:
    """Regenerate record_entities from current applicants data."""
    init_db()
    with get_db() as conn:
        result = reprocess_entities(
            conn,
            record_id=args.record_id,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            conn.commit()

    if args.dry_run:
        print(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        print(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['entities_linked']:,} entity link(s) written."
        )


def cmd_rebuild(args: argparse.Namespace) -> None:
    """Rebuild the database from archived sources."""
    logger = logging.getLogger(__name__)
    output = Path(args.output)

    result = rebuild_from_sources(
        output_path=output,
        data_dir=DATA_DIR,
        force=args.force,
    )

    print(f"\nRebuilt database: {output}")
    print(f"  Records:      {result.records:,}")
    print(f"  From diffs:   {result.from_diffs:,}")
    print(f"  From snaps:   {result.from_snapshots:,}")
    print(f"  Locations:    {result.locations:,}")
    print(f"  Entities:     {result.entities:,}")
    print(f"  Outcome links: {result.outcome_links:,}")
    print(f"  Endorsements: {result.endorsement_mappings_discovered} new mappings")
    print(f"  Elapsed:      {result.elapsed_seconds:.1f}s")

    if args.verify:
        print(f"\nVerifying against production database: {DB_PATH}")
        if not DB_PATH.exists():
            logger.error("Production database not found: %s", DB_PATH)
            sys.exit(1)
        cmp = compare_databases(DB_PATH, output)
        print(f"  Production records:  {cmp.prod_count:,}")
        print(f"  Rebuilt records:     {cmp.rebuilt_count:,}")
        print(f"  Missing from rebuilt: {cmp.missing_from_rebuilt:,}")
        print(f"  Extra in rebuilt:    {cmp.extra_in_rebuilt:,}")
        if cmp.section_counts:
            print("  Per-section breakdown:")
            for sec, counts in sorted(cmp.section_counts.items()):
                diff = counts["rebuilt"] - counts["prod"]
                sign = "+" if diff > 0 else ""
                print(
                    f"    {sec:<20} prod={counts['prod']:,}"
                    f"  rebuilt={counts['rebuilt']:,}  ({sign}{diff:,})"
                )
        if cmp.sample_missing:
            print("  Sample missing records (in prod, not rebuilt):")
            for key in cmp.sample_missing[:5]:
                print(f"    {key}")
        if cmp.sample_extra:
            print("  Sample extra records (in rebuilt, not prod):")
            for key in cmp.sample_extra[:5]:
                print(f"    {key}")
        if cmp.missing_from_rebuilt > 0 or cmp.extra_in_rebuilt > 0:
            sys.exit(1)
        else:
            print("  \u2705 Databases match!")


# -- Admin subcommands -----------------------------------------------


def cmd_admin_add_user(args: argparse.Namespace) -> None:
    """Add an admin user by email."""
    email = args.email.strip()
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM admin_users WHERE email = ? COLLATE NOCASE", (email,)
        ).fetchone()
        if existing:
            print(f"User already exists: {email}")
            return
        conn.execute(
            "INSERT INTO admin_users (email, created_by) VALUES (?, 'cli')",
            (email,),
        )
        conn.commit()
    print(f"Added admin user: {email}")


def cmd_admin_list_users(_args: argparse.Namespace) -> None:
    """List all admin users."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT email, role, created_at, created_by FROM admin_users ORDER BY created_at"
        ).fetchall()
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
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM admin_users WHERE email = ? COLLATE NOCASE", (email,)
        ).fetchone()
        if not row:
            print(f"User not found: {email}")
            sys.exit(1)
        count: int = conn.execute("SELECT COUNT(*) FROM admin_users").fetchone()[0]
        if count <= 1:
            print("Cannot remove the last admin user.")
            sys.exit(1)
        conn.execute("DELETE FROM admin_users WHERE email = ? COLLATE NOCASE", (email,))
        conn.commit()
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
        help="Parse and export CSV without writing to the database.",
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
