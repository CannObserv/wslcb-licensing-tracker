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

Extracted from ``scraper.py`` as part of the Phase 1 architecture
refactor (#17).
"""
import argparse
import sys

from log_config import setup_logging


def cmd_scrape(args):
    """Run a live scrape of the WSLCB licensing page."""
    from scraper import scrape
    scrape()


def cmd_backfill_snapshots(args):
    """Ingest records from archived HTML snapshots."""
    from backfill_snapshots import backfill_from_snapshots
    backfill_from_snapshots()


def cmd_backfill_diffs(args):
    """Ingest records from unified-diff archives."""
    from backfill_diffs import backfill_diffs
    from parser import SECTION_DIR_MAP

    backfill_diffs(
        section=args.section,
        single_file=args.file,
        limit=args.limit,
        dry_run=args.dry_run,
    )


def cmd_backfill_provenance(args):
    """Populate source provenance for existing records."""
    from backfill_provenance import backfill_provenance
    backfill_provenance()


def cmd_backfill_addresses(args):
    """Validate un-validated locations via the address API."""
    from database import init_db, get_db
    from address_validator import backfill_addresses
    init_db()
    with get_db() as conn:
        backfill_addresses(conn)


def cmd_refresh_addresses(args):
    """Re-validate all locations via the address API."""
    from database import init_db, get_db
    from address_validator import refresh_addresses
    init_db()
    with get_db() as conn:
        refresh_addresses(conn)


def cmd_rebuild_links(args):
    """Rebuild all application→outcome links from scratch."""
    from database import init_db, get_db
    from link_records import build_all_links
    init_db()
    with get_db() as conn:
        build_all_links(conn)


def main():
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
    from parser import SECTION_DIR_MAP
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
    p.set_defaults(func=cmd_backfill_addresses)

    # refresh-addresses
    p = sub.add_parser(
        "refresh-addresses",
        help="Re-validate all locations via the address API",
    )
    p.set_defaults(func=cmd_refresh_addresses)

    # rebuild-links
    p = sub.add_parser(
        "rebuild-links",
        help="Rebuild all application→outcome links",
    )
    p.set_defaults(func=cmd_rebuild_links)

    args = top.parse_args()
    if not args.command:
        top.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
