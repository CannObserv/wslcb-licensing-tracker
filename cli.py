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


def cmd_check(args):
    """Run database integrity checks."""
    from database import init_db, get_db
    from integrity import run_all_checks, print_report
    init_db()
    with get_db() as conn:
        report = run_all_checks(conn, fix=args.fix)
        issues = print_report(report)
    if issues:
        sys.exit(1)


def cmd_cleanup_redundant(args):
    """Remove data from scrapes that found no new records."""
    from database import get_db, init_db
    from scraper import cleanup_redundant_scrapes

    init_db()
    with get_db() as conn:
        result = cleanup_redundant_scrapes(
            conn, delete_files=not args.keep_files,
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


def cmd_rebuild(args):
    """Rebuild the database from archived sources."""
    import logging
    from pathlib import Path
    from database import DATA_DIR, DB_PATH
    from rebuild import rebuild_from_sources, compare_databases

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
                diff = counts['rebuilt'] - counts['prod']
                sign = '+' if diff > 0 else ''
                print(f"    {sec:<20} prod={counts['prod']:,}  rebuilt={counts['rebuilt']:,}  ({sign}{diff:,})")
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

    args = top.parse_args()
    if not args.command:
        top.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
