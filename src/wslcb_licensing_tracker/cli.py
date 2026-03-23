"""Unified CLI entry point for the WSLCB Licensing Tracker.

All operational commands are exposed as click subcommands grouped by domain:

- ``ingest``: scrape, backfill-snapshots, backfill-diffs, backfill-addresses, refresh-addresses
- ``db``: check, rebuild-links, cleanup-redundant, reprocess-endorsements, reprocess-entities
- ``admin``: add-user, list-users, remove-user

Usage::

    wslcb ingest scrape                # live scrape
    wslcb ingest backfill-snapshots    # replay archived HTML
    wslcb ingest backfill-diffs        # replay diff archives
    wslcb ingest backfill-addresses    # validate un-validated locations
    wslcb ingest refresh-addresses     # re-validate all locations
    wslcb db rebuild-links             # rebuild application→outcome links
    wslcb db check                     # run integrity checks
    wslcb db check --fix               # run checks and auto-fix safe issues
    wslcb admin add-user EMAIL         # add admin user
    wslcb admin list-users             # list admin users
    wslcb admin remove-user EMAIL      # remove admin user

Uses PostgreSQL via SQLAlchemy async engine (Phase 6 migration, #94).
"""

import asyncio
import logging
import sys
from pathlib import Path

import click
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

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Top-level group
# ---------------------------------------------------------------------------


@click.group(invoke_without_command=True)
@click.pass_context
def main(ctx: click.Context) -> None:
    """WSLCB Licensing Tracker — operational commands."""
    setup_logging()
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# ---------------------------------------------------------------------------
# ingest group
# ---------------------------------------------------------------------------


@main.group()
def ingest() -> None:
    """Data ingestion commands (scrape, backfill)."""


@ingest.command()
@click.option(
    "--rate-limit",
    type=float,
    default=0.1,
    show_default=True,
    help="Seconds between address API calls.",
)
def scrape(rate_limit: float) -> None:
    """Run a live scrape of the WSLCB page."""
    engine = create_engine_from_env()
    asyncio.run(pg_scrape(engine))

    # Post-scrape: standardize any new locations via the address API.
    # Failure here is non-fatal — the weekly timer catches stragglers.
    try:

        async def _backfill() -> None:
            async with get_db(engine) as conn:
                await pg_backfill_addresses(conn, rate_limit=rate_limit)
                await conn.commit()

        asyncio.run(_backfill())
    except Exception:  # noqa: BLE001 — intentionally broad; backfill failure is non-fatal
        logger.warning("Post-scrape address backfill failed", exc_info=True)

    engine.dispose()


@ingest.command("backfill-snapshots")
def backfill_snapshots() -> None:
    """Ingest records from archived HTML snapshots."""
    engine = create_engine_from_env()
    asyncio.run(pg_backfill_snapshots(engine))
    engine.dispose()


@ingest.command("backfill-diffs")
@click.option(
    "--section",
    type=click.Choice(list(SECTION_DIR_MAP.keys())),
    default=None,
    help="Process only this section subdirectory.",
)
@click.option("--file", "single_file", default=None, help="Process a single diff file.")
@click.option("--limit", type=int, default=None, help="Process at most N diff files.")
@click.option("--dry-run", is_flag=True, help="Parse and count, no writes.")
def backfill_diffs(
    section: str | None,
    single_file: str | None,
    limit: int | None,
    dry_run: bool,
) -> None:
    """Ingest records from unified-diff archives."""
    engine = create_engine_from_env()
    result = asyncio.run(
        pg_backfill_diffs(
            engine,
            section=section,
            single_file=single_file,
            limit=limit,
            dry_run=dry_run,
        )
    )
    engine.dispose()
    if dry_run:
        click.echo(
            f"[dry-run] Would insert {result['inserted']:,} record(s)"
            f" from {result['files_processed']:,} file(s)."
        )
    else:
        click.echo(
            f"Processed {result['files_processed']:,} file(s): "
            f"{result['inserted']:,} inserted, {result['skipped']:,} skipped, "
            f"{result['errors']:,} errors."
        )


@ingest.command("backfill-addresses")
@click.option(
    "--rate-limit",
    type=float,
    default=0.1,
    show_default=True,
    help="Seconds between API calls.",
)
def backfill_addresses(rate_limit: float) -> None:
    """Validate un-validated locations via the address API."""
    engine = create_engine_from_env()

    async def _run() -> None:
        async with get_db(engine) as conn:
            await pg_backfill_addresses(conn, rate_limit=rate_limit)
            await conn.commit()

    asyncio.run(_run())
    engine.dispose()


@ingest.command("refresh-addresses")
@click.option(
    "--rate-limit",
    type=float,
    default=0.1,
    show_default=True,
    help="Seconds between API calls.",
)
@click.option(
    "--location-ids",
    default=None,
    type=click.Path(exists=True),
    help="File of newline-separated location IDs to re-validate.",
)
def refresh_addresses(rate_limit: float, location_ids: str | None) -> None:
    """Re-validate locations via the address API."""
    engine = create_engine_from_env()

    ids: list[int] | None = None
    if location_ids:
        with Path(location_ids).open() as fh:
            ids = [int(line.strip()) for line in fh if line.strip()]

    async def _run() -> None:
        async with get_db(engine) as conn:
            if ids is not None:
                await pg_refresh_specific_addresses(conn, ids, rate_limit=rate_limit)
            else:
                await pg_refresh_addresses(conn, rate_limit=rate_limit)
            await conn.commit()

    asyncio.run(_run())
    engine.dispose()


# ---------------------------------------------------------------------------
# db group
# ---------------------------------------------------------------------------


@main.group()
def db() -> None:
    """Database maintenance commands."""


@db.command()
@click.option("--fix", is_flag=True, help="Auto-fix safe issues.")
def check(fix: bool) -> None:
    """Run database integrity checks."""
    engine = create_engine_from_env()

    async def _run() -> dict:
        async with get_db(engine) as conn:
            return await pg_run_all_checks(conn, fix=fix)

    report = asyncio.run(_run())
    engine.dispose()
    issues = print_report(report)
    if issues:
        sys.exit(1)


@db.command("rebuild-links")
def rebuild_links() -> None:
    """Rebuild all application-outcome links."""
    engine = create_engine_from_env()

    async def _run() -> None:
        async with get_db(engine) as conn:
            await pg_build_all_links(conn)
            await conn.commit()

    asyncio.run(_run())
    engine.dispose()


@db.command("cleanup-redundant")
@click.option("--keep-files", is_flag=True, help="Don't delete snapshot files from disk.")
def cleanup_redundant(keep_files: bool) -> None:
    """Remove data from scrapes that found no new records."""
    engine = create_engine_from_env()
    result = asyncio.run(pg_cleanup_redundant(engine, delete_files=not keep_files))
    engine.dispose()
    if result["scrape_logs"] == 0:
        click.echo("Nothing to clean up.")
    else:
        click.echo(
            f"Cleaned {result['scrape_logs']} redundant scrape(s): "
            f"{result['files']} snapshot files removed."
        )


@db.command("reprocess-endorsements")
@click.option("--record-id", type=int, default=None, help="Only reprocess this record ID.")
@click.option("--code", default=None, help="Only reprocess records with this license-type code.")
@click.option("--dry-run", is_flag=True, help="Report what would change without writing.")
def reprocess_endorsements(record_id: int | None, code: str | None, dry_run: bool) -> None:
    """Regenerate record_endorsements from current code mappings."""
    engine = create_engine_from_env()

    async def _run() -> dict:
        async with get_db(engine) as conn:
            result = await pg_reprocess_endorsements(
                conn,
                record_id=record_id,
                code=code,
                dry_run=dry_run,
            )
            if not dry_run:
                await conn.commit()
            return result

    result = asyncio.run(_run())
    engine.dispose()
    if dry_run:
        click.echo(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        click.echo(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['endorsements_linked']:,} endorsement link(s) written."
        )


@db.command("reprocess-entities")
@click.option("--record-id", type=int, default=None, help="Only reprocess this record ID.")
@click.option("--dry-run", is_flag=True, help="Report what would change without writing.")
def reprocess_entities(record_id: int | None, dry_run: bool) -> None:
    """Regenerate record_entities from current applicants data."""
    engine = create_engine_from_env()

    async def _run() -> dict:
        async with get_db(engine) as conn:
            result = await pg_reprocess_entities(
                conn,
                record_id=record_id,
                dry_run=dry_run,
            )
            if not dry_run:
                await conn.commit()
            return result

    result = asyncio.run(_run())
    engine.dispose()
    if dry_run:
        click.echo(f"[dry-run] Would process {result['records_processed']:,} record(s).")
    else:
        click.echo(
            f"Reprocessed {result['records_processed']:,} record(s); "
            f"{result['entities_linked']:,} entity link(s) written."
        )


# ---------------------------------------------------------------------------
# admin group
# ---------------------------------------------------------------------------


@main.group()
def admin() -> None:
    """Admin user management."""


@admin.command("add-user")
@click.argument("email")
def admin_add_user(email: str) -> None:
    """Add an admin user by email."""
    email = email.strip()
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
                click.echo(f"User already exists: {email}")
                return
            await conn.execute(pg_insert(admin_users).values(email=email, created_by="cli"))
            await conn.commit()
        click.echo(f"Added admin user: {email}")

    asyncio.run(_run())
    engine.dispose()


@admin.command("list-users")
def admin_list_users() -> None:
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
        click.echo("No admin users.")
        return
    click.echo(f"{'Email':<40} {'Role':<10} {'Created At':<20} {'Created By'}")
    click.echo("-" * 90)
    for email, role, created_at, created_by in rows:
        click.echo(f"{email:<40} {role:<10} {created_at:<20} {created_by}")


@admin.command("remove-user")
@click.argument("email")
def admin_remove_user(email: str) -> None:
    """Remove an admin user by email."""
    email = email.strip()
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
        click.echo(error)
        sys.exit(1)
    click.echo(f"Removed admin user: {email}")


if __name__ == "__main__":
    main()
