"""Unified CLI entry point for the WSLCB Licensing Tracker.

All operational commands are exposed as click subcommands grouped by domain:

- ``ingest``: scrape, backfill-snapshots, backfill-diffs, backfill-addresses, refresh-addresses,
  compress-snapshots, compress-diffs
- ``db``: check, rebuild-links, cleanup-redundant, reprocess-endorsements, reprocess-entities
- ``admin``: add-user, list-users, remove-user
- ``ops``: disk-hygiene

Top-level aliases exist for backward compatibility with the systemd
``wslcb-task@%i`` template (which passes a single token).  Both forms
work: ``wslcb scrape`` and ``wslcb ingest scrape``.

Usage::

    wslcb scrape                       # top-level alias (systemd compat)
    wslcb ingest scrape                # grouped form
    wslcb ingest backfill-snapshots    # replay archived HTML
    wslcb ingest backfill-diffs        # replay diff archives
    wslcb ingest backfill-addresses    # validate un-validated locations
    wslcb ingest refresh-addresses     # re-validate all locations
    wslcb ingest compress-snapshots    # compress .html snapshots to .html.gz in place
    wslcb ingest compress-diffs        # compress diff archive .txt files to .txt.gz in place
    wslcb db rebuild-links             # rebuild application→outcome links
    wslcb db check                     # run integrity checks
    wslcb db check --fix               # run checks and auto-fix safe issues
    wslcb admin add-user EMAIL         # add admin user
    wslcb admin list-users             # list admin users
    wslcb admin remove-user EMAIL      # remove admin user
    wslcb ops disk-hygiene [--dry-run] # weekly cache/worktree/data-straggler cleanup (#138)

Uses PostgreSQL via SQLAlchemy async engine (Phase 6 migration, #94).
"""

import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import click
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from .address_validator import backfill_addresses as run_backfill_addresses
from .address_validator import refresh_addresses as run_refresh_addresses
from .address_validator import refresh_specific_addresses as run_refresh_specific_addresses
from .admin_audit import log_action
from .backfill_diffs import backfill_diffs as run_backfill_diffs
from .backfill_snapshots import backfill_from_snapshots as run_backfill_snapshots
from .db import DATA_DIR, DIFF_GLOB, SNAPSHOT_GLOB
from .disk_hygiene import CompressResult, compress_files, run_disk_hygiene
from .endorsements import reprocess_endorsements as run_reprocess_endorsements
from .engine import create_engine_from_env, get_db
from .entities import reprocess_entities as run_reprocess_entities
from .integrity import print_report
from .integrity import run_all_checks as run_integrity_checks
from .link_records import build_all_links as run_build_all_links
from .log_config import setup_logging
from .models import admin_users
from .parser import SECTION_DIR_MAP
from .scraper import cleanup_redundant_scrapes as run_cleanup_redundant
from .scraper import scrape as run_scrape

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_with_engine[T](coro_fn: Callable[[AsyncEngine], Awaitable[T]]) -> T:
    """Create an async engine, run *coro_fn(engine)*, dispose, return result."""
    engine = create_engine_from_env()

    async def _go() -> T:
        try:
            return await coro_fn(engine)
        finally:
            await engine.dispose()

    return asyncio.run(_go())


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
    default=0.2,
    show_default=True,
    help="Seconds between address API calls.",
)
def scrape(rate_limit: float) -> None:
    """Run a live scrape of the WSLCB page."""

    async def _run(engine: AsyncEngine) -> None:
        await run_scrape(engine)
        # Post-scrape: standardize any new locations via the address API.
        # Failure here is non-fatal — the weekly timer catches stragglers.
        try:
            async with get_db(engine) as conn:
                await run_backfill_addresses(conn, rate_limit=rate_limit)
        except Exception:  # noqa: BLE001 — intentionally broad; backfill failure is non-fatal
            logger.warning("Post-scrape address backfill failed", exc_info=True)

    _run_with_engine(_run)


@ingest.command("backfill-snapshots")
def backfill_snapshots() -> None:
    """Ingest records from archived HTML snapshots."""
    _run_with_engine(run_backfill_snapshots)


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
    result = _run_with_engine(
        lambda engine: run_backfill_diffs(
            engine,
            section=section,
            single_file=single_file,
            limit=limit,
            dry_run=dry_run,
        )
    )
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
    default=0.2,
    show_default=True,
    help="Seconds between API calls.",
)
def backfill_addresses(rate_limit: float) -> None:
    """Validate un-validated locations via the address API."""

    async def _run(engine: AsyncEngine) -> None:
        async with get_db(engine) as conn:
            await run_backfill_addresses(conn, rate_limit=rate_limit)

    _run_with_engine(_run)


@ingest.command("refresh-addresses")
@click.option(
    "--rate-limit",
    type=float,
    default=0.2,
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
    ids: list[int] | None = None
    if location_ids:
        with Path(location_ids).open() as fh:
            ids = [int(line.strip()) for line in fh if line.strip()]

    async def _run(engine: AsyncEngine) -> None:
        async with get_db(engine) as conn:
            if ids is not None:
                await run_refresh_specific_addresses(conn, ids, rate_limit=rate_limit)
            else:
                await run_refresh_addresses(conn, rate_limit=rate_limit)

    _run_with_engine(_run)


def _report_compress_result(
    noun: str, orphan_ext: str, result: CompressResult, dry_run: bool
) -> None:
    """Echo a summary line for a compress-* command, matching the compress-snapshots format."""
    if dry_run:
        summary = (
            f"[dry-run] Would compress {result.compressed} file(s), "
            f"{result.skipped} already compressed."
        )
        if result.would_unlink:
            summary += f" {result.would_unlink} orphaned .{orphan_ext} would be removed."
        click.echo(summary)
    else:
        click.echo(
            f"Compressed {result.compressed} {noun}(s), {result.skipped} already compressed. "
            f"Freed {result.saved_bytes / 1_048_576:.1f} MB."
        )


@ingest.command("compress-snapshots")
@click.option("--dry-run", is_flag=True, help="Report what would be compressed without writing.")
def compress_snapshots(dry_run: bool) -> None:
    """Compress existing .html snapshots to .html.gz in place.

    One-time migration: converts all uncompressed HTML snapshots under
    data/wslcb/licensinginfo/ to gzip-compressed .html.gz files, then
    removes the originals.  Safe to re-run — already-compressed files are
    skipped.
    """
    html_files = sorted(DATA_DIR.glob(SNAPSHOT_GLOB))
    if not html_files:
        click.echo("No uncompressed snapshots found.")
        return

    result = compress_files(html_files, dry_run)
    _report_compress_result("snapshot", "html", result, dry_run)


@ingest.command("compress-diffs")
@click.option("--dry-run", is_flag=True, help="Report what would be compressed without writing.")
def compress_diffs(dry_run: bool) -> None:
    """Compress existing diff archive .txt files to .txt.gz in place.

    One-time migration: converts all uncompressed diff files under
    data/wslcb/licensinginfo-diffs/ to gzip-compressed .txt.gz files, then
    removes the originals.  Safe to re-run — already-compressed files are
    skipped.  See #137.
    """
    txt_files = sorted(DATA_DIR.glob(DIFF_GLOB))
    if not txt_files:
        click.echo("No uncompressed diffs found.")
        return

    result = compress_files(txt_files, dry_run)
    _report_compress_result("diff", "txt", result, dry_run)


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

    async def _run(engine: AsyncEngine) -> dict:
        async with get_db(engine) as conn:
            return await run_integrity_checks(conn, fix=fix)

    report = _run_with_engine(_run)
    issues = print_report(report)
    if issues:
        sys.exit(1)


@db.command("rebuild-links")
def rebuild_links() -> None:
    """Rebuild all application-outcome links."""

    async def _run(engine: AsyncEngine) -> None:
        async with get_db(engine) as conn:
            await run_build_all_links(conn)
            await conn.commit()

    _run_with_engine(_run)


@db.command("cleanup-redundant")
@click.option("--keep-files", is_flag=True, help="Don't delete snapshot files from disk.")
def cleanup_redundant(keep_files: bool) -> None:
    """Remove data from scrapes that found no new records."""
    result = _run_with_engine(
        lambda engine: run_cleanup_redundant(engine, delete_files=not keep_files)
    )
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

    async def _run(engine: AsyncEngine) -> dict:
        async with get_db(engine) as conn:
            result = await run_reprocess_endorsements(
                conn,
                record_id=record_id,
                code=code,
                dry_run=dry_run,
            )
            if not dry_run:
                await conn.commit()
            return result

    result = _run_with_engine(_run)
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

    async def _run(engine: AsyncEngine) -> dict:
        async with get_db(engine) as conn:
            result = await run_reprocess_entities(
                conn,
                record_id=record_id,
                dry_run=dry_run,
            )
            if not dry_run:
                await conn.commit()
            return result

    result = _run_with_engine(_run)
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
    email = email.strip().lower()

    async def _run(engine: AsyncEngine) -> None:
        async with get_db(engine) as conn:
            existing = (
                await conn.execute(
                    select(admin_users.c.id).where(func.lower(admin_users.c.email) == email)
                )
            ).fetchone()
            if existing:
                click.echo(f"User already exists: {email}")
                return
            result = await conn.execute(
                pg_insert(admin_users)
                .values(email=email, created_by="cli")
                .returning(admin_users.c.id)
            )
            new_id = result.scalar_one()
            await log_action(
                conn,
                email="cli",
                action="admin_user.add",
                target_type="admin_user",
                target_id=new_id,
                details={"added_email": email},
            )
            await conn.commit()
        click.echo(f"Added admin user: {email}")

    _run_with_engine(_run)


@admin.command("list-users")
def admin_list_users() -> None:
    """List all admin users."""

    async def _run(engine: AsyncEngine) -> list:
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

    rows = _run_with_engine(_run)
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
    email = email.strip().lower()

    async def _run(engine: AsyncEngine) -> str | None:
        """Return error message string on failure, None on success."""
        async with get_db(engine) as conn:
            row = (
                await conn.execute(
                    select(admin_users.c.id).where(func.lower(admin_users.c.email) == email)
                )
            ).fetchone()
            if not row:
                return f"User not found: {email}"
            count = (await conn.execute(select(func.count()).select_from(admin_users))).scalar_one()
            if count <= 1:
                return "Cannot remove the last admin user."
            await conn.execute(delete(admin_users).where(func.lower(admin_users.c.email) == email))
            await log_action(
                conn,
                email="cli",
                action="admin_user.remove",
                target_type="admin_user",
                target_id=row[0],
                details={"removed_email": email},
            )
            await conn.commit()
            return None

    error = _run_with_engine(_run)
    if error:
        click.echo(error)
        sys.exit(1)
    click.echo(f"Removed admin user: {email}")


# ---------------------------------------------------------------------------
# ops group
# ---------------------------------------------------------------------------


@main.group()
def ops() -> None:
    """Operational maintenance commands (disk hygiene, etc.)."""


@ops.command("disk-hygiene")
@click.option("--dry-run", is_flag=True, help="Report what would be pruned without deleting.")
def disk_hygiene(dry_run: bool) -> None:
    """Prune VS Code/npm/uv/pre-commit caches, orphaned worktrees, and data stragglers.

    Weekly scheduled maintenance job (see #138) — safe to run manually.
    """
    summary = run_disk_hygiene(dry_run=dry_run)
    for warning in summary["warnings"]:
        click.echo(f"WARNING: {warning}")
    verb = "Would free" if dry_run else "Freed"
    click.echo(f"{verb} {summary['freed_bytes'] / 1_048_576:.1f} MB.")


# ---------------------------------------------------------------------------
# Top-level aliases — backward compatibility with systemd wslcb-task@%i
# ---------------------------------------------------------------------------
# The systemd template unit passes %i (instance name) as a single CLI
# argument, e.g. wslcb-task@scrape → ``cli scrape``.  These aliases keep
# flat invocations working alongside the grouped form.

_ALIASES = [
    (scrape, "scrape"),
    (backfill_snapshots, "backfill-snapshots"),
    (backfill_diffs, "backfill-diffs"),
    (backfill_addresses, "backfill-addresses"),
    (refresh_addresses, "refresh-addresses"),
    (check, "check"),
    (rebuild_links, "rebuild-links"),
    (cleanup_redundant, "cleanup-redundant"),
    (reprocess_endorsements, "reprocess-endorsements"),
    (reprocess_entities, "reprocess-entities"),
    (disk_hygiene, "disk-hygiene"),
]
for _cmd, _name in _ALIASES:
    _alias = click.Command(
        _name,
        callback=_cmd.callback,
        params=list(_cmd.params),
        help=_cmd.help,
        hidden=True,
    )
    main.add_command(_alias)


if __name__ == "__main__":
    main()
