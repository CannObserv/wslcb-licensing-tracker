"""Weekly disk-hygiene job.

Prunes VS Code / npm / uv / pre-commit caches, orphaned worktree
directories, and detects uncompressed data stragglers.

Selection logic (what to prune) is factored into pure functions over
listing + metadata so each rule is independently unit-testable without
touching the real ``~/.vscode-server``. Removal itself is fail-open — one
path failing to delete must never abort the rest of the run.

See docs/plans/2026-07-09-disk-hygiene-design.md.
"""

import gzip
import json
import logging
import re
import shutil
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple

from .pg_db import DATA_DIR, DIFF_GLOB, SNAPSHOT_GLOB

logger = logging.getLogger(__name__)

_EXTENSION_VERSION_RE = re.compile(r"^(.+?)-(\d+\.\d+\.\d+)")

VSIX_MAX_AGE_DAYS = 14
NPX_MAX_AGE_DAYS = 30


def _pgrep_match(pattern: str) -> bool:
    """Return True if any running process's command line matches *pattern*."""
    try:
        # Fixed command; pattern is built from an internal path/dir name, not user input.
        result = subprocess.run(  # noqa: S603
            ["pgrep", "-f", pattern],  # noqa: S607
            capture_output=True,
            check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def is_vscode_server_running(name: str) -> bool:
    """Return True if a process is actively running from cli/servers/<name>/."""
    return _pgrep_match(f"cli/servers/{name}/")


def is_extension_dir_in_use(path: Path) -> bool:
    """Return True if any process has a command line referencing extension dir *path*.

    Guards against pruning an "older" version that a running session hasn't
    reloaded away from yet — a newer version existing on disk doesn't
    guarantee the active process has picked it up.
    """
    return _pgrep_match(str(path))


def _read_lru_keep_list(lru_path: Path) -> list[str] | None:
    """Return the parsed ``lru.json`` list, or None if missing/empty/malformed."""
    try:
        lru = json.loads(lru_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(lru, list) or not lru:
        return None
    return lru


def select_vscode_servers_to_prune(
    servers_dir: Path,
    keep_count: int = 2,
    is_running: Callable[[str], bool] = is_vscode_server_running,
) -> tuple[list[Path], list[str]]:
    """Return ``(dirs_to_remove, warnings)`` for ``~/.vscode-server/cli/servers/*``.

    Keeps the *keep_count* most-recent entries per ``lru.json``'s ordering,
    plus any build with a live process regardless of LRU position. A
    missing or unreadable ``lru.json`` skips this section entirely — an
    empty keep-list must never be treated as "keep nothing", which would
    delete the currently-running build too.
    """
    if not servers_dir.is_dir():
        return [], []

    lru_path = servers_dir / "lru.json"
    lru = _read_lru_keep_list(lru_path)
    if lru is None:
        return [], [f"could not read keep-list from {lru_path} — skipping"]

    keep = set(lru[:keep_count])
    to_remove = [
        entry
        for entry in sorted(servers_dir.iterdir())
        if entry.is_dir() and entry.name not in keep and not is_running(entry.name)
    ]
    return to_remove, []


def select_extensions_to_prune(
    ext_dir: Path,
    is_running: Callable[[Path], bool] = is_extension_dir_in_use,
) -> list[Path]:
    """Keep only the newest (parsed semver) version dir per extension family.

    Dir names look like ``publisher.name-1.2.3`` or
    ``publisher.name-1.2.3-linux-x64``. Dirs not matching that pattern are
    left alone. Mtime is only a tiebreak for equal versions — comparison is
    numeric on the parsed version tuple, not lexical or mtime-only.

    An older version still referenced by a running process (an update on
    disk that the active session hasn't reloaded away from) is never
    pruned, regardless of version order.
    """
    if not ext_dir.is_dir():
        return []

    groups: dict[str, list[tuple[tuple[int, ...], float, Path]]] = {}
    for entry in ext_dir.iterdir():
        if not entry.is_dir():
            continue
        m = _EXTENSION_VERSION_RE.match(entry.name)
        if not m:
            continue
        version = tuple(int(x) for x in m.group(2).split("."))
        groups.setdefault(m.group(1), []).append((version, entry.stat().st_mtime, entry))

    to_remove: list[Path] = []
    for entries in groups.values():
        entries.sort()
        to_remove.extend(path for _, _, path in entries[:-1] if not is_running(path))
    return to_remove


def select_aged_paths(dir_path: Path, max_age_days: int, now: datetime | None = None) -> list[Path]:
    """Return top-level entries under *dir_path* older than *max_age_days*."""
    if not dir_path.is_dir():
        return []
    reference = now or datetime.now(UTC)
    cutoff = reference.timestamp() - max_age_days * 86400
    return [entry for entry in dir_path.iterdir() if entry.stat().st_mtime < cutoff]


def _parse_worktree_list(repo_root: Path) -> tuple[set[Path], str | None]:
    """Return ``(registered worktree paths, warning)`` via ``git worktree list --porcelain``.

    Parses ``worktree <path>`` lines by stripping the fixed prefix rather
    than splitting on whitespace, so paths containing spaces are handled
    correctly (a split-on-whitespace parse would truncate such a path and
    silently invert the orphan-protection check for it).

    A non-zero exit, or output with no ``worktree <path>`` line at all
    (a successful call always lists at least the primary checkout), returns
    an empty set *with a warning*. Callers must not treat that the same as
    "no worktrees registered" — doing so would make every subdirectory look
    orphaned and delete active worktrees, including ones with uncommitted
    work.
    """
    # Fixed command, repo_root is an internal path — not user input.
    result = subprocess.run(  # noqa: S603
        ["git", "-C", str(repo_root), "worktree", "list", "--porcelain"],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return (
            set(),
            f"git worktree list failed (exit {result.returncode}) — skipping orphan sweep",
        )

    prefix = "worktree "
    paths = {
        Path(line[len(prefix) :]) for line in result.stdout.splitlines() if line.startswith(prefix)
    }
    if not paths:
        return set(), "git worktree list returned no entries — skipping orphan sweep"
    return paths, None


def find_orphaned_worktrees(
    repo_root: Path,
    worktree_dirs: list[Path],
    grace_minutes: int = 30,
    now: datetime | None = None,
) -> tuple[list[Path], list[str]]:
    """Return ``(orphaned worktree dirs, warnings)``.

    Excludes anything younger than *grace_minutes* — a directory can exist
    moments before ``git worktree add`` finishes registering it, so sweeping
    too eagerly races worktree creation.
    """
    registered_paths, warning = _parse_worktree_list(repo_root)
    if warning:
        return [], [warning]

    registered = {p.resolve() for p in registered_paths}
    reference = now or datetime.now(UTC)
    cutoff = reference.timestamp() - grace_minutes * 60

    orphaned: list[Path] = []
    for base in worktree_dirs:
        if not base.is_dir():
            continue
        for entry in base.iterdir():
            if not entry.is_dir():
                continue
            if entry.resolve() in registered:
                continue
            if entry.stat().st_mtime >= cutoff:
                continue
            orphaned.append(entry)
    return orphaned, []


def _path_size(path: Path) -> int:
    """Return the total size in bytes of *path* (a file, or a directory tree)."""
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _remove_path(path: Path, *, dry_run: bool) -> int:
    """Remove *path* (file or directory tree), returning bytes freed.

    Fail-open: a removal error is logged as a warning and returns 0 rather
    than raising — one path failing to delete must never abort the rest of
    a hygiene run (address-validator#162 hit this live: a root-owned stray
    under a leaked worktree killed the whole run under ``set -e``).
    """
    if not path.exists():
        return 0

    size = _path_size(path)
    if dry_run:
        logger.info("[dry-run] Would remove %s (%.1f MB)", path, size / 1_048_576)
        return 0

    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    except OSError as exc:
        logger.warning("Could not fully remove %s: %s", path, exc)
        return 0
    return size


def _run_subprocess_step(cmd: list[str], *, dry_run: bool, label: str) -> None:
    """Run *cmd*, logging and swallowing failures rather than raising.

    Dry-run only logs the intended command.
    """
    if dry_run:
        logger.info("[dry-run] Would run: %s", label)
        return
    try:
        # cmd is always an internally-constructed fixed argv, never user input.
        subprocess.run(cmd, capture_output=True, check=True)  # noqa: S603
    except (OSError, subprocess.CalledProcessError) as exc:
        logger.warning("%s failed (non-fatal): %s", label, exc)


def check_usage_threshold(threshold_pct: int = 75, path: str = "/") -> None:
    """Log a warning if the filesystem containing *path* is at/above *threshold_pct*."""
    usage = shutil.disk_usage(path)
    pct = usage.used / usage.total * 100
    if pct >= threshold_pct:
        logger.warning("Root filesystem at %.0f%% (threshold %d%%)", pct, threshold_pct)


class CompressResult(NamedTuple):
    """Summary counters returned by :func:`compress_files`."""

    compressed: int
    skipped: int
    would_unlink: int
    saved_bytes: int


def compress_files(paths: list[Path], dry_run: bool) -> CompressResult:  # noqa: FBT001
    """Gzip each of *paths* to a ``.gz`` sibling, removing the original.

    Cleans up orphaned plain files whose ``.gz`` sibling already exists
    (covers a previously-interrupted run).
    """
    compressed = 0
    skipped = 0
    would_unlink = 0
    saved_bytes = 0

    for path in paths:
        gz_path = path.parent / (path.name + ".gz")
        if gz_path.exists():
            if dry_run:
                would_unlink += 1
            else:
                path.unlink()
            skipped += 1
            continue
        original_size = path.stat().st_size
        if dry_run:
            logger.info("[dry-run] Would compress %s", path.name)
            compressed += 1
            continue
        content = path.read_bytes()
        gz_path.write_bytes(gzip.compress(content))
        compressed_size = gz_path.stat().st_size
        saved_bytes += original_size - compressed_size
        path.unlink()
        compressed += 1

    return CompressResult(compressed, skipped, would_unlink, saved_bytes)


def compress_data_stragglers(*, dry_run: bool) -> dict[str, Any]:
    """Detect and compress any leftover uncompressed snapshot/diff archive files."""
    html_files = sorted(DATA_DIR.glob(SNAPSHOT_GLOB))
    txt_files = sorted(DATA_DIR.glob(DIFF_GLOB))
    return {
        "snapshots": compress_files(html_files, dry_run) if html_files else None,
        "diffs": compress_files(txt_files, dry_run) if txt_files else None,
    }


def _usage_line(path: str = "/") -> str:
    usage = shutil.disk_usage(path)
    pct = usage.used / usage.total * 100
    free_gb = usage.free / 1_073_741_824
    used_gb = usage.used / 1_073_741_824
    return f"{used_gb:.1f}G used, {free_gb:.1f}G free ({pct:.0f}%)"


def _prune_vscode_caches(home: Path, *, dry_run: bool) -> tuple[int, list[str]]:
    """Prune stale server builds, old extension versions, and aged VSIX cache."""
    freed = 0
    warnings: list[str] = []

    servers_dir = home / ".vscode-server" / "cli" / "servers"
    to_remove, server_warnings = select_vscode_servers_to_prune(servers_dir)
    warnings.extend(server_warnings)
    for path in to_remove:
        freed += _remove_path(path, dry_run=dry_run)

    ext_dir = home / ".vscode-server" / "extensions"
    for path in select_extensions_to_prune(ext_dir):
        freed += _remove_path(path, dry_run=dry_run)

    vsix_dir = home / ".vscode-server" / "data" / "CachedExtensionVSIXs"
    for path in select_aged_paths(vsix_dir, max_age_days=VSIX_MAX_AGE_DAYS):
        freed += _remove_path(path, dry_run=dry_run)

    return freed, warnings


def _prune_dev_caches(home: Path, repo_root: Path, *, dry_run: bool) -> int:
    """Prune aged npx sandboxes and shell out to npm/uv/pre-commit cache pruning."""
    freed = 0
    npx_dir = home / ".npm" / "_npx"
    for path in select_aged_paths(npx_dir, max_age_days=NPX_MAX_AGE_DAYS):
        freed += _remove_path(path, dry_run=dry_run)

    if shutil.which("npm"):
        _run_subprocess_step(
            ["npm", "cache", "clean", "--force"], dry_run=dry_run, label="npm cache clean"
        )
    if shutil.which("uv"):
        _run_subprocess_step(["uv", "cache", "prune"], dry_run=dry_run, label="uv cache prune")
    pre_commit = repo_root / ".venv" / "bin" / "pre-commit"
    if pre_commit.exists():
        _run_subprocess_step([str(pre_commit), "gc"], dry_run=dry_run, label="pre-commit gc")

    return freed


def _prune_orphaned_worktrees(repo_root: Path, *, dry_run: bool) -> tuple[int, list[str]]:
    worktree_dirs = [repo_root / ".worktrees", repo_root / ".claude" / "worktrees"]
    orphaned, warnings = find_orphaned_worktrees(repo_root, worktree_dirs)
    freed = 0
    for path in orphaned:
        freed += _remove_path(path, dry_run=dry_run)
    return freed, warnings


def run_disk_hygiene(
    *,
    dry_run: bool,
    home: Path | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """Run the full weekly disk-hygiene sweep.

    Prunes VS Code server/extension/VSIX caches, npm/uv/pre-commit caches,
    and orphaned worktree dirs, then detects and compresses any leftover
    uncompressed data archives. Fail-open throughout — no single section's
    failure aborts the rest of the run.

    *home* and *repo_root* default to ``Path.home()`` and this package's
    repo root; overridable for testing.
    """
    home = home or Path.home()
    repo_root = repo_root or Path(__file__).resolve().parents[2]

    logger.info("disk-hygiene start: %s", _usage_line())

    vscode_freed, warnings = _prune_vscode_caches(home, dry_run=dry_run)
    freed = vscode_freed
    freed += _prune_dev_caches(home, repo_root, dry_run=dry_run)
    worktree_freed, worktree_warnings = _prune_orphaned_worktrees(repo_root, dry_run=dry_run)
    freed += worktree_freed
    warnings.extend(worktree_warnings)

    compress_result = compress_data_stragglers(dry_run=dry_run)
    check_usage_threshold()

    logger.info("disk-hygiene end: %s, freed %.1f MB", _usage_line(), freed / 1_048_576)

    return {"freed_bytes": freed, "warnings": warnings, "compress": compress_result}
