# Post-Scrape Address Standardization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run `backfill-addresses` automatically after every scrape, add a weekly systemd timer as safety net, and remove the unused `validate_addresses` option from `IngestOptions`.

**Architecture:** Post-scrape address standardization is handled in the CLI layer (`cmd_scrape`), not the pipeline. A new systemd timer reuses the existing `wslcb-task@` template. The unused `validate_addresses` field is removed from `IngestOptions` and all call sites.

**Tech Stack:** Python 3.12, asyncio, argparse, systemd timers, pytest

**Worktree:** `.worktrees/113-post-scrape-address` (branch `113-post-scrape-address`)

---

### Task 1: Remove `validate_addresses` from `IngestOptions`

**Files:**
- Modify: `src/wslcb_licensing_tracker/pg_pipeline.py:28-35` — remove field from dataclass
- Modify: `src/wslcb_licensing_tracker/pg_scraper.py:156` — remove kwarg from `IngestOptions()`
- Modify: `src/wslcb_licensing_tracker/pg_backfill_snapshots.py:138` — remove kwarg
- Modify: `src/wslcb_licensing_tracker/pg_backfill_diffs.py:98` — remove kwarg
- Modify: `tests/test_pg_pipeline.py` — remove kwarg from all `IngestOptions()` calls

- [ ] **Step 1: Remove `validate_addresses` field from `IngestOptions` dataclass**

In `src/wslcb_licensing_tracker/pg_pipeline.py`, remove line 31 (`validate_addresses: bool = True`) from the `IngestOptions` dataclass. Update the module docstring (line 6) to remove the "Address validation is STUBBED" note. Keep the stub comment at line 277 and the `STEP_ADDRESS` constant — these remain for potential future use per the design spec.

- [ ] **Step 2: Remove `validate_addresses=` kwarg from all callers**

Four files reference `validate_addresses=`:
- `src/wslcb_licensing_tracker/pg_scraper.py:156` — remove `validate_addresses=True,`
- `src/wslcb_licensing_tracker/pg_backfill_snapshots.py:138` — remove `validate_addresses=False,`
- `src/wslcb_licensing_tracker/pg_backfill_diffs.py:98` — remove `validate_addresses=False,`

Also remove any comments about deferred address validation near these call sites if they reference the removed option.

- [ ] **Step 3: Remove `validate_addresses=False` from all test call sites**

In `tests/test_pg_pipeline.py`, remove `validate_addresses=False` from every `IngestOptions()` constructor call. There are 7 occurrences (lines 157, 168, 191, 211, 250, 287, 320). Each becomes e.g. `IngestOptions(link_outcomes=False)` or `IngestOptions(link_outcomes=False, source_id=source_id)`.

- [ ] **Step 4: Run tests to verify nothing breaks**

Run: `cd .worktrees/113-post-scrape-address && uv run pytest tests/ -v`
Expected: All 277 tests pass, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_pipeline.py \
        src/wslcb_licensing_tracker/pg_scraper.py \
        src/wslcb_licensing_tracker/pg_backfill_snapshots.py \
        src/wslcb_licensing_tracker/pg_backfill_diffs.py \
        tests/test_pg_pipeline.py
git commit -m "#113 refactor: remove unused validate_addresses from IngestOptions"
```

---

### Task 2: Chain `backfill-addresses` after scrape in CLI

**Files:**
- Modify: `src/wslcb_licensing_tracker/cli.py:47-51` — rewrite `cmd_scrape` to call backfill after scrape
- Modify: `src/wslcb_licensing_tracker/cli.py:322-323` — add `--rate-limit` arg to scrape subparser

- [ ] **Step 1: Write the failing test**

Create `tests/test_cli_scrape.py`. The test mocks `pg_scrape` and `pg_backfill_addresses` and verifies both are called when `cmd_scrape` runs:

```python
"""Tests for cmd_scrape post-scrape address backfill."""

import argparse
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from wslcb_licensing_tracker.cli import cmd_scrape


def _mock_get_db(*_args, **_kwargs):
    """Return an async context manager yielding a mock connection."""
    @asynccontextmanager
    async def _ctx(*_a, **_kw):
        conn = AsyncMock()
        yield conn
    return _ctx()


class TestCmdScrapeBackfill:
    """cmd_scrape calls backfill_addresses after scraping."""

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_called_after_scrape(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """backfill_addresses is called after a successful scrape."""
        mock_engine = mock_engine_factory.return_value
        args = argparse.Namespace(rate_limit=0.1)
        cmd_scrape(args)
        mock_scrape.assert_called_once()
        mock_backfill.assert_called_once()
        mock_engine.dispose.assert_called_once()

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_uses_rate_limit(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """backfill_addresses receives the rate_limit from args."""
        args = argparse.Namespace(rate_limit=0.5)
        cmd_scrape(args)
        mock_backfill.assert_called_once()
        assert mock_backfill.call_args.kwargs["rate_limit"] == 0.5

    @patch("wslcb_licensing_tracker.cli.get_db", side_effect=_mock_get_db)
    @patch("wslcb_licensing_tracker.cli.pg_backfill_addresses", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.pg_scrape", new_callable=AsyncMock)
    @patch("wslcb_licensing_tracker.cli.create_engine_from_env")
    def test_backfill_failure_does_not_raise(
        self, mock_engine_factory, mock_scrape, mock_backfill, mock_get_db
    ):
        """If backfill_addresses raises, cmd_scrape still completes."""
        mock_backfill.side_effect = Exception("API down")
        args = argparse.Namespace(rate_limit=0.1)
        # Should not raise
        cmd_scrape(args)
        mock_scrape.assert_called_once()
        mock_engine_factory.return_value.dispose.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd .worktrees/113-post-scrape-address && uv run pytest tests/test_cli_scrape.py -v`
Expected: FAIL — `cmd_scrape` doesn't accept `rate_limit` arg yet, and doesn't call `pg_backfill_addresses`.

- [ ] **Step 3: Implement `cmd_scrape` changes**

In `src/wslcb_licensing_tracker/cli.py`:

1. Rewrite `cmd_scrape` to call backfill after scrape:

```python
def cmd_scrape(args: argparse.Namespace) -> None:
    """Run a live scrape, then backfill un-standardized addresses."""
    engine = create_engine_from_env()
    asyncio.run(pg_scrape(engine))

    # Post-scrape: standardize any new locations via the address API.
    # Failure here is non-fatal — the weekly timer catches stragglers.
    try:
        async def _backfill() -> None:
            async with get_db(engine) as conn:
                await pg_backfill_addresses(conn, rate_limit=args.rate_limit)
                await conn.commit()

        asyncio.run(_backfill())
    except Exception:
        logger.warning("Post-scrape address backfill failed", exc_info=True)

    engine.dispose()
```

2. Add `import logging` to the stdlib imports at the top of `cli.py` (it's not currently imported). Then add `logger = logging.getLogger(__name__)` after the last import block (after line 44).

3. Add `--rate-limit` arg to the scrape subparser (after line 323):

```python
p.add_argument(
    "--rate-limit",
    type=float,
    default=0.1,
    metavar="SECONDS",
    help="Seconds to sleep between address API calls (default: 0.1)",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd .worktrees/113-post-scrape-address && uv run pytest tests/test_cli_scrape.py -v`
Expected: All 3 tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `cd .worktrees/113-post-scrape-address && uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/wslcb_licensing_tracker/cli.py tests/test_cli_scrape.py
git commit -m "#113 feat: run backfill-addresses after every scrape"
```

---

### Task 3: Add weekly systemd timer

**Files:**
- Create: `infra/wslcb-address-validation.timer`
- Modify: `docs/DEPLOYMENT.md` — add timer to services table and deployment instructions

- [ ] **Step 1: Create the timer unit file**

Create `infra/wslcb-address-validation.timer`:

```ini
[Unit]
Description=Weekly address validation backfill (Sunday 2:00 AM Pacific)

[Timer]
Unit=wslcb-task@backfill-addresses.service
OnCalendar=Sun *-*-* 02:00:00 America/Los_Angeles
Persistent=true
RandomizedDelaySec=300

[Install]
WantedBy=timers.target
```

This reuses the existing `wslcb-task@.service` template — no new service file needed.

- [ ] **Step 2: Update DEPLOYMENT.md — services table**

Add `wslcb-address-validation.timer` to the services table in `docs/DEPLOYMENT.md`:

```markdown
| `wslcb-address-validation.timer` | Weekly address backfill, Sunday 2:00 AM Pacific, ±5 min jitter |
```

- [ ] **Step 3: Update DEPLOYMENT.md — deployment instructions**

In the "After changing service files" `sudo cp` command, add the new timer file:

```bash
sudo cp infra/wslcb-web.service infra/wslcb-task@.service infra/wslcb-scraper.timer \
     infra/wslcb-address-validation.timer \
     infra/wslcb-healthcheck.service infra/wslcb-healthcheck.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-address-validation.timer
sudo systemctl enable --now wslcb-healthcheck.timer
sudo systemctl restart wslcb-web.service
```

- [ ] **Step 4: Commit**

```bash
git add infra/wslcb-address-validation.timer docs/DEPLOYMENT.md
git commit -m "#113 feat: add weekly systemd timer for address validation backfill"
```

---

### Task 4: Update docs and clean up references

**Files:**
- Modify: `tests/TEST_PATTERNS.md:68` — update example to remove `validate_addresses`
- Modify: `docs/architecture-review.md:190,217-218` — update references to removed field

- [ ] **Step 1: Update TEST_PATTERNS.md**

In `tests/TEST_PATTERNS.md:68`, change:
```python
opts = IngestOptions(validate_addresses=False, link_outcomes=False)
```
to:
```python
opts = IngestOptions(link_outcomes=False)
```

- [ ] **Step 2: Update architecture-review.md**

In `docs/architecture-review.md`, remove `validate_addresses` references:
- Line 190: remove `validate_addresses: bool = True` from the `IngestOptions` listing
- Lines 217-218: remove `validate_addresses=False` from the backfill option examples

- [ ] **Step 3: Run full test suite one final time**

Run: `cd .worktrees/113-post-scrape-address && uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/TEST_PATTERNS.md docs/architecture-review.md
git commit -m "#113 docs: update test patterns and architecture review for IngestOptions cleanup"
```
