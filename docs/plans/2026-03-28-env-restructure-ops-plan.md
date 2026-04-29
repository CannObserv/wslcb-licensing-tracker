# Env Restructure + Operational Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Standardize env file naming, clean the env split, remove a file-parsing anti-pattern in `pg_address_validator.py`, fix broken skill scripts, and add operational docs.

**Architecture:** The repo `env` file becomes `.env`; `/etc/wslcb-licensing-tracker/env` becomes `.env` with only true production vars. The file-reading logic in `pg_address_validator._load_api_key()` is deleted — the service already injects the var via `EnvironmentFile`, and skill scripts will source `.env` before running code.

**Tech Stack:** Python 3.12, pytest, bash, systemd, FastAPI/uvicorn, AGENTS.md/DEPLOYMENT.md docs.

**Issue:** #125

---

## File Map

| File | Change |
|---|---|
| `src/wslcb_licensing_tracker/pg_address_validator.py` | Delete `_cached_api_key`, `_env_candidates`, `_load_api_key()`; replace callers; remove `Path` import |
| `tests/test_pg_address_validator.py` | Replace `_load_api_key` patches with `os.environ` patches |
| `.gitignore` | `env` → `.env` |
| `env` (filesystem only, gitignored) | Rename to `.env`; rename `GITHUB_TOKEN*` → `GH_TOKEN*` |
| `infra/wslcb-web.service` | `EnvironmentFile=/etc/wslcb-licensing-tracker/.env` |
| `/etc/wslcb-licensing-tracker/env` (system) | Rename to `.env`; remove dev-only vars |
| `.claude/skills/reviewing-code-claude/scripts/gather-context.sh` | Fix `venv/` → `.venv/`; source env files before pytest |
| `.claude/skills/using-git-worktrees/SKILL.md` | Add project-specific setup: copy `.env`, document port 8001 |
| `skills/shipping-work-claude/SKILL.md` | Fix token grep: `GITHUB_TOKEN env` → `GH_TOKEN .env` |
| `AGENTS.md` | Add Infrastructure section + Server Lifecycle table |
| `docs/DEPLOYMENT.md` | Update `env` path references |

---

## Task 1: Delete `_load_api_key()` — update tests first (TDD)

**Files:**
- Modify: `tests/test_pg_address_validator.py` (lines ~273–308)
- Modify: `src/wslcb_licensing_tracker/pg_address_validator.py`

- [ ] **Step 1: Update the four `_load_api_key` patches in tests**

Replace all four `patch("wslcb_licensing_tracker.pg_address_validator._load_api_key", ...)` occurrences with `patch.dict("os.environ", ...)`.

```python
# tests/test_pg_address_validator.py

# TestStandardizeHTTP.test_returns_none_without_api_key
async def test_returns_none_without_api_key(self):
    with patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": ""}):
        result = await standardize("123 MAIN ST")
    assert result is None

# TestStandardizeHTTP.test_returns_data_on_success
async def test_returns_data_on_success(self):
    expected = {"address_line_1": "123 MAIN ST", "city": "SEATTLE", "warnings": []}
    mock_response = httpx.Response(200, json=expected)
    with (
        patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
        patch(
            "wslcb_licensing_tracker.pg_address_validator._post_with_retry",
            return_value=mock_response,
        ),
    ):
        result = await standardize("123 MAIN ST")
    assert result == expected

# TestValidateHTTP.test_returns_none_without_api_key
async def test_returns_none_without_api_key(self):
    with patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": ""}):
        result = await validate("123 MAIN ST")
    assert result is None

# TestValidateHTTP.test_returns_none_when_post_returns_none
async def test_returns_none_when_post_returns_none(self):
    with (
        patch.dict(os.environ, {"ADDRESS_VALIDATOR_API_KEY": "key"}),
        patch(
            "wslcb_licensing_tracker.pg_address_validator._post_with_retry",
            return_value=None,
        ),
    ):
        result = await validate("123 MAIN ST")
    assert result is None
```

Verify `import os` is present at the top of the test file (it likely already is via `patch.dict`).

- [ ] **Step 2: Run tests — expect failures (function still exists)**

```bash
uv run pytest tests/test_pg_address_validator.py::TestStandardizeHTTP tests/test_pg_address_validator.py::TestValidateHTTP -v
```

Expected: failures or errors because `_load_api_key` is still present and the `patch.dict` approach changes behavior.

- [ ] **Step 3: Delete `_load_api_key`, `_env_candidates`, `_cached_api_key` from `pg_address_validator.py`**

Remove these blocks entirely from `pg_address_validator.py`:

```python
# DELETE this global (line ~46):
_cached_api_key: str | None = None

# DELETE this block (lines ~63-66):
_env_candidates: list[Path] = [
    Path("/etc/wslcb-licensing-tracker/env"),
    Path(__file__).resolve().parent.parent.parent / "env",
]

# DELETE this entire function (lines ~69-97):
def _load_api_key() -> str:
    """Load the API key from the ./env file or environment variable.
    ...
    """
    global _cached_api_key
    ...
    return _cached_api_key
```

Remove the `Path` import (line 24: `from pathlib import Path`) — it's only used by `_env_candidates`.

Replace both `_load_api_key()` call sites in `standardize()` and `validate()`:

```python
# In standardize() — replace:
api_key = _load_api_key()
# with:
api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")

# In validate() — replace:
api_key = _load_api_key()
# with:
api_key = os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")
```

Update the module docstring comment if it mentions the file-reading fallback (lines ~60-61).

- [ ] **Step 4: Run tests — expect pass**

```bash
uv run pytest tests/test_pg_address_validator.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Run full suite**

```bash
uv run pytest tests/ -v
```

Expected: all tests pass (PG tests skip if `TEST_DATABASE_URL` not set — that's fine).

- [ ] **Step 6: Commit**

```bash
git add src/wslcb_licensing_tracker/pg_address_validator.py tests/test_pg_address_validator.py
git commit -m "$(cat <<'EOF'
#125 refactor: delete _load_api_key() — use os.environ directly

File-parsing logic was inverted: read from disk first, os.environ as fallback.
The service injects ADDRESS_VALIDATOR_API_KEY via EnvironmentFile; os.environ
is sufficient. Removes _env_candidates, _cached_api_key, Path import.
Tests updated to patch os.environ instead of _load_api_key.
EOF
)"
```

---

## Task 2: Rename repo `env` → `.env` and update `.gitignore`

**Files:**
- Modify: `.gitignore`
- Filesystem: `env` → `.env` (gitignored, not tracked)

- [ ] **Step 1: Rename the file and update its content**

```bash
# Rename
mv env .env

# Rename the token vars inside it (sed in-place)
sed -i 's/^GITHUB_TOKEN_GF_SKILLS=/GH_TOKEN_GF_SKILLS=/' .env
sed -i 's/^GITHUB_TOKEN=/GH_TOKEN=/' .env
```

Verify the result looks right:
```bash
grep -E "^GH_TOKEN|^ADDRESS_VALIDATOR|^ADMIN_DEV|^TEST_DATABASE|^ENABLE_ADDRESS" .env
```

Expected output (values redacted here):
```
GH_TOKEN=...
GH_TOKEN_GF_SKILLS=...
ADDRESS_VALIDATOR_API_KEY=...
ADMIN_DEV_EMAIL=...
TEST_DATABASE_URL=...
ENABLE_ADDRESS_VALIDATION=...
```

- [ ] **Step 2: Update `.gitignore`**

In `.gitignore`, replace the `env` line with `.env`:

```
# Before:
env

# After:
.env
```

- [ ] **Step 3: Verify `.env` is ignored**

```bash
git check-ignore -v .env
```

Expected: `.gitignore:N:.env  .env` (shows `.env` is gitignored).

```bash
git status --short
```

Expected: `.env` does NOT appear in the output (it's gitignored). `.gitignore` appears as modified (`M .gitignore`).

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "$(cat <<'EOF'
#125 chore: rename env → .env, GITHUB_TOKEN* → GH_TOKEN*

Standard .env convention. Token vars renamed to GH_TOKEN / GH_TOKEN_GF_SKILLS.
.gitignore updated accordingly.
EOF
)"
```

---

## Task 3: Update service file and rename `/etc/` env file

**Files:**
- Modify: `infra/wslcb-web.service`
- System: `/etc/wslcb-licensing-tracker/env` → `.env` (clean production vars only)

- [ ] **Step 1: Update `infra/wslcb-web.service`**

Change line `EnvironmentFile=/etc/wslcb-licensing-tracker/env` to:

```ini
EnvironmentFile=/etc/wslcb-licensing-tracker/.env
```

- [ ] **Step 2: Verify the change**

```bash
grep EnvironmentFile infra/wslcb-web.service
```

Expected: `EnvironmentFile=/etc/wslcb-licensing-tracker/.env`

- [ ] **Step 3: Create clean `/etc/wslcb-licensing-tracker/.env`**

Read the current content, then write the clean version with only production runtime vars:

```bash
# Check what's currently in /etc
sudo grep -v "^#" /etc/wslcb-licensing-tracker/env | grep -v "^$"
```

Note the values for `DATABASE_URL`, `ADDRESS_VALIDATOR_API_KEY`, and `ENABLE_ADDRESS_VALIDATION`, then create the new file:

```bash
sudo tee /etc/wslcb-licensing-tracker/.env > /dev/null <<'EOF'
DATABASE_URL=<value from old file>
ADDRESS_VALIDATOR_API_KEY=<value from old file>
ENABLE_ADDRESS_VALIDATION=<value from old file>
EOF

sudo chmod 640 /etc/wslcb-licensing-tracker/.env
sudo chown root:exedev /etc/wslcb-licensing-tracker/.env
```

(Replace `<value from old file>` with actual values.)

- [ ] **Step 4: Install updated service file and restart**

```bash
sudo cp infra/wslcb-web.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart wslcb-web.service
```

- [ ] **Step 5: Verify service is healthy**

```bash
systemctl status wslcb-web.service --no-pager
curl -sf http://localhost:8000/api/v1/health && echo "OK"
```

Expected: service `active (running)`, health endpoint returns 200.

- [ ] **Step 6: Remove old `/etc/` env file**

```bash
sudo rm /etc/wslcb-licensing-tracker/env
```

- [ ] **Step 7: Commit the service file change**

```bash
git add infra/wslcb-web.service
git commit -m "$(cat <<'EOF'
#125 chore: update service EnvironmentFile path to .env
EOF
)"
```

---

## Task 4: Fix `gather-context.sh`

**Files:**
- Modify: `.claude/skills/reviewing-code-claude/scripts/gather-context.sh`

- [ ] **Step 1: Fix the broken venv path and add env sourcing**

Replace the last two lines of `gather-context.sh`:

```bash
# Current (broken):
echo ""
echo "=== Tests ==="
source venv/bin/activate && python -m pytest tests/ -q --tb=short 2>&1 || true
```

With:

```bash
echo ""
echo "=== Tests ==="
# Load env files so PG tests get DATABASE_URL and ADDRESS_VALIDATOR_API_KEY.
if [ -r /etc/wslcb-licensing-tracker/.env ]; then
    set -a; source /etc/wslcb-licensing-tracker/.env; set +a
fi
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a; source "$PROJECT_ROOT/.env"; set +a
fi
source .venv/bin/activate && python -m pytest tests/ -q --tb=short 2>&1 || true
```

- [ ] **Step 2: Smoke-test the script**

```bash
bash .claude/skills/reviewing-code-claude/scripts/gather-context.sh
```

Expected: runs without error, pytest output visible at end (PG tests should run if `TEST_DATABASE_URL` is in `.env`).

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/reviewing-code-claude/scripts/gather-context.sh
git commit -m "$(cat <<'EOF'
#125 fix: gather-context.sh — correct venv path, source env before pytest
EOF
)"
```

---

## Task 5: Update `using-git-worktrees` skill

**Files:**
- Modify: `.claude/skills/using-git-worktrees/SKILL.md`

The local override is currently the vanilla vendor skill. Add project-specific setup after the `uv sync` step in the "Run Project Setup" section, and document dev port 8001.

- [ ] **Step 1: Update the skill**

In `.claude/skills/using-git-worktrees/SKILL.md`, replace the Python setup block in "### 3. Run Project Setup":

```bash
# Python
if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
if [ -f pyproject.toml ]; then poetry install; fi
```

With:

```bash
# Python (uv)
if [ -f pyproject.toml ] && command -v uv &>/dev/null; then
    uv sync --dev
    # Copy .env from main worktree so API keys and TEST_DATABASE_URL are available.
    MAIN_WT=$(git worktree list | head -1 | awk '{print $1}')
    [ -f "$MAIN_WT/.env" ] && cp "$MAIN_WT/.env" .env
elif [ -f requirements.txt ]; then
    pip install -r requirements.txt
fi
```

Also add a "Dev server" note to the Quick Reference table:

```markdown
| Testing in worktree | `uv run uvicorn wslcb_licensing_tracker.app:app --host 0.0.0.0 --port 8001` |
| Dev proxy URL | `https://wslcb-licensing-tracker.exe.xyz:8001/` |
```

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/using-git-worktrees/SKILL.md
git commit -m "$(cat <<'EOF'
#125 chore: update using-git-worktrees skill — uv sync, copy .env, port 8001
EOF
)"
```

---

## Task 6: Update `shipping-work-claude` skill

**Files:**
- Modify: `skills/shipping-work-claude/SKILL.md` (line ~159)

- [ ] **Step 1: Update the token load line**

Replace:

```
- Load GH token for `gh` commands: `export GH_TOKEN=$(grep GITHUB_TOKEN env | cut -d= -f2)`
```

With:

```
- Load GH token for `gh` commands: `export GH_TOKEN=$(grep ^GH_TOKEN= .env | head -1 | cut -d= -f2)`
```

- [ ] **Step 2: Verify**

```bash
grep "GH_TOKEN" skills/shipping-work-claude/SKILL.md
```

Expected: shows updated line with `^GH_TOKEN=` and `.env`.

- [ ] **Step 3: Commit**

```bash
git add skills/shipping-work-claude/SKILL.md
git commit -m "$(cat <<'EOF'
#125 chore: update shipping-work-claude — GH_TOKEN var name, .env filename
EOF
)"
```

---

## Task 7: Add Infrastructure + Server Lifecycle to AGENTS.md

**Files:**
- Modify: `AGENTS.md`

Add two new sections. Insert them between `## Common Tasks` and the end of the file (or before `## Git Workflow` — whichever reads more naturally).

- [ ] **Step 1: Add Infrastructure section**

Insert after the `## Common Tasks` section:

```markdown
## Infrastructure

Single-VM setup: production service and dev work share one machine.

| Port | Service |
|---|---|
| 8000 | `wslcb-web.service` (systemd, always running) |
| 8001 | Dev/worktree uvicorn (manual, short-lived) |

Both ports are reachable via the exe.dev proxy:
- Production: `https://wslcb-licensing-tracker.exe.xyz:8000/`
- Dev: `https://wslcb-licensing-tracker.exe.xyz:8001/`

**Environment files:**

| File | Owner | Content |
|---|---|---|
| `/etc/wslcb-licensing-tracker/.env` | `root:exedev 640` | Production runtime vars: `DATABASE_URL`, `ADDRESS_VALIDATOR_API_KEY`, `ENABLE_ADDRESS_VALIDATION` |
| `.env` (repo root, gitignored) | `exedev` | Dev/agent vars: `GH_TOKEN`, `GH_TOKEN_GF_SKILLS`, `ADMIN_DEV_EMAIL`, `TEST_DATABASE_URL` |

The systemd service loads only `/etc/wslcb-licensing-tracker/.env`. Dev code and agents source the repo `.env` directly.

## Server Lifecycle

| Situation | Action |
|---|---|
| Python or template change | `sudo systemctl restart wslcb-web.service` |
| Service file change | `sudo cp infra/wslcb-web.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart wslcb-web.service` |
| CSS change | `scripts/build-css.sh` (pre-commit hook does this automatically on commit) |
| DB schema change | `uv run alembic upgrade head` (no service restart needed) |
| Test in a worktree | `uv run uvicorn wslcb_licensing_tracker.app:app --host 0.0.0.0 --port 8001` |
| Check live logs | `journalctl -u wslcb-web.service -f` |
| Stale process on port 8000 | `sudo systemctl restart wslcb-web.service` — never kill manually |
```

- [ ] **Step 2: Verify the sections appear**

```bash
grep -n "## Infrastructure\|## Server Lifecycle" AGENTS.md
```

Expected: both section headers found.

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "$(cat <<'EOF'
#125 docs: add Infrastructure and Server Lifecycle sections to AGENTS.md
EOF
)"
```

---

## Task 8: Update DEPLOYMENT.md

**Files:**
- Modify: `docs/DEPLOYMENT.md` (lines ~60-63)

- [ ] **Step 1: Update the Address Validation section**

Replace the stale lines in the Address Validation section:

```markdown
# Before (lines ~60-63):
- API key: `/etc/wslcb-licensing-tracker/env` (`ADDRESS_VALIDATOR_API_KEY=...`), `640 root:exedev`
- Falls back to `<project-root>/env` for local dev
- `ENABLE_ADDRESS_VALIDATION=true` in env enables DPV validation (phase 2); otherwise only standardization runs
- Services load env via `EnvironmentFile=/etc/wslcb-licensing-tracker/env`
```

With:

```markdown
- API key: `ADDRESS_VALIDATOR_API_KEY` in `/etc/wslcb-licensing-tracker/.env` (owned `root:exedev`, mode `640`)
- `ENABLE_ADDRESS_VALIDATION=true` enables DPV validation; otherwise only standardization runs
- Services load env via `EnvironmentFile=/etc/wslcb-licensing-tracker/.env`
```

The "falls back to repo env" line is removed — that file-reading logic no longer exists.

- [ ] **Step 2: Verify no remaining stale `env` path references**

```bash
grep -n "/etc/wslcb-licensing-tracker/env\b\|<project-root>/env" docs/DEPLOYMENT.md
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add docs/DEPLOYMENT.md
git commit -m "$(cat <<'EOF'
#125 docs: update DEPLOYMENT.md env paths to .env
EOF
)"
```

---

## Self-Review

**Spec coverage check:**

| Design item | Task |
|---|---|
| Rename `env` → `.env` (repo + `/etc/`) | Task 2, Task 3 |
| Clean `/etc/` to production vars only | Task 3 |
| Rename `GITHUB_TOKEN*` → `GH_TOKEN*` | Task 2 |
| Service file EnvironmentFile path | Task 3 |
| Delete `_load_api_key()` + tests | Task 1 |
| Fix `gather-context.sh` | Task 4 |
| Update `using-git-worktrees` skill | Task 5 |
| Update `shipping-work-claude` skill | Task 6 |
| AGENTS.md Infrastructure + Server Lifecycle | Task 7 |
| DEPLOYMENT.md updates | Task 8 |

All items covered. No placeholders. Types consistent (no new types introduced).
