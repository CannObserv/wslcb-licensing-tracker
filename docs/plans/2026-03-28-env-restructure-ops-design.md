# Env Restructure + Operational Hardening

**Date:** 2026-03-28
**Inspired by:** CannObserv/watcher#49

## Goal

Standardize env file naming, clean up the env split, remove a file-parsing anti-pattern
in `pg_address_validator.py`, harden skill scripts that run pytest, and add operational
documentation that explains the single-VM dev+prod setup.

## Approved Approach

### 1. Env file rename

Rename both env files to the standard `.env` convention:

- Repo `env` → `.env`
- `/etc/wslcb-licensing-tracker/env` → `/etc/wslcb-licensing-tracker/.env`
- `.gitignore`: replace `env` entry with `.env`
- Service file: update `EnvironmentFile=/etc/wslcb-licensing-tracker/.env`

### 2. Clean env split

Remove dev-only vars from `/etc/` — they're never needed by the running service:

| Var | `/etc/.env` | repo `.env` |
|---|---|---|
| `DATABASE_URL` | ✓ | — |
| `ADDRESS_VALIDATOR_API_KEY` | ✓ | — |
| `ENABLE_ADDRESS_VALIDATION` | ✓ | — |
| `GH_TOKEN` | — | ✓ |
| `GH_TOKEN_GF_SKILLS` | — | ✓ |
| `ADMIN_DEV_EMAIL` | — | ✓ |
| `TEST_DATABASE_URL` | — | ✓ |

### 3. Rename `GITHUB_TOKEN*` → `GH_TOKEN*`

- Repo `.env` content (two vars)
- `shipping-work-claude` skill: `grep GITHUB_TOKEN env` → `grep ^GH_TOKEN .env`

### 4. Delete `_load_api_key()` and `_env_candidates` in `pg_address_validator.py`

`_load_api_key()` manually parses env files and uses `os.environ` only as a fallback —
inverted logic. The service injects `ADDRESS_VALIDATOR_API_KEY` via `EnvironmentFile`;
the CLI sources `.env` before running. `os.environ.get()` is sufficient.

**Delete:** `_env_candidates` list, `_load_api_key()` function, `_cached_api_key` global.
**Replace callers** with direct `os.environ.get("ADDRESS_VALIDATOR_API_KEY", "")` — the
same pattern `_is_validation_enabled()` already uses.
**Tests:** replace `_env_candidates` monkeypatching with `os.environ` patching.

### 5. Skill fixes

**`gather-context.sh`** (reviewing-code-claude):
- Fix broken path: `venv/` → `.venv/`
- Source env files before pytest: `/etc/wslcb-licensing-tracker/.env` (if readable),
  then repo `.env` (if present)

**`using-git-worktrees`** (local override):
- After `uv sync`: copy `.env` from main worktree into new worktree
- Document dev port 8001 (vs production port 8000)
- Note exe.dev proxy: `https://wslcb-licensing-tracker.exe.xyz:8001/`

**`shipping-work-claude`** (vendor skill reference):
- Update `grep GITHUB_TOKEN env` → `grep ^GH_TOKEN .env`

### 6. AGENTS.md additions

Add **Infrastructure** section:
- Single-VM dev+prod model
- Port 8000 = systemd service, port 8001 = dev/worktree server
- exe.dev proxy exposes both

Add **Server Lifecycle** table: what to do after code changes, worktree testing,
CSS/DB changes, and service debugging.

### 7. DEPLOYMENT.md updates

Update env file path references: `env` → `.env` throughout.

## Out of Scope

- Changing how `DATABASE_URL` is loaded in `database.py` (already uses `os.environ` correctly)
- Adding a second `EnvironmentFile` to the service (service doesn't need dev vars)
- CI/CD changes (no CI pipeline exists)
