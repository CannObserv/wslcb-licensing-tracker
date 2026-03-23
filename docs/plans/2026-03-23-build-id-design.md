# Design: Replace git subprocess with BUILD_ID env var

**Issue:** #110 (supersedes #107)
**Date:** 2026-03-23

## Goal

Remove the `subprocess.run(git rev-parse)` call from `app.py` module load. Replace with a `BUILD_ID` environment variable set by systemd at service start. Surface the build ID in the health endpoint and page footer for debugging.

## Approved approach

### BUILD_ID lifecycle

1. **systemd** computes `BUILD_ID` once at service start via `ExecStartPre`, writes it to `/run/wslcb-build-id`, and loads it as an `EnvironmentFile`.
2. **App** reads `os.environ.get("BUILD_ID")`; logs a warning and falls back to `"dev"` if unset.
3. No git fallback in Python. No subprocess. No shutil.

### Changes

| Area | What changes |
|---|---|
| `deploy/wslcb-web.service` | Add `ExecStartPre` to write `/run/wslcb-build-id`; add `EnvironmentFile=/run/wslcb-build-id` |
| `app.py` | Replace `_get_css_version()` with env var read + warning. Expose as `build_id` Jinja2 global. Remove `subprocess`, `shutil` imports. |
| Templates (5 files) | Rename `css_version` → `build_id` |
| `api_routes.py` | Add `"build"` field to `/api/v1/health` response |
| `templates/base.html` | Add build ID tag in footer |
| `docs/DEPLOYMENT.md` | Document `BUILD_ID` env var |
| `docs/STYLE.md` | Update `css_version` reference to `build_id` |
| Tests | Update `test_app.py` to reflect new global name and env var |

### Key decisions

- **No git fallback in Python** — `BUILD_ID` is set by systemd on every restart; local dev gets `"dev"` (acceptable).
- **Warning on missing env var** — `logger.warning()` at startup if `BUILD_ID` is not set.
- **Consistent naming** — env var `BUILD_ID`, Jinja2 global `build_id`, everywhere.

### Out of scope

- CI/CD pipeline or deploy scripts
- Build manifest files
- Pre-ship hooks for setting BUILD_ID
