# BUILD_ID Env Var Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `subprocess git rev-parse` with a `BUILD_ID` env var for static asset cache-busting, and surface the build ID in the health endpoint and page footer.

**Architecture:** `BUILD_ID` is set by systemd `ExecStartPre` at service start, written to `/run/wslcb-build-id`. The app reads the env var, warns if unset, falls back to `"dev"`. No git subprocess in Python.

**Tech Stack:** FastAPI, Jinja2, systemd, pytest

**Design doc:** `docs/plans/2026-03-23-build-id-design.md`
**Issue:** #110 (supersedes #107)

---

### Task 1: Replace `_get_css_version()` with `BUILD_ID` env var in `app.py`

**Files:**
- Modify: `src/wslcb_licensing_tracker/app.py:7-11` (imports), `src/wslcb_licensing_tracker/app.py:95-109` (version logic)
- Test: `tests/test_app.py`

- [ ] **Step 1: Write failing test for BUILD_ID env var**

Add to `tests/test_app.py`:

```python
import os


class TestBuildId:
    def test_build_id_jinja2_global_exists(self):
        """build_id must be set as a Jinja2 global."""
        from wslcb_licensing_tracker.app import templates
        assert "build_id" in templates.env.globals

    def test_build_id_is_string(self):
        """build_id Jinja2 global must be a string."""
        from wslcb_licensing_tracker.app import templates
        assert isinstance(templates.env.globals["build_id"], str)
        assert len(templates.env.globals["build_id"]) > 0

    def test_build_id_no_css_version_global(self):
        """css_version Jinja2 global must not exist (replaced by build_id)."""
        from wslcb_licensing_tracker.app import templates
        assert "css_version" not in templates.env.globals
```

Note: module-level `_BUILD_ID` is evaluated once at import time. Testing env var
override would require `importlib.reload()`, which is fragile with FastAPI's shared
`app` object. These tests verify the contract (global exists, is a non-empty string,
old name is gone) without reloading.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_app.py::TestBuildId -v`
Expected: FAIL — `build_id` key not found / `css_version` still present

- [ ] **Step 3: Implement BUILD_ID logic in app.py**

In `src/wslcb_licensing_tracker/app.py`:

1. Remove `import shutil` (line 10) and `import subprocess` (line 11)
2. Add `import os` between `import logging` (line 9) and `from collections.abc` (line 12) — `os` is not currently imported in this file
3. Replace lines 95–109 with:

```python
_BUILD_ID = os.environ.get("BUILD_ID")
if not _BUILD_ID:
    logger.warning("BUILD_ID not set; static asset cache-busting disabled")
    _BUILD_ID = "dev"
templates.env.globals["build_id"] = _BUILD_ID
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_app.py -v`
Expected: All pass (including existing tests — they don't depend on `css_version` by name)

- [ ] **Step 5: Commit**

```bash
git add src/wslcb_licensing_tracker/app.py tests/test_app.py
git commit -m "#110 feat: replace git subprocess with BUILD_ID env var"
```

---

### Task 2: Rename `css_version` → `build_id` in all templates

**Files:**
- Modify: `templates/base.html:8`
- Modify: `templates/search.html:137`
- Modify: `templates/detail.html:333`
- Modify: `templates/admin/endorsements.html:540`

- [ ] **Step 1: Replace `css_version` with `build_id` in all four templates**

Each file has exactly one occurrence. Find-and-replace `css_version` → `build_id` in:
- `templates/base.html` — `?v={{ css_version }}` → `?v={{ build_id }}`
- `templates/search.html` — same pattern
- `templates/detail.html` — same pattern
- `templates/admin/endorsements.html` — same pattern

- [ ] **Step 2: Run tests to verify nothing broke**

Run: `uv run pytest tests/test_app.py -v`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add templates/base.html templates/search.html templates/detail.html templates/admin/endorsements.html
git commit -m "#110 refactor: rename css_version to build_id in templates"
```

---

### Task 3: Add `build` field to `/api/v1/health` endpoint

**Files:**
- Modify: `src/wslcb_licensing_tracker/api_routes.py:111-113`
- Test: `tests/test_api_routes.py`

- [ ] **Step 1: Write failing test for build field in health response**

Add to `tests/test_api_routes.py` inside `class TestHealthEndpoint`:

```python
    def test_healthy_data_contains_build(self, client):
        resp = client.get("/api/v1/health")
        assert "build" in resp.json()["data"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_api_routes.py::TestHealthEndpoint::test_healthy_data_contains_build -v`
Expected: FAIL — `"build"` not in data dict

- [ ] **Step 3: Add build field to health endpoint**

In `src/wslcb_licensing_tracker/api_routes.py`:

1. Add `import os` between `import logging` (line 14) and `from collections.abc` (line 15) — `os` is not currently imported in this file
2. Modify the healthy response (line 112) to include build:

```python
        return JSONResponse(
            {"ok": True, "message": "Healthy", "data": {"db": "ok", "build": os.environ.get("BUILD_ID", "dev")}},
            status_code=200,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_api_routes.py::TestHealthEndpoint -v`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add src/wslcb_licensing_tracker/api_routes.py tests/test_api_routes.py
git commit -m "#110 feat: add build field to /api/v1/health response"
```

---

### Task 4: Add build ID to page footer

**Files:**
- Modify: `templates/base.html:68-72` (footer area)

- [ ] **Step 1: Add build ID tag to footer**

In `templates/base.html`, after the "Data sourced from" paragraph (line 72), add:

```html
            <p class="text-[10px] text-gray-300 text-center font-mono">{{ build_id }}</p>
```

This is intentionally subtle — `text-gray-300` and `text-[10px]` make it visible only when you're looking for it.

- [ ] **Step 2: Visually verify**

Run: `uv run uvicorn wslcb_licensing_tracker.app:app --port 8000`
Check footer at `http://localhost:8000/` — should show "dev" in tiny gray text.

- [ ] **Step 3: Commit**

```bash
git add templates/base.html
git commit -m "#110 feat: show build ID in page footer"
```

---

### Task 5: Update systemd service file

**Files:**
- Modify: `infra/wslcb-web.service`

- [ ] **Step 1: Add ExecStartPre and EnvironmentFile**

In `infra/wslcb-web.service`, add these two lines before the existing `ExecStart`:

```ini
ExecStartPre=/bin/bash -c 'echo BUILD_ID=$(/usr/bin/git -C /home/exedev/wslcb-licensing-tracker rev-parse --short HEAD) > /run/wslcb-build-id'
EnvironmentFile=/run/wslcb-build-id
```

Full `[Service]` section should read:

```ini
[Service]
Type=simple
User=exedev
WorkingDirectory=/home/exedev/wslcb-licensing-tracker
ExecStartPre=/bin/bash -c 'echo BUILD_ID=$(/usr/bin/git -C /home/exedev/wslcb-licensing-tracker rev-parse --short HEAD) > /run/wslcb-build-id'
ExecStart=/home/exedev/wslcb-licensing-tracker/.venv/bin/uvicorn wslcb_licensing_tracker.app:app --host 0.0.0.0 --port 8000
ExecStartPost=/bin/bash -c 'for i in $(seq 1 20); do sleep 3 && /usr/bin/curl -sf http://localhost:8000/api/v1/health && exit 0; done; exit 1'
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/etc/wslcb-licensing-tracker/env
EnvironmentFile=/run/wslcb-build-id
```

- [ ] **Step 2: Commit**

```bash
git add infra/wslcb-web.service
git commit -m "#110 feat: set BUILD_ID via ExecStartPre in systemd service"
```

---

### Task 6: Update documentation

**Files:**
- Modify: `docs/DEPLOYMENT.md`
- Modify: `docs/STYLE.md:95`

- [ ] **Step 1: Add BUILD_ID section to DEPLOYMENT.md**

After the "Environment" section (line 97), add:

```markdown
### BUILD_ID

Set automatically by `wslcb-web.service` at startup — `ExecStartPre` writes the short git SHA to `/run/wslcb-build-id`. Used for static asset cache-busting (`?v=<build_id>`) and shown in `/api/v1/health` response and page footer. Falls back to `"dev"` if unset.
```

- [ ] **Step 2: Update STYLE.md reference**

In `docs/STYLE.md:95`, change:

```
Cache-bust via `?v={{ css_version }}` query param (auto-set to current git SHA at startup).
```

to:

```
Cache-bust via `?v={{ build_id }}` query param (set from `BUILD_ID` env var at startup).
```

- [ ] **Step 3: Commit**

```bash
git add docs/DEPLOYMENT.md docs/STYLE.md
git commit -m "#110 docs: document BUILD_ID env var"
```

---

### Task 7: Deploy

- [ ] **Step 1: Push to remote**

```bash
git push
```

- [ ] **Step 2: Copy updated service file and reload**

```bash
sudo cp infra/wslcb-web.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart wslcb-web.service
```

- [ ] **Step 3: Verify**

```bash
curl -s http://localhost:8000/api/v1/health | python3 -m json.tool
```

Expected: `"build"` field contains a 7-char git SHA, not `"dev"`.

- [ ] **Step 4: Close issues**

```bash
gh issue close 110 --comment "Deployed. BUILD_ID set via systemd ExecStartPre."
gh issue close 107 --comment "Superseded by #110."
```
