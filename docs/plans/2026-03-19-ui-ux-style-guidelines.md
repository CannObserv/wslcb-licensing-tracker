# UI/UX Style Guidelines & Gap Remediation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement all performance, accessibility, responsiveness, and contemporary UX improvements from the approved design, closing GH #91 (which also closes #12).

**Architecture:** Infrastructure-first — Tailwind CLI build replaces Play CDN, JS extracted to static files, cache headers added. Then template-level accessibility and responsiveness fixes applied. All work is in worktree `.worktrees/feature/91-ui-ux-style-guidelines`.

**Tech Stack:** Tailwind CLI v3.4.17 (standalone binary), FastAPI/Starlette middleware, Jinja2 templates, HTMX 2.0.4, vanilla JS.

---

## Pre-flight observations (read before starting)

- **R3 already done:** `index.html:73` already has `grid-cols-2 md:grid-cols-3 lg:grid-cols-6`. Skip.
- **R2 partially done:** `record_table.html:7` and `detail.html:280` already have `overflow-x-auto`. Only need scroll-shadow CSS and `entities_results.html` wrapper.
- **R5 already done:** All 2-column grids in `detail.html` already use `grid-cols-1 md:grid-cols-2`. Task 12 Step 12.2 will confirm then skip.
- The Tailwind Play CDN is at `base.html:8`: `<script src="https://cdn.tailwindcss.com?v=3.4.17">`. The inline config is `base.html:9-27`. The `<style>` block is `base.html:29-36`.
- `search.html` JS block starts at line 137, ends at line 279. Data injection (Jinja2 → JS global) is at line 128-130 and must stay inline.
- Admin flash banners are in `admin/endorsements.html:18-38`.
- **Task ordering constraint:** Task 3 (app.py — `css_version` global) must be committed before Tasks 4 and 5 (JS extraction), since the extracted `<script src>` tags use `?v={{ css_version }}`.
- **admin/base.html has no `</body>`** — it only defines `{% block content %}` inside `base.html`. Auto-dismiss JS (Task 13) goes at the bottom of `{% block admin_content %}` in `admin/base.html`, rendering inside `<body>` before `</body>`.
- All work committed with `#91` prefix.

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `.gitignore` | Modify | Add `scripts/bin/` |
| `tailwind.config.js` | Create | Extracted Tailwind config (required by CLI) |
| `static/css/input.css` | Create | Tailwind directives + existing custom CSS |
| `static/css/tailwind.css` | Generate + commit | Built output (~15KB minified) |
| `scripts/download-tailwind.sh` | Create | Fetch CLI binary for current platform |
| `scripts/build-css.sh` | Create | Auto-download + build; called by pre-commit hook |
| `scripts/install-hooks.sh` | Create | Installs pre-commit hook |
| `static/js/search.js` | Create | Extracted from `search.html` |
| `static/js/admin-endorsements.js` | Create | Extracted from `admin/endorsements.html` |
| `templates/base.html` | Modify | CDN → static link, hamburger nav, git-sha |
| `templates/index.html` | Modify | Emoji aria-hidden (A5) |
| `templates/search.html` | Modify | aria-live (A6), touch targets (U5), JS extraction (P2) |
| `templates/entities.html` | Modify | aria-live on `#entities-results` (A6) |
| `templates/detail.html` | Modify | Emoji aria-hidden (A1, A4), contrast (A2), grids (R5) |
| `templates/partials/record_table.html` | Modify | Focus ring (A3), aria-label (A7), shape cues (A8), sticky thead (U2) |
| `templates/partials/results.html` | No change | `aria-live` goes on the wrapping div in `search.html`, not this partial |
| `templates/partials/source_viewer.html` | Modify | Emoji aria-hidden, mobile iframe (U4) |
| `templates/partials/entities_results.html` | Modify | overflow-x-auto wrapper (R2) |
| `templates/admin/base.html` | Modify | Flash banner auto-dismiss JS (U3) |
| `templates/admin/endorsements.html` | Modify | JS extraction (P2), flash dismiss class |
| `src/wslcb_licensing_tracker/app.py` | Modify | Cache-Control middleware, git-sha global (P3) |
| `docs/STYLE.md` | Modify | Add all new rules |
| `AGENTS.md` | Modify | Dev setup section (install-hooks.sh) |

---

## Task 1: Tailwind build infrastructure

**Files:**
- Modify: `.gitignore`
- Create: `scripts/download-tailwind.sh`
- Create: `scripts/build-css.sh`
- Create: `scripts/install-hooks.sh`
- Create: `tailwind.config.js`
- Create: `static/css/input.css`

- [ ] **Step 1.1: Add `scripts/bin/` to .gitignore**

Append to `.gitignore`:
```
scripts/bin/
```

- [ ] **Step 1.2: Create `tailwind.config.js`**

Extract from `base.html:10-26`:
```js
/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        "./templates/**/*.html",
    ],
    theme: {
        extend: {
            colors: {
                'co-green': '#8cbe69',
                'co-purple': {
                    DEFAULT: '#6d4488',
                    50:  '#f5f0f8',
                    100: '#ebe1f1',
                    600: '#6d4488',
                    700: '#5a3870',
                    800: '#472c59',
                },
            },
        },
    },
}
```

- [ ] **Step 1.3: Create `static/css/input.css`**

Move the existing custom CSS from `base.html:29-36` and add Tailwind directives:
```css
@tailwind base;
@tailwind components;
@tailwind utilities;

/* HTMX loading states */
.htmx-request { opacity: 0.5; transition: opacity 300ms; }
.htmx-indicator { display: none; }
.htmx-request .htmx-indicator { display: inline-block; }

/* Record type badges */
.badge-new { background: #dbeafe; color: #1e40af; }
.badge-approved { background: #dcfce7; color: #166534; }
.badge-discontinued { background: #fee2e2; color: #991b1b; }

/* Table scroll shadow — indicates horizontal scrollability */
.scroll-shadow-right {
    -webkit-mask-image: linear-gradient(to right, black calc(100% - 2rem), transparent 100%);
    mask-image: linear-gradient(to right, black calc(100% - 2rem), transparent 100%);
}
```

- [ ] **Step 1.4: Create `scripts/download-tailwind.sh`**

```bash
#!/usr/bin/env bash
# Download the Tailwind CLI standalone binary for the current platform.
# Output: scripts/bin/tailwindcss  (executable)
# Safe to re-run — exits immediately if binary already exists.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
BINARY="$BIN_DIR/tailwindcss"

if [ -f "$BINARY" ]; then
    exit 0
fi

mkdir -p "$BIN_DIR"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)         ARCH="x64"   ;;
    aarch64|arm64)  ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
case "$OS" in
    linux)  PLATFORM="linux-${ARCH}"  ;;
    darwin) PLATFORM="macos-${ARCH}"  ;;
    *) echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

VERSION="v3.4.17"
URL="https://github.com/tailwindlabs/tailwindcss/releases/download/${VERSION}/tailwindcss-${PLATFORM}"

echo "Downloading Tailwind CLI ${VERSION} for ${PLATFORM}..." >&2
curl -fsSL -o "$BINARY" "$URL"
chmod +x "$BINARY"
echo "Tailwind CLI installed at $BINARY" >&2
```

Make executable: `chmod +x scripts/download-tailwind.sh`

- [ ] **Step 1.5: Create `scripts/build-css.sh`**

```bash
#!/usr/bin/env bash
# Build static/css/tailwind.css from templates.
# Auto-downloads the CLI binary if not present.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Auto-download binary if missing
"$SCRIPT_DIR/download-tailwind.sh"

BINARY="$SCRIPT_DIR/bin/tailwindcss"
"$BINARY" \
    -c "$ROOT/tailwind.config.js" \
    -i "$ROOT/static/css/input.css" \
    -o "$ROOT/static/css/tailwind.css" \
    --minify
echo "Built $ROOT/static/css/tailwind.css" >&2
```

Make executable: `chmod +x scripts/build-css.sh`

- [ ] **Step 1.6: Create `scripts/install-hooks.sh`**

```bash
#!/usr/bin/env bash
# Install the pre-commit hook that auto-rebuilds and stages tailwind.css.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HOOK_FILE="$ROOT/.git/hooks/pre-commit"

cat > "$HOOK_FILE" << 'HOOK'
#!/bin/sh
# Auto-rebuild Tailwind CSS before commit; stage if changed.
ROOT="$(git rev-parse --show-toplevel)"
"$ROOT/scripts/build-css.sh" || {
    echo "[pre-commit] Tailwind build failed — commit aborted." >&2
    exit 1
}
if ! git diff --quiet -- static/css/tailwind.css 2>/dev/null; then
    git add static/css/tailwind.css
    echo "[pre-commit] auto-staged regenerated static/css/tailwind.css"
fi
HOOK

chmod +x "$HOOK_FILE"
echo "Installed pre-commit hook at $HOOK_FILE" >&2
```

Make executable: `chmod +x scripts/install-hooks.sh`

- [ ] **Step 1.7: Run the build to generate `static/css/tailwind.css`**

```bash
cd /home/exedev/wslcb-licensing-tracker/.worktrees/feature/91-ui-ux-style-guidelines
./scripts/build-css.sh
```

Expected: `Built .../static/css/tailwind.css` — verify file exists and is non-empty:
```bash
ls -lh static/css/tailwind.css
```
Expected: file exists, size ~10-30KB.

- [ ] **Step 1.8: Commit**

```bash
git add .gitignore tailwind.config.js static/css/input.css static/css/tailwind.css \
    scripts/download-tailwind.sh scripts/build-css.sh scripts/install-hooks.sh
git commit -m "#91 feat: add Tailwind CLI build infrastructure (P1)"
```

---

## Task 2: Replace Play CDN in base.html

**Files:**
- Modify: `templates/base.html`

- [ ] **Step 2.1: Replace Play CDN script + inline config with static link**

In `base.html`, replace lines 8-27:
```html
    <script src="https://cdn.tailwindcss.com?v=3.4.17"></script>
    <script>
    tailwind.config = {
        theme: {
            extend: {
                colors: {
                    'co-green': '#8cbe69',
                    'co-purple': {
                        DEFAULT: '#6d4488',
                        50:  '#f5f0f8',
                        100: '#ebe1f1',
                        600: '#6d4488',
                        700: '#5a3870',
                        800: '#472c59',
                    },
                },
            },
        },
    }
    </script>
```
with:
```html
    <link rel="stylesheet" href="/static/css/tailwind.css?v={{ css_version }}">
```

Also remove the entire `<style>` block at lines 29-36 (those classes are now in `input.css`).

- [ ] **Step 2.2: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 621 passed.

- [ ] **Step 2.3: Commit**

```bash
git add templates/base.html
git commit -m "#91 feat: replace Tailwind Play CDN with pre-built static CSS (P1)"
```

---

## Task 3: Cache-Control headers and git-sha versioning

**Files:**
- Modify: `src/wslcb_licensing_tracker/app.py`
- Test: `tests/test_app.py`

- [ ] **Step 3.1: Write failing test**

Add to `tests/test_app.py`:
```python
def test_static_files_have_cache_control_header(client):
    """Static CSS/JS assets must have long-lived cache headers."""
    resp = client.get("/static/css/tailwind.css")
    assert resp.status_code == 200
    assert "public" in resp.headers.get("cache-control", "")
    assert "max-age=31536000" in resp.headers.get("cache-control", "")
```

Run: `uv run pytest tests/test_app.py::test_static_files_have_cache_control_header -v`
Expected: FAIL.

- [ ] **Step 3.2: Add git-sha helper and `css_version` Jinja2 global**

Add `import subprocess` to the imports in `app.py`.

After `templates = Jinja2Templates(directory="templates")` (line 96):
```python
def _get_css_version() -> str:
    """Return short git SHA for cache-busting static assets. Falls back to 'dev'."""
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True
        ).stdout.strip()
    except Exception:
        return "dev"

_CSS_VERSION = _get_css_version()
templates.env.globals["css_version"] = _CSS_VERSION
```

- [ ] **Step 3.3: Add Cache-Control middleware**

After `app = FastAPI(...)` and before `app.mount(...)`:
```python
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

class _StaticCacheMiddleware(BaseHTTPMiddleware):
    """Add long-lived Cache-Control headers to all /static/ responses."""

    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "public, max-age=31536000"
        return response

app.add_middleware(_StaticCacheMiddleware)
```

- [ ] **Step 3.4: Run tests — expect pass**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed (621 + 1 new).

- [ ] **Step 3.5: Commit**

```bash
git add src/wslcb_licensing_tracker/app.py tests/test_app.py
git commit -m "#91 feat: Cache-Control headers and git-sha cache-busting for static assets (P3)"
```

---

## Task 4: Extract search.html JavaScript

**Files:**
- Create: `static/js/search.js`
- Modify: `templates/search.html`

The JS at `search.html:137-279` references global `SUBSTANCE_ENDORSEMENTS` (injected inline at line 128-130). Keep the data injection inline; move all logic to `static/js/search.js`.

- [ ] **Step 4.1: Create `static/js/search.js`**

Copy everything between `<script>` and `</script>` at `search.html:137-279` (excluding the tags themselves) into `static/js/search.js`. This includes:
- The `section_type` change listener (lines 138-148)
- The `state` change listener (lines 150-178)
- The endorsement dropdown IIFE (lines 180-278)

The file references `SUBSTANCE_ENDORSEMENTS` as a global — that's fine since the inline `<script>` above it sets it before this file loads.

- [ ] **Step 4.2: Replace inline JS in search.html**

Replace the `<script>` block at lines 137-279 (the entire `<script>...</script>` block after `<div id="results">`) with:
```html
<script src="/static/js/search.js?v={{ css_version }}"></script>
```

Keep the data injection inline script at lines 128-130:
```html
<script>
const SUBSTANCE_ENDORSEMENTS = {{ filters.regulated_substance | tojson }};
</script>
```

- [ ] **Step 4.3: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 4.4: Commit**

```bash
git add static/js/search.js templates/search.html
git commit -m "#91 refactor: extract search.html inline JS to static/js/search.js (P2)"
```

---

## Task 5: Extract admin/endorsements.html JavaScript

**Files:**
- Create: `static/js/admin-endorsements.js`
- Modify: `templates/admin/endorsements.html`

- [ ] **Step 5.1: Locate JS block in admin/endorsements.html**

```bash
grep -n "<script" templates/admin/endorsements.html
```

Note the line numbers. The JS block is near the bottom of the file.

- [ ] **Step 5.2: Check for Jinja2 control flow in the JS block**

```bash
grep -n "{%" templates/admin/endorsements.html | grep -A2 -B2 "script"
```

The block contains `{% if q %}filterEndorsements();{% endif %}` — 2 lines of Jinja2 conditional logic inside the JS. Keep those 2 lines inline and extract everything else.

- [ ] **Step 5.3: Create `static/js/admin-endorsements.js` and update template**

Copy all JS content except the `{% if q %}filterEndorsements();{% endif %}` snippet to `static/js/admin-endorsements.js`. In the template, replace the large `<script>` block with:
```html
<script src="/static/js/admin-endorsements.js?v={{ css_version }}"></script>
<script>{% if q %}filterEndorsements();{% endif %}</script>
```

- [ ] **Step 5.4: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 5.5: Commit**

```bash
git add static/js/admin-endorsements.js templates/admin/endorsements.html
git commit -m "#91 refactor: extract admin endorsements inline JS to static file (P2)"
```

---

## Task 6: Accessibility — emoji aria-hidden (A1, A5)

**Files:**
- Modify: `templates/detail.html`
- Modify: `templates/partials/record_table.html`
- Modify: `templates/partials/source_viewer.html`
- Modify: `templates/index.html`
- Modify: `templates/base.html`

Rule: every decorative emoji must be `<span aria-hidden="true">emoji</span>`.

- [ ] **Step 6.1: Fix `detail.html` — outcome icon and reverse link emoji**

`detail.html:19`: change `<span class="text-lg">{{ outcome.icon }}</span>` to `<span class="text-lg" aria-hidden="true">{{ outcome.icon }}</span>`

`detail.html:40`: change `<span class="text-lg">📋</span>` to `<span class="text-lg" aria-hidden="true">📋</span>`

`detail.html:218`: The badge button renders `{{ group.icon }}` inline. Wrap it:
```html
<span aria-hidden="true">{{ group.icon }}</span> {{ group.label }} ...
```
Apply same pattern to the non-button `<span>` at line 221.

`detail.html:226`: `🔧 Repaired` → `<span aria-hidden="true">🔧</span> Repaired`

`detail.html:246`: `{% if s.role == 'repaired' %} 🔧{% endif %}` → `{% if s.role == 'repaired' %} <span aria-hidden="true">🔧</span>{% endif %}`

- [ ] **Step 6.2: Fix `record_table.html` — outcome column emojis**

`record_table.html:69-77`: wrap each emoji:
```html
{% if ost.status == 'approved' %}
<a ...><span aria-hidden="true">✅</span> Approved</a>
{% elif ost.status == 'discontinued' %}
<a ...><span aria-hidden="true">🚫</span> Disc.</a>
{% elif ost.status == 'pending' %}
<span ...><span aria-hidden="true">⏳</span> Pending</span>
{% elif ost.status == 'data_gap' %}
<span ...><span aria-hidden="true">📁</span> N/A</span>
{% elif ost.status == 'unknown' %}
<span ...><span aria-hidden="true">❓</span></span>
```

- [ ] **Step 6.3: Fix `source_viewer.html` — header icon**

`source_viewer.html:10`: change inline emoji conditionals to wrap each:
```html
<span aria-hidden="true">{% if source.source_type == 'live_scrape' %}⚡{% elif source.source_type in ('co_archive', 'co_diff_archive') %}📁{% elif source.source_type == 'internet_archive' %}🏛️{% else %}•{% endif %}</span>
```

- [ ] **Step 6.4: Fix `index.html` — pipeline card emojis (A5)**

`index.html:79-102`: each pipeline card label like `✅ Approved` → `<span aria-hidden="true">✅</span> Approved`. Apply to all 5 cards (✅⏳❓🚫📁).

`index.html:115-121` Last Scrape status: wrap ✓, ✗, ⟳ emojis in aria-hidden.

- [ ] **Step 6.5: Fix `base.html` — footer emoji triad (A5)**

`base.html:72`: change:
```html
<span aria-label="seedling, classical building, magnifying glass">🌱🏛️🔍</span>
```
to:
```html
<span aria-hidden="true">🌱🏛️🔍</span>
```
(The adjacent "Cannabis Observer" text already provides the meaningful label; the emojis are decorative.)

- [ ] **Step 6.6: Grep to verify no bare emojis remain in templates**

```bash
grep -rn "[⚡📁🌐🔧✅🚫⏳❓📋🌱🏛️🔍]" templates/ | grep -v 'aria-hidden'
```
Expected: only matches inside `aria-hidden="true"` spans, or in Jinja comments.

- [ ] **Step 6.7: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 6.8: Commit**

```bash
git add templates/detail.html templates/partials/record_table.html \
    templates/partials/source_viewer.html templates/index.html templates/base.html
git commit -m "#91 fix: wrap all decorative emojis in aria-hidden spans (A1, A5)"
```

---

## Task 7: Accessibility — contrast, focus ring, title removal (A2, A3, A4)

**Files:**
- Modify: `templates/detail.html`
- Modify: `templates/partials/record_table.html`

- [ ] **Step 7.1: Fix contrast on `<details>` summary (A2)**

`detail.html:234`: change `text-gray-400` to `text-gray-600`:
```html
<summary class="text-xs text-gray-600 cursor-pointer hover:text-gray-800">Show all {{ provenance.total }} sources</summary>
```
(Also bump hover to `text-gray-800` for proportional increase.)

- [ ] **Step 7.2: Remove `title` tooltip attributes (A4)**

`detail.html:217` (badge button): remove `title="View original source — ..."` attribute entirely.
`detail.html:221` (badge span): remove `title="Confirmed in ..."` attribute.
`detail.html:226`: remove `title="Data was repaired..."` from the repaired span.
`detail.html:245`: remove `title="{{ s.role }} — view source"`.
`detail.html:249`: remove `title="{{ s.role }}"`.
`detail.html:90`: remove `title="Original text from WSLCB"` from the business location source div.
`detail.html:133`: remove `title="Original text from WSLCB"` from the previous location source div.
`detail.html:148`: remove `title="Original text from WSLCB"` from the new location source div.
`record_table.html:42`: remove `title="Previous: {{ r.previous_business_name }}"`.
`record_table.html:48`: remove `title="Previous: {{ r.previous_business_location }}"`.

For each removed tooltip, verify the same info is available in the visible UI (it is — the label text and expanded source list already provide context).

- [ ] **Step 7.3: Add visible focus ring to interactive rows (A3)**

`record_table.html:26`: update the row `class` attribute. Current:
```
class="hover:bg-gray-50 cursor-pointer focus-within:bg-gray-50"
```
Replace with:
```
class="hover:bg-gray-50 cursor-pointer focus-within:bg-gray-50 focus-within:ring-2 focus-within:ring-co-purple focus-within:ring-offset-1"
```

`detail.html:292` (related records rows): same update — add `focus-within:ring-2 focus-within:ring-co-purple focus-within:ring-offset-1` to the `class`.

- [ ] **Step 7.4: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 7.5: Commit**

```bash
git add templates/detail.html templates/partials/record_table.html
git commit -m "#91 fix: contrast ratio, focus ring, remove inaccessible title tooltips (A2, A3, A4)"
```

---

## Task 8: Accessibility — aria-live, aria-label, shape cues (A6, A7, A8)

**Files:**
- Modify: `templates/search.html`
- Modify: `templates/entities.html`
- Modify: `templates/partials/record_table.html`
- Modify: `templates/detail.html`

- [ ] **Step 8.1: Add `aria-live` to all HTMX swap targets (A6)**

`search.html:133`: change:
```html
<div id="results">
```
to:
```html
<div id="results" aria-live="polite" aria-atomic="false">
```

`templates/entities.html:65`: find `<div id="entities-results">` and change to:
```html
<div id="entities-results" aria-live="polite" aria-atomic="false">
```

(`aria-atomic="false"` lets screen readers announce partial updates rather than re-reading the whole region.)

- [ ] **Step 8.2: Improve `aria-label` on clickable rows (A7)**

`record_table.html:27`: change `aria-label="View record {{ r.id }}"` to:
```html
aria-label="View record: {{ r.business_name }}, {{ r.record_date }}"
```

`detail.html:295` (related records rows): change `aria-label="View record {{ r.id }}"` to:
```html
aria-label="View record: {{ r.business_name }}, {{ r.record_date }}"
```

- [ ] **Step 8.3: Add shape/symbol cues to status badges (A8)**

`record_table.html:69-80`: add `aria-hidden="true"` shape prefix inside each badge (the shape supplements color — it is decorative, hence aria-hidden):
```html
{% if ost.status == 'approved' %}
<a ...><span aria-hidden="true">✅ ●</span> Approved</a>
{% elif ost.status == 'discontinued' %}
<a ...><span aria-hidden="true">🚫 ×</span> Disc.</a>
{% elif ost.status == 'pending' %}
<span ...><span aria-hidden="true">⏳ ◐</span> Pending</span>
{% elif ost.status == 'data_gap' %}
<span ...><span aria-hidden="true">📁</span> N/A</span>
{% elif ost.status == 'unknown' %}
<span ...><span aria-hidden="true">❓ ○</span></span>
```

Note: The shape symbols (●, ×, ◐, ○) follow the emoji and provide an additional non-color signal for colorblind users. Both are wrapped together in `aria-hidden` since the text label ("Approved", "Disc.", etc.) is the accessible name.

- [ ] **Step 8.4: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 8.5: Commit**

```bash
git add templates/search.html templates/entities.html templates/partials/record_table.html templates/detail.html
git commit -m "#91 fix: aria-live region, descriptive row labels, status badge shape cues (A6, A7, A8)"
```

---

## Task 9: Mobile hamburger navigation (R1)

**Files:**
- Modify: `templates/base.html`

- [ ] **Step 9.1: Add hamburger button and mobile nav to base.html**

In `base.html`, the nav's inner flex div (line 41-58) needs:
1. A hamburger button that appears at `<md`
2. The existing links hidden at `<md` and shown at `md+`
3. A mobile dropdown panel (hidden by default, toggled by button)

Replace `base.html:41-58` with:
```html
        <div class="flex justify-between h-16 items-center">
            <div class="flex items-center gap-3">
                <a href="/" class="flex items-center gap-2">
                    <img src="/static/images/cannabis_observer-icon-square.svg" alt="Cannabis Observer" class="w-8 h-8">
                    <span class="text-lg font-bold text-gray-900">WSLCB Licensing Tracker</span>
                </a>
            </div>
            {# Desktop nav links #}
            <div class="hidden md:flex items-center gap-4">
                <a href="/" class="text-sm text-gray-600 hover:text-co-purple-700">Dashboard</a>
                <a href="/search" class="text-sm text-gray-600 hover:text-co-purple-700">Records</a>
                {% if current_user %}
                <a href="/entities" class="text-sm text-gray-600 hover:text-co-purple-700">Entities</a>
                <a href="/admin/" class="text-sm text-co-purple hover:text-co-purple-700 font-medium">Admin</a>
                <span class="text-xs text-gray-400 font-mono hidden sm:inline">{{ current_user.email }}</span>
                {% endif %}
            </div>
            {# Mobile hamburger button #}
            <button id="nav-toggle" type="button"
                    class="md:hidden p-2 rounded-md text-gray-500 hover:text-co-purple hover:bg-gray-100"
                    aria-expanded="false" aria-controls="mobile-nav" aria-label="Open navigation menu">
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path id="nav-icon-open" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/>
                    <path id="nav-icon-close" class="hidden" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
                </svg>
            </button>
        </div>
```

Then add the mobile dropdown panel just after the closing `</div>` of the navbar container, before `</nav>`:
```html
    {# Mobile nav dropdown #}
    <div id="mobile-nav" class="hidden md:hidden border-t border-gray-200 bg-white">
        <div class="px-4 py-3 space-y-1">
            <a href="/" class="block py-2 text-sm text-gray-600 hover:text-co-purple-700">Dashboard</a>
            <a href="/search" class="block py-2 text-sm text-gray-600 hover:text-co-purple-700">Records</a>
            {% if current_user %}
            <a href="/entities" class="block py-2 text-sm text-gray-600 hover:text-co-purple-700">Entities</a>
            <a href="/admin/" class="block py-2 text-sm text-co-purple hover:text-co-purple-700 font-medium">Admin</a>
            <div class="py-1 text-xs text-gray-400 font-mono">{{ current_user.email }}</div>
            {% endif %}
        </div>
    </div>
```

Add the toggle script just before `</body>`:
```html
<script>
(function () {
    const btn = document.getElementById('nav-toggle');
    const menu = document.getElementById('mobile-nav');
    const iconOpen = document.getElementById('nav-icon-open');
    const iconClose = document.getElementById('nav-icon-close');
    btn.addEventListener('click', function () {
        const open = menu.classList.toggle('hidden');
        btn.setAttribute('aria-expanded', String(!open));
        iconOpen.classList.toggle('hidden', !open);
        iconClose.classList.toggle('hidden', open);
    });
}());
</script>
```

- [ ] **Step 9.2: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 9.3: Commit**

```bash
git add templates/base.html
git commit -m "#91 feat: mobile hamburger navigation at <md breakpoint (R1)"
```

---

## Task 10: Table scroll shadow (R2) + entities table wrapper

**Files:**
- Modify: `templates/partials/entities_results.html`
- Note: `record_table.html` and `detail.html` already have `overflow-x-auto` wrapper. `input.css` already has `.scroll-shadow-right` CSS from Task 1.

- [ ] **Step 10.1: Add overflow-x-auto to entities_results.html table**

Read `templates/partials/entities_results.html`. Find the `<table>` element and ensure it is wrapped in `<div class="overflow-x-auto">`. Add if missing.

- [ ] **Step 10.2: Add scroll shadow class to overflow containers**

In `record_table.html:7`: add `scroll-shadow-right` to the `overflow-x-auto` div:
```html
<div class="overflow-x-auto scroll-shadow-right">
```

In `detail.html:280`: same addition.

In `entities_results.html`: same on the wrapper added in 10.1.

- [ ] **Step 10.3: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 10.4: Commit**

```bash
git add templates/partials/entities_results.html templates/partials/record_table.html templates/detail.html
git commit -m "#91 feat: table scroll shadow and entities table overflow wrapper (R2)"
```

---

## Task 11: Admin sidebar responsive stack (R4)

**Files:**
- Modify: `templates/admin/base.html`

- [ ] **Step 11.1: Make admin layout stack on mobile**

`admin/base.html:4`: current `class="flex gap-6"` → `class="flex flex-col md:flex-row gap-6"`.

`admin/base.html:6`: current `class="w-48 flex-shrink-0"` → `class="w-full md:w-48 flex-shrink-0"`.

The sidebar nav links already have `block` display and `px-4 py-2` padding — they work horizontally on mobile without changes. On `<md` the sidebar becomes a full-width top strip with vertically stacked links.

- [ ] **Step 11.2: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 11.3: Commit**

```bash
git add templates/admin/base.html
git commit -m "#91 feat: admin sidebar stacks vertically on mobile (R4)"
```

---

## Task 12: Detail page grids + sticky table headers (R5, U2)

**Files:**
- Modify: `templates/detail.html`
- Modify: `templates/partials/record_table.html`

- [ ] **Step 12.1: Read detail.html grids**

```bash
grep -n "grid-cols-2\|grid-cols" templates/detail.html
```

Find all 2-column detail grids (Change of Location, Assumption, main details grid). Each needs `grid-cols-1 sm:grid-cols-2`.

- [ ] **Step 12.2: Verify detail page grids (R5) — likely no-op**

Run:
```bash
grep -n "grid-cols-2" templates/detail.html
```

All occurrences should already have a `grid-cols-1` companion (e.g., `grid-cols-1 md:grid-cols-2`). If any bare `grid-cols-2` without a mobile override is found, add `grid-cols-1 sm:grid-cols-2` to it. If all are already responsive, skip this step.

- [ ] **Step 12.3: Add sticky thead to record_table.html (U2)**

`record_table.html:9`: change `<thead class="bg-gray-50">` to:
```html
<thead class="bg-gray-50 sticky top-0 z-10 shadow-sm">
```

The `bg-gray-50` is kept (matching existing style) so the header occludes scrolled rows beneath it. Do not remove it.

- [ ] **Step 12.4: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 12.5: Commit**

```bash
git add templates/detail.html templates/partials/record_table.html
git commit -m "#91 feat: responsive detail grids, sticky table headers (R5, U2)"
```

---

## Task 13: Auto-dismiss admin flash banners (U3)

**Files:**
- Modify: `templates/admin/endorsements.html`
- Modify: `templates/admin/base.html`

- [ ] **Step 13.1: Add `js-flash-banner` class to all flash banner divs**

In `admin/endorsements.html:19-38`, add class `js-flash-banner` to each flash `<div>`:
```html
<div class="js-flash-banner rounded-md bg-green-50 border border-green-200 px-4 py-3 text-sm text-green-800">
    Alias saved.
    <button type="button" onclick="this.closest('.js-flash-banner').remove()" class="float-right text-green-600 hover:text-green-900 font-bold leading-none" aria-label="Dismiss">&times;</button>
</div>
```
Apply the same pattern (with matching color classes for `×` button) to all flash variants including the red error banner.

- [ ] **Step 13.2: Add auto-dismiss JS to admin/base.html**

`admin/base.html` has no `</body>` of its own — it only defines `{% block admin_content %}` inside the parent `base.html`. Add the auto-dismiss script at the **bottom of `{% block admin_content %}`** in `admin/base.html`, just before `{% endblock %}`. It will render inside `<body>` correctly.
```html
<script>
(function () {
    document.querySelectorAll('.js-flash-banner').forEach(function (el) {
        setTimeout(function () {
            el.style.transition = 'opacity 0.5s';
            el.style.opacity = '0';
            setTimeout(function () { el.remove(); }, 500);
        }, 5000);
    });
}());
</script>
```

- [ ] **Step 13.3: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 13.4: Commit**

```bash
git add templates/admin/endorsements.html templates/admin/base.html
git commit -m "#91 feat: auto-dismiss admin flash banners with close button (U3)"
```

---

## Task 14: Source viewer mobile cap + endorsement touch targets (U4, U5)

**Files:**
- Modify: `templates/partials/source_viewer.html`
- Modify: `templates/search.html`

- [ ] **Step 14.1: Cap source viewer iframe height on mobile (U4)**

`source_viewer.html:31-37`: replace iframe style:
```html
    <iframe
      id="source-viewer-iframe"
      sandbox="allow-same-origin"
      class="w-full border-0 block min-h-[80px] max-h-[60vh] overflow-y-auto"
      srcdoc="{{ srcdoc_attr | safe }}"
      onload="this.style.height=Math.min(this.contentWindow.document.body.scrollHeight+16, window.innerHeight*0.6)+'px'"
    ></iframe>
```

This caps the iframe at 60vh while still auto-expanding for content shorter than that.

- [ ] **Step 14.2: Bump endorsement dropdown touch targets (U5)**

In `search.html`, find the `.endorsement-option` items (the checkbox wrapper divs in the endorsement panel). Each label wrapping a checkbox should have `min-h-[44px] flex items-center`:
```html
<label class="endorsement-option flex items-center gap-2 px-3 min-h-[44px] cursor-pointer hover:bg-gray-50 text-sm">
    <input type="checkbox" name="endorsement" value="{{ e.name }}" ...>
    <span>{{ e.name }}</span>
</label>
```

Verify the current markup structure by reading the relevant section of `search.html` and apply the appropriate `min-h-[44px]` class.

- [ ] **Step 14.3: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 14.4: Commit**

```bash
git add templates/partials/source_viewer.html templates/search.html
git commit -m "#91 feat: source viewer mobile cap, endorsement touch targets (U4, U5)"
```

---

## Task 15: Update STYLE.md and AGENTS.md

**Files:**
- Modify: `docs/STYLE.md`
- Modify: `AGENTS.md`

- [ ] **Step 15.1: Add new rules to STYLE.md**

Append a new section at the end of `docs/STYLE.md`:

```markdown
## Accessibility Rules

- **Emoji:** All decorative emojis must be wrapped in `<span aria-hidden="true">emoji</span>`.
  Never use bare emojis in template output. Exception: emoji in `aria-label` text is acceptable.
- **Focus — interactive rows:** Rows with `tabindex="0" role="link"` must include
  `focus-within:ring-2 focus-within:ring-co-purple focus-within:ring-offset-1`.
- **Focus — inputs:** `focus:border-co-purple focus:ring-1 focus:ring-co-purple` (existing rule, now explicit).
- **HTMX live regions:** All primary HTMX swap targets that display search results must have
  `aria-live="polite" aria-atomic="false"`.
- **Row aria-label:** Clickable rows must have `aria-label="View record: [business name], [date]"`
  (not just record ID).
- **Status badges:** Always pair color with a shape/symbol cue inside `aria-hidden="true"`.
- **Muted text:** Minimum `text-gray-600` for secondary/muted UI text (never `text-gray-400`
  or `text-gray-500` on white backgrounds for readable text).
- **`title` attributes:** Do not use — inaccessible to keyboard and touch users. Surface
  the information in visible UI instead.

## Performance Rules

- **Tailwind CSS:** Never use Play CDN (`cdn.tailwindcss.com`). Always use the pre-built
  `static/css/tailwind.css`. Run `scripts/build-css.sh` after changing templates or
  `tailwind.config.js`.
- **JavaScript:** No large inline `<script>` blocks in templates. Extract logic to
  `static/js/*.js`. Keep only small data-injection snippets inline (e.g., `const DATA = {{ data | tojson }}`).
- **Static assets:** All `/static/` responses have `Cache-Control: public, max-age=31536000`.
  Cache-bust via `?v={{ css_version }}` query param (auto-set to current git SHA at startup).

## Responsiveness Rules

- **Tables:** Always wrap `<table>` in `<div class="overflow-x-auto scroll-shadow-right">`.
- **Sticky headers:** `<thead>` on all paginated tables must have `sticky top-0 z-10 shadow-sm`.
- **Touch targets:** Interactive elements (buttons, checkbox labels, dropdown items) must have
  minimum `44px` height (`min-h-[44px]`).
- **Mobile nav:** Hamburger menu required at `<md` breakpoint in `base.html`.
- **Admin sidebar:** Must use `flex-col md:flex-row` to stack on mobile.
- **Detail grids:** 2-column detail grids must use `grid-cols-1 sm:grid-cols-2`.

## Flash Banners (Admin)

- All flash banner `<div>`s must have class `js-flash-banner`.
- Include an inline `×` close button.
- Auto-dismiss logic in `admin/base.html` fades them out after 5 seconds.
```

- [ ] **Step 15.2: Add dev setup section to AGENTS.md**

In `AGENTS.md`, under the "Common Tasks" section, add:

```markdown
## Dev Setup (one-time after clone)

```bash
# Install pre-commit hook (auto-rebuilds Tailwind CSS before each commit)
scripts/install-hooks.sh

# Manual CSS rebuild (if you change templates or tailwind.config.js without committing)
scripts/build-css.sh
```

The hook auto-downloads the Tailwind CLI binary on first run (~26MB, platform-specific, gitignored).
```

- [ ] **Step 15.3: Run tests**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed.

- [ ] **Step 15.4: Commit**

```bash
git add docs/STYLE.md AGENTS.md
git commit -m "#91 docs: update STYLE.md with all new rules; add dev setup to AGENTS.md"
```

---

## Final verification

- [ ] **Step 16.1: Full test suite**

```bash
uv run pytest tests/ -v
```
Expected: 622 passed, 0 failed.

- [ ] **Step 16.2: Grep for any remaining bare emojis in templates**

```bash
grep -rn ">[^<]*[⚡📁🌐🔧✅🚫⏳❓📋🌱🏛️🔍]" templates/ | grep -v 'aria-hidden'
```
Expected: no output.

- [ ] **Step 16.3: Verify Tailwind CDN is gone**

```bash
grep -rn "cdn.tailwindcss.com" templates/
```
Expected: no output.

- [ ] **Step 16.4: Verify no large inline script blocks remain**

```bash
grep -c "<script>" templates/search.html templates/admin/endorsements.html
```
Expected: 1 (the data injection inline script) in search.html, 0 or 1 in endorsements.html.
