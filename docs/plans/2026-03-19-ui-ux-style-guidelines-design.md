# UI/UX Style Guidelines & Gap Remediation Design

**Date:** 2026-03-19
**Scope:** Comprehensive style guide update covering performance, accessibility, responsiveness, and contemporary UX. Also addresses all items in GH issue #12.

---

## Goal

Audit the current UI implementation, establish formal style guidelines, and remediate identified gaps across four dimensions: performance, accessibility, responsiveness, and contemporary UX. Mobile is a first-class target (expected ~50% of usage). Internationalization is out of scope.

---

## Approved Approach

### Performance

#### P1 — Replace Tailwind Play CDN with CLI build

The Play CDN generates CSS at runtime in the browser (~3MB, no tree-shaking, uncacheable). Replace with a static pre-built stylesheet served from `static/css/tailwind.css`.

**Mechanics:**
- `scripts/download-tailwind.sh` — fetches the platform-appropriate Tailwind CLI binary to `scripts/bin/tailwindcss` (path gitignored); detects OS/arch automatically
- `scripts/build-css.sh` — calls `download-tailwind.sh` if binary is absent, then runs `tailwindcss --minify -c tailwind.config.js -i static/css/input.css -o static/css/tailwind.css`
- `scripts/install-hooks.sh` — installs a pre-commit hook that runs `build-css.sh` and auto-stages `static/css/tailwind.css` if it changed; aborts only if binary download fails
- `tailwind.config.js` — extracted from the inline `<script>` block in `base.html` to a standalone file required by the CLI
- `static/css/input.css` — minimal source file: Tailwind directives + any custom CSS
- `static/css/tailwind.css` — committed generated output; serves tree-shaken, minified CSS (~10–20KB)
- `base.html` — replace `<script src="https://cdn.tailwindcss.com">` with `<link rel="stylesheet" href="/static/css/tailwind.css">`

HTMX remains on CDN (small, rarely changes, widely cached).

Dev setup: clone → `scripts/install-hooks.sh` → done. The binary auto-downloads on first `build-css.sh` call.

#### P2 — Extract inline JavaScript

Large JS blocks in `search.html` (endorsement widget, city select, substance sync) and `admin/endorsements.html` (alias machinery, tab filtering, checkbox logic) are extracted to:
- `static/js/search.js`
- `static/js/admin-endorsements.js`

Browser caches them across page loads. Currently, inline scripts are re-parsed on every navigation.

#### P3 — Static asset cache headers

FastAPI `StaticFiles` mount gains middleware setting `Cache-Control: public, max-age=31536000` for CSS/JS assets. Cache-busting via `?v=<git-sha>` query param injected into `base.html` at app startup (read once from `git rev-parse --short HEAD` or a baked-in env var).

---

### Accessibility (includes all GH #12 items)

#### A1 — Provenance badge emojis (GH #12)
All emoji used as icons in provenance badges (⚡ Live Scrape, 📁 CO Archive, etc.) wrapped in `<span aria-hidden="true">`. Adjacent text label provides the accessible name.

#### A2 — Low contrast on `<details>` summary (GH #12)
`text-gray-400` (~2.7:1) → `text-gray-600` (~7:1) on "Show all N sources" toggle. Codified as the minimum text contrast class for secondary/muted UI text.

#### A3 — Focus indicator on clickable rows (GH #12)
Related record rows and all `role="link"` table rows gain:
```
focus-within:ring-2 focus-within:ring-co-purple focus-within:ring-offset-1
```
Codified in STYLE.md as the standard focus pattern for interactive rows.

#### A4 — `title` attribute tooltips (GH #12)
Badge `title` text (e.g., "Confirmed in 3 live scrapes") moved to the expanded source detail view. `title` attributes removed — they are inaccessible to keyboard and touch users.

#### A5 — Decorative emojis in dashboard and footer
Dashboard pipeline status emojis (✅ ⏳ ❓ 🚫 📁) and footer emoji triad (🌱 🏛️ 🔍) all wrapped in `<span aria-hidden="true">`.

#### A6 — HTMX swap targets need `aria-live`
`#results` container and any other HTMX primary swap targets that display search results gain `aria-live="polite"` so screen readers announce content changes after partial page updates.

#### A7 — Clickable row `aria-label`
Rows with `role="link"` gain descriptive `aria-label` attributes (e.g., `"View record: [business name], [date]"`) so screen readers announce meaningful context rather than the raw first-cell content.

#### A8 — Color-only status differentiation
Status badges gain a shape/symbol prefix alongside color so colorblind users have a non-color signal:
- Approved: `●` (filled circle, green)
- Pending: `◐` (half circle, amber)
- Discontinued: `×` (cross, red)
- Unknown: `○` (open circle, gray)

Symbols wrapped in `aria-hidden="true"`; color and text label remain the semantic identifiers.

---

### Responsiveness

#### R1 — Mobile navigation
At `<md` breakpoint: hamburger button (3-line icon) collapses nav links into a vertical dropdown below the navbar bar. Toggle uses `aria-expanded` and `aria-controls`. ~10 lines of vanilla JS in `base.html`. At `md+`: current inline layout unchanged.

#### R2 — Record table horizontal scroll
All record table instances wrapped in `overflow-x-auto` container with a subtle right-edge scroll shadow (CSS `mask-image` gradient) indicating scrollability. No card-view rewrite — horizontal scroll is appropriate for this data density.

#### R3 — Dashboard pipeline grid
`grid-cols-2 sm:grid-cols-3 lg:grid-cols-6` — pairs on mobile, triplets on tablet, full 6-column row on desktop.

#### R4 — Admin sidebar on mobile
`flex-col md:flex-row` — sidebar becomes a horizontal scrollable top nav strip on mobile. Nav items retain active highlight.

#### R5 — Detail page 2-column grids
Change-of-location and assumption detail grids: explicit `grid-cols-1 sm:grid-cols-2`.

---

### Contemporary UX

#### U1 — Mobile nav (see R1 above)

#### U2 — Sticky table headers
`<thead>` on all paginated tables gains `sticky top-0 bg-white z-10 shadow-sm` so column headers remain visible while scrolling long result sets.

#### U3 — Auto-dismiss admin flash banners
Flash banners in admin auto-dismiss after 5 seconds with CSS fade-out transition. Close button (`×`) allows immediate dismiss. ~15 lines of vanilla JS in `admin/base.html`.

#### U4 — Source viewer on mobile
Cap iframe at `max-h-[60vh]` with `overflow-y-auto` instead of unbounded auto-height on load. Prevents the iframe from pushing page content off-screen on small viewports.

#### U5 — Endorsement dropdown touch targets
All checkbox labels in the multi-select endorsement dropdown bumped to `min-h-[44px]` with adjusted padding, meeting Apple HIG and WCAG 2.5.5 touch target minimums.

---

## Style Guide Updates (STYLE.md additions)

The following rules are added to `docs/STYLE.md`:

| Rule | Requirement |
|---|---|
| Emoji | All decorative emojis must be wrapped in `<span aria-hidden="true">` |
| Focus — interactive rows | `focus-within:ring-2 focus-within:ring-co-purple focus-within:ring-offset-1` |
| Focus — inputs | `focus:border-co-purple focus:ring-1 focus:ring-co-purple` (existing, now explicit) |
| HTMX live regions | All HTMX primary swap targets showing search results must have `aria-live="polite"` |
| Status badges | Always pair color with a shape/symbol cue (see A8) |
| Tables | Always wrap in `overflow-x-auto` container |
| Sticky headers | Required on all paginated tables |
| Touch targets | Interactive elements minimum `44px` height/width |
| Muted text | Minimum `text-gray-600` for secondary/muted UI text |
| Tailwind CSS | Never use Play CDN; always use pre-built `static/css/tailwind.css` |
| JS files | No large inline script blocks; extract to `static/js/*.js` |

---

## Out of Scope

- Internationalization / multi-language support
- Card-view alternative for tables on mobile
- Redesign of existing color palette or brand identity
- CI drift check for generated CSS (can be added later trivially)
- Replacing HTMX with a JS framework
