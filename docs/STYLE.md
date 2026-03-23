# Style Guide

Cannabis Observer brand + visual conventions for all Jinja2 templates.

## Brand Assets

| File | Purpose |
|---|---|
| `static/images/cannabis_observer-icon-square.svg` | Square icon (510×510); used in navbar (32×32) and footer (16×16) |
| `static/images/cannabis_observer-name.svg` | Stylized "cannabis OBSERVER" wordmark; available for future use |

Footer emoji triad: 🌱🏛️🔍 (seedling + classical building + magnifying glass).

## Color Palette

Defined in `tailwind.config.js`:

| Token | Hex | Usage |
|---|---|---|
| `co-green` | `#8cbe69` | Brand green (from SVG). Reserved; not currently used as UI accent |
| `co-purple` (DEFAULT/600) | `#6d4488` | **Primary UI accent** — buttons, links, active pagination, highlight panels, entity links |
| `co-purple-50` | `#f5f0f8` | Highlight panel backgrounds (Assumption "Buyer", Change of Location "New Location") |
| `co-purple-100` | `#ebe1f1` | Highlight panel borders |
| `co-purple-700` | `#5a3870` | Hover states for buttons and links |
| `co-purple-800` | `#472c59` | Available for darker emphasis |

Favicon: bright green `#17de6b` (distinct from brand palette).

## Record-Type Badges

**Semantic — do NOT change to brand colors.**

| Class | Background | Text | Used for |
|---|---|---|---|
| `.badge-new` | `#dbeafe` (blue-100) | `#1e40af` (blue-800) | New Application |
| `.badge-approved` | `#dcfce7` (green-100) | `#166534` (green-800) | Approved |
| `.badge-discontinued` | `#fee2e2` (red-100) | `#991b1b` (red-800) | Discontinued |

Fixed hex values (not Tailwind classes) — visually distinct from brand accent.

## Dashboard Stat Card Colors

**Semantic — do NOT change to brand colors.**

- **New Applications**: `border-blue-200`, `text-blue-600`/`text-blue-700`
- **Approved**: `border-green-200`, `text-green-600`/`text-green-700`
- **Discontinued**: `border-red-200`, `text-red-600`/`text-red-700`
- **Neutral** (Total Records, Unique Businesses, etc.): `border-gray-200`, `text-gray-900`

## Component Conventions

- **Buttons** (Search, submit): `bg-co-purple text-white hover:bg-co-purple-700`
- **Text links** (back links, Export CSV): `text-co-purple hover:text-co-purple-700`
- **Entity name links**: `text-co-purple hover:text-co-purple-700 hover:underline`
- **Focus rings**: `focus:border-co-purple focus:ring-1 focus:ring-co-purple`
- **Active pagination**: `bg-co-purple text-white border-co-purple`
- **Detail page accent panels** ("Buyer (New) →", "New Location →"): `bg-co-purple-50 border-co-purple-100` with `text-co-purple` header
- **Entity type badges**: `bg-co-purple-50 text-co-purple` (Organization), `bg-amber-100 text-amber-800` (Person)
- **Provenance source badges**:
  - Live Scrape: `bg-green-50 text-green-700 border-green-200`
  - CO Archive / CO Diff Archive: `bg-co-purple-50 text-co-purple border-co-purple-100`
  - Internet Archive: `bg-amber-50 text-amber-700 border-amber-200`
  - Repaired: `bg-sky-50 text-sky-700 border-sky-200`
- **Provenance display**: collapsed badges with count + date range; `<details>` expands when >3 sources
- **Search filter grid**: `grid-cols-1 md:grid-cols-3 lg:grid-cols-[repeat(auto-fill,minmax(9rem,1fr))]`
- **Search button**: always bottom-right of filter card (alongside "Clear filters" at bottom-left)
- **Navbar**: icon (32×32) + bold site title; nav links use `hover:text-co-purple-700`
- **Footer**: two lines — (1) "A project of [icon] Cannabis Observer 🌱🏛️🔍" → `https://cannabis.observer/`, (2) WSLCB data source attribution

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
  Cache-bust via `?v={{ build_id }}` query param (set from `BUILD_ID` env var at startup).

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
