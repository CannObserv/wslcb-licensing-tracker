# Template Branding Review — Findings Report

## 1. Completeness of indigo → co-purple Migration

**Status: CLEAN** — No remaining `indigo` references found anywhere in `templates/`. No indigo hex values (#4f46e5, #4338ca, #3730a3, #6366f1, etc.) found. Migration is complete.

## 2. Tailwind Config — co-purple Shade Coverage

**Status: CLEAN** — The config defines shades: DEFAULT, 50, 100, 600, 700, 800. Templates only use: `co-purple` (DEFAULT), `co-purple-50`, `co-purple-100`, `co-purple-700`. No undefined shades are referenced. No risk of silent failures.

Note: `co-purple-600` and `co-purple-800` are defined but never used. This is harmless (future-proofing), not a bug.

## 3. Consistency

**Status: CLEAN** — Patterns are consistent across all files:

| Pattern | Files | Consistent? |
|---|---|---|
| Back links: `text-co-purple hover:text-co-purple-700` | detail.html, entity.html, 404.html | ✓ |
| Buttons: `bg-co-purple text-white ... hover:bg-co-purple-700` | index.html, search.html | ✓ |
| Nav links: `text-gray-600 hover:text-co-purple-700` | base.html | ✓ |
| Export link: `text-co-purple hover:text-co-purple-700` | results.html | ✓ |
| Entity links: `text-co-purple hover:text-co-purple-700 hover:underline` | detail.html macro | ✓ |
| Focus inputs: `focus:border-co-purple focus:ring-co-purple` | index.html, search.html | ✓ |
| Highlight panels: `bg-co-purple-50 border-co-purple-100` | detail.html (2 instances) | ✓ |
| Pagination active: `bg-co-purple text-white border-co-purple` | results.html | ✓ |
| Footer CO link: `text-co-purple hover:text-co-purple-700` | base.html | ✓ |

## 4. Accessibility — Contrast Ratios

**Status: ALL PASS WCAG AA**

| Combination | Usage | Ratio | AA Normal (≥4.5) | AA Large (≥3.0) |
|---|---|---|---|---|
| co-purple (#6d4488) on white (#fff) | Links, text | 7.41:1 | ✓ Pass | ✓ Pass |
| co-purple-700 (#5a3870) on white | Hover states | 9.40:1 | ✓ Pass | ✓ Pass |
| White on co-purple (#6d4488) | Buttons | 7.41:1 | ✓ Pass | ✓ Pass |
| White on co-purple-700 (#5a3870) | Button hover | 9.40:1 | ✓ Pass | ✓ Pass |
| co-purple on co-purple-50 (#f5f0f8) | Panel headers (detail.html) | 6.60:1 | ✓ Pass | ✓ Pass |
| co-purple on co-purple-100 (#ebe1f1) | (not currently used as text bg) | 5.85:1 | ✓ Pass | ✓ Pass |

No accessibility concerns.

## 5. Favicon Data URI

**Status: CLEAN** — The SVG data URI is valid. The hex encoding `%2317de6b` correctly decodes to `#17de6b` (co-green). The SVG structure (circle + line = magnifying glass icon) is well-formed.

## 6. Footer Vertical Alignment — `<p>` as Flex Container

**File:** `base.html`, footer first `<p>` element  
**Severity:** style  

Findings:
- **HTML validity:** Valid. `<p>` contains only phrasing content (`<a>`, `<span>`, `<img>`, text nodes). No block-level children.
- **`<p>` as flex container:** Technically works in all browsers, but semantically unusual. A `<div>` or `<span>` would be more conventional for a flex container.
- **`text-center` redundancy:** The element has both `text-center` and `inline-flex items-center justify-center`. The `text-center` is redundant since `justify-center` handles horizontal centering of flex children. However, `text-center` won't cause visual issues — it's just dead CSS.
- **`inline-flex` + `w-full`:** The `<p>` is `inline-flex` (inline-level) but also `w-full`. This works because `w-full` sets `width: 100%` which overrides the inline shrink-to-fit behavior. It would be more conventional to use `flex` instead of `inline-flex` when the element should be full-width. No visual bug, but the intent is slightly unclear.

## 7. Navbar Icon — 463KB SVG at 32×32

**File:** `base.html`, navbar `<img>` tag  
**Severity:** perf  

The file `cannabis_observer-icon-square.svg` is **463KB** because it contains an embedded base64-encoded PNG raster image (plus 20 `xlink:href` references and Inkscape editor metadata). Loading a 463KB asset to display at 32×32 pixels is a significant performance concern, especially on first page load. This SVG is essentially a bitmap wrapped in SVG markup.

Recommendation would be to optimize (export as actual SVG paths, or use a small PNG/WebP), but no fix requested here.

## 8. Footer Icon — Same 463KB SVG at 16×16

**File:** `base.html`, footer `<img>` tag  
**Severity:** perf (mitigated)

The same `cannabis_observer-icon-square.svg` is loaded again at 16×16. However, since both reference the same URL (`/static/images/cannabis_observer-icon-square.svg`), the browser will serve the second from cache — so the actual network cost is **one** 463KB load, not two. Still, 463KB for icons rendered at 32px and 16px is excessive. A 2-4KB optimized SVG or a small PNG would be appropriate.

## 9. Entity Type Badges — Tailwind `purple` vs `co-purple`

**File:** `entity.html`, line 14  
**Severity:** minor  

The Organization badge uses Tailwind's built-in `bg-purple-100 text-purple-800`. This is intentionally **not** `co-purple`, and the visual distinction is real:

| Color | Background | Text |
|---|---|---|
| Tailwind purple | `#f3e8ff` (purple-100) | `#6b21a8` (purple-800) |
| co-purple | `#f5f0f8` (co-purple-50) | `#6d4488` (co-purple DEFAULT) |

The backgrounds are similar (`#f3e8ff` vs `#f5f0f8` — both light lavender), but the text colors differ noticeably: Tailwind's `purple-800` (#6b21a8) is a more vivid/saturated purple, while `co-purple` (#6d4488) is more muted/dusty. This could cause subtle visual confusion in the detail.html page where co-purple panels (Change of Location, Assumption) appear alongside entity links that open entity.html with Tailwind-purple badges.

However, since entity type badges are a categorization mechanism (like the existing blue/green/red section badges) and not branding elements, using Tailwind's built-in purple is a defensible design choice — it signals "this is a UI category, not a brand element." The risk is low but worth noting.

## Additional Findings

### A10. `focus:ring-co-purple` Without Ring Width

**Files:** `index.html` line 90, `search.html` line 15  
**Severity:** minor  

Both search inputs use `focus:border-co-purple focus:ring-co-purple`. The `focus:ring-co-purple` class sets `--tw-ring-color` but does not set a ring width. Without the `@tailwindcss/forms` plugin (not loaded — only the CDN play script is used), there is no default ring. The `focus:ring-co-purple` may have no visible effect. The `focus:border-co-purple` does work and provides a visible focus indicator, so this isn't an accessibility failure, but the ring color class may be dead CSS.

This was likely carried over from the original indigo implementation (`focus:ring-indigo-500`) and had the same potential issue before the rebrand.

### A11. `co-green` Defined But Unused

**File:** `base.html`, Tailwind config  
**Severity:** style  

`co-green: '#8cbe69'` is defined in the Tailwind config but never referenced in any template. It's only used in the favicon data URI as `#17de6b` (which is a different green entirely — the favicon's green doesn't match `co-green`). Harmless dead config, but worth noting for cleanup.
