# Documentation Review Findings — Post-Rebrand

Reviewer: AI doc review agent
Date: 2025-02-26
Scope: AGENTS.md, README.md vs. actual templates (base.html, detail.html, entity.html, search.html, index.html, 404.html, partials/results.html, partials/record_table.html)

---

## 1. Accuracy — Style Guide vs. Actual Code

### Finding 1.1 — Entity link `hover:underline` not documented
- **File:** AGENTS.md, Style Guide → Component Conventions
- **Severity:** Minor
- **Description:** The style guide says text links use `text-co-purple hover:text-co-purple-700`. In `detail.html` line 6, the `entity_links` macro also adds `hover:underline` to entity name links. This additional hover behavior is not mentioned in the Component Conventions.

### Finding 1.2 — All hex values, class patterns, and file references are correct
- **Severity:** N/A (no issue)
- **Description:** Verified: all six Tailwind config tokens (co-green, co-purple DEFAULT/50/100/600/700/800) match `base.html` lines 14–21 exactly. All three badge CSS definitions match `base.html` lines 28–30 exactly. Brand asset filenames match `static/images/` listing. Navbar icon is 32×32 (`w-8 h-8`), footer icon is 16×16 (`w-4 h-4`) — matches style guide. Favicon hex `#17de6b` matches `base.html` line 5. Accent panels in `detail.html` use `bg-co-purple-50 border-co-purple-100` with `text-co-purple` headers — matches style guide.

---

## 2. Completeness

### Finding 2.1 — `partials/record_table.html` not mentioned anywhere
- **File:** AGENTS.md (Key Files table and Templates section); README.md (Project Structure)
- **Severity:** Minor
- **Description:** `templates/partials/record_table.html` exists and is included by both `partials/results.html` and `entity.html`. It is not mentioned in AGENTS.md's Key Files table, the Templates row's Notes, or README.md's Project Structure tree. The Key Files table row for `templates/` says "`partials/results.html` is the HTMX target" but doesn't mention `record_table.html`. README.md's tree only shows `results.html` under `partials/`.

### Finding 2.2 — Dashboard stat cards use semantic Tailwind colors (blue/green/red borders) — not documented
- **File:** AGENTS.md, Style Guide
- **Severity:** Style
- **Description:** `index.html` uses `border-blue-200`, `text-blue-600`/`text-blue-700` for New Applications; `border-green-200`, `text-green-600`/`text-green-700` for Approved; `border-red-200`, `text-red-600`/`text-red-700` for Discontinued. These are semantic colors similar to the badge rationale. The style guide documents the badge CSS classes but doesn't mention the dashboard stat card color conventions. These aren't brand colors and follow the same "semantic — do NOT change" philosophy as badges, but the pattern is undocumented.

### Finding 2.3 — `co-green` token defined but explicitly described as unused
- **Severity:** N/A (no issue)
- **Description:** The style guide correctly notes co-green is "Reserved for future emphasis; not currently used as a UI accent." Confirmed: `co-green` appears nowhere in templates except its Tailwind config definition. This is properly documented.

---

## 3. Consistency between AGENTS.md and README.md

### Finding 3.1 — README.md does not mention custom Tailwind config, branding, favicon, or Cannabis Observer
- **File:** README.md
- **Severity:** Minor
- **Description:** README.md's Architecture table says "Tailwind CSS (CDN)" and the AGENTS.md Architecture section says "Tailwind CSS via CDN." Neither is wrong, but README.md gives no hint that there's a custom `tailwind.config` block in `base.html` with project-specific color tokens. README.md also has no mention of: (a) the Cannabis Observer branding/affiliation, (b) the favicon, (c) the `static/images/` SVG brand assets, or (d) the project's visual identity. For a public-facing README (linked from GitHub), the Cannabis Observer affiliation and the fact that there's a custom Tailwind config are worth noting.

### Finding 3.2 — README.md Project Structure doesn't expand `static/`
- **File:** README.md, Project Structure section
- **Severity:** Style
- **Description:** The project structure tree shows `static/                 # Static assets` as a single unexpanded line. With the addition of tracked brand assets in `static/images/`, expanding this to show `static/images/cannabis_observer-icon-square.svg` and `cannabis_observer-name.svg` would be consistent with how other directories are expanded (e.g., `templates/partials/`, `data/wslcb/`).

---

## 4. Staleness — Badge Info After Refactor

### Finding 4.1 — Badge info is fully preserved in the Style Guide
- **Severity:** N/A (no issue)
- **Description:** The old Templates section's badge line ("Color-coded badges: `.badge-new` (blue), `.badge-approved` (green), `.badge-discontinued` (red)") was replaced with a cross-reference to the Style Guide. The Style Guide's "Record-Type Badges" table contains strictly more detail: exact hex values for both background and text colors, the CSS class names, and what each is used for. The note "semantic — do NOT change to brand colors" adds useful guidance that wasn't present before. Nothing was lost.

---

## 5. Key Files Table

### Finding 5.1 — `static/images/` and SVG brand assets not in Key Files table
- **File:** AGENTS.md, Key Files table
- **Severity:** Style
- **Description:** The Key Files table lists individual Python modules and template directories but not `static/images/` or the two SVG brand assets. These are referenced by the Style Guide's Brand Assets table (which is sufficient for discoverability), so this is low-priority. However, the Key Files table does include `templates/entity.html` as a standalone row — applying the same granularity, brand assets could merit a row for `static/images/` with a note like "Cannabis Observer SVG brand assets (icon + wordmark)."

---

## 6. Project Structure in README.md

### Finding 6.1 — (Same as Finding 3.2)
- See Finding 3.2 above.

---

## 7. Architecture at a Glance

### Finding 7.1 — Architecture diagram and description are still accurate
- **Severity:** N/A (no issue)
- **Description:** The ASCII diagram and bullet points describe the data pipeline and tech stack. The rebrand doesn't change the architecture. The "No build step" bullet already covers the CDN approach. The Tailwind config block is a runtime customization loaded by the CDN script, not a build step, so the description remains accurate.

---

## 8. Cross-References

### Finding 8.1 — Style guide correctly references "Tailwind config block in `base.html`"
- **Severity:** N/A (no issue)
- **Description:** The Color Palette section says "Custom colors are defined in the Tailwind config block in `base.html`" — confirmed accurate (lines 11–24 of base.html).

### Finding 8.2 — Templates section cross-reference to Style Guide is correct
- **Severity:** N/A (no issue)
- **Description:** The Templates subsection under Conventions ends with "See **Style Guide** below for colors, branding, and component conventions" — this correctly points to the Style Guide section that immediately follows.

---

## Summary

| # | Finding | Severity | File(s) |
|---|---|---|---|
| 1.1 | Entity link `hover:underline` not in style guide | Minor | AGENTS.md |
| 2.1 | `partials/record_table.html` not mentioned anywhere | Minor | AGENTS.md, README.md |
| 2.2 | Dashboard stat card semantic colors undocumented | Style | AGENTS.md |
| 3.1 | README.md has no mention of Cannabis Observer, custom config, favicon, or branding | Minor | README.md |
| 3.2 | README.md `static/` not expanded to show brand assets | Style | README.md |
| 5.1 | Brand assets not in Key Files table | Style | AGENTS.md |

No bugs found. All hex values, class names, and file references in the Style Guide are accurate against actual template code.
