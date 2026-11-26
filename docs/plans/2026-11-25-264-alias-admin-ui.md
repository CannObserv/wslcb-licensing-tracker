---
title: "#264 entity Links admin UI: panel, review-queue Links tab, audit actions"
date: 2026-11-25
status: approved
---

# #264 entity Links admin UI

Design: [#247 spec](../specs/2026-11-23-247-entity-name-linking-design.md) §4
"Operator UI". The layer it writes through: [ENTITY_ALIASES.md](../ENTITY_ALIASES.md)
"Operator writes" (#262). The page it sits on: the #263 party pages.

## Problem

The alias layer has a write API (`add_manual_alias`, `remove_manual_alias`,
`dismiss_pair`, `undismiss_pair`), but no route calls it and no audit row
records it. The operator cannot link `TY` to `TY PENDERGRAFT`, unlink a wrong
format-key pair, or work the 96 comma-drop suggestions the spec sends to
review. A global variant's page also redirects (#263), so an admin can't
reach the page to manage its own links.

## Approach

**A service module, `alias_review.py`**, beside `classification_review.py`.
It wraps the write API and writes the audit rows in the same transaction:

| Function | Calls | Audit action |
|---|---|---|
| `link(variant, target, scope, note)` | `add_manual_alias` | `entity.alias_set` |
| `unlink(alias_id)`, manual row | `remove_manual_alias` | `entity.alias_remove` |
| `unlink(alias_id)`, rule row | `dismiss_pair`, scope = the row's | `entity.alias_dismiss` (source `rule`) |
| `dismiss(a, b, scope, note)` | `dismiss_pair` | `entity.alias_dismiss` (source `manual`) |
| `undismiss(dismissal_id)` | `undismiss_pair` | `entity.alias_undismiss` |

- `details` carries both ids and **both names**, the scope, the source (and
  the rule), the note, and the suggestion kind when the action came from the
  Links tab.
- `links_detail(conn, id)` feeds the panel: the entity's outgoing rows, its
  incoming rows, its dismissals, and its licences.
- A refused write (`AliasError`) is a 400, and a missing entity or row a 404,
  as for the classification routes. The cycle message names the closing rows
  **by name and id**, not by id alone.

**The entity page's admin panel gains a "Links" section**
(`partials/entity_links_panel.html`), shown below Classification:
- **Links from this entity** and **Links to this entity**: the other name
  (linked to its page), source (`format_key`, or the operator's email), scope,
  date, note, and **Unlink**. A manual row is deleted; a rule row gets a
  dismissal in its own scope (the confirm text says so).
- **Dismissed pairs**, each with **Undo**.
- **Same as…** and **Not linked to…** forms:
  - a target field that takes an id, or a name picked from a list that HTMX
    fills from `GET /admin/entities/lookup?q=` (`<datalist>`, 20 names);
  - a scope: *this licence* (a select of the entity's own licences, the
    default) or *everywhere*; *everywhere* only if the entity has no licences;
  - a note.
- **Staying on a variant's page:** `/entity/{id}?stay=1` skips the #263
  redirect **for signed-in admins only**. The panel's links to other entities
  carry it, and so does the panel's own `next`. Public visitors are still
  redirected.

**The review queue gains a "Links" tab** (`/admin/entities/review?tab=links`).
`alias_suggestions.py` computes the suggestions when the tab loads (the spec
lets the builder measure first: the queries below take milliseconds, so no
cache table).

| Kind | Rule | Measured on prod today | Scope |
|---|---|---|---|
| Comma-drop | A `LAST, FIRST` person whose comma-dropped key (`LAST FIRST`, no reorder) is another person group: suggest that group's party → the comma form's party. A *trap* when the natural form `FIRST LAST` also exists. | 73 traps, 19 comma-dropped only, 4 more whose dropped form is itself another comma name's natural form (shown, labelled) | everywhere |
| Legal designator | Organization keys equal once trailing designators (`LLC`, `L L C`, `INC`, `CORP[ORATION]`, `CO[MPANY]`, `LTD`, `LP`, `LLP`, `PLLC`, `PC`, `PS`) are stripped: suggest each other party → the elected one. The election prefers a designator form, then the most links, then the lowest id (Sugar Mountain → 41265). | 10 variants in 10 groups | everywhere |
| Single token | A one-token person with exactly one other person party on a licence whose key contains the token. | 4: `TY`, `HALEY`, `KIRANDEEP`, `SCHAFER` (as the spec measured) | that licence |

- A pair already resolving to one party in that scope is left out, and so is a
  pair with any dismissal or a not-a-name end. Linking or dismissing therefore
  removes the suggestion.
- Each suggestion shows the evidence: its shared licences and records.
- *Link* posts the manual row with the scope pre-filled; *Dismiss* posts a
  dismissal in the same scope. Both return to the tab.
- **Middle-name conflicts (404 groups) are not built here.** The issue says
  they come with the middle-name rule; #265 adds a generator to the same
  registry.

**Not-a-name:** the refusal while the entity is in a manual row already exists
(#262, `record_manual`). The panel shows that reason in place of the button.
Every classification audit row now also carries the entity's **name**, so the
`ASSOCIATES, LLC` step leaves an audit row with a name, as the issue's "Done
when" asks. This is the spec's §4 "from now on" sentence. It overlaps #261's
"audit details carry the name"; #261 keeps the tombstone half.

## Tradeoffs / alternatives

- **Write audit rows inside `entity_aliases.py`:** rejected. #262 kept that
  module free of admin concerns; `classification_review.py` is the pattern.
- **A target picker with no JavaScript** (a GET search that reloads the page
  with candidates): it works, but costs a page load per search; HTMX is
  already vendored and loaded on every page. The id field works without it.
- **Skip the redirect for every signed-in admin** (no `?stay`): rejected. The
  admin would then never see the party page from a variant link, which is
  the page the public sees.
- **Cache suggestions in a derived table** (the spec's other option): not
  needed at this size; revisit when #265 adds 404 conflict groups.
- **No undo for a dismissal** (the issue names three actions): an Unlink of a
  rule row would then be irreversible from the UI, with no CLI either. I
  added `entity.alias_undismiss`; see the open questions.

## Steps

1. `alias_review.py`: link / unlink / dismiss / undismiss with audit rows, and
   `links_detail`. Cycle and refusal messages carry names. Classification
   audit rows gain `name`. Tests first (`tests/test_alias_review.py`).
2. `alias_suggestions.py`: the three generators and their exclusions; fixture
   tests for each kind, plus TY / Sugar Mountain shapes.
3. Routes in `admin_entity_routes.py`: `POST /admin/entities/{id}/alias`,
   `/alias/{alias_id}/unlink`, `/dismiss`, `/dismissal/{id}/undo`, and
   `GET /admin/entities/lookup`. Add the `links` review tab. Route tests,
   including 400s with the message and `next` safety.
4. `app.entity_detail`: `?stay=1` for admins; pass `links_detail` to the
   template. Route tests: an admin stays, the public is redirected.
5. Templates: the Links panel, the tab, the Not-a-name reason. Build the CSS.
6. Docs: ENTITY_ALIASES.md "Operator UI", README route table, SCHEMA/AGENTS
   only if their budgets allow. Audit action list wherever it is documented.
7. Rehearse the "Done when" on a scratch copy of prod through a dev server on
   :8001: Pendergraft steps 3–4, Sugar Mountain (global, then `SMC` scoped to
   450267), `ASSOCIATES, LLC` not-a-name. Check each audit row. Time the Links
   tab.

## Open questions / risks

- **`entity.alias_undismiss`:** kept (operator, 2026-11-26).
- **The worked cases on prod:** rehearsed on the scratch copy, then run on prod
  after shipping (operator, 2026-11-26).
- **Review-tab counts:** the nav shows a count per tab. Computing the Links
  count on every queue load costs one suggestion pass (target < 100 ms). If
  it is slower, the Links tab shows no count until it is opened.
- No migration. Shipping means a restart of `wslcb-web` only.
