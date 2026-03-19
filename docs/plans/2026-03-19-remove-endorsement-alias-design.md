# Remove Endorsement Alias — Design

**Date:** 2026-03-19

## Goal

Allow admins to remove an incorrect alias from the Endorsement List tab, making the variant endorsement standalone again.

## Approved Approach

Option A: inline `<details>` confirmation popover, consistent with the existing "Rename" action pattern.

## Key Decisions

- **Action placement:** "Remove alias" `<details>` block added to the Actions column, visible only on variant rows (`is_variant=True`).
- **Post-removal state:** The variant endorsement becomes standalone (no re-aliasing prompted).
- **Confirmation:** Inline `<details>` popover with a brief message and a destructive "Confirm" button. No record count shown.
- **FTS sync:** After alias deletion, the route reprocesses `resolved_endorsements` for all `license_records` linked to the unaliased endorsement (via `process_record()` for each affected record ID from `record_endorsements`). Done synchronously in the same request before redirect.
- **Audit:** Action logged via `admin_audit.log_action()`. Caller commits.
- **Filter cache:** Invalidated after removal.

## Implementation Scope

### `endorsements.py`
- New function `remove_alias(conn, endorsement_id, removed_by)`: deletes the `endorsement_aliases` row for the given variant. Raises if no alias exists. Caller commits.

### `admin_routes.py`
- New route `POST /admin/endorsements/unalias`. Accepts `endorsement_id`. Validates endorsement exists and is a variant. Calls `remove_alias()`, reprocesses `resolved_endorsements` for all affected records, logs audit, invalidates filter cache, commits, redirects with flash.

### `templates/admin/endorsements.html`
- In the Actions column, add a `<details>` block rendered only when `is_variant=True`. Summary: "Remove alias". Body: confirmation message + form POSTing to `/admin/endorsements/unalias` with a red "Confirm" submit button.

## Out of Scope

- Removing aliases from the canonical side (only variant rows get this action).
- Any additional UX after removal (no re-alias prompt).
