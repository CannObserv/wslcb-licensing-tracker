# Admin Test Hardening

**Date:** 2026-03-27

## Goal

Clean up test infrastructure so admin tables are truncated between test
sessions, and add real-DB integration tests for admin user CRUD covering both
CLI and web route code paths.

## Context

Three test users (`newguy@example.com`, `dup@example.com`,
`first@example.com`) appeared in the production `admin_users` table on
2026-03-07, created during manual testing of the admin routes extraction (#54)
and database.py retirement (#60) refactors. The existing admin tests use fully
mocked connections and cannot touch a real database, but `_PG_SESSION_TABLES`
in `conftest.py` does not include `admin_users` or `admin_audit_log` — a gap
that would allow stale data to accumulate if integration tests are added.

## Approved Approach (Option B — Separate integration test files)

### Infra change

Add `admin_users` and `admin_audit_log` to `_PG_SESSION_TABLES` in
`tests/conftest.py`.

### New test files

| File | Covers |
|---|---|
| `tests/test_pg_admin_users.py` | Web route DB logic — add, remove, list, duplicate handling, last-admin guard |
| `tests/test_pg_admin_auth.py` | CLI DB logic — add-user, list-users, remove-user, duplicate noop, last-admin rejection |

### What they test

- Real `INSERT`/`SELECT`/`DELETE` against `admin_users` via `pg_conn` fixture
- Call async functions directly (not through Click or FastAPI transport)
- Audit log rows created by `log_action()` for add/remove operations

### What they don't test (already covered by mocks)

- Click argument parsing, exit codes, output formatting
- FastAPI routing, redirects, status codes, auth middleware

## Key decisions

1. **Separate files over inline tests** — follows `test_pg_*` naming
   convention, avoids Click event loop nesting, clear separation of unit vs
   integration.
2. **Direct async calls over transport** — avoids `asyncio.run()` nesting
   with Click and FastAPI `TestClient` lifecycle; existing mocked tests already
   cover the transport layer.
3. **Truncation as safety net** — adding admin tables to `_PG_SESSION_TABLES`
   prevents future data accumulation even if tests commit.

## Out of scope

- Production data cleanup (manual `DELETE` operation)
- `DATABASE_URL` rejection guard in `pg_engine` fixture (not needed given
  mocked admin tests + `TEST_DATABASE_URL` gating)
