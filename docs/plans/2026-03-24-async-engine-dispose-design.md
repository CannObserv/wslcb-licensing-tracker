# Fix AsyncEngine.dispose RuntimeWarning in CLI

**Issue:** #115
**Date:** 2026-03-24

## Goal

Fix `RuntimeWarning: coroutine 'AsyncEngine.dispose' was never awaited` across
all 13 CLI commands, and reduce the repeated engine-create/dispose boilerplate.

## Approved approach

Add a private `_run_with_engine(coro_fn)` helper in `cli.py` that:

1. Creates the engine via `create_engine_from_env()`
2. Runs `coro_fn(engine)` and `engine.dispose()` in a single `asyncio.run()`
3. Returns the coroutine's result

Each command collapses from:

```python
engine = create_engine_from_env()
asyncio.run(some_task(engine))
engine.dispose()  # BUG: never awaited
```

to:

```python
_run_with_engine(some_task)
```

Commands needing local async logic pass a lambda or local `async def`.

## Key decisions

- Helper lives in `cli.py` (CLI-specific; not a general DB utility)
- No signature changes to any public function
- No new module
- `engine.dispose()` always awaited inside the same event loop as the work

## Out of scope

- Refactoring command signatures or grouping
- Moving engine lifecycle to `database.py`
