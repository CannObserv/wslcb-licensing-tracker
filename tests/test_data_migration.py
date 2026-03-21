"""Tests for data_migration.py — data-only migration framework."""

from unittest.mock import AsyncMock, MagicMock, patch


async def test_run_pending_migrations_all_applied():
    """When all 6 migrations are already in data_migrations, no fn is called."""
    all_names = [
        "0001_seed_endorsements",
        "0002_repair_code_name_endorsements",
        "0003_merge_mixed_case_endorsements",
        "0004_backfill_endorsements",
        "0005_backfill_entities",
        "0006_build_record_links",
    ]

    # Build a fake connection that returns all names from the SELECT
    fake_conn = AsyncMock()
    select_result = MagicMock()
    select_result.fetchall.return_value = [(n,) for n in all_names]
    fake_conn.execute.return_value = select_result

    # engine.connect() is an async context manager
    fake_engine = MagicMock()
    fake_engine.connect.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
    fake_engine.connect.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_fns = [MagicMock() for _ in range(6)]
    patched_migrations = list(zip(all_names, mock_fns))

    from wslcb_licensing_tracker import data_migration

    with patch.object(data_migration, "_MIGRATIONS", patched_migrations):
        await data_migration.run_pending_migrations(fake_engine)

    for fn in mock_fns:
        fn.assert_not_called()


async def test_run_pending_migrations_applies_pending():
    """When data_migrations is empty, all 6 fns are called and 6 rows are inserted."""
    all_names = [
        "0001_seed_endorsements",
        "0002_repair_code_name_endorsements",
        "0003_merge_mixed_case_endorsements",
        "0004_backfill_endorsements",
        "0005_backfill_entities",
        "0006_build_record_links",
    ]

    mock_fns = [AsyncMock() for _ in range(6)]
    patched_migrations = list(zip(all_names, mock_fns))

    call_count = 0
    insert_calls = []

    def make_conn():
        """Return a fresh AsyncMock connection each time engine.connect() is entered."""
        conn = AsyncMock()

        async def execute_side_effect(stmt, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            # First call per outer context is the SELECT (returns empty list)
            result.fetchall.return_value = []
            # Track insert stmt objects
            insert_calls.append(stmt)
            return result

        conn.execute.side_effect = execute_side_effect
        conn.commit = AsyncMock()
        return conn

    # The first engine.connect() call (for SELECT) returns empty applied set.
    # Subsequent calls (one per migration) each get their own conn.
    connections = [make_conn() for _ in range(7)]  # 1 initial + 6 migrations
    conn_iter = iter(connections)

    class FakeContextManager:
        def __init__(self):
            self._conn = next(conn_iter)

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *args):
            return False

    fake_engine = MagicMock()
    fake_engine.connect.side_effect = lambda: FakeContextManager()

    # Patch SELECT to return empty on first conn, and patch pg_insert to track inserts
    first_conn = connections[0]
    select_result = MagicMock()
    select_result.fetchall.return_value = []
    first_conn.execute.side_effect = None
    first_conn.execute.return_value = select_result

    from wslcb_licensing_tracker import data_migration

    with patch.object(data_migration, "_MIGRATIONS", patched_migrations):
        await data_migration.run_pending_migrations(fake_engine)

    for fn in mock_fns:
        fn.assert_called_once()

    # Each of the 6 migration connections should have called commit
    for conn in connections[1:7]:
        conn.commit.assert_called_once()


async def test_get_record_link_returns_none_when_no_row():
    """get_record_link returns None when no matching row exists."""
    conn = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = None
    conn.execute.return_value = result

    from wslcb_licensing_tracker.pg_queries import get_record_link

    assert await get_record_link(conn, 999) is None


async def test_get_entity_by_id_returns_none_when_not_found():
    """get_entity_by_id returns None when no entity with the given id exists."""
    conn = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = None
    conn.execute.return_value = result

    from wslcb_licensing_tracker.pg_entities import get_entity_by_id

    assert await get_entity_by_id(conn, 12345) is None
