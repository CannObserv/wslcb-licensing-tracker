"""Tests for data_migration.py — data-only migration framework."""

from unittest.mock import AsyncMock, MagicMock, patch


def _make_conn_always_applied():
    """Return a fake connection where the SELECT check returns a row (already applied)."""
    conn = AsyncMock()
    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = "applied"  # truthy → skip
    conn.execute.return_value = select_result
    conn.commit = AsyncMock()
    return conn


def _make_conn_not_applied():
    """Return a fake connection where the SELECT check returns None (not yet applied)."""
    conn = AsyncMock()
    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = None  # not applied → run
    conn.execute.return_value = select_result
    conn.commit = AsyncMock()
    return conn


def _fake_engine_from_conns(conns: list):
    """Return a MagicMock engine whose connect() yields connections in order."""
    conn_iter = iter(conns)

    class _CM:
        def __init__(self):
            self._c = next(conn_iter)

        async def __aenter__(self):
            return self._c

        async def __aexit__(self, *args):
            return False

    engine = MagicMock()
    engine.connect.side_effect = lambda: _CM()
    return engine


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

    mock_fns = [AsyncMock() for _ in range(6)]
    patched_migrations = list(zip(all_names, mock_fns))

    # One connection per migration, all reporting "already applied"
    conns = [_make_conn_always_applied() for _ in range(6)]
    fake_engine = _fake_engine_from_conns(conns)

    from wslcb_licensing_tracker import data_migration

    with patch.object(data_migration, "_MIGRATIONS", patched_migrations):
        await data_migration.run_pending_migrations(fake_engine)

    for fn in mock_fns:
        fn.assert_not_called()
    for conn in conns:
        conn.commit.assert_not_called()


async def test_run_pending_migrations_applies_pending():
    """When data_migrations is empty, all 6 fns are called and each conn committed."""
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

    # One connection per migration, all reporting "not yet applied"
    conns = [_make_conn_not_applied() for _ in range(6)]
    fake_engine = _fake_engine_from_conns(conns)

    from wslcb_licensing_tracker import data_migration

    with patch.object(data_migration, "_MIGRATIONS", patched_migrations):
        await data_migration.run_pending_migrations(fake_engine)

    for fn in mock_fns:
        fn.assert_called_once()

    # Each connection must have been committed after the fn ran
    for conn in conns:
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
