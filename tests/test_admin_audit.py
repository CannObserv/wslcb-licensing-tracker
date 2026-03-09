"""Tests for admin_audit.py — log_action and get_audit_log."""
import pytest

from wslcb_licensing_tracker.admin_audit import get_audit_log, log_action


# ---------------------------------------------------------------------------
# log_action
# ---------------------------------------------------------------------------


def test_log_action_inserts_row(db):
    """log_action inserts a row and returns the new id."""
    row_id = log_action(db, "admin@example.com", "endorsement.rename", "endorsement")
    db.commit()
    assert isinstance(row_id, int) and row_id > 0
    row = db.execute("SELECT * FROM admin_audit_log WHERE id = ?", (row_id,)).fetchone()
    assert row is not None
    assert row[1] == "admin@example.com"  # admin_email
    assert row[2] == "endorsement.rename"  # action
    assert row[3] == "endorsement"        # target_type


def test_log_action_with_target_id(db):
    """log_action stores target_id when provided."""
    row_id = log_action(
        db, "admin@example.com", "endorsement.set_canonical", "endorsement",
        target_id=42,
    )
    db.commit()
    row = db.execute("SELECT target_id FROM admin_audit_log WHERE id = ?", (row_id,)).fetchone()
    assert row[0] == 42


def test_log_action_none_details_stores_null(db):
    """log_action with details=None stores NULL in the DB."""
    row_id = log_action(
        db, "admin@example.com", "admin_user.add", "admin_user", details=None
    )
    db.commit()
    row = db.execute("SELECT details FROM admin_audit_log WHERE id = ?", (row_id,)).fetchone()
    assert row[0] is None


def test_log_action_dict_details_round_trips(db):
    """log_action serialises dict details to JSON; get_audit_log deserialises it."""
    payload = {"old": "FOO", "new": "BAR", "count": 3}
    row_id = log_action(
        db, "admin@example.com", "endorsement.rename", "endorsement",
        target_id=7, details=payload,
    )
    db.commit()
    rows, _ = get_audit_log(db)
    match = next(r for r in rows if r["id"] == row_id)
    assert match["details_parsed"] == payload


# ---------------------------------------------------------------------------
# get_audit_log — pagination
# ---------------------------------------------------------------------------


def _seed(db, n: int = 5) -> list[int]:
    """Insert *n* audit rows and return their ids."""
    ids = []
    for i in range(n):
        rid = log_action(
            db,
            f"user{i}@example.com",
            "endorsement.rename" if i % 2 == 0 else "admin_user.add",
            "endorsement" if i % 2 == 0 else "admin_user",
            target_id=i,
        )
        ids.append(rid)
    db.commit()
    return ids


def test_get_audit_log_returns_all(db):
    """get_audit_log returns correct total_count."""
    _seed(db, 5)
    rows, total = get_audit_log(db, per_page=100)
    assert total == 5
    assert len(rows) == 5


def test_get_audit_log_pagination(db):
    """get_audit_log paginates correctly."""
    _seed(db, 5)
    rows_p1, total = get_audit_log(db, page=1, per_page=3)
    rows_p2, _ = get_audit_log(db, page=2, per_page=3)
    assert total == 5
    assert len(rows_p1) == 3
    assert len(rows_p2) == 2
    # No overlap
    ids_p1 = {r["id"] for r in rows_p1}
    ids_p2 = {r["id"] for r in rows_p2}
    assert ids_p1.isdisjoint(ids_p2)


def test_get_audit_log_ordered_newest_first(db):
    """Rows are returned newest-first (descending id)."""
    _seed(db, 3)
    rows, _ = get_audit_log(db)
    ids = [r["id"] for r in rows]
    assert ids == sorted(ids, reverse=True)


# ---------------------------------------------------------------------------
# get_audit_log — filters
# ---------------------------------------------------------------------------


def test_filter_by_action(db):
    """Filtering by action returns only matching rows."""
    _seed(db, 5)
    rows, total = get_audit_log(db, filters={"action": "admin_user.add"})
    assert total == 2  # indices 1, 3 are admin_user.add
    assert all(r["action"] == "admin_user.add" for r in rows)


def test_filter_by_target_type(db):
    """Filtering by target_type returns only matching rows."""
    _seed(db, 5)
    rows, total = get_audit_log(db, filters={"target_type": "endorsement"})
    assert total == 3  # indices 0, 2, 4 are endorsement
    assert all(r["target_type"] == "endorsement" for r in rows)


def test_filter_by_admin_email(db):
    """Filtering by admin_email (case-insensitive) returns only matching rows."""
    _seed(db, 5)
    rows, total = get_audit_log(db, filters={"admin_email": "USER0@EXAMPLE.COM"})
    assert total == 1
    assert rows[0]["admin_email"] == "user0@example.com"


def test_filter_by_date_from(db):
    """date_from filter excludes rows before that date."""
    _seed(db, 3)
    rows_all, total_all = get_audit_log(db)
    # Use the created_at of the first (oldest) row as date_from for the 2nd
    oldest_date = rows_all[-1]["created_at"][:10]  # YYYY-MM-DD
    rows, total = get_audit_log(db, filters={"date_from": oldest_date})
    assert total == total_all  # all rows on or after oldest date


def test_filter_by_date_to(db):
    """date_to filter excludes rows after that date."""
    _seed(db, 3)
    from datetime import date
    today = date.today().isoformat()
    rows, total = get_audit_log(db, filters={"date_to": today})
    assert total == 3


def test_filter_no_matches_returns_empty(db):
    """Filters that match nothing return empty list and zero total."""
    _seed(db, 3)
    rows, total = get_audit_log(db, filters={"action": "nonexistent.action"})
    assert total == 0
    assert rows == []


def test_empty_table_returns_empty(db):
    """get_audit_log on an empty table returns empty list, total 0."""
    rows, total = get_audit_log(db)
    assert rows == []
    assert total == 0
