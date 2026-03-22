"""Tests for pg_scraper.py — pure helper logic."""

import pytest
from wslcb_licensing_tracker.pg_scraper import compute_content_hash, get_last_content_hash


def test_compute_content_hash_is_deterministic():
    """compute_content_hash returns the same hash for the same input."""
    assert compute_content_hash("<html>test</html>") == compute_content_hash("<html>test</html>")


def test_get_last_content_hash_is_importable():
    """pg_scraper.get_last_content_hash is an async function."""
    import inspect
    assert inspect.iscoroutinefunction(get_last_content_hash)
