"""Tests for address_validator.py — address validation API client.

All tests use httpx.MockTransport / monkeypatching to avoid real network
calls.  Database operations use the in-memory ``db`` fixture.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import logging

import httpx
import pytest

from wslcb_licensing_tracker import address_validator as av


# ── Helpers ─────────────────────────────────────────────────────────

V1_VALIDATE_URL = "https://address-validator.exe.xyz:8000/api/v1/validate"
V1_STANDARDIZE_URL = "https://address-validator.exe.xyz:8000/api/v1/standardize"

_GOOD_VALIDATE_RESPONSE = {
    "address_line_1": "123 MAIN ST",
    "address_line_2": "",
    "city": "SEATTLE",
    "region": "WA",
    "postal_code": "98101",
    "country": "US",
    "validated": "123 MAIN ST  SEATTLE WA 98101",
    "components": {
        "spec": "usps-pub28",
        "spec_version": "unknown",
        "values": {"primary_number": "123", "street_name": "MAIN"},
    },
    "validation": {
        "status": "confirmed",
        "dpv_match_code": "Y",
        "provider": "usps",
    },
    "latitude": 47.6062,
    "longitude": -122.3321,
    "api_version": "1",
}

_NOT_CONFIRMED_RESPONSE = {
    "address_line_1": None,
    "address_line_2": None,
    "city": None,
    "region": None,
    "postal_code": None,
    "country": "US",
    "validated": None,
    "components": None,
    "validation": {
        "status": "not_confirmed",
        "dpv_match_code": "N",
        "provider": "usps",
    },
    "latitude": None,
    "longitude": None,
    "api_version": "1",
}


def _mock_client(response_json: dict, status_code: int = 200) -> httpx.Client:
    """Return an httpx.Client whose POST always returns *response_json*."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = response_json
    client = MagicMock(spec=httpx.Client)
    client.post.return_value = mock_resp
    return client


# ── _load_api_key ────────────────────────────────────────────────────


def test_load_api_key_from_env_var(monkeypatch, tmp_path):
    """Falls back to the environment variable when no ./env file exists."""
    monkeypatch.setattr(av, "_cached_api_key", None)
    monkeypatch.setenv("ADDRESS_VALIDATOR_API_KEY", "env-key-123")
    # Override candidates with a nonexistent path so no file is found
    monkeypatch.setattr(av, "_env_candidates", [tmp_path / "nonexistent.env"])
    assert av._load_api_key() == "env-key-123"


def test_load_api_key_from_file(monkeypatch, tmp_path):
    """Reads the API key from the project-root ./env file."""
    monkeypatch.setattr(av, "_cached_api_key", None)
    monkeypatch.delenv("ADDRESS_VALIDATOR_API_KEY", raising=False)
    env_file = tmp_path / "env"
    env_file.write_text("# comment\nADDRESS_VALIDATOR_API_KEY=file-key-456\n")
    monkeypatch.setattr(av, "_env_candidates", [env_file])
    assert av._load_api_key() == "file-key-456"


# ── validate() ───────────────────────────────────────────────────────


def test_validate_calls_v1_validate_url(monkeypatch):
    """validate() POSTs to /api/v1/validate."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    av.validate("123 MAIN ST, SEATTLE, WA 98101", client=client)
    call_url = client.post.call_args[0][0]
    assert call_url == V1_VALIDATE_URL


def test_validate_sends_address_in_payload(monkeypatch):
    """validate() sends the full raw address string in the request body."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    av.validate("123 MAIN ST, SEATTLE, WA 98101", client=client)
    payload = client.post.call_args[1]["json"]
    assert payload == {"address": "123 MAIN ST, SEATTLE, WA 98101"}


def test_validate_returns_response_on_success(monkeypatch):
    """validate() returns the full response dict on 200."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    result = av.validate("123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert result is not None
    assert result["validation"]["status"] == "confirmed"
    assert result["validation"]["dpv_match_code"] == "Y"
    assert result["latitude"] == 47.6062


def test_validate_returns_none_on_non_200(monkeypatch):
    """validate() returns None when the API returns a non-200 status."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client({"error": "bad_request", "message": "oops"}, status_code=400)
    assert av.validate("bad address", client=client) is None


def test_validate_returns_none_without_api_key(monkeypatch):
    """validate() returns None when no API key is configured."""
    monkeypatch.setattr(av, "_cached_api_key", "")
    assert av.validate("123 MAIN ST") is None


def test_validate_returns_none_on_timeout(monkeypatch):
    """validate() returns None on a timeout."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = MagicMock(spec=httpx.Client)
    client.post.side_effect = httpx.TimeoutException("timed out")
    assert av.validate("123 MAIN ST", client=client) is None


def test_validate_logs_warnings(monkeypatch, caplog):
    """validate() emits a warning log for each entry in the API warnings list."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    response_with_warnings = dict(_GOOD_VALIDATE_RESPONSE, warnings=["ambiguous_input"])
    client = _mock_client(response_with_warnings)
    with caplog.at_level(logging.WARNING, logger="wslcb_licensing_tracker.address_validator"):
        av.validate("123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert any("ambiguous_input" in m for m in caplog.messages)


# ── validate_location() ──────────────────────────────────────────────


def test_validate_location_writes_all_validate_columns(db, monkeypatch):
    """validate_location() writes std_address_line_1/2 and the five new columns on confirmed."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    db.execute(
        "INSERT INTO locations (raw_address, city, state, zip_code) VALUES (?, ?, ?, ?)",
        ("123 MAIN ST, SEATTLE, WA 98101", "SEATTLE", "WA", "98101"),
    )
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?",
        ("123 MAIN ST, SEATTLE, WA 98101",),
    ).fetchone()[0]
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    ok = av.validate_location(db, loc_id, "123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert ok is True
    row = db.execute(
        "SELECT std_address_line_1, std_address_line_2, std_city, std_region,"
        "       std_postal_code, std_country, std_address_string, validation_status,"
        "       dpv_match_code, latitude, longitude, address_validated_at"
        " FROM locations WHERE id = ?",
        (loc_id,),
    ).fetchone()
    assert row["std_address_line_1"] == "123 MAIN ST"
    assert row["std_address_line_2"] == ""
    assert row["std_city"] == "SEATTLE"
    assert row["std_region"] == "WA"
    assert row["std_postal_code"] == "98101"
    assert row["std_country"] == "US"
    assert row["std_address_string"] == "123 MAIN ST  SEATTLE WA 98101"
    assert row["validation_status"] == "confirmed"
    assert row["dpv_match_code"] == "Y"
    assert row["latitude"] == 47.6062
    assert row["longitude"] == -122.3321
    assert row["address_validated_at"] is not None


def test_validate_location_not_confirmed_stores_status_no_validated_at(db, monkeypatch):
    """validate_location() stores validation_status for not_confirmed but leaves address_validated_at NULL."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("999 FAKE ST, NOWHERE, WA 99999",))
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?", ("999 FAKE ST, NOWHERE, WA 99999",)
    ).fetchone()[0]
    client = _mock_client(_NOT_CONFIRMED_RESPONSE)
    ok = av.validate_location(db, loc_id, "999 FAKE ST, NOWHERE, WA 99999", client=client)
    assert ok is False
    row = db.execute(
        "SELECT validation_status, dpv_match_code, address_validated_at FROM locations WHERE id = ?",
        (loc_id,),
    ).fetchone()
    assert row["validation_status"] == "not_confirmed"
    assert row["dpv_match_code"] == "N"
    assert row["address_validated_at"] is None


def test_validate_location_unavailable_returns_false(db, monkeypatch):
    """validate_location() returns False and stores unavailable status when provider is not configured."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    unavailable_response = {
        "address_line_1": None, "address_line_2": None,
        "city": None, "region": None, "postal_code": None,
        "country": "US", "validated": None, "components": None,
        "validation": {"status": "unavailable", "dpv_match_code": None, "provider": None},
        "latitude": None, "longitude": None,
    }
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("123 MAIN ST, SEATTLE, WA",))
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?", ("123 MAIN ST, SEATTLE, WA",)
    ).fetchone()[0]
    client = _mock_client(unavailable_response)
    ok = av.validate_location(db, loc_id, "123 MAIN ST, SEATTLE, WA", client=client)
    assert ok is False
    row = db.execute(
        "SELECT validation_status, address_validated_at FROM locations WHERE id = ?",
        (loc_id,),
    ).fetchone()
    assert row["validation_status"] == "unavailable"
    assert row["address_validated_at"] is None


@pytest.mark.parametrize("bad_country", [
    "USA",   # 3 chars
    "U",     # 1 char
    "",      # empty
    "U1",    # contains digit
    "ÜS",    # non-ASCII letter — isalpha() returns True but isascii() returns False
])
def test_validate_location_rejects_non_alpha2_country(db, monkeypatch, bad_country):
    """validate_location() stores '' when country is not a valid ISO 3166-1 alpha-2 code.

    Covers: wrong length, digit, and non-ASCII Unicode letters that pass
    str.isalpha() but fail str.isascii().
    """
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    bad_response = dict(_GOOD_VALIDATE_RESPONSE, country=bad_country)
    raw = f"123 MAIN ST TEST {repr(bad_country)}, SEATTLE, WA 98101"
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", (raw,))
    loc_id = db.execute("SELECT id FROM locations WHERE raw_address = ?",
                        (raw,)).fetchone()[0]
    client = _mock_client(bad_response)
    av.validate_location(db, loc_id, raw, client=client)
    row = db.execute("SELECT std_country FROM locations WHERE id = ?", (loc_id,)).fetchone()
    assert row["std_country"] == ""


def test_validate_location_skips_empty_address(db, monkeypatch):
    """validate_location() returns False and makes no API call for empty address."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("",))
    loc_id = db.execute("SELECT id FROM locations WHERE raw_address = ?", ("",)).fetchone()[0]
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    ok = av.validate_location(db, loc_id, "", client=client)
    assert ok is False
    client.post.assert_not_called()


def test_validate_location_returns_false_on_api_failure(db, monkeypatch):
    """validate_location() returns False without writing when API fails."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    db.execute(
        "INSERT INTO locations (raw_address) VALUES (?)",
        ("456 FAIL AVE, NOWHERE, WA 99999",),
    )
    loc_id = db.execute("SELECT id FROM locations WHERE raw_address = ?",
                        ("456 FAIL AVE, NOWHERE, WA 99999",)).fetchone()[0]
    client = _mock_client({}, status_code=500)
    ok = av.validate_location(db, loc_id, "456 FAIL AVE, NOWHERE, WA 99999", client=client)
    assert ok is False
    row = db.execute("SELECT address_validated_at FROM locations WHERE id = ?",
                     (loc_id,)).fetchone()
    assert row["address_validated_at"] is None


# ── backfill_addresses() / refresh_addresses() ───────────────────────


def test_backfill_addresses_skips_already_validated_legacy(db, monkeypatch):
    """backfill_addresses() skips locations where address_validated_at is already set
    AND address_standardized_at is also set (fully processed)."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address, address_standardized_at, address_validated_at)"
        " VALUES (?, ?, ?)",
        ("ALREADY DONE, SEATTLE, WA 98101", "2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"),
    )
    db.execute(
        "INSERT INTO locations (raw_address) VALUES (?)",
        ("NEEDS WORK, TACOMA, WA 98401",),
    )
    db.commit()

    calls = []

    def fake_standardize_location(conn, loc_id, raw_address, client=None):
        calls.append(raw_address)
        return True

    monkeypatch.setattr(av, "standardize_location", fake_standardize_location)
    monkeypatch.setattr(av, "validate_location", lambda *a, **kw: False)
    av.backfill_addresses(db)
    assert len(calls) == 1
    assert calls[0] == "NEEDS WORK, TACOMA, WA 98401"


def test_refresh_addresses_processes_all(db, monkeypatch):
    """refresh_addresses() standardizes all locations regardless of current status."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address, address_standardized_at, address_validated_at)"
        " VALUES (?, ?, ?)",
        ("ALREADY DONE, SEATTLE, WA 98101", "2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"),
    )
    db.execute(
        "INSERT INTO locations (raw_address) VALUES (?)",
        ("NEEDS WORK, TACOMA, WA 98401",),
    )
    db.commit()

    calls = []

    def fake_standardize_location(conn, loc_id, raw_address, client=None):
        calls.append(raw_address)
        return True

    monkeypatch.setattr(av, "standardize_location", fake_standardize_location)
    monkeypatch.setattr(av, "validate_location", lambda *a, **kw: False)
    av.refresh_addresses(db)
    assert len(calls) == 2


def test_backfill_addresses_passes_rate_limit(db, monkeypatch):
    """backfill_addresses() forwards rate_limit to _validate_batch()."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    captured = {}

    def fake_batch(conn, rows, label, batch_size=100, rate_limit=0.1):
        captured["rate_limit"] = rate_limit
        return 0

    monkeypatch.setattr(av, "_validate_batch", fake_batch)
    av.backfill_addresses(db, rate_limit=0.5)
    assert captured["rate_limit"] == 0.5


def test_refresh_addresses_passes_rate_limit(db, monkeypatch):
    """refresh_addresses() forwards rate_limit to _validate_batch()."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    captured = {}

    def fake_batch(conn, rows, label, batch_size=100, rate_limit=0.1):
        captured["rate_limit"] = rate_limit
        return 0

    monkeypatch.setattr(av, "_validate_batch", fake_batch)
    av.refresh_addresses(db, rate_limit=0.25)
    assert captured["rate_limit"] == 0.25


# ── refresh_specific_addresses() ─────────────────────────────────────


def test_refresh_specific_addresses_only_processes_given_ids(db, monkeypatch):
    """refresh_specific_addresses() processes only the requested location IDs."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("ADDR ONE, SEATTLE, WA",))
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("ADDR TWO, TACOMA, WA",))
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("ADDR THREE, OLYMPIA, WA",))
    db.commit()
    id1 = db.execute("SELECT id FROM locations WHERE raw_address = ?", ("ADDR ONE, SEATTLE, WA",)).fetchone()[0]
    id3 = db.execute("SELECT id FROM locations WHERE raw_address = ?", ("ADDR THREE, OLYMPIA, WA",)).fetchone()[0]

    calls = []

    def fake_standardize_location(conn, loc_id, raw_address, client=None):
        calls.append(loc_id)
        return True

    monkeypatch.setattr(av, "standardize_location", fake_standardize_location)
    monkeypatch.setattr(av, "validate_location", lambda *a, **kw: False)
    result = av.refresh_specific_addresses(db, [id1, id3])
    assert sorted(calls) == sorted([id1, id3])
    assert result == 2


def test_refresh_specific_addresses_returns_zero_for_empty_list(db, monkeypatch):
    """refresh_specific_addresses() with an empty ID list does nothing and returns 0."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    calls = []

    def fake_standardize_location(conn, loc_id, raw_address, client=None):
        calls.append(loc_id)
        return True

    monkeypatch.setattr(av, "standardize_location", fake_standardize_location)
    monkeypatch.setattr(av, "validate_location", lambda *a, **kw: False)
    result = av.refresh_specific_addresses(db, [])
    assert calls == []
    assert result == 0


def test_refresh_specific_addresses_passes_rate_limit(db, monkeypatch):
    """refresh_specific_addresses() forwards rate_limit to _validate_batch()."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    captured = {}

    def fake_batch(conn, rows, label, batch_size=100, rate_limit=0.1):
        captured["rate_limit"] = rate_limit
        return 0

    monkeypatch.setattr(av, "_validate_batch", fake_batch)
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("ANY ADDR, WA",))
    db.commit()
    loc_id = db.execute("SELECT id FROM locations WHERE raw_address = ?", ("ANY ADDR, WA",)).fetchone()[0]
    av.refresh_specific_addresses(db, [loc_id], rate_limit=0.5)
    assert captured["rate_limit"] == 0.5


# ── _validate_batch() commit frequency ───────────────────────────────


class _TrackingConn:
    """sqlite3.Connection wrapper that counts commit() calls."""

    def __init__(self, conn):
        self._conn = conn
        self.commit_count = 0

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def commit(self):
        self.commit_count += 1
        return self._conn.commit()


def test_validate_batch_commits_after_each_record(db, monkeypatch):
    """_validate_batch() commits after every record to keep write lock hold time near zero.

    Holding the lock open for the entire batch (100 records × 0.1 s = 10 s) causes
    'database is locked' errors on the web app because Python's busy_timeout is 5 s.
    Committing per-record drops the hold time to milliseconds.
    """
    for i in range(3):
        db.execute("INSERT INTO locations (raw_address) VALUES (?)", (f"COMMIT TEST ADDR {i}, WA",))
    db.commit()
    rows = db.execute(
        "SELECT id, raw_address FROM locations WHERE raw_address LIKE 'COMMIT TEST%'"
    ).fetchall()

    def fake_standardize(conn, loc_id, raw, client=None):
        return True

    monkeypatch.setattr(av, "standardize_location", fake_standardize)
    monkeypatch.setattr(av, "validate_location", lambda *a, **kw: False)

    tracking = _TrackingConn(db)
    av._validate_batch(tracking, rows, "Test", batch_size=100, rate_limit=0)

    # With 3 records and batch_size=100, the old code commits exactly once (at the end).
    # The new code should commit at least once per record (3+).
    assert tracking.commit_count >= 3


# ── CLI --location-ids integration ───────────────────────────────────


def test_cmd_refresh_addresses_dispatches_to_refresh_specific_when_file_given(
    tmp_path, monkeypatch
):
    """cmd_refresh_addresses() calls pg_refresh_specific_addresses() when --location-ids is set."""
    import types
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock, patch
    from wslcb_licensing_tracker import cli

    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("101\n202\n303\n")

    called_with = {}

    async def fake_specific(conn, location_ids, rate_limit=0.1):
        called_with["ids"] = location_ids

    async def fake_refresh(conn, rate_limit=0.1):
        called_with["all"] = True

    mock_conn = AsyncMock()

    @asynccontextmanager
    async def fake_get_db(_engine):
        yield mock_conn

    args = types.SimpleNamespace(location_ids=str(ids_file), rate_limit=0.1)
    with patch("wslcb_licensing_tracker.cli.create_engine_from_env", return_value=MagicMock()), \
         patch("wslcb_licensing_tracker.cli.pg_refresh_specific_addresses", fake_specific), \
         patch("wslcb_licensing_tracker.cli.pg_refresh_addresses", fake_refresh), \
         patch("wslcb_licensing_tracker.cli.get_db", fake_get_db):
        cli.cmd_refresh_addresses(args)

    assert called_with.get("ids") == [101, 202, 303]
    assert "all" not in called_with


def test_cmd_refresh_addresses_dispatches_to_refresh_all_without_file(monkeypatch):
    """cmd_refresh_addresses() calls pg_refresh_addresses() when no --location-ids flag."""
    import types
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock, patch
    from wslcb_licensing_tracker import cli

    called_with = {}

    async def fake_specific(conn, location_ids, rate_limit=0.1):
        called_with["specific"] = True

    async def fake_refresh(conn, rate_limit=0.1):
        called_with["all"] = True

    mock_conn = AsyncMock()

    @asynccontextmanager
    async def fake_get_db(_engine):
        yield mock_conn

    args = types.SimpleNamespace(location_ids=None, rate_limit=0.1)
    with patch("wslcb_licensing_tracker.cli.create_engine_from_env", return_value=MagicMock()), \
         patch("wslcb_licensing_tracker.cli.pg_refresh_specific_addresses", fake_specific), \
         patch("wslcb_licensing_tracker.cli.pg_refresh_addresses", fake_refresh), \
         patch("wslcb_licensing_tracker.cli.get_db", fake_get_db):
        cli.cmd_refresh_addresses(args)

    assert called_with.get("all") is True
    assert "specific" not in called_with


# ── standardize() ────────────────────────────────────────────────────

_GOOD_STANDARDIZE_RESPONSE = {
    "address_line_1": "123 MAIN ST",
    "address_line_2": "",
    "city": "SEATTLE",
    "region": "WA",
    "postal_code": "98101",
    "country": "US",
    "standardized": "123 MAIN ST  SEATTLE WA 98101",
    "components": {
        "spec": "usps-pub28",
        "spec_version": "unknown",
        "values": {"primary_number": "123", "street_name": "MAIN"},
    },
    "warnings": [],
    "api_version": "1",
}


def test_standardize_calls_v1_standardize_url(monkeypatch):
    """standardize() POSTs to /api/v1/standardize."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_STANDARDIZE_RESPONSE)
    av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    call_url = client.post.call_args[0][0]
    assert call_url == V1_STANDARDIZE_URL


def test_standardize_sends_address_in_payload(monkeypatch):
    """standardize() sends the full raw address string in the request body."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_STANDARDIZE_RESPONSE)
    av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    payload = client.post.call_args[1]["json"]
    assert payload == {"address": "123 MAIN ST, SEATTLE, WA 98101"}


def test_standardize_returns_response_on_success(monkeypatch):
    """standardize() returns the full response dict on 200."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_STANDARDIZE_RESPONSE)
    result = av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert result is not None
    assert result["standardized"] == "123 MAIN ST  SEATTLE WA 98101"
    assert result["address_line_1"] == "123 MAIN ST"


def test_standardize_returns_none_on_non_200(monkeypatch):
    """standardize() returns None when the API returns a non-200 status."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client({"error": "bad_request"}, status_code=400)
    assert av.standardize("bad address", client=client) is None


def test_standardize_returns_none_without_api_key(monkeypatch):
    """standardize() returns None when no API key is configured."""
    monkeypatch.setattr(av, "_cached_api_key", "")
    assert av.standardize("123 MAIN ST") is None


def test_standardize_returns_none_on_timeout(monkeypatch):
    """standardize() returns None on a timeout."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = MagicMock(spec=httpx.Client)
    client.post.side_effect = httpx.TimeoutException("timed out")
    assert av.standardize("123 MAIN ST", client=client) is None


# ── standardize_location() ───────────────────────────────────────────


def test_standardize_location_writes_std_columns(db, monkeypatch):
    """standardize_location() writes std_* fields, std_address_string, validation_status,
    and sets address_standardized_at; dpv_match_code and lat/lng remain NULL."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address) VALUES (?)",
        ("123 MAIN ST, SEATTLE, WA 98101",),
    )
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?",
        ("123 MAIN ST, SEATTLE, WA 98101",),
    ).fetchone()[0]
    client = _mock_client(_GOOD_STANDARDIZE_RESPONSE)
    ok = av.standardize_location(db, loc_id, "123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert ok is True
    row = db.execute(
        "SELECT std_address_line_1, std_city, std_region, std_postal_code, std_country,"
        "       std_address_string, validation_status, dpv_match_code,"
        "       latitude, longitude, address_standardized_at, address_validated_at"
        " FROM locations WHERE id = ?",
        (loc_id,),
    ).fetchone()
    assert row["std_address_line_1"] == "123 MAIN ST"
    assert row["std_city"] == "SEATTLE"
    assert row["std_region"] == "WA"
    assert row["std_postal_code"] == "98101"
    assert row["std_country"] == "US"
    assert row["std_address_string"] == "123 MAIN ST  SEATTLE WA 98101"
    assert row["validation_status"] == "standardized"
    assert row["dpv_match_code"] is None
    assert row["latitude"] is None
    assert row["longitude"] is None
    assert row["address_standardized_at"] is not None
    assert row["address_validated_at"] is None


def test_standardize_location_skips_empty_address(db, monkeypatch):
    """standardize_location() returns False and makes no API call for empty address."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("",))
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?", ("",)
    ).fetchone()[0]
    client = _mock_client(_GOOD_STANDARDIZE_RESPONSE)
    ok = av.standardize_location(db, loc_id, "", client=client)
    assert ok is False
    client.post.assert_not_called()


def test_standardize_location_returns_false_on_api_failure(db, monkeypatch):
    """standardize_location() returns False without writing when the API fails."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("456 FAIL AVE, WA",))
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?", ("456 FAIL AVE, WA",)
    ).fetchone()[0]
    client = _mock_client({}, status_code=500)
    ok = av.standardize_location(db, loc_id, "456 FAIL AVE, WA", client=client)
    assert ok is False
    row = db.execute(
        "SELECT address_standardized_at FROM locations WHERE id = ?", (loc_id,)
    ).fetchone()
    assert row["address_standardized_at"] is None


# ── validate_location() — ENABLE_ADDRESS_VALIDATION flag ─────────────


def test_validate_location_flag_off_is_noop(db, monkeypatch):
    """validate_location() makes no API call and returns False when flag is off."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.delenv("ENABLE_ADDRESS_VALIDATION", raising=False)
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("123 MAIN ST, SEATTLE, WA",))
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?", ("123 MAIN ST, SEATTLE, WA",)
    ).fetchone()[0]
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    ok = av.validate_location(db, loc_id, "123 MAIN ST, SEATTLE, WA", client=client)
    assert ok is False
    client.post.assert_not_called()
    row = db.execute(
        "SELECT address_validated_at FROM locations WHERE id = ?", (loc_id,)
    ).fetchone()
    assert row["address_validated_at"] is None


def test_validate_location_flag_on_confirmed_overlays_validation(db, monkeypatch):
    """validate_location() overlays std_address_string, dpv_match_code, lat/lng,
    validation_status, and sets address_validated_at when flag is on and confirmed."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    db.execute(
        "INSERT INTO locations (raw_address, city, state, zip_code) VALUES (?, ?, ?, ?)",
        ("123 MAIN ST, SEATTLE, WA 98101", "SEATTLE", "WA", "98101"),
    )
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?",
        ("123 MAIN ST, SEATTLE, WA 98101",),
    ).fetchone()[0]
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    ok = av.validate_location(db, loc_id, "123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert ok is True
    row = db.execute(
        "SELECT std_address_string, validation_status, dpv_match_code,"
        "       latitude, longitude, address_validated_at"
        " FROM locations WHERE id = ?",
        (loc_id,),
    ).fetchone()
    assert row["std_address_string"] == "123 MAIN ST  SEATTLE WA 98101"
    assert row["validation_status"] == "confirmed"
    assert row["dpv_match_code"] == "Y"
    assert row["latitude"] == 47.6062
    assert row["longitude"] == -122.3321
    assert row["address_validated_at"] is not None


def test_validate_location_flag_on_not_confirmed_leaves_validated_at_null(db, monkeypatch):
    """validate_location() leaves address_validated_at NULL for not_confirmed when flag is on."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    monkeypatch.setenv("ENABLE_ADDRESS_VALIDATION", "true")
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("999 FAKE ST, WA 99999",))
    loc_id = db.execute(
        "SELECT id FROM locations WHERE raw_address = ?", ("999 FAKE ST, WA 99999",)
    ).fetchone()[0]
    client = _mock_client(_NOT_CONFIRMED_RESPONSE)
    ok = av.validate_location(db, loc_id, "999 FAKE ST, WA 99999", client=client)
    assert ok is False
    row = db.execute(
        "SELECT validation_status, address_validated_at FROM locations WHERE id = ?",
        (loc_id,),
    ).fetchone()
    assert row["validation_status"] == "not_confirmed"
    assert row["address_validated_at"] is None


# ── backfill_addresses() — new filter semantics ───────────────────────


def test_backfill_addresses_skips_fully_processed_locations(db, monkeypatch):
    """backfill_addresses() skips locations where both timestamps are set."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address, address_standardized_at, address_validated_at)"
        " VALUES (?, ?, ?)",
        ("FULLY DONE, SEATTLE, WA 98101", "2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"),
    )
    db.execute(
        "INSERT INTO locations (raw_address) VALUES (?)",
        ("NEEDS WORK, TACOMA, WA 98401",),
    )
    db.commit()

    calls = []

    def fake_standardize_location(conn, loc_id, raw_address, client=None):
        calls.append(raw_address)
        return True

    monkeypatch.setattr(av, "standardize_location", fake_standardize_location)
    monkeypatch.setattr(av, "validate_location", lambda *a, **kw: False)
    av.backfill_addresses(db)
    assert len(calls) == 1
    assert calls[0] == "NEEDS WORK, TACOMA, WA 98401"


def test_backfill_addresses_includes_standardized_but_not_validated(db, monkeypatch):
    """backfill_addresses() processes locations standardized but not yet validated
    (address_validated_at IS NULL) so they can be validated when the flag is on."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address, address_standardized_at) VALUES (?, ?)",
        ("STANDARDIZED ONLY, SEATTLE, WA 98101", "2025-01-01T00:00:00+00:00"),
    )
    db.commit()

    calls = []

    def fake_standardize_location(conn, loc_id, raw_address, client=None):
        calls.append(raw_address)
        return True

    monkeypatch.setattr(av, "standardize_location", fake_standardize_location)
    monkeypatch.setattr(av, "validate_location", lambda *a, **kw: False)
    av.backfill_addresses(db)
    assert calls == ["STANDARDIZED ONLY, SEATTLE, WA 98101"]
