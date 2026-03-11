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
    # Point the module's __file__ to a tmp dir so no ./env file is found
    monkeypatch.setattr(av, "__file__", str(tmp_path / "address_validator.py"))
    assert av._load_api_key() == "env-key-123"


def test_load_api_key_from_file(monkeypatch, tmp_path):
    """Reads the API key from the ./env file."""
    monkeypatch.setattr(av, "_cached_api_key", None)
    env_file = tmp_path / "env"
    env_file.write_text("# comment\nADDRESS_VALIDATOR_API_KEY=file-key-456\n")
    monkeypatch.setattr(av, "__file__", str(tmp_path / "address_validator.py"))
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
        "       std_postal_code, std_country, validated_address, validation_status,"
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
    assert row["validated_address"] == "123 MAIN ST  SEATTLE WA 98101"
    assert row["validation_status"] == "confirmed"
    assert row["dpv_match_code"] == "Y"
    assert row["latitude"] == 47.6062
    assert row["longitude"] == -122.3321
    assert row["address_validated_at"] is not None


def test_validate_location_not_confirmed_stores_status_no_validated_at(db, monkeypatch):
    """validate_location() stores validation_status for not_confirmed but leaves address_validated_at NULL."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
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
    db.execute("INSERT INTO locations (raw_address) VALUES (?)", ("",))
    loc_id = db.execute("SELECT id FROM locations WHERE raw_address = ?", ("",)).fetchone()[0]
    client = _mock_client(_GOOD_VALIDATE_RESPONSE)
    ok = av.validate_location(db, loc_id, "", client=client)
    assert ok is False
    client.post.assert_not_called()


def test_validate_location_returns_false_on_api_failure(db, monkeypatch):
    """validate_location() returns False without writing when API fails."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
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


def test_backfill_addresses_only_processes_unvalidated(db, monkeypatch):
    """backfill_addresses() skips already-validated locations."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address, address_validated_at) VALUES (?, ?)",
        ("ALREADY DONE, SEATTLE, WA 98101", "2025-01-01T00:00:00+00:00"),
    )
    db.execute(
        "INSERT INTO locations (raw_address) VALUES (?)",
        ("NEEDS VALIDATION, TACOMA, WA 98401",),
    )
    db.commit()

    calls = []
    def fake_validate_location(conn, loc_id, raw_address, client=None):
        calls.append(raw_address)
        return True

    monkeypatch.setattr(av, "validate_location", fake_validate_location)
    av.backfill_addresses(db)
    assert len(calls) == 1
    assert calls[0] == "NEEDS VALIDATION, TACOMA, WA 98401"


def test_refresh_addresses_processes_all(db, monkeypatch):
    """refresh_addresses() validates all locations regardless of validation status."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address, address_validated_at) VALUES (?, ?)",
        ("ALREADY DONE, SEATTLE, WA 98101", "2025-01-01T00:00:00+00:00"),
    )
    db.execute(
        "INSERT INTO locations (raw_address) VALUES (?)",
        ("NEEDS VALIDATION, TACOMA, WA 98401",),
    )
    db.commit()

    calls = []
    def fake_validate_location(conn, loc_id, raw_address, client=None):
        calls.append(raw_address)
        return True

    monkeypatch.setattr(av, "validate_location", fake_validate_location)
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
