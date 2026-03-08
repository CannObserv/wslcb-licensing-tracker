"""Tests for address_validator.py — address validation API client.

All tests use httpx.MockTransport / monkeypatching to avoid real network
calls.  Database operations use the in-memory ``db`` fixture.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import httpx
import pytest

import address_validator as av


# ── Helpers ─────────────────────────────────────────────────────────

V1_STANDARDIZE_URL = "https://address-validator.exe.xyz:8000/api/v1/standardize"

_GOOD_RESPONSE = {
    "address_line_1": "123 MAIN ST",
    "address_line_2": "",
    "city": "SEATTLE",
    "region": "WA",
    "postal_code": "98101",
    "country": "US",
    "standardized": "123 MAIN ST, SEATTLE, WA 98101",
    "components": {
        "spec": "usps-pub28",
        "spec_version": "unknown",
        "values": {"primary_number": "123", "street_name": "MAIN"},
    },
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


# ── standardize() ───────────────────────────────────────────────────


def test_standardize_calls_v1_url(monkeypatch):
    """standardize() POSTs to /api/v1/standardize, not the deprecated /api/standardize."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_RESPONSE)
    result = av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    call_url = client.post.call_args[0][0]
    assert call_url == V1_STANDARDIZE_URL


def test_standardize_returns_correct_fields(monkeypatch):
    """standardize() returns the full v1 response dict on success."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_RESPONSE)
    result = av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert result is not None
    assert result["region"] == "WA"
    assert result["postal_code"] == "98101"
    assert result["country"] == "US"
    assert result["address_line_1"] == "123 MAIN ST"


def test_standardize_returns_none_on_non_200(monkeypatch):
    """standardize() returns None when the API returns a non-200 status."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client({"error": "bad_request", "message": "oops"}, status_code=400)
    result = av.standardize("bad address", client=client)
    assert result is None


def test_standardize_returns_none_without_api_key(monkeypatch):
    """standardize() returns None when no API key is configured."""
    monkeypatch.setattr(av, "_cached_api_key", "")
    result = av.standardize("123 MAIN ST")
    assert result is None


def test_standardize_returns_none_on_timeout(monkeypatch):
    """standardize() returns None on a timeout."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = MagicMock(spec=httpx.Client)
    client.post.side_effect = httpx.TimeoutException("timed out")
    result = av.standardize("123 MAIN ST", client=client)
    assert result is None


# ── standardize() — warnings ────────────────────────────────────────


def test_standardize_logs_warnings(monkeypatch, caplog):
    """standardize() emits a warning log for each entry in the API warnings list."""
    import logging
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    response_with_warnings = dict(_GOOD_RESPONSE, warnings=["ambiguous_input", "address_not_found"])
    client = _mock_client(response_with_warnings)
    with caplog.at_level(logging.WARNING, logger="address_validator"):
        av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    messages = [r.message for r in caplog.records]
    assert any("ambiguous_input" in m for m in messages)
    assert any("address_not_found" in m for m in messages)


def test_standardize_no_log_when_warnings_absent(monkeypatch, caplog):
    """standardize() emits no warning logs when the warnings field is absent."""
    import logging
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    client = _mock_client(_GOOD_RESPONSE)  # _GOOD_RESPONSE has no 'warnings' key
    with caplog.at_level(logging.WARNING, logger="address_validator"):
        av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert caplog.records == []


def test_standardize_no_log_when_warnings_empty(monkeypatch, caplog):
    """standardize() emits no warning logs when warnings is an empty list."""
    import logging
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    response_empty_warnings = dict(_GOOD_RESPONSE, warnings=[])
    client = _mock_client(response_empty_warnings)
    with caplog.at_level(logging.WARNING, logger="address_validator"):
        av.standardize("123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert caplog.records == []


# ── validate_location() ──────────────────────────────────────────────


def test_validate_location_writes_renamed_columns(db, monkeypatch):
    """validate_location() writes std_region, std_postal_code, std_country."""
    monkeypatch.setattr(av, "_cached_api_key", "test-key")
    db.execute(
        "INSERT INTO locations (raw_address, city, state, zip_code) VALUES (?, ?, ?, ?)",
        ("123 MAIN ST, SEATTLE, WA 98101", "SEATTLE", "WA", "98101"),
    )
    loc_id = db.execute("SELECT id FROM locations WHERE raw_address = ?",
                        ("123 MAIN ST, SEATTLE, WA 98101",)).fetchone()[0]
    client = _mock_client(_GOOD_RESPONSE)
    ok = av.validate_location(db, loc_id, "123 MAIN ST, SEATTLE, WA 98101", client=client)
    assert ok is True
    row = db.execute(
        "SELECT std_region, std_postal_code, std_country, std_city, address_validated_at"
        " FROM locations WHERE id = ?", (loc_id,)
    ).fetchone()
    assert row["std_region"] == "WA"
    assert row["std_postal_code"] == "98101"
    assert row["std_country"] == "US"
    assert row["std_city"] == "SEATTLE"
    assert row["address_validated_at"] is not None


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
    bad_response = dict(_GOOD_RESPONSE, country=bad_country)
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
    client = _mock_client(_GOOD_RESPONSE)
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
