# Address Validator v2 Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the `/api/v1/standardize` call with `/api/v1/validate` to capture USPS DPV confirmation status, lat/long, and validated address string; rename `address_line_1`/`address_line_2` to `std_address_line_1`/`std_address_line_2`; add `--rate-limit` to backfill/refresh CLI commands.

**Architecture:** New migration `_m013` renames two columns and adds five new nullable columns on `locations`. `address_validator.py` replaces `standardize()` with `validate()` (same call pattern, different URL and richer response). `validate_location()` uses a two-tier write: when the provider confirms an address it writes all columns; for `not_confirmed`/`unavailable` it stores the status only and returns `False` so `backfill-addresses` retries. The COALESCE fallback pattern in `queries.py` and `enrich_record()` is unchanged.

**Tech Stack:** Python 3.12, SQLite (RENAME COLUMN requires ≥3.25 — already in prod), httpx, pytest, uv

**Worktree:** `.worktrees/feat/address-validator-v2`

**Run all tests:** `uv run pytest tests/ -v` (546 passing baseline)

---

### Task 1: Schema migration — rename address_line columns, add new columns

**Files:**
- Modify: `src/wslcb_licensing_tracker/schema.py`
- Test: `tests/test_schema.py`

**Step 1: Write the failing test for `_m001_baseline` column names**

Open `tests/test_schema.py`. Find the test around line 450 that checks `address_line_1`/`address_line_2` in a fresh DB. Add a new test (alongside existing ones):

```python
def test_fresh_db_has_std_address_line_columns(db):
    """Fresh DB baseline has std_address_line_1 and std_address_line_2, not the old unprefixed names."""
    cols = [row[1] for row in db.execute("PRAGMA table_info(locations)").fetchall()]
    assert "std_address_line_1" in cols
    assert "std_address_line_2" in cols
    assert "address_line_1" not in cols
    assert "address_line_2" not in cols

def test_fresh_db_has_validate_columns(db):
    """Fresh DB baseline includes all five new /validate columns."""
    cols = [row[1] for row in db.execute("PRAGMA table_info(locations)").fetchall()]
    for col in ("validated_address", "validation_status", "dpv_match_code", "latitude", "longitude"):
        assert col in cols, f"Missing column: {col}"
```

**Step 2: Run to verify they fail**

```bash
cd .worktrees/feat/address-validator-v2
uv run pytest tests/test_schema.py::test_fresh_db_has_std_address_line_columns tests/test_schema.py::test_fresh_db_has_validate_columns -v
```
Expected: FAIL — `std_address_line_1` not in cols / `validated_address` not in cols.

**Step 3: Update `_m001_baseline()` in `schema.py`**

In `_m001_baseline()`, find the `CREATE TABLE IF NOT EXISTS locations` block and change:
- `address_line_1 TEXT DEFAULT '',` → `std_address_line_1 TEXT DEFAULT '',`
- `address_line_2 TEXT DEFAULT '',` → `std_address_line_2 TEXT DEFAULT '',`

Then add the five new columns after `std_country TEXT DEFAULT '',`:
```sql
validated_address TEXT,
validation_status TEXT,
dpv_match_code TEXT,
latitude REAL,
longitude REAL,
```

**Step 4: Write the migration function**

After `_m012_entities_name_index`, add:

```python
def _m013_address_validator_v2(conn: sqlite3.Connection) -> None:
    """Rename address_line_1/2 to std_address_line_1/2; add validate columns.

    Renames the two unprefixed address line columns to use the std_ prefix
    (consistent with std_city, std_region, etc.), and adds five columns to
    store results from the new /api/v1/validate endpoint.
    """
    if not _table_exists(conn, "locations"):
        return

    # Rename address_line_1 -> std_address_line_1
    if _column_exists(conn, "locations", "address_line_1") and not _column_exists(conn, "locations", "std_address_line_1"):
        conn.execute("ALTER TABLE locations RENAME COLUMN address_line_1 TO std_address_line_1")

    # Rename address_line_2 -> std_address_line_2
    if _column_exists(conn, "locations", "address_line_2") and not _column_exists(conn, "locations", "std_address_line_2"):
        conn.execute("ALTER TABLE locations RENAME COLUMN address_line_2 TO std_address_line_2")

    # Add new validate columns (each guarded for idempotency)
    new_cols = [
        ("validated_address", "TEXT"),
        ("validation_status", "TEXT"),
        ("dpv_match_code", "TEXT"),
        ("latitude", "REAL"),
        ("longitude", "REAL"),
    ]
    for col, col_type in new_cols:
        if not _column_exists(conn, "locations", col):
            conn.execute(f"ALTER TABLE locations ADD COLUMN {col} {col_type}")
```

**Step 5: Register migration in `MIGRATIONS` list**

Append to `MIGRATIONS`:
```python
(13, "address_validator_v2", _m013_address_validator_v2),
```

**Step 6: Write migration test for existing DB**

In `tests/test_schema.py`, add:

```python
def test_m013_renames_address_line_columns_and_adds_validate_cols():
    """_m013 renames address_line_1/2 → std_address_line_1/2 and adds 5 new columns."""
    from wslcb_licensing_tracker.schema import _m013_address_validator_v2
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # Create locations table using OLD column names (pre-migration state)
    conn.execute("""
        CREATE TABLE locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_address TEXT NOT NULL,
            city TEXT DEFAULT '',
            state TEXT DEFAULT 'WA',
            zip_code TEXT DEFAULT '',
            address_line_1 TEXT DEFAULT '',
            address_line_2 TEXT DEFAULT '',
            std_city TEXT DEFAULT '',
            std_region TEXT DEFAULT '',
            std_postal_code TEXT DEFAULT '',
            std_country TEXT DEFAULT '',
            address_validated_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(raw_address)
        )
    """)
    conn.execute("INSERT INTO locations (raw_address, address_line_1) VALUES ('123 MAIN ST', '123 MAIN ST')")
    conn.commit()

    _m013_address_validator_v2(conn)

    cols = [row[1] for row in conn.execute("PRAGMA table_info(locations)").fetchall()]
    assert "std_address_line_1" in cols
    assert "std_address_line_2" in cols
    assert "address_line_1" not in cols
    assert "address_line_2" not in cols
    for col in ("validated_address", "validation_status", "dpv_match_code", "latitude", "longitude"):
        assert col in cols

    # Verify data was preserved under new name
    row = conn.execute("SELECT std_address_line_1 FROM locations").fetchone()
    assert row[0] == "123 MAIN ST"
    conn.close()


def test_m013_is_idempotent():
    """Running _m013 twice does not raise."""
    from wslcb_licensing_tracker.schema import _m013_address_validator_v2
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_address TEXT NOT NULL,
            address_line_1 TEXT DEFAULT '',
            address_line_2 TEXT DEFAULT '',
            UNIQUE(raw_address)
        )
    """)
    _m013_address_validator_v2(conn)
    _m013_address_validator_v2(conn)  # should not raise
    conn.close()
```

**Step 7: Run all schema tests**

```bash
uv run pytest tests/test_schema.py -v
```
Expected: All pass. Note: two existing tests (around lines 454–455) that assert `address_line_1` is in a pre-migration fixture table are fine — they test old DB upgrade paths, not fresh DB.

**Step 8: Commit**

```bash
git add src/wslcb_licensing_tracker/schema.py tests/test_schema.py
git commit -m "feat: migration 013 — rename address_line cols, add validate columns"
```

---

### Task 2: Replace `standardize()` with `validate()` in `address_validator.py`

**Files:**
- Modify: `src/wslcb_licensing_tracker/address_validator.py`
- Test: `tests/test_address_validator.py`

**Step 1: Write failing tests for `validate()`**

In `tests/test_address_validator.py`, add a new constant for the validate API fixture at the top of the file (after `_GOOD_RESPONSE`):

```python
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
```

Add new test functions for `validate()`:

```python
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
```

Add new tests for `validate_location()` new behaviour:

```python
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
```

Add tests for rate_limit parameter:

```python
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
```

**Step 2: Run to verify the new tests fail**

```bash
uv run pytest tests/test_address_validator.py::test_validate_calls_v1_validate_url \
    tests/test_address_validator.py::test_validate_location_writes_all_validate_columns \
    tests/test_address_validator.py::test_validate_location_not_confirmed_stores_status_no_validated_at \
    -v
```
Expected: FAIL — `av` has no `validate` attribute yet.

**Step 3: Implement `validate()` in `address_validator.py`**

Add after the `standardize()` function (keep `standardize()` intact for now — removed in Step 5):

```python
def validate(address: str, client: httpx.Client | None = None) -> dict | None:
    """Validate an address via POST /api/v1/validate.

    Sends the full raw address string. The server runs parse → standardize
    internally before calling the USPS DPV provider.

    Args:
        address: The raw address string to validate.
        client: Optional httpx.Client to reuse for connection pooling.

    Returns:
        A dict with keys (address_line_1, address_line_2, city, region,
        postal_code, country, validated, components, validation, latitude,
        longitude) on success, or None on any failure (network error,
        non-200 status, timeout). Note: a 200 response with
        validation.status='not_confirmed' or 'unavailable' is returned
        as a dict (not None) — the caller decides how to handle it.
    """
    api_key = _load_api_key()
    if not api_key:
        return None

    url = f"{BASE_URL}/api/v1/validate"
    headers = {"X-API-Key": api_key}
    payload = {"address": address}

    try:
        if client is not None:
            response = client.post(url, json=payload, headers=headers)
        else:
            response = httpx.post(url, json=payload, headers=headers, timeout=TIMEOUT)

        if response.status_code != 200:
            logger.warning(
                "Address validation API returned status %d for: %s",
                response.status_code, address,
            )
            return None

        data = response.json()
        for warn in data.get("warnings") or []:
            logger.warning("Address API warning for %r: %s", address, warn)
        return data

    except httpx.TimeoutException:
        logger.warning("Timeout calling address validation API for: %s", address)
        return None
    except httpx.HTTPError as e:
        logger.warning("HTTP error calling address validation API: %s", e)
        return None
    except Exception as e:
        logger.warning("Unexpected error calling address validation API: %s", e)
        return None
```

**Step 4: Update `validate_location()` to call `validate()` and write new columns**

Replace the body of `validate_location()` with:

```python
def validate_location(
    conn: sqlite3.Connection,
    location_id: int,
    raw_address: str,
    client: httpx.Client | None = None,
) -> bool:
    """Validate and update a single location row via POST /api/v1/validate.

    On confirmed/corrected response: writes all std_* and new validate
    columns, sets address_validated_at. On not_confirmed/unavailable:
    writes validation_status and dpv_match_code only, leaves
    address_validated_at NULL so backfill-addresses will retry.

    Does NOT commit — the caller is responsible for committing.
    Skips (returns False) if raw_address is empty or None.

    Returns:
        True if address_validated_at was set (confirmed/corrected), False otherwise.
    """
    if not raw_address or not raw_address.strip():
        return False

    result = validate(raw_address, client=client)
    if result is None:
        return False

    validation = result.get("validation") or {}
    status = validation.get("status", "")
    dpv = validation.get("dpv_match_code")

    # address_line_1 is None when provider could not confirm the address
    has_address = result.get("address_line_1") is not None

    try:
        if has_address:
            raw_country = result.get("country", "")
            std_country = (
                raw_country
                if (len(raw_country) == 2 and raw_country.isalpha() and raw_country.isascii())
                else ""
            )
            conn.execute(
                """UPDATE locations SET
                    std_address_line_1 = ?, std_address_line_2 = ?,
                    std_city = ?, std_region = ?, std_postal_code = ?, std_country = ?,
                    validated_address = ?, validation_status = ?, dpv_match_code = ?,
                    latitude = ?, longitude = ?,
                    address_validated_at = ?
                WHERE id = ?""",
                (
                    result.get("address_line_1", ""),
                    result.get("address_line_2", ""),
                    result.get("city", ""),
                    result.get("region", ""),
                    result.get("postal_code", ""),
                    std_country,
                    result.get("validated"),
                    status,
                    dpv,
                    result.get("latitude"),
                    result.get("longitude"),
                    datetime.now(timezone.utc).isoformat(),
                    location_id,
                ),
            )
            return True
        else:
            # not_confirmed or unavailable: store status so we know validation
            # was attempted, but leave address_validated_at NULL for retry.
            conn.execute(
                "UPDATE locations SET validation_status = ?, dpv_match_code = ? WHERE id = ?",
                (status, dpv, location_id),
            )
            return False
    except Exception as e:
        logger.warning("Failed to update location %d: %s", location_id, e)
        return False
```

**Step 5: Add `rate_limit` parameter to `_validate_batch()`, `backfill_addresses()`, `refresh_addresses()`**

In `_validate_batch()`, change the signature and `time.sleep` line:
```python
def _validate_batch(
    conn: sqlite3.Connection,
    rows: list,
    label: str,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> int:
```
Replace `time.sleep(0.05)` with `time.sleep(rate_limit)`.

In `backfill_addresses()`:
```python
def backfill_addresses(conn: sqlite3.Connection, batch_size: int = 100, rate_limit: float = 0.1) -> int:
```
Update the `_validate_batch` call: `return _validate_batch(conn, rows, "Backfilling addresses", batch_size=batch_size, rate_limit=rate_limit)`

In `refresh_addresses()`:
```python
def refresh_addresses(conn: sqlite3.Connection, batch_size: int = 100, rate_limit: float = 0.1) -> int:
```
Update the `_validate_batch` call similarly.

**Step 6: Remove `standardize()` from `address_validator.py`**

Delete the entire `standardize()` function. It is no longer called anywhere.

**Step 7: Remove/replace old `test_standardize_*` tests**

The existing `test_standardize_*` tests (lines 72–148) test the removed function. Replace them with equivalents that test `validate()`. In practice, the new tests in Step 1 above already cover the same behaviours — delete the old `test_standardize_*` block entirely.

Also update the existing `test_validate_location_writes_renamed_columns` test to use `_GOOD_VALIDATE_RESPONSE` and check `std_address_line_1`/`std_address_line_2` plus the new columns. The new test `test_validate_location_writes_all_validate_columns` added in Step 1 is the replacement — delete the old one.

Similarly update `test_validate_location_rejects_non_alpha2_country` to use `_GOOD_VALIDATE_RESPONSE` as base instead of `_GOOD_RESPONSE`.

**Step 8: Run all address validator tests**

```bash
uv run pytest tests/test_address_validator.py -v
```
Expected: All pass.

**Step 9: Commit**

```bash
git add src/wslcb_licensing_tracker/address_validator.py tests/test_address_validator.py
git commit -m "feat: replace standardize() with validate(), add rate_limit, write new columns"
```

---

### Task 3: Update `queries.py` column references

**Files:**
- Modify: `src/wslcb_licensing_tracker/queries.py`
- Test: `tests/test_queries.py`

**Step 1: Check for failing query tests first**

```bash
uv run pytest tests/test_queries.py -v
```
Some tests may already be failing after the schema migration (if they insert into `locations` using old column names). Fix as you go.

**Step 2: Update `RECORD_COLUMNS` (the shared SELECT constant near line 59)**

Replace all four occurrences of `address_line_1` / `address_line_2` in the column list:

```sql
-- Change:
COALESCE(loc.address_line_1, '') AS address_line_1,
COALESCE(loc.address_line_2, '') AS address_line_2,
-- To:
COALESCE(loc.std_address_line_1, '') AS std_address_line_1,
COALESCE(loc.std_address_line_2, '') AS std_address_line_2,
```

Do the same for the `ploc` aliases:
```sql
-- Change:
COALESCE(ploc.address_line_1, '') AS prev_address_line_1,
COALESCE(ploc.address_line_2, '') AS prev_address_line_2,
-- To:
COALESCE(ploc.std_address_line_1, '') AS prev_std_address_line_1,
COALESCE(ploc.std_address_line_2, '') AS prev_std_address_line_2,
```

**Step 3: Update `_EXPORT_SELECT` (near line 367)**

Same four replacements in the export SQL constant. The export aliases should also update:
- `AS address_line_1` → `AS std_address_line_1`
- `AS address_line_2` → `AS std_address_line_2`
- `AS prev_address_line_1` → `AS prev_std_address_line_1`
- `AS prev_address_line_2` → `AS prev_std_address_line_2`

**Step 4: Run query tests**

```bash
uv run pytest tests/test_queries.py -v
```
Expected: All pass. If any test fails due to referencing `address_line_1` in an expected dict key, update the key to `std_address_line_1`.

**Step 5: Commit**

```bash
git add src/wslcb_licensing_tracker/queries.py tests/test_queries.py
git commit -m "refactor: rename address_line_1/2 → std_address_line_1/2 in queries"
```

---

### Task 4: Add `--rate-limit` to CLI commands

**Files:**
- Modify: `src/wslcb_licensing_tracker/cli.py`

**Step 1: Write failing test**

In `tests/test_address_validator.py`, the `test_backfill_addresses_passes_rate_limit` and `test_refresh_addresses_passes_rate_limit` tests written in Task 2 already cover the function-level behaviour. No additional test needed here — verify CLI argument parsing manually.

**Step 2: Update `cmd_backfill_addresses()` and `cmd_refresh_addresses()` in `cli.py`**

```python
def cmd_backfill_addresses(args):
    """Validate un-validated locations via the address API."""
    from wslcb_licensing_tracker.db import get_db
    from wslcb_licensing_tracker.schema import init_db
    from wslcb_licensing_tracker.address_validator import backfill_addresses
    init_db()
    with get_db() as conn:
        backfill_addresses(conn, rate_limit=args.rate_limit)


def cmd_refresh_addresses(args):
    """Re-validate all locations via the address API."""
    from wslcb_licensing_tracker.db import get_db
    from wslcb_licensing_tracker.schema import init_db
    from wslcb_licensing_tracker.address_validator import refresh_addresses
    init_db()
    with get_db() as conn:
        refresh_addresses(conn, rate_limit=args.rate_limit)
```

**Step 3: Add `--rate-limit` argument to both subparsers**

In the `backfill-addresses` parser block:
```python
p = sub.add_parser(
    "backfill-addresses",
    help="Validate un-validated locations via the address API",
)
p.add_argument(
    "--rate-limit",
    type=float,
    default=0.1,
    metavar="SECONDS",
    help="Seconds to sleep between API calls (default: 0.1)",
)
p.set_defaults(func=cmd_backfill_addresses)
```

Same for the `refresh-addresses` parser block.

**Step 4: Run all tests**

```bash
uv run pytest tests/ -v
```
Expected: All 546+ tests pass.

**Step 5: Commit**

```bash
git add src/wslcb_licensing_tracker/cli.py
git commit -m "feat: add --rate-limit to backfill-addresses and refresh-addresses CLI commands"
```

---

### Task 5: Update `detail.html` template

**Files:**
- Modify: `templates/detail.html`

**Step 1: Find all references**

The template references these keys (around lines 85–141):
- `record.address_line_1` → `record.std_address_line_1`
- `record.address_line_2` → `record.std_address_line_2`
- `record.prev_address_line_1` → `record.prev_std_address_line_1`
- `record.prev_address_line_2` → `record.prev_std_address_line_2`

**Step 2: Replace all four variable names in `detail.html`**

Open `templates/detail.html` and make the four rename operations. There are 6 total occurrences across lines 85–141.

**Step 3: Run full test suite**

```bash
uv run pytest tests/ -v
```
Expected: All tests pass.

**Step 4: Commit**

```bash
git add templates/detail.html
git commit -m "refactor: update detail.html to use std_address_line_1/2 column names"
```

---

### Task 6: Final verification

**Step 1: Run full test suite**

```bash
uv run pytest tests/ -v
```
Expected: All tests pass (≥546).

**Step 2: Smoke-test the migration on the real DB**

```bash
# From the project root (not the worktree) to test against real data/wslcb.db
uv run python -c "
from wslcb_licensing_tracker.schema import init_db, migrate
from wslcb_licensing_tracker.db import get_db
migrate()
with get_db() as conn:
    cols = [r[1] for r in conn.execute('PRAGMA table_info(locations)').fetchall()]
    print('New cols present:', all(c in cols for c in ['std_address_line_1', 'validated_address', 'validation_status', 'latitude']))
    print('Old cols gone:', 'address_line_1' not in cols)
"
```
Expected output:
```
New cols present: True
Old cols gone: True
```

**Step 3: Commit and push**

The work is ready to ship. Use `shipping-work-claude` skill.
