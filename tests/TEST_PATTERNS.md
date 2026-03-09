# Test Patterns Reference

## 1. Fixture Setup (`conftest.py`)

### Database fixture — `db`
- Uses **in-memory SQLite** (`:memory:`) for speed — no disk I/O, no network.
- Calls `get_connection(":memory:")` then `init_db(conn)` to build full schema.
- Yielded to the test; closed in teardown.
- Reference by name: `def test_foo(self, db):`

### Sample record dict fixtures
- Five fixtures provide canonical record dicts matching the shape from `parser.parse_records_from_table`:
  - `standard_new_application` — plain NEW APPLICATION
  - `assumption_record` — ASSUMPTION with buyer/seller (previous_*) fields
  - `change_of_location_record` — CHANGE OF LOCATION with previous address
  - `approved_numeric_code` — approved section, bare numeric license_type
  - `discontinued_code_name` — discontinued section, "CODE, NAME" license_type
- Every dict has **all 18 fields** populated (empty string for unused fields, never `None`).
- Dates are `"YYYY-MM-DD"` strings; `scraped_at` is ISO-8601 with timezone.

### Path helper
- `FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"` — constant (not a fixture) for locating HTML fixture files.

## 2. Naming Conventions

| Element | Convention | Examples |
|---------|-----------|----------|
| Test files | `test_<module>.py` | `test_schema.py`, `test_pipeline.py` |
| Test classes | `class Test<Feature>:` (no `unittest.TestCase`) | `TestUserVersion`, `TestIngestRecord`, `TestMigrate` |
| Test methods | `test_<behavior_description>` (snake_case, descriptive) | `test_default_is_zero`, `test_duplicate_returns_existing` |
| Docstrings | Every test method has a one-line docstring explaining intent | `"""Duplicate records should return is_new=False."""` |
| Module docstring | Describes scope, notes in-memory SQLite usage | `"""Tests for pipeline.py — unified ingestion pipeline."""` |

## 3. Assertion Style

- **Plain `assert` statements** — no `self.assertEqual`, no `pytest.raises` wrappers (for value checks).
- Direct equality/membership checks:
  ```python
  assert result.is_new is True
  assert result.record_id is not None
  assert r1.record_id == r2.record_id
  assert "JOHN DOE" in names
  assert len(endorsements) > 0
  ```
- Use `is True` / `is False` / `is None` / `is not None` (identity, not equality).
- Failure messages only on non-obvious assertions:
  ```python
  assert "locations" not in tables, "baseline ran when it should have been skipped"
  ```

## 4. Import Style

- **Local imports inside test methods** for the module-under-test:
  ```python
  def test_inserts_new_record(self, db, standard_new_application):
      from pipeline import ingest_record, IngestOptions
  ```
- Top-of-file imports only for test infrastructure (`pytest`, `sqlite3`) and shared helpers (`endorsements.seed_endorsements`).
- This keeps each test self-documenting about its dependencies.

## 5. Common Test Patterns

### Seed before test
Many pipeline tests call `seed_endorsements(db)` as the first line to populate lookup tables.

### Options object to disable side-effects
```python
opts = IngestOptions(validate_addresses=False, link_outcomes=False)
```
Tests disable expensive/network features via options, enabling only what's under test.

### Direct SQL verification
Results are verified by querying the DB directly rather than through application code:
```python
endorsements = db.execute(
    "SELECT le.name FROM record_endorsements re "
    "JOIN license_endorsements le ON le.id = re.endorsement_id "
    "WHERE re.record_id = ?",
    (result.record_id,),
).fetchall()
```

### Dict spread for record variants
```python
approved = {
    **approved_numeric_code,
    "license_number": standard_new_application["license_number"],
    "record_date": "2025-06-17",
}
```
Override specific fields from fixture dicts using `{**base, key: val}` syntax.

### Row-as-dict access
SQLite rows support dict-style access: `link["role"]`, `e["name"]`.

## 6. Template for a New Test

```python
"""Tests for <module>.py — <brief description>.

All tests use in-memory SQLite via the ``db`` fixture.
"""
import pytest


class TestFeatureName:
    """<What this group tests>."""

    def test_happy_path(self, db, standard_new_application):
        """<Expected behavior in plain English>."""
        from module_under_test import function_under_test

        # Arrange
        seed_endorsements(db)  # if needed
        # ...

        # Act
        result = function_under_test(db, standard_new_application)

        # Assert
        assert result is not None
        assert result.some_field == expected_value
```
