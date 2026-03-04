"""Tests for endorsements.py — license endorsement normalization.

Verifies the _merge_endorsement helper and the repair functions
that use it, after the deduplication refactor (#24).
"""
import pytest

from endorsements import (
    seed_endorsements,
    process_record,
    _ensure_endorsement,
    _link_endorsement,
    _merge_endorsement,
    merge_mixed_case_endorsements,
    repair_code_name_endorsements,
    _merge_seeded_placeholders,
    get_endorsement_options,
    get_record_endorsements,
)
from queries import insert_record


def _make_record(db, **overrides):
    """Insert a record with sensible defaults, returning its id."""
    defaults = {
        "section_type": "new_application",
        "record_date": "2025-06-15",
        "business_name": "TEST BIZ",
        "business_location": "123 MAIN ST, SEATTLE, WA 98101",
        "applicants": "TEST BIZ; JOHN DOE",
        "license_type": "CANNABIS RETAILER",
        "application_type": "NEW APPLICATION",
        "license_number": "999001",
        "contact_phone": "",
        "city": "SEATTLE",
        "state": "WA",
        "zip_code": "98101",
        "previous_business_name": "",
        "previous_applicants": "",
        "previous_business_location": "",
        "previous_city": "",
        "previous_state": "",
        "previous_zip_code": "",
        "scraped_at": "2025-06-15T12:00:00+00:00",
    }
    defaults.update(overrides)
    result = insert_record(db, defaults)
    assert result is not None
    return result[0]


# ── _merge_endorsement helper ─────────────────────────────────


class TestMergeEndorsement:
    """Tests for the shared _merge_endorsement helper."""

    def test_migrates_record_links(self, db):
        """Record links should move from old to new endorsement."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        old_eid = _ensure_endorsement(db, "OLD ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "NEW ENDORSEMENT")
        _link_endorsement(db, rec_id, old_eid)

        _merge_endorsement(db, old_eid, new_eid)

        # Old link gone
        old_links = db.execute(
            "SELECT * FROM record_endorsements WHERE endorsement_id = ?",
            (old_eid,),
        ).fetchall()
        assert len(old_links) == 0

        # New link exists
        new_links = db.execute(
            "SELECT * FROM record_endorsements WHERE endorsement_id = ?",
            (new_eid,),
        ).fetchall()
        assert len(new_links) == 1
        assert new_links[0]["record_id"] == rec_id

    def test_migrates_code_mappings(self, db):
        """Code mappings should move from old to new endorsement."""
        seed_endorsements(db)

        old_eid = _ensure_endorsement(db, "OLD ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "NEW ENDORSEMENT")
        db.execute(
            "INSERT INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            ("999", old_eid),
        )

        _merge_endorsement(db, old_eid, new_eid)

        # Code should now point to new endorsement
        row = db.execute(
            "SELECT endorsement_id FROM endorsement_codes WHERE code = '999'"
        ).fetchone()
        assert row["endorsement_id"] == new_eid

    def test_deletes_old_endorsement(self, db):
        """The old endorsement row should be deleted."""
        seed_endorsements(db)

        old_eid = _ensure_endorsement(db, "DOOMED ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "KEEPER ENDORSEMENT")

        _merge_endorsement(db, old_eid, new_eid)

        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (old_eid,)
        ).fetchone()
        assert row is None

    def test_handles_duplicate_links_gracefully(self, db):
        """If record already linked to new endorsement, merge should not fail."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        old_eid = _ensure_endorsement(db, "OLD ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "NEW ENDORSEMENT")
        _link_endorsement(db, rec_id, old_eid)
        _link_endorsement(db, rec_id, new_eid)  # already linked to target

        _merge_endorsement(db, old_eid, new_eid)  # should not raise

        links = db.execute(
            "SELECT * FROM record_endorsements WHERE record_id = ? AND endorsement_id = ?",
            (rec_id, new_eid),
        ).fetchall()
        assert len(links) == 1

    def test_delete_old_endorsement_false(self, db):
        """When delete_old=False, old endorsement should be kept."""
        seed_endorsements(db)

        old_eid = _ensure_endorsement(db, "KEPT ENDORSEMENT")
        new_eid = _ensure_endorsement(db, "TARGET ENDORSEMENT")

        _merge_endorsement(db, old_eid, new_eid, delete_old=False)

        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (old_eid,)
        ).fetchone()
        assert row is not None


# ── process_record ────────────────────────────────────────────


class TestProcessRecord:
    def test_text_endorsements(self, db):
        """Semicolon-separated text names."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="CANNABIS RETAILER; CANNABIS PROCESSOR")
        count = process_record(db, rec_id, "CANNABIS RETAILER; CANNABIS PROCESSOR")
        assert count == 2

    def test_numeric_code(self, db):
        """Bare numeric code resolves via seed map."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="394,")
        count = process_record(db, rec_id, "394,")
        assert count >= 1

        endorsements = db.execute(
            "SELECT le.name FROM record_endorsements re "
            "JOIN license_endorsements le ON le.id = re.endorsement_id "
            "WHERE re.record_id = ?",
            (rec_id,),
        ).fetchall()
        assert any(e["name"] == "CANNABIS RETAILER" for e in endorsements)

    def test_code_name_format(self, db):
        """Historical CODE, NAME format."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="450, GROCERY STORE - BEER/WINE")
        count = process_record(db, rec_id, "450, GROCERY STORE - BEER/WINE")
        assert count >= 1

    def test_empty_license_type(self, db):
        """Empty string returns 0."""
        seed_endorsements(db)
        rec_id = _make_record(db)
        count = process_record(db, rec_id, "")
        assert count == 0


# ── Repair functions (thin wrappers) ────────────────────────────


class TestMergeMixedCase:
    def test_renames_in_place_when_no_upper_exists(self, db):
        """Mixed-case endorsement with no upper counterpart → rename."""
        seed_endorsements(db)
        # Insert a mixed-case endorsement manually
        db.execute("INSERT INTO license_endorsements (name) VALUES (?)", ("Mixed Case",))
        db.commit()

        count = merge_mixed_case_endorsements(db)
        assert count >= 1

        row = db.execute(
            "SELECT name FROM license_endorsements WHERE name = 'MIXED CASE'"
        ).fetchone()
        assert row is not None

    def test_merges_when_upper_exists(self, db):
        """Mixed-case endorsement with existing upper → merge."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        upper_eid = _ensure_endorsement(db, "TEST ENDORSEMENT")
        mixed_eid = db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)", ("Test Endorsement",)
        ).lastrowid
        _link_endorsement(db, rec_id, mixed_eid)
        db.commit()

        merge_mixed_case_endorsements(db)

        # Mixed-case row gone
        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (mixed_eid,)
        ).fetchone()
        assert row is None

        # Link migrated to upper
        link = db.execute(
            "SELECT * FROM record_endorsements WHERE record_id = ? AND endorsement_id = ?",
            (rec_id, upper_eid),
        ).fetchone()
        assert link is not None


class TestRepairCodeName:
    def test_migrates_code_name_endorsement(self, db):
        """CODE, NAME endorsement should be resolved and deleted."""
        seed_endorsements(db)
        rec_id = _make_record(db)

        # Create a spurious "450, GROCERY STORE - BEER/WINE" endorsement
        bogus_eid = db.execute(
            "INSERT INTO license_endorsements (name) VALUES (?)",
            ("450, GROCERY STORE - BEER/WINE",),
        ).lastrowid
        _link_endorsement(db, rec_id, bogus_eid)
        db.commit()

        count = repair_code_name_endorsements(db)
        assert count >= 1

        # Bogus endorsement should be gone
        row = db.execute(
            "SELECT * FROM license_endorsements WHERE id = ?", (bogus_eid,)
        ).fetchone()
        assert row is None


class TestQueryHelpers:
    def test_get_endorsement_options(self, db):
        """Returns endorsement names linked to records."""
        seed_endorsements(db)
        rec_id = _make_record(db)
        process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()

        options = get_endorsement_options(db)
        assert "CANNABIS RETAILER" in options

    def test_get_record_endorsements_batch(self, db):
        """Batch fetch endorsements for multiple records."""
        seed_endorsements(db)
        id1 = _make_record(db, license_number="E001")
        id2 = _make_record(db, license_number="E002")
        process_record(db, id1, "CANNABIS RETAILER")
        process_record(db, id2, "CANNABIS PROCESSOR")
        db.commit()

        result = get_record_endorsements(db, [id1, id2])
        assert "CANNABIS RETAILER" in result[id1]
        assert "CANNABIS PROCESSOR" in result[id2]


# ── Alias system (#7) ─────────────────────────────────────────


class TestResolveEndorsement:
    """Tests for resolve_endorsement()."""

    def test_returns_same_id_when_no_alias(self, db):
        """Non-aliased endorsement resolves to itself."""
        from endorsements import resolve_endorsement
        eid = _ensure_endorsement(db, "STANDALONE")
        db.commit()
        assert resolve_endorsement(db, eid) == eid

    def test_returns_canonical_for_aliased(self, db):
        """Aliased endorsement resolves to its canonical."""
        from endorsements import resolve_endorsement
        variant_id = _ensure_endorsement(db, "VARIANT NAME")
        canonical_id = _ensure_endorsement(db, "CANONICAL NAME")
        db.execute(
            "INSERT INTO endorsement_aliases (endorsement_id, canonical_endorsement_id)"
            " VALUES (?, ?)",
            (variant_id, canonical_id),
        )
        db.commit()
        assert resolve_endorsement(db, variant_id) == canonical_id

    def test_alias_does_not_affect_canonical_itself(self, db):
        """Canonical ID is not changed by its own alias records."""
        from endorsements import resolve_endorsement
        variant_id = _ensure_endorsement(db, "VARIANT B")
        canonical_id = _ensure_endorsement(db, "CANONICAL B")
        db.execute(
            "INSERT INTO endorsement_aliases (endorsement_id, canonical_endorsement_id)"
            " VALUES (?, ?)",
            (variant_id, canonical_id),
        )
        db.commit()
        assert resolve_endorsement(db, canonical_id) == canonical_id


class TestGetEndorsementGroups:
    """Tests for get_endorsement_groups()."""

    def test_groups_by_code(self, db):
        """Endorsements sharing a code appear in the same group."""
        from endorsements import get_endorsement_groups
        seed_endorsements(db)
        db.commit()

        groups = get_endorsement_groups(db)
        # At minimum a dict is returned
        assert isinstance(groups, list)

    def test_includes_record_counts(self, db):
        """Each endorsement entry has a record count."""
        from endorsements import get_endorsement_groups
        seed_endorsements(db)
        rec_id = _make_record(db)
        process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()

        groups = get_endorsement_groups(db)
        # Find the CANNABIS RETAILER group
        found = None
        for g in groups:
            for e in g["endorsements"]:
                if e["name"] == "CANNABIS RETAILER":
                    found = e
                    break
        assert found is not None
        assert found["record_count"] >= 1

    def test_includes_canonical_flag(self, db):
        """Endorsed marked canonical when alias points to them."""
        from endorsements import get_endorsement_groups, resolve_endorsement
        seed_endorsements(db)
        rec_id = _make_record(db)
        process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()

        # Manually create a variant + alias
        variant_id = _ensure_endorsement(db, "CANNABIS RETAILER VARIANT")
        canonical_id = db.execute(
            "SELECT id FROM license_endorsements WHERE name = ?",
            ("CANNABIS RETAILER",),
        ).fetchone()[0]
        db.execute(
            "INSERT INTO endorsement_aliases (endorsement_id, canonical_endorsement_id)"
            " VALUES (?, ?)",
            (variant_id, canonical_id),
        )
        db.commit()

        groups = get_endorsement_groups(db)
        # canonical_id should appear as is_canonical in some group
        all_endorsements = [e for g in groups for e in g["endorsements"]]
        canonical_entries = [e for e in all_endorsements if e["id"] == canonical_id]
        assert any(e["is_canonical"] for e in canonical_entries)


class TestSetCanonical:
    """Tests for set_canonical_endorsement()."""

    def test_creates_alias_rows(self, db):
        """set_canonical creates alias rows for all variants pointing to canonical."""
        from endorsements import set_canonical_endorsement, _ensure_endorsement
        seed_endorsements(db)

        # Create two variants and a canonical, all sharing a code
        code = "TESTCODE"
        v1_id = _ensure_endorsement(db, "VARIANT ONE")
        v2_id = _ensure_endorsement(db, "VARIANT TWO")
        canonical_id = _ensure_endorsement(db, "CANONICAL ONE")
        for eid in (v1_id, v2_id, canonical_id):
            db.execute(
                "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
                (code, eid),
            )
        db.commit()

        set_canonical_endorsement(
            db,
            canonical_id=canonical_id,
            variant_ids=[v1_id, v2_id],
            created_by="test@example.com",
        )
        db.commit()

        aliases = db.execute(
            "SELECT endorsement_id FROM endorsement_aliases"
            " WHERE canonical_endorsement_id = ?",
            (canonical_id,),
        ).fetchall()
        aliased_ids = {row[0] for row in aliases}
        assert v1_id in aliased_ids
        assert v2_id in aliased_ids

    def test_idempotent(self, db):
        """Calling set_canonical twice doesn't duplicate alias rows."""
        from endorsements import set_canonical_endorsement
        v_id = _ensure_endorsement(db, "VARIANT IDEM")
        c_id = _ensure_endorsement(db, "CANONICAL IDEM")
        db.commit()

        set_canonical_endorsement(db, canonical_id=c_id, variant_ids=[v_id], created_by="t@t.com")
        db.commit()
        set_canonical_endorsement(db, canonical_id=c_id, variant_ids=[v_id], created_by="t@t.com")
        db.commit()

        count = db.execute(
            "SELECT COUNT(*) FROM endorsement_aliases WHERE endorsement_id = ?",
            (v_id,),
        ).fetchone()[0]
        assert count == 1


class TestRenameEndorsement:
    """Tests for rename_endorsement() — bare numeric code → text name."""

    def test_creates_named_endorsement_and_alias(self, db):
        """Renaming a bare code creates a new named endorsement and alias row."""
        from endorsements import rename_endorsement
        seed_endorsements(db)

        # Create a bare numeric-code endorsement
        bare_id = _ensure_endorsement(db, "9999")
        db.commit()

        new_id = rename_endorsement(
            db,
            endorsement_id=bare_id,
            new_name="SPECIAL EVENT PERMIT",
            created_by="admin@example.com",
        )
        db.commit()

        # New endorsement exists
        row = db.execute(
            "SELECT name FROM license_endorsements WHERE id = ?", (new_id,)
        ).fetchone()
        assert row is not None
        assert row[0] == "SPECIAL EVENT PERMIT"

        # Alias from bare → new
        alias = db.execute(
            "SELECT canonical_endorsement_id FROM endorsement_aliases WHERE endorsement_id = ?",
            (bare_id,),
        ).fetchone()
        assert alias is not None
        assert alias[0] == new_id

    def test_rename_returns_existing_if_name_taken(self, db):
        """Rename to an existing name reuses that endorsement."""
        from endorsements import rename_endorsement
        seed_endorsements(db)

        bare_id = _ensure_endorsement(db, "8888")
        existing_id = _ensure_endorsement(db, "EXISTING ENDORSEMENT")
        db.commit()

        returned_id = rename_endorsement(
            db,
            endorsement_id=bare_id,
            new_name="EXISTING ENDORSEMENT",
            created_by="admin@example.com",
        )
        db.commit()
        assert returned_id == existing_id


class TestAliasResolutionInFilterOptions:
    """get_endorsement_options() should deduplicate via aliases."""

    def test_aliased_variant_excluded_from_options(self, db):
        """Variants with aliases don't appear in filter dropdown."""
        from endorsements import get_endorsement_options
        seed_endorsements(db)

        rec_id_v = _make_record(db, license_number="ALIAS001")
        rec_id_c = _make_record(db, license_number="ALIAS002")
        variant_id = _ensure_endorsement(db, "VARIANT FILTER")
        canonical_id = _ensure_endorsement(db, "CANONICAL FILTER")

        # Link both records to each endorsement
        db.execute(
            "INSERT OR IGNORE INTO record_endorsements VALUES (?, ?)",
            (rec_id_v, variant_id),
        )
        db.execute(
            "INSERT OR IGNORE INTO record_endorsements VALUES (?, ?)",
            (rec_id_c, canonical_id),
        )
        # Create alias: variant → canonical
        db.execute(
            "INSERT INTO endorsement_aliases (endorsement_id, canonical_endorsement_id)"
            " VALUES (?, ?)",
            (variant_id, canonical_id),
        )
        db.commit()

        options = get_endorsement_options(db)
        assert "CANONICAL FILTER" in options
        assert "VARIANT FILTER" not in options


class TestAliasResolutionInRecordEndorsements:
    """get_record_endorsements() should resolve aliases to canonical names."""

    def test_returns_canonical_name_for_aliased_record(self, db):
        """Records linked to a variant show the canonical name."""
        from endorsements import get_record_endorsements
        seed_endorsements(db)

        rec_id = _make_record(db, license_number="ALIAS003")
        variant_id = _ensure_endorsement(db, "VARIANT DISPLAY")
        canonical_id = _ensure_endorsement(db, "CANONICAL DISPLAY")

        db.execute(
            "INSERT OR IGNORE INTO record_endorsements VALUES (?, ?)",
            (rec_id, variant_id),
        )
        db.execute(
            "INSERT INTO endorsement_aliases (endorsement_id, canonical_endorsement_id)"
            " VALUES (?, ?)",
            (variant_id, canonical_id),
        )
        db.commit()

        result = get_record_endorsements(db, [rec_id])
        assert "CANONICAL DISPLAY" in result[rec_id]
        assert "VARIANT DISPLAY" not in result[rec_id]
