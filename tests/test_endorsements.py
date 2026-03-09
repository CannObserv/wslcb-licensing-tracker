"""Tests for endorsements.py — license endorsement normalization.

Verifies the _merge_endorsement helper and the repair functions
that use it, after the deduplication refactor (#24).
"""
import pytest

from wslcb_licensing_tracker.endorsements import (
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
    endorsement_similarity,
    get_endorsement_list,
    suggest_duplicate_endorsements,
    dismiss_suggestion,
    get_code_mappings,
    add_code_mapping,
    remove_code_mapping,
    create_code,
)
from wslcb_licensing_tracker.queries import insert_record


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
        from wslcb_licensing_tracker.endorsements import resolve_endorsement
        eid = _ensure_endorsement(db, "STANDALONE")
        db.commit()
        assert resolve_endorsement(db, eid) == eid

    def test_returns_canonical_for_aliased(self, db):
        """Aliased endorsement resolves to its canonical."""
        from wslcb_licensing_tracker.endorsements import resolve_endorsement
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
        from wslcb_licensing_tracker.endorsements import resolve_endorsement
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
        from wslcb_licensing_tracker.endorsements import get_endorsement_groups
        seed_endorsements(db)

        # Manually create two endorsements that share a code
        eid_a = _ensure_endorsement(db, "GROUP CODE A")
        eid_b = _ensure_endorsement(db, "GROUP CODE B")
        shared_code = "TESTGROUP"
        db.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            (shared_code, eid_a),
        )
        db.execute(
            "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            (shared_code, eid_b),
        )
        db.commit()

        groups = get_endorsement_groups(db)
        assert isinstance(groups, list)

        # Find the TESTGROUP group
        test_group = next((g for g in groups if g["code"] == shared_code), None)
        assert test_group is not None, "group for shared code must exist"
        group_ids = {e["id"] for e in test_group["endorsements"]}
        assert eid_a in group_ids, "first endorsement must appear in the group"
        assert eid_b in group_ids, "second endorsement must appear in the group"

    def test_group_entries_are_independent_copies(self, db):
        """Modifying one group's entry dict must not affect another group."""
        from wslcb_licensing_tracker.endorsements import get_endorsement_groups
        seed_endorsements(db)

        # Create an endorsement that belongs to two codes
        eid = _ensure_endorsement(db, "MULTI CODE")
        for code in ("CODE_X", "CODE_Y"):
            db.execute(
                "INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
                (code, eid),
            )
        db.commit()

        groups = get_endorsement_groups(db)
        group_x = next((g for g in groups if g["code"] == "CODE_X"), None)
        group_y = next((g for g in groups if g["code"] == "CODE_Y"), None)
        assert group_x and group_y

        entry_x = next(e for e in group_x["endorsements"] if e["id"] == eid)
        entry_y = next(e for e in group_y["endorsements"] if e["id"] == eid)

        # Mutate one; the other must be unaffected
        entry_x["name"] = "MUTATED"
        assert entry_y["name"] != "MUTATED", "entries must be independent copies"

    def test_includes_record_counts(self, db):
        """Each endorsement entry has a record count."""
        from wslcb_licensing_tracker.endorsements import get_endorsement_groups
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
        from wslcb_licensing_tracker.endorsements import get_endorsement_groups, resolve_endorsement
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
        from wslcb_licensing_tracker.endorsements import set_canonical_endorsement, _ensure_endorsement
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
        from wslcb_licensing_tracker.endorsements import set_canonical_endorsement
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
        from wslcb_licensing_tracker.endorsements import rename_endorsement
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
        from wslcb_licensing_tracker.endorsements import rename_endorsement
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
        from wslcb_licensing_tracker.endorsements import get_endorsement_options
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
        from wslcb_licensing_tracker.endorsements import get_record_endorsements
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


class TestSearchFilterAliasResolution:
    """search_records endorsement filter must match variant-linked records."""

    def test_canonical_filter_matches_variant_linked_records(self, db):
        """Filtering by canonical name returns records linked to variants."""
        from wslcb_licensing_tracker.endorsements import set_canonical_endorsement, _ensure_endorsement
        from wslcb_licensing_tracker.queries import search_records
        seed_endorsements(db)

        rec_variant = _make_record(db, license_number="SF001")
        rec_canonical = _make_record(db, license_number="SF002")

        variant_id = _ensure_endorsement(db, "TAKE OUT/DELIVERY ENDORSEMENT")
        canonical_id = _ensure_endorsement(db, "TAKEOUT/DELIVERY")
        db.execute(
            "INSERT OR IGNORE INTO record_endorsements VALUES (?, ?)",
            (rec_variant, variant_id),
        )
        db.execute(
            "INSERT OR IGNORE INTO record_endorsements VALUES (?, ?)",
            (rec_canonical, canonical_id),
        )
        set_canonical_endorsement(db, canonical_id=canonical_id,
                                  variant_ids=[variant_id], created_by="t@t.com")
        db.commit()

        results, total = search_records(db, endorsement="TAKEOUT/DELIVERY")
        found_nums = {r["license_number"] for r in results}
        assert "SF001" in found_nums, "variant-linked record must appear under canonical filter"
        assert "SF002" in found_nums, "canonical-linked record must appear"
        assert total == 2


# ── process_record idempotency ───────────────────────────────────────────────


class TestProcessRecordIdempotent:
    """process_record() must be safe to call multiple times on the same record."""

    def test_second_call_does_not_duplicate_links(self, db):
        """Calling process_record twice produces the same endorsement count."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="CANNABIS RETAILER")

        count1 = process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()
        rows_after_first = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?",
            (rec_id,),
        ).fetchone()[0]

        count2 = process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()
        rows_after_second = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?",
            (rec_id,),
        ).fetchone()[0]

        assert count1 == count2
        assert rows_after_first == rows_after_second

    def test_second_call_with_different_type_replaces_links(self, db):
        """Re-calling with a different license_type replaces the old endorsements."""
        seed_endorsements(db)
        rec_id = _make_record(db, license_type="CANNABIS RETAILER")

        process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()

        process_record(db, rec_id, "CANNABIS PROCESSOR")
        db.commit()

        names = [
            r[0]
            for r in db.execute(
                """SELECT le.name FROM record_endorsements re
                   JOIN license_endorsements le ON le.id = re.endorsement_id
                   WHERE re.record_id = ?""",
                (rec_id,),
            ).fetchall()
        ]
        assert "CANNABIS PROCESSOR" in names
        assert "CANNABIS RETAILER" not in names


# ── reprocess_endorsements ───────────────────────────────────────────────────


class TestReprocessEndorsements:
    """reprocess_endorsements() should regenerate record_endorsements rows."""

    def test_reprocess_all_records(self, db):
        """reprocess_endorsements() with no filter reprocesses all records."""
        from wslcb_licensing_tracker.endorsements import reprocess_endorsements

        seed_endorsements(db)
        rec1 = _make_record(db, license_number="RP001", license_type="CANNABIS RETAILER")
        rec2 = _make_record(db, license_number="RP002", license_type="CANNABIS PROCESSOR")
        process_record(db, rec1, "CANNABIS RETAILER")
        process_record(db, rec2, "CANNABIS PROCESSOR")
        db.commit()

        # Manually corrupt endorsements to verify reprocess fixes them
        db.execute("DELETE FROM record_endorsements")
        db.commit()

        result = reprocess_endorsements(db)
        db.commit()
        assert result["records_processed"] >= 2
        # Both records should have endorsements again
        for rid in (rec1, rec2):
            count = db.execute(
                "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?",
                (rid,),
            ).fetchone()[0]
            assert count >= 1

    def test_reprocess_by_record_id(self, db):
        """reprocess_endorsements(record_id=X) only touches that record."""
        from wslcb_licensing_tracker.endorsements import reprocess_endorsements

        seed_endorsements(db)
        rec1 = _make_record(db, license_number="RP003", license_type="CANNABIS RETAILER")
        rec2 = _make_record(db, license_number="RP004", license_type="CANNABIS PROCESSOR")
        process_record(db, rec1, "CANNABIS RETAILER")
        process_record(db, rec2, "CANNABIS PROCESSOR")
        db.commit()

        db.execute("DELETE FROM record_endorsements")
        db.commit()

        result = reprocess_endorsements(db, record_id=rec1)
        db.commit()
        assert result["records_processed"] == 1

        # rec1 should have endorsements, rec2 should not
        c1 = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?", (rec1,)
        ).fetchone()[0]
        c2 = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?", (rec2,)
        ).fetchone()[0]
        assert c1 >= 1
        assert c2 == 0

    def test_reprocess_by_code(self, db):
        """reprocess_endorsements(code=X) only touches records with that code."""
        from wslcb_licensing_tracker.endorsements import reprocess_endorsements

        seed_endorsements(db)
        # code 394 = CANNABIS RETAILER
        rec_target = _make_record(db, license_number="RP005",
                                  license_type="394,",
                                  section_type="approved")
        rec_other = _make_record(db, license_number="RP006",
                                 license_type="393,",
                                 section_type="approved")
        process_record(db, rec_target, "394,")
        process_record(db, rec_other, "393,")
        db.commit()

        db.execute("DELETE FROM record_endorsements")
        db.commit()

        result = reprocess_endorsements(db, code="394")
        db.commit()
        assert result["records_processed"] == 1

        c_target = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?",
            (rec_target,),
        ).fetchone()[0]
        c_other = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?",
            (rec_other,),
        ).fetchone()[0]
        assert c_target >= 1
        assert c_other == 0

    def test_reprocess_updates_enrichment_version(self, db):
        """reprocess_endorsements() bumps the record_enrichments version stamp."""
        from wslcb_licensing_tracker.endorsements import reprocess_endorsements

        seed_endorsements(db)
        rec_id = _make_record(db, license_number="RP007", license_type="CANNABIS RETAILER")
        process_record(db, rec_id, "CANNABIS RETAILER")
        # Seed an old enrichment stamp with version='1'
        db.execute(
            "INSERT OR REPLACE INTO record_enrichments (record_id, step, completed_at, version)"
            " VALUES (?, 'endorsements', '2025-01-01T00:00:00+00:00', '1')",
            (rec_id,),
        )
        db.commit()

        reprocess_endorsements(db, record_id=rec_id)
        db.commit()

        row = db.execute(
            "SELECT version FROM record_enrichments WHERE record_id = ? AND step = 'endorsements'",
            (rec_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "2"  # bumped from '1' to '2'

    def test_dry_run_makes_no_changes(self, db):
        """dry_run=True reports counts without writing to the database."""
        from wslcb_licensing_tracker.endorsements import reprocess_endorsements

        seed_endorsements(db)
        rec_id = _make_record(db, license_number="RP008", license_type="CANNABIS RETAILER")
        process_record(db, rec_id, "CANNABIS RETAILER")
        db.commit()

        before = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?", (rec_id,)
        ).fetchone()[0]

        result = reprocess_endorsements(db, dry_run=True)

        after = db.execute(
            "SELECT COUNT(*) FROM record_endorsements WHERE record_id = ?", (rec_id,)
        ).fetchone()[0]

        assert result["records_processed"] >= 1
        assert before == after  # no changes written


# ── endorsement_similarity ───────────────────────────────────────────────────


class TestEndorsementSimilarity:
    """Tests for the token-overlap similarity algorithm."""

    def test_identical_names_score_1(self):
        assert endorsement_similarity("CANNABIS RETAILER", "CANNABIS RETAILER") == 1.0

    def test_take_out_normalisation(self):
        """TAKE OUT → TAKEOUT normalisation yields score >= threshold."""
        score = endorsement_similarity(
            "TAKEOUT/DELIVERY", "TAKE OUT/DELIVERY ENDORSEMENT"
        )
        assert score >= 0.70

    def test_growlers_variant(self):
        score = endorsement_similarity(
            "GROWLERS TAKEOUT/DELIVERY", "GROWLERS TAKE OUT/DELIVERY"
        )
        assert score >= 0.70

    def test_containment_boost(self):
        """COCKTAILS/WINE TO-GO is subset of PREMIXED COCKTAILS/WINE TO-GO."""
        score = endorsement_similarity(
            "PREMIXED COCKTAILS/WINE TO-GO", "COCKTAILS/WINE TO-GO"
        )
        assert score >= 0.70

    def test_angle_bracket_artefact(self):
        """HTML-stripped names (missing '>') still score 1.0."""
        score = endorsement_similarity(
            "DOMESTIC WINERY > 249,999 LITERS",
            "DOMESTIC WINERY  249,999 LITERS",
        )
        assert score >= 0.90

    def test_unrelated_names_score_low(self):
        score = endorsement_similarity("CANNABIS RETAILER", "BEER/WINE RESTAURANT")
        assert score < 0.70

    def test_plus_minus_preserved_as_distinct_tokens(self):
        """+ and - are semantically significant and must not be stripped.

        SPIRITS/BR/WN REST LOUNGE + and SPIRITS/BR/WN REST LOUNGE - are
        distinct licence categories; stripping the trailing symbol would make
        them score 1.00 and be falsely suggested as duplicates.
        """
        score = endorsement_similarity(
            "SPIRITS/BR/WN REST LOUNGE +",
            "SPIRITS/BR/WN REST LOUNGE -",
        )
        # With + and - preserved as PLUS/MINUS tokens the feature sets differ,
        # so the score must be strictly below the 0.70 threshold.
        assert score < 0.70

    def test_empty_name_returns_zero(self):
        assert endorsement_similarity("", "CANNABIS RETAILER") == 0.0
        assert endorsement_similarity("CANNABIS RETAILER", "") == 0.0


# ── get_endorsement_list ─────────────────────────────────────────────────────


class TestGetEndorsementList:
    """Tests for the flat endorsement list returned by get_endorsement_list."""

    def test_returns_all_endorsements(self, db):
        seed_endorsements(db)
        result = get_endorsement_list(db)
        assert len(result) >= 1

    def test_entry_structure(self, db):
        eid = _ensure_endorsement(db, "TEST ENDORSEMENT")
        db.commit()
        result = get_endorsement_list(db)
        entry = next(e for e in result if e["id"] == eid)
        assert "name" in entry
        assert "record_count" in entry
        assert "is_canonical" in entry
        assert "is_variant" in entry
        assert "canonical_id" in entry
        assert "canonical_name" in entry
        assert "codes" in entry

    def test_standalone_flags(self, db):
        eid = _ensure_endorsement(db, "STANDALONE ENDO")
        db.commit()
        result = get_endorsement_list(db)
        entry = next(e for e in result if e["id"] == eid)
        assert entry["is_canonical"] is False
        assert entry["is_variant"] is False
        assert entry["canonical_id"] is None

    def test_variant_flags(self, db):
        from wslcb_licensing_tracker.endorsements import set_canonical_endorsement
        cid = _ensure_endorsement(db, "CANONICAL ENDO")
        vid = _ensure_endorsement(db, "VARIANT ENDO")
        set_canonical_endorsement(db, canonical_id=cid, variant_ids=[vid], created_by="test")
        db.commit()
        result = get_endorsement_list(db)
        canon_entry = next(e for e in result if e["id"] == cid)
        variant_entry = next(e for e in result if e["id"] == vid)
        assert canon_entry["is_canonical"] is True
        assert variant_entry["is_variant"] is True
        assert variant_entry["canonical_id"] == cid
        assert variant_entry["canonical_name"] == "CANONICAL ENDO"

    def test_codes_populated(self, db):
        eid = _ensure_endorsement(db, "CODED ENDO")
        db.execute("INSERT INTO endorsement_codes (code, endorsement_id) VALUES ('999', ?)", (eid,))
        db.commit()
        result = get_endorsement_list(db)
        entry = next(e for e in result if e["id"] == eid)
        assert "999" in entry["codes"]


# ── suggest_duplicate_endorsements ──────────────────────────────────────────


class TestSuggestDuplicateEndorsements:
    """Tests for the duplicate-suggestion algorithm."""

    def test_surfaces_similar_pair(self, db):
        _ensure_endorsement(db, "TAKEOUT/DELIVERY")
        _ensure_endorsement(db, "TAKE OUT/DELIVERY ENDORSEMENT")
        db.commit()
        suggestions = suggest_duplicate_endorsements(db)
        names = {(s["name_a"], s["name_b"]) for s in suggestions}
        pair = ("TAKE OUT/DELIVERY ENDORSEMENT", "TAKEOUT/DELIVERY")
        # normalised order: smaller id first; just check both names appear
        found = any(
            {s["name_a"], s["name_b"]} == {"TAKEOUT/DELIVERY", "TAKE OUT/DELIVERY ENDORSEMENT"}
            for s in suggestions
        )
        assert found

    def test_excludes_aliased_pair(self, db):
        from wslcb_licensing_tracker.endorsements import set_canonical_endorsement
        cid = _ensure_endorsement(db, "TAKEOUT/DELIVERY")
        vid = _ensure_endorsement(db, "TAKE OUT/DELIVERY ENDORSEMENT")
        set_canonical_endorsement(db, canonical_id=cid, variant_ids=[vid], created_by="test")
        db.commit()
        suggestions = suggest_duplicate_endorsements(db)
        found = any(
            {s["name_a"], s["name_b"]} == {"TAKEOUT/DELIVERY", "TAKE OUT/DELIVERY ENDORSEMENT"}
            for s in suggestions
        )
        assert not found

    def test_excludes_dismissed_pair(self, db):
        id_a = _ensure_endorsement(db, "TAKEOUT/DELIVERY")
        id_b = _ensure_endorsement(db, "TAKE OUT/DELIVERY ENDORSEMENT")
        dismiss_suggestion(db, id_a, id_b, "admin@test.com")
        db.commit()
        suggestions = suggest_duplicate_endorsements(db)
        found = any(
            {s["name_a"], s["name_b"]} == {"TAKEOUT/DELIVERY", "TAKE OUT/DELIVERY ENDORSEMENT"}
            for s in suggestions
        )
        assert not found

    def test_ordered_by_score_descending(self, db):
        # Identical pair should score 1.0; similar but not identical scores lower
        _ensure_endorsement(db, "GROWLERS TAKEOUT/DELIVERY")
        _ensure_endorsement(db, "GROWLERS TAKE OUT/DELIVERY")
        _ensure_endorsement(db, "PREMIXED COCKTAILS/WINE TO-GO")
        _ensure_endorsement(db, "COCKTAILS/WINE TO-GO")
        db.commit()
        suggestions = suggest_duplicate_endorsements(db)
        scores = [s["score"] for s in suggestions]
        assert scores == sorted(scores, reverse=True)

    def test_pair_id_ordering(self, db):
        id_a = _ensure_endorsement(db, "AARDVARK LICENCE")
        id_b = _ensure_endorsement(db, "AARDVARK LICENSE")
        db.commit()
        suggestions = suggest_duplicate_endorsements(db)
        found = [s for s in suggestions if {s["name_a"], s["name_b"]} == {"AARDVARK LICENCE", "AARDVARK LICENSE"}]
        if found:
            assert found[0]["id_a"] < found[0]["id_b"]


# ── dismiss_suggestion ───────────────────────────────────────────────────────


class TestDismissSuggestion:
    """Tests for dismiss_suggestion."""

    def test_inserts_dismissed_row(self, db):
        a = _ensure_endorsement(db, "ENDO A")
        b = _ensure_endorsement(db, "ENDO B")
        dismiss_suggestion(db, a, b, "admin@test.com")
        db.commit()
        row = db.execute(
            "SELECT dismissed_by FROM endorsement_dismissed_suggestions "
            "WHERE endorsement_id_a = ? AND endorsement_id_b = ?",
            (min(a, b), max(a, b)),
        ).fetchone()
        assert row is not None
        assert row[0] == "admin@test.com"

    def test_normalises_id_order(self, db):
        a = _ensure_endorsement(db, "ENDO X")
        b = _ensure_endorsement(db, "ENDO Y")
        # Pass larger id first — should still be stored as min(a,b), max(a,b)
        dismiss_suggestion(db, max(a, b), min(a, b), "test")
        db.commit()
        row = db.execute(
            "SELECT 1 FROM endorsement_dismissed_suggestions "
            "WHERE endorsement_id_a = ? AND endorsement_id_b = ?",
            (min(a, b), max(a, b)),
        ).fetchone()
        assert row is not None

    def test_idempotent(self, db):
        a = _ensure_endorsement(db, "ENDO P")
        b = _ensure_endorsement(db, "ENDO Q")
        dismiss_suggestion(db, a, b, "test")
        dismiss_suggestion(db, a, b, "test")  # should not raise
        db.commit()
        count = db.execute(
            "SELECT COUNT(*) FROM endorsement_dismissed_suggestions "
            "WHERE endorsement_id_a = ? AND endorsement_id_b = ?",
            (min(a, b), max(a, b)),
        ).fetchone()[0]
        assert count == 1


# ── get_code_mappings ────────────────────────────────────────────────────────


class TestGetCodeMappings:
    """Tests for get_code_mappings."""

    def test_returns_list(self, db):
        seed_endorsements(db)
        result = get_code_mappings(db)
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_entry_structure(self, db):
        eid = _ensure_endorsement(db, "MAP TEST ENDO")
        db.execute("INSERT INTO endorsement_codes (code, endorsement_id) VALUES ('777', ?)", (eid,))
        db.commit()
        result = get_code_mappings(db)
        entry = next(cm for cm in result if cm["code"] == "777")
        assert "endorsements" in entry
        assert "record_count" in entry
        assert any(e["id"] == eid for e in entry["endorsements"])

    def test_numeric_sort_order(self, db):
        eid = _ensure_endorsement(db, "SORT TEST A")
        eid2 = _ensure_endorsement(db, "SORT TEST B")
        db.execute("INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES ('20', ?)", (eid,))
        db.execute("INSERT OR IGNORE INTO endorsement_codes (code, endorsement_id) VALUES ('100', ?)", (eid2,))
        db.commit()
        codes = [cm["code"] for cm in get_code_mappings(db)]
        idx20 = next((i for i, c in enumerate(codes) if c == "20"), None)
        idx100 = next((i for i, c in enumerate(codes) if c == "100"), None)
        if idx20 is not None and idx100 is not None:
            assert idx20 < idx100  # 20 < 100 numerically


# ── add_code_mapping / remove_code_mapping / create_code ─────────────────────


class TestCodeMappingMutations:
    """Tests for add_code_mapping, remove_code_mapping, create_code."""

    def test_add_returns_true_for_new_row(self, db):
        eid = _ensure_endorsement(db, "ADD TEST")
        result = add_code_mapping(db, "888", eid)
        assert result is True

    def test_add_returns_false_for_duplicate(self, db):
        eid = _ensure_endorsement(db, "DUP TEST")
        add_code_mapping(db, "889", eid)
        result = add_code_mapping(db, "889", eid)
        assert result is False

    def test_remove_returns_true_for_existing(self, db):
        eid = _ensure_endorsement(db, "REMOVE TEST")
        add_code_mapping(db, "890", eid)
        db.commit()
        result = remove_code_mapping(db, "890", eid)
        assert result is True

    def test_remove_returns_false_when_absent(self, db):
        eid = _ensure_endorsement(db, "ABSENT TEST")
        result = remove_code_mapping(db, "891", eid)
        assert result is False

    def test_create_code_inserts_mappings(self, db):
        eid1 = _ensure_endorsement(db, "CREATE A")
        eid2 = _ensure_endorsement(db, "CREATE B")
        inserted = create_code(db, "892", [eid1, eid2])
        assert inserted == 2
        rows = db.execute(
            "SELECT endorsement_id FROM endorsement_codes WHERE code = '892'"
        ).fetchall()
        assert {r[0] for r in rows} == {eid1, eid2}

    def test_create_code_skips_existing_mapping(self, db):
        eid = _ensure_endorsement(db, "SKIP TEST")
        add_code_mapping(db, "893", eid)
        # Should not raise; inserted count = 0 for the duplicate
        inserted = create_code(db, "893", [eid])
        assert inserted == 0


# ── Regulated Substances ──────────────────────────────────────────────────────

def _ensure_substance(conn, name: str, display_order: int = 0) -> int:
    """Helper: insert a substance and return its id."""
    conn.execute(
        "INSERT OR IGNORE INTO regulated_substances (name, display_order) VALUES (?, ?)",
        (name, display_order),
    )
    return conn.execute(
        "SELECT id FROM regulated_substances WHERE name = ?", (name,)
    ).fetchone()[0]


class TestGetRegulatedSubstances:
    def test_returns_substances_in_display_order(self, db):
        from wslcb_licensing_tracker.endorsements import get_regulated_substances
        _ensure_substance(db, "Alcohol", 2)
        _ensure_substance(db, "Cannabis", 1)
        db.commit()
        results = get_regulated_substances(db)
        names = [r["name"] for r in results]
        assert names == ["Cannabis", "Alcohol"]

    def test_includes_endorsements_list(self, db):
        from wslcb_licensing_tracker.endorsements import get_regulated_substances
        eid = _ensure_endorsement(db, "CANNABIS RETAILER")
        sid = _ensure_substance(db, "Cannabis", 1)
        db.execute(
            "INSERT OR IGNORE INTO regulated_substance_endorsements (substance_id, endorsement_id) VALUES (?, ?)",
            (sid, eid),
        )
        db.commit()
        results = get_regulated_substances(db)
        cannabis = next(r for r in results if r["name"] == "Cannabis")
        assert "CANNABIS RETAILER" in cannabis["endorsements"]

    def test_empty_when_no_seeded_endorsements(self, db):
        """Without seeded endorsements the substances exist but have no
        endorsement associations (junction rows reference non-existent eids)."""
        from wslcb_licensing_tracker.endorsements import get_regulated_substances
        results = get_regulated_substances(db)
        # Substances are seeded by migration 009; endorsements list is empty
        # until endorsements are seeded.
        cannabis = next((r for r in results if r["name"] == "Cannabis"), None)
        assert cannabis is not None
        assert cannabis["endorsements"] == []


class TestGetSubstanceEndorsementIds:
    def test_returns_ids_for_substance(self, db):
        from wslcb_licensing_tracker.endorsements import get_substance_endorsement_ids
        eid1 = _ensure_endorsement(db, "BEER DISTRIBUTOR")
        eid2 = _ensure_endorsement(db, "WINE DISTRIBUTOR")
        sid = _ensure_substance(db, "Alcohol", 2)
        db.execute("INSERT OR IGNORE INTO regulated_substance_endorsements VALUES (?,?)", (sid, eid1))
        db.execute("INSERT OR IGNORE INTO regulated_substance_endorsements VALUES (?,?)", (sid, eid2))
        db.commit()
        ids = get_substance_endorsement_ids(db, sid)
        assert set(ids) == {eid1, eid2}

    def test_returns_empty_for_unknown_substance(self, db):
        from wslcb_licensing_tracker.endorsements import get_substance_endorsement_ids
        assert get_substance_endorsement_ids(db, 9999) == []


class TestSetSubstanceEndorsements:
    def test_replaces_associations(self, db):
        from wslcb_licensing_tracker.substances import set_substance_endorsements
        eid1 = _ensure_endorsement(db, "OLD ENDORSEMENT")
        eid2 = _ensure_endorsement(db, "NEW ENDORSEMENT")
        sid = _ensure_substance(db, "Test Substance")
        db.execute("INSERT OR IGNORE INTO regulated_substance_endorsements VALUES (?,?)", (sid, eid1))
        db.commit()
        set_substance_endorsements(db, sid, [eid2])
        db.commit()
        ids = db.execute(
            "SELECT endorsement_id FROM regulated_substance_endorsements WHERE substance_id = ?",
            (sid,),
        ).fetchall()
        assert {r[0] for r in ids} == {eid2}

    def test_audit_log_written_by_caller(self, db):
        """Audit logging is the caller's responsibility; verify pattern works."""
        from wslcb_licensing_tracker.substances import set_substance_endorsements
        from wslcb_licensing_tracker.admin_audit import log_action
        sid = _ensure_substance(db, "Audit Test")
        set_substance_endorsements(db, sid, [])
        log_action(db, "admin@example.com", "substance.set_endorsements",
                   "regulated_substance", target_id=sid, details={"endorsement_count": 0})
        db.commit()
        row = db.execute(
            "SELECT admin_email, action FROM admin_audit_log WHERE action = 'substance.set_endorsements'"
        ).fetchone()
        assert row is not None
        assert row[0] == "admin@example.com"

    def test_clearing_all_endorsements(self, db):
        from wslcb_licensing_tracker.substances import set_substance_endorsements
        eid = _ensure_endorsement(db, "CLEAR ME")
        sid = _ensure_substance(db, "Clear Test")
        db.execute("INSERT OR IGNORE INTO regulated_substance_endorsements VALUES (?,?)", (sid, eid))
        db.commit()
        set_substance_endorsements(db, sid, [])
        db.commit()
        count = db.execute(
            "SELECT COUNT(*) FROM regulated_substance_endorsements WHERE substance_id = ?", (sid,)
        ).fetchone()[0]
        assert count == 0


class TestAddSubstance:
    def test_inserts_and_returns_id(self, db):
        from wslcb_licensing_tracker.substances import add_substance
        sid = add_substance(db, "Test Sub", display_order=5)
        db.commit()
        row = db.execute(
            "SELECT name, display_order FROM regulated_substances WHERE id = ?", (sid,)
        ).fetchone()
        assert row is not None
        assert row[0] == "Test Sub"
        assert row[1] == 5

    def test_audit_log_written_by_caller(self, db):
        """Audit logging is the caller's responsibility; verify pattern works."""
        from wslcb_licensing_tracker.substances import add_substance
        from wslcb_licensing_tracker.admin_audit import log_action
        sid = add_substance(db, "Audit Sub", display_order=1)
        log_action(db, "admin@x.com", "substance.add", "regulated_substance",
                   target_id=sid, details={"name": "Audit Sub"})
        db.commit()
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'substance.add'"
        ).fetchone()
        assert row is not None


class TestRemoveSubstance:
    def test_deletes_substance(self, db):
        from wslcb_licensing_tracker.substances import add_substance, remove_substance
        sid = add_substance(db, "Delete Me", display_order=1)
        db.commit()
        remove_substance(db, sid)
        db.commit()
        row = db.execute(
            "SELECT id FROM regulated_substances WHERE id = ?", (sid,)
        ).fetchone()
        assert row is None

    def test_cascades_to_junction(self, db):
        from wslcb_licensing_tracker.substances import add_substance, remove_substance
        eid = _ensure_endorsement(db, "CASCADE TEST")
        sid = add_substance(db, "Cascade Sub", display_order=1)
        db.execute("INSERT OR IGNORE INTO regulated_substance_endorsements VALUES (?,?)", (sid, eid))
        db.commit()
        remove_substance(db, sid)
        db.commit()
        count = db.execute(
            "SELECT COUNT(*) FROM regulated_substance_endorsements WHERE substance_id = ?", (sid,)
        ).fetchone()[0]
        assert count == 0

    def test_audit_log_written_by_caller(self, db):
        """Audit logging is the caller's responsibility; verify pattern works."""
        from wslcb_licensing_tracker.substances import add_substance, remove_substance
        from wslcb_licensing_tracker.admin_audit import log_action
        sid = add_substance(db, "Audit Remove", display_order=1)
        db.commit()
        name = remove_substance(db, sid)
        log_action(db, "a@b.com", "substance.remove", "regulated_substance",
                   target_id=sid, details={"name": name})
        db.commit()
        row = db.execute(
            "SELECT action FROM admin_audit_log WHERE action = 'substance.remove'"
        ).fetchone()
        assert row is not None
