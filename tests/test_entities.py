"""Tests for entity normalization, parse_and_link_entities, and reprocess_entities."""
import pytest


# ── Helpers ──────────────────────────────────────────────────────────

def _insert_record(db, record):
    """Insert a minimal license record directly and return its id."""
    from queries import insert_record
    result = insert_record(db, record)
    assert result is not None
    return result[0]


def _entity_links(db, record_id, role="applicant"):
    rows = db.execute(
        """SELECT e.name, re.position
           FROM record_entities re
           JOIN entities e ON e.id = re.entity_id
           WHERE re.record_id = ? AND re.role = ?
           ORDER BY re.position""",
        (record_id, role),
    ).fetchall()
    return [(r["name"], r["position"]) for r in rows]


# ── parse_and_link_entities ──────────────────────────────────────────

class TestParseAndLinkEntities:
    def test_basic_links(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)
        # Clear pipeline-inserted links so we test parse_and_link_entities directly
        db.execute("DELETE FROM record_entities WHERE record_id = ?", (rid,))

        from entities import parse_and_link_entities
        n = parse_and_link_entities(db, rid, "BIZ; ALICE; BOB")
        assert n == 2
        links = _entity_links(db, rid)
        assert links == [("ALICE", 0), ("BOB", 1)]

    def test_no_semicolon_returns_zero(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)
        db.execute("DELETE FROM record_entities WHERE record_id = ?", (rid,))

        from entities import parse_and_link_entities
        n = parse_and_link_entities(db, rid, "JUST A NAME")
        assert n == 0
        assert _entity_links(db, rid) == []

    def test_delete_existing_false_preserves_old(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)
        db.execute("DELETE FROM record_entities WHERE record_id = ?", (rid,))

        from entities import parse_and_link_entities
        parse_and_link_entities(db, rid, "BIZ; ALICE")
        # Second call without delete_existing — INSERT OR IGNORE, so no duplicates
        n = parse_and_link_entities(db, rid, "BIZ; ALICE")
        assert n == 0  # all already present
        assert len(_entity_links(db, rid)) == 1

    def test_delete_existing_removes_stale_links(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)
        db.execute("DELETE FROM record_entities WHERE record_id = ?", (rid,))

        from entities import parse_and_link_entities
        parse_and_link_entities(db, rid, "BIZ; ALICE; BOB")
        assert len(_entity_links(db, rid)) == 2

        # Reprocess with different applicants — ALICE & BOB gone, CAROL added
        n = parse_and_link_entities(db, rid, "BIZ; CAROL", delete_existing=True)
        assert n == 1
        links = _entity_links(db, rid)
        assert [name for name, _ in links] == ["CAROL"]

    def test_delete_existing_on_empty_string_clears_links(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)
        db.execute("DELETE FROM record_entities WHERE record_id = ?", (rid,))

        from entities import parse_and_link_entities
        parse_and_link_entities(db, rid, "BIZ; ALICE")
        assert len(_entity_links(db, rid)) == 1

        # Reprocess with an empty string — should clear the role
        n = parse_and_link_entities(db, rid, "", delete_existing=True)
        assert n == 0
        assert _entity_links(db, rid) == []

    def test_delete_existing_only_affects_given_role(self, db, assumption_record):
        rid = _insert_record(db, assumption_record)
        db.execute("DELETE FROM record_entities WHERE record_id = ?", (rid,))

        from entities import parse_and_link_entities
        parse_and_link_entities(db, rid, "BIZ; ALICE", role="applicant")
        parse_and_link_entities(db, rid, "OLD BIZ; BOB", role="previous_applicant")

        # Reprocess only the applicant role
        parse_and_link_entities(db, rid, "BIZ; CAROL", role="applicant", delete_existing=True)

        applicants = _entity_links(db, rid, "applicant")
        previous = _entity_links(db, rid, "previous_applicant")

        assert [n for n, _ in applicants] == ["CAROL"]
        assert [n for n, _ in previous] == ["BOB"]  # untouched


# ── reprocess_entities ───────────────────────────────────────────────

class TestReprocessEntities:
    def test_dry_run_returns_counts_no_writes(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)
        # Note existing links from pipeline
        before = db.execute(
            "SELECT COUNT(*) FROM record_entities WHERE record_id = ?", (rid,)
        ).fetchone()[0]

        from entities import reprocess_entities
        result = reprocess_entities(db, dry_run=True)
        assert result["records_processed"] >= 1

        after = db.execute(
            "SELECT COUNT(*) FROM record_entities WHERE record_id = ?", (rid,)
        ).fetchone()[0]
        assert before == after  # no writes

    def test_all_records_processed(self, db, standard_new_application, assumption_record):
        _insert_record(db, standard_new_application)
        _insert_record(db, assumption_record)

        from entities import reprocess_entities
        result = reprocess_entities(db)
        assert result["records_processed"] == 2

    def test_single_record_id(self, db, standard_new_application, assumption_record):
        rid1 = _insert_record(db, standard_new_application)
        rid2 = _insert_record(db, assumption_record)

        from entities import reprocess_entities
        result = reprocess_entities(db, record_id=rid1)
        assert result["records_processed"] == 1

    def test_idempotent(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)

        from entities import reprocess_entities
        r1 = reprocess_entities(db)
        db.commit()
        links_after_first = db.execute(
            "SELECT COUNT(*) FROM record_entities WHERE record_id = ?", (rid,)
        ).fetchone()[0]

        r2 = reprocess_entities(db)
        db.commit()
        links_after_second = db.execute(
            "SELECT COUNT(*) FROM record_entities WHERE record_id = ?", (rid,)
        ).fetchone()[0]

        assert links_after_first == links_after_second
        assert r1["entities_linked"] == r2["entities_linked"]

    def test_version_stamp_bumped(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)

        from entities import reprocess_entities, _ENTITY_REPROCESS_VERSION
        reprocess_entities(db, record_id=rid)
        db.commit()

        row = db.execute(
            "SELECT version FROM record_enrichments WHERE record_id = ? AND step = 'entities'",
            (rid,),
        ).fetchone()
        assert row is not None
        assert int(row["version"]) == _ENTITY_REPROCESS_VERSION

    def test_version_stamp_bumped_for_all(self, db, standard_new_application, assumption_record):
        rid1 = _insert_record(db, standard_new_application)
        rid2 = _insert_record(db, assumption_record)

        from entities import reprocess_entities, _ENTITY_REPROCESS_VERSION
        reprocess_entities(db)
        db.commit()

        for rid in (rid1, rid2):
            row = db.execute(
                "SELECT version FROM record_enrichments WHERE record_id = ? AND step = 'entities'",
                (rid,),
            ).fetchone()
            assert row is not None
            assert int(row["version"]) == _ENTITY_REPROCESS_VERSION

    def test_stale_links_replaced(self, db, standard_new_application):
        """Reprocessing should replace links if the canonical names changed."""
        rid = _insert_record(db, standard_new_application)

        # Manually inject a stale entity link that doesn't match applicants
        stale_id = db.execute(
            "INSERT INTO entities (name, entity_type) VALUES ('STALE PERSON', 'person') RETURNING id"
        ).fetchone()[0]
        db.execute(
            "INSERT OR IGNORE INTO record_entities (record_id, entity_id, role, position) VALUES (?, ?, 'applicant', 99)",
            (rid, stale_id),
        )
        db.commit()

        from entities import reprocess_entities
        reprocess_entities(db, record_id=rid)
        db.commit()

        names = [r["name"] for r in db.execute(
            """SELECT e.name FROM record_entities re
               JOIN entities e ON e.id = re.entity_id
               WHERE re.record_id = ? AND re.role = 'applicant'""",
            (rid,),
        ).fetchall()]
        assert "STALE PERSON" not in names

    def test_previous_applicants_also_reprocessed(self, db, assumption_record):
        rid = _insert_record(db, assumption_record)

        from entities import reprocess_entities
        result = reprocess_entities(db, record_id=rid)
        db.commit()

        prev_links = _entity_links(db, rid, "previous_applicant")
        # assumption_record has previous_applicants with names after the biz name
        assert len(prev_links) > 0
        assert result["entities_linked"] > 0

    def test_dry_run_version_not_bumped(self, db, standard_new_application):
        rid = _insert_record(db, standard_new_application)
        # Record the current version stamp
        before = db.execute(
            "SELECT version FROM record_enrichments WHERE record_id = ? AND step = 'entities'",
            (rid,),
        ).fetchone()

        from entities import reprocess_entities
        reprocess_entities(db, record_id=rid, dry_run=True)

        after = db.execute(
            "SELECT version FROM record_enrichments WHERE record_id = ? AND step = 'entities'",
            (rid,),
        ).fetchone()

        # Version should be unchanged (dry-run writes nothing)
        before_v = before["version"] if before else None
        after_v = after["version"] if after else None
        assert before_v == after_v


class TestAdditionalNamesMarker:
    """Tests for ADDITIONAL_NAMES_MARKERS skip logic in parse_and_link_entities."""

    def test_marker_constant_exported(self):
        from entities import ADDITIONAL_NAMES_MARKERS
        assert "ADDITIONAL NAMES ON FILE" in ADDITIONAL_NAMES_MARKERS
        assert "ADDTIONAL NAMES ON FILE" in ADDITIONAL_NAMES_MARKERS

    def test_exact_marker_not_created_as_entity(self, db):
        from entities import parse_and_link_entities
        record_id = db.execute(
            "INSERT INTO license_records (section_type, record_date, license_number, "
            "application_type, scraped_at) VALUES ('new_application', '2025-01-01', "
            "'111111', 'NEW APPLICATION', '2025-01-01T00:00:00+00:00') RETURNING id"
        ).fetchone()[0]
        applicants = "ACME LLC; ADDITIONAL NAMES ON FILE; JANE DOE"
        parse_and_link_entities(db, record_id, applicants)
        db.commit()
        names = [r[0] for r in db.execute(
            "SELECT e.name FROM record_entities re "
            "JOIN entities e ON e.id = re.entity_id "
            "WHERE re.record_id = ?", (record_id,)
        ).fetchall()]
        assert "ADDITIONAL NAMES ON FILE" not in names
        assert "JANE DOE" in names

    def test_typo_marker_not_created_as_entity(self, db):
        from entities import parse_and_link_entities
        record_id = db.execute(
            "INSERT INTO license_records (section_type, record_date, license_number, "
            "application_type, scraped_at) VALUES ('new_application', '2025-01-01', "
            "'111112', 'NEW APPLICATION', '2025-01-01T00:00:00+00:00') RETURNING id"
        ).fetchone()[0]
        applicants = "ACME LLC; ADDTIONAL NAMES ON FILE; JANE DOE"
        parse_and_link_entities(db, record_id, applicants)
        db.commit()
        names = [r[0] for r in db.execute(
            "SELECT e.name FROM record_entities re "
            "JOIN entities e ON e.id = re.entity_id "
            "WHERE re.record_id = ?", (record_id,)
        ).fetchall()]
        assert "ADDTIONAL NAMES ON FILE" not in names
        assert "JANE DOE" in names

    def test_positions_are_contiguous_across_marker(self, db):
        """Positions for real entities must be 0,1,2,… with no gap for the skipped marker."""
        from entities import parse_and_link_entities
        record_id = db.execute(
            "INSERT INTO license_records (section_type, record_date, license_number, "
            "application_type, scraped_at) VALUES ('new_application', '2025-01-01', "
            "'111113', 'NEW APPLICATION', '2025-01-01T00:00:00+00:00') RETURNING id"
        ).fetchone()[0]
        # BIZ; ALICE; [marker]; BOB; CAROL — should give positions 0, 1, 2
        applicants = "ACME LLC; ALICE SMITH; ADDITIONAL NAMES ON FILE; BOB JONES; CAROL WHITE"
        parse_and_link_entities(db, record_id, applicants)
        db.commit()
        rows = db.execute(
            "SELECT e.name, re.position FROM record_entities re "
            "JOIN entities e ON e.id = re.entity_id "
            "WHERE re.record_id = ? ORDER BY re.position", (record_id,)
        ).fetchall()
        positions = [r[1] for r in rows]
        names = [r[0] for r in rows]
        assert positions == list(range(len(positions))), "positions must be contiguous"
        assert names == ["ALICE SMITH", "BOB JONES", "CAROL WHITE"]


# ── strip_duplicate_marker ──────────────────────────────────────────

class TestStripDuplicateMarker:
    """Tests for the pure strip_duplicate_marker() helper."""

    def _strip(self, name):
        from entities import strip_duplicate_marker
        return strip_duplicate_marker(name)

    def test_parenthesized(self):
        assert self._strip("ADAM (DUPLICATE) BENTON") == "ADAM BENTON"

    def test_parenthesized_numbered_2(self):
        assert self._strip("KATIE (DUPLICATE 2) DAVIS") == "KATIE DAVIS"

    def test_parenthesized_numbered_3(self):
        assert self._strip("KATIE (DUPLICATE 3) DAVIS") == "KATIE DAVIS"

    def test_inline_mid(self):
        assert self._strip("ANNA MARIE DUPLICATE ADAMS") == "ANNA MARIE ADAMS"

    def test_inline_prefix(self):
        assert self._strip("DUPLICATE ITALIAN SUPPLY, LLC") == "ITALIAN SUPPLY, LLC"

    def test_asterisk_variant(self):
        assert self._strip("PAUL *DUPLICATE* SONG") == "PAUL SONG"

    def test_trailing_with_paren(self):
        # "JAY WON (DUPLICATE)" -> "JAY WON"
        assert self._strip("JAY WON (DUPLICATE)") == "JAY WON"

    def test_unclosed_paren(self):
        # WSLCB source has one case with an unclosed open paren:
        # 'ELIZABETH (DUPLICATE A MATTHEWS'
        assert self._strip("ELIZABETH (DUPLICATE A MATTHEWS") == "ELIZABETH A MATTHEWS"

    def test_no_marker_unchanged(self):
        assert self._strip("ALICE SMITH") == "ALICE SMITH"

    def test_collapse_extra_spaces(self):
        """Double spaces left after DUPLICATE removal are collapsed to one."""
        # Double space before AND after the marker: after removal 'ANNA  MARIE'
        # must be collapsed to 'ANNA MARIE'.
        assert self._strip("ANNA  DUPLICATE  MARIE") == "ANNA MARIE"

    def test_result_has_no_double_space(self):
        assert "  " not in self._strip("NEALY DUPLICATE EVANS")


# ── DUPLICATE marker filtering in clean_applicants_string ────────────

class TestCleanApplicantsStringDuplicate:
    """DUPLICATE markers must be stripped by clean_applicants_string()."""

    def _clean(self, s):
        from entities import clean_applicants_string
        return clean_applicants_string(s)

    def test_parenthesized_marker_stripped(self):
        result = self._clean("BIZ; ADAM (DUPLICATE) BENTON; ADAM BENTON")
        # Both tokens clean to "ADAM BENTON"; duplicates are collapsed
        assert "(DUPLICATE)" not in result
        assert "DUPLICATE" not in result

    def test_inline_marker_stripped(self):
        result = self._clean("BIZ; NEALY DUPLICATE EVANS; NEALY EVANS")
        assert "DUPLICATE" not in result

    def test_deduplication_after_stripping(self):
        """When stripping leaves two identical tokens, only one is kept."""
        result = self._clean("BIZ; ADAM (DUPLICATE) BENTON; ADAM BENTON")
        parts = [p.strip() for p in result.split(";")] if result else []
        assert parts.count("ADAM BENTON") == 1

    def test_only_duplicate_token_synthesizes_clean_name(self):
        """A lone DUPLICATE-annotated token should yield the stripped clean name."""
        result = self._clean("BIZ; LORIE DUPLICATE FAZIO")
        assert "LORIE FAZIO" in result
        assert "DUPLICATE" not in result


# ── DUPLICATE marker filtering in parse_and_link_entities ────────────

class TestParseAndLinkEntitiesDuplicate:
    """Entities created from DUPLICATE-annotated names must use the clean name."""

    def _make_record(self, db, license_number):
        return db.execute(
            "INSERT INTO license_records (section_type, record_date, license_number, "
            "application_type, scraped_at) VALUES ('new_application', '2025-01-01', "
            "?, 'NEW APPLICATION', '2025-01-01T00:00:00+00:00') RETURNING id",
            (license_number,),
        ).fetchone()[0]

    def test_duplicate_annotated_name_links_clean_entity(self, db):
        from entities import parse_and_link_entities
        rid = self._make_record(db, "DUP001")
        # "ADAM (DUPLICATE) BENTON" must resolve to / create "ADAM BENTON"
        parse_and_link_entities(db, rid, "BIZ; ADAM (DUPLICATE) BENTON")
        db.commit()
        names = [
            r[0] for r in db.execute(
                "SELECT e.name FROM record_entities re "
                "JOIN entities e ON e.id = re.entity_id WHERE re.record_id = ?",
                (rid,),
            ).fetchall()
        ]
        assert names == ["ADAM BENTON"]
        assert not any("DUPLICATE" in n for n in names)

    def test_duplicate_and_clean_in_same_string_links_once(self, db):
        """When the source lists both forms, only one entity link is created."""
        from entities import parse_and_link_entities
        rid = self._make_record(db, "DUP002")
        parse_and_link_entities(
            db, rid, "BIZ; ADAM (DUPLICATE) BENTON; ADAM BENTON"
        )
        db.commit()
        rows = db.execute(
            "SELECT e.name, re.position FROM record_entities re "
            "JOIN entities e ON e.id = re.entity_id WHERE re.record_id = ? "
            "ORDER BY re.position",
            (rid,),
        ).fetchall()
        # exactly one link, at position 0
        assert len(rows) == 1
        assert rows[0]["name"] == "ADAM BENTON"
        assert rows[0]["position"] == 0

    def test_lone_duplicate_token_creates_synthesized_entity(self, db):
        """When no clean counterpart exists, synthesize and create the clean entity."""
        from entities import parse_and_link_entities
        rid = self._make_record(db, "DUP003")
        parse_and_link_entities(db, rid, "BIZ; LORIE DUPLICATE FAZIO")
        db.commit()
        names = [
            r[0] for r in db.execute(
                "SELECT e.name FROM record_entities re "
                "JOIN entities e ON e.id = re.entity_id WHERE re.record_id = ?",
                (rid,),
            ).fetchall()
        ]
        assert names == ["LORIE FAZIO"]
        # Ensure no DUPLICATE-bearing entity was stored
        dup_row = db.execute(
            "SELECT id FROM entities WHERE name LIKE '%DUPLICATE%'"
        ).fetchone()
        assert dup_row is None

    def test_numbered_parenthesized_all_resolve_to_same_entity(self, db):
        """(DUPLICATE), (DUPLICATE 2), (DUPLICATE 3) all collapse to the same entity."""
        from entities import parse_and_link_entities
        rid = self._make_record(db, "DUP004")
        applicants = (
            "BIZ; KATIE (DUPLICATE) DAVIS; KATIE (DUPLICATE 2) DAVIS; "
            "KATIE (DUPLICATE 3) DAVIS; KATIE DAVIS"
        )
        parse_and_link_entities(db, rid, applicants)
        db.commit()
        rows = db.execute(
            "SELECT e.name FROM record_entities re "
            "JOIN entities e ON e.id = re.entity_id WHERE re.record_id = ?",
            (rid,),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["name"] == "KATIE DAVIS"
