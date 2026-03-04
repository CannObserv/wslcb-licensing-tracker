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
        # rid2 untouched by record_id filter — but both exist

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
