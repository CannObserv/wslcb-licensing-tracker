"""Tests for display.py — presentation formatting and logic.

Pure unit tests — no database, no network.
"""
import pytest


# ── format_outcome ─────────────────────────────────────────────────


class TestFormatOutcome:
    def test_approved_has_display_properties(self):
        """format_outcome should add CSS classes and icon to semantic data."""
        from wslcb_licensing_tracker.display import format_outcome

        semantic = {
            "status": "approved",
            "label": "Approved",
            "detail": "Approved on 2025-06-17 (2 days after application)",
            "linked_record_id": 42,
            "confidence": "high",
        }
        result = format_outcome(semantic)

        assert result["icon"] == "✅"
        assert "bg-green" in result["css_bg"]
        assert "border-green" in result["css_border"]
        assert result["status"] == "approved"
        # Original keys are preserved
        assert result["linked_record_id"] == 42

    def test_discontinued_has_display_properties(self):
        from wslcb_licensing_tracker.display import format_outcome

        semantic = {
            "status": "discontinued",
            "label": "Discontinued",
            "detail": "Discontinued on 2025-06-17",
            "linked_record_id": 99,
            "confidence": "high",
        }
        result = format_outcome(semantic)

        assert result["icon"] == "🚫"
        assert "bg-red" in result["css_bg"]

    def test_pending_has_display_properties(self):
        from wslcb_licensing_tracker.display import format_outcome

        semantic = {
            "status": "pending",
            "label": "Pending",
            "detail": "Filed 30 days ago.",
            "linked_record_id": None,
            "confidence": None,
        }
        result = format_outcome(semantic)

        assert result["icon"] == "⏳"
        assert "bg-amber" in result["css_bg"]

    def test_data_gap_has_display_properties(self):
        from wslcb_licensing_tracker.display import format_outcome

        semantic = {
            "status": "data_gap",
            "label": "Data Unavailable",
            "detail": "WSLCB stopped publishing.",
            "linked_record_id": None,
            "confidence": None,
        }
        result = format_outcome(semantic)

        assert result["icon"] == "📁"
        assert "bg-slate" in result["css_bg"]

    def test_unknown_has_display_properties(self):
        from wslcb_licensing_tracker.display import format_outcome

        semantic = {
            "status": "unknown",
            "label": "No Outcome Recorded",
            "detail": "No matching record found.",
            "linked_record_id": None,
            "confidence": None,
        }
        result = format_outcome(semantic)

        assert result["icon"] == "❓"
        assert "bg-gray" in result["css_bg"]

    def test_none_status_passthrough(self):
        """Records that aren't linkable return status=None unchanged."""
        from wslcb_licensing_tracker.display import format_outcome

        semantic = {"status": None}
        result = format_outcome(semantic)
        assert result["status"] is None
        assert "icon" not in result


# ── summarize_provenance ───────────────────────────────────────


class TestSummarizeProvenance:
    def test_groups_by_source_type(self):
        """Sources should be grouped by type with counts."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00", "role": "first_seen"},
            {"source_type": "live_scrape", "captured_at": "2025-06-16T12:00:00", "role": "confirmed"},
            {"source_type": "co_archive", "captured_at": "2025-06-10T00:00:00", "role": "confirmed"},
        ]
        result = summarize_provenance(sources)

        assert result["groups"]["live_scrape"]["count"] == 2
        assert result["groups"]["co_archive"]["count"] == 1

    def test_computes_date_range(self):
        """Summary should include first and last dates."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00", "role": "first_seen"},
            {"source_type": "live_scrape", "captured_at": "2025-06-20T12:00:00", "role": "confirmed"},
        ]
        result = summarize_provenance(sources)

        assert result["first_date"] == "2025-06-15"
        assert result["last_date"] == "2025-06-20"

    def test_detects_repaired(self):
        """Summary should flag if any source has 'repaired' role."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"source_type": "co_archive", "captured_at": "2025-06-10T00:00:00", "role": "repaired"},
        ]
        result = summarize_provenance(sources)

        assert result["repaired"] is True

    def test_no_repaired_flag(self):
        """When no repaired role, flag should be False."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00", "role": "first_seen"},
        ]
        result = summarize_provenance(sources)

        assert result["repaired"] is False

    def test_empty_sources(self):
        from wslcb_licensing_tracker.display import summarize_provenance

        result = summarize_provenance([])

        assert result["groups"] == {}
        assert result["first_date"] == ""
        assert result["last_date"] == ""
        assert result["repaired"] is False

    def test_group_badges_have_display_properties(self):
        """Each source type group should have icon, label, and CSS."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00", "role": "first_seen"},
            {"source_type": "internet_archive", "captured_at": "2025-05-01T00:00:00", "role": "confirmed"},
        ]
        result = summarize_provenance(sources)

        live = result["groups"]["live_scrape"]
        assert "icon" in live
        assert "label" in live
        assert "css_bg" in live

        ia = result["groups"]["internet_archive"]
        assert "icon" in ia
        assert "label" in ia

    def test_null_captured_at_ignored(self):
        """Sources without captured_at should not break date computation."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"source_type": "live_scrape", "captured_at": None, "role": "first_seen"},
            {"source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00", "role": "confirmed"},
        ]
        result = summarize_provenance(sources)

        assert result["first_date"] == "2025-06-15"
        assert result["last_date"] == "2025-06-15"

    def test_group_has_primary_source_id(self):
        """Each group in summarize_provenance should include primary_source_id."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"id": 10, "source_type": "live_scrape", "captured_at": "2025-06-14T12:00:00",
             "role": "confirmed", "snapshot_path": "some/path.html"},
            {"id": 11, "source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00",
             "role": "first_seen", "snapshot_path": "some/path2.html"},
            {"id": 20, "source_type": "co_archive", "captured_at": "2025-05-01T00:00:00",
             "role": "confirmed", "snapshot_path": "some/archive.html"},
        ]
        result = summarize_provenance(sources)

        # first_seen beats confirmed within same type
        assert result["groups"]["live_scrape"]["primary_source_id"] == 11
        # only one co_archive source
        assert result["groups"]["co_archive"]["primary_source_id"] == 20

    def test_primary_source_id_prefers_snapshot_path(self):
        """primary_source_id should prefer sources with a snapshot_path."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"id": 5, "source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00",
             "role": "confirmed", "snapshot_path": None},
            {"id": 6, "source_type": "live_scrape", "captured_at": "2025-06-14T12:00:00",
             "role": "confirmed", "snapshot_path": "some/path.html"},
        ]
        result = summarize_provenance(sources)
        assert result["groups"]["live_scrape"]["primary_source_id"] == 6

    def test_primary_source_id_first_seen_over_confirmed_no_snapshot(self):
        """first_seen role wins even when it has no snapshot path."""
        from wslcb_licensing_tracker.display import summarize_provenance

        sources = [
            {"id": 1, "source_type": "live_scrape", "captured_at": "2025-06-15T12:00:00",
             "role": "first_seen", "snapshot_path": None},
            {"id": 2, "source_type": "live_scrape", "captured_at": "2025-06-14T12:00:00",
             "role": "confirmed", "snapshot_path": "some/path.html"},
        ]
        result = summarize_provenance(sources)
        # first_seen with snapshot=None still beats confirmed with snapshot
        assert result["groups"]["live_scrape"]["primary_source_id"] == 1
