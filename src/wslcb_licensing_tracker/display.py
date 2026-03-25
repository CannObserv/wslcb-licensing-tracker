"""Presentation formatting for WSLCB licensing tracker.

Owns all display-layer concerns: CSS class mapping, icons, emoji,
badge text, and provenance aggregation.  Domain-layer functions
(``link_records.get_outcome_status()``, ``queries.get_record_sources()``)
return semantic data; this module adds the visual presentation.

Keeping presentation separate from domain logic ensures that adding a
JSON API or different frontend doesn't require touching the domain layer.
"""

from .pg_db import SOURCE_ROLE_PRIORITY as _ROLE_PRIORITY

# -- Outcome status display ------------------------------------------------

OUTCOME_STYLES: dict[str, dict[str, str]] = {
    "approved": {
        "icon": "✅",
        "css_bg": "bg-green-50",
        "css_border": "border-green-200",
        "css_text": "text-green-800",
        "css_accent": "text-green-600",
    },
    "discontinued": {
        "icon": "🚫",
        "css_bg": "bg-red-50",
        "css_border": "border-red-200",
        "css_text": "text-red-800",
        "css_accent": "text-red-600",
    },
    "pending": {
        "icon": "⏳",
        "css_bg": "bg-amber-50",
        "css_border": "border-amber-200",
        "css_text": "text-amber-800",
        "css_accent": "text-amber-600",
    },
    "data_gap": {
        "icon": "📁",
        "css_bg": "bg-slate-50",
        "css_border": "border-slate-200",
        "css_text": "text-slate-700",
        "css_accent": "text-slate-500",
    },
    "unknown": {
        "icon": "❓",
        "css_bg": "bg-gray-50",
        "css_border": "border-gray-200",
        "css_text": "text-gray-600",
        "css_accent": "text-gray-400",
    },
}


def format_outcome(outcome: dict) -> dict:
    """Add display properties (CSS, icons) to a semantic outcome dict.

    If ``outcome['status']`` is ``None`` (non-linkable record), the dict
    is returned unchanged.
    """
    status = outcome.get("status")
    if status is None:
        return outcome
    style = OUTCOME_STYLES.get(status)
    if style is None:
        return outcome
    return {**outcome, **style}


# -- Provenance display ----------------------------------------------------

_SOURCE_TYPE_DISPLAY: dict[str, dict[str, str]] = {
    "live_scrape": {
        "icon": "⚡",
        "label": "Live Scrape",
        "css_bg": "bg-green-50",
        "css_text": "text-green-700",
        "css_border": "border-green-200",
    },
    "co_archive": {
        "icon": "📁",
        "label": "CO Archive",
        "css_bg": "bg-co-purple-50",
        "css_text": "text-co-purple",
        "css_border": "border-co-purple-100",
    },
    "co_diff_archive": {
        "icon": "📄",
        "label": "CO Diff Archive",
        "css_bg": "bg-co-purple-50",
        "css_text": "text-co-purple",
        "css_border": "border-co-purple-100",
    },
    "internet_archive": {
        "icon": "🏛️",
        "label": "Internet Archive",
        "css_bg": "bg-amber-50",
        "css_text": "text-amber-700",
        "css_border": "border-amber-200",
    },
    "manual": {
        "icon": "✏️",
        "label": "Manual",
        "css_bg": "bg-gray-50",
        "css_text": "text-gray-600",
        "css_border": "border-gray-200",
    },
}

_DEFAULT_SOURCE_DISPLAY: dict[str, str] = {
    "icon": "•",
    "label": "Other",
    "css_bg": "bg-gray-50",
    "css_text": "text-gray-600",
    "css_border": "border-gray-200",
}


def _normalize_captured(raw: object) -> tuple[str, str]:
    """Return (iso_string, date_string) for a captured_at value."""
    if raw is None:
        return ("", "")
    if hasattr(raw, "isoformat"):
        return (raw.isoformat(), raw.strftime("%Y-%m-%d"))
    s = str(raw)
    return (s, s[:10])


def summarize_provenance(sources: list[dict]) -> dict:
    """Aggregate provenance sources into a display-ready summary.

    Returns a dict with:
      - ``groups``: ``{source_type: {count, icon, label, css_bg, css_text,
        css_border, primary_source_id}}`` — ``primary_source_id`` is the ``id``
        of the best source for that type (role priority, then snapshot presence,
        then newest ``captured_at``)
      - ``first_date``: earliest captured_at date (str, YYYY-MM-DD) or ``""``
      - ``last_date``: latest captured_at date (str) or ``""``
      - ``repaired``: ``True`` if any source has role ``'repaired'``
      - ``total``: total source count
    """
    groups: dict[str, dict] = {}
    # Tracks the best (priority, captured_at) seen per source_type for
    # primary_source_id selection.  Lower priority tuple = better.
    _group_best: dict[str, tuple] = {}  # source_type -> (role_rank, no_snap, neg_captured_at)

    first_date = ""
    last_date = ""
    repaired = False

    for s in sources:
        st = s.get("source_type", "")
        if st not in groups:
            display = _SOURCE_TYPE_DISPLAY.get(st, _DEFAULT_SOURCE_DISPLAY)
            groups[st] = {"count": 0, "primary_source_id": None, **display}
            _group_best[st] = (999, 999, "")
        groups[st]["count"] += 1

        # Compute priority tuple for this source within the group.
        role_rank = _ROLE_PRIORITY.get(s.get("role", ""), 2)
        no_snap = 0 if s.get("snapshot_path") else 1
        captured, captured_date = _normalize_captured(s.get("captured_at"))
        # Store as (role_rank, no_snap, captured_at) — compare captured_at descending.
        current_best = _group_best[st]
        # Compare: lower role_rank and no_snap is better; higher captured_at is better.
        new_better = (
            role_rank < current_best[0]
            or (role_rank == current_best[0] and no_snap < current_best[1])
            or (
                role_rank == current_best[0]
                and no_snap == current_best[1]
                and captured > current_best[2]
            )
        )
        if groups[st]["primary_source_id"] is None or new_better:
            groups[st]["primary_source_id"] = s.get("id")
            _group_best[st] = (role_rank, no_snap, captured)

        if s.get("role") == "repaired":
            repaired = True

        if captured_date:
            if not last_date or captured_date > last_date:
                last_date = captured_date
            if not first_date or captured_date < first_date:
                first_date = captured_date

    return {
        "groups": groups,
        "first_date": first_date,
        "last_date": last_date,
        "repaired": repaired,
        "total": len(sources),
    }
