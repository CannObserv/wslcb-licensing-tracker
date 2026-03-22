"""Pure-string text normalization utilities for the WSLCB licensing tracker.

These helpers have no database dependency and are used by both the ingestion
pipeline and schema migrations.  Extracted from ``db.py`` so they can be
imported without pulling in SQLAlchemy or SQLite machinery.
"""

import re

# ---------------------------------------------------------------------------
# Compiled regexes
# ---------------------------------------------------------------------------

# Suffixes where a trailing period is legitimate and should be kept.
# The full list: INC, LLC, L.L.C, LTD, CORP, CO, L.P, L.L.P, PTY, P.C, N.A, P.A,
# JR, SR, S.P.A, F.O.E, U.P, D.B.A, W. & S
_LEGIT_TRAILING_DOT = re.compile(
    r"(?:(?<=\s)|(?<=^))"
    r"(?:INC|LLC|L\.L\.C|L\.L\.P|LTD|CORP|CO|L\.P|PTY"
    r"|JR|SR"
    r"|S\.P\.A|F\.O\.E|U\.P|D\.B\.A|P\.C|N\.A|P\.A"
    r"|W\. & S)"
    r"\.\s*$"
)

# Regex matching WSLCB "DUPLICATE" annotation tokens embedded in applicant names.
_DUPLICATE_MARKER_RE = re.compile(
    r"\s*"
    r"(?:"
    r"\(\s*DUPLICATE(?:\s+\d+)?\s*\)"  # (DUPLICATE), (DUPLICATE 2), ... — closed
    r"|\*DUPLICATE\*"  # *DUPLICATE*
    r"|\(\s*DUPLICATE(?:\s+\d+)?"  # (DUPLICATE ... — unclosed paren
    r"|DUPLICATE"  # bare word
    r")",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def _normalize_raw_address(raw: str) -> str:
    r"""Normalize whitespace variants (NBSP → space) in raw address strings.

    The WSLCB source page sometimes uses non-breaking spaces (\xa0)
    instead of regular spaces.  We normalize before lookup so that
    cosmetically-identical strings map to the same location row.
    """
    if not raw:
        return raw
    return re.sub(r"\xa0+", " ", raw)


def clean_entity_name(name: str) -> str:
    """Normalize an entity name: uppercase, strip whitespace, remove stray trailing punctuation.

    The WSLCB source occasionally appends periods or commas to names
    as data-entry artifacts (e.g., ``WOLDU ARAYA BERAKI.``).  This
    strips those while preserving legitimate endings like ``INC.`` or
    ``JR.``.
    """
    cleaned = name.strip().upper()
    cleaned = re.sub(r"\s+", " ", cleaned)
    while cleaned and cleaned[-1] in ".," and not _LEGIT_TRAILING_DOT.search(cleaned):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def strip_duplicate_marker(name: str) -> str:
    """Remove WSLCB DUPLICATE annotation token(s) from an applicant name.

    Handles all observed formats::

        ADAM (DUPLICATE) BENTON           -> ADAM BENTON
        NEALY DUPLICATE EVANS             -> NEALY EVANS
        KATIE (DUPLICATE 2) DAVIS         -> KATIE DAVIS
        PAUL *DUPLICATE* SONG             -> PAUL SONG
        DUPLICATE ITALIAN SUPPLY, LLC     -> ITALIAN SUPPLY, LLC
        JAY WON (DUPLICATE)               -> JAY WON
        ELIZABETH (DUPLICATE A MATTHEWS   -> ELIZABETH A MATTHEWS  (unclosed paren)

    Collapses any resulting runs of whitespace and strips leading/trailing
    spaces.  The caller is responsible for full normalization (e.g. uppercase)
    via ``clean_entity_name()``.
    """
    stripped = _DUPLICATE_MARKER_RE.sub("", name)
    return re.sub(r" {2,}", " ", stripped).strip()


def clean_applicants_string(applicants: str | None) -> str | None:
    """Clean each semicolon-separated part of an applicants string.

    Applies ``strip_duplicate_marker()`` then ``clean_entity_name()`` to
    every element so the stored string is consistent with entity names in
    the ``entities`` table.  After stripping, duplicate tokens are removed
    (first occurrence wins — preserving order).  Empty parts after cleaning
    are dropped.  Returns ``None`` unchanged.
    """
    if not applicants:
        return applicants
    parts = [clean_entity_name(strip_duplicate_marker(p)) for p in applicants.split(";")]
    seen: set[str] = set()
    deduped: list[str] = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            deduped.append(p)
    return "; ".join(deduped)
