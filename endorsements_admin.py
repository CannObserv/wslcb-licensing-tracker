"""Admin UI query helpers for endorsement management.

This module contains the admin-facing endorsement helpers:
- Similarity algorithm for duplicate detection
- ``get_endorsement_list()``
- ``suggest_duplicate_endorsements()`` / ``dismiss_suggestion()``
- Code-mapping CRUD: ``get_code_mappings()``, ``add_code_mapping()``,
  ``remove_code_mapping()``, ``create_code()``

Note: ``get_endorsement_groups()`` is the legacy grouped view and lives in
``endorsements.py``, not here.

All functions that perform mutations accept a caller-supplied ``conn``;
the caller is responsible for committing.  Audit logging is performed
by the route handler, not here, so this module has no dependency on
``admin_audit``.
"""
import logging
import re
import sqlite3
from itertools import combinations

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Similarity algorithm
# ---------------------------------------------------------------------------

# Stop-words stripped before similarity comparison.
_SIM_STOP_WORDS: frozenset[str] = frozenset({
    "ENDORSEMENT", "THE", "AND", "OF", "FOR", "A",
})

# Spelling normalizations applied *before* tokenization (phrase → phrase).
_SIM_SPELLING: list[tuple[str, str]] = [
    ("TAKE OUT", "TAKEOUT"),
]

_SIM_THRESHOLD: float = 0.70


def _sim_normalize(name: str) -> str:
    """Apply phrase-level spelling normalisations before tokenising."""
    upper = name.upper()
    for old, new in _SIM_SPELLING:
        upper = upper.replace(old, new)
    return upper


def _sim_tokenize(name: str) -> list[str]:
    """Split *name* into uppercase tokens, strip punctuation & stop-words.

    ``+`` and ``-`` are *preserved* as standalone tokens because they carry
    semantic meaning in WSLCB endorsement names (e.g. ``SPIRITS/BR/WN REST
    LOUNGE +`` vs ``SPIRITS/BR/WN REST LOUNGE -`` are distinct licence
    categories, not spelling variants).

    ``<`` and ``>`` are stripped because they are HTML-parsing artefacts
    that produce mangled names like ``DOMESTIC WINERY  249,999 LITERS``
    (the ``>`` was lost, leaving an extra space).
    """
    normalized = _sim_normalize(name)
    # Normalise + and - to sentinel tokens before splitting on other punctuation
    # so they survive as discrete tokens rather than being swallowed by the
    # general punctuation regex.
    sentinel = re.sub(r"\+", " PLUS ", normalized)
    sentinel = re.sub(r"(?<![a-zA-Z0-9])\-(?![a-zA-Z0-9])", " MINUS ", sentinel)
    # Remove < > and remaining punctuation/whitespace
    cleaned = re.sub(r"[<>,.;:!?/\\()[\]{}\"'`@#$%^&*=|~]", " ", sentinel)
    tokens = [
        t for t in cleaned.split()
        if t and t not in _SIM_STOP_WORDS
    ]
    return tokens


def _sim_features(tokens: list[str]) -> frozenset[str]:
    """Return unigrams + adjacent bigrams for *tokens*."""
    unigrams: list[str] = tokens
    bigrams: list[str] = [
        f"{tokens[i]} {tokens[i + 1]}" for i in range(len(tokens) - 1)
    ]
    return frozenset(unigrams + bigrams)


_SIM_POLAR_TOKENS: frozenset[str] = frozenset({"PLUS", "MINUS"})


def endorsement_similarity(name_a: str, name_b: str) -> float:
    """Blended similarity between two endorsement name strings.

    Score = 0.70 × Jaccard(token+bigram features) + 0.30 × containment.

    Containment = max(|A∩B|/|A|, |A∩B|/|B|) on unigram sets, which catches
    cases where one name is a proper subset of the other.

    Hard rule: if the symmetric difference of the unigram sets is exactly
    ``{PLUS, MINUS}`` (i.e. the two names are identical except that one has
    ``+`` and the other has ``-``), return 0.0.  These are semantically
    distinct licence categories, not spelling variants.

    Returns a float in [0.0, 1.0].
    """
    tok_a = _sim_tokenize(name_a)
    tok_b = _sim_tokenize(name_b)
    if not tok_a or not tok_b:
        return 0.0

    uni_a = frozenset(tok_a)
    uni_b = frozenset(tok_b)

    # Hard exclusion: sole difference is + vs − polarity token.
    if (uni_a | uni_b) - (uni_a & uni_b) == _SIM_POLAR_TOKENS:
        return 0.0

    feat_a = _sim_features(tok_a)
    feat_b = _sim_features(tok_b)

    # Jaccard on token+bigram features
    intersection = feat_a & feat_b
    union = feat_a | feat_b
    jaccard = len(intersection) / len(union) if union else 0.0

    # Containment on plain unigrams
    uni_inter = uni_a & uni_b
    containment = max(
        len(uni_inter) / len(uni_a) if uni_a else 0.0,
        len(uni_inter) / len(uni_b) if uni_b else 0.0,
    )

    return 0.70 * jaccard + 0.30 * containment


# ---------------------------------------------------------------------------
# Endorsement list / group helpers
# ---------------------------------------------------------------------------

def get_endorsement_list(conn: sqlite3.Connection) -> list[dict]:
    """Return all endorsements as a flat list for the revised admin UI.

    Each dict has:
    - ``id``, ``name``, ``record_count``
    - ``is_canonical`` (True if at least one alias points to this endorsement)
    - ``is_variant`` (True if this endorsement has an alias pointing elsewhere)
    - ``canonical_id``, ``canonical_name`` (None if not a variant)
    - ``codes`` — list of numeric code strings this endorsement is mapped to
    """
    rows = conn.execute("""
        SELECT
            le.id,
            le.name,
            COUNT(re.record_id) AS record_count
        FROM license_endorsements le
        LEFT JOIN record_endorsements re ON re.endorsement_id = le.id
        GROUP BY le.id
        ORDER BY le.name COLLATE NOCASE
    """).fetchall()

    alias_map: dict[int, int] = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT endorsement_id, canonical_endorsement_id FROM endorsement_aliases"
        ).fetchall()
    }
    canonical_ids: set[int] = set(alias_map.values())

    # canonical name lookup
    name_by_id: dict[int, str] = {r[0]: r[1] for r in rows}

    eid_to_codes: dict[int, list[str]] = {}
    for eid, code in conn.execute(
        "SELECT endorsement_id, code FROM endorsement_codes"
    ).fetchall():
        eid_to_codes.setdefault(eid, []).append(code)

    result = []
    for eid, name, count in rows:
        canonical_id = alias_map.get(eid)
        result.append({
            "id": eid,
            "name": name,
            "record_count": count,
            "is_canonical": eid in canonical_ids,
            "is_variant": eid in alias_map,
            "canonical_id": canonical_id,
            "canonical_name": name_by_id.get(canonical_id) if canonical_id else None,
            "codes": sorted(eid_to_codes.get(eid, []), key=lambda c: (0, int(c)) if c.isdigit() else (1, c)),
        })
    return result


def suggest_duplicate_endorsements(
    conn: sqlite3.Connection,
    threshold: float = _SIM_THRESHOLD,
) -> list[dict]:
    """Return suggested duplicate endorsement pairs above *threshold*.

    Pairs that are already aliased or have been dismissed are excluded.
    Each dict has:
    - ``id_a``, ``name_a``, ``count_a``
    - ``id_b``, ``name_b``, ``count_b``
    - ``score`` (float)

    Ordered by score descending.
    """
    # Fetch all non-variant endorsements (variants already resolved)
    rows = conn.execute("""
        SELECT le.id, le.name, COUNT(re.record_id) AS cnt
        FROM license_endorsements le
        LEFT JOIN record_endorsements re ON re.endorsement_id = le.id
        WHERE le.id NOT IN (SELECT endorsement_id FROM endorsement_aliases)
        GROUP BY le.id
    """).fetchall()

    # Already-aliased pairs (either direction)
    aliased_pairs: set[tuple[int, int]] = set()
    for a, b in conn.execute(
        "SELECT endorsement_id, canonical_endorsement_id FROM endorsement_aliases"
    ).fetchall():
        aliased_pairs.add((min(a, b), max(a, b)))

    # Dismissed pairs
    dismissed_pairs: set[tuple[int, int]] = {
        (r[0], r[1])
        for r in conn.execute(
            "SELECT endorsement_id_a, endorsement_id_b FROM endorsement_dismissed_suggestions"
        ).fetchall()
    }

    suggestions: list[dict] = []
    for (id_a, name_a, cnt_a), (id_b, name_b, cnt_b) in combinations(rows, 2):
        pair = (min(id_a, id_b), max(id_a, id_b))
        if pair in aliased_pairs or pair in dismissed_pairs:
            continue
        score = endorsement_similarity(name_a, name_b)
        if score >= threshold:
            # normalise so id_a < id_b
            if id_a > id_b:
                id_a, name_a, cnt_a, id_b, name_b, cnt_b = id_b, name_b, cnt_b, id_a, name_a, cnt_a
            suggestions.append({
                "id_a": id_a,
                "name_a": name_a,
                "count_a": cnt_a,
                "id_b": id_b,
                "name_b": name_b,
                "count_b": cnt_b,
                "score": round(score, 3),
            })

    suggestions.sort(key=lambda s: s["score"], reverse=True)
    return suggestions


def dismiss_suggestion(
    conn: sqlite3.Connection,
    id_a: int,
    id_b: int,
    dismissed_by: str,
) -> None:
    """Permanently suppress a duplicate suggestion pair.

    *id_a* and *id_b* are normalised (smaller first) before insert.
    Idempotent — re-dismissing the same pair is a no-op.
    """
    a, b = (min(id_a, id_b), max(id_a, id_b))
    conn.execute(
        """
        INSERT OR IGNORE INTO endorsement_dismissed_suggestions
            (endorsement_id_a, endorsement_id_b, dismissed_by)
        VALUES (?, ?, ?)
        """,
        (a, b, dismissed_by),
    )


# ---------------------------------------------------------------------------
# Code-mapping CRUD
# ---------------------------------------------------------------------------

def get_code_mappings(conn: sqlite3.Connection) -> list[dict]:
    """Return all WSLCB numeric codes with their endorsement mappings.

    Each dict has:
    - ``code`` — the numeric string (e.g. ``'450'``)
    - ``endorsements`` — list of ``{id, name, record_count, is_variant, canonical_id, canonical_name}``
    - ``record_count`` — total records ingested with this code as ``license_type``

    Ordered numerically by code.
    """
    # code → endorsements
    code_to_eids: dict[str, list[int]] = {}
    for code, eid in conn.execute(
        "SELECT code, endorsement_id FROM endorsement_codes ORDER BY code"
    ).fetchall():
        code_to_eids.setdefault(code, []).append(eid)

    # endorsement metadata
    eid_meta: dict[int, dict] = {}
    for eid, name, cnt in conn.execute("""
        SELECT le.id, le.name, COUNT(re.record_id)
        FROM license_endorsements le
        LEFT JOIN record_endorsements re ON re.endorsement_id = le.id
        GROUP BY le.id
    """).fetchall():
        eid_meta[eid] = {"id": eid, "name": name, "record_count": cnt}

    alias_map: dict[int, int] = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT endorsement_id, canonical_endorsement_id FROM endorsement_aliases"
        ).fetchall()
    }
    name_by_id = {m["id"]: m["name"] for m in eid_meta.values()}

    # records per code (license_type matches "CODE," or "CODE, NAME" patterns)
    # We count records_endorsements via code's endorsement set as a proxy
    # (exact code-string matching in license_records is unreliable for historical data)
    code_record_counts: dict[str, int] = {
        r[0]: r[1]
        for r in conn.execute("""
            SELECT ec.code, COUNT(DISTINCT re.record_id)
            FROM endorsement_codes ec
            JOIN record_endorsements re ON re.endorsement_id = ec.endorsement_id
            GROUP BY ec.code
        """).fetchall()
    }

    def _sort_key(code: str) -> tuple:
        try:
            return (0, int(code))
        except ValueError:
            return (1, code)

    result = []
    for code in sorted(code_to_eids.keys(), key=_sort_key):
        eids = code_to_eids[code]
        endorsements = []
        for eid in eids:
            meta = eid_meta.get(eid, {"id": eid, "name": str(eid), "record_count": 0})
            canonical_id = alias_map.get(eid)
            endorsements.append({
                **meta,
                "is_variant": eid in alias_map,
                "canonical_id": canonical_id,
                "canonical_name": name_by_id.get(canonical_id) if canonical_id else None,
            })
        endorsements.sort(key=lambda e: e["name"])
        result.append({
            "code": code,
            "endorsements": endorsements,
            "record_count": code_record_counts.get(code, 0),
        })
    return result


def add_code_mapping(
    conn: sqlite3.Connection,
    code: str,
    endorsement_id: int,
) -> bool:
    """Add *endorsement_id* to the expansion of *code*.

    Returns True if a new row was inserted, False if it already existed.
    """
    try:
        conn.execute(
            "INSERT INTO endorsement_codes (code, endorsement_id) VALUES (?, ?)",
            (code, endorsement_id),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def remove_code_mapping(
    conn: sqlite3.Connection,
    code: str,
    endorsement_id: int,
) -> bool:
    """Remove *endorsement_id* from the expansion of *code*.

    Returns True if a row was deleted, False if it did not exist.
    """
    cursor = conn.execute(
        "DELETE FROM endorsement_codes WHERE code = ? AND endorsement_id = ?",
        (code, endorsement_id),
    )
    return cursor.rowcount > 0


def create_code(
    conn: sqlite3.Connection,
    code: str,
    endorsement_ids: list[int],
) -> int:
    """Create a new code entry and assign the given endorsement(s) to it.

    Returns the number of endorsement mappings inserted.  Existing mappings
    for the same (code, endorsement_id) pair are silently skipped.
    """
    inserted = 0
    for eid in endorsement_ids:
        if add_code_mapping(conn, code, eid):
            inserted += 1
    return inserted
