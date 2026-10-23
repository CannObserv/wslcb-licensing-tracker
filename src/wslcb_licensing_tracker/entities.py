"""Async PostgreSQL entity normalization for WSLCB licensing tracker.

Uses async SQLAlchemy Core with table objects from models.py.

Manages the ``entities`` and ``record_entities`` tables — extracting
individual people and organizations from semicolon-delimited applicant
strings, deduplicating by name, and linking them to license records.
"""

import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import delete, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .entity_classification import (
    SourceAnswer,
    confirmed_noise_ids,
    current_answers,
    protected_ids,
    resolve_entities,
    with_jev_answer,
)
from .models import (
    entities,
    entity_classifications,
    entity_review_flags,
    license_records,
    record_enrichments,
    record_entities,
)
from .text_utils import clean_entity_name, strip_duplicate_marker

logger = logging.getLogger(__name__)

# Enrichment version written by reprocess_entities.
# Bump this integer when entity processing logic changes.
_ENTITY_REPROCESS_VERSION = 4

# Version of the regex classifier rules (``_CATEGORY_CHECKS`` + ``REGEX_SUBTYPE``),
# stored as ``entity_classifications.model`` on regex rows (#206).  Bump it only
# when the rules change: ``reclassify_entities`` then writes a fresh regex row
# for every entity.  Kept separate from ``_ENTITY_REPROCESS_VERSION`` so a
# name-cleaning change does not re-stamp ~67K identical regex answers.
_REGEX_CLASSIFIER_VERSION = 1
REGEX_MODEL = f"regex-{_REGEX_CLASSIFIER_VERSION}"

# Meta-labels that WSLCB embeds in applicant lists as truncation notices.
# These are not real people or organizations and must be excluded from entity creation.
ADDITIONAL_NAMES_MARKERS: frozenset[str] = frozenset(
    {
        "ADDITIONAL NAMES ON FILE",
        "ADDTIONAL NAMES ON FILE",  # typo variant present in WSLCB source
    }
)

# ---------------------------------------------------------------------------
# Noise filter (#203)
# ---------------------------------------------------------------------------
# Applicant-list parts that are not names at all: WAC citations, ownership-count
# annotations, and fragments of a business name split on a stray ';'.  Census of
# production on 2026-10-22; see docs/research/person-vs-org-classification.md.
# Real names that merely carry an appended citation
# (``7-ELEVEN, INC. PER WAC 314-07-035``) are *not* noise and are left as-is.

# Exact fragments observed in production.
_NOISE_EXACT: frozenset[str] = frozenset(
    {
        "TRUSTEE",
        "SOCIETY",
        "RESERVATION",
        "(ESTATE PROJECT)",
        "ADVISORY COUNCIL MEMBERS",
    }
)

_NOISE_PATTERNS = re.compile(
    r"^(?:"
    # WAC citation with nothing else: PER WAC 314-07-035, (PER WAC …(1)), ON LIC PER WAC …
    r"\(?\s*(?:ON\s+LIC(?:ENSE)?\s+)?(?:AS\s+)?(?:PER\s+)?WAC\b[\s\d.\-()]*"
    # Ownership-count annotations: 17 SHAREHOLDERS W/10% OR LESS, 4 MBRS W/LESS …
    r"|\d+\s+(?:SHAREHOLDERS?|STOCKHOLDERS?|MBRS|MEMBERS?|OWNERS?)\b.*"
    # Non-profit annotation: NO PEOPLE REQUIRED[ - NON PROFIT ARTS]
    r"|NO\s+PEOPLE\s+REQUIRED\b.*"
    # A bare legal suffix split off its business name
    r"|(?:LLC|L\.L\.C\.?|INC\.?|CORP\.?|CORPORATION|PARTNERSHIP|LTD\.?|LIMITED|LLP|LP)"
    r")$"
)


def is_noise_name(name: str) -> bool:
    """Return True if a cleaned applicant-list part is not a person or organization name.

    Catches strings with no letters (``035``, ``314-07-035``, ``5``), single
    characters, bare WAC citations, ownership-count annotations, bare legal
    suffixes, and a short list of fragments observed in production.
    """
    if len(name) <= 1 or not re.search(r"[A-Z]", name):
        return True
    return name in _NOISE_EXACT or _NOISE_PATTERNS.match(name) is not None


# ---------------------------------------------------------------------------
# Person vs. organization classifier (#203)
# ---------------------------------------------------------------------------
# One regex per category.  ``entity_type`` is binary; the category maps to an
# ``entity_subtype`` through ``REGEX_SUBTYPE`` (#206).  Estates, trusts, tribes,
# governments and clubs are all ``organization``.  Validated against
# docs/research/person-vs-org/gold.csv.
#
# Token boundaries use lookarounds, not ``\b``: ``CO\.\b`` can never match
# before a space or end of string.  Avoid bare words that are common surnames
# (CHURCH, POST, INN, PIZZA, YOUNG, TRUSTY, LODGE, BAR, GRILL, KITCHEN, TEMPLE,
# WASHINGTON, …) — use anchored multi-token forms instead.

_B = r"(?<![A-Z0-9'])"  # token start
_E = r"(?![A-Z0-9'])"  # token end


def _words(*alts: str) -> str:
    return _B + "(?:" + "|".join(alts) + ")" + _E


# WSLCB truncates entries at 30 (older: 45) characters.  The tail rules below
# only fire on names at least this long.
_TRUNCATION_MIN_LEN = 28

_TRUST = re.compile(
    _words(
        r"TRUSTS?",
        r"TRUSTEES?",
        r"TRUSTE",
        r"TRST",
        r"TTEES?",
        r"TRSTE",
        r"REVOC\w*",
        r"IRREV\w*",
        r"IRREVOKABLE",
        r"ILIT",
        r"FBO",
        r"SURVIVOR'?S",
        r"TRUS",
    )
    + r"|"
    + _B
    + r"(?:LIVING|FAMILY|REVOCABLE|BUSINESS|LAND|MARITAL|BYPASS|CHARITABLE|GRANTOR|\d{4})"
    r"\s+TRU?S?T?" + _E
)
_TRUST_TRUNCATED = re.compile(_B + r"FAMILY(?:\s+T)?$")

_LEGAL_SUFFIX = re.compile(
    _words(
        r"LLC",
        r"L\.L\.C\.?",
        r"L\.LC",
        r"LLP",
        r"L\.L\.P\.?",
        r"PLLC",
        r"INC\.?",
        r"INCORPORATED",
        r"CORP\.?",
        r"CORPORATION",
        r"LTD\.?",
        r"LIMITED",
        r"CO\.?",
        r"COMPANY",
        r"CO-OP",
        r"LP",
        r"L\.P\.?",
        r"PTY",
        r"GMBH",
        r"S\.A\.?",
        r"S\.P\.A\.?",
        r"SPA",
        r"N\.?V\.?",
        r"PARTNERS",
        r"PARTNERSHIP",
        r"HOLDINGS",
        r"GROUP",
        r"ENTERPRISES?",
        r"ASSOCIATION",
        r"ASSOCIATES",
        r"FOUNDATION",
        r"DBA",
        r"D/B/A",
    )
    # Suffixes that double as initials only count at the end of the name.
    + r"|,\s*S\.?L\.?$|[\s,]P\.[CS]\.?$"
)

# Truncated legal-suffix tails: ``, LL`` / ``L.L`` anywhere; ``, L`` and word
# prefixes only on names long enough to have been cut off.
_TRUNC_ANYWHERE = re.compile(r",\s*LL\.?$|" + _B + r"L\.L\.?$")
_TRUNC_LONG = re.compile(r",\s*L\.?$")
# word -> shortest prefix accepted as a truncated tail
_TRUNCATED_WORDS: dict[str, int] = {
    "INCORPORATED": 4,
    "CORPORATION": 5,
    "COMPANY": 4,
    "LIMITED": 4,
    "PARTNERSHIP": 5,
    "HOLDINGS": 5,
    "ENTERPRISES": 5,
    "ASSOCIATION": 5,
    "ASSOCIATES": 5,
    "FOUNDATION": 5,
    "INVESTMENTS": 5,
    "HOSPITALITY": 5,
    "MANAGEMENT": 5,
    "RESTAURANT": 5,
    "DEVELOPMENT": 5,
    "VENTURES": 5,
    "INTERNATIONAL": 6,
    "PROPERTIES": 5,
    "DISTRIBUTING": 6,
    "DISTRIBUTION": 6,
    "DISTRIBUTORS": 6,
    "ENTERTAINMENT": 6,
    "BREWING": 5,
    "BREWERY": 5,
    "DISTILLERY": 6,
    "WINERY": 5,
    "VINEYARDS": 5,
}
_TAIL_TOKEN = re.compile(r"([A-Z]+)\W*$")

# Tribe is checked before government, so CONFEDERATED TRIBES … COUNTY is a tribe.
_TRIBE = re.compile(
    _words(
        r"TRIBES?",
        r"TRIBAL",
        r"INDIANS",
        r"CONFEDERATED",
        r"RESERVATION",
        r"INDIAN NATION",
        r"NATION OF",
        r"BAND OF",
    )
)

_GOVERNMENT = re.compile(
    _words(
        r"COUNTY",
        r"DISTRICT",
        r"UNIVERSITY",
        r"COLLEGE",
        r"SCHOOLS?",
        r"DEPARTMENT",
        r"COMMISSION",
        r"MUNICIPAL",
        r"GOVERNMENT",
        r"AUTHORITY",
        r"REGIONAL",
        r"(?:CITY|TOWN|COUNTY|STATE|PORT|VILLAGE|BOARD|CHAMBER) OF",
        r"CITY LIGHT",
        r"PUBLIC UTILITY",
    )
)

_ORG_KEYWORD = re.compile(
    _words(
        # clubs, fraternal, civic, religious
        r"CLUB",
        r"YACHT",
        r"GOLF",
        r"LEGION",
        r"VFW",
        r"ELKS",
        r"MOOSE\s?LODGE\w*",
        r"AERIE",
        r"FRATERNAL",
        r"BENEVOLENT",
        r"ORDER OF",
        r"AUXILIARY",
        r"SOCIETY",
        r"MUSEUM",
        r"THEATRE",
        r"THEATER",
        r"PRESBYTERIAN",
        r"LUTHERAN",
        r"CATHOLIC",
        r"DIOCESE",
        r"MINISTRIES",
        r"CHURCH OF",
        r"(?:BAPTIST|METHODIST|EPISCOPAL|COMMUNITY|LIFE|BIBLE|CHRISTIAN|UNITED|EVANGELICAL)"
        r" CHURCH",
        r"LODGE\s*(?:NO\b\.?|#|\d)",
        r"POST\s*(?:NO\b\.?\s*)?#?\s*\d+",
        # alcohol / food / hospitality
        r"WINERY",
        r"WINERIES",
        r"WINES",
        r"VINEYARDS?",
        r"VINTNERS",
        r"WINEGROWERS",
        r"CELLARS",
        r"BREWING",
        r"BREWERY",
        r"BREWERIES",
        r"BREWERS",
        r"BREWPUB",
        r"BREWHOUSE",
        r"TAPROOM",
        r"TAPHOUSE",
        r"ALEHOUSE",
        r"DISTILLERY",
        r"DISTILLERIES",
        r"DISTILLING",
        r"DISTILLERS",
        r"SPIRITS",
        r"CIDERY",
        r"CIDER",
        r"CIDERWORKS",
        r"MEADERY",
        r"TAVERN",
        r"SALOON",
        r"PUB",
        r"RESTAURANTS?",
        r"CAFE",
        r"CAFES",
        r"CAFFE",
        r"COFFEE",
        r"ESPRESSO",
        r"BAKERY",
        r"PIZZERIA",
        r"EATERY",
        r"TAQUERIA",
        r"CANTINA",
        r"BISTRO",
        r"BRASSERIE",
        r"TRATTORIA",
        r"STEAKHOUSE",
        r"ROADHOUSE",
        r"LOUNGE",
        r"NIGHTCLUB",
        r"CATERING",
        r"FOODS",
        r"FOODSERVICE",
        r"GROCERY",
        r"SUPERMARKET",
        r"LIQUORS?",
        r"BEVERAGES?",
        r"HOSPITALITY",
        r"HOTELS?",
        r"MOTEL",
        r"RESORTS?",
        r"GAMING",
        r"(?:&|AND) BAR",
        r"BAR (?:&|AND)",
        r"(?:WINE|DESSERT|SPORTS|COCKTAIL|OYSTER|TAPAS|SUSHI|PIANO|TIKI) BAR",
        r"(?:FIRED|STYLE|CERTIFIED) PIZZA",
        r"PIZZA (?:CO|COMPANY|HUT|PARLOR|HOUSE|SHOP|FACTORY)",
        # commerce
        r"DISTRIBUTING",
        r"DISTRIBUTION",
        r"DISTRIBUTORS",
        r"IMPORTS",
        r"IMPORTING",
        r"IMPORTERS",
        r"EXPORTS",
        r"TRADING",
        r"INDUSTRIES",
        r"INVESTMENTS?",
        r"INVESTORS",
        r"VENTURES?",
        r"PROPERTIES",
        r"REALTY",
        r"REAL ESTATE",
        r"ESTATES",
        r"DEVELOPMENTS?",
        r"MANAGEMENT",
        r"MGMT",
        r"SERVICES",
        r"SOLUTIONS",
        r"CONSULTING",
        r"CONSULTANTS",
        r"ENTERTAINMENT",
        r"PRODUCTIONS",
        r"STUDIOS",
        r"HEALTHCARE",
        r"PHARMACY",
        r"RETIREMENT",
        r"SENIOR LIVING",
        r"COOPERATIVE",
        r"SMOKE SHOP",
        r"PETROLEUM",
        r"FRANCHISOR",
        r"FRANCHISE",
        r"OPCO",
        r"PROPCO",
        r"TECHNOLOGIES",
        r"LABORATORIES",
        r"LOGISTICS",
        r"LEASING",
        r"AIRPORT",
        r"SHIPPING",
        r"INTERNATIONAL",
        r"PROJECTS?",
        r"COLLECTIVE",
    )
)

# GIVEN ESTATE SURNAME, NAME - ESTATE, NAME-ESTATE, ESTATE OF NAME.
_ESTATE = re.compile(r"(?<![A-Z])ESTATE(?![A-Z])")


def _is_truncated_suffix(name: str) -> bool:
    if _TRUNC_ANYWHERE.search(name):
        return True
    if len(name) < _TRUNCATION_MIN_LEN:
        return False
    if _TRUNC_LONG.search(name):
        return True
    m = _TAIL_TOKEN.search(name)
    if not m:
        return False
    tail = m.group(1)
    return any(
        len(tail) >= min_len and word.startswith(tail) for word, min_len in _TRUNCATED_WORDS.items()
    )


def _is_trust(name: str) -> bool:
    if _TRUST.search(name):
        return True
    return len(name) >= _TRUNCATION_MIN_LEN and _TRUST_TRUNCATED.search(name) is not None


# Checked in order; first match wins.  Estate is last so ESTATE WINES /
# REAL ESTATE fall to their org category first.
_CATEGORY_CHECKS: tuple[tuple[str, Callable[[str], bool]], ...] = (
    ("trust", _is_trust),
    ("legal_suffix", lambda n: _LEGAL_SUFFIX.search(n) is not None),
    ("truncated_suffix", _is_truncated_suffix),
    ("tribe", lambda n: _TRIBE.search(n) is not None),
    ("government", lambda n: _GOVERNMENT.search(n) is not None),
    ("org_keyword", lambda n: _ORG_KEYWORD.search(n) is not None),
    ("estate", lambda n: _ESTATE.search(n) is not None),
)


def entity_category(name: str) -> str | None:
    """Return the non-person category matched by *name*, or None for a person.

    Categories, checked in order: ``trust``, ``legal_suffix``,
    ``truncated_suffix``, ``tribe``, ``government``, ``org_keyword``, ``estate``.
    *name* is expected to be cleaned (uppercase; see ``clean_entity_name``).
    """
    return next((cat for cat, check in _CATEGORY_CHECKS if check(name)), None)


# Regex category -> entity_subtype.  The regex never claims ``nonprofit`` (an
# INC may be either); only Jev or an operator can.
REGEX_SUBTYPE: dict[str, str] = {
    "trust": "trust",
    "estate": "estate",
    "tribe": "tribe",
    "government": "government",
    "legal_suffix": "business",
    "truncated_suffix": "business",
    "org_keyword": "business",
}


def regex_classification(name: str) -> tuple[str, str | None]:
    """Return the regex's ``(entity_type, entity_subtype)`` for a cleaned *name*.

    ``("person", None)`` when no category matches.
    """
    cat = entity_category(name)
    return ("organization", REGEX_SUBTYPE[cat]) if cat else ("person", None)


def _classify_entity_type(name: str) -> str:
    """Classify an entity name as 'person' or 'organization'."""
    return regex_classification(name)[0]


async def get_or_create_entity(conn: AsyncConnection, name: str) -> int:
    """Return the entity id for *name*, creating if needed.

    Names are cleaned via ``clean_entity_name`` and uppercased.
    Uses ON CONFLICT DO NOTHING + RETURNING + fallback SELECT for idempotency.
    A real insert also writes the entity's regex ``entity_classifications``
    row; the resolved columns start as the regex answer (#206).

    Raises ``ValueError`` if *name* is empty after cleaning.
    """
    normalized = clean_entity_name(name)
    if not normalized:
        msg = "Entity name must not be empty"
        raise ValueError(msg)

    entity_type, entity_subtype = regex_classification(normalized)

    stmt = (
        pg_insert(entities)
        .values(
            name=normalized,
            entity_type=entity_type,
            entity_subtype=entity_subtype,
            classification_source="regex",
        )
        .on_conflict_do_nothing(index_elements=["name"])
        .returning(entities.c.id)
    )
    row_id = (await conn.execute(stmt)).scalar_one_or_none()
    if row_id is not None:
        await conn.execute(
            insert(entity_classifications).values(
                entity_id=row_id,
                source="regex",
                entity_type=entity_type,
                entity_subtype=entity_subtype,
                model=REGEX_MODEL,
            )
        )
    else:
        row_id = (
            await conn.execute(
                select(entities.c.id).where(func.lower(entities.c.name) == normalized.lower())
            )
        ).scalar_one()
    return row_id


def extract_entity_names(
    applicants_str: str | None,
    *,
    record_id: int | None = None,
    role: str = "applicant",
) -> list[str]:
    """Return the cleaned, de-duplicated entity names in a semicolon-delimited applicants string.

    The first element (the business name) is skipped, as are
    ``ADDITIONAL_NAMES_MARKERS`` and non-name strings (``is_noise_name``).
    *record_id* / *role* only label the debug log lines.
    """
    if not applicants_str or ";" not in applicants_str:
        return []

    names: list[str] = []
    seen: set[str] = set()
    for raw in applicants_str.split(";")[1:]:
        clean = clean_entity_name(strip_duplicate_marker(raw.strip()))
        if not clean or clean in seen:
            continue
        seen.add(clean)
        if clean in ADDITIONAL_NAMES_MARKERS or is_noise_name(clean):
            logger.debug("Skipping non-name %r in record %s (role %s)", clean, record_id, role)
            continue
        names.append(clean)
    return names


async def parse_and_link_entities(
    conn: AsyncConnection,
    record_id: int,
    applicants_str: str,
    role: str = "applicant",
    *,
    delete_existing: bool = False,
) -> int:
    """Split applicants string, skip first element (business name), create entities and link.

    Assigns contiguous 0-based positions to successfully linked entities.
    Returns the number of entities linked.

    Parameters
    ----------
    delete_existing:
        If True, delete existing ``record_entities`` rows for this
        ``(record_id, role)`` pair before inserting (idempotent mode).
    """
    if delete_existing:
        await conn.execute(
            delete(record_entities).where(
                (record_entities.c.record_id == record_id) & (record_entities.c.role == role)
            )
        )

    linked = 0
    for name in extract_entity_names(applicants_str, record_id=record_id, role=role):
        entity_id = await get_or_create_entity(conn, name)
        stmt = (
            pg_insert(record_entities)
            .values(record_id=record_id, entity_id=entity_id, role=role, position=linked)
            .on_conflict_do_nothing()
        )
        result = await conn.execute(stmt)
        linked += result.rowcount

    return linked


async def get_record_entities(
    conn: AsyncConnection, record_ids: list[int]
) -> dict[int, dict[str, list[dict]]]:
    """Batch-fetch entities for a list of record ids.

    Returns ``{record_id: {"applicant": [...], "previous_applicant": [...]}}``.
    Each entity dict has keys ``id``, ``name``, ``entity_type``,
    ``entity_subtype``, ``classification_source``.
    """
    if not record_ids:
        return {}

    result: dict[int, dict[str, list[dict]]] = {
        rid: {"applicant": [], "previous_applicant": []} for rid in record_ids
    }

    chunk_size = 500
    for i in range(0, len(record_ids), chunk_size):
        batch = record_ids[i : i + chunk_size]
        stmt = (
            select(
                record_entities.c.record_id,
                record_entities.c.role,
                record_entities.c.position,
                entities.c.id.label("entity_id"),
                entities.c.name,
                entities.c.entity_type,
                entities.c.entity_subtype,
                entities.c.classification_source,
            )
            .select_from(record_entities)
            .join(entities, entities.c.id == record_entities.c.entity_id)
            .where(record_entities.c.record_id.in_(batch))
            .order_by(
                record_entities.c.record_id,
                record_entities.c.role,
                record_entities.c.position,
            )
        )
        rows = (await conn.execute(stmt)).mappings().all()
        for r in rows:
            rid = r["record_id"]
            role = r["role"]
            if rid not in result:
                result[rid] = {"applicant": [], "previous_applicant": []}
            if role not in result[rid]:
                result[rid][role] = []
            result[rid][role].append(
                {
                    "id": r["entity_id"],
                    "name": r["name"],
                    "entity_type": r["entity_type"],
                    "entity_subtype": r["entity_subtype"],
                    "classification_source": r["classification_source"],
                }
            )

    return result


async def merge_duplicate_entities(conn: AsyncConnection) -> int:
    """Find and merge entities whose names differ only by casing (case-insensitive dupes).

    For each group of entities sharing the same ``lower(name)``, keeps the
    one with the lowest id as canonical and re-links all ``record_entities``
    rows from the others to it, then deletes the duplicates.  A duplicate's
    manual classification rows and review flags move to the canonical entity,
    which is then re-resolved; its regex/Jev rows are deleted with it (#206).

    Returns the number of entities merged (deleted).
    """
    # Find case-insensitive duplicates: groups with more than one member
    subq = (
        select(
            func.lower(entities.c.name).label("lower_name"),
            func.min(entities.c.id).label("canonical_id"),
            func.count(entities.c.id).label("cnt"),
        )
        .group_by(func.lower(entities.c.name))
        .having(func.count(entities.c.id) > 1)
        .subquery()
    )

    dup_rows = (
        await conn.execute(
            select(entities.c.id, entities.c.name, subq.c.canonical_id)
            .join(subq, func.lower(entities.c.name) == subq.c.lower_name)
            .where(entities.c.id != subq.c.canonical_id)
        )
    ).fetchall()

    merged = 0
    for dirty_id, dirty_name, canon_id in dup_rows:
        # Fetch links for the duplicate entity
        dirty_links = (
            await conn.execute(
                select(
                    record_entities.c.record_id,
                    record_entities.c.role,
                    record_entities.c.position,
                ).where(record_entities.c.entity_id == dirty_id)
            )
        ).fetchall()

        for link in dirty_links:
            rid, role, pos = link[0], link[1], link[2]
            # Check if canonical already has a link for this (record, role)
            existing = (
                await conn.execute(
                    select(record_entities.c.position).where(
                        (record_entities.c.record_id == rid)
                        & (record_entities.c.entity_id == canon_id)
                        & (record_entities.c.role == role)
                    )
                )
            ).scalar_one_or_none()

            if existing is not None:
                # Conflict — keep lower position on canonical
                if pos < existing:
                    await conn.execute(
                        update(record_entities)
                        .where(
                            (record_entities.c.record_id == rid)
                            & (record_entities.c.entity_id == canon_id)
                            & (record_entities.c.role == role)
                        )
                        .values(position=pos)
                    )
            else:
                await conn.execute(
                    update(record_entities)
                    .where(
                        (record_entities.c.record_id == rid)
                        & (record_entities.c.entity_id == dirty_id)
                        & (record_entities.c.role == role)
                    )
                    .values(entity_id=canon_id)
                )

        # Operator input survives the merge (#206); regex/Jev rows cascade away.
        await conn.execute(
            update(entity_classifications)
            .where(
                (entity_classifications.c.entity_id == dirty_id)
                & (entity_classifications.c.source == "manual")
            )
            .values(entity_id=canon_id)
        )
        await conn.execute(
            update(entity_review_flags)
            .where(entity_review_flags.c.entity_id == dirty_id)
            .values(entity_id=canon_id)
        )
        # Delete remaining dirty links (conflict cases) and the entity itself
        await conn.execute(delete(record_entities).where(record_entities.c.entity_id == dirty_id))
        await conn.execute(delete(entities).where(entities.c.id == dirty_id))
        logger.info("Merged entity %d %r → %d", dirty_id, dirty_name, canon_id)
        merged += 1

    if merged:
        await resolve_entities(conn, sorted({canon for _, _, canon in dup_rows}))
        logger.info("Merged %d duplicate entities", merged)

    return merged


async def reprocess_entities(
    conn: AsyncConnection,
    *,
    record_id: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Regenerate ``record_entities`` for all or a subset of records.

    Treats ``record_entities`` as derived data: existing rows for each
    targeted ``(record_id, role)`` pair are deleted and rebuilt from
    ``license_records.applicants`` / ``previous_applicants`` using the
    current entity-normalization logic.  The ``record_enrichments``
    version stamp is updated to ``_ENTITY_REPROCESS_VERSION`` for every
    processed record.

    Parameters
    ----------
    conn:
        Open database connection. Caller is responsible for committing.
    record_id:
        If given, only reprocess this single record.
    dry_run:
        If True, compute what would be done and return counts without changes.

    Returns:
    -------
    dict
        ``{"records_processed": int, "entities_linked": int}``
    """
    if record_id is not None:
        stmt = select(
            license_records.c.id,
            license_records.c.applicants,
            license_records.c.previous_applicants,
        ).where(license_records.c.id == record_id)
    else:
        stmt = select(
            license_records.c.id,
            license_records.c.applicants,
            license_records.c.previous_applicants,
        )

    rows = (await conn.execute(stmt)).fetchall()

    records_processed = 0
    entities_linked = 0
    now = datetime.now(UTC)

    for row in rows:
        rid = row[0]
        applicants = row[1] or ""
        previous_applicants = row[2] or ""

        if dry_run:
            records_processed += 1
            continue

        linked = await parse_and_link_entities(
            conn, rid, applicants, "applicant", delete_existing=True
        )
        linked += await parse_and_link_entities(
            conn, rid, previous_applicants, "previous_applicant", delete_existing=True
        )
        entities_linked += linked
        records_processed += 1

        enrich_stmt = (
            pg_insert(record_enrichments)
            .values(
                record_id=rid,
                step="entities",
                completed_at=now,
                version=str(_ENTITY_REPROCESS_VERSION),
            )
            .on_conflict_do_update(
                index_elements=["record_id", "step"],
                set_={
                    "completed_at": now,
                    "version": str(_ENTITY_REPROCESS_VERSION),
                },
            )
        )
        await conn.execute(enrich_stmt)

    if dry_run:
        logger.info(
            "reprocess_entities (dry-run): would process %d record(s).",
            records_processed,
        )
    else:
        logger.info(
            "reprocess_entities: processed %d record(s), linked %d entity link(s).",
            records_processed,
            entities_linked,
        )

    return {"records_processed": records_processed, "entities_linked": entities_linked}


async def reclassify_entities(
    conn: AsyncConnection,
    *,
    dry_run: bool = False,
    sample_limit: int = 25,
) -> dict:
    """Re-apply the regex classifier and noise filter to existing ``entities`` rows.

    ``get_or_create_entity()`` never updates an existing row, so a classifier
    change reaches stored entities only through this pass (#203).  It never
    writes ``entities.entity_type`` directly (#206):

    1. purge set = noise-named entities (``is_noise_name``) without a manual
       decision or open flag, plus operator-confirmed non-names
       (``confirmed_noise_ids``);
    2. for every other entity, appends a regex ``entity_classifications`` row
       when its current one is missing or differs in type, subtype or model;
    3. deletes the purge set with its ``record_entities`` links, renumbering
       the remaining positions of each affected ``(record_id, role)``;
    4. re-resolves every entity (manual > Jev >= threshold > regex).

    ``dry_run`` runs the same steps inside a savepoint and rolls it back.
    Idempotent: a second run reports zero changes.  Caller commits.

    Returns:
    -------
    dict
        ``entities_examined``, ``reclassified`` (resolved ``entity_type``
        changes), ``transitions`` (``"old->new"`` counts),
        ``reclassified_sample``, ``subtype_changes``, ``regex_rows_written``,
        ``noise_entities`` (entities removed), ``noise_links``,
        ``noise_sample``, ``protected_noise_skipped``, ``confirmed_noise``.
    """
    if not dry_run:
        return await _reclassify(conn, sample_limit=sample_limit, dry_run=False)
    savepoint = await conn.begin_nested()
    try:
        return await _reclassify(conn, sample_limit=sample_limit, dry_run=True)
    finally:
        await savepoint.rollback()


async def _reclassify(conn: AsyncConnection, *, sample_limit: int, dry_run: bool) -> dict:
    rows = (
        await conn.execute(select(entities.c.id, entities.c.name).order_by(entities.c.id))
    ).fetchall()
    names = dict(rows)

    noise = {eid for eid, name in rows if is_noise_name(name)}
    protected = await protected_ids(conn)
    confirmed = await confirmed_noise_ids(conn)
    purge = sorted((noise - protected) | confirmed)
    purge_set = set(purge)

    regex_rows = _plan_regex_rows(
        [(eid, name) for eid, name in rows if eid not in purge_set], await current_answers(conn)
    )
    for i in range(0, len(regex_rows), 1000):
        await conn.execute(insert(entity_classifications), regex_rows[i : i + 1000])

    noise_links = 0
    if purge:
        noise_links = (
            await conn.execute(select(func.count()).where(record_entities.c.entity_id.in_(purge)))
        ).scalar_one()
        await _purge_entities(conn, purge)

    resolved = await resolve_entities(conn)
    type_changes = [c for c in resolved.changes if c.old[0] != c.new[0]]
    transitions: dict[str, int] = {}
    for c in type_changes:
        key = f"{c.old[0] or '(blank)'}->{c.new[0]}"
        transitions[key] = transitions.get(key, 0) + 1
    sample = [
        {
            "id": c.entity_id,
            "name": names[c.entity_id],
            "old": c.old[0],
            "new": c.new[0],
            "category": entity_category(names[c.entity_id]),
        }
        for c in type_changes[:sample_limit]
    ]
    subtype_changes = sum(1 for c in resolved.changes if c.old[1] != c.new[1])
    protected_noise = len(noise & protected)

    tag = " (dry-run)" if dry_run else ""
    if not dry_run:
        for c in type_changes:
            logger.info(
                "Reclassified entity %d %r: %s -> %s (%s)",
                c.entity_id,
                names[c.entity_id],
                c.old,
                c.new,
                entity_category(names[c.entity_id]),
            )
        if purge:
            logger.info(
                "Removed %d noise entit(ies) and %d record link(s)", len(purge), noise_links
            )
    logger.info(
        "reclassify_entities%s: examined %d, %d regex row(s), reclassified %d %s, "
        "%d subtype change(s), noise %d (%d link(s)), %d protected, %d confirmed",
        tag,
        len(rows),
        len(regex_rows),
        len(type_changes),
        transitions,
        subtype_changes,
        len(purge),
        noise_links,
        protected_noise,
        len(confirmed),
    )
    return {
        "entities_examined": len(rows),
        "reclassified": len(type_changes),
        "transitions": transitions,
        "reclassified_sample": sample,
        "subtype_changes": subtype_changes,
        "regex_rows_written": len(regex_rows),
        "noise_entities": len(purge),
        "noise_links": noise_links,
        "noise_sample": [{"id": eid, "name": names[eid]} for eid in purge[:sample_limit]],
        "protected_noise_skipped": protected_noise,
        "confirmed_noise": len(confirmed),
    }


def _plan_regex_rows(
    rows: list[tuple[int, str]], answers: dict[int, dict[str, SourceAnswer]]
) -> list[dict]:
    """Return regex rows to insert for ``(id, name)`` pairs whose current regex answer is stale."""
    out: list[dict] = []
    for eid, name in rows:
        entity_type, entity_subtype = regex_classification(name)
        current = answers.get(eid, {}).get("regex")
        if current is None or (current.entity_type, current.entity_subtype, current.model) != (
            entity_type,
            entity_subtype,
            REGEX_MODEL,
        ):
            out.append(
                {
                    "entity_id": eid,
                    "source": "regex",
                    "entity_type": entity_type,
                    "entity_subtype": entity_subtype,
                    "model": REGEX_MODEL,
                }
            )
    return out


async def _purge_entities(conn: AsyncConnection, entity_ids: list[int]) -> None:
    """Delete *entity_ids* and their links, then re-compact link positions."""
    affected = (
        await conn.execute(
            select(record_entities.c.record_id, record_entities.c.role)
            .where(record_entities.c.entity_id.in_(entity_ids))
            .distinct()
        )
    ).fetchall()
    await conn.execute(delete(record_entities).where(record_entities.c.entity_id.in_(entity_ids)))
    await conn.execute(delete(entities).where(entities.c.id.in_(entity_ids)))
    for record_id, role in affected:
        await conn.execute(
            text("""
                UPDATE record_entities re SET position = r.rn - 1
                FROM (
                    SELECT entity_id,
                           ROW_NUMBER() OVER (ORDER BY position, entity_id) AS rn
                    FROM record_entities
                    WHERE record_id = :rid AND role = :role
                ) r
                WHERE re.record_id = :rid AND re.role = :role
                  AND re.entity_id = r.entity_id AND re.position <> r.rn - 1
            """),
            {"rid": record_id, "role": role},
        )


async def get_entity_by_id(
    conn: AsyncConnection,
    entity_id: int,
) -> dict | None:
    """Fetch a single entity by id, with its resolved classification and current Jev answer.

    Keys: ``id``, ``name``, ``entity_type``, ``entity_subtype``,
    ``classification_source``, ``created_at``, ``jev_confidence``, ``jev_subtype``.
    """
    row = (
        (
            await conn.execute(
                select(
                    entities.c.id,
                    entities.c.name,
                    entities.c.entity_type,
                    entities.c.entity_subtype,
                    entities.c.classification_source,
                    entities.c.created_at,
                ).where(entities.c.id == entity_id)
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return (await with_jev_answer(conn, [dict(row)]))[0]


async def backfill_entities(conn: AsyncConnection) -> int:
    """Backfill record_entities for records missing entity rows but having semicolons in applicants.

    Async port of entities.backfill_entities. Returns number of records processed.
    Caller must commit.
    """
    stmt = (
        select(
            license_records.c.id,
            license_records.c.applicants,
            license_records.c.previous_applicants,
        )
        .outerjoin(
            record_entities,
            record_entities.c.record_id == license_records.c.id,
        )
        .where(record_entities.c.record_id.is_(None))
        .where(
            (license_records.c.applicants.contains(";"))
            | (license_records.c.previous_applicants.contains(";"))
        )
    )

    rows = (await conn.execute(stmt)).fetchall()
    processed = 0
    for row in rows:
        r_id, r_applicants, r_prev = row[0], row[1] or "", row[2] or ""
        await parse_and_link_entities(conn, r_id, r_applicants, "applicant")
        if r_prev:
            await parse_and_link_entities(conn, r_id, r_prev, "previous_applicant")
        processed += 1

    if processed:
        await merge_duplicate_entities(conn)

    logger.info("backfill_entities: processed %d record(s)", processed)
    return processed
