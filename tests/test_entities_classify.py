"""Tests for the entity classifier and noise filter in entities.py (#203).

Pure-function tests — no database. Cases come from the hand-labeled gold set
(``docs/research/person-vs-org/gold.csv``) and the production noise census.
"""

import pytest

from wslcb_licensing_tracker.entities import (
    _classify_entity_type,
    entity_category,
    extract_entity_names,
    is_noise_name,
)

# ── defects named in #203 ────────────────────────────────────────────

ORGS = [
    # CO. / bare CO never matched (\b after '.')
    "ALASKAN BREWING & BOTTLING CO.",
    "KINGS & CONVICTS HOLDING CO.",
    "SEABROOK MANAGEMENT CO",
    "HUANG CONSULTING CO.",
    "OLD HIGHWAY 99 CO",
    # LLP absent
    "GLEASON & KOATZ LLP",
    "RED RIVER CATTLE LLP",
    # 30-char truncation
    "FAVIA ERICKSON WINEGROWERS, LL",
    "TABLE & CO HOSPITALITY CORPORA",
    "AUSTIN NICHOLS & CO., INCORPOR",
    "JOHNSON BROTHERS LIQUOR COMPAN",
    "CATOCTIN CREEK MANAGEMENT COMP",
    "AIRPORT MANAGEMENT SERVICES, L",
    "LEVY PREMIUM FOODSERVICE LIMIT",
    "........COMMERCIAL KITCHEN, & GALLERY L.L",
    # suffix-less orgs
    "FORGE CELLARS",
    "COLUMBIA GORGE WINERY",
    "IN THE SHADOW BREWING",
    "GARRISON BROTHERS DISTILLERY",
    "FIDALGO YACHT CLUB",
    "AMERICAN LEGION POST 150",
    "MOSES LAKE ELKS LODGE 2892",
    "WOODLAND LODGE NO. 2394, LOYAL",
    "EPIC LIFE CHURCH",
    "SEATTLE ASIAN ART MUSEUM (WAC 314-07-035)",
    "HAAN TANDOOR & BAR",
    "HERBS BAR AND GRILL",
    "STONE CREEK WOOD FIRED PIZZA",
    # government / tribe
    "LOWER ELWHA KLALLAM TRIBE",
    "SPOKANE TRIBE OF INDIANS",
    "CITY OF LONGVIEW",
    "SEATTLE CITY LIGHT",
    "UNIVERSITY OF WA BOARD OF REGE",
    # still-matched originals
    "ACME HOLDINGS, LLC",
    "7-ELEVEN, INC.",
    "SMITH FAMILY TRUST",
]

TRUSTS = [
    "DAVID C DRUMMOND REVOCABLE TRU",
    "ISABELLE MORGAN CLENDENEN TRST",
    "SALLY A CLARK, TTEE OF THE CLA",
    "MARTIN F BLAKE 2020 IRREVOCABL",
    "MILLIE B JONES IRREVOCABLE",
    "THE BOWER 2010 IRREVOKABLE ASS",
    "THE MESTRE-CLAVIER LIVING TRUS",
    "STEVEN R PRIDE TRUSTEE",
    "ADAM TRUSTEE TAPLIN",
    "THE VASSILIADIS SURVIVOR'S TRU",
]

ESTATES = [
    "DARRYL R ESTATE WILCOX",
    "KAREN RENEE HERZOG - ESTATE",
    "ARLIS M AKINS-ESTATE",
    "GLENN ESTATE OF BIDDLE",
    "SHAW, RICHARD HENRY ESTATE",
]

# Real persons whose surnames look like org/trust words — must stay persons.
PERSONS = [
    "ROBERT J PIZZA",
    "LINDA T PIZZA",
    "AMBER HENNINGSEN POST",
    "WANPEN POST",
    "CHARLES POST",
    "TRAVIS JAMES POST",
    "HILLARY S TRUSTY",
    "TRUSTY, LELAND GLENN",
    "LELAND TRUSTY",
    "ERICKA CHURCH",
    "MATTHEW DAVID CHURCH",
    "TAYLOR, ALEXANDER CHURCH",
    "WARREN K L INN",
    "ALEXA SO INN",
    "NATION SON",
    "NA YOUNG LEE",
    "MARK CHRISTOPHER SEE",
    "KELLY JO OFFICER",
    "THE NGOC NGUYEN",
    "JOHN DOE",
    "SMITH, JOHN",
    # 30-char truncated persons
    "JUAN CRISTOBAL SEPULVEDA MATIC",
    "MARTINEZ-ZAMORA, MARIA DEL CAR",
    "BIN ABDULL RASHED, MUHAMMAD FA",
]


@pytest.mark.parametrize("name", ORGS + TRUSTS + ESTATES)
def test_non_persons_are_organizations(name):
    assert _classify_entity_type(name) == "organization"


@pytest.mark.parametrize("name", PERSONS)
def test_surname_traps_stay_persons(name):
    assert _classify_entity_type(name) == "person"
    assert entity_category(name) is None


@pytest.mark.parametrize("name", TRUSTS)
def test_trust_category(name):
    assert entity_category(name) == "trust"


@pytest.mark.parametrize("name", ESTATES)
def test_estate_category(name):
    assert entity_category(name) == "estate"


@pytest.mark.parametrize(
    ("name", "category"),
    [
        ("GLEASON & KOATZ LLP", "legal_suffix"),
        ("SEABROOK MANAGEMENT CO", "legal_suffix"),
        ("FAVIA ERICKSON WINEGROWERS, LL", "truncated_suffix"),
        ("TABLE & CO HOSPITALITY CORPORA", "legal_suffix"),
        ("LEVY PREMIUM FOODSERVICE LIMIT", "truncated_suffix"),
        ("CATOCTIN CREEK MAPLE SYRUP COMP", "truncated_suffix"),
        ("FORGE CELLARS", "org_keyword"),
        ("CITY OF LONGVIEW", "government_tribe"),
        ("LOWER ELWHA KLALLAM TRIBE", "government_tribe"),
        ("REAL ESTATE PARTNERS OF WA", "legal_suffix"),
        ("GRGICH HILLS ESTATE WINES", "org_keyword"),
    ],
)
def test_category(name, category):
    assert entity_category(name) == category


def test_truncated_tail_requires_truncation_length():
    # A short name ending in a suffix prefix is not treated as truncated.
    assert _classify_entity_type("JOHN COMP") == "person"
    assert _classify_entity_type("SMITH, L") == "person"


# ── noise filter ─────────────────────────────────────────────────────

NOISE = [
    "PER WAC 314-07-035",
    "(PER WAC 314-07-035)",
    "PER WAC 314-07-035(1)",
    "PER WAC 314-07-035 (1)",
    "PER WAC 314.07.035",
    "WAC 314-07-035",
    "ON LIC PER WAC 314-07-035",
    "314-07-035",
    "314-07-035(1)",
    "314.07.035",
    "-035",
    "035",
    "5",
    "A",
    "17 SHAREHOLDERS W/10% OR LESS",
    "4 MBRS W/LESS THAN 10% EACH",
    "TRUSTEE",
    "SOCIETY",
    "(ESTATE PROJECT)",
    "NO PEOPLE REQUIRED",
    "NO PEOPLE REQUIRED - NON PROFIT ARTS",
    "LLC",
    "PARTNERSHIP",
]

NOT_NOISE = [
    "7-ELEVEN, INC. PER WAC 314-07-035",
    "JAMES AND WENDY HULL AS PER WAC 314-55-035",
    "GRANT COUNTY LICENSED PER WAC 314-07-035",
    "BP WEST COAST PRODUCTS LLC PER WAC 314-07-035",
    "ALYSSA JEAN WACHTVEITL",
    "501 CONSULTANTS",
    "1744FOE",
    "3.14 LLC",
    "NA",
    "TY",
    "IL",
    "LI",
    "KIRANDEEP",
    "JOHN DOE",
]


@pytest.mark.parametrize("name", NOISE)
def test_noise(name):
    assert is_noise_name(name)


@pytest.mark.parametrize("name", NOT_NOISE)
def test_not_noise(name):
    assert not is_noise_name(name)


def test_extract_entity_names_skips_business_markers_and_noise():
    applicants = (
        "PNWRM, LLC; BP WEST COAST PRODUCTS, LLC PER WAC 314.07; 035; "
        "MURGASH, MARK JOHN; ADDITIONAL NAMES ON FILE; murgash, mark john; TRUSTEE"
    )
    assert extract_entity_names(applicants) == [
        "BP WEST COAST PRODUCTS, LLC PER WAC 314.07",
        "MURGASH, MARK JOHN",
    ]


def test_extract_entity_names_without_semicolon():
    assert extract_entity_names("JUST A BUSINESS") == []
    assert extract_entity_names("") == []
