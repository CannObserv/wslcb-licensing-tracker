# ruff: noqa: INP001, T201 — research script; prints are its CLI output.
"""Person-vs-org classifier evaluation against the hand-labeled gold set (research only).

Not part of the application. Usage (from repo root):

    uv run python docs/research/person-vs-org/evaluate.py score
    uv run python docs/research/person-vs-org/evaluate.py jev  # re-query Jev; needs .env key

``jev`` rewrites the jev5 / jev5_conf / jev5_rev / jev2 columns of predictions.csv.
The regex, probablepeople, and haiku columns were produced out-of-band (see
../person-vs-org-classification.md, "Method").

Labels: P person, H household of persons, O organization, T trust, E estate,
N not-a-name (noise), A ambiguous (excluded). Binary scoring maps P/H -> person,
O/T/E -> non-person.
"""

import asyncio
import csv
import sys
from pathlib import Path

import httpx

HERE = Path(__file__).parent
GOLD = HERE / "gold.csv"
PREDS = HERE / "predictions.csv"
URL = "https://api.typesafe.ai/v1/systemone"
MODEL = "jev-1.14.0"
CTX = (
    "An applicant-list entry from a Washington State Liquor and Cannabis Board licensing record. "
    'Entries are uppercase, may be "LAST, FIRST", and may be cut off at 30 characters.'
)
FIVE = {
    "person": "One or more natural individual humans named directly "
    "(e.g. JOHN SMITH, SMITH, JOHN A, or a couple like JOHN & MARY SMITH)",
    "organization": "A business, nonprofit, club, lodge, church, tribe, school, or government body",
    "trust": "A trust, or a trustee acting for a trust "
    "(e.g. SMITH FAMILY TRUST, JOHN DOE TTEE, REVOCABLE TR)",
    "estate": "The estate of a deceased individual (e.g. JOHN ESTATE SMITH, ESTATE OF JANE DOE)",
    "not_a_name": "Administrative text, regulation citations, counts, or numbers "
    "rather than a name",
}
CODE = {"person": "P", "organization": "O", "trust": "T", "estate": "E", "not_a_name": "N"}


def _five(order: list[str]) -> dict:
    criteria = {k: FIVE[k] for k in order}
    instr = "What kind of entity does `name` refer to?"
    return {"kind": {"type": "choice", "instructions": instr, "criteria": criteria}}


CONFIGS = {
    "jev5": (True, _five(list(FIVE))),
    "jev5_rev": (True, _five(list(FIVE)[::-1])),
    "jev2": (
        False,
        {
            "kind": {
                "type": "choice",
                "instructions": "Is this a person or an organization?",
                "criteria": {"person": "A person", "organization": "An organization"},
            }
        },
    ),
}


def _key() -> str:
    for line in (HERE.parents[2] / ".env").read_text().splitlines():
        if line.startswith("TYPESAFE_API_KEY="):
            return line.split("=", 1)[1].strip().strip("\"'")
    msg = "TYPESAFE_API_KEY not in .env"
    raise SystemExit(msg)


async def _ask(client: httpx.AsyncClient, sem: asyncio.Semaphore, cfg: str, name: str) -> dict:
    with_ctx, questions = CONFIGS[cfg]
    state = {"name": name, "context": CTX} if with_ctx else name
    body = {"model": MODEL, "state": state, "questions": questions}
    for attempt in range(6):
        async with sem:
            r = await client.post(URL, json=body)
        if r.status_code in (429, 529):
            await asyncio.sleep(2**attempt)
            continue
        r.raise_for_status()
        return r.json()["answers"]["kind"]
    msg = "rate limited"
    raise RuntimeError(msg)


async def run_jev() -> None:
    """Re-query every gold name under each Jev config and rewrite those columns."""
    gold = list(csv.DictReader(GOLD.open()))
    preds = {r["entity_id"]: r for r in csv.DictReader(PREDS.open())}
    sem = asyncio.Semaphore(8)
    headers = {"Authorization": f"Bearer {_key()}"}
    async with httpx.AsyncClient(timeout=60, headers=headers) as client:
        for cfg in CONFIGS:
            answers = await asyncio.gather(*(_ask(client, sem, cfg, g["name"]) for g in gold))
            for g, a in zip(gold, answers, strict=True):
                preds[g["entity_id"]][cfg] = CODE[a["choice"]]
                if cfg == "jev5":
                    preds[g["entity_id"]]["jev5_conf"] = round(a["confidence"], 3)
    fields = list(next(iter(preds.values())).keys())
    with PREDS.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(preds.values())


def score() -> None:
    """Print the per-stratum comparison table."""
    gold = list(csv.DictReader(GOLD.open()))
    preds = {r["entity_id"]: r for r in csv.DictReader(PREDS.open())}
    cols = [c for c in next(iter(preds.values())) if c not in ("entity_id", "jev5_conf")]

    def is_person(code: str) -> bool:
        return code in ("P", "H")

    def flagged(col: str, g: dict) -> bool:  # predicted non-person, and not an error/miss
        return preds[g["entity_id"]][col] in ("O", "T", "E", "N")

    a = [g for g in gold if g["stratum"] == "A"]
    b = [g for g in gold if g["stratum"] == "B" and g["label"] not in ("N", "A")]
    c = [g for g in gold if g["stratum"] == "C"]

    def count(col: str, pool: list[dict], wanted: str | None, hit) -> str:  # noqa: ANN001
        sel = [g for g in pool if wanted is None or g["label"] in wanted]
        return f"{sum(hit(col, g) for g in sel)}/{len(sel)}"

    def wrong(col: str, g: dict) -> bool:
        return not is_person(preds[g["entity_id"]][col])

    rows = [
        ("A persons wrongly flagged", a, None, wrong),
        ("B persons wrongly flagged", b, "PH", wrong),
        ("B O caught", b, "O", flagged),
        ("B T caught", b, "T", flagged),
        ("B E caught", b, "E", flagged),
        ("C orgs caught", c, None, flagged),
    ]
    print(f"{'metric':28s}" + "".join(f"{col:>16s}" for col in cols))
    for label, pool, wanted, hit in rows:
        print(f"{label:28s}" + "".join(f"{count(col, pool, wanted, hit):>16s}" for col in cols))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "score"
    if cmd == "jev":
        asyncio.run(run_jev())
    score()
