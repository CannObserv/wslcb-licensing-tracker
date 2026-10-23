# ruff: noqa: INP001, T201, EM102, TRY003 — research script; prints and exits are its CLI output.
r"""Production-path Jev evaluation of the org-subtype question on the gold set (#210; research).

Not part of the application. Usage (from repo root):

    uv run python docs/research/person-vs-org/subtype_eval.py run      # live; needs a key
    uv run python docs/research/person-vs-org/subtype_eval.py score
    uv run python docs/research/person-vs-org/subtype_eval.py supplement-sql --n 60 > supp.sql
    psql "$PROD_URL" -X -q -v ON_ERROR_STOP=1 -f supp.sql \\
        > docs/research/person-vs-org/subtype_supplement.csv      # operator
    uv run python docs/research/person-vs-org/subtype_eval.py supplement  # live; needs a key

``run`` sends the 834 non-ambiguous gold names through the shipped
``jev_client.classify_batch`` (25 per request, a kind and an org question per
name), in ``entity_id`` order as production batches them, and rewrites the
``<tag>_kind`` / ``_kind_conf`` / ``_org`` / ``_org_conf`` columns of
predictions.csv (``--tag``, default ``prod``; subtype_wordings.csv pins each tag
to the ORG_CRITERIA it was sent with).  The key is ``CO_WSLCB_LICENSING_TRACKER_TYPESAFE_API_KEY``,
else ``TYPESAFE_API_KEY`` in the repo ``.env`` (``evaluate._key()``).  The
first batch is sent alone; the run stops if it, plus the spend already in
subtype_spend.csv (every live send is appended there), passes
``SPEND_CAP_USD``.

``supplement-sql`` prints a read-only SQL script (``COPY … TO STDOUT``) that samples regex
``tribe`` / ``government`` entities and a nonprofit keyword pool from
production by ``md5(name)``, gold ids excluded (each pool: the regex's subtype,
or a keyword the regex may miss), into subtype_supplement.csv.
The operator runs it.  The rows are hand-labeled (``label``, ``subtype``,
``subtype_sure``) and committed; only then does ``supplement`` send them, and
it writes the answers to a separate subtype_supplement_preds.csv.  Jev never
saw these names while the wording was drafted, so they are the held-out check:
a subtype with >= ``HELD_OUT_MIN`` held-out answers below the bar fails.

``run`` and ``supplement`` refuse to send while their label file has
uncommitted changes: labels are fixed before Jev is seen.

A Jev org subtype reaches ``entities.entity_subtype`` only when the kind
answer is ``organization`` at or above the threshold (the resolver's gate) and
the org answer is too (``map_answer``).  ``score`` counts exactly those.
"""

import argparse
import asyncio
import csv
import hashlib
import json
import math
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import evaluate as ev  # sibling module; run from repo root or this directory
import httpx

from wslcb_licensing_tracker import jev_client
from wslcb_licensing_tracker.config import JEV_USD_PER_MILLION_INPUT_TOKENS
from wslcb_licensing_tracker.entities import regex_classification
from wslcb_licensing_tracker.entity_classification import JEV_ORG_SUBTYPES
from wslcb_licensing_tracker.jev_classifier import map_answer

HERE = Path(__file__).parent
SUPP = HERE / "subtype_supplement.csv"
SUPP_PREDS = HERE / "subtype_supplement_preds.csv"
# Every live send, appended (and committed): SPEND_CAP_USD is checked against the
# total, so run + supplement + any re-run share one approval.
SPEND = HERE / "subtype_spend.csv"
SPEND_FIELDS = ["at", "cmd", "requests", "tokens", "usd"]
# Each ORG_CRITERIA wording writes its own columns, ``<tag>_kind`` etc.; this file
# pins every tag to the wording it was sent with (#210: ``prod`` = as shipped
# by #207, ``prod2`` = the reworded criteria).
WORDINGS = HERE / "subtype_wordings.csv"
HELD_OUT_MIN = 10
THRESHOLD = 0.7
BAR = 0.90
MIN_EVIDENCE = 20
SPEND_CAP_USD = 0.10
# Nonprofit-looking names (ORG_CRITERIA's nonprofit list).  Broad on purpose:
# for-profit lookalikes (IPIC THEATERS, FLIGHT CLUB LLC) test precision too.
# PARISH and a bare POST are surnames (#203).
NONPROFIT_PATTERN = (
    r"\m(LODGE|AERIE|ELKS|MOOSE|EAGLES|LEGION|VFW|POST (NO\.? ?|# ?)?[0-9]+|CHURCH|"
    r"MUSEUM|THEATRE|THEATER|FOUNDATION|ASSOCIATION|ASSN|SOCIETY|CLUB|GRANGE|ORDER OF|"
    r"FRATERNAL|VETERANS|CHARIT[A-Z]*|SYMPHONY|OPERA|BALLET|ROTARY|KIWANIS|YMCA|YWCA|"
    r"FESTIVAL|GUILD|CHAMBER OF COMMERCE|FRIENDS OF)\M"
)
# Tribe / government names the regex may miss (legal suffixes win over its
# tribe and government checks).  Lookalikes (COUNTY LINE TAVERN) are welcome.
TRIBE_PATTERN = r"\m(TRIBES?|TRIBAL|NATION|BAND OF|INDIANS?|RESERVATION|RANCHERIA|PUEBLO)\M"
GOVERNMENT_PATTERN = (
    r"\m(CITY OF|TOWN OF|COUNTY|PORT OF|PORT DISTRICT|SCHOOL DISTRICT|PUBLIC SCHOOLS?|"
    r"COLLEGE|UNIVERSITY|PUD|PUBLIC UTILITY|PUBLIC DEVELOPMENT AUTHORITY|DEPARTMENT OF|"
    r"STATE OF|PARKS? (AND|&) RECREATION|AIRPORT|DISTRICT)\M"
)


def _rows(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def _write(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def _require_committed(path: Path, column: str) -> None:
    """Exit unless *path* has a *column* and no uncommitted changes (label-blind)."""
    if column not in next(iter(_rows(path)), {}):
        raise SystemExit(f"{path.name} has no {column!r} column; label it first")
    dirty = subprocess.run(  # noqa: S603 — fixed argv
        ["git", "status", "--porcelain", "--", str(path)],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if dirty:
        raise SystemExit(f"{path.name} has uncommitted changes; commit the labels first")


def _usd(tokens: int) -> float:
    return tokens * JEV_USD_PER_MILLION_INPUT_TOKENS / 1e6


def _spent_usd() -> float:
    """Total recorded live spend so far."""
    return sum(float(r["usd"]) for r in _rows(SPEND)) if SPEND.exists() else 0.0


def _record_spend(cmd: str, requests: int, tokens: int) -> None:
    new = not SPEND.exists()
    with SPEND.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SPEND_FIELDS)
        if new:
            w.writeheader()
        at = datetime.now(UTC).isoformat(timespec="seconds")
        w.writerow(
            {
                "at": at,
                "cmd": cmd,
                "requests": requests,
                "tokens": tokens,
                "usd": f"{_usd(tokens):.6f}",
            }
        )


def _key() -> str:
    return jev_client.get_api_key() or ev._key()  # noqa: SLF001


def _cols(tag: str) -> tuple[str, str, str, str]:
    return f"{tag}_kind", f"{tag}_kind_conf", f"{tag}_org", f"{tag}_org_conf"


def _wording_sha() -> str:
    return hashlib.sha256(json.dumps(jev_client.ORG_CRITERIA, sort_keys=True).encode()).hexdigest()


def _pin_wording(tag: str) -> None:
    """Exit if *tag* was sent with a different ORG_CRITERIA; record it if new."""
    sha = _wording_sha()
    pinned = {r["tag"]: r["sha256"] for r in _rows(WORDINGS)} if WORDINGS.exists() else {}
    if tag in pinned and pinned[tag] != sha:
        raise SystemExit(f"tag {tag!r} was sent with another ORG_CRITERIA; use a new --tag")
    if tag not in pinned:
        new = not WORDINGS.exists()
        with WORDINGS.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["tag", "sha256", "org_criteria"])
            if new:
                w.writeheader()
            w.writerow(
                {"tag": tag, "sha256": sha, "org_criteria": json.dumps(jev_client.ORG_CRITERIA)}
            )


async def _classify(cmd: str, names: list[str]) -> tuple[list[jev_client.JevAnswer], dict]:
    """Send *names* 25 per request; return the answers in order and the token stats.

    Refuses when the recorded spend plus what the first batch implies would pass
    ``SPEND_CAP_USD``; every billed batch is recorded in ``SPEND``.
    """
    if not names:
        raise SystemExit(f"{cmd}: no names to send")
    prior = _spent_usd()
    if prior >= SPEND_CAP_USD:
        raise SystemExit(f"${prior:.4f} already spent; the cap is ${SPEND_CAP_USD:.2f}")
    size = jev_client.BATCH_SIZE
    chunks = [names[i : i + size] for i in range(0, len(names), size)]
    key, model = _key(), jev_client.get_model()
    sem = asyncio.Semaphore(jev_client.CONCURRENCY)

    async def one(client: httpx.AsyncClient, chunk: list[str]) -> jev_client.JevBatch:
        async with sem:
            return await jev_client.classify_batch(chunk, api_key=key, model=model, client=client)

    async with httpx.AsyncClient() as client:
        try:
            first = await one(client, chunks[0])
            implied_usd = _usd(round(first.input_tokens / len(chunks[0]) * len(names)))
            print(
                f"first batch: {first.input_tokens} tokens; implies ${implied_usd:.4f} for the set"
                f" (${prior:.4f} spent before)"
            )
            if prior + implied_usd > SPEND_CAP_USD:
                _record_spend(cmd, 1, first.input_tokens)
                raise SystemExit(f"over the ${SPEND_CAP_USD:.2f} cap; nothing written")
            rest = await asyncio.gather(
                *(one(client, ch) for ch in chunks[1:]), return_exceptions=True
            )
        except jev_client.JevError as e:
            raise SystemExit(f"Jev failed (status {e.status}); nothing written: {e}") from None
    batches = [first, *rest]
    billed = [b for b in batches if isinstance(b, jev_client.JevBatch)]
    _record_spend(cmd, len(billed), sum(b.input_tokens for b in billed))
    failed = [r for r in batches if isinstance(r, BaseException)]
    if failed:
        # The batches that did answer were billed: report them before giving up.
        spent = sum(b.input_tokens for b in billed)
        print(
            f"{len(failed)}/{len(batches)} batches failed;"
            f" {spent} tokens (${_usd(spent):.5f}) spent"
        )
        e = failed[0]
        status = getattr(e, "status", None)
        raise SystemExit(f"Jev failed (status {status}); nothing written: {e}")
    full = [(b, ch) for b, ch in zip(batches, chunks, strict=True) if len(ch) == size]
    stats = {
        "model": sorted({b.model for b in batches}),
        "requests": len(batches),
        "names": len(names),
        "tokens": sum(b.input_tokens for b in batches),
        "full_tokens": sum(b.input_tokens for b, _ in full),
        "full_names": sum(len(ch) for _, ch in full),
    }
    return [a for b in batches for a in b.answers], stats


def _print_stats(stats: dict) -> None:
    usd = _usd(stats["tokens"])
    per_name = stats["tokens"] / stats["names"]
    per_full = stats["full_tokens"] / stats["full_names"] if stats["full_names"] else 0
    print(f"recorded spend so far: ${_spent_usd():.5f} (cap ${SPEND_CAP_USD:.2f})")
    print(
        f"model={','.join(stats['model'])} requests={stats['requests']} names={stats['names']} "
        f"tokens={stats['tokens']} spend=${usd:.5f} tok/name={per_name:.1f} "
        f"tok/name(full batches)={per_full:.1f}"
    )


def _prod_cols(a: jev_client.JevAnswer, tag: str) -> dict:
    kind, kind_conf, org, org_conf = _cols(tag)
    return {
        kind: ev.CODE[a.kind],
        kind_conf: round(a.kind_confidence, 3),
        org: a.org,
        org_conf: round(a.org_confidence, 3),
    }


async def run(tag: str) -> None:
    """Classify the non-ambiguous gold names and rewrite the *tag* columns."""
    _require_committed(ev.GOLD, "subtype")
    _pin_wording(tag)
    gold = sorted(
        (g for g in _rows(ev.GOLD) if g["label"] != "A"), key=lambda g: int(g["entity_id"])
    )
    answers, stats = await _classify(f"run:{tag}", [g["name"] for g in gold])
    cols = _cols(tag)
    preds = {r["entity_id"]: r for r in _rows(ev.PREDS)}
    for r in preds.values():
        r.update(dict.fromkeys(cols, ""))
    for g, a in zip(gold, answers, strict=True):
        preds[g["entity_id"]].update(_prod_cols(a, tag))
    fields = [c for c in next(iter(preds.values())) if c not in cols] + list(cols)
    _write(ev.PREDS, list(preds.values()), fields)
    _print_stats(stats)


async def supplement(tag: str) -> None:
    """Classify the labeled supplement rows into subtype_supplement_preds.csv's *tag* columns."""
    _require_committed(SUPP, "subtype")
    _pin_wording(tag)
    rows = [r for r in _rows(SUPP) if r["label"] != "A"]
    if any(not r["label"] for r in rows):
        raise SystemExit(f"{SUPP.name} has unlabeled rows")
    _require_subtypes(rows, SUPP.name)
    answers, stats = await _classify(f"supplement:{tag}", [r["name"] for r in rows])
    cols = _cols(tag)
    old = {r["entity_id"]: r for r in _rows(SUPP_PREDS)} if SUPP_PREDS.exists() else {}
    out = []
    for r, a in zip(rows, answers, strict=True):
        kept = {k: v for k, v in old.get(r["entity_id"], {}).items() if k not in cols}
        out.append({**kept, "entity_id": r["entity_id"], **_prod_cols(a, tag)})
    fields = list(dict.fromkeys(k for row in out for k in row))
    _write(SUPP_PREDS, out, fields)
    _print_stats(stats)


def supplement_sql(n: int) -> str:
    """Return the read-only psql script that writes subtype_supplement.csv."""
    ids = ",".join(g["entity_id"] for g in _rows(ev.GOLD))
    pools = {
        "tribe": f"(entity_subtype = 'tribe' OR name ~ '{TRIBE_PATTERN}')",
        "government": f"(entity_subtype = 'government' OR name ~ '{GOVERNMENT_PATTERN}')",
        "nonprofit": f"name ~ '{NONPROFIT_PATTERN}'",
    }
    parts = [
        f"SELECT '{pool}' AS pool, id AS entity_id, name, "
        "row_number() OVER (ORDER BY md5(name)) AS rn "
        f"FROM entities WHERE {where} AND id NOT IN ({ids})"
        for pool, where in pools.items()
    ]
    query = (
        "SELECT DISTINCT ON (entity_id) pool, entity_id, name FROM ("
        + " UNION ALL ".join(parts)
        + f") s WHERE rn <= {n} ORDER BY entity_id, pool"
    )
    return f"SET default_transaction_read_only = on;\nCOPY ({query}) TO STDOUT WITH CSV HEADER;\n"


def _assigned(p: dict, t: float, tag: str) -> str | None:
    """The org subtype production would store and resolve for this answer, else None."""
    kind, kind_conf, org, org_conf = _cols(tag)
    if p.get(kind) != "O" or float(p[kind_conf]) < t:
        return None
    answer = jev_client.JevAnswer(
        "organization", float(p[kind_conf]), p[org], float(p[org_conf]), {}
    )
    _, sub = map_answer(answer, threshold=t)
    return sub if sub in JEV_ORG_SUBTYPES else None


def _gold_subtype(g: dict) -> str | None:
    return g["subtype"] if g["label"] == "O" else None


def _require_subtypes(rows: list[dict], source: str) -> None:
    """Exit unless every ``O`` row carries an org subtype: a blank one would score as an error."""
    blank = [r["name"] for r in rows if r["label"] == "O" and r["subtype"] not in JEV_ORG_SUBTYPES]
    if blank:
        raise SystemExit(f"{source}: {len(blank)} O rows without an org subtype, e.g. {blank[0]!r}")


def _wilson_low(ok: int, n: int, z: float = 1.96) -> float:
    """95% Wilson lower bound on a proportion: how far drift could pull a pass down."""
    if not n:
        return 0.0
    p = ok / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - margin) / (1 + z * z / n)


def _verdict(n: int, precision: float, held_n: int, held_ok: int) -> str:
    """Gold + supplement decide; the held-out supplement alone can veto (operator, #210)."""
    if held_n >= HELD_OUT_MIN and held_ok / held_n < BAR:
        return "fail(held)"
    if n < MIN_EVIDENCE:
        return "insufficient"
    return "pass" if precision >= BAR else "fail"


def score(tag: str) -> None:
    """Print the kind regression gate and the per-subtype precision table for *tag*."""
    kind, kind_conf, _, org_conf = _cols(tag)
    gold = [g for g in _rows(ev.GOLD) if g["label"] != "A"]
    _require_subtypes(gold, ev.GOLD.name)
    preds = {r["entity_id"]: r for r in _rows(ev.PREDS)}
    if not preds[gold[0]["entity_id"]].get(kind):
        raise SystemExit(f"predictions.csv has no {tag}_* answers; run `run --tag {tag}` first")
    a_fp = sum(preds[g["entity_id"]][kind] != "P" for g in gold if g["stratum"] == "A")
    five = sum(
        preds[g["entity_id"]][kind] == ("P" if g["label"] in "PH" else g["label"]) for g in gold
    )
    print(f"[{tag}] A persons wrongly flagged: {a_fp}/300   5-way: {five}/{len(gold)}")

    pool = [(g, preds[g["entity_id"]], "gold") for g in gold]
    if SUPP.exists() and SUPP_PREDS.exists():
        sp = {r["entity_id"]: r for r in _rows(SUPP_PREDS) if r.get(kind)}
        supp = [r for r in _rows(SUPP) if r["entity_id"] in sp]
        _require_subtypes(supp, SUPP.name)
        pool += [(r, sp[r["entity_id"]], "supp") for r in supp]
    for t in (THRESHOLD, 0.8, 0.9):
        print(
            f"\nthreshold {t}  (bar {BAR:.0%}, min {MIN_EVIDENCE} answers; "
            f"held-out veto at >= {HELD_OUT_MIN})"
        )
        print(
            f"{'subtype':12s}{'labeled':>8s}{'answered':>10s}{'correct':>9s}"
            f"{'precision':>11s}{'95% low':>9s}{'recall':>8s}{'held-out':>10s}  verdict"
        )
        for sub in JEV_ORG_SUBTYPES:
            hits = [(g, src) for g, p, src in pool if _assigned(p, t, tag) == sub]
            ok = sum(_gold_subtype(g) == sub for g, _ in hits)
            n_gold = sum(_gold_subtype(g) == sub for g, _, _ in pool)
            prec = ok / len(hits) if hits else 0.0
            held = [g for g, src in hits if src == "supp"]
            held_ok = sum(_gold_subtype(g) == sub for g in held)
            verdict = _verdict(len(hits), prec, len(held), held_ok)
            print(
                f"{sub:12s}{n_gold:8d}{len(hits):10d}{ok:9d}{prec:11.1%}"
                f"{_wilson_low(ok, len(hits)):9.1%}{ok / n_gold if n_gold else 0:8.1%}"
                f"{f'{held_ok}/{len(held)}':>10s}  {verdict}"
            )

    print(f"\nerrors at {THRESHOLD} (gold label/subtype -> Jev org answer):")
    for g, p, src in pool:
        sub = _assigned(p, THRESHOLD, tag)
        if sub and _gold_subtype(g) != sub:
            print(
                f"  [{src}] {g['name']!r}: {g['label']}/{g['subtype'] or '-'} -> {sub} "
                f"(kind {p[kind_conf]}, org {p[org_conf]})"
            )

    print(f"\nregex org subtypes a trusted Jev answer would override, at {THRESHOLD}:")
    for g, p, src in pool:
        sub = _assigned(p, THRESHOLD, tag)
        rtype, rsub = regex_classification(g["name"])
        if sub and rtype == "organization" and rsub in JEV_ORG_SUBTYPES and rsub != sub:
            right = "right" if _gold_subtype(g) == sub else "WRONG"
            print(f"  [{src}] {g['name']!r}: regex {rsub} -> Jev {sub} ({right})")


def main() -> None:
    """Dispatch the subcommand."""
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "score", "supplement-sql", "supplement"])
    ap.add_argument("--n", type=int, default=60)
    ap.add_argument("--tag", default="prod", help="column prefix, one per ORG_CRITERIA wording")
    args = ap.parse_args()
    if args.cmd == "supplement-sql":
        sys.stdout.write(supplement_sql(args.n))
        return
    if args.cmd == "run":
        asyncio.run(run(args.tag))
    elif args.cmd == "supplement":
        asyncio.run(supplement(args.tag))
    score(args.tag)


if __name__ == "__main__":
    main()
