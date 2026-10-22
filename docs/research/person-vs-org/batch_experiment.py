# ruff: noqa: INP001, T201 — research script; prints are its CLI output.
"""Jev batch-size experiment: K gold names per request, sharing one ``state``.

Usage (repo root)::

    uv run python docs/research/person-vs-org/batch_experiment.py 1 25 50
    uv run python docs/research/person-vs-org/batch_experiment.py --shuffle --defs-in-state 25

Each name gets its own Choice question (``names.n<i>``). ``--shuffle`` mixes the
strata (gold.csv is sorted by stratum); ``--defs-in-state`` moves the five label
definitions from every question's ``criteria`` into the shared state.
Prints one summary line per K; makes live API calls (TYPESAFE_API_KEY in .env).
"""

import argparse
import asyncio
import csv
import random
import statistics
import time

import evaluate as ev  # sibling module; run from repo root or this directory
import httpx


def _body(chunk: list[dict], *, defs_in_state: bool) -> dict:
    names = {f"n{i}": g["name"] for i, g in enumerate(chunk)}
    state: dict = {"context": ev.CTX, "names": names}
    instr = "What kind of entity does `names.{k}` refer to?"
    if defs_in_state:
        state["label_definitions"] = ev.FIVE
        instr += " Use `label_definitions`."
        criteria = {k: k.replace("_", " ") for k in ev.FIVE}
    else:
        criteria = ev.FIVE
    qs = {
        k: {"type": "choice", "instructions": instr.format(k=k), "criteria": criteria}
        for k in names
    }
    return {"model": ev.MODEL, "state": state, "questions": qs}


async def _call(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, chunk: list[dict], body: dict
) -> tuple[list[dict], dict, float]:
    for attempt in range(6):
        async with sem:
            t = time.perf_counter()
            r = await client.post(ev.URL, json=body)
            dt = time.perf_counter() - t
        if r.status_code in (429, 529):
            await asyncio.sleep(2**attempt)
            continue
        r.raise_for_status()
        return chunk, r.json(), dt
    msg = "rate limited"
    raise RuntimeError(msg)


async def _run(gold: list[dict], preds: dict, k: int, *, defs_in_state: bool) -> str:
    chunks = [gold[i : i + k] for i in range(0, len(gold), k)]
    sem = asyncio.Semaphore(8)
    headers = {"Authorization": f"Bearer {ev._key()}"}  # noqa: SLF001
    async with httpx.AsyncClient(timeout=120, headers=headers) as client:
        t0 = time.perf_counter()
        res = await asyncio.gather(
            *(_call(client, sem, ch, _body(ch, defs_in_state=defs_in_state)) for ch in chunks)
        )
        wall = time.perf_counter() - t0
    correct = agree = low = tokens = a_fp = 0
    for chunk, resp, _ in res:
        tokens += resp["usage"]["input_tokens"]
        for i, g in enumerate(chunk):
            ans = resp["answers"][f"n{i}"]
            code = ev.CODE[ans["choice"]]
            correct += code == ("P" if g["label"] in "PH" else g["label"])
            agree += code == preds[g["entity_id"]]["jev5"]
            low += ans["confidence"] < 0.7  # noqa: PLR2004
            a_fp += g["stratum"] == "A" and code != "P"
    p50 = statistics.median(dt for _, _, dt in res) * 1000
    n = len(gold)
    return (
        f"K={k:3d} reqs={len(chunks):3d} 5way={correct}/{n} agree_jev5={agree} A_fp={a_fp}/300 "
        f"conf<.7={low} tok/name={tokens / n:.0f} p50={p50:.0f}ms wall={wall:.1f}s"
    )


def main() -> None:
    """Run the experiment for each batch size given on the command line."""
    ap = argparse.ArgumentParser()
    ap.add_argument("sizes", nargs="+", type=int)
    ap.add_argument("--shuffle", action="store_true")
    ap.add_argument("--defs-in-state", action="store_true")
    args = ap.parse_args()
    with ev.GOLD.open() as f:
        gold = [g for g in csv.DictReader(f) if g["label"] != "A"]
    with ev.PREDS.open() as f:
        preds = {r["entity_id"]: r for r in csv.DictReader(f)}
    if args.shuffle:
        random.Random(7).shuffle(gold)  # noqa: S311 — deterministic mixing, not crypto
    for k in args.sizes:
        print(asyncio.run(_run(gold, preds, k, defs_in_state=args.defs_in_state)))


if __name__ == "__main__":
    main()
