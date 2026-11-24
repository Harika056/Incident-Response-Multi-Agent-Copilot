"""Manual-baseline stopwatch: how long does a HUMAN take to diagnose the same faults?

    python -m eval.baseline --per-fault 3 --out results/baseline.csv

Do this honestly: use only what an on-call engineer would (tail the log file, kafka-consumer-groups,
psql). Do NOT look at OrderHub's /admin endpoints or the copilot. Ideally have a teammate (or
yourself on a different day) do it so you don't remember the pattern.

For each trial a random fault is injected without telling you which. Press Enter when you have
your root cause; the clock stops, then you pick the cause.
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import time

from copilot.config import get_settings

from .harness import FAULTS, FaultInjector, LoadGenerator, median

LABELS = list(FAULTS)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-fault", type=int, default=3)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--settle", type=float, default=8.0)
    ap.add_argument("--out", default="results/baseline.csv")
    args = ap.parse_args()

    settings = get_settings()
    injector = FaultInjector(settings)
    plan = [label for label in LABELS for _ in range(args.per_fault)]
    random.Random(args.seed).shuffle(plan)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    rows = []
    for i, label in enumerate(plan, 1):
        injector.clear()
        time.sleep(2)
        injector.warmup()
        with LoadGenerator(settings):
            injector.inject(label)
            time.sleep(args.settle)
            input(f"\n[{i}/{len(plan)}] A fault is active. Press Enter to START the clock... ")
            started = time.perf_counter()
            input("Investigate with logs / kafka / SQL. Press Enter the moment you know the root cause... ")
            seconds = time.perf_counter() - started
        print("Which was it?")
        for n, name in enumerate(LABELS, 1):
            print(f"  {n}) {name}")
        choice = LABELS[int(input("> ").strip()) - 1]
        rows.append({"trial": i, "injected": label, "answer": choice, "correct": choice == label,
                     "seconds": round(seconds, 1)})
        print(f"{'correct' if choice == label else 'WRONG'} - {seconds:.0f}s")
    injector.clear()

    with open(args.out, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["trial", "injected", "answer", "correct", "seconds"])
        writer.writeheader()
        writer.writerows(rows)
    secs = [r["seconds"] for r in rows]
    print(f"\nMedian manual time to diagnosis: {median(secs) / 60:.1f} min over {len(rows)} trials -> {args.out}")


if __name__ == "__main__":
    main()
