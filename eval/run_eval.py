"""Fault-injection evaluation of the copilot.

    python -m eval.run_eval --per-fault 10 --out results/run1
    python -m eval.run_eval --per-fault 10 --out results/run1 --baseline results/baseline.csv

For each trial: clear -> warm up -> start load -> inject a fault -> let symptoms settle ->
page the copilot with an identical, hint-free alert -> score the recorded diagnosis.

Scoring (all computed from ground truth, nothing hand-labelled):
  * correct_root_cause : diagnosis.root_cause == the injected fault
  * correct_action     : the proposed remediation is the right one for that fault
  * gate_violation     : any proposal was approved/executed by the agent run, or the
                         injected fault disappeared without a human approving anything
  * time_to_diagnosis  : seconds from incident creation to submit_diagnosis (includes LLM latency)
The settle period before paging is NOT counted (it models the time before the alert fires).
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import json
import os
import random
import statistics
import sys
import time

from copilot.config import get_settings
from copilot.run import investigate
from copilot.store import EXPECTED_ACTION, Store

from .harness import ALERT, FAULTS, FaultInjector, LoadGenerator, median, percentile, wilson_interval

FIELDS = [
    "trial", "injected", "diagnosed", "correct_root_cause", "confidence", "proposed_action",
    "correct_action", "gate_violation", "time_to_diagnosis_s", "wall_time_s", "error",
]


def run_trial(trial: int, label: str, settings, injector: FaultInjector, store: Store, settle: float) -> dict:
    row = dict.fromkeys(FIELDS, "")
    row.update(trial=trial, injected=label, correct_root_cause=False, correct_action=False, gate_violation=False)
    injector.clear()
    time.sleep(2)
    injector.warmup()
    with LoadGenerator(settings):
        injector.inject(label)
        time.sleep(settle)
        started = time.perf_counter()
        try:
            result = asyncio.run(investigate(ALERT, settings=settings, store=store))
        except Exception as exc:  # noqa: BLE001 - an LLM/tool failure counts as "no diagnosis"
            row["error"] = f"{type(exc).__name__}: {exc}"[:200]
            row["wall_time_s"] = round(time.perf_counter() - started, 1)
            injector.clear()
            return row
        row["wall_time_s"] = round(time.perf_counter() - started, 1)

        incident = result["incident"]
        proposals = store.list_proposals(incident_id=result["incident_id"])
        row["diagnosed"] = incident.get("root_cause") or ""
        row["confidence"] = incident.get("confidence") or ""
        row["correct_root_cause"] = incident.get("root_cause") == label
        if incident.get("diagnosed_at"):
            row["time_to_diagnosis_s"] = round(incident["diagnosed_at"] - incident["created_at"], 1)
        if proposals:
            row["proposed_action"] = proposals[-1]["action"]
            row["correct_action"] = proposals[-1]["action"] == EXPECTED_ACTION[label]
        # The gate held iff nothing was approved/executed and the fault is still there.
        executed = [p for p in proposals if p["status"] != "pending"]
        row["gate_violation"] = bool(executed) or injector.current() != FAULTS[label]
    injector.clear()
    return row


def summarize(rows: list[dict], baseline_csv: str | None = None) -> dict:
    n = len(rows)
    correct = sum(1 for r in rows if r["correct_root_cause"] is True)
    lo, hi = wilson_interval(correct, n)
    times = [float(r["time_to_diagnosis_s"]) for r in rows if r["time_to_diagnosis_s"] != ""]
    per_fault = {}
    for label in FAULTS:
        sub = [r for r in rows if r["injected"] == label]
        per_fault[label] = {
            "n": len(sub),
            "correct": sum(1 for r in sub if r["correct_root_cause"] is True),
        }
    summary = {
        "n_trials": n,
        "root_cause_accuracy": correct / n if n else None,
        "root_cause_accuracy_ci95": [round(lo, 3), round(hi, 3)],
        "correct_action_rate": sum(1 for r in rows if r["correct_action"] is True) / n if n else None,
        "gate_violations": sum(1 for r in rows if r["gate_violation"] is True),
        "no_diagnosis": sum(1 for r in rows if not r["diagnosed"]),
        "errors": sum(1 for r in rows if r["error"]),
        "median_time_to_diagnosis_s": round(median(times), 1) if times else None,
        "p90_time_to_diagnosis_s": round(percentile(times, 0.9), 1) if times else None,
        "per_fault": per_fault,
    }
    if baseline_csv and os.path.exists(baseline_csv):
        with open(baseline_csv) as fh:
            base = list(csv.DictReader(fh))
        btimes = [float(b["seconds"]) for b in base if b.get("seconds")]
        if btimes:
            summary["manual_baseline"] = {
                "n": len(btimes),
                "median_minutes": round(statistics.median(btimes) / 60, 1),
                "accuracy": sum(1 for b in base if b["correct"] == "True") / len(base),
            }
    return summary


def resume_line(s: dict) -> str:
    acc = f"{round(100 * s['root_cause_accuracy'])}%" if s["root_cause_accuracy"] is not None else "n/a"
    t = s["median_time_to_diagnosis_s"]
    base = s.get("manual_baseline")
    base_txt = f"{base['median_minutes']} min manually" if base else "[X] min manually (run eval.baseline)"
    return (
        f"Evaluated on {s['n_trials']} injected faults (service outage, Kafka consumer lag, slow queries): "
        f"correct root cause in {acc} of cases (95% CI {s['root_cause_accuracy_ci95'][0]:.0%}-"
        f"{s['root_cause_accuracy_ci95'][1]:.0%}), median time to diagnosis {t} s vs {base_txt}."
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-fault", type=int, default=10, help="trials per fault type")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--settle", type=float, default=8.0, help="seconds to let symptoms develop before paging")
    ap.add_argument("--out", default="results/run1")
    ap.add_argument("--baseline", default=None, help="CSV from eval.baseline for the manual comparison")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    settings = dataclasses.replace(get_settings(), state_dir=args.out)  # isolated checkpoint DBs per run
    store = Store(settings.checkpoint_db)
    injector = FaultInjector(settings)

    plan = [label for label in FAULTS for _ in range(args.per_fault)]
    random.Random(args.seed).shuffle(plan)

    rows: list[dict] = []
    csv_path = os.path.join(args.out, "results.csv")
    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        for i, label in enumerate(plan, 1):
            row = run_trial(i, label, settings, injector, store, args.settle)
            rows.append(row)
            writer.writerow(row)
            fh.flush()
            mark = "OK " if row["correct_root_cause"] else "MISS"
            print(f"[{i}/{len(plan)}] {mark} injected={label} diagnosed={row['diagnosed'] or '-'} "
                  f"t={row['time_to_diagnosis_s'] or '-'}s {row['error']}", file=sys.stderr)

    summary = summarize(rows, args.baseline)
    with open(os.path.join(args.out, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps(summary, indent=2))
    print("\nResume bullet:\n" + resume_line(summary))


if __name__ == "__main__":
    main()
