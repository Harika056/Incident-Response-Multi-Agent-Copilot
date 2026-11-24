import csv

from eval.harness import median, percentile, wilson_interval
from eval.run_eval import FIELDS, resume_line, summarize


def row(i, injected, diagnosed, t, action=None, violation=False, err=""):
    from copilot.store import EXPECTED_ACTION
    r = dict.fromkeys(FIELDS, "")
    r.update(trial=i, injected=injected, diagnosed=diagnosed, correct_root_cause=injected == diagnosed,
             proposed_action=action or EXPECTED_ACTION.get(diagnosed, ""),
             correct_action=(action or EXPECTED_ACTION.get(diagnosed)) == EXPECTED_ACTION[injected],
             gate_violation=violation, time_to_diagnosis_s=t, error=err)
    return r


def test_wilson_is_wide_for_small_n():
    lo, hi = wilson_interval(3, 3)
    assert lo < 0.5 and hi == 1.0           # 3/3 must not read as "100% sure"
    lo, hi = wilson_interval(27, 30)
    assert 0.74 < lo < 0.8 and hi > 0.95


def test_median_and_percentile():
    assert median([3, 1, 2]) == 2
    assert percentile([1, 2, 3, 4, 5], 0.9) == 4.6


def test_summary_and_resume_line(tmp_path):
    rows = [row(1, "SLOW_QUERY", "SLOW_QUERY", 20.0), row(2, "KAFKA_CONSUMER_LAG", "KAFKA_CONSUMER_LAG", 30.0),
            row(3, "SERVICE_OUTAGE", "SLOW_QUERY", 40.0), row(4, "SERVICE_OUTAGE", "", "", err="LLM timeout")]
    base = tmp_path / "base.csv"
    with open(base, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["trial", "injected", "answer", "correct", "seconds"])
        w.writeheader()
        w.writerows([{"trial": 1, "injected": "X", "answer": "X", "correct": True, "seconds": 300},
                     {"trial": 2, "injected": "X", "answer": "Y", "correct": False, "seconds": 600}])
    s = summarize(rows, str(base))
    assert s["n_trials"] == 4 and s["root_cause_accuracy"] == 0.5
    assert s["median_time_to_diagnosis_s"] == 30.0
    assert s["no_diagnosis"] == 1 and s["errors"] == 1 and s["gate_violations"] == 0
    assert s["manual_baseline"]["median_minutes"] == 7.5
    line = resume_line(s)
    assert "4 injected faults" in line and "50%" in line and "7.5 min manually" in line
