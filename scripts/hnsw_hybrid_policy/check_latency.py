# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Evaluate a config-selected policy against its paired legacy baseline."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
from statistics import mean, median


LEGACY = "legacy"
POLICY = "latency"
RECHECK = "legacy_repeat"
RECALL_FLOOR = 0.95
REGRESSION_MARGIN = 0.05
MIN_REPETITIONS = 8


def common_case(row):
    if row["workload"].startswith("customer"):
        return True
    if row["k"] > 100:
        return False
    if row["family"].startswith(("numeric-single", "tag-", "text-")):
        return True
    return (row["family"].startswith("numeric-negation-excludes-")
            and row["actual_matches"] >= row["index_size"] / 2)


def outcomes(rows, baseline, candidate):
    case_results = []
    for row in rows:
        pairs = row["runs"]
        if len(pairs) < MIN_REPETITIONS:
            raise ValueError(f"{row['case']} has fewer than {MIN_REPETITIONS} paired repetitions")
        left = [pair[baseline] for pair in pairs]
        right = [pair[candidate] for pair in pairs]
        left_ok = all(r["status"] == "ok" for r in left)
        right_ok = all(r["status"] == "ok" for r in right)
        record = {
            "case": row["case"],
            "common": common_case(row),
            "baseline_ok": left_ok,
            "candidate_ok": right_ok,
            "baseline_pass": left_ok and min(r["recall"] for r in left) >= RECALL_FLOOR,
            "candidate_pass": right_ok and min(r["recall"] for r in right) >= RECALL_FLOOR,
            "mean_ms_before": None,
            "mean_ms_after": None,
            "median_paired_ratio": None,
            "confirmed_regression": left_ok and not right_ok,
        }
        if left_ok and right_ok:
            ratios = [b["ms"] / a["ms"] for a, b in zip(left, right)]
            record["mean_ms_before"] = mean(r["ms"] for r in left)
            record["mean_ms_after"] = mean(r["ms"] for r in right)
            record["median_paired_ratio"] = median(ratios)
            record["confirmed_regression"] = (
                sum(ratio > 1 + REGRESSION_MARGIN for ratio in ratios) >= len(ratios) - 1
            )
        case_results.append(record)
    return case_results


def summarize(results):
    measured = [r for r in results if r["mean_ms_before"] is not None]
    latency_before = mean(r["mean_ms_before"] for r in measured) if measured else None
    latency_after = mean(r["mean_ms_after"] for r in measured) if measured else None
    regressions = [r["case"] for r in results if r["confirmed_regression"]]
    return {
        "cases": len(results),
        "legacy_mean_ms": latency_before,
        "candidate_mean_ms": latency_after,
        "mean_change_percent": 100 * (latency_after / latency_before - 1) if measured else None,
        "median_paired_ratio": median(r["median_paired_ratio"] for r in measured) if measured else None,
        "any_positive_median_delta": sum(r["median_paired_ratio"] > 1 for r in measured),
        "median_regressions_over_5_percent": sum(
            r["median_paired_ratio"] > 1 + REGRESSION_MARGIN for r in measured
        ),
        "confirmed_regressions": len(regressions),
        "confirmed_regression_share": len(regressions) / len(results) if results else None,
        "regression_cases": regressions,
        "legacy_recall_passes": sum(r["baseline_pass"] for r in results),
        "candidate_recall_passes": sum(r["candidate_pass"] for r in results),
        "lost_legacy_recall": [r["case"] for r in results if r["baseline_pass"] and not r["candidate_pass"]],
        "new_failures": [r["case"] for r in results if r["baseline_ok"] and not r["candidate_ok"]],
        "new_recall_passes": [r["case"] for r in results if r["candidate_pass"] and not r["baseline_pass"]],
    }


def load(paths):
    rows = [json.loads(line) for path in paths for line in Path(path).read_text().splitlines()]
    if len({r["case"] for r in rows}) != len(rows):
        raise ValueError("input contains duplicate cases")
    return rows


def evaluate(rows):
    if not rows:
        raise ValueError("evaluation requires at least one measured case")
    results = outcomes(rows, LEGACY, POLICY)
    versus_recheck = outcomes(rows, RECHECK, POLICY)
    for result, recheck_result in zip(results, versus_recheck):
        result["confirmed_regression"] &= recheck_result["confirmed_regression"]
    groups = defaultdict(list)
    for row, result in zip(rows, results):
        groups[row["workload"]].append(result)
    all_cases = summarize(results)
    common = summarize([r for r in results if r["common"]])
    edge = summarize([r for r in results if not r["common"]])
    trained = summarize(outcomes(rows, "trained", POLICY))
    recheck = summarize(outcomes(rows, LEGACY, RECHECK))
    common_vs_recheck = summarize([r for r in versus_recheck if r["common"]])
    gates = {
        "faster_on_common": (common["mean_change_percent"] is not None
                             and common["mean_change_percent"] < 0
                             and common_vs_recheck["mean_change_percent"] < 0),
        "no_confirmed_common_regression": common["confirmed_regressions"] == 0,
        "under_ten_percent_regressions": all_cases["confirmed_regression_share"] < 0.10,
        "preserves_legacy_recall": not all_cases["lost_legacy_recall"],
        "no_new_query_failures": not all_cases["new_failures"],
    }
    return {
        "gates": gates, "gate_pass": all(gates.values()),
        "all_cases": all_cases, "common_cases": common, "edge_cases": edge,
        "by_workload": {name: summarize(group) for name, group in sorted(groups.items())},
        "compared_with_first_trained": trained, "legacy_recheck": recheck,
        "common_vs_legacy_recheck": common_vs_recheck,
        "cases": results,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    rows = load(args.inputs)
    report = evaluate(rows)
    core = [r for r in rows if not r["family"].startswith(
        ("numeric-single-wide-", "numeric-negation-excludes-")
    )]
    if len(core) != len(rows):
        report["original_matrix"] = evaluate(core)
        report["gate_pass"] &= report["original_matrix"]["gate_pass"]
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["gates"], indent=2))


if __name__ == "__main__":
    main()
