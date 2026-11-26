# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Fit an initial exact-search override for the unified filtered KNN replay."""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
from statistics import mean, median

from fit_tree import CURRENT, MAX_DEPTH, fit_tree, route

EXACT = "exact"
AUTO = "auto"
EXPECTED = {
    "mod-20220-original-d32": 615,
    "mod-20220-original-d96": 154,
    "mod-20220-original-fp16": 70,
    "mod-20220-expanded-d32": 879,
    "mod-20220-expanded-d96": 202,
    "mod-20220-expanded-fp16": 70,
    "mod-20500": 10_000,
}
ROOTS = ("NUMERIC", "UNION", "INTERSECT", "TEXT", "TAG")
UNIFIED_CONFIGURATIONS = ((32, 96, 1024), (100,))


def planner_features(row):
    planner = row["planner"]
    if (planner is None or planner["filter_root"] not in ROOTS
            or planner["estimate"] <= 0):
        return None
    docs, k = row["index_size"], row["k"]
    estimate = min(planner["estimate"], docs)
    return {
        "documents": docs,
        "k": k,
        "dimension": row["dim"],
        "estimate": estimate,
        "selectivity_ppm": estimate * 1_000_000 // docs,
        "estimate_per_k_milli": estimate * 1_000 // k,
        **{f"root_{kind.lower()}": int(planner["filter_root"] == kind) for kind in ROOTS},
    }


def current_action(row):
    """Reproduce the already qualified early-exact leaf on the published corpus."""
    if row["scenario"] == "mod-20500" and row["family"] == "and":
        children = (row["planner"] or {}).get("child_estimates")
        if children is not None:
            low, high = (min(value, row["index_size"]) for value in children)
            product = low * high // row["index_size"]
            if product <= 86_872 or (product <= 133_378 and high <= 558_180):
                return EXACT
    return AUTO


def completed_latency(trials):
    if any(trial["status"] != "ok" for trial in trials):
        return None
    latencies = [trial["ms"] for trial in trials]
    if not latencies or not all(math.isfinite(ms) and ms > 0 for ms in latencies):
        raise ValueError("missing or invalid latency")
    return median(latencies)


def load_cases(folder, expected, reference):
    rows = []
    for scenario, count in expected.items():
        name = f"{scenario}.jsonl"
        with (folder / name).open() as source:
            cases = [json.loads(line) for line in source]
        ids = {row["case"] for row in cases}
        if len(cases) != count or len(ids) != count:
            raise ValueError(f"{scenario}: expected {count} unique cases")
        original = {}
        if reference is not None:
            with (reference / name).open() as source:
                original = {row["case"]: row for row in map(json.loads, source)}
            if ids != original.keys():
                raise ValueError(f"{scenario}: case IDs differ from the unified benchmark")
        for row in cases:
            if row["scenario"] != scenario or set(row["runs"]) != {AUTO, EXACT, "batches"}:
                raise ValueError(f"{row['case']}: wrong scenario or missing stock action")
            if original:
                source = original[row["case"]]
                fields = ("index_size", "k", "dim", "m", "ef_runtime", "family")
                if any(source[key] != row[key] for key in fields):
                    raise ValueError(f"{row['case']}: case shape differs from the unified benchmark")
                matches = source["actual_matches"]
                if matches is not None and row["truth_count"] != min(matches, row["k"]):
                    raise ValueError(f"{row['case']}: eligible result count changed")
            outcomes = {mode: completed_latency(trials) for mode, trials in row["runs"].items()}
            if (row["planner"] or {}).get("auto_mode") == "HYBRID_ADHOC_BF":
                # Both commands used direct exact. Do not fit measurement noise.
                if outcomes[AUTO] is not None and outcomes[EXACT] is not None:
                    outcomes[EXACT] = outcomes[AUTO]
            row["time"] = outcomes
            row["features"] = planner_features(row)
            row["current"] = current_action(row)
            row["bucket"] = scenario.rsplit("-", 1)[-1] if scenario.startswith("mod-20220") else scenario
            rows.append(row)
    return rows


def held_out(row):
    if row["scenario"] == "mod-20500":
        digest = hashlib.blake2b(row["case"].encode(), digest_size=8).digest()
        return int.from_bytes(digest, "big") % 5 == 0
    return "-expanded-" in row["scenario"]


def fit(rows):
    # Score relative latency per case and give each configuration equal weight.
    # The 10,000 published cases should not outvote the customer workload.
    eligible = [row for row in rows if row["features"] is not None and row["current"] == AUTO]
    sizes = Counter(row["bucket"] for row in eligible)
    data = []
    for row in eligible:
        baseline, exact = row["time"][AUTO], row["time"][EXACT]
        weight = 1 / sizes[row["bucket"]]
        safe = {CURRENT}
        if (baseline is not None and exact is not None and row["lost_vs_auto"][EXACT] == 0
                and not material_slowdown(baseline, exact)):
            safe.add(EXACT)
        data.append({
            **row, "safe": safe, "cost": {
                CURRENT: weight,
                EXACT: weight * exact / baseline if EXACT in safe else weight,
            },
        })
    return fit_tree(data, (EXACT,), MAX_DEPTH)[1]


def material_slowdown(baseline, selected):
    return selected > 1.2 * baseline and selected - baseline > 0.1


def chosen_action(row, tree):
    if row["current"] == EXACT or row["features"] is None:
        return row["current"]
    if "by_dimension" in tree:
        tree = tree["by_dimension"].get(str(row["dim"]), {"action": CURRENT})
    return EXACT if route(tree, row["features"]) == EXACT else AUTO


def evaluate(rows, tree):
    counts = Counter()
    latencies = defaultdict(list)
    recalls = defaultdict(list)
    changed = []
    for row in rows:
        current, chosen = row["current"], chosen_action(row, tree)
        counts["cases"] += 1
        counts["planner_features"] += row["features"] is not None
        counts["overrides"] += current != chosen
        counts["changed_execution_path"] += current != chosen and (
            (row["planner"] or {}).get("auto_mode") != "HYBRID_ADHOC_BF"
        )
        policy_runs = {name: row["runs"][action] for name, action
                       in (("auto", AUTO), ("current", current), ("tree", chosen))}
        for name, runs in policy_runs.items():
            if row["truth_count"]:
                recalls[name].append(min(run.get("hits", 0) for run in runs) / row["truth_count"])
        lost = row["lost_vs_auto"][EXACT] if chosen == EXACT else 0
        counts["lost_hits_vs_auto"] += lost
        counts["lost_queries_vs_auto"] += bool(lost)
        counts["lost_hits_vs_current"] += lost if current != chosen else 0
        counts["lost_queries_vs_current"] += bool(lost and current != chosen)
        selected = row["time"][chosen]
        baseline = row["time"][current]
        if current != chosen and (lost or selected is None or (
                baseline is not None and material_slowdown(baseline, selected))):
            changed.append({
                "case": row["case"], "auto_mode": (row["planner"] or {}).get("auto_mode"),
                "baseline_ms": baseline, "candidate_ms": selected, "lost_hits": lost,
            })
        for label, action in (("auto", AUTO), ("current", current)):
            base = row["time"][action]
            counts[f"new_failures_vs_{label}"] += base is not None and selected is None
            if base is None or selected is None:
                continue
            ratio = selected / base
            latencies[label].append((base, selected, ratio))
            counts[f"slower_gt_5pct_vs_{label}"] += ratio > 1.05
            counts[f"material_slowdowns_vs_{label}"] += material_slowdown(base, selected)
    return {
        **counts,
        "first_regressions_vs_current": changed[:20],
        "mean_worst_repeat_recall": {key: mean(values) for key, values in recalls.items() if values},
        "latency": {
            key: {
                "comparable": len(values),
                "mean_per_case_latency_delta_pct": 100 * (mean(ratio for _, _, ratio in values) - 1),
                "summed_median_delta_pct": 100 * (
                    sum(selected for _, selected, _ in values) / sum(base for base, _, _ in values) - 1
                ),
            }
            for key, values in latencies.items()
        },
    }


def slices(rows, tree, name):
    groups = defaultdict(list)
    for row in rows:
        groups[str(row[name])].append(row)
    return {key: evaluate(group, tree) for key, group in sorted(groups.items())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("measurements", type=Path)
    parser.add_argument("--reference", type=Path, help="Unified benchmark artifacts for case-ID validation")
    parser.add_argument("--historical-only", action="store_true")
    groups = parser.add_mutually_exclusive_group()
    groups.add_argument("--split-by-dimension", action="store_true",
                        help="Fit one shallow subtree for each measured vector dimension")
    groups.add_argument("--split-published-configuration", action="store_true",
                        help="Fit separate subtrees for the published and historical dimensions")
    args = parser.parse_args()
    expected = EXPECTED.copy()
    if args.historical_only:
        del expected["mod-20500"]
    rows = load_cases(args.measurements, expected, args.reference)
    train = [row for row in rows if not held_out(row)]
    holdout = [row for row in rows if held_out(row)]
    if args.split_by_dimension or args.split_published_configuration:
        dimensions = sorted({row["dim"] for row in train})
        partitions = ([(dim,) for dim in dimensions] if args.split_by_dimension
                      else UNIFIED_CONFIGURATIONS)
        trees = {}
        for partition in partitions:
            group = [row for row in train if row["dim"] in partition]
            if group:
                subtree = fit(group)
                trees.update({str(dim): subtree for dim in partition if dim in dimensions})
        tree = {"by_dimension": trees}
    else:
        tree = fit(train)
    report = evaluate(holdout, tree)
    buckets = slices(holdout, tree, "bucket")
    delta = mean(bucket["latency"]["current"]["mean_per_case_latency_delta_pct"]
                 for bucket in buckets.values())
    quality = not any(report[key] for key in (
        "lost_hits_vs_auto", "lost_hits_vs_current", "new_failures_vs_auto", "new_failures_vs_current",
    ))
    print(json.dumps({
        "policy": tree,
        "scope": expected,
        "training": evaluate(train, tree),
        "holdout": report,
        "full_replay": evaluate(rows, tree),
        "holdout_equal_configuration_latency_delta_pct": delta,
        "holdout_by_configuration": buckets,
        "holdout_by_scenario": slices(holdout, tree, "scenario"),
        "holdout_by_index_size": slices(holdout, tree, "index_size"),
        "holdout_by_k": slices(holdout, tree, "k"),
        "holdout_by_filter": slices(holdout, tree, "family"),
        "full_replay_by_configuration": slices(rows, tree, "bucket"),
        "passes_holdout": quality and delta < 0 and bool(report["changed_execution_path"])
        and not report["material_slowdowns_vs_current"]
        and all(bucket["latency"]["current"]["mean_per_case_latency_delta_pct"] <= 0
                for bucket in buckets.values()),
    }, allow_nan=False, indent=2))


if __name__ == "__main__":
    main()
