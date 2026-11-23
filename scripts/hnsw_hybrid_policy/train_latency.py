# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Fit a legacy-fallback decision tree from paired AUTO and alternative measurements."""

import argparse
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
from statistics import mean, median

import numpy as np
from sklearn.tree import DecisionTreeClassifier, export_text

from check_latency import RECALL_FLOOR, REGRESSION_MARGIN, common_case


LEGACY = "legacy"
EXACT = "ADHOC_BF"
FEATURES = ("k", "indexSize", "dimension", "m", "selectivity", "eligible", "eligiblePerK")
REGRESSION_RATIO = 1 + REGRESSION_MARGIN


def outcome(runs):
    ok = all(r["status"] == "ok" for r in runs)
    if not ok:
        return None
    latencies = [r["ms"] for r in runs]
    return (mean(latencies), median(latencies), all(r["recall"] >= RECALL_FLOOR for r in runs))


def features(record):
    eligible = min(record["subset_size"] or 0, record["index_size"])
    return (record["k"], record["index_size"], record["dim"], record["m"],
            np.float32(eligible) / np.float32(record["label_count"]),
            eligible, np.float32(eligible) / np.float32(record["k"]))


def load(paths):
    rows = []
    seen = set()
    for path in paths:
        for line in Path(path).read_text().splitlines():
            record = json.loads(line)
            if record["case"] in seen:
                raise ValueError(f"duplicate case {record['case']} in {path}")
            seen.add(record["case"])
            runs = record["runs"]
            if not runs:
                raise ValueError(f"{record['case']} has no paired measurements")
            names = set(runs[0])
            if any(set(run) != names for run in runs):
                raise ValueError(f"{record['case']} has unpaired measurements")
            options = {name: outcome([run[name] for run in runs]) for name in sorted(names)}
            if record.get("controls"):
                options[EXACT] = outcome([run[EXACT] for run in record["controls"]])
            initial_mode = (record["profile_mode"].get(LEGACY) or "")
            if initial_mode == EXACT:
                options[EXACT] = options[LEGACY]
            record["options"] = options
            record["can_change"] = initial_mode.startswith("BATCHES")
            rows.append(record)
    return rows


def report(cases, policy):
    results = []
    for row in cases:
        baseline = row["options"][LEGACY]
        choice = policy(row)
        observed = row["options"].get(choice)
        results.append((row, baseline, choice, observed))
    completed = [r for r in results if r[1] and r[3]]
    common_completed = [r for r in completed if common_case(r[0])]
    regressions = [r for r in completed if r[3][1] > REGRESSION_RATIO * r[1][1]]
    failures = [r for r in results if r[1] and not r[3]]
    return {
        "cases": len(results),
        "common_cases": sum(common_case(r[0]) for r in results),
        "legacy_mean_ms": mean(r[1][0] for r in completed) if completed else None,
        "mean_ms": mean(r[3][0] for r in completed) if completed else None,
        "common_legacy_mean_ms": mean(r[1][0] for r in common_completed) if common_completed else None,
        "common_mean_ms": mean(r[3][0] for r in common_completed) if common_completed else None,
        "legacy_qualified": sum(bool(r[1] and r[1][2]) for r in results),
        "qualified": sum(bool(r[3] and r[3][2]) for r in results),
        "lost_legacy_qualified": sum(bool(r[1] and r[1][2] and not (r[3] and r[3][2])) for r in results),
        "gained_recall": sum(bool(r[3] and r[3][2] and not (r[1] and r[1][2])) for r in results),
        "lost_trained_qualified": sum(bool(r[0]["options"].get("trained") and r[0]["options"]["trained"][2]
                                          and not (r[3] and r[3][2])) for r in results),
        "regressions": len(regressions) + len(failures),
        "common_regressions": sum(common_case(r[0]) for r in regressions + failures),
        "regression_cases": [r[0]["case"] for r in regressions + failures],
        "choices": dict(Counter(r[2] for r in results)),
    }


def fit(cases, actions, *, max_leaves, min_leaf, latency_budget, gain_value, common_zero):
    eligible = [r for r in cases if r["can_change"] and r["options"][LEGACY]]
    x = np.array([features(r) for r in eligible], dtype=np.float32)

    def utility(row, action):
        baseline = row["options"][LEGACY]
        option = row["options"].get(action)
        if option is None or (baseline[2] and not option[2]):
            return -20
        increases = option[1] > REGRESSION_RATIO * baseline[1]
        if common_zero and common_case(row) and increases:
            return -20
        saving = max(-20, min(20, baseline[0] - option[0]))
        gained_recall = option[2] and not baseline[2]
        return saving + gain_value * gained_recall - latency_budget * increases

    scores = np.array([[utility(row, action) for action in actions] for row in eligible])
    best = np.argmax(scores, axis=1)
    # Use the cost of changing the decision rather than the magnitude of the
    # latency ratio. Cap the split influence of a single very slow case.
    weights = 1 + np.ptp(scores, axis=1)
    tree = DecisionTreeClassifier(random_state=0, max_leaf_nodes=max_leaves, min_samples_leaf=min_leaf)
    tree.fit(x, best, sample_weight=weights)
    leaf_nodes = tree.apply(x)
    leaves = {}
    for node in sorted(set(leaf_nodes)):
        indices = np.flatnonzero(leaf_nodes == node)
        group = [eligible[i] for i in indices]
        candidates = []
        for col, action in enumerate(actions):
            assessment = report(group, lambda _: action)
            if any(r["options"].get(action) is None for r in group):
                continue
            if assessment["lost_legacy_qualified"] or (
                common_zero and assessment["common_regressions"]
            ):
                continue
            if assessment["regressions"] >= 0.10 * len(group):
                continue
            candidates.append((float(scores[indices, col].sum()), -col, action))
        leaves[node] = max(candidates)[2] if candidates else LEGACY
    return tree, leaves


def predict(model, row):
    if not row["can_change"] or row["index_size"] > 1_000_000 or row["k"] > 4_000:
        return LEGACY
    if (row["dim"], row["m"]) not in {(32, 8), (96, 32), (1024, 16)}:
        return LEGACY
    tree, leaves = model
    node, = tree.apply(np.array([features(row)], dtype=np.float32))
    return leaves[node]


def unsaturated(row):
    return features(row)[4] < np.float32(0.99)


def render_plan(model):
    tree, leaves = model
    tree = tree.tree_

    def render(node):
        if node in leaves:
            return ["return {true, false};"] if leaves[node] == EXACT else []
        left = render(tree.children_left[node])
        right = render(tree.children_right[node])
        if left == right:
            return left
        variable = FEATURES[tree.feature[node]]
        cutoff = tree.threshold[node].hex()
        if not left:
            return [f"if ({variable} > {cutoff}) {{", *["    " + line for line in right], "}"]
        if not right:
            return [f"if ({variable} <= {cutoff}) {{", *["    " + line for line in left], "}"]
        return [
            f"if ({variable} <= {cutoff}) {{",
            *["    " + line for line in left],
            "} else {",
            *["    " + line for line in right],
            "}",
        ]

    return ["    " + line for line in render(0)]


def write_header(path, model):
    lines = [
        "// Generated by train_latency.py.",
        "#pragma once",
        "",
        '#include "hnsw_policy_candidate.h"',
        "",
        "struct HnswLatencyPlan {",
        "    bool adhoc = false;",
        "    bool allowEarlyAdhoc = false;",
        "};",
        "",
        "inline HnswLatencyPlan latencyHybridPlan(const HnswPolicyInput &input) {",
        "    if (input.k == 0 || input.k > 4000 || input.labelCount == 0 || input.indexSize > 1000000) {",
        "        return {};",
        "    }",
        "    if (!((input.dimension == 32 && input.m == 8) ||",
        "          (input.dimension == 96 && input.m == 32) ||",
        "          (input.dimension == 1024 && input.m == 16))) {",
        "        return {};",
        "    }",
        "    [[maybe_unused]] const float k = static_cast<float>(input.k);",
        "    [[maybe_unused]] const float indexSize = static_cast<float>(input.indexSize);",
        "    [[maybe_unused]] const float dimension = static_cast<float>(input.dimension);",
        "    [[maybe_unused]] const float m = static_cast<float>(input.m);",
        "    [[maybe_unused]] const float eligible =",
        "        static_cast<float>(std::min(input.subsetSize, input.indexSize));",
        "    [[maybe_unused]] const float selectivity = eligible / static_cast<float>(input.labelCount);",
        "    [[maybe_unused]] const float eligiblePerK = eligible / k;",
        "    if (selectivity >= 0.99f) {",
        "        return {false, true};",
        "    }",
    ]
    lines.extend(render_plan(model))
    lines.extend(["    return {};", "}", ""])
    path.write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--eval", nargs="+", default=())
    parser.add_argument("--max-leaves", type=int, default=20)
    parser.add_argument("--min-leaf", type=int, default=5)
    parser.add_argument("--regression-cost-ms", type=float, default=2)
    parser.add_argument("--recall-gain-value-ms", type=float, default=2)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--cpp", type=Path)
    args = parser.parse_args()
    train = load(args.inputs)
    actions = [LEGACY, EXACT]
    fit_options = dict(max_leaves=args.max_leaves, min_leaf=args.min_leaf,
                       latency_budget=args.regression_cost_ms, gain_value=args.recall_gain_value_ms,
                       common_zero=True)
    model = fit([r for r in train if unsaturated(r)], actions, **fit_options)

    def prediction(row):
        return predict(model, row) if unsaturated(row) else LEGACY

    evidence = {"training": report(train, prediction)}
    evidence["inputs"] = [
        {"name": Path(p).name, "sha256": sha256(Path(p).read_bytes()).hexdigest()}
        for p in args.inputs
    ]
    evidence["fit_options"] = fit_options
    evidence["adaptive_cases_replayed_as_legacy"] = sum(not unsaturated(r) for r in train)
    if args.eval:
        validation = load(args.eval)
        if {r["case"] for r in train} & {r["case"] for r in validation}:
            raise ValueError("training and validation contain the same case IDs")
        evidence["validation"] = report(validation, prediction)
    args.report.write_text(json.dumps(evidence, indent=2) + "\n")
    if args.cpp:
        write_header(args.cpp, model)
    print(export_text(model[0], feature_names=list(FEATURES)))
    print(model[1])


if __name__ == "__main__":
    main()
