# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Fit a bounded decision tree that routes filtered KNN to stock Redis modes."""

import argparse
from collections import Counter
import json
from pathlib import Path
from statistics import median

from fit_selector import FAMILIES, MODES, STOCK_ACTIONS


CURRENT = "current"
AND_EXACT_THRESHOLD = 86_872
MIN_LEAF_QUERIES = 32
MIN_LEAF_GAIN = 0.05
SPLIT_PENALTY = 0.005
SPLIT_QUANTILES = 16
MAX_DEPTH = 2


def planner_features(row):
    """Extract the values available when the planner chooses a stock action."""
    features = {"estimate": row["estimate"]}
    if row["family"] == "and":
        children = row.get("child_estimates")
        features.update(
            product=row.get("product"),
            smaller_child=children[0] if children else None,
            larger_child=children[1] if children else None,
        )
    return features


def current_action(row):
    """Apply the existing selector's AND rule on the calibrated index."""
    product = row.get("product")
    if row["family"] == "and" and product is not None and product <= AND_EXACT_THRESHOLD:
        return "adhoc"
    return "auto"


def measurements(path):
    """Load paired costs and the actions that preserve both reference hit sets."""
    rows = []
    ids = set()
    actions = None
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            if row["id"] in ids:
                raise ValueError(f"duplicate query {row['id']}")
            ids.add(row["id"])
            available = set(row["samples_ns"])
            if actions is None:
                actions = tuple(action for action in STOCK_ACTIONS if action in available)
            if set(actions) != available or not set(MODES) <= available:
                raise ValueError("every query must contain the same supported stock actions")
            if row["family"] not in FAMILIES or row["split"] not in ("train", "holdout"):
                raise ValueError("unexpected family or split")
            row["cost"] = {action: median(timings) for action, timings in row["samples_ns"].items()}
            row["hits"] = {action: set(hits) for action, hits in row["hits"].items()}
            equivalent = {
                "HYBRID_ADHOC_BF": "adhoc",
                "HYBRID_BATCHES": "batches",
            }.get(row["auto_mode"])
            if equivalent and row["hits"][equivalent] == row["hits"]["auto"]:
                # These forced choices follow the same stock path as AUTO. Timing
                # noise between equivalent runs should never justify a split.
                row["cost"][equivalent] = row["cost"]["auto"]
            current = current_action(row)
            row["cost"][CURRENT] = row["cost"][current]
            row["hits"][CURRENT] = row["hits"][current]
            required = row["hits"]["auto"] | row["hits"][CURRENT]
            row["safe"] = {action for action in (*actions, CURRENT) if required <= row["hits"][action]}
            row["features"] = planner_features(row)
            rows.append(row)
    if not rows:
        raise ValueError("no measurements")
    return rows, actions


def fit_leaf(rows, actions, current_cost):
    """Require a measurable gain before overriding the current selector."""
    cost, chosen = current_cost, CURRENT
    for action in actions:
        if not all(action in row["safe"] for row in rows):
            continue
        alternative = sum(row["cost"][action] for row in rows)
        if alternative < cost and alternative <= (1 - MIN_LEAF_GAIN) * current_cost:
            cost, chosen = alternative, action
    return cost, {"action": chosen}


def fit_tree(rows, actions, depth):
    """Compare a single leaf with supported two-way splits on planner estimates."""
    current_cost = sum(row["cost"][CURRENT] for row in rows)
    if len(rows) < MIN_LEAF_QUERIES:
        return current_cost, {"action": CURRENT}
    best_cost, best_tree = fit_leaf(rows, actions, current_cost)
    if depth == 0 or len(rows) < 2 * MIN_LEAF_QUERIES:
        return best_cost, best_tree

    for feature in rows[0]["features"]:
        distinct = {row["features"][feature] for row in rows}
        if None in distinct or len(distinct) < 2:
            continue
        values = sorted(distinct)
        thresholds = {
            values[(len(values) - 1) * q // SPLIT_QUANTILES] for q in range(1, SPLIT_QUANTILES)
        }
        for threshold in sorted(thresholds):
            left = [row for row in rows if row["features"][feature] <= threshold]
            right = [row for row in rows if row["features"][feature] > threshold]
            if min(len(left), len(right)) < MIN_LEAF_QUERIES:
                continue
            left_cost, left_tree = fit_tree(left, actions, depth - 1)
            right_cost, right_tree = fit_tree(right, actions, depth - 1)
            split_cost = left_cost + right_cost + SPLIT_PENALTY * current_cost
            if split_cost < best_cost:
                best_cost = split_cost
                best_tree = {
                    "feature": feature, "at_most": threshold, "left": left_tree, "right": right_tree,
                }
    return best_cost, best_tree


def route(tree, features):
    """Fall back to the current selector if a required estimate is unavailable."""
    while "feature" in tree:
        value = features[tree["feature"]]
        if value is None:
            return CURRENT
        tree = tree["left"] if value <= tree["at_most"] else tree["right"]
    return tree["action"]


def score(rows, select):
    """Measure latency and per-query hit retention against both controls."""
    if not rows:
        raise ValueError("each filter family needs training and held-out measurements")
    counts = Counter()
    costs = Counter()
    losses = {baseline: [] for baseline in ("auto", CURRENT)}
    for row in rows:
        chosen = select(row)
        counts[chosen] += 1
        costs["tree"] += row["cost"][chosen]
        for baseline, missing in losses.items():
            costs[baseline] += row["cost"][baseline]
            if lost := row["hits"][baseline] - row["hits"][chosen]:
                missing.append({"query": row["id"], "hits": len(lost)})
    return {
        "queries": len(rows),
        "actions": counts,
        "summed_median_ms": {action: cost / 1e6 for action, cost in costs.items()},
        "delta_vs_auto_pct": 100 * (costs["tree"] / costs["auto"] - 1),
        "delta_vs_current_pct": 100 * (costs["tree"] / costs[CURRENT] - 1),
        "lost_hits": {baseline: sum(loss["hits"] for loss in missing)
                      for baseline, missing in losses.items()},
        "queries_with_lost_hits": losses,
    }


def main():
    """Fit on training rows and evaluate the frozen rules on held-out rows."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    args = parser.parse_args()
    rows, actions = measurements(args.samples)
    train = [row for row in rows if row["split"] == "train"]
    holdout = [row for row in rows if row["split"] == "holdout"]
    trees = {}
    for family in FAMILIES:
        candidates = [row for row in train if row["family"] == family]
        if len(candidates) < MIN_LEAF_QUERIES:
            raise ValueError(f"training needs at least {MIN_LEAF_QUERIES} {family} queries")
        trees[family] = fit_tree(candidates, actions, MAX_DEPTH)[1]

    def select(row):
        return route(trees[row["family"]], row["features"])

    def fastest_safe(row):
        return min((action for action in (*actions, CURRENT) if action in row["safe"]),
                   key=row["cost"].get)

    holdout_result = score(holdout, select)
    per_family = {family: score([row for row in holdout if row["family"] == family], select)
                  for family in FAMILIES}
    print(json.dumps({
        "policy": trees,
        "train": score(train, select),
        "holdout": holdout_result,
        "holdout_by_family": per_family,
        "holdout_hindsight": score(holdout, fastest_safe),
        "passes_screen": not any(holdout_result["lost_hits"].values())
        and holdout_result["delta_vs_current_pct"] < 0
        and all(result["delta_vs_current_pct"] <= 0 for result in per_family.values()),
    }, indent=2))


if __name__ == "__main__":
    main()
