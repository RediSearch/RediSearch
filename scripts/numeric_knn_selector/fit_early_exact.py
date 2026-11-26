# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Find early-exact AND routes using the full stock-mode replay."""
import argparse
import json
from pathlib import Path
from statistics import median

from fit_tree import CURRENT, MAX_DEPTH, fit_tree, planner_features, route, score


def main():
    """Restrict fitting to the AND query IDs in the earlier samples."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replay", type=Path)
    parser.add_argument("--calibration-samples", type=Path, nargs="+", required=True)
    args = parser.parse_args()
    known = set()
    for path in args.calibration_samples:
        with path.open() as source:
            known.update(row["id"] for row in map(json.loads, source))
    rows = []
    with args.replay.open() as source:
        population = list(map(json.loads, source))
    if len(population) != 3333 or len({row["id"] for row in population}) != 3333:
        raise ValueError("the complete AND family replay is required")
    for row in population:
        if "hits" not in row:
            continue
        if row["current"] != "auto" or row["choice"] != "adhoc":
            raise ValueError("expected stock AUTO and exact-search measurements")
        row["cost"] = {mode: median(samples) for mode, samples in row["ns"].items()}
        row["hits"] = {mode: set(ids) for mode, ids in row["hits"].items()}
        row["cost"][CURRENT] = row["cost"]["auto"]
        row["hits"][CURRENT] = row["hits"]["auto"]
        if not row["hits"][CURRENT] <= row["hits"]["adhoc"]:
            raise ValueError(f"query {row['id']} lost a ground-truth hit in exact search")
        if row["auto_mode"] == "HYBRID_ADHOC_BF":
            row["cost"]["adhoc"] = row["cost"][CURRENT]
        row["safe"] = {CURRENT, "adhoc"}
        row["features"] = planner_features(row)
        rows.append(row)

    train = [row for row in rows if row["id"] in known]
    unseen = [row for row in rows if row["id"] not in known]
    model = fit_tree(train, ("adhoc",), MAX_DEPTH)[1]

    def selected(row):
        return route(model, row["features"])

    def fastest(row):
        return min((CURRENT, "adhoc"), key=row["cost"].get)

    print(json.dumps({
        "policy": {"single": {"action": CURRENT}, "and": model, "or": {"action": CURRENT}},
        "and_queries": len(population),
        "queries_replayed_in_exact_mode": len(rows),
        "calibration": score(train, selected),
        "unseen": score(unseen, selected),
        "unseen_hindsight": score(unseen, fastest),
        "all": score(rows, selected),
    }, indent=2))


if __name__ == "__main__":
    main()
