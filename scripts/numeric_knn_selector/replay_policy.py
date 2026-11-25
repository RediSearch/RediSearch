# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Replay a fitted route with stock query options for a complete filter family."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys

from fit_selector import FAMILIES, measure, planning_profile
from fit_tree import CURRENT, current_action, planner_features, route
from replay import connection, read_queries, require_stock_calibration


def main():
    """Capture paired durations and exact-hit sets for changed queries."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("socket")
    parser.add_argument("policy", type=Path, help="fitter report or family-to-tree JSON map")
    parser.add_argument("output", type=Path)
    parser.add_argument("--data", type=Path, default=Path("data"))
    parser.add_argument("--family", choices=FAMILIES, default="and")
    args = parser.parse_args()
    document = json.loads(args.policy.read_text())
    policy = document.get("policy", document)
    tree = policy.get(args.family)
    if tree is None:
        parser.error(f"{args.policy} has no tree for the {args.family} family")

    client = connection(args)
    require_stock_calibration(client)
    queries = read_queries(args.data / "tests.jsonl", 10_000)

    def check(i):
        query = queries[i]
        row = {"id": i, "family": query[-1], **planning_profile(client, query)}
        chosen = route(tree, planner_features(row))
        old = current_action(row)
        row.update(choice=chosen, current=old)
        if chosen in (CURRENT, old):
            return row
        modes = tuple(dict.fromkeys(("auto", old, chosen)))
        first = modes if i % 2 == 0 else modes[::-1]
        for name in ("hits", "ns"):
            row[name] = {}
        for order in (first, first[::-1]):
            for mode in order:
                elapsed, hits = measure(client, query, mode)
                row["ns"].setdefault(mode, []).append(elapsed)
                if row["hits"].setdefault(mode, hits) != hits:
                    raise ValueError(f"query {i} returned unstable ground-truth hits in {mode}")
        return row

    ids = [i for i, query in enumerate(queries) if query[-1] == args.family]
    with args.output.open("x") as output, ThreadPoolExecutor(max_workers=6) as pool:
        for count, row in enumerate(pool.map(check, ids), start=1):
            print(json.dumps(row), file=output, flush=True)
            if count % 200 == 0 or count == len(ids):
                print(f"Checked {count}/{len(ids)} queries", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
