# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Fit and check a shallow routing rule using only Redis's stock hybrid modes."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import itertools
import json
from pathlib import Path
import random
from statistics import median
import sys
import time

from replay import DATA_ROWS, connection, read_queries, require_stock_calibration, search


FAMILIES = ("single", "and", "or")
MODES = ("auto", "adhoc", "batches")
STOCK_ACTIONS = (*MODES, "batch-size-64", "batch-size-128", "batch-size-256")
ESTIMATE = b"Estimated number of matches"


def pairs(items):
    return dict(zip(items[::2], items[1::2], strict=True))


def estimate(node):
    if ESTIMATE in node:
        return int(node[ESTIMATE])
    children = node.get(b"Child iterators", [])
    if not children:
        return None
    counts = [estimate(pairs(child)) for child in children]
    if any(count is None for count in counts):
        return None
    if node[b"Type"] == b"INTERSECT":
        return min(counts)
    return min(DATA_ROWS, sum(counts))


def planning_profile(client, query):
    _, profile = search(client, query, "auto", 2048, profile=True)
    shard = pairs(pairs(profile)[b"Shards"][0])
    vector = pairs(shard[b"Iterators profile"])
    node = pairs(vector[b"Child iterator"])
    children = [estimate(pairs(child)) for child in node.get(b"Child iterators", [])]
    conjunction = query[-1] == "and" and len(children) == 2 and all(c is not None for c in children)
    child_estimates = sorted(min(count, DATA_ROWS) for count in children) if conjunction else None
    return {
        "estimate": estimate(node),
        "product": child_estimates[0] * child_estimates[1] // DATA_ROWS if child_estimates else None,
        "child_estimates": child_estimates,
        "auto_mode": vector[b"Vector search mode"].decode(),
        "stock_batches": int(vector.get(b"Batches number", 0)),
    }


def measure(client, query, mode, *, query_id=None):
    start = time.perf_counter_ns()
    result = search(client, query, mode, 2048)
    elapsed = time.perf_counter_ns() - start
    returned = {int(key.split(b":")[-1]) for key in result[1::2]}
    truth = query[-2]
    label = f"query {query_id} in {mode}" if query_id is not None else mode
    if not result or result[0] != len(returned) or len(result) != 1 + 2 * len(returned):
        raise ValueError(f"{label} returned a malformed KNN reply")
    if len(returned) != len(truth):
        raise ValueError(f"{label} returned {len(returned)} results, expected {len(truth)}")
    return elapsed, sorted(truth & returned)


def selected_queries(queries, per_family, warmup, seed=20500, excluded=()):
    rand = random.Random(seed)
    selected = []
    for family in FAMILIES:
        ids = [i for i, query in enumerate(queries) if query[-1] == family and i not in excluded]
        sampled = rand.sample(ids, 3 * per_family + warmup)
        selected.extend((i, "train") for i in sampled[:2 * per_family])
        selected.extend((i, "holdout") for i in sampled[2 * per_family:3 * per_family])
        selected.extend((i, "warmup") for i in sampled[3 * per_family:])
    rand.shuffle(selected)
    return selected


def capture(args):
    queries = read_queries(args.data / "tests.jsonl", 10000)
    client = connection(args)
    require_stock_calibration(client)
    excluded_ids = set()
    if args.exclude_samples:
        with args.exclude_samples.open() as source:
            excluded_ids.update(json.loads(line)["id"] for line in source)
    modes = tuple(args.actions)
    if len(set(modes)) != len(modes) or not set(MODES) <= set(modes):
        raise ValueError("actions must include auto, adhoc, and batches exactly once")
    scheduled = selected_queries(queries, args.per_family_holdout, 16, args.seed, excluded_ids)
    for i, split in scheduled:
        if split == "warmup":
            for mode in modes:
                measure(client, queries[i], mode, query_id=i)

    def sample(item):
        i, split = item
        query = queries[i]
        worker = connection(args)
        try:
            samples = {mode: [] for mode in modes}
            hits = {}
            order = random.Random(args.seed ^ i).sample(modes, len(modes))
            for repeat in range(args.repeats):
                offset = repeat % len(order)
                for mode in order[offset:] + order[:offset]:
                    elapsed, found = measure(worker, query, mode, query_id=i)
                    samples[mode].append(elapsed)
                    if hits.setdefault(mode, found) != found:
                        raise ValueError(f"query {i} returned varying ground-truth hits in {mode}")
            return {
                "id": i,
                "family": query[-1],
                "split": split,
                "samples_ns": samples,
                "hits": hits,
                "eligible": len(query[-2]),
            }
        finally:
            worker.close()

    scheduled = [(i, split) for i, split in scheduled if split != "warmup"]
    with args.output.open("x") as file:
        with ThreadPoolExecutor(max_workers=args.clients) as pool:
            records = []
            for row in pool.map(sample, scheduled):
                records.append(row)
                if len(records) % 100 == 0 or len(records) == len(scheduled):
                    print(f"Timed {len(records)}/{len(scheduled)} queries", file=sys.stderr, flush=True)
        for index, row in enumerate(records, start=1):
            try:
                row.update(planning_profile(client, queries[row["id"]]))
            except KeyError as error:
                raise ValueError(f"query {row['id']} lacks a stock profile field: {error}") from error
            print(json.dumps(row), file=file, flush=True)
            if index % 100 == 0 or index == len(records):
                print(f"Captured planner estimates for {index}/{len(records)} queries",
                      file=sys.stderr, flush=True)


def choose(row, rule):
    if rule is None or row[rule["feature"]] is None:
        return "auto"
    value = row[rule["feature"]]
    match = value <= rule["threshold"] if rule["side"] == "low" else value >= rule["threshold"]
    return rule["mode"] if match else "auto"


def fit_family(rows, family):
    best = (0, None)
    features = ("estimate", "product") if family == "and" else ("estimate",)
    for feature in features:
        thresholds = sorted({row[feature] for row in rows if row[feature] is not None})
        for mode, threshold, side in itertools.product(MODES[1:], thresholds, ("low", "high")):
            rule = dict(mode=mode, feature=feature, threshold=threshold, side=side)
            region = [row for row in rows if choose(row, rule) == mode]
            if len(region) < 20 or any(row["hits"]["auto"] - row["hits"][mode] for row in region):
                continue
            old = sum(row["median_ns"]["auto"] for row in region)
            new = sum(row["median_ns"][mode] for row in region)
            if old - new > best[0] and new <= 0.95 * old:
                best = old - new, rule
    return best[1]


def summarize(rows, policy, mode=None):
    routing = Counter()
    legacy_modes = Counter()
    old_time = new_time = old_hits = new_hits = added = lost = 0
    loss_queries = []
    for row in rows:
        if mode == "oracle":
            choice = min(
                (m for m in MODES if row["hits"]["auto"] <= row["hits"][m]),
                key=row["median_ns"].get,
            )
        else:
            choice = mode or choose(row, policy[row["family"]])
        routing[choice] += 1
        legacy_modes[row["auto_mode"]] += 1
        old_time += row["median_ns"]["auto"]
        new_time += row["median_ns"][choice]
        old_hits += len(row["hits"]["auto"])
        new_hits += len(row["hits"][choice])
        missing = row["hits"]["auto"] - row["hits"][choice]
        if missing:
            loss_queries.append(row["id"])
        lost += len(missing)
        added += len(row["hits"][choice] - row["hits"]["auto"])
    return {
        "queries": len(rows),
        "eligible_hits": sum(row["eligible"] for row in rows),
        "choices": routing,
        "legacy_modes": legacy_modes,
        "legacy_median_sum_ms": old_time / 1e6,
        "candidate_median_sum_ms": new_time / 1e6,
        "candidate_latency_change_pct": 100 * (new_time / old_time - 1),
        "legacy_hits": old_hits,
        "candidate_hits": new_hits,
        "added_hits": added,
        "lost_hits": lost,
        "queries_with_lost_hits": len(loss_queries),
        "first_queries_with_lost_hits": loss_queries[:20],
    }


def fit(args):
    rows = [json.loads(line) for line in args.samples.read_text().splitlines()]
    if len({row["id"] for row in rows}) != len(rows):
        raise ValueError("query IDs must be unique across the training and holdout sets")
    for row in rows:
        row["median_ns"] = {mode: median(samples) for mode, samples in row["samples_ns"].items()}
        row["hits"] = {mode: set(hits) for mode, hits in row["hits"].items()}
    training = [row for row in rows if row["split"] == "train"]
    holdout = [row for row in rows if row["split"] == "holdout"]
    policy = {
        family: fit_family([row for row in training if row["family"] == family], family)
        for family in FAMILIES
    }
    holdout_result = summarize(holdout, policy)
    and_exact_policy = dict.fromkeys(FAMILIES)
    if policy["and"] and policy["and"]["mode"] == "adhoc":
        and_exact_policy["and"] = policy["and"]
    and_exact_holdout = summarize(holdout, and_exact_policy)
    report = {
        "training": summarize(training, policy),
        "policy": policy,
        "holdout": holdout_result,
        "passes_screen": (
            any(policy.values())
            and holdout_result["lost_hits"] == 0
            and holdout_result["candidate_latency_change_pct"] < 0
        ),
        "holdout_per_family": {
            family: summarize([row for row in holdout if row["family"] == family], policy)
            for family in FAMILIES
        },
        "holdout_forced_modes": {mode: summarize(holdout, policy, mode) for mode in MODES[1:]},
        "holdout_oracle_summed_cost": summarize(holdout, policy, "oracle"),
        "and_exact_policy": and_exact_policy,
        "and_exact_policy_holdout": and_exact_holdout,
        "and_exact_policy_passes_screen": (
            and_exact_policy["and"] is not None
            and and_exact_holdout["lost_hits"] == 0
            and and_exact_holdout["candidate_latency_change_pct"] < 0
        ),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(required=True)
    capture_parser = sub.add_parser("capture")
    capture_parser.add_argument("socket")
    capture_parser.add_argument("output", type=Path)
    capture_parser.add_argument("--data", type=Path, default=Path("data"))
    capture_parser.add_argument("--clients", type=int, default=6)
    capture_parser.add_argument("--per-family-holdout", type=int, default=100)
    capture_parser.add_argument("--repeats", type=int, default=3)
    capture_parser.add_argument("--seed", type=int, default=20500)
    capture_parser.add_argument("--exclude-samples", type=Path)
    capture_parser.add_argument("--actions", choices=STOCK_ACTIONS, nargs="+", default=MODES)
    capture_parser.set_defaults(run=capture)
    fit_parser = sub.add_parser("fit")
    fit_parser.add_argument("samples", type=Path)
    fit_parser.set_defaults(run=fit)
    options = parser.parse_args()
    options.run(options)
