#!/usr/bin/env python3
"""
Generate synthetic HASH and JSON datasets for GROUPBY + COLLECT benchmarks.

The generated data exercises a grouped event workload:
- many event records grouped by entityName
- nested event fields for JSON, flattened event_* fields for HASH
- deterministic skew profiles and payload padding
- matching HASH and JSON command files
"""

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path


INDEX_NAME = "idx:entity_events"
KEY_PREFIX = "entity:"
DATASET_PREFIX = "groupby-collect-entity-events"
DEFAULT_SEED = 14808
DEFAULT_DOC_COUNT = 100_000
DEFAULT_QUERY_COUNT = 100_000
DEFAULT_PAYLOAD_BYTES = 512

ENTITY_NAMES = [
    "Entity Alpha",
    "Entity Beta",
    "Entity Gamma",
    "Entity Delta",
    "Entity Epsilon",
    "Entity Zeta",
    "Entity Eta",
    "Entity Theta",
    "Entity Iota",
    "Entity Kappa",
]

EVENT_TYPES = ["TYPE_A", "TYPE_B", "TYPE_C"]
BOOLS = ["true", "false"]
DATE0 = date(2020, 1, 1)
DATE_RANGE_DAYS = 4017  # 2020-01-01 .. 2030-12-31

GROUP_DISTRIBUTIONS = {
    "uniform": [100_000] * 10,
    "moderate": [300_000, 120_000, 120_000, 120_000, 120_000, 44_000, 44_000, 44_000, 44_000, 44_000],
    "heavy": [700_000, 70_000, 70_000, 70_000, 15_000, 15_000, 15_000, 15_000, 15_000, 15_000],
}

SORT_ARGS = ["@target", "DESC", "@dueDate", "ASC", "@eventId", "ASC"]

QUERY_VARIANTS = {
    "collect-fields-1-k50": {
        "load": ["@entityName", "@eventId", "@target", "@dueDate"],
        "fields": ["@eventId"],
        "limit": (0, 50),
    },
    "collect-fields-explicit-k50": {
        "load": [
            "@entityName",
            "@eventId",
            "@type",
            "@target",
            "@processed",
            "@dueDate",
        ],
        "fields": ["@eventId", "@type", "@target", "@processed", "@dueDate"],
        "limit": (0, 50),
    },
    "collect-fields-star-k50": {
        "load": "*",
        "fields": "*",
        "limit": (0, 50),
    },
    "collect-fields-explicit-offset500-k50": {
        "load": [
            "@entityName",
            "@eventId",
            "@type",
            "@target",
            "@processed",
            "@dueDate",
        ],
        "fields": ["@eventId", "@type", "@target", "@processed", "@dueDate"],
        "limit": (500, 50),
    },
    "collect-fields-star-offset500-k50": {
        "load": "*",
        "fields": "*",
        "limit": (500, 50),
    },
}


def scale_profile(profile, total):
    base = GROUP_DISTRIBUTIONS[profile]
    base_total = sum(base)
    if total == base_total:
        return list(base)

    quotas = [group_size * total / base_total for group_size in base]
    sizes = [int(quota) for quota in quotas]
    remainder = total - sum(sizes)

    order = sorted(range(len(base)), key=lambda i: (quotas[i] - sizes[i], -i), reverse=True)
    for i in order[:remainder]:
        sizes[i] += 1

    return sizes


def ordinal_date(days):
    return (DATE0 + timedelta(days=days)).isoformat()


def size_label(doc_count):
    if doc_count % 1_000_000 == 0:
        return f"{doc_count // 1_000_000}M"
    if doc_count % 1_000 == 0:
        return f"{doc_count // 1_000}K"
    return str(doc_count)


def make_payload(record_id, payload_bytes):
    if payload_bytes <= 0:
        return ""
    token = f"payload-{record_id}-"
    repeat = (payload_bytes // len(token)) + 1
    return (token * repeat)[:payload_bytes]


def iter_docs(group_sizes, seed, payload_bytes):
    rng = random.Random(seed)
    total = sum(group_sizes)
    emitted = [0] * len(group_sizes)

    for i in range(total):
        best_group = -1
        best_ratio = 2.0
        for group_idx, group_size in enumerate(group_sizes):
            if emitted[group_idx] >= group_size:
                continue
            ratio = emitted[group_idx] / group_size
            if ratio < best_ratio:
                best_ratio = ratio
                best_group = group_idx

        emitted[best_group] += 1

        record_id = 200_000_000 + i
        event_id = 600_000_000 + i
        doc = {
            "recordId": record_id,
            "entityName": ENTITY_NAMES[best_group],
            "event": {
                "id": event_id,
                "type": rng.choice(EVENT_TYPES),
                "target": rng.choice(BOOLS),
                "hasNotes": rng.choice(BOOLS),
                "processed": rng.choice(BOOLS),
                "dueDate": ordinal_date(rng.randint(0, DATE_RANGE_DAYS)),
                "payload": make_payload(record_id, payload_bytes),
            },
        }
        yield record_id, doc


def flatten_for_hash(doc):
    event = doc["event"]
    return {
        "recordId": doc["recordId"],
        "entityName": doc["entityName"],
        "event_id": event["id"],
        "event_type": event["type"],
        "event_target": event["target"],
        "event_hasNotes": event["hasNotes"],
        "event_processed": event["processed"],
        "event_dueDate": event["dueDate"],
        "event_payload": event["payload"],
    }


def write_setup_file(output_file, backend, docs):
    with output_file.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
        for record_id, doc in docs:
            key = f"{KEY_PREFIX}{record_id}"
            if backend == "hash":
                row = ["WRITE", "W1", "1", "HSET", key]
                for field, value in flatten_for_hash(doc).items():
                    row.extend([field, value])
                writer.writerow(row)
            elif backend == "json":
                writer.writerow([
                    "WRITE",
                    "W1",
                    "1",
                    "JSON.SET",
                    key,
                    "$",
                    json.dumps(doc, separators=(",", ":")),
                ])
            else:
                raise ValueError(f"unsupported backend: {backend}")


def collect_args_for_variant(variant):
    fields = variant["fields"]
    if fields == "*":
        collect_args = ["FIELDS", "*"]
    else:
        collect_args = ["FIELDS", str(len(fields)), *fields]

    offset, count = variant["limit"]
    collect_args.extend(["SORTBY", str(len(SORT_ARGS)), *SORT_ARGS])
    collect_args.extend(["LIMIT", str(offset), str(count)])
    return collect_args


def query_for_variant(variant):
    load = variant["load"]
    if load == "*":
        load_args = ["LOAD", "*"]
    else:
        load_args = ["LOAD", str(len(load)), *load]

    collect_args = collect_args_for_variant(variant)
    return [
        "FT.AGGREGATE",
        INDEX_NAME,
        "*",
        *load_args,
        "GROUPBY",
        "1",
        "@entityName",
        "REDUCE",
        "COLLECT",
        str(len(collect_args)),
        *collect_args,
        "AS",
        "events",
        # Keep the grouped rows deterministic; the COLLECT SORTBY above remains the measured sort.
        "SORTBY",
        "2",
        "@entityName",
        "ASC",
        "LIMIT",
        "0",
        "50",
    ]


def write_query_file(output_file, variant, count):
    command = query_for_variant(variant)
    with output_file.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
        for _ in range(count):
            writer.writerow(["READ", "R1", "1", *command])


def suffix_for_backend(backend):
    if backend == "hash":
        return "redisearch"
    if backend == "json":
        return "redisjson"
    raise ValueError(f"unsupported backend: {backend}")


def generate_backend(output_dir, dataset_name, backend, group_sizes, seed, payload_bytes, query_count):
    suffix = suffix_for_backend(backend)
    setup_file = output_dir / f"{dataset_name}.{suffix}.commands.SETUP.csv"
    print(f"Generating {backend} setup: {setup_file}")
    write_setup_file(setup_file, backend, iter_docs(group_sizes, seed, payload_bytes))

    query_files = {}
    for variant_name, variant in QUERY_VARIANTS.items():
        query_file = output_dir / f"{dataset_name}.{suffix}.commands.BENCH.QUERY_{variant_name}.csv"
        print(f"Generating {backend} query {variant_name}: {query_file}")
        write_query_file(query_file, variant, query_count)
        query_files[variant_name] = str(query_file.name)

    return {
        "setup_file": str(setup_file.name),
        "query_files": query_files,
    }


def parse_backends(value):
    if value == "all":
        return ["hash", "json"]
    return [item.strip() for item in value.split(",") if item.strip()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("./output"))
    parser.add_argument("--doc-count", type=int, default=DEFAULT_DOC_COUNT)
    parser.add_argument("--query-count", type=int, default=DEFAULT_QUERY_COUNT)
    parser.add_argument("--profile", choices=sorted(GROUP_DISTRIBUTIONS), default="heavy")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--payload-bytes", type=int, default=DEFAULT_PAYLOAD_BYTES)
    parser.add_argument("--backends", default="all", help="all, hash, json, or comma-separated list")
    parser.add_argument("--dataset-prefix", default=DATASET_PREFIX)
    args = parser.parse_args()

    backends = parse_backends(args.backends)
    for backend in backends:
        if backend not in {"hash", "json"}:
            raise SystemExit(f"unsupported backend: {backend}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    group_sizes = scale_profile(args.profile, args.doc_count)
    manifest = {
        "dataset_prefix": args.dataset_prefix,
        "doc_count": args.doc_count,
        "query_count": args.query_count,
        "profile": args.profile,
        "group_sizes": dict(zip(ENTITY_NAMES, group_sizes)),
        "seed": args.seed,
        "payload_bytes": args.payload_bytes,
        "key_prefix": KEY_PREFIX,
        "index_name": INDEX_NAME,
        "query_variants": QUERY_VARIANTS,
        "backends": {},
    }

    for backend in backends:
        dataset_name = f"{args.dataset_prefix}-{size_label(args.doc_count)}-{args.profile}-{backend}"
        manifest["backends"][backend] = {
            "dataset_name": dataset_name,
            **generate_backend(
                args.output_dir,
                dataset_name,
                backend,
                group_sizes,
                args.seed,
                args.payload_bytes,
                args.query_count,
            ),
        }

    manifest_file = args.output_dir / f"{args.dataset_prefix}-{size_label(args.doc_count)}-{args.profile}.manifest.json"
    manifest_file.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote manifest: {manifest_file}")


if __name__ == "__main__":
    main()
