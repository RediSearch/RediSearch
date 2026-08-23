#!/usr/bin/env python3
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""
Generate an FTSB workload for FT.DROPINDEX KEEPDOCS cleanup contention.

The setup stream creates many small HASH indexes and loads documents into their
matching prefixes. The benchmark stream then drops each index with forced
KEEPDOCS semantics and immediately runs a bounded batch of short foreground
Redis writes. This keeps the measured writer workload adjacent to background
index cleanup and avoids a long writer-only tail after all indexes are gone.
"""

import argparse
import csv
from pathlib import Path


DEFAULT_DATASET_NAME = "dropindex-keepdocs-gil-10K-indexes-500-docs"
DEFAULT_INDEX_COUNT = 10_000
DEFAULT_DOCS_PER_INDEX = 500
DEFAULT_WRITES_PER_DROP = 100
DEFAULT_PAYLOAD_BYTES = 64


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"expected a positive integer, got {value!r}")
    return parsed


def non_negative_int(value):
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError(f"expected a non-negative integer, got {value!r}")
    return parsed


def make_payload(index_id, doc_id, payload_bytes):
    if payload_bytes == 0:
        return ""

    token = f"dropindex-gil idx-{index_id} doc-{doc_id} "
    repeats = payload_bytes // len(token) + 1
    return (token * repeats)[:payload_bytes]


def write_setup_file(output_file, index_count, docs_per_index, payload_bytes):
    with output_file.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

        for index_id in range(1, index_count + 1):
            writer.writerow(
                [
                    "WRITE",
                    "create-index",
                    "1",
                    "FT.CREATE",
                    f"idx:{index_id}",
                    "ON",
                    "HASH",
                    "PREFIX",
                    "1",
                    f"doc:{index_id}:",
                    "SCHEMA",
                    "body",
                    "TEXT",
                ]
            )

        for index_id in range(1, index_count + 1):
            for doc_id in range(1, docs_per_index + 1):
                writer.writerow(
                    [
                        "WRITE",
                        "load-doc",
                        "1",
                        "HSET",
                        f"doc:{index_id}:{doc_id}",
                        "body",
                        make_payload(index_id, doc_id, payload_bytes),
                    ]
                )


def write_benchmark_file(output_file, index_count, writes_per_drop):
    with output_file.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

        for index_id in range(1, index_count + 1):
            writer.writerow(
                [
                    "WRITE",
                    "drop-index",
                    "1",
                    "FT._DROPINDEXIFX",
                    f"idx:{index_id}",
                    "_FORCEKEEPDOCS",
                ]
            )

            for write_id in range(1, writes_per_drop + 1):
                writer.writerow(
                    [
                        "WRITE",
                        "foreground-write",
                        "1",
                        "INCR",
                        f"writer:{index_id}:{write_id}",
                    ]
                )


def generate_files(
    output_dir,
    dataset_name,
    index_count,
    docs_per_index,
    writes_per_drop,
    payload_bytes,
):
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_file = output_dir / f"{dataset_name}.redisearch.commands.SETUP.csv"
    benchmark_file = output_dir / f"{dataset_name}.redisearch.commands.BENCH.csv"

    write_setup_file(setup_file, index_count, docs_per_index, payload_bytes)
    write_benchmark_file(benchmark_file, index_count, writes_per_drop)

    return setup_file, benchmark_file


def main():
    parser = argparse.ArgumentParser(
        description="Generate RediSearch FT.DROPINDEX KEEPDOCS writer contention benchmark data."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("./output"))
    parser.add_argument("--dataset-name", default=DEFAULT_DATASET_NAME)
    parser.add_argument("--index-count", type=positive_int, default=DEFAULT_INDEX_COUNT)
    parser.add_argument("--docs-per-index", type=positive_int, default=DEFAULT_DOCS_PER_INDEX)
    parser.add_argument("--writes-per-drop", type=positive_int, default=DEFAULT_WRITES_PER_DROP)
    parser.add_argument("--payload-bytes", type=non_negative_int, default=DEFAULT_PAYLOAD_BYTES)
    args = parser.parse_args()

    setup_file, benchmark_file = generate_files(
        args.output_dir,
        args.dataset_name,
        args.index_count,
        args.docs_per_index,
        args.writes_per_drop,
        args.payload_bytes,
    )

    setup_commands = args.index_count + (args.index_count * args.docs_per_index)
    benchmark_commands = args.index_count * (args.writes_per_drop + 1)

    print("Dataset generation complete")
    print(f"  Dataset: {args.dataset_name}")
    print(f"  Indexes: {args.index_count:,}")
    print(f"  Documents: {args.index_count * args.docs_per_index:,}")
    print(f"  Setup commands: {setup_commands:,}")
    print(f"  Benchmark commands: {benchmark_commands:,}")
    print(f"  Setup file: {setup_file}")
    print(f"  Benchmark file: {benchmark_file}")
    print(
        "  Upload: "
        f"aws s3 cp {args.output_dir}/ "
        f"s3://benchmarks.redislabs/redisearch/datasets/{args.dataset_name}/ --recursive"
    )


if __name__ == "__main__":
    main()
