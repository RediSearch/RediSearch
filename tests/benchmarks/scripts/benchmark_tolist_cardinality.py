#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
"""Compare TOLIST cardinalities on two modules in serial, standalone Redis runs.

Requires redis-py. Example:
  python benchmark_tolist_cardinality.py --redis-server /path/redis-server \
    --baseline-module /path/base.so --candidate-module /path/candidate.so \
    --output /tmp/tolist.json

This synthetic benchmark measures client elapsed time, including complete replies
and redis-py parsing. It does not model distributed fan-in or establish production
tail latency: p99 from the small default sample is descriptive only. Each round
alternates module order; no servers or measured queries run concurrently.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time

import redis


CASES = [
    # name, rows per group, distinct values per group, value bytes
    ("tiny_1", 1, 1, 16),
    ("tiny_4", 4, 4, 16),
    ("duplicates_16_short", 256, 16, 16),
    ("duplicates_16_long", 256, 16, 512),
    ("duplicates_17_long", 256, 17, 512),
    ("distinct_256", 256, 256, 16),
    ("distinct_4096", 4096, 4096, 16),
]


def digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


@contextmanager
def server(executable, module):
    with tempfile.TemporaryDirectory(prefix="tolist-") as directory:
        socket = str(Path(directory) / "redis.sock")
        with open(Path(directory) / "redis.log", "w+") as log:
            process = subprocess.Popen(
                [str(executable), "--port", "0", "--unixsocket", socket,
                 "--unixsocketperm", "700", "--save", "", "--appendonly", "no",
                 "--dir", directory, "--loadmodule", str(module)],
                stdout=log, stderr=subprocess.STDOUT,
            )
            client = redis.Redis(unix_socket_path=socket, decode_responses=True,
                                 socket_timeout=120)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None or time.monotonic() > deadline:
                        log.seek(0)
                        raise RuntimeError("Redis startup failed:\n" + log.read())
                    try:
                        if client.ping():
                            break
                    except redis.ConnectionError:
                        time.sleep(0.05)
                yield client
            finally:
                client.close()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


def load_case(client, rows, case):
    _, group_rows, distinct, width = case
    client.flushall()
    client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1,
                           "doc:", "SCHEMA", "group", "TAG", "SORTABLE",
                           "value", "TAG", "SORTABLE")
    values = ["x" * (width - 8) + f"{i:08d}" for i in range(distinct)]
    pipeline = client.pipeline(transaction=False)
    for row in range(rows):
        pipeline.hset(f"doc:{row}", mapping={
            "group": f"g{row // group_rows}",
            "value": values[(row % group_rows) % distinct],
        })
        if (row + 1) % 1024 == 0:
            pipeline.execute()
    pipeline.execute()
    expected_groups = rows // group_rows
    deadline = time.monotonic() + 30
    while True:
        info = client.execute_command("FT.INFO", "idx")
        info = dict(zip(info[::2], info[1::2]))
        if int(info["num_docs"]) == rows and int(info["indexing"]) == 0:
            break
        if time.monotonic() > deadline:
            raise RuntimeError(f"Index did not finish loading: {info}")
        time.sleep(0.05)
    command = ["FT.AGGREGATE", "idx", "*", "GROUPBY", 1, "@group",
               "REDUCE", "TOLIST", 1, "@value", "AS", "values",
               "LIMIT", 0, expected_groups, "TIMEOUT", 0, "DIALECT", 2]
    return command, expected_groups, set(values)


def validate(reply, groups, values):
    if reply[0] != groups or len(reply) != groups + 1:
        raise AssertionError(f"Incomplete group count: {reply[0]}, expected {groups}")
    seen = set()
    for row in reply[1:]:
        fields = dict(zip(row[::2], row[1::2]))
        group = fields["group"]
        actual = fields["values"]
        if group in seen or len(actual) != len(values) or set(actual) != values:
            raise AssertionError(f"Unexpected group or distinct values: {group}")
        seen.add(group)
    if seen != {f"g{i}" for i in range(groups)}:
        raise AssertionError("Unexpected group identities")


def percentile(samples, fraction):
    ordered = sorted(samples)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(samples):
    return {"samples": len(samples), "mean_ms": statistics.mean(samples),
            "p50_ms": percentile(samples, 0.5), "p99_ms": percentile(samples, 0.99),
            "min_ms": min(samples), "max_ms": max(samples)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-server", type=Path, required=True)
    parser.add_argument("--baseline-module", type=Path, required=True)
    parser.add_argument("--candidate-module", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=32768)
    parser.add_argument("--repetitions", type=int, default=4)
    parser.add_argument("--samples", type=int, default=40)
    args = parser.parse_args()
    if args.rows < 4096 or args.rows % 4096:
        parser.error("--rows must be a positive multiple of 4096")
    if args.samples < 2 or args.repetitions < 1:
        parser.error("--samples must be >= 2 and --repetitions must be >= 1")
    paths = {"redis_server": args.redis_server.resolve(),
             "baseline": args.baseline_module.resolve(),
             "candidate": args.candidate_module.resolve()}
    result = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(), "python": platform.python_version(),
        "redis_py": redis.__version__, "rows": args.rows,
        "repetitions": args.repetitions, "samples_per_round": args.samples,
        "warmup_queries": 5,
        "scope": "Synthetic standalone, client elapsed including full reply decoding",
        "p99_caveat": "Small-sample descriptive percentile, not robust tail evidence",
        "binaries": {name: {"path": str(path), "sha256": digest(path)}
                     for name, path in paths.items()},
        "cases": [{"name": name, "rows_per_group": size,
                   "distinct_per_group": unique, "value_bytes": width}
                  for name, size, unique, width in CASES],
        "runs": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for repetition in range(args.repetitions):
        order = ["baseline", "candidate"] if repetition % 2 == 0 else ["candidate", "baseline"]
        for arm in order:
            with server(paths["redis_server"], paths[arm]) as client:
                runtime = {"redis": client.info("server")["redis_version"],
                           "modules": client.execute_command("MODULE", "LIST")}
                for case in CASES:
                    command, groups, values = load_case(client, args.rows, case)
                    validate(client.execute_command(*command), groups, values)
                    for _ in range(result["warmup_queries"]):
                        client.execute_command(*command)
                    samples = []
                    for _ in range(args.samples):
                        start = time.perf_counter_ns()
                        reply = client.execute_command(*command)
                        samples.append((time.perf_counter_ns() - start) / 1_000_000)
                    validate(reply, groups, values)
                    run = {"repetition": repetition, "arm": arm, "case": case[0],
                           "runtime": runtime, "command": command,
                           "complete_results_verified": True, "samples_ms": samples,
                           **summarize(samples)}
                    result["runs"].append(run)
                    args.output.write_text(json.dumps(result, indent=2) + "\n")
                    print(f"round={repetition} {arm} {case[0]} "
                          f"p50={run['p50_ms']:.3f}ms p99={run['p99_ms']:.3f}ms", flush=True)
    result["completed_utc"] = datetime.now(timezone.utc).isoformat()
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
