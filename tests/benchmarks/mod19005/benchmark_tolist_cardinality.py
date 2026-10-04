#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
"""Compare TOLIST value cardinalities in serial standalone Redis runs.

The default four cases are tiny groups, duplicate-heavy short/long values, and
many distinct values. Three additional cases guard the inline/promotion limits
and large-group behavior. Timings include client reply parsing. CPU and memory
are Redis INFO snapshots around the complete timing batch; memory is explicitly
not a peak measurement.
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


CASES = {
    # name: (rows per group, distinct values per group, value bytes)
    "tiny_1": (1, 1, 16),
    "tiny_4": (4, 4, 16),
    "duplicates_16_short": (256, 16, 16),
    "duplicates_16_long": (256, 16, 512),
    "duplicates_17_long": (256, 17, 512),
    "distinct_256": (256, 256, 16),
    "distinct_4096": (4096, 4096, 16),
}
DEFAULT_CASES = ["tiny_4", "duplicates_16_short", "duplicates_16_long", "distinct_256"]
EXTENDED_CASES = ["tiny_1", "duplicates_17_long", "distinct_4096"]


def digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


@contextmanager
def server(executable, module):
    with tempfile.TemporaryDirectory(prefix="mod19005-tolist-") as directory:
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


def load_case(client, rows, case_name):
    group_rows, distinct, width = CASES[case_name]
    if rows % group_rows:
        raise ValueError(f"rows={rows} must be divisible by {group_rows} for {case_name}")
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


def redis_metrics(client):
    cpu = client.info("cpu")
    memory = client.info("memory")
    return {
        "cpu_sys_s": cpu.get("used_cpu_sys"),
        "cpu_user_s": cpu.get("used_cpu_user"),
        "cpu_sys_main_thread_s": cpu.get("used_cpu_sys_main_thread"),
        "cpu_user_main_thread_s": cpu.get("used_cpu_user_main_thread"),
        "used_memory_bytes": memory.get("used_memory"),
        "used_memory_rss_bytes": memory.get("used_memory_rss"),
    }


def rowblock_config(client):
    try:
        values = client.config_get("search-internal-row-block-format")
        value = values.get("search-internal-row-block-format")
        return {"value": value, "source": "CONFIG GET" if value is not None else "not returned"}
    except redis.RedisError as error:
        return {"value": None, "source": "unavailable", "error": str(error)}


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


def delta(after, before, key):
    if after.get(key) is None or before.get(key) is None:
        return None
    return after[key] - before[key]


def median_if_values(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def pct_change(candidate, baseline):
    if baseline in (None, 0) or candidate is None:
        return None
    return 100 * (candidate / baseline - 1)


def compact_summary(result):
    """Return per-arm summaries and paired per-round deltas, without raw samples."""
    grouped = {}
    for run in result["runs"]:
        grouped.setdefault((run["arm"], run["case"]), []).append(run)
    arms = []
    for (arm, case), runs in sorted(grouped.items()):
        arms.append({
            "arm": arm,
            "case": case,
            "rounds": len(runs),
            "median_round_p50_ms": statistics.median(run["p50_ms"] for run in runs),
            "median_round_cpu_sys_s": median_if_values(
                [run["cpu_delta"]["sys_s"] for run in runs]),
            "median_round_cpu_user_s": median_if_values(
                [run["cpu_delta"]["user_s"] for run in runs]),
            "median_round_used_memory_change_bytes": median_if_values(
                [run["memory_delta"]["used_memory_bytes"] for run in runs]),
            "median_round_rss_change_bytes": median_if_values(
                [run["memory_delta"]["used_memory_rss_bytes"] for run in runs]),
        })
    paired = []
    pair_keys = sorted({run["case"] for run in result["runs"]})
    for case in pair_keys:
        by_round = {}
        for run in result["runs"]:
            if run["case"] == case:
                by_round.setdefault(run["repetition"], {})[run["arm"]] = run
        for round_id, pair in sorted(by_round.items()):
            if "baseline" not in pair or "candidate" not in pair:
                continue
            base = pair["baseline"]
            cand = pair["candidate"]
            paired.append({
                "case": case,
                "repetition": round_id,
                "p50_change_pct": pct_change(cand["p50_ms"], base["p50_ms"]),
                "cpu_sys_change_pct": pct_change(
                    cand["cpu_delta"]["sys_s"], base["cpu_delta"]["sys_s"]),
                "cpu_user_change_pct": pct_change(
                    cand["cpu_delta"]["user_s"], base["cpu_delta"]["user_s"]),
            })
    return {
        "started_utc": result["started_utc"],
        "completed_utc": result.get("completed_utc"),
        "scope": result["scope"],
        "rowblock": result["rowblock"],
        "binaries": result["binaries"],
        "case_definitions": result["case_definitions"],
        "arms": arms,
        "paired_round_changes": paired,
        "memory_caveat": "INFO snapshots before and after each batch; these are not peak measurements.",
    }


def write_results(raw_path, summary_path, result):
    raw_path.write_text(json.dumps(result, indent=2) + "\n")
    if summary_path:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(compact_summary(result), indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-server", type=Path, required=True)
    parser.add_argument("--baseline-module", type=Path, required=True)
    parser.add_argument("--candidate-module", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True,
                        help="full raw JSON output; keep this outside the repository")
    parser.add_argument("--summary-output", type=Path,
                        help="optional compact JSON summary suitable for check-in")
    parser.add_argument("--cases", default=",".join(DEFAULT_CASES),
                        help="comma-separated case names, or all (default: four primary cases)")
    parser.add_argument("--rows", type=int, default=32768)
    parser.add_argument("--repetitions", type=int, default=4)
    parser.add_argument("--samples", type=int, default=40)
    parser.add_argument("--warmup-queries", type=int, default=5)
    args = parser.parse_args()
    if args.rows < 4096 or args.rows % 4096:
        parser.error("--rows must be a positive multiple of 4096")
    if args.samples < 2 or args.repetitions < 1 or args.warmup_queries < 0:
        parser.error("--samples must be >= 2, --repetitions >= 1, and warmups >= 0")
    cases = list(CASES) if args.cases == "all" else [name.strip() for name in args.cases.split(",")]
    unknown = [name for name in cases if name not in CASES]
    if not cases or unknown:
        parser.error(f"invalid --cases selection; unknown={unknown}; available={list(CASES)}")
    paths = {"redis_server": args.redis_server.resolve(),
             "baseline": args.baseline_module.resolve(),
             "candidate": args.candidate_module.resolve()}
    result = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(), "python": platform.python_version(),
        "redis_py": redis.__version__, "rows": args.rows,
        "repetitions": args.repetitions, "samples_per_round": args.samples,
        "warmup_queries": args.warmup_queries,
        "scope": "Synthetic standalone aggregate; client elapsed includes full reply decoding",
        "transport": {"row_block_transport_active": False,
                      "note": "Standalone runs do not exercise shard-to-coordinator row-block transport."},
        "p99_caveat": "Small-sample descriptive percentile, not robust tail evidence",
        "binaries": {name: {"path": str(path), "sha256": digest(path)}
                     for name, path in paths.items()},
        "case_definitions": [{"name": name, "rows_per_group": CASES[name][0],
                              "distinct_per_group": CASES[name][1],
                              "value_bytes": CASES[name][2],
                              "role": "primary" if name in DEFAULT_CASES else "extended guardrail"}
                             for name in cases],
        "rowblock": None,
        "runs": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for repetition in range(args.repetitions):
        order = ["baseline", "candidate"] if repetition % 2 == 0 else ["candidate", "baseline"]
        for arm in order:
            with server(paths["redis_server"], paths[arm]) as client:
                runtime = {"redis": client.info("server")["redis_version"],
                           "modules": client.execute_command("MODULE", "LIST")}
                setting = rowblock_config(client)
                if result["rowblock"] is None:
                    result["rowblock"] = {
                        "standalone_transport_active": False,
                        "setting_per_arm": {},
                    }
                result["rowblock"]["setting_per_arm"][arm] = setting
                for case_name in cases:
                    command, groups, values = load_case(client, args.rows, case_name)
                    validate(client.execute_command(*command), groups, values)
                    for _ in range(args.warmup_queries):
                        client.execute_command(*command)
                    before = redis_metrics(client)
                    samples = []
                    batch_start = time.perf_counter_ns()
                    for _ in range(args.samples):
                        start = time.perf_counter_ns()
                        reply = client.execute_command(*command)
                        samples.append((time.perf_counter_ns() - start) / 1_000_000)
                    batch_wall_s = (time.perf_counter_ns() - batch_start) / 1_000_000_000
                    after = redis_metrics(client)
                    validate(reply, groups, values)
                    cpu_delta = {
                        "sys_s": delta(after, before, "cpu_sys_s"),
                        "user_s": delta(after, before, "cpu_user_s"),
                        "sys_main_thread_s": delta(after, before, "cpu_sys_main_thread_s"),
                        "user_main_thread_s": delta(after, before, "cpu_user_main_thread_s"),
                    }
                    memory_delta = {
                        key: delta(after, before, key)
                        for key in ("used_memory_bytes", "used_memory_rss_bytes")
                    }
                    run = {
                        "repetition": repetition, "arm": arm, "case": case_name,
                        "runtime": runtime, "rowblock_setting": setting,
                        "command": command, "complete_results_verified": True,
                        "samples_ms": samples, **summarize(samples),
                        "timing_batch": {
                            "queries": args.samples,
                            "wall_s": batch_wall_s,
                            "server_metrics_before": before,
                            "server_metrics_after": after,
                            "server_cpu_delta_s": cpu_delta,
                            "memory_delta_bytes": memory_delta,
                            "memory_is_peak": False,
                        },
                        # Flat aliases simplify summary consumption while preserving the snapshots.
                        "cpu_delta": {"sys_s": cpu_delta["sys_s"],
                                      "user_s": cpu_delta["user_s"]},
                        "memory_delta": memory_delta,
                    }
                    result["runs"].append(run)
                    write_results(args.output, args.summary_output, result)
                    cpu_total = sum(value for value in (cpu_delta["sys_s"], cpu_delta["user_s"])
                                    if value is not None)
                    print(f"round={repetition} {arm} {case_name} "
                          f"p50={run['p50_ms']:.3f}ms cpu="
                          f"{cpu_total:.3f}s "
                          f"memoryΔ={memory_delta['used_memory_bytes']}B", flush=True)
    result["completed_utc"] = datetime.now(timezone.utc).isoformat()
    write_results(args.output, args.summary_output, result)


if __name__ == "__main__":
    main()
