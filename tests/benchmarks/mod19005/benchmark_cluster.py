#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
"""Alternating baseline/candidate measurements on an existing six-shard cluster.

The caller supplies the cluster control script and its working directory. The
control script must implement `stop` and `start <module-path>`. This runner
verifies module paths, row-block configuration, cluster health and full results;
it records ordinary-query latency and runs FT.PROFILE separately.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys
import time

import redis


def digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def connected_clients(ports):
    return [redis.Redis(host="127.0.0.1", port=port, decode_responses=True,
                        socket_timeout=130) for port in ports]


def control(args, *extra):
    command = [sys.executable, str(args.cluster_control), *extra]
    subprocess.run(command, cwd=args.cluster_workdir, check=True)


def check_cluster(clients, module_path, expected_docs):
    module_path = str(module_path.resolve())
    states = []
    for client in clients:
        modules = client.execute_command("MODULE", "LIST")
        if module_path not in str(modules):
            raise RuntimeError(f"Expected module {module_path}, loaded modules: {modules}")
        state = client.cluster("info").get("cluster_state")
        if state != "ok":
            raise RuntimeError(f"Cluster is unhealthy: {state}")
        setting = client.config_get("search-internal-row-block-format")
        value = setting.get("search-internal-row-block-format")
        if str(value).lower() not in ("yes", "1", "true"):
            raise RuntimeError(f"Row-block default/config is not enabled: {setting}")
        states.append({"module": modules, "rowblock_setting": value,
                       "dbsize": client.dbsize(), "cluster_state": state})
    info = clients[0].execute_command("FT.INFO", "idx:bench")
    info = dict(zip(info[::2], info[1::2]))
    if int(info["num_docs"]) != expected_docs or int(info["indexing"]) != 0:
        raise RuntimeError(f"Unexpected index state: {info}")
    if sum(item["dbsize"] for item in states) != expected_docs + 1:
        raise RuntimeError(f"Unexpected cluster key count: {states}")
    return states


def metrics(clients):
    result = []
    for client in clients:
        cpu = client.info("cpu")
        memory = client.info("memory")
        stats = client.info("stats")
        result.append({
            "port": client.connection_pool.connection_kwargs["port"],
            "cpu_sys_s": cpu.get("used_cpu_sys"),
            "cpu_user_s": cpu.get("used_cpu_user"),
            "used_memory_bytes": memory.get("used_memory"),
            "used_memory_rss_bytes": memory.get("used_memory_rss"),
            "total_net_output_bytes": stats.get("total_net_output_bytes"),
        })
    return result


def as_map(flat):
    if not isinstance(flat, list) or len(flat) % 2:
        raise RuntimeError(f"Expected flat map, got {type(flat).__name__}")
    return dict(zip(flat[::2], flat[1::2]))


def result_rows(reply, expected_total_groups):
    if not isinstance(reply, list) or len(reply) != 401:
        size = len(reply) - 1 if isinstance(reply, list) and reply else "invalid"
        raise RuntimeError(f"Expected 400 returned groups, got {size}")
    if int(reply[0]) != int(expected_total_groups):
        raise RuntimeError(f"Total group count {reply[0]} != reference {expected_total_groups}")
    rows = {}
    for row in reply[1:]:
        if not isinstance(row, list) or len(row) % 2:
            raise RuntimeError("Malformed aggregate result row")
        fields = as_map(row)
        key = (fields["parent_id"], fields["profile_id"])
        if key in rows:
            raise RuntimeError(f"Duplicate output group key: {key}")
        rows[key] = fields
    return rows


def equivalent_rows(actual, expected):
    if actual.keys() != expected.keys():
        return False
    for key in actual:
        if actual[key].keys() != expected[key].keys():
            return False
        for field, value in actual[key].items():
            target = expected[key][field]
            if isinstance(value, list) and isinstance(target, list):
                if sorted(value) != sorted(target):
                    return False
            elif value == target:
                continue
            else:
                if field not in ("the_score", "bm25_score", "vector_distance", "created_at"):
                    return False
                try:
                    if not math.isclose(float(value), float(target),
                                        rel_tol=1e-10, abs_tol=1e-12):
                        return False
                except (TypeError, ValueError):
                    return False
    return True


def check_profile(profile):
    if not isinstance(profile, list) or len(profile) != 2:
        raise RuntimeError("Unexpected FT.PROFILE reply shape")
    data = as_map(profile[1])
    coordinator = as_map(data["Coordinator"])
    if coordinator.get("Warning") != ["None"]:
        raise RuntimeError(f"Coordinator profile warning: {coordinator.get('Warning')}")
    shards = data["Shards"]
    if not shards:
        raise RuntimeError("FT.PROFILE returned no shard profile entries")
    for shard in shards:
        if as_map(shard).get("Warning") != ["None"]:
            raise RuntimeError("A shard profile reported a warning")


def subtract(after, before, key):
    if after is None or before is None:
        return None
    return after - before


def host_cpu_busy():
    fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    values = [int(value) for value in fields[:8]]
    idle = values[3] + values[4]
    return sum(values), idle


def wait_for_host_quiet(window_s, threshold, timeout_s):
    deadline = time.monotonic() + timeout_s
    observations = []
    while time.monotonic() < deadline:
        total_before, idle_before = host_cpu_busy()
        time.sleep(window_s)
        total_after, idle_after = host_cpu_busy()
        total_delta = total_after - total_before
        busy = (total_delta - (idle_after - idle_before)) / total_delta if total_delta else 0
        observations.append({"window_s": window_s, "busy_fraction": busy})
        if busy <= threshold:
            return observations
    raise RuntimeError(f"Host did not remain below {threshold:.0%} CPU busy: {observations}")


def query_arm(args, arm, module_path, query, baseline_result, round_id):
    control(args, "stop")
    control(args, "start", str(module_path.resolve()))
    clients = connected_clients(args.ports)
    try:
        cluster_metadata = check_cluster(clients, module_path, args.expected_docs)
        coordinator = clients[0]
        reply = coordinator.execute_command(*query)
        canonical = result_rows(reply, args.total_groups)
        if baseline_result is not None and not equivalent_rows(canonical, baseline_result):
            raise RuntimeError(f"Result differs from baseline: arm={arm}, round={round_id}")
        for _ in range(args.warmup):
            if not equivalent_rows(result_rows(coordinator.execute_command(*query), args.total_groups), baseline_result):
                raise RuntimeError("Warmup result mismatch")
        host_quiet = wait_for_host_quiet(args.quiet_window_seconds,
                                         args.quiet_threshold, args.quiet_timeout_seconds)
        before = metrics(clients)
        samples_ms = []
        last_reply = None
        for _ in range(args.queries):
            start_ns = time.perf_counter_ns()
            last_reply = coordinator.execute_command(*query)
            samples_ms.append((time.perf_counter_ns() - start_ns) / 1_000_000)
            if not equivalent_rows(result_rows(last_reply, args.total_groups), baseline_result):
                raise RuntimeError("Measured result mismatch")
        after = metrics(clients)
        if not equivalent_rows(result_rows(last_reply, args.total_groups), canonical):
            raise RuntimeError("Result changed during the timing batch")

        # Run profile once after timing. It is diagnostic output and is excluded
        # from the ordinary latency samples above.
        profile = coordinator.execute_command(
            "FT.PROFILE", query[1], "AGGREGATE", "LIMITED", "QUERY", *query[2:])
        check_profile(profile)
        if not equivalent_rows(result_rows(profile[0], args.total_groups), baseline_result):
            raise RuntimeError("Profile result mismatch")
        profile_path = args.output.parent / f"{args.output.stem}-r{round_id}-{arm}-profile.json"
        profile_path.write_text(json.dumps(profile, indent=2) + "\n")

        row = {
            "round": round_id,
            "arm": arm,
            "module_path": str(module_path.resolve()),
            "module_sha256": digest(module_path),
            "cluster": cluster_metadata,
            "queries": args.queries,
            "samples_ms": samples_ms,
            "mean_ms": statistics.mean(samples_ms),
            "p50_ms": statistics.median(samples_ms),
            "min_ms": min(samples_ms),
            "max_ms": max(samples_ms),
            "complete_400_groups_verified": True,
            "result_matches_baseline": baseline_result is None or equivalent_rows(canonical, baseline_result),
            "metrics_before": before,
            "metrics_after": after,
            "host_quiet_check": {
                "window_s": args.quiet_window_seconds,
                "max_busy_fraction": args.quiet_threshold,
                "observations": host_quiet,
            },
            "cpu_delta_s": [
                {"port": a["port"],
                 "sys_s": subtract(a["cpu_sys_s"], b["cpu_sys_s"], "cpu_sys_s"),
                 "user_s": subtract(a["cpu_user_s"], b["cpu_user_s"], "cpu_user_s")}
                for a, b in zip(after, before)],
            "memory_after_minus_before_bytes": [
                {"port": a["port"],
                 "used_memory": subtract(a["used_memory_bytes"], b["used_memory_bytes"],
                                          "used_memory_bytes"),
                 "used_memory_rss": subtract(a["used_memory_rss_bytes"],
                                              b["used_memory_rss_bytes"],
                                              "used_memory_rss_bytes")}
                for a, b in zip(after, before)],
            "memory_measurement": "before/after only; not peak",
            "wire_bytes_delta_per_node": [
                {"port": a["port"],
                 "bytes": subtract(a["total_net_output_bytes"],
                                   b["total_net_output_bytes"], "total_net_output_bytes")}
                for a, b in zip(after, before)],
            "profile_output": str(profile_path),
        }
        return row, canonical
    finally:
        for client in clients:
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster-control", type=Path, required=True,
                        help="script accepting `stop` and `start <module-path>`")
    parser.add_argument("--cluster-workdir", type=Path, required=True,
                        help="working directory for the supplied cluster-control script")
    parser.add_argument("--ports", required=True,
                        help="comma-separated Redis node ports, coordinator first")
    parser.add_argument("--baseline-module", type=Path, required=True)
    parser.add_argument("--candidate-module", type=Path, required=True)
    parser.add_argument("--query", type=Path, required=True,
                        help="JSON array containing the complete FT.AGGREGATE command")
    parser.add_argument("--reference-rows", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True,
                        help="raw JSON output; keep outside the repository")
    parser.add_argument("--expected-docs", type=int, default=5_700_000)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--queries", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--quiet-window-seconds", type=float, default=3.0)
    parser.add_argument("--quiet-threshold", type=float, default=0.20)
    parser.add_argument("--quiet-timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()
    args.cluster_control = args.cluster_control.resolve()
    args.cluster_workdir = args.cluster_workdir.resolve()
    args.ports = [int(value) for value in args.ports.split(",")]
    if not args.cluster_control.is_file() or not args.cluster_workdir.is_dir():
        parser.error("cluster-control and cluster-workdir must exist")
    if (len(args.ports) < 2 or args.rounds < 1 or args.queries < 5 or args.warmup < 5
            or args.quiet_window_seconds <= 0 or not 0 < args.quiet_threshold < 1
            or args.quiet_timeout_seconds <= 0):
        parser.error("need >=2 ports, >=1 round, >=5 timed queries and warmups, plus valid quiet settings")
    arms = {"baseline": args.baseline_module.resolve(),
            "candidate": args.candidate_module.resolve()}
    query = json.loads(args.query.read_text())
    if not isinstance(query, list) or len(query) < 3 or query[0] != "FT.AGGREGATE":
        parser.error("--query must contain a complete FT.AGGREGATE command array")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "cluster_control": str(args.cluster_control),
        "cluster_workdir": str(args.cluster_workdir),
        "ports": args.ports,
        "query": query,
        "expected_docs": args.expected_docs,
        "rounds": args.rounds,
        "timed_queries_per_arm_round": args.queries,
        "warmup_queries": args.warmup,
        "rowblock_required": "yes on every node",
        "runs": [],
    }
    reference = json.loads(args.reference_rows.read_text())
    args.total_groups = reference[0]
    baseline_result = result_rows(reference, args.total_groups)
    for round_id in range(args.rounds):
        order = ["baseline", "candidate"] if round_id % 2 == 0 else ["candidate", "baseline"]
        for arm in order:
            row, canonical = query_arm(args, arm, arms[arm], query, baseline_result, round_id)
            if arm == "baseline" and baseline_result is None:
                baseline_result = canonical
            result["runs"].append(row)
            result["completed_utc"] = datetime.now(timezone.utc).isoformat()
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            print(f"round={round_id} {arm} p50={row['p50_ms']:.3f}ms "
                  f"cpu_sys={sum(x['sys_s'] or 0 for x in row['cpu_delta_s']):.3f}s "
                  f"profile={row['profile_output']}", flush=True)
    result["completed_utc"] = datetime.now(timezone.utc).isoformat()
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
