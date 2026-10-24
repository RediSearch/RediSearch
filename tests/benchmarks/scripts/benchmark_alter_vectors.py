#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Measure FT.ALTER backfill on private Redis processes; emit one JSON record per trial."""

import argparse
from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import platform
import random
import shutil
import subprocess
import tempfile
import threading
import time

import numpy as np
import redis


INDEX = "alter-bench"
COUNTERS = (
    "search_total_indexing_ops_vector_fields",
    "search_total_relabel_ops_vector_fields",
)


def pairs(reply):
    return {k.decode(): v for k, v in zip(reply[::2], reply[1::2])}


def json_value(value):
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, dict):
        return {json_value(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return value


def index_info(conn):
    return pairs(conn.execute_command("FT.INFO", INDEX))


def wait_for_scan(conn, timeout, poll_interval):
    deadline = time.monotonic() + timeout
    while True:
        info = index_info(conn)
        if int(info["indexing"]) == 0:
            if float(info["percent_indexed"]) != 1:
                raise RuntimeError(f"Background scan stopped incomplete: {info}")
            return info
        if time.monotonic() >= deadline:
            raise TimeoutError("Background scan did not complete")
        time.sleep(poll_interval)


@contextmanager
def server(binary, module, optimize, timeout):
    # Each trial owns its process and Unix socket; no existing Redis is modified.
    with tempfile.TemporaryDirectory(prefix="alter-bench-") as directory:
        socket_path = str(Path(directory) / "redis.sock")
        log_path = Path(directory) / "redis.log"
        with log_path.open("w+") as log:
            proc = subprocess.Popen(
                [binary, "--port", "0", "--unixsocket", socket_path,
                 "--save", "", "--appendonly", "no", "--dir", directory,
                 "--enable-debug-command", "yes",
                 "--loadmodule", str(module), "WORKERS", "0",
                 "OPTIMIZE_PARTIAL_UPDATE", optimize],
                stdout=log, stderr=subprocess.STDOUT,
            )
            conn = redis.Redis(unix_socket_path=socket_path, socket_timeout=timeout)
            try:
                deadline = time.monotonic() + timeout
                while True:
                    if proc.poll() is not None:
                        log.flush()
                        raise RuntimeError(log_path.read_text())
                    try:
                        conn.ping()
                        break
                    except redis.ConnectionError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Redis did not start")
                        time.sleep(0.05)
                yield conn, socket_path
            except Exception as error:
                log.flush()
                detail = log_path.read_text(errors="replace")
                raise RuntimeError(f"Redis trial failed (exit={proc.poll()}):\n{detail}") from error
            finally:
                conn.close()
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()


def vector_schema(field, dim):
    return [field, "VECTOR", "HNSW", 10, "TYPE", "FLOAT32", "DIM", dim,
            "DISTANCE_METRIC", "L2", "M", 16, "EF_CONSTRUCTION", 200]


def load(conn, args):
    schema = ["body", "TEXT"]
    for field in range(args.vector_fields):
        schema.extend(vector_schema(f"vec{field}", args.dim))
    conn.execute_command("FT.CREATE", INDEX, "ON", "HASH", "PREFIX", 1,
                         "doc:", "SCHEMA", *schema)
    rng = np.random.default_rng(args.seed)
    pipe = conn.pipeline(transaction=False)
    for doc in range(args.documents):
        values = {"body": "initial", "added_tag": "present"}
        for field in range(args.vector_fields + (args.scenario == "add-vector")):
            values[f"vec{field}"] = rng.standard_normal(args.dim).astype("<f4").tobytes()
        pipe.hset(f"doc:{doc}", mapping=values)
        if (doc + 1) % 256 == 0:
            pipe.execute()
    pipe.execute()
    wait_for_scan(conn, args.timeout, args.poll_interval)
    if int(index_info(conn)["num_docs"]) != args.documents:
        raise RuntimeError("Initial document count does not match the dataset")


class Writer(threading.Thread):
    def __init__(self, socket_path, args, rate):
        super().__init__()
        self.socket_path, self.args, self.rate = socket_path, args, rate
        self.stop_event = threading.Event()
        self.completed = 0
        self.missed_slots = 0
        self.error = None
        self.first_completion = None

    def run(self):
        rng = random.Random(self.args.seed)
        conn = redis.Redis(unix_socket_path=self.socket_path, socket_timeout=self.args.timeout)
        origin = time.monotonic()
        slot = 0
        try:
            while not self.stop_event.is_set():
                due = origin + slot / self.rate
                if self.stop_event.wait(max(0, due - time.monotonic())):
                    break
                conn.hset(f"doc:{rng.randrange(self.args.documents)}",
                          "body", f"rewrite{self.completed}")
                self.completed += 1
                if self.first_completion is None:
                    self.first_completion = time.perf_counter()
                slot += 1
                # Do not turn missed deadlines into a catch-up burst under overload.
                next_slot = max(slot, math.ceil((time.monotonic() - origin) * self.rate))
                self.missed_slots += next_slot - slot
                slot = next_slot
        except Exception as error:
            self.error = error
        finally:
            conn.close()


def counters(conn):
    info = conn.info("modules")
    # Older/gate-off builds can omit the relabel counter; retain that fact in output.
    if COUNTERS[0] not in info:
        raise RuntimeError(f"Required counter absent: {COUNTERS[0]}")
    return {key: int(info[key]) if key in info else None for key in COUNTERS}


def trial(binary, module, label, args, rate, repetition):
    with server(binary, module, args.optimize_partial_update, args.timeout) as (conn, socket_path):
        load(conn, args)
        before = counters(conn)
        cpu_before = conn.info("cpu")
        memory_before = conn.info("memory")
        writer = Writer(socket_path, args, rate) if rate else None
        added = (vector_schema(f"vec{args.vector_fields}", args.dim)
                 if args.scenario == "add-vector" else ["added_tag", "TAG"])
        start = time.perf_counter()
        conn.execute_command("FT.ALTER", INDEX, "SCHEMA", "ADD", *added)
        command_seconds = time.perf_counter() - start
        if writer:
            writer.start()
        try:
            scan_info = wait_for_scan(conn, args.timeout, args.poll_interval)
            elapsed = time.perf_counter() - start
        finally:
            if writer:
                writer.stop_event.set()
                writer.join()
        if writer and writer.error:
            raise RuntimeError("Concurrent writer failed") from writer.error
        if writer and (writer.first_completion is None or writer.first_completion >= start + elapsed):
            raise RuntimeError("No writer completion overlapped scan observation; increase dataset size")
        after = counters(conn)
        cpu_after = conn.info("cpu")
        memory_after = conn.info("memory")
        if int(scan_info["num_docs"]) != args.documents:
            raise RuntimeError("Document count changed during ALTER")
        if int(scan_info.get("hash_indexing_failures", 0)):
            raise RuntimeError("Indexing failures during ALTER")
        deltas = {key: (after[key] or 0) - (before[key] or 0) for key in COUNTERS}
        if not writer:
            expected_relabels = (args.documents * args.vector_fields
                                 if args.optimize_partial_update == "true" else 0)
            expected_inserts = (args.documents * (args.scenario == "add-vector")
                                + (args.documents * args.vector_fields - expected_relabels))
            if list(deltas.values()) != [expected_inserts, expected_relabels]:
                raise RuntimeError(f"Unexpected vector path counts: {deltas}; expected "
                                   f"{expected_inserts} inserts, {expected_relabels} relabels")
        # Check every vector index's label count after the timed interval.
        fields = args.vector_fields + (args.scenario == "add-vector")
        for field in range(fields):
            info = pairs(conn.execute_command("_FT.DEBUG", "VECSIM_INFO", INDEX, f"vec{field}"))
            if int(info["INDEX_LABEL_COUNT"]) != args.documents:
                raise RuntimeError(f"Wrong vector label count: {info}")
        if args.scenario == "add-tag":
            count = conn.execute_command("FT.SEARCH", INDEX, "@added_tag:{present}",
                                         "LIMIT", 0, 0, "TIMEOUT", 0)[0]
            if int(count) != args.documents:
                raise RuntimeError("Added TAG field is incomplete")
        return {
            "label": label, "module_path": str(module),
            "module_sha256": hashlib.sha256(module.read_bytes()).hexdigest(),
            "redis_version": conn.info("server")["redis_version"],
            "redis_git_sha1": conn.info("server")["redis_git_sha1"],
            "modules": json_value(conn.module_list()),
            "hostname": platform.node(), "scenario": args.scenario,
            "documents": args.documents, "dim": args.dim,
            "preexisting_vector_fields": args.vector_fields, "seed": args.seed,
            "workers": 0, "optimize_partial_update": args.optimize_partial_update,
            "repetition": repetition, "writer_target_ops_per_second": rate,
            "writer_completed": writer.completed if writer else 0,
            "writer_effective_ops_per_second": writer.completed / elapsed if writer else 0,
            "writer_first_completion_seconds": writer.first_completion - start if writer else None,
            "writer_missed_slots": writer.missed_slots if writer else 0,
            "scan_observed_seconds": elapsed, "alter_command_seconds": command_seconds,
            "poll_interval_seconds": args.poll_interval,
            "counters_before": before, "counters_after": after,
            "counter_deltas": deltas,
            "cpu_seconds": {key: cpu_after[key] - cpu_before[key]
                            for key in ("used_cpu_user", "used_cpu_sys")},
            "used_memory_before": memory_before["used_memory"],
            "used_memory_after": memory_after["used_memory"],
            "used_memory_peak": memory_after["used_memory_peak"],
        }


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def positive_float(value):
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("--documents", type=positive_int, default=100_000)
    parser.add_argument("--dim", type=positive_int, default=128)
    parser.add_argument("--vector-fields", type=positive_int, default=1)
    parser.add_argument("--scenario", choices=("add-vector", "add-tag"), default="add-vector")
    parser.add_argument("--writer-rates", type=int, nargs="+", default=[0, 1000])
    parser.add_argument("--repetitions", type=positive_int, default=3)
    parser.add_argument("--seed", type=int, default=18278)
    parser.add_argument("--timeout", type=positive_float, default=600)
    parser.add_argument("--poll-interval", type=positive_float, default=0.01)
    parser.add_argument("--optimize-partial-update", choices=("true", "false"), default="true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.writer_rates) < 0:
        parser.error("writer rates must be non-negative")
    binary = shutil.which(args.redis_server)
    if not binary:
        parser.error("redis-server executable not found")
    modules = []
    for entry in args.module:
        label, separator, path = entry.partition("=")
        if not separator or not label or not Path(path).is_file():
            parser.error(f"expected LABEL=PATH to an existing module, got {entry!r}")
        modules.append((label, Path(path).resolve()))
    # Exclusive creation avoids mixing trials from unrelated invocations.
    with args.output.open("x") as output:
        for repetition in range(args.repetitions):
            for rate in args.writer_rates:
                # Alternate A/B order to reduce systematic warmup/order bias.
                for label, module in modules[::1 if repetition % 2 == 0 else -1]:
                    result = trial(binary, module, label, args, rate, repetition)
                    record = json.dumps(result)
                    output.write(record + "\n")
                    output.flush()
                    print(record, flush=True)


if __name__ == "__main__":
    main()
