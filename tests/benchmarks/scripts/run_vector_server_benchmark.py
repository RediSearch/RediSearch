#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Run ANN vector benchmarks directly against a real Redis server."""

import argparse
import json
import socket
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

import h5py
import numpy as np
import redis

import generate_ann_vector_dataset as dataset_generator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=dataset_generator.DEFAULT_SOURCE)
    parser.add_argument("--module", type=Path, help="Start a temporary Redis with this module")
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--algorithm", choices=("HNSW", "FLAT"), default="HNSW")
    parser.add_argument("--query-type", choices=("knn", "range", "both"), default="both")
    parser.add_argument("--max-vectors", type=int)
    parser.add_argument("--max-queries", type=int, default=10_000)
    parser.add_argument("--load-workers", type=int, default=8)
    parser.add_argument("--query-workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--range-neighbor-rank", type=int, default=100)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def available_port(host: str) -> int:
    with socket.socket() as listener:
        listener.bind((host, 0))
        return int(listener.getsockname()[1])


def client(host: str, port: int) -> redis.Redis:
    return redis.Redis(host=host, port=port, decode_responses=False, socket_timeout=120)


@contextmanager
def redis_server(args: argparse.Namespace) -> Iterator[tuple[str, int, str | None]]:
    if args.module is None:
        if args.port < 1:
            raise ValueError("--port is required when connecting to an existing Redis server")
        yield args.host, args.port, None
        return

    module = args.module.expanduser().resolve()
    if not module.is_file():
        raise FileNotFoundError(f"RediSearch module does not exist: {module}")
    port = args.port or available_port(args.host)
    with tempfile.TemporaryDirectory(prefix="redisearch-vector-server-") as server_dir:
        log_file = tempfile.NamedTemporaryFile(
            prefix="redisearch-vector-server-", suffix=".log", delete=False
        )
        log_path = Path(log_file.name)
        command = [
            args.redis_server,
            "--bind",
            args.host,
            "--port",
            str(port),
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            server_dir,
            "--loadmodule",
            str(module),
        ]
        with log_file as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            redis_client = client(args.host, port)
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        log.flush()
                        raise RuntimeError(
                            f"redis-server exited with {process.returncode}:\n"
                            f"{log_path.read_text(errors='replace')}"
                        )
                    try:
                        if redis_client.ping():
                            break
                    except redis.ConnectionError:
                        time.sleep(0.1)
                else:
                    raise TimeoutError("Timed out waiting for redis-server")
                yield args.host, port, str(log_path)
            finally:
                try:
                    redis_client.shutdown(nosave=True)
                except redis.RedisError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=10)


def pairs_to_dict(reply: list[Any] | dict[Any, Any]) -> dict[str, Any]:
    if isinstance(reply, dict):
        items = reply.items()
    else:
        items = zip(reply[::2], reply[1::2])
    result: dict[str, Any] = {}
    for key, value in items:
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        result[str(key)] = value
    return result


def create_index(redis_client: redis.Redis, algorithm: str, vector_count: int) -> None:
    common = ["TYPE", "FLOAT32", "DIM", 100, "DISTANCE_METRIC", "COSINE"]
    if algorithm == "HNSW":
        parameters = common + [
            "M",
            36,
            "EF_CONSTRUCTION",
            250,
            "EF_RUNTIME",
            300,
            "INITIAL_CAP",
            vector_count,
        ]
    else:
        parameters = common + ["INITIAL_CAP", vector_count]
    redis_client.execute_command(
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        1,
        "glove-server-bench:",
        "SCHEMA",
        "vector",
        "VECTOR",
        algorithm,
        len(parameters),
        *parameters,
    )


def submit_bounded(
    executor: ThreadPoolExecutor,
    pending: set[Future[Any]],
    limit: int,
    function: Callable[..., Any],
    *args: Any,
) -> None:
    pending.add(executor.submit(function, *args))
    if len(pending) >= limit:
        done, still_pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            future.result()
        pending.clear()
        pending.update(still_pending)


def load_vectors(
    host: str,
    port: int,
    train: h5py.Dataset,
    vector_count: int,
    workers: int,
    batch_size: int,
) -> dict[str, float | int]:
    local = threading.local()

    def load_batch(start: int, vectors: np.ndarray) -> None:
        if not hasattr(local, "client"):
            local.client = client(host, port)
        pipeline = local.client.pipeline(transaction=False)
        for offset, vector in enumerate(vectors):
            pipeline.hset(
                f"glove-server-bench:{start + offset}",
                mapping={"vector": vector.tobytes(order="C")},
            )
        pipeline.execute(raise_on_error=True)

    started = time.monotonic()
    pending: set[Future[Any]] = set()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for start in range(0, vector_count, batch_size):
            end = min(start + batch_size, vector_count)
            vectors = np.ascontiguousarray(train[start:end], dtype="<f4")
            submit_bounded(executor, pending, workers * 2, load_batch, start, vectors)
            if end == vector_count or end % 100_000 < batch_size:
                print(f"Queued {end:,}/{vector_count:,} vectors", file=sys.stderr)
        for future in pending:
            future.result()
    elapsed = time.monotonic() - started
    return {
        "vectors": vector_count,
        "seconds": elapsed,
        "vectors_per_second": vector_count / elapsed,
    }


def wait_for_index(redis_client: redis.Redis, vector_count: int) -> dict[str, Any]:
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        info = pairs_to_dict(redis_client.execute_command("FT.INFO", "idx"))
        num_docs = int(info["num_docs"])
        indexing = int(info.get("indexing", 0))
        if num_docs == vector_count and indexing == 0:
            return info
        time.sleep(0.5)
    raise TimeoutError(f"Index did not reach {vector_count} documents")


def percentile_summary(latencies_ms: list[float], result_counts: list[int]) -> dict[str, Any]:
    values = np.asarray(latencies_ms, dtype=np.float64)
    return {
        "requests": len(latencies_ms),
        "p50_ms": float(np.percentile(values, 50)),
        "p95_ms": float(np.percentile(values, 95)),
        "p99_ms": float(np.percentile(values, 99)),
        "mean_ms": float(np.mean(values)),
        "result_count_mean": float(np.mean(result_counts)),
        "result_count_min": min(result_counts),
        "result_count_max": max(result_counts),
    }


def search_result_count(reply: list[Any] | dict[Any, Any]) -> int:
    if isinstance(reply, dict):
        return int(reply.get("total_results", reply.get(b"total_results", 0)))
    return int(reply[0])


def run_queries(
    host: str,
    port: int,
    vectors: np.ndarray,
    workers: int,
    query_builder: Callable[[bytes, int], tuple[Any, ...]],
) -> dict[str, Any]:
    local = threading.local()

    def execute(item: tuple[int, np.ndarray]) -> tuple[float, int]:
        query_id, vector = item
        if not hasattr(local, "client"):
            local.client = client(host, port)
        started = time.perf_counter_ns()
        reply = local.client.execute_command(*query_builder(vector.tobytes(order="C"), query_id))
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        return elapsed_ms, search_result_count(reply)

    warmup_count = min(100, len(vectors))
    for query_id in range(warmup_count):
        execute((query_id, vectors[query_id]))

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(execute, enumerate(vectors)))
    wall_seconds = time.monotonic() - started
    latencies, counts = zip(*results)
    summary = percentile_summary(list(latencies), list(counts))
    summary["wall_seconds"] = wall_seconds
    summary["requests_per_second"] = len(vectors) / wall_seconds
    return summary


def benchmark(args: argparse.Namespace, host: str, port: int) -> dict[str, Any]:
    redis_client = client(host, port)
    redis_client.flushall()
    with tempfile.TemporaryDirectory(prefix="redisearch-ann-source-") as temp_dir:
        source_path = dataset_generator.resolve_source(args.source, Path(temp_dir))
        with h5py.File(source_path, "r") as dataset:
            train, test, distances, vector_count, query_count, dimension = (
                dataset_generator.validate_dataset(
                    dataset, args.max_vectors, args.max_queries, args.range_neighbor_rank
                )
            )
            if dimension != 100:
                raise ValueError(f"Expected 100-dimensional vectors, got {dimension}")
            create_index(redis_client, args.algorithm, vector_count)
            load = load_vectors(
                host,
                port,
                train,
                vector_count,
                args.load_workers,
                args.batch_size,
            )
            index_info = wait_for_index(redis_client, vector_count)
            query_vectors = np.ascontiguousarray(test[:query_count], dtype="<f4")
            results: dict[str, Any] = {
                "algorithm": args.algorithm,
                "dimension": dimension,
                "load": load,
                "vector_index_sz_mb": float(index_info["vector_index_sz_mb"]),
            }

            if args.query_type in ("knn", "both"):

                def knn(blob: bytes, _: int) -> tuple[Any, ...]:
                    return (
                        "FT.SEARCH", "idx", "*=>[KNN 10 @vector $query AS distance]",
                        "PARAMS", 2, "query", blob, "NOCONTENT", "DIALECT", 2, "LIMIT", 0, 10,
                    )

                results["knn"] = run_queries(
                    host, port, query_vectors, args.query_workers, knn
                )

            if args.query_type in ("range", "both"):
                radii = np.asarray(
                    distances[:query_count, args.range_neighbor_rank - 1], dtype=np.float64
                )

                def vector_range(blob: bytes, query_id: int) -> tuple[Any, ...]:
                    return (
                        "FT.SEARCH", "idx", "@vector:[VECTOR_RANGE $radius $query]",
                        "PARAMS", 4, "radius", repr(float(radii[query_id])), "query", blob,
                        "NOCONTENT", "DIALECT", 2, "LIMIT", 0,
                        max(1000, args.range_neighbor_rank * 2),
                    )

                results["range"] = run_queries(
                    host, port, query_vectors, args.query_workers, vector_range
                )

    memory = redis_client.info("memory")
    results["redis_memory"] = {
        "used_memory": int(memory["used_memory"]),
        "used_memory_rss": int(memory["used_memory_rss"]),
    }
    return results


def main() -> int:
    args = parse_args()
    for name in ("max_vectors", "max_queries", "load_workers", "query_workers", "batch_size"):
        value = getattr(args, name)
        if value is not None and value < 1:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")

    with redis_server(args) as (host, port, log_path):
        results = benchmark(args, host, port)
        results["server"] = {"host": host, "port": port, "log": log_path}
    output = json.dumps(results, indent=2, sort_keys=True)
    print(output)
    if args.output:
        args.output.write_text(output + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, redis.RedisError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
