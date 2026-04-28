"""Benchmark driver.

For each (version, topology, query_type, index_size, timeout_ms) cell, run N
queries (filter='*') and record per-query: success/timeout, client-observed
latency in ms. Appends rows to a single CSV.
"""
from __future__ import annotations

import csv
import os
import sys
import time
from dataclasses import dataclass

import redis
from redis.cluster import ClusterNode

sys.path.insert(0, os.path.dirname(__file__))
from harness import Cluster  # noqa: E402
from loader import (  # noqa: E402
    INDEX, create_index_sa, create_index_cluster, load_sa, load_cluster,
    wait_indexed, wait_indexed_cluster,
)


MASTER_SO = "/home/ubuntu/RediSearch/bin/linux-x64-release/search-community/redisearch.so"
OLD_SO = "/home/ubuntu/RediSearch-v8.6.0/bin/linux-x64-release/search-community/redisearch.so"


@dataclass
class Cell:
    version: str     # "old" (8.6) or "new" (master)
    topology: str    # "sa" or "cluster3" / "cluster5" / "cluster7"
    query_type: str  # "SEARCH" | "AGGREGATE"
    index_size: int
    timeout_ms: int


VERSIONS = {"old": OLD_SO, "new": MASTER_SO}


# `ON_TIMEOUT FAIL` is set at module-load time via harness (works in both SA
# and cluster mode on both versions). `FT.CONFIG` is disabled in cluster mode
# so runtime setting is not portable.


def build_query(query_type: str, index_size: int, timeout_ms: int) -> list:
    # We want queries that actually do enough work to exceed sub-second
    # timeouts. `*` alone is cheap, so we force per-doc loads:
    #   SEARCH    -> drop NOCONTENT; cap LIMIT so reply size stays bounded.
    #   AGGREGATE -> LOAD 2 title body; LIMIT spans the whole index.
    if query_type == "SEARCH":
        limit = min(index_size, 10_000)
        return ["FT.SEARCH", INDEX, "*",
                "LIMIT", "0", str(limit),
                "TIMEOUT", str(timeout_ms)]
    if query_type == "AGGREGATE":
        return ["FT.AGGREGATE", INDEX, "*",
                "LOAD", "2", "title", "body",
                "LIMIT", "0", str(index_size),
                "TIMEOUT", str(timeout_ms)]
    raise ValueError(query_type)


def run_cell(coord: redis.Redis, cell: Cell, n: int, writer: csv.writer,
             csv_fh) -> None:
    cmd = build_query(cell.query_type, cell.index_size, cell.timeout_ms)
    for i in range(n):
        t0 = time.monotonic()
        err = ""
        try:
            coord.execute_command(*cmd)
            timed_out = 0
        except redis.ResponseError as e:
            msg = str(e)
            err = msg[:120]
            timed_out = 1 if ("imeout" in msg or "limit was reached" in msg.lower()) else 0
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:120]
            timed_out = -1  # connection/protocol error
        observed_ms = (time.monotonic() - t0) * 1000.0
        writer.writerow([cell.version, cell.topology, cell.query_type,
                         cell.index_size, cell.timeout_ms, i,
                         f"{observed_ms:.3f}", timed_out, err])
    csv_fh.flush()


def cluster_client(cl: Cluster) -> "redis.RedisCluster":
    nodes = [ClusterNode("127.0.0.1", s.port) for s in cl.shards]
    return redis.RedisCluster(startup_nodes=nodes, decode_responses=True,
                              socket_timeout=120)


def run_outer(version: str, topology: str, num_shards: int, index_size: int,
              query_types: list[str], timeouts: list[int], n: int,
              writer, csv_fh) -> None:
    print(f"\n=== {version} / {topology} / size={index_size} ===", flush=True)
    so = VERSIONS[version]
    cl = Cluster(module_so=so, num_shards=num_shards, workers=4,
                 base_port=18000 + hash((version, topology)) % 1000)
    cl.start()
    try:
        conns = cl.all_conns()
        print(f"  loading {index_size} docs...", flush=True)
        t0 = time.time()
        if num_shards == 1:
            create_index_sa(conns[0])
            load_sa(conns[0], index_size)
            total = wait_indexed(conns[0])
        else:
            create_index_cluster(conns)
            cc = cluster_client(cl)
            load_cluster(cc, index_size)
            total = wait_indexed_cluster(conns)
        print(f"  loaded {total} docs in {time.time() - t0:.1f}s", flush=True)

        coord = cl.coordinator()

        for qt in query_types:
            for to in timeouts:
                t1 = time.time()
                cell = Cell(version, topology, qt, index_size, to)
                run_cell(coord, cell, n, writer, csv_fh)
                print(f"    {qt:9s} to={to:>5d}ms  {n} iters in "
                      f"{time.time() - t1:.1f}s", flush=True)
    finally:
        cl.stop()


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("-n", "--iters", type=int, default=100)
    ap.add_argument("--versions", nargs="+", default=["old", "new"])
    ap.add_argument("--topologies", nargs="+", default=["sa", "cluster3"])
    ap.add_argument("--sizes", nargs="+", type=int, default=[100_000, 1_000_000])
    ap.add_argument("--qtypes", nargs="+", default=["SEARCH", "AGGREGATE"])
    ap.add_argument("--timeouts", nargs="+", type=int,
                    default=[100, 500, 1000, 2000, 5000])
    args = ap.parse_args()

    topo_shards = {"sa": 1, "cluster3": 3, "cluster5": 5, "cluster7": 7}

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    new_file = not os.path.exists(args.out)
    with open(args.out, "a", newline="") as fh:
        w = csv.writer(fh)
        if new_file:
            w.writerow(["version", "topology", "query_type", "index_size",
                        "timeout_ms", "iter", "observed_ms", "timed_out",
                        "error"])
        for ver in args.versions:
            for topo in args.topologies:
                for sz in args.sizes:
                    run_outer(ver, topo, topo_shards[topo], sz,
                              args.qtypes, args.timeouts, args.iters, w, fh)


if __name__ == "__main__":
    main()
