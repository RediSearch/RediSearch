"""Benchmark driver.

For each (version, topology, query_type, index_size, timeout_ms) cell, run N
queries (filter='*') and record per-query: success/timeout, client-observed
latency in ms, and the per-cell mean server time from INFO commandstats.
Appends rows to a single CSV.
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
    probe_vector, wait_indexed, wait_indexed_cluster,
)


MASTER_SO = "/home/ubuntu/RediSearch/bin/linux-x64-release/search-community/redisearch.so"
OLD_SO = "/home/ubuntu/RediSearch-v8.6.0/bin/linux-x64-release/search-community/redisearch.so"


@dataclass
class Cell:
    version: str     # "old" (8.6) or "new" (master)
    topology: str    # "sa" or "cluster3" / "cluster5" / "cluster7"
    query_type: str  # "SEARCH" | "AGGREGATE" | "HYBRID"
    index_size: int
    timeout_ms: int


VERSIONS = {"old": OLD_SO, "new": MASTER_SO}
QUERY_COMMANDS = {
    "SEARCH": "FT.SEARCH",
    "AGGREGATE": "FT.AGGREGATE",
    "HYBRID": "FT.HYBRID",
}
LEGACY_CSV_COLUMNS = [
    "version", "topology", "query_type", "index_size",
    "timeout_ms", "iter", "observed_ms", "timed_out",
    "error",
]
CSV_COLUMNS = [*LEGACY_CSV_COLUMNS, "server_mean_ms"]


# `ON_TIMEOUT FAIL` is set at module-load time via harness (works in both SA
# and cluster mode on both versions). `FT.CONFIG` is disabled in cluster mode
# so runtime setting is not portable.


def build_query(query_type: str, index_size: int, timeout_ms: int) -> list:
    # We want queries that actually do enough work to exceed sub-second
    # timeouts. `*` alone is cheap, so we force per-doc loads:
    #   SEARCH    -> drop NOCONTENT; cap LIMIT so reply size stays bounded.
    #   AGGREGATE -> LOAD 2 title body; LIMIT spans the whole index.
    #   HYBRID    -> SEARCH * + VSIM over FLAT index (brute-force) + LOAD *
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
    if query_type == "HYBRID":
        limit = min(index_size, 1000)
        return ["FT.HYBRID", INDEX,
                "SEARCH", "*",
                "VSIM", "@v", "$vec",
                "LOAD", "*",
                "PARAMS", "2", "vec", probe_vector(),
                "LIMIT", "0", str(limit),
                "TIMEOUT", str(timeout_ms)]
    raise ValueError(query_type)


def _needs_vector(qtypes: list[str]) -> bool:
    return any(q == "HYBRID" for q in qtypes)


@dataclass
class CommandStat:
    calls: int = 0
    usec: int = 0
    found: bool = False


def _normalise_command_key(key: str) -> str:
    key = key.lower()
    if key.startswith("cmdstat_"):
        key = key[len("cmdstat_"):]
    # In OSS cluster mode the coordinator dispatches an internal `_FT.X` to
    # every shard (including itself) for each public `FT.X` request, so the
    # coordinator records both `cmdstat_FT.X` (the dispatched public call,
    # ~ wall time) and `cmdstat__FT.X` (its own per-shard contribution).
    # Skip the internal variant so the coordinator-only server time isn't
    # inflated by double-counting.
    if key.startswith("_"):
        return ""
    return key.replace("|", ".").replace("_", ".")


def _parse_stat_fields(value: object) -> CommandStat:
    if isinstance(value, dict):
        return CommandStat(
            calls=int(float(value.get("calls", 0) or 0)),
            usec=int(float(value.get("usec", 0) or 0)),
            found=True,
        )
    fields = {}
    for part in str(value).split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            fields[k] = v
    return CommandStat(
        calls=int(float(fields.get("calls", 0) or 0)),
        usec=int(float(fields.get("usec", 0) or 0)),
        found=bool(fields),
    )


def _parse_commandstats(info: object) -> dict[str, CommandStat]:
    out: dict[str, CommandStat] = {}

    def add_stat(key: str, stat: CommandStat) -> None:
        dest = out.setdefault(_normalise_command_key(key), CommandStat())
        dest.calls += stat.calls
        dest.usec += stat.usec
        dest.found = dest.found or stat.found

    if isinstance(info, bytes):
        info = info.decode("utf-8", "replace")
    if isinstance(info, str):
        for line in info.splitlines():
            if not line or line.startswith("#") or ":" not in line:
                continue
            key, value = line.split(":", 1)
            add_stat(key, _parse_stat_fields(value))
        return out
    if isinstance(info, dict):
        for key, value in info.items():
            add_stat(str(key), _parse_stat_fields(value))
    return out


def _read_commandstat(conn: redis.Redis, query_type: str) -> CommandStat:
    target = _normalise_command_key(QUERY_COMMANDS[query_type])
    info = conn.info("commandstats")
    return _parse_commandstats(info).get(target, CommandStat())


def _read_commandstats(conns: list[redis.Redis], query_type: str) -> CommandStat | None:
    total = CommandStat()
    try:
        for conn in conns:
            stat = _read_commandstat(conn, query_type)
            total.calls += stat.calls
            total.usec += stat.usec
            total.found = total.found or stat.found
    except Exception as e:
        print(f"warning: failed to read INFO commandstats: {e}", file=sys.stderr,
              flush=True)
        return None
    return total


def _server_mean_ms(before: CommandStat | None, after: CommandStat | None,
                    n: int) -> str:
    if before is None or after is None or not (before.found or after.found):
        return ""
    delta_usec = max(0, after.usec - before.usec)
    if n <= 0:
        return ""
    return f"{(delta_usec / n) / 1000.0:.3f}"


def run_cell(coord: redis.Redis, coord_bytes: redis.Redis, cell: Cell, n: int,
             server_timing_conns: list[redis.Redis], writer: csv.writer,
             csv_fh) -> None:
    cmd = build_query(cell.query_type, cell.index_size, cell.timeout_ms)
    # HYBRID responses can contain binary vector bytes in LOAD output; use a
    # non-decoding client so the success path doesn't raise UnicodeDecodeError
    # and get mis-reported as a protocol error.
    client = coord_bytes if cell.query_type == "HYBRID" else coord
    server_before = _read_commandstats(server_timing_conns, cell.query_type)
    rows = []
    for i in range(n):
        t0 = time.monotonic()
        err = ""
        try:
            client.execute_command(*cmd)
            timed_out = 0
        except redis.ResponseError as e:
            msg = str(e)
            err = msg[:120]
            timed_out = 1 if ("imeout" in msg or "limit was reached" in msg.lower()) else 0
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:120]
            timed_out = -1  # connection/protocol error
        observed_ms = (time.monotonic() - t0) * 1000.0
        rows.append([cell.version, cell.topology, cell.query_type,
                     cell.index_size, cell.timeout_ms, i,
                     f"{observed_ms:.3f}", timed_out, err])
    server_after = _read_commandstats(server_timing_conns, cell.query_type)
    server_mean_ms = _server_mean_ms(server_before, server_after, n)
    for row in rows:
        writer.writerow([*row, server_mean_ms])
    csv_fh.flush()


def cluster_client(cl: Cluster) -> "redis.RedisCluster":
    nodes = [ClusterNode("127.0.0.1", s.port) for s in cl.shards]
    return redis.RedisCluster(startup_nodes=nodes, decode_responses=True,
                              socket_timeout=120)


def run_outer(version: str, topology: str, num_shards: int, index_size: int,
              query_types: list[str], timeouts: list[int], n: int,
              server_timing_scope: str,
              writer, csv_fh) -> None:
    print(f"\n=== {version} / {topology} / size={index_size} ===", flush=True)
    so = VERSIONS[version]
    cl = Cluster(module_so=so, num_shards=num_shards, workers=4,
                 base_port=18000 + hash((version, topology)) % 1000)
    cl.start()
    try:
        conns = cl.all_conns()
        with_vec = _needs_vector(query_types)
        print(f"  loading {index_size} docs"
              f"{' (+ vector)' if with_vec else ''}...", flush=True)
        t0 = time.time()
        if num_shards == 1:
            create_index_sa(conns[0], with_vector=with_vec)
            load_sa(conns[0], index_size, with_vector=with_vec)
            total = wait_indexed(conns[0])
        else:
            create_index_cluster(conns, with_vector=with_vec)
            cc = cluster_client(cl)
            load_cluster(cc, index_size, with_vector=with_vec)
            total = wait_indexed_cluster(conns)
        print(f"  loaded {total} docs in {time.time() - t0:.1f}s", flush=True)

        coord = cl.coordinator()
        coord_bytes = redis.Redis(host="127.0.0.1", port=cl.shards[0].port,
                                  decode_responses=False, socket_timeout=120)
        server_timing_conns = conns if server_timing_scope == "all-shards" else [coord]

        for qt in query_types:
            for to in timeouts:
                t1 = time.time()
                cell = Cell(version, topology, qt, index_size, to)
                run_cell(coord, coord_bytes, cell, n, server_timing_conns,
                         writer, csv_fh)
                print(f"    {qt:9s} to={to:>5d}ms  {n} iters in "
                      f"{time.time() - t1:.1f}s", flush=True)
    finally:
        cl.stop()


def ensure_csv_schema(path: str) -> bool:
    out_dir = os.path.dirname(path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return True

    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
    if header == CSV_COLUMNS:
        return False
    if header != LEGACY_CSV_COLUMNS:
        raise RuntimeError(
            f"{path} has an unexpected CSV header; expected {CSV_COLUMNS!r}, "
            f"got {header!r}"
        )

    tmp_path = f"{path}.tmp"
    with open(path, newline="") as src, open(tmp_path, "w", newline="") as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in reader:
            upgraded = {col: row.get(col, "") for col in CSV_COLUMNS}
            writer.writerow(upgraded)
    os.replace(tmp_path, path)
    print(f"upgraded {path} with server_mean_ms column", flush=True)
    return False


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("-n", "--iters", type=int, default=50)
    ap.add_argument("--versions", nargs="+", default=["old", "new"])
    ap.add_argument("--topologies", nargs="+",
                    default=["sa", "cluster3", "cluster5", "cluster7"])
    ap.add_argument("--sizes", nargs="+", type=int,
                    default=[10_000, 100_000, 500_000, 1_000_000, 2_000_000])
    ap.add_argument("--qtypes", nargs="+",
                    default=["SEARCH", "AGGREGATE", "HYBRID"])
    ap.add_argument("--timeouts", nargs="+", type=int,
                    default=[50, 100, 500, 1000, 2000])
    ap.add_argument("--server-timing-scope",
                    choices=["coordinator", "all-shards"],
                    default="coordinator",
                    help=("Source for INFO commandstats deltas. "
                          "coordinator matches client-visible request time; "
                          "all-shards sums deltas across every shard."))
    args = ap.parse_args()

    topo_shards = {"sa": 1, "cluster3": 3, "cluster5": 5, "cluster7": 7}

    new_file = ensure_csv_schema(args.out)
    with open(args.out, "a", newline="") as fh:
        w = csv.writer(fh)
        if new_file:
            w.writerow(CSV_COLUMNS)
        for ver in args.versions:
            for topo in args.topologies:
                for sz in args.sizes:
                    run_outer(ver, topo, topo_shards[topo], sz,
                              args.qtypes, args.timeouts, args.iters,
                              args.server_timing_scope, w, fh)


if __name__ == "__main__":
    main()
