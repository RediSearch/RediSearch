#!/usr/bin/env python3
"""Replay a frozen filtered-vector bundle through the production FT.SEARCH path."""

import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
import uuid

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry


POLICIES = ("AUTO", "BATCHES", "ADHOC_BF")


def shared_module(root):
    path = Path(root) / "filtered_bench.py"
    spec = importlib.util.spec_from_file_location("filtered_bench", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def text(value):
    return value.decode() if isinstance(value, bytes) else str(value)


def json_value(value):
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return {text(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    return value


def pairs(value):
    if isinstance(value, dict):
        return {text(k): v for k, v in value.items()}
    if not isinstance(value, list) or len(value) % 2:
        raise ValueError("expected a RESP2 key/value list")
    return {text(value[i]): value[i + 1] for i in range(0, len(value), 2)}


def debug_info(client, index):
    return pairs(client.execute_command("_FT.DEBUG", "VECSIM_INFO", index, "vec"))


def backend_info(info):
    return pairs(info["BACKEND_INDEX"]) if "BACKEND_INDEX" in info else info


def verify_engine(info, engine):
    actual_disk = int(backend_info(info).get("IS_DISK", 0)) == 1
    if actual_disk != (engine == "disk"):
        raise ValueError(f"requested {engine}, but vector index IS_DISK={int(actual_disk)}")


def create_command(args, bundle):
    vector_options = ["TYPE", "FLOAT32", "DIM", bundle.manifest["dimensions"],
                      "DISTANCE_METRIC", bundle.manifest["metric"].upper(),
                      "M", args.m, "EF_CONSTRUCTION", args.ef_construction,
                      "EF_RUNTIME", args.ef_runtime]
    if args.rerank is not None:
        vector_options += ["RERANK", args.rerank.upper()]
    extra = []
    if args.create_options_json:
        extra = json.loads(Path(args.create_options_json).read_text())
        if not isinstance(extra, list) or any(not isinstance(x, (str, int)) for x in extra):
            raise ValueError("create-options-json must contain a JSON argument list")
    return ["FT.CREATE", args.index, "ON", "HASH", "PREFIX", 1, args.prefix,
            "SKIPINITIALSCAN", *extra, "SCHEMA", "filter", "TAG", "vec", "VECTOR",
            "HNSW", len(vector_options), *vector_options]


def query_command(args, bundle, case, policy):
    # Even 100% membership uses a real child iterator, so forced policies are exercised.
    tag = f"f{list(bundle.filters).index(case['filter_id'])}"
    options = "" if policy == "AUTO" else f" HYBRID_POLICY {policy}"
    if policy != "ADHOC_BF":
        options += f" EF_RUNTIME {args.ef_runtime}"
    if args.rerank is not None:
        options += f" RERANK {args.rerank.upper()}"
    query = f"(@filter:{{{tag}}})=>[KNN {case['k']} @vec $query{options} AS distance]"
    blob = bundle.queries[case["query_id"]].astype("<f4", copy=False).tobytes()
    return ["FT.SEARCH", args.index, query, "PARAMS", 2, "query", blob,
            "SORTBY", "distance", "ASC", "NOCONTENT", "WITHSORTKEYS", "LIMIT", 0,
            case["k"], "TIMEOUT", args.timeout_ms, "DIALECT", 2]


def parse_reply(reply, prefix):
    if not isinstance(reply, list) or not reply or not isinstance(reply[0], int):
        raise ValueError("expected an FT.SEARCH RESP2 reply")
    if (len(reply) - 1) % 2:
        raise ValueError("malformed FT.SEARCH result pairs")
    ids, distances = [], []
    for offset in range(1, len(reply), 2):
        key = text(reply[offset])
        if not key.startswith(prefix) or not key[len(prefix):].isdigit():
            raise ValueError(f"unexpected document key: {key}")
        sort_key = text(reply[offset + 1])
        if not sort_key.startswith("#"):
            raise ValueError("expected a numeric vector-distance sort key")
        distance = float(sort_key[1:])
        if not math.isfinite(distance):
            raise ValueError("non-finite returned distance")
        ids.append(int(key[len(prefix):]))
        distances.append(distance)
    return ids, distances


def wait_ready(client, args, count):
    deadline = time.monotonic() + args.load_timeout_seconds
    while time.monotonic() < deadline:
        info = pairs(client.execute_command("FT.INFO", args.index))
        vec = debug_info(client, args.index)
        verify_engine(vec, args.engine)
        backend = backend_info(vec)
        frontend = pairs(vec["FRONTEND_INDEX"]) if "FRONTEND_INDEX" in vec else {}
        workers = pairs(client.execute_command("_FT.DEBUG", "WORKERS", "STATS"))
        # Disk leaves BACKGROUND_INDEXING unset and schedules graph repairs after
        # removing vectors from the frontend, so its size alone is not a barrier.
        indexing_states = (-1, 0) if args.engine == "disk" else (0,)
        if (int(info.get("num_docs", 0)) == count
                and int(info.get("indexing", 0)) == 0
                and int(backend.get("INDEX_SIZE", -1)) == count
                and int(frontend.get("INDEX_SIZE", 0)) == 0
                and int(vec.get("BACKGROUND_INDEXING", 0)) in indexing_states
                and int(workers.get("totalPendingJobs", -1)) == 0
                and int(workers.get("numJobsInProgress", -1)) == 0):
            if int(info.get("hash_indexing_failures", 0)):
                raise ValueError("document indexing failures reported by FT.INFO")
            return
        time.sleep(0.1)
    raise TimeoutError("index did not finish loading/draining within load timeout")


def prepare(client, args, bundle):
    if client.exists(args.metadata_key):
        raise ValueError("benchmark metadata already exists; choose a fresh index/prefix")
    if next(client.scan_iter(match=args.prefix + "*", count=1), None) is not None:
        raise ValueError("document prefix already exists; choose a fresh prefix")
    client.execute_command(*create_command(args, bundle))
    membership = [[] for _ in range(len(bundle.vectors))]
    for i, ids in enumerate(bundle.filters.values()):
        for doc_id in ids:
            membership[doc_id].append(f"f{i}")
    pipe = client.pipeline(transaction=False)
    for doc_id, vector in enumerate(bundle.vectors):
        pipe.hset(f"{args.prefix}{doc_id}", mapping={
            "vec": vector.astype("<f4", copy=False).tobytes(),
            "filter": ",".join(membership[doc_id]),
        })
        if (doc_id + 1) % 500 == 0:
            pipe.execute()
    pipe.execute()
    wait_ready(client, args, len(bundle.vectors))
    metadata = {"bundle_id": bundle.bundle_id, "prefix": args.prefix,
                "graph_id": args.graph_id or str(uuid.uuid4()), "engine": args.engine,
                "create_command": create_command(args, bundle)}
    client.set(args.metadata_key, json.dumps(metadata), nx=True)
    return metadata


def reuse(client, args, bundle):
    stored = client.get(args.metadata_key)
    if stored is None:
        raise ValueError("index has no benchmark manifest; use --prepare on a fresh index")
    metadata = json.loads(stored)
    for name, expected in (("bundle_id", bundle.bundle_id), ("prefix", args.prefix),
                           ("engine", args.engine)):
        if metadata.get(name) != expected:
            raise ValueError(f"index metadata {name} does not match this run")
    if args.graph_id and args.graph_id != metadata["graph_id"]:
        raise ValueError("graph-id differs from stored graph identity")
    wait_ready(client, args, len(bundle.vectors))
    return metadata


def require_fail_on_timeout(client):
    config = client.config_get("search-on-timeout")
    normalized = {text(k): text(v).lower() for k, v in config.items()}
    if normalized.get("search-on-timeout") != "fail":
        raise ValueError("start the benchmark server with --search-on-timeout fail; "
                         "partial timeout replies must not count as successful queries")


def revision(path):
    result = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"],
                            text=True, capture_output=True)
    return result.stdout.strip() if result.returncode == 0 else None


def artifact_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def connection(args):
    # Retrying a timed request would hide failures and execute an unrecorded warm-up.
    return redis.Redis(host=args.host, port=args.port, username=args.username,
                       password=os.environ.get("REDIS_PASSWORD"), ssl=args.tls,
                       protocol=2, socket_timeout=max(30, args.timeout_ms / 500),
                       socket_connect_timeout=10, retry=Retry(NoBackoff(), 0))


def run(args):
    shared = shared_module(args.shared_root)
    bundle = shared.load_bundle(args.bundle)
    if args.engine == "disk" and "AUTO" in args.policy:
        raise ValueError("filtered disk queries require an explicit policy; AUTO is unsupported")
    if args.engine == "disk" and args.rerank is None:
        args.rerank = "true"
    args.metadata_key = f"__filtered_benchmark__:{args.index}"
    if args.metadata_key.startswith(args.prefix):
        raise ValueError("document prefix must not include the benchmark metadata key")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    client = connection(args)
    require_fail_on_timeout(client)
    metadata = prepare(client, args, bundle) if args.prepare else reuse(client, args, bundle)
    environment = json.loads(Path(args.environment_json).read_text()) if args.environment_json else {}
    if not isinstance(environment, dict):
        raise ValueError("environment-json must contain an object")
    config = {k: v for k, v in vars(args).items() if k != "metadata_key"}
    manifest = {"schema_version": 1, "bundle_id": bundle.bundle_id, "index": metadata,
                "configuration": config, "environment": environment,
                "timing_scope": "client FT.SEARCH round trip; single outstanding request",
                "cache_state_scope": "start of run; later queries can warm caches",
                "runner_revision": revision(Path(__file__).resolve().parents[3]),
                "shared_revision": revision(args.shared_root),
                "declared_server_source_revision": args.source_revision,
                "module_sha256": artifact_hash(args.module_artifact) if args.module_artifact else None,
                "client_hardware": {"platform": platform.platform(), "machine": platform.machine(),
                             "processor": platform.processor(), "logical_cpus": os.cpu_count()},
                "server": json_value(client.info("server")),
                "modules": json_value(client.execute_command("MODULE", "LIST")),
                "index_info": json_value(client.execute_command("FT.INFO", args.index)),
                "vector_info": json_value(debug_info(client, args.index)),
                "memory_before": json_value(client.info("memory")), "completed": False}
    manifest_path = output / "run.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    errors = 0
    with (output / "results.jsonl").open("x") as stream:
        # Rotate policy order between repetitions to expose rather than systematically
        # favor one policy with warmer caches. This is still a sequential latency test.
        for repeat in range(args.repeats):
            start = repeat % len(args.policy)
            policies = args.policy[start:] + args.policy[:start]
            for case in bundle.cases:
                for policy in policies:
                    command = query_command(args, bundle, case, policy)
                    row = {"bundle_id": bundle.bundle_id, "case_id": case["case_id"],
                           "repeat": repeat, "requested_policy": policy,
                           "effective_policy": None, "ids": [], "distances": []}
                    began = time.perf_counter_ns()
                    try:
                        reply = client.execute_command(*command)
                        row["latency_ms"] = (time.perf_counter_ns() - began) / 1e6
                        row["ids"], row["distances"] = parse_reply(reply, args.prefix)
                        row["status"] = "ok"
                    except (redis.RedisError, ValueError, KeyError, TypeError) as error:
                        row["latency_ms"] = (time.perf_counter_ns() - began) / 1e6
                        row["status"] = ("timeout" if isinstance(error, redis.TimeoutError)
                                         or "timeout" in str(error).lower() else "error")
                        row["error"] = str(error)
                        errors += 1
                    stream.write(json.dumps(row, allow_nan=False) + "\n")
                    stream.flush()
    profiles = []
    for case in bundle.cases[:args.profile_samples]:
        for policy in args.policy:
            command = query_command(args, bundle, case, policy)
            try:
                reply = client.execute_command("FT.PROFILE", args.index, "SEARCH", "QUERY",
                                               *command[2:])
                profiles.append({"case_id": case["case_id"], "policy": policy,
                                 "reply": json_value(reply)})
            except redis.RedisError as error:
                profiles.append({"case_id": case["case_id"], "policy": policy,
                                 "error": str(error)})
    (output / "profiles.json").write_text(json.dumps(profiles, indent=2) + "\n")
    manifest.update(completed=True, query_errors=errors,
                    memory_after=json_value(client.info("memory")))
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    client.close()
    return 1 if errors else 0


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--bundle", required=True)
    result.add_argument("--output", required=True, help="new directory; never overwritten")
    result.add_argument("--shared-root", default=str(Path(__file__).resolve().parents[3]
                        / "deps/VectorSimilarity/tests/benchmark/filtered"))
    result.add_argument("--host", default="127.0.0.1")
    result.add_argument("--port", type=int, default=6379)
    result.add_argument("--username")
    result.add_argument("--tls", action="store_true")
    result.add_argument("--index", default="filtered-bench")
    result.add_argument("--prefix", default="filtered-bench-doc:")
    result.add_argument("--prepare", action="store_true", help="create and load a fresh index")
    result.add_argument("--policy", action="append", choices=POLICIES)
    result.add_argument("--ef-runtime", type=int, default=100)
    result.add_argument("--m", type=int, default=16)
    result.add_argument("--ef-construction", type=int, default=200)
    result.add_argument("--repeats", type=int, default=3)
    result.add_argument("--timeout-ms", type=int, default=10000)
    result.add_argument("--load-timeout-seconds", type=int, default=600)
    result.add_argument("--profile-samples", type=int, default=1)
    result.add_argument("--create-options-json")
    result.add_argument("--environment-json")
    result.add_argument("--run-label", required=True)
    result.add_argument("--engine", choices=("ram", "disk"), default="ram")
    result.add_argument("--rerank", choices=("true", "false"))
    result.add_argument("--source-revision", required=True,
                        help="source revision used to build the running server module")
    result.add_argument("--module-artifact", help="optional local loaded module for SHA256 provenance")
    result.add_argument("--cache-state", choices=("warm", "cold", "unknown"), default="unknown")
    result.add_argument("--graph-id")
    return result


def main():
    args = parser().parse_args()
    args.policy = args.policy or list(POLICIES)
    if len(set(args.policy)) != len(args.policy):
        raise ValueError("duplicate policies")
    for name in ("ef_runtime", "m", "ef_construction", "repeats", "timeout_ms", "load_timeout_seconds"):
        if getattr(args, name) <= 0:
            raise ValueError(f"{name} must be positive")
    if args.profile_samples < 0:
        raise ValueError("profile-samples must be non-negative")
    if not re.fullmatch(r"[A-Za-z0-9_:-]+", args.prefix):
        raise ValueError("prefix must contain only letters, digits, underscore, colon or hyphen")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
