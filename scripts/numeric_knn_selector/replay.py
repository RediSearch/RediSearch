# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Reproduce the pinned range-filter KNN requests on a single Redis shard."""
import argparse
import json
import mmap
from pathlib import Path
import random
import struct
import time

INDEX = "range20500"
PREFIX = "range20500:"
K = 25
DIM = 100
DATA_ROWS = 1_000_000


def connection(args):
    import redis

    return redis.Redis(unix_socket_path=args.socket, socket_timeout=120)


def index_info(client):
    info = client.execute_command("FT.INFO", INDEX)
    return dict(zip(info[::2], info[1::2]))


def build(args):
    client = connection(args)
    client.execute_command(
        "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", 1, PREFIX, "SCHEMA",
        "a", "NUMERIC", "b", "NUMERIC", "vector", "VECTOR", "HNSW", 10,
        "TYPE", "FLOAT32", "DIM", DIM, "DISTANCE_METRIC", "COSINE",
        "M", 64, "EF_CONSTRUCTION", 512,
    )
    pipeline = client.pipeline(transaction=False)
    begin = time.monotonic()
    with (args.data / "vectors.npy").open("rb") as vector_file, (
        args.data / "payloads.jsonl"
    ).open() as payloads, mmap.mmap(vector_file.fileno(), 0, access=mmap.ACCESS_READ) as vectors:
        if vectors[:8] != b"\x93NUMPY\x01\x00":
            raise ValueError("expected a v1 NUMPY array")
        start = 10 + struct.unpack_from("<H", vectors, 8)[0]
        stride = DIM * 4
        if len(vectors) != start + DATA_ROWS * stride:
            raise ValueError("unexpected vector array size")
        for i, line in enumerate(payloads):
            row = json.loads(line)
            begin_vector = start + i * stride
            pipeline.hset(
                f"{PREFIX}{i}",
                mapping={"a": row["a"], "b": row["b"],
                         "vector": vectors[begin_vector:begin_vector + stride]},
            )
            if (i + 1) % 128 == 0:
                pipeline.execute()
            if (i + 1) % 10_000 == 0:
                print(f"uploaded {i + 1} rows in {time.monotonic() - begin:.1f}s", flush=True)
        pipeline.execute()
        if i + 1 != DATA_ROWS:
            raise RuntimeError("incomplete payload data")
    while True:
        info = index_info(client)
        if info.get(b"hash_indexing_failures", 0) != 0:
            raise RuntimeError("indexing failures")
        vec = client.execute_command("_FT.DEBUG", "VECSIM_INFO", INDEX, "vector")
        fields = dict(zip(vec[::2], vec[1::2]))
        jobs = fields.get(b"BACKGROUND_INDEXING")
        if jobs is None:
            raise RuntimeError(f"missing tiered indexing state: {vec!r}")
        print(f"FT.INFO docs={info[b'num_docs']} indexing={info[b'indexing']}; background={jobs}", flush=True)
        if info[b"num_docs"] == DATA_ROWS and info[b"indexing"] == 0 and not jobs:
            break
        time.sleep(10)
    client.execute_command("SAVE")
    print(f"sealed index in {time.monotonic() - begin:.1f}s", flush=True)


def read_queries(path, query_count):
    source = []
    with path.open() as rows:
        for row in map(json.loads, rows):
            (operator, terms), = row["conditions"].items()
            ranges = []
            params = []
            for i, term in enumerate(terms):
                (field, spec), = term.items()
                boundaries = spec["range"]
                if set(boundaries) != {"gt", "lt"}:
                    raise ValueError(f"unexpected range bounds: {boundaries}")
                ranges.append(f"@{field}:[($lo{i} ($hi{i}]")
                params.extend([f"lo{i}", boundaries["gt"], f"hi{i}", boundaries["lt"]])
            connector = " " if operator == "and" else " | "
            if operator not in ("and", "or"):
                raise ValueError(f"unexpected predicate: {operator}")
            family = "single" if len(terms) == 1 else operator
            expr = "(" + connector.join(ranges) + ")"
            vector = struct.pack(f"<{DIM}f", *row["query"])
            truth = frozenset(row["closest_ids"][:K])
            source.append((expr, vector, params, truth, family))
    if len(source) != 10_000:
        raise ValueError("unexpected query file size")
    if query_count == len(source):
        return source
    return random.Random(20500).sample(source, query_count)


def search(client, query, method, ef, profile=False):
    expr, vector, params, _, _ = query
    attributes = f" EF_RUNTIME {ef}" if method != "adhoc" else ""
    if method.startswith("batch-size-"):
        attributes += " BATCH_SIZE " + method.removeprefix("batch-size-")
    if method == "batches":
        attributes += " HYBRID_POLICY BATCHES"
    if method == "adhoc":
        attributes += " HYBRID_POLICY ADHOC_BF"
    knn = f"{expr}=>[KNN {K} @vector $vec_param{attributes} AS vector_score]"
    command = ("FT.PROFILE", INDEX, "SEARCH", "QUERY") if profile else ("FT.SEARCH", INDEX)
    return client.execute_command(
        *command, knn, "SORTBY", "vector_score", "ASC", "LIMIT", 0, K,
        "RETURN", 1, "vector_score", "DIALECT", 4, "TIMEOUT", 60_000,
        "PARAMS", 2 + len(params), "vec_param", vector, *params,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create the stock numeric-range KNN benchmark index.")
    parser.add_argument("--socket", required=True)
    parser.add_argument("--data", type=Path, required=True)
    build(parser.parse_args())
