"""Populate an index with synthetic hash docs.

Schema is the same on both 8.6 and master so FT.SEARCH/FT.AGGREGATE behave
identically across versions. Docs are wide enough that `*` scans do real work.

When ``with_vector=True`` a FLAT VECTOR field ``v`` (FLOAT32, DIM=128,
COSINE) is added to the schema and every doc gets a deterministic vector
payload. FLAT is chosen on purpose: it forces a brute-force scan during
``VSIM`` so FT.HYBRID at 1 M docs is guaranteed to do enough per-query work
to probe the timeout path.
"""
from __future__ import annotations

import random
import string
import struct

import redis

INDEX = "bench_idx"
VECTOR_DIM = 128

_BASE_SCHEMA = [
    "title", "TEXT",
    "body", "TEXT",
    "num", "NUMERIC", "SORTABLE",
    "tag", "TAG", "SORTABLE",
]

_VECTOR_FIELD = [
    "v", "VECTOR", "FLAT", "6",
    "TYPE", "FLOAT32",
    "DIM", str(VECTOR_DIM),
    "DISTANCE_METRIC", "COSINE",
]


def _schema(with_vector: bool) -> list:
    fields = list(_BASE_SCHEMA)
    if with_vector:
        fields.extend(_VECTOR_FIELD)
    return ["FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", *fields]


def _vec_bytes(i: int) -> bytes:
    # Deterministic, well-spread vectors so COSINE distances vary.
    # Cheap to generate; FLOAT32 little-endian.
    rnd = random.Random(i * 2654435761 & 0xFFFFFFFF)
    return struct.pack(f"<{VECTOR_DIM}f",
                       *(rnd.uniform(-1.0, 1.0) for _ in range(VECTOR_DIM)))


def probe_vector(seed: int = 0) -> bytes:
    return _vec_bytes(seed)


def _payload(i: int, rnd: random.Random, with_vector: bool) -> dict:
    # Deterministic-ish content; body ~200 chars so LOAD * does real work.
    body_tokens = [
        "".join(rnd.choices(string.ascii_lowercase, k=rnd.randint(4, 10)))
        for _ in range(25)
    ]
    out = {
        "title": f"document number {i}",
        "body": " ".join(body_tokens),
        "num": i,
        "tag": f"tag{i % 32}",
    }
    if with_vector:
        out["v"] = _vec_bytes(i)
    return out


def _create_index_on(conn: redis.Redis, with_vector: bool) -> None:
    try:
        conn.execute_command("FT.DROPINDEX", INDEX)
    except Exception:
        pass
    conn.execute_command(*_schema(with_vector))


def create_index_sa(conn: redis.Redis, with_vector: bool = False) -> None:
    _create_index_on(conn, with_vector)


def create_index_cluster(conns: list[redis.Redis],
                         with_vector: bool = False) -> None:
    # Every shard needs the index definition when using OSS cluster mode.
    for c in conns:
        _create_index_on(c, with_vector)


def load_sa(conn: redis.Redis, num_docs: int, seed: int = 42,
            batch: int = 2000, with_vector: bool = False) -> None:
    """Load docs into an SA server. Uses pipelining for speed."""
    rnd = random.Random(seed)
    pipe = conn.pipeline(transaction=False)
    for i in range(num_docs):
        pipe.hset(f"doc:{i}", mapping=_payload(i, rnd, with_vector))
        if (i + 1) % batch == 0:
            pipe.execute()
    pipe.execute()


def load_cluster(cluster_client: "redis.RedisCluster", num_docs: int,
                 seed: int = 42, batch: int = 2000,
                 with_vector: bool = False) -> None:
    """Load docs into an OSS cluster using a RedisCluster client.

    RedisCluster handles routing per key. We batch HSETs via a mapping writer
    without cross-slot pipelines (which RedisCluster doesn't support broadly).
    """
    rnd = random.Random(seed)
    # RedisCluster supports mset_nonatomic-style scattering; here we issue
    # HSETs in small bursts and rely on its per-key routing. For 1M docs this
    # is acceptable (~30-60s).
    for i in range(num_docs):
        cluster_client.hset(f"doc:{i}", mapping=_payload(i, rnd, with_vector))


def wait_indexed(conn: redis.Redis, index: str = INDEX, timeout: float = 120.0,
                 poll: float = 0.5) -> int:
    """Block until the index reports no pending docs. Returns final num_docs."""
    import time
    deadline = time.time() + timeout
    last_docs = -1
    while time.time() < deadline:
        info = conn.execute_command("FT.INFO", index)
        it = iter(info)
        d = dict(zip(it, it))
        # master may not expose 'indexing' same as 8.6; use 'num_docs' + 'indexing'
        indexing = int(d.get("indexing", 0) or 0)
        num_docs = int(d.get("num_docs", 0) or 0)
        if indexing == 0 and num_docs > 0 and num_docs == last_docs:
            return num_docs
        last_docs = num_docs
        time.sleep(poll)
    return last_docs


def wait_indexed_cluster(conns: list[redis.Redis], index: str = INDEX,
                         timeout: float = 180.0, poll: float = 0.5) -> int:
    """Across-shards variant.

    FT.INFO in coord-enabled builds returns cluster-aggregated counts from any
    shard, so we only query one. We still require `indexing == 0` everywhere.
    """
    import time
    deadline = time.time() + timeout
    last_total = -1
    while time.time() < deadline:
        any_indexing = False
        total = 0
        for i, c in enumerate(conns):
            info = c.execute_command("FT.INFO", index)
            it = iter(info)
            d = dict(zip(it, it))
            if int(d.get("indexing", 0) or 0):
                any_indexing = True
            if i == 0:
                total = int(d.get("num_docs", 0) or 0)
        if not any_indexing and total > 0 and total == last_total:
            return total
        last_total = total
        time.sleep(poll)
    return last_total
