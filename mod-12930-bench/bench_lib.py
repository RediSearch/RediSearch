"""Shared machinery for the MOD-12930 benchmark: server management, corpus loading,
contender commands, correctness oracle, and the timing loop."""

import glob
import os
import re
import subprocess
from itertools import cycle
from time import perf_counter, sleep

import numpy as np
import redis

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(BENCH_DIR)
MODULE = sorted(glob.glob(os.path.join(ROOT, "bin", "*release*", "search-community", "redisearch.so")))[-1]
PORT = 6412
IDX = "dbpedia"

WORKERS_VALUES = [2]
OUT_K = 10  # final output size, constant across all cells
N_QUERY_SET = 256
N_WARMUP, N_TIMED = 500, 5000
GATE_QUERIES = 16


def load_data(path="data.npz"):
    """Cache from download_data.py: first corpus_max rows are indexable, the rest are
    held out as query sources."""
    d = np.load(path, allow_pickle=True)
    return d["titles"], d["texts"], d["embeddings"], int(d["corpus_max"])


# --- redis server ---

_proc = None

def stop_redis():
    global _proc
    if _proc is not None:
        _proc.terminate()
        _proc.wait()
        _proc = None

def start_redis(workers=0):
    """TIMEOUT 0 matters: the default 500ms timeout with ON_TIMEOUT RETURN would
    silently truncate broad queries."""
    global _proc
    stop_redis()
    logf = open(f"redis-{PORT}.log", "ab")
    _proc = subprocess.Popen(
        ["redis-server", "--port", str(PORT), "--save", "",
         "--loadmodule", MODULE, "TIMEOUT", "0", "ON_TIMEOUT", "FAIL",
         "WORKERS", str(workers)],
        stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(200):
        try:
            # Long socket timeout + retry survives brief machine suspensions mid-run.
            rc = redis.Redis(port=PORT, socket_timeout=600, retry_on_timeout=True)
            rc.ping()
            return rc
        except redis.exceptions.ConnectionError:
            sleep(0.1)
    raise RuntimeError("redis-server did not start; see redis log")

def set_workers(r, n):
    r.execute_command("CONFIG", "SET", "search-workers", str(n))
    v = deep_str(r.execute_command("CONFIG", "GET", "search-workers"))
    v = v["search-workers"] if isinstance(v, dict) else v[1]
    assert int(v) == n, f"workers not applied: {v!r}"


# --- reply parsing ---

def deep_str(x):
    if isinstance(x, bytes):
        return x.decode("utf-8", "replace")
    if isinstance(x, dict):
        return {deep_str(k): deep_str(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [deep_str(i) for i in x]
    return x

def parse_rows(reply):
    """FT.HYBRID rows are flat dicts; FT.AGGREGATE nests fields under 'extra_attributes'."""
    d = deep_str(reply)
    if isinstance(d, dict):
        return [row.get("extra_attributes", row) if isinstance(row, dict)
                else dict(zip(row[::2], row[1::2])) for row in d.get("results", [])]
    return [dict(zip(map(str, item[::2]), item[1::2]))
            for item in d[1:] if isinstance(item, list)]

def ft_info(r):
    raw = deep_str(r.execute_command("FT.INFO", IDX))
    return raw if isinstance(raw, dict) else {raw[i]: raw[i + 1] for i in range(0, len(raw), 2)}


# --- corpus ---

def load_corpus(r, titles, texts, emb, n):
    r.flushall()
    r.execute_command("FT.CREATE", IDX, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                      "title", "TEXT", "text", "TEXT",
                      "text_vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
                      "DIM", str(emb.shape[1]), "DISTANCE_METRIC", "COSINE")
    t0 = perf_counter()
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        pipe.hset(f"doc:{i}", mapping={"title": str(titles[i]), "text": str(texts[i]),
                                       "text_vector": emb[i].tobytes()})
        if (i + 1) % 1000 == 0:
            pipe.execute()
    pipe.execute()
    while True:
        info = ft_info(r)
        if int(info["num_docs"]) >= n and float(info["percent_indexed"]) >= 1.0 \
                and int(info.get("indexing", 0)) == 0:
            break
        sleep(1)
    print(f"loaded+indexed {n} docs in {perf_counter() - t0:.1f}s")
    warm_index(r, emb, n)

def warm_index(r, emb, n):
    """The first timed command on a cold index measured 3-4x slower (500K HNSW);
    touch the vector graph broadly and sweep the text postings once."""
    for i in range(300):
        r.execute_command("FT.SEARCH", IDX, "*=>[KNN 100 @text_vector $v]",
                          "NOCONTENT", "LIMIT", "0", "1", "PARAMS", "2",
                          "v", emb[(i * 37) % n].tobytes(), "DIALECT", "2")
    r.execute_command("FT.SEARCH", IDX, "~@text:(time|world|first|city|american|part)",
                      "SCORER", "BM25STD", "NOCONTENT", "LIMIT", "0", "1", "DIALECT", "2")


# --- text tokenization / match counting ---

STOP = set("""a is the an and are as at be but by for if in into it no not of on or such
that their then there these they this to was will with""".split())
_tok_re = re.compile(r"[a-z]{3,}")

def tok(s):
    return [t for t in _tok_re.findall(str(s).lower()) if t not in STOP]

def count_matches(r, q):
    rep = deep_str(r.execute_command("FT.SEARCH", IDX, q, "LIMIT", "0", "0", "DIALECT", "2"))
    return int(rep["total_results"] if isinstance(rep, dict) else rep[0])


# --- contenders (BM25STD, DIALECT 2; fields=True adds document loading) ---

def hybrid_linear(q, vec, k=10, window=20, fields=False):
    args = ["FT.HYBRID", IDX,
            "SEARCH", q, "SCORER", "BM25STD", "YIELD_SCORE_AS", "text_score",
            "VSIM", "@text_vector", "$vector", "KNN", "2", "K", str(k),
            "YIELD_SCORE_AS", "vector_score",
            "COMBINE", "LINEAR", "8", "ALPHA", "0.3", "BETA", "0.7",
            "WINDOW", str(window), "YIELD_SCORE_AS", "combined_score"]
    if fields:
        args += ["LOAD", "2", "@title", "@text"]
    return args + ["LIMIT", "0", str(OUT_K), "PARAMS", "2", "vector", vec]

def hybrid_rrf(q, vec, k=10, window=20, fields=False):
    args = ["FT.HYBRID", IDX,
            "SEARCH", q, "SCORER", "BM25STD", "YIELD_SCORE_AS", "text_score",
            "VSIM", "@text_vector", "$vector", "KNN", "2", "K", str(k),
            "YIELD_SCORE_AS", "vector_score",
            "COMBINE", "RRF", "6", "CONSTANT", "60",
            "WINDOW", str(window), "YIELD_SCORE_AS", "combined_score"]
    if fields:
        args += ["LOAD", "2", "@title", "@text"]
    return args + ["LIMIT", "0", str(OUT_K), "PARAMS", "2", "vector", vec]

def search_branch(q, vec, k=10, window=20, fields=False):
    """Text-subquery equivalent. LIMIT (window-OUT_K, OUT_K): the sort heap is still
    window-sized (offset+count) but the reply carries the same OUT_K rows hybrid returns."""
    content = ["RETURN", "2", "title", "text"] if fields else ["NOCONTENT"]
    return ["FT.SEARCH", IDX, q, "SCORER", "BM25STD", "WITHSCORES", *content,
            "LIMIT", str(window - OUT_K), str(OUT_K), "DIALECT", "2"]

def vsim_branch(q, vec, k=10, window=20, fields=False):
    """Vector-subquery equivalent: K-deep KNN and sort, OUT_K-row reply."""
    args = ["FT.AGGREGATE", IDX, f"*=>[KNN {k} @text_vector $vector AS vector_distance]",
            "SORTBY", "2", "@vector_distance", "ASC", "MAX", str(k),
            "LIMIT", "0", str(OUT_K)]
    if fields:
        args += ["LOAD", "2", "@title", "@text"]
    return args + ["PARAMS", "2", "vector", vec, "DIALECT", "2"]

CONTENDERS = {
    "hybrid_linear": hybrid_linear,
    "hybrid_rrf": hybrid_rrf,
    "search_branch": search_branch,
    "vsim_branch": vsim_branch,
}


# --- correctness oracle & gates ---

def oracle_results(r, q, vec, k=10, window=20):
    """Offline fusion of the two raw queries, replicating FT.HYBRID exactly.
    LINEAR: 0.3*bm25 + 0.7*(2-d)/2 (cosine normalization per src/vector_normalization.h);
    RRF: sum of 1/(60+rank); a missing branch contributes 0."""
    txt = deep_str(r.execute_command("FT.SEARCH", IDX, q, "SCORER", "BM25STD", "WITHSCORES",
                                     "NOCONTENT", "LIMIT", "0", str(window), "DIALECT", "2"))
    text_rank = {}
    for item in txt["results"]:
        text_rank[item["id"]] = (len(text_rank) + 1, float(item["score"]))
    knn = deep_str(r.execute_command("FT.SEARCH", IDX,
                                     f"*=>[KNN {k} @text_vector $vector AS vector_distance]",
                                     "SORTBY", "vector_distance", "ASC",
                                     "RETURN", "1", "vector_distance", "LIMIT", "0", str(k),
                                     "PARAMS", "2", "vector", vec, "DIALECT", "2"))
    vec_rank = {}
    for item in knn["results"]:
        vec_rank[item["id"]] = (len(vec_rank) + 1, float(item["extra_attributes"]["vector_distance"]))

    linear, rrf = {}, {}
    for key in set(text_rank) | set(vec_rank):
        s = rr = 0.0
        if key in text_rank:
            s += 0.3 * text_rank[key][1]
            rr += 1.0 / (60 + text_rank[key][0])
        if key in vec_rank:
            s += 0.7 * (2.0 - vec_rank[key][1]) / 2.0
            rr += 1.0 / (60 + vec_rank[key][0])
        linear[key] = s
        rrf[key] = rr
    return linear, rrf

def gate_check(got, fused_full, tol=1e-6):
    """Tie-aware equivalence: every returned score matches the oracle, and the returned
    top-K score vector equals the oracle's (membership may differ among exact ties)."""
    if not got:
        return len(fused_full) == 0
    if any(k not in fused_full or abs(v - fused_full[k]) > tol for k, v in got.items()):
        return False
    exp = sorted(fused_full.values(), reverse=True)[:len(got)]
    return len(got) == min(OUT_K, len(fused_full)) and \
        all(abs(a - b) <= tol for a, b in zip(sorted(got.values(), reverse=True), exp))


# --- timing ---

def bench(r, args_list, n_warm=None, n_timed=None):
    n_warm = N_WARMUP if n_warm is None else n_warm
    n_timed = N_TIMED if n_timed is None else n_timed
    it = cycle(args_list)
    for _ in range(n_warm):
        r.execute_command(*next(it))
    lat = np.empty(n_timed)
    for i in range(n_timed):
        a = next(it)
        t0 = perf_counter()
        r.execute_command(*a)
        lat[i] = perf_counter() - t0
    lat_ms = lat * 1000
    return dict(qps=n_timed / lat.sum(),
                mean_ms=float(lat_ms.mean()),
                p50_ms=float(np.percentile(lat_ms, 50)),
                p90_ms=float(np.percentile(lat_ms, 90)),
                p99_ms=float(np.percentile(lat_ms, 99)),
                p999_ms=float(np.percentile(lat_ms, 99.9)))
