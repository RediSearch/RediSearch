"""MOD-12930 benchmark machinery. The notebook stays narrative; everything mechanical lives here.

Implements ../MOD-12930-benchmark-plan.md: contender command builders, corpus loading,
selectivity-measured query generation, tie-aware equivalence gates against the untimed
two-query oracle, the timing loop, and the matrix orchestrator.
"""

import glob
import os
import re
import subprocess
from collections import Counter
from itertools import cycle
from time import perf_counter, sleep

import numpy as np
import redis

# --- configuration (import-time constants; override before calling run_matrix) ---
BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(BENCH_DIR)
MODULE = sorted(glob.glob(os.path.join(ROOT, "bin", "*release*", "search-community", "redisearch.so")))[-1]
REDIS_SERVER = "redis-server"
PORT = 6412
IDX = "dbpedia"

SIZES = [10_000, 100_000, 500_000]
WORKERS_VALUES = [0, 4]
SELECTIVITIES = ["selective", "medium", "broad"]
OUT_K = 10  # final output size (LIMIT 0 OUT_K), constant across all cells
# Per-selectivity retrieval depth: broader text queries get a proportionally deeper
# vector branch (KNN K) and fusion WINDOW, so the vector subquery and the merger are
# exercised instead of staying at constant trivial cost while the text branch scales.
# K = WINDOW/2 (the engine enforces KNN K <= WINDOW). Depth is constant across dataset
# sizes so cross-size degradation ratios stay interpretable.
DEPTH = {
    "selective": dict(k=10, window=20),
    "medium": dict(k=100, window=200),
    "broad": dict(k=1000, window=2000),
}
N_QUERY_SET = 256
N_WARMUP, N_TIMED = 500, 5000
GATE_QUERIES = 16

# --- data ---

def load_data(path="data.npz"):
    """Local cache from download_data.py. First corpus_max rows are indexable;
    the trailing query_rows rows are held out as query sources."""
    d = np.load(path, allow_pickle=True)
    return d["titles"], d["texts"], d["embeddings"], int(d["corpus_max"])


# --- redis server management ---

_proc = None

def stop_redis():
    global _proc
    if _proc is not None:
        _proc.terminate()
        _proc.wait()
        _proc = None

def start_redis(workers=0):
    """Fresh redis-server with the module. TIMEOUT 0 is critical: the default 500ms
    query timeout with ON_TIMEOUT RETURN would silently truncate broad queries."""
    global _proc
    stop_redis()
    logf = open(f"redis-{PORT}.log", "ab")
    _proc = subprocess.Popen(
        [REDIS_SERVER, "--port", str(PORT), "--save", "",
         "--loadmodule", MODULE, "TIMEOUT", "0", "ON_TIMEOUT", "FAIL",
         "WORKERS", str(workers)],
        stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(200):
        try:
            # Generous socket timeout + retry: survives brief process suspensions
            # (macOS sleep/App Nap) that would otherwise wedge the TCP read mid-run.
            # Commands are read-only, so a resend on timeout is safe.
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


# --- reply plumbing ---

def deep_str(x):
    if isinstance(x, bytes):
        return x.decode("utf-8", "replace")
    if isinstance(x, dict):
        return {deep_str(k): deep_str(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [deep_str(i) for i in x]
    return x

def parse_rows(reply):
    """FT.AGGREGATE/FT.HYBRID reply -> list of dicts. FT.HYBRID rows are flat;
    FT.AGGREGATE rows nest fields under 'extra_attributes'."""
    d = deep_str(reply)
    if isinstance(d, dict):
        rows = []
        for row in d.get("results", []):
            if isinstance(row, dict):
                rows.append(row.get("extra_attributes", row))
            else:
                rows.append(dict(zip(row[::2], row[1::2])))
        return rows
    return [dict(zip(map(str, item[::2]), item[1::2]))
            for item in d[1:] if isinstance(item, list)]

def ft_info(r):
    raw = deep_str(r.execute_command("FT.INFO", IDX))
    if isinstance(raw, dict):
        return raw
    return {raw[i]: raw[i + 1] for i in range(0, len(raw), 2)}


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
    print(f"loaded+indexed {n} docs in {perf_counter() - t0:.1f}s "
          f"(hash_indexing_failures={info.get('hash_indexing_failures')})")


# --- query generation (selectivity measured, never assumed) ---

STOP = set("""a is the an and are as at be but by for if in into it no not of on or such
that their then there these they this to was will with""".split())
_tok_re = re.compile(r"[a-z]{3,}")

def tok(s):
    return [t for t in _tok_re.findall(str(s).lower()) if t not in STOP]

def count_matches(r, q):
    rep = deep_str(r.execute_command("FT.SEARCH", IDX, q, "LIMIT", "0", "0", "DIALECT", "2"))
    return int(rep["total_results"] if isinstance(rep, dict) else rep[0])

def gen_queries(r, texts, q_texts, q_titles, q_emb, n):
    """Per held-out row: query vector = its embedding; text query from its tokens.
    selective: AND of the row's 2 rarest tokens (verified >0 matches).
    medium: OR of 3 tokens with df in [1%, 10%] of N.
    broad: PERF-473 style `~@text:(up to 17 tokens OR'd)` -> matches everything."""
    df = Counter()
    for i in range(n):
        df.update(set(tok(texts[i])))
    lo, hi = max(1, int(0.01 * n)), int(0.10 * n)
    mid_global = [t for t, c in df.most_common() if lo <= c <= hi][:200]

    qsets = {s: [] for s in SELECTIVITIES}
    for qi in range(N_QUERY_SET):
        toks = list(dict.fromkeys(tok(q_texts[qi]) + tok(q_titles[qi])))
        known = [t for t in toks if df[t] > 0]
        vec = q_emb[qi].tobytes()

        rare = sorted(known, key=lambda t: df[t])
        sel_q = None
        for a in range(min(4, len(rare))):
            for b in range(a + 1, min(6, len(rare))):
                cand = f"@text:({rare[a]} {rare[b]})"
                if count_matches(r, cand) > 0:
                    sel_q = cand
                    break
            if sel_q:
                break
        if sel_q is None and rare:
            sel_q = f"@text:({rare[0]})"
        qsets["selective"].append((sel_q, vec))

        mid = [t for t in known if lo <= df[t] <= hi]
        while len(mid) < 3:
            mid.append(mid_global[(qi * 3 + len(mid)) % len(mid_global)])
        qsets["medium"].append(("@text:(" + "|".join(mid[:3]) + ")", vec))

        broad = known[:17] if known else ["ancient"]
        qsets["broad"].append(("~@text:(" + "|".join(broad) + ")", vec))

    meas = {}
    for s in SELECTIVITIES:
        sample = qsets[s] if s != "broad" else qsets[s][:8]
        counts = [count_matches(r, q) for q, _ in sample]
        meas[s] = dict(mean=float(np.mean(counts)), median=float(np.median(counts)),
                       min=int(np.min(counts)), max=int(np.max(counts)))
        print(f"{s:10s} |matches| mean={meas[s]['mean']:.0f} median={meas[s]['median']:.0f} "
              f"min={meas[s]['min']} max={meas[s]['max']}  (N={n})")
    return qsets, meas


# --- contenders (BM25STD, DIALECT 2 everywhere; K/WINDOW from DEPTH[selectivity]) ---

# `fields` axis: False = top-10 keys+scores only (work identical across contenders — the
# exact-decomposition mode); True = same commands + title/text (same *goal*: each command
# pays its own loading cost; FT.HYBRID loads WINDOW+K rows per branch pre-fusion — the
# cross-mode delta per contender measures that amplification).

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
    # Faithful mirror of the hybrid SEARCH subquery: native score sort, top-WINDOW.
    # NOT an FT.AGGREGATE with ADDSCORES+SORTBY: that pipeline is ~30-45% slower for the
    # same top-K-by-score job and would overstate branch cost.
    # LIMIT (window-OUT_K, OUT_K): the sorter heap is still sized `window` (offset+num),
    # but the reply carries only OUT_K rows — same heap work as the hybrid branch without
    # paying a window-sized reply the hybrid doesn't pay.
    content = ["RETURN", "2", "title", "text"] if fields else ["NOCONTENT"]
    return ["FT.SEARCH", IDX, q, "SCORER", "BM25STD", "WITHSCORES", *content,
            "LIMIT", str(window - OUT_K), str(OUT_K), "DIALECT", "2"]

def vsim_branch(q, vec, k=10, window=20, fields=False):
    # KNN k with a k-sized sort; only OUT_K rows (and their fields, if any) returned.
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


# --- correctness oracle & gates (untimed) ---

def oracle_results(r, q, vec, k=10, window=20):
    """Two raw queries + offline fusion replicating FT.HYBRID exactly.
    LINEAR: 0.3*bm25 + 0.7*(2-d)/2 (cosine normalization, src/vector_normalization.h);
    RRF: sum 1/(60+rank); a missing branch contributes 0. Returns FULL fused dicts."""
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
    for k in set(text_rank) | set(vec_rank):
        s = 0.0
        if k in text_rank:
            s += 0.3 * text_rank[k][1]
        if k in vec_rank:
            s += 0.7 * (2.0 - vec_rank[k][1]) / 2.0
        linear[k] = s
        rr = 0.0
        if k in text_rank:
            rr += 1.0 / (60 + text_rank[k][0])
        if k in vec_rank:
            rr += 1.0 / (60 + vec_rank[k][0])
        rrf[k] = rr
    return linear, rrf

def gate_check(got, fused_full, tol=1e-6):
    """Tie-aware equivalence: every returned key's score matches the oracle fusion,
    and the returned top-K score vector equals the oracle's. Membership may
    legitimately differ among exact ties at the K boundary."""
    if not got:
        return len(fused_full) == 0
    if any(k not in fused_full or abs(v - fused_full[k]) > tol for k, v in got.items()):
        return False
    exp_vec = sorted(fused_full.values(), reverse=True)[:len(got)]
    got_vec = sorted(got.values(), reverse=True)
    return len(got) == min(OUT_K, len(fused_full)) and \
        all(abs(a - b) <= tol for a, b in zip(got_vec, exp_vec))

def run_gates(r, qsets, n, meas):
    out = []
    for sel in SELECTIVITIES:
        d = DEPTH[sel]
        ok_lin = ok_rrf = 0
        for q, vec in qsets[sel][:GATE_QUERIES]:
            full_lin, full_rrf = oracle_results(r, q, vec, **d)
            got_lin = {row["__key"]: float(row["combined_score"])
                       for row in parse_rows(r.execute_command(*hybrid_linear(q, vec, **d))) if "__key" in row}
            got_rrf = {row["__key"]: float(row["combined_score"])
                       for row in parse_rows(r.execute_command(*hybrid_rrf(q, vec, **d))) if "__key" in row}
            ok_lin += gate_check(got_lin, full_lin)
            ok_rrf += gate_check(got_rrf, full_rrf)
        row = dict(size=n, selectivity=sel, matches_mean=meas[sel]["mean"],
                   gate_linear=f"{ok_lin}/{GATE_QUERIES}", gate_rrf=f"{ok_rrf}/{GATE_QUERIES}")
        out.append(row)
        print(row)
    return out


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


# --- the matrix ---

def run_matrix(titles, texts, emb, corpus_max):
    """Full plan matrix: per size -> load, generate queries, gates, then
    workers x selectivity x contender timing. Returns (results, gates, profiles, meta)."""
    q_titles, q_texts, q_emb = titles[corpus_max:], texts[corpus_max:], emb[corpus_max:]
    results, gate_results, profiles = [], [], {}
    redis_version = None

    for n in SIZES:
        print(f"\n===== dataset size {n} =====")
        r = start_redis(workers=6)  # workers speed up HNSW indexing during load
        redis_version = deep_str(r.info().get("redis_version", "n/a"))
        load_corpus(r, titles, texts, emb, n)
        qsets, meas = gen_queries(r, texts, q_texts, q_titles, q_emb, n)
        set_workers(r, 0)
        gate_results += run_gates(r, qsets, n, meas)

        for w in WORKERS_VALUES:
            set_workers(r, w)
            for sel in SELECTIVITIES:
                d = DEPTH[sel]
                for f in [False, True]:
                    fname = "title+text" if f else "none"
                    for name, builder in CONTENDERS.items():
                        args_list = [builder(q, vec, fields=f, **d) for q, vec in qsets[sel]]
                        sleep(2)
                        stats = bench(r, args_list)
                        results.append(dict(size=n, workers=w, selectivity=sel, fields=fname,
                                            contender=name, matches_mean=meas[sel]["mean"],
                                            k=d["k"], window=d["window"], **stats))
                        print(f"n={n} w={w} {sel:10s} fields={fname:10s} {name:14s} "
                              f"qps={stats['qps']:8.1f} p50={stats['p50_ms']:.2f}ms "
                              f"p99={stats['p99_ms']:.2f}ms")

        # FT.PROFILE attribution captures (workers=0; representative cells)
        set_workers(r, 0)
        for sel in ["selective", "broad"]:
            q, vec = qsets[sel][0]
            try:
                prof = r.execute_command("FT.PROFILE", IDX, "HYBRID", "QUERY",
                                         *hybrid_linear(q, vec, **DEPTH[sel])[2:])
                profiles[f"{n}_{sel}"] = deep_str(prof)
            except redis.exceptions.ResponseError as e:
                profiles[f"{n}_{sel}"] = f"FT.PROFILE failed: {e}"
        stop_redis()

    meta = dict(
        module=MODULE,
        git_sha=subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
                               text=True).stdout.strip(),
        redis_version=redis_version,
        out_k=OUT_K, depth=DEPTH, dim=int(emb.shape[1]),
        n_query_set=N_QUERY_SET, n_warmup=N_WARMUP, n_timed=N_TIMED,
    )
    return results, gate_results, profiles, meta
