"""Read-only probe: attribute hybrid overhead at workers=6, broad@100K, depth 1000/2000.
Does NOT touch the benchmark suite."""

import json
from itertools import cycle
from time import perf_counter, sleep

import numpy as np

import bench_lib as B

titles, texts, emb, corpus_max = B.load_data()
B.PORT = 6420
r = B.start_redis(workers=6)
B.load_corpus(r, titles, texts, emb, 100_000)

q_texts, q_titles, q_emb = texts[corpus_max:], titles[corpus_max:], emb[corpus_max:]
queries = []
for qi in range(64):
    toks = list(dict.fromkeys(B.tok(q_texts[qi]) + B.tok(q_titles[qi])))[:17]
    queries.append(("~@text:(" + "|".join(toks) + ")", q_emb[qi].tobytes()))

def hybrid_variant(q, vec, k, window, yields=True, load=True):
    args = ["FT.HYBRID", B.IDX, "SEARCH", q, "SCORER", "BM25STD"]
    if yields:
        args += ["YIELD_SCORE_AS", "text_score"]
    args += ["VSIM", "@text_vector", "$vector", "KNN", "2", "K", str(k)]
    if yields:
        args += ["YIELD_SCORE_AS", "vector_score"]
    n = "8" if yields else "6"
    args += ["COMBINE", "LINEAR", n, "ALPHA", "0.3", "BETA", "0.7", "WINDOW", str(window)]
    if yields:
        args += ["YIELD_SCORE_AS", "combined_score"]
    if load:
        args += ["LOAD", "3", "@__key", "@title", "@text"]
    args += ["LIMIT", "0", "10", "PARAMS", "2", "vector", vec]
    return args

def bench(name, build, n_warm=30, n_timed=200):
    it = cycle(queries)
    for _ in range(n_warm):
        r.execute_command(*build(*next(it)))
    lat = np.empty(n_timed)
    for i in range(n_timed):
        a = build(*next(it))
        t0 = perf_counter()
        r.execute_command(*a)
        lat[i] = perf_counter() - t0
    print(f"{name:58s} p50={np.percentile(lat,50)*1000:7.2f}ms")

for w in [6, 0]:
    B.set_workers(r, w)
    print(f"--- workers={w}")
    bench("hybrid depth 1000/2000 (benchmark shape)", lambda q, v: hybrid_variant(q, v, 1000, 2000))
    bench("hybrid depth 1000/2000, no YIELD_SCORE_AS", lambda q, v: hybrid_variant(q, v, 1000, 2000, yields=False))
    bench("hybrid K=1000, WINDOW=1000 (smaller fusion set)", lambda q, v: hybrid_variant(q, v, 1000, 1000))
    bench("hybrid K=10, WINDOW=2000 (cheap vector, same window)", lambda q, v: hybrid_variant(q, v, 10, 2000))
    bench("hybrid K=10, WINDOW=20 (shallow reference)", lambda q, v: hybrid_variant(q, v, 10, 20))

# profile at workers=6, depth
B.set_workers(r, 6)
q, vec = queries[0]
prof = B.deep_str(r.execute_command("FT.PROFILE", B.IDX, "HYBRID", "QUERY",
                                    *hybrid_variant(q, vec, 1000, 2000)[2:]))
p = prof["Profile"]
out = {"execution_time_ms": prof.get("execution_time")}
for shard in ["SEARCH", "VSIM"]:
    s = p["Shards"][0].get(shard) if isinstance(p["Shards"], list) else p.get(shard)
    if s is None:
        for sh in p["Shards"]:
            if shard in sh:
                s = sh[shard]
    out[shard] = {kk: s[kk] for kk in s if "time" in kk.lower()}
    rps = s.get("Result processors profile", [])
    out[shard]["RPs"] = [(x.get("Type"), x.get("Time"), x.get("Results processed")) for x in rps]
coord = p.get("Coordinator", {})
out["TAIL"] = [(x.get("Type"), x.get("Time"), x.get("Results processed"))
               for x in coord.get("Result processors profile", [])]
print(json.dumps(out, indent=1, default=str))
B.stop_redis()
