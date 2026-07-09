"""A/B: does YIELD_SCORE_AS on the SEARCH subquery add O(N) per-doc cost?"""

import re
import subprocess
from itertools import cycle
from time import perf_counter, sleep

import numpy as np
import redis

import bench_lib as B

titles, texts, emb, corpus_max = B.load_data()
B.PORT = 6417
N = 100_000

r = B.start_redis(workers=6)
B.load_corpus(r, titles, texts, emb, N)
B.set_workers(r, 0)

q_texts, q_titles, q_emb = texts[corpus_max:], titles[corpus_max:], emb[corpus_max:]
queries = []
for qi in range(64):
    toks = list(dict.fromkeys(B.tok(q_texts[qi]) + B.tok(q_titles[qi])))[:17]
    queries.append(("~@text:(" + "|".join(toks) + ")", q_emb[qi].tobytes()))

def bench(name, build, n_warm=50, n_timed=400):
    it = cycle(queries)
    for _ in range(n_warm):
        r.execute_command(*build(*next(it)))
    lat = np.empty(n_timed)
    for i in range(n_timed):
        a = build(*next(it))
        t0 = perf_counter()
        r.execute_command(*a)
        lat[i] = perf_counter() - t0
    print(f"{name:55s} p50={np.percentile(lat,50)*1000:7.2f}ms mean={lat.mean()*1000:7.2f}ms")

def hybrid(yield_search, yield_vsim, yield_combined):
    def b(q, v):
        args = ["FT.HYBRID", B.IDX, "SEARCH", q, "SCORER", "BM25STD"]
        if yield_search:
            args += ["YIELD_SCORE_AS", "text_score"]
        args += ["VSIM", "@text_vector", "$vector", "KNN", "2", "K", "10"]
        if yield_vsim:
            args += ["YIELD_SCORE_AS", "vector_score"]
        if yield_combined:
            args += ["COMBINE", "LINEAR", "8", "ALPHA", "0.3", "BETA", "0.7",
                     "WINDOW", "20", "YIELD_SCORE_AS", "combined_score"]
        else:
            args += ["COMBINE", "LINEAR", "6", "ALPHA", "0.3", "BETA", "0.7", "WINDOW", "20"]
        args += ["LOAD", "3", "@__key", "@title", "@text", "LIMIT", "0", "10",
                 "PARAMS", "2", "vector", v]
        return args
    return b

bench("search branch (FT.SEARCH NOCONTENT top-20)", lambda q, v: [
    "FT.SEARCH", B.IDX, q, "SCORER", "BM25STD", "WITHSCORES", "NOCONTENT",
    "LIMIT", "0", "20", "DIALECT", "2"])
bench("hybrid: no YIELD_SCORE_AS anywhere", hybrid(False, False, False))
bench("hybrid: YIELD on SEARCH only", hybrid(True, False, False))
bench("hybrid: YIELD on VSIM only", hybrid(False, True, False))
bench("hybrid: YIELD on COMBINE only", hybrid(False, False, True))
bench("hybrid: YIELD everywhere (notebook command)", hybrid(True, True, True))

B.stop_redis()
print("done")
