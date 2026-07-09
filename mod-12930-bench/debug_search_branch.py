"""Isolate why search_branch > hybrid at broad/100K: LOAD/reply-size asymmetry?"""

import re
import subprocess
from itertools import cycle
from time import perf_counter, sleep

import numpy as np
import redis

MODULE = "/Users/itzik.vaknin/dev/RediSearchClean/.claude/worktrees/warm-swimming-stallman/bin/macos-arm64v8-release/search-community/redisearch.so"
PORT = 6415
IDX = "dbpedia"
K, WINDOW = 10, 20
N = 100_000

data = np.load("data.npz", allow_pickle=True)
titles, texts, emb = data["titles"], data["texts"], data["embeddings"]
CORPUS_MAX = int(data["corpus_max"])
q_texts, q_titles, q_emb = texts[CORPUS_MAX:], titles[CORPUS_MAX:], emb[CORPUS_MAX:]

proc = subprocess.Popen(
    ["redis-server", "--port", str(PORT), "--save", "",
     "--loadmodule", MODULE, "TIMEOUT", "0", "ON_TIMEOUT", "FAIL", "WORKERS", "6"],
    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
sleep(1)
r = redis.Redis(port=PORT)
r.execute_command("FT.CREATE", IDX, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                  "title", "TEXT", "text", "TEXT",
                  "text_vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
                  "DIM", str(emb.shape[1]), "DISTANCE_METRIC", "COSINE")
pipe = r.pipeline(transaction=False)
for i in range(N):
    pipe.hset(f"doc:{i}", mapping={"title": str(titles[i]), "text": str(texts[i]),
                                   "text_vector": emb[i].tobytes()})
    if (i + 1) % 1000 == 0:
        pipe.execute()
pipe.execute()
while True:
    info = r.execute_command("FT.INFO", IDX)
    d = info if isinstance(info, dict) else dict(zip(info[::2], info[1::2]))
    d = {(k.decode() if isinstance(k, bytes) else k): v for k, v in d.items()}
    if int(d["num_docs"]) >= N and float(d["percent_indexed"]) >= 1.0:
        break
    sleep(1)
r.execute_command("CONFIG", "SET", "search-workers", "0")
print("loaded 100K, workers=0")

STOP = set("""a is the an and are as at be but by for if in into it no not of on or such
that their then there these they this to was will with""".split())
tok_re = re.compile(r"[a-z]{3,}")
def tok(s):
    return [t for t in tok_re.findall(str(s).lower()) if t not in STOP]

queries = []
for qi in range(64):
    toks = list(dict.fromkeys(tok(q_texts[qi]) + tok(q_titles[qi])))[:17]
    queries.append(("~@text:(" + "|".join(toks) + ")", q_emb[qi].tobytes()))

def bench(name, build, n_warm=50, n_timed=300):
    it = cycle(queries)
    for _ in range(n_warm):
        r.execute_command(*build(*next(it)))
    lat = np.empty(n_timed)
    for i in range(n_timed):
        a = build(*next(it))
        t0 = perf_counter()
        r.execute_command(*a)
        lat[i] = perf_counter() - t0
    print(f"{name:45s} p50={np.percentile(lat,50)*1000:7.2f}ms mean={lat.mean()*1000:7.2f}ms")

V = [
  ("search: SORTBY MAX 20 + LOAD key,title,text", lambda q, v: [
    "FT.AGGREGATE", IDX, q, "SCORER", "BM25STD", "ADDSCORES",
    "SORTBY", "2", "@__score", "DESC", "MAX", str(WINDOW),
    "LOAD", "3", "@__key", "@title", "@text", "DIALECT", "2"]),
  ("search: same + LIMIT 0 10", lambda q, v: [
    "FT.AGGREGATE", IDX, q, "SCORER", "BM25STD", "ADDSCORES",
    "SORTBY", "2", "@__score", "DESC", "MAX", str(WINDOW),
    "LIMIT", "0", "10",
    "LOAD", "3", "@__key", "@title", "@text", "DIALECT", "2"]),
  ("search: LOAD @__key only", lambda q, v: [
    "FT.AGGREGATE", IDX, q, "SCORER", "BM25STD", "ADDSCORES",
    "SORTBY", "2", "@__score", "DESC", "MAX", str(WINDOW),
    "LOAD", "1", "@__key", "DIALECT", "2"]),
  ("search: no LOAD at all", lambda q, v: [
    "FT.AGGREGATE", IDX, q, "SCORER", "BM25STD", "ADDSCORES",
    "SORTBY", "2", "@__score", "DESC", "MAX", str(WINDOW), "DIALECT", "2"]),
  ("hybrid LINEAR: LOAD key,title,text", lambda q, v: [
    "FT.HYBRID", IDX, "SEARCH", q, "SCORER", "BM25STD",
    "VSIM", "@text_vector", "$vector", "KNN", "2", "K", str(K),
    "COMBINE", "LINEAR", "6", "ALPHA", "0.3", "BETA", "0.7", "WINDOW", str(WINDOW),
    "LOAD", "3", "@__key", "@title", "@text", "LIMIT", "0", str(K),
    "PARAMS", "2", "vector", v]),
  ("hybrid LINEAR: LOAD @__key only", lambda q, v: [
    "FT.HYBRID", IDX, "SEARCH", q, "SCORER", "BM25STD",
    "VSIM", "@text_vector", "$vector", "KNN", "2", "K", str(K),
    "COMBINE", "LINEAR", "6", "ALPHA", "0.3", "BETA", "0.7", "WINDOW", str(WINDOW),
    "LOAD", "1", "@__key", "LIMIT", "0", str(K),
    "PARAMS", "2", "vector", v]),
]
for name, b in V:
    sleep(1)
    bench(name, b)

proc.terminate()
print("done")
