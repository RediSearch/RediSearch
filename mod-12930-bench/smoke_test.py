"""Smoke-test every benchmark command against a tiny index; print raw FT.HYBRID reply."""

import subprocess
from time import sleep

import numpy as np
import redis

MODULE = "/Users/itzik.vaknin/dev/RediSearchClean/.claude/worktrees/warm-swimming-stallman/bin/macos-aarch64-release/search-community/redisearch.so"
PORT = 6413
proc = subprocess.Popen(
    ["redis-server", "--port", str(PORT), "--save", "",
     "--loadmodule", MODULE, "TIMEOUT", "0", "ON_TIMEOUT", "FAIL", "WORKERS", "0"],
    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
sleep(1)
r = redis.Redis(port=PORT)
r.ping()

DIM = 4
r.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                  "title", "TEXT", "text", "TEXT",
                  "text_vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
                  "DIM", str(DIM), "DISTANCE_METRIC", "COSINE")
words = ["ancient egypt pyramids nile", "modern city skyline", "river africa nile boats",
         "pyramids pharaohs tombs", "random other words here", "egypt africa history"]
rng = np.random.default_rng(1)
for i, w in enumerate(words):
    v = rng.random(DIM, dtype=np.float32)
    r.hset(f"doc:{i}", mapping={"title": f"t{i}", "text": w, "text_vector": v.tobytes()})
sleep(1)
vec = rng.random(DIM, dtype=np.float32).tobytes()
q = "~@text:(egypt|pyramids|nile)"

def run(name, args):
    try:
        rep = r.execute_command(*args)
        print(f"--- {name}: OK")
        return rep
    except redis.exceptions.ResponseError as e:
        print(f"--- {name}: ERROR: {e}")
        return None

hy = run("hybrid_linear", ["FT.HYBRID", "idx",
    "SEARCH", q, "SCORER", "BM25STD", "YIELD_SCORE_AS", "text_score",
    "VSIM", "@text_vector", "$vector", "KNN", "2", "K", "3", "YIELD_SCORE_AS", "vector_score",
    "COMBINE", "LINEAR", "8", "ALPHA", "0.3", "BETA", "0.7", "WINDOW", "5",
    "YIELD_SCORE_AS", "combined_score",
    "LOAD", "3", "@__key", "@title", "@text",
    "LIMIT", "0", "3", "PARAMS", "2", "vector", vec])
print("RAW HYBRID REPLY:", hy)

run("hybrid_rrf", ["FT.HYBRID", "idx",
    "SEARCH", q, "SCORER", "BM25STD", "YIELD_SCORE_AS", "text_score",
    "VSIM", "@text_vector", "$vector", "KNN", "2", "K", "3", "YIELD_SCORE_AS", "vector_score",
    "COMBINE", "RRF", "6", "CONSTANT", "60", "WINDOW", "5", "YIELD_SCORE_AS", "combined_score",
    "LOAD", "3", "@__key", "@title", "@text",
    "LIMIT", "0", "3", "PARAMS", "2", "vector", vec])

run("search_branch", ["FT.AGGREGATE", "idx", q, "SCORER", "BM25STD", "ADDSCORES",
    "SORTBY", "2", "@__score", "DESC", "MAX", "5",
    "LOAD", "3", "@__key", "@title", "@text", "DIALECT", "2"])

run("vsim_branch", ["FT.AGGREGATE", "idx", "*=>[KNN 3 @text_vector $vector AS vector_distance]",
    "SORTBY", "2", "@vector_distance", "ASC", "MAX", "3",
    "LOAD", "3", "@__key", "@title", "@text", "PARAMS", "2", "vector", vec, "DIALECT", "2"])

run("rerank_aggregate", ["FT.AGGREGATE", "idx", f"({q})=>[KNN 3 @text_vector $vector AS vector_distance]",
    "SCORER", "BM25STD", "ADDSCORES", "LOAD", "3", "@__key", "@title", "@text", "DIALECT", "2",
    "APPLY", "(2 - @vector_distance)/2", "AS", "vector_similarity",
    "APPLY", "@__score", "AS", "text_score",
    "APPLY", "0.3 * @text_score + 0.7 * @vector_similarity", "AS", "hybrid_score",
    "SORTBY", "2", "@hybrid_score", "DESC", "MAX", "3", "PARAMS", "2", "vector", vec])

txt = run("oracle_text", ["FT.SEARCH", "idx", q, "SCORER", "BM25STD", "WITHSCORES",
                          "NOCONTENT", "LIMIT", "0", "5", "DIALECT", "2"])
print("RAW TEXT REPLY:", txt)
knn = run("oracle_knn", ["FT.SEARCH", "idx", "*=>[KNN 3 @text_vector $vector AS vector_distance]",
                         "SORTBY", "vector_distance", "ASC", "RETURN", "1", "vector_distance",
                         "LIMIT", "0", "3", "PARAMS", "2", "vector", vec, "DIALECT", "2"])
print("RAW KNN REPLY:", knn)

try:
    r.execute_command("CONFIG", "SET", "search-workers", "2")
    print("--- CONFIG SET search-workers: OK ->", r.execute_command("CONFIG", "GET", "search-workers"))
except redis.exceptions.ResponseError as e:
    print("--- CONFIG SET search-workers: ERROR:", e)

prof = run("profile_hybrid", ["FT.PROFILE", "idx", "HYBRID", "QUERY",
    "SEARCH", q, "SCORER", "BM25STD",
    "VSIM", "@text_vector", "$vector", "KNN", "2", "K", "3",
    "COMBINE", "LINEAR", "6", "ALPHA", "0.3", "BETA", "0.7", "WINDOW", "5",
    "LOAD", "1", "@__key", "LIMIT", "0", "3", "PARAMS", "2", "vector", vec])
if prof is not None:
    print("PROFILE REPLY (truncated):", str(prof)[:400])

proc.terminate()
print("smoke done")
