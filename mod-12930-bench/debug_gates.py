"""Reproduce the rerank-overlap=0 anomaly and a failing gate on a live 10K index."""

import re
import subprocess
from collections import Counter
from time import sleep

import numpy as np
import redis

MODULE = "/Users/itzik.vaknin/dev/RediSearchClean/.claude/worktrees/warm-swimming-stallman/bin/macos-arm64v8-release/search-community/redisearch.so"
PORT = 6414
IDX = "dbpedia"
K, WINDOW = 10, 20

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

N = 10_000
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
sleep(3)


def deep_str(x):
    if isinstance(x, bytes):
        return x.decode("utf-8", "replace")
    if isinstance(x, dict):
        return {deep_str(k): deep_str(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [deep_str(i) for i in x]
    return x


def parse_rows(reply):
    d = deep_str(reply)
    if isinstance(d, dict):
        return [row if isinstance(row, dict) else dict(zip(row[::2], row[1::2]))
                for row in d.get("results", [])]
    return [dict(zip(map(str, item[::2]), item[1::2]))
            for item in d[1:] if isinstance(item, list)]


STOP = set("""a is the an and are as at be but by for if in into it no not of on or such
that their then there these they this to was will with""".split())
tok_re = re.compile(r"[a-z]{3,}")
def tok(s):
    return [t for t in tok_re.findall(str(s).lower()) if t not in STOP]

qi = 3
toks = list(dict.fromkeys(tok(q_texts[qi]) + tok(q_titles[qi])))
broad_q = "~@text:(" + "|".join(toks[:17]) + ")"
vec = q_emb[qi].tobytes()

# --- rerank overlap debug ---
rer_reply = r.execute_command(
    "FT.AGGREGATE", IDX, f"({broad_q})=>[KNN {K} @text_vector $vector AS vector_distance]",
    "SCORER", "BM25STD", "ADDSCORES", "LOAD", "3", "@__key", "@title", "@text", "DIALECT", "2",
    "APPLY", "(2 - @vector_distance)/2", "AS", "vector_similarity",
    "APPLY", "@__score", "AS", "text_score",
    "APPLY", "0.3 * @text_score + 0.7 * @vector_similarity", "AS", "hybrid_score",
    "SORTBY", "2", "@hybrid_score", "DESC", "MAX", str(K), "PARAMS", "2", "vector", vec)
rows = parse_rows(rer_reply)
print("rerank first row keys:", list(rows[0].keys()) if rows else "NO ROWS")
print("rerank keys:", [row.get("__key") for row in rows][:5])

# --- oracle vs hybrid RRF debug ---
txt = deep_str(r.execute_command("FT.SEARCH", IDX, broad_q, "SCORER", "BM25STD", "WITHSCORES",
                                 "NOCONTENT", "LIMIT", "0", str(WINDOW), "DIALECT", "2"))
text_rank = {}
for item in txt["results"]:
    text_rank[item["id"]] = (len(text_rank) + 1, float(item["score"]))
knn = deep_str(r.execute_command("FT.SEARCH", IDX,
                                 f"*=>[KNN {K} @text_vector $vector AS vector_distance]",
                                 "SORTBY", "vector_distance", "ASC",
                                 "RETURN", "1", "vector_distance", "LIMIT", "0", str(K),
                                 "PARAMS", "2", "vector", vec, "DIALECT", "2"))
vec_rank = {}
for item in knn["results"]:
    vec_rank[item["id"]] = (len(vec_rank) + 1, float(item["extra_attributes"]["vector_distance"]))

rrf = {}
for k in set(text_rank) | set(vec_rank):
    s = 0.0
    if k in text_rank:
        s += 1.0 / (60 + text_rank[k][0])
    if k in vec_rank:
        s += 1.0 / (60 + vec_rank[k][0])
    rrf[k] = s
exp = dict(sorted(rrf.items(), key=lambda kv: -kv[1])[:K])

hyb = parse_rows(r.execute_command(
    "FT.HYBRID", IDX, "SEARCH", broad_q, "SCORER", "BM25STD", "YIELD_SCORE_AS", "text_score",
    "VSIM", "@text_vector", "$vector", "KNN", "2", "K", str(K), "YIELD_SCORE_AS", "vector_score",
    "COMBINE", "RRF", "6", "CONSTANT", "60", "WINDOW", str(WINDOW), "YIELD_SCORE_AS", "combined_score",
    "LOAD", "3", "@__key", "@title", "@text", "LIMIT", "0", str(K), "PARAMS", "2", "vector", vec))
got = {row["__key"]: float(row["combined_score"]) for row in hyb if "__key" in row}
print("\nRRF gate: sets equal?", set(got) == set(exp))
print("only in expected:", {k: round(v, 6) for k, v in exp.items() if k not in got})
print("only in got     :", {k: round(v, 6) for k, v in got.items() if k not in exp})
common = [k for k in got if k in exp]
diffs = {k: (got[k], exp[k]) for k in common if abs(got[k] - exp[k]) > 1e-6}
print("score diffs on common keys:", diffs if diffs else "none")
print("\nexpected boundary (ranks 8-13 of oracle fusion):",
      [(k, round(v, 6)) for k, v in sorted(rrf.items(), key=lambda kv: -kv[1])[7:13]])
print("text ranks around window boundary (18-22):",
      [(k, round(s, 4)) for k, (rk, s) in sorted(text_rank.items(), key=lambda kv: kv[1][0])[17:22]])

proc.terminate()
print("done")
