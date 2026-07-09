"""Stream the dbpedia dataset rows needed for the MOD-12930 benchmark and cache locally.

Saves data.npz with: titles, texts (object arrays), embeddings (float32 [N, dim]).
First CORPUS_MAX rows are the indexable corpus; the following QUERY_ROWS rows are
held out as query sources (their embeddings serve as query vectors).
"""

import numpy as np
from datasets import load_dataset

CORPUS_MAX = 500_000
QUERY_ROWS = 256
TOTAL = CORPUS_MAX + QUERY_ROWS

ds = load_dataset(
    "filipecosta90/dbpedia-openai-1M-text-embedding-3-large-512d",
    split="train",
    streaming=True,
)

titles, texts, embs = [], [], []
emb_col = None
for i, row in enumerate(ds):
    if emb_col is None:
        candidates = [k for k, v in row.items() if isinstance(v, list) and len(v) > 100]
        assert candidates, f"no embedding column found in {list(row.keys())}"
        emb_col = candidates[0]
        print(f"columns: {list(row.keys())}, embedding column: {emb_col}, dim={len(row[emb_col])}")
    titles.append(row.get("title", ""))
    texts.append(row.get("text", ""))
    embs.append(np.asarray(row[emb_col], dtype=np.float32))
    if (i + 1) % 10_000 == 0:
        print(f"{i + 1}/{TOTAL}")
    if i + 1 >= TOTAL:
        break

emb_arr = np.vstack(embs)
np.savez(
    "data.npz",
    titles=np.array(titles, dtype=object),
    texts=np.array(texts, dtype=object),
    embeddings=emb_arr,
    corpus_max=CORPUS_MAX,
    query_rows=QUERY_ROWS,
)
print(f"saved data.npz: {emb_arr.shape[0]} rows, dim={emb_arr.shape[1]}")
