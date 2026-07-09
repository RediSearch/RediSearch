"""Generate the slim mod12930_benchmark.ipynb (machinery lives in bench_lib.py)."""

import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s))
code = lambda s: cells.append(nbf.v4.new_code_cell(s))

md("""# MOD-12930 — Fair FT.HYBRID Benchmark

Implements [`../MOD-12930-benchmark-plan.md`](../MOD-12930-benchmark-plan.md).
All machinery (server management, query generation, gates, timing) lives in
[`bench_lib.py`](bench_lib.py) — this notebook is the narrative and the results.

**Question**: PERF-473 saw FT.HYBRID degrade ~6× moving from 10K to 100K docs while an
FT.AGGREGATE "equivalent" stayed flat. Is that an implementation problem, or inherent to
what FT.HYBRID computes?

**Timed contenders** (all server-side; BM25STD; final output fixed at top-10):

| contender | what it is |
|---|---|
| `hybrid_linear` / `hybrid_rrf` | `FT.HYBRID` under test |
| `search_branch` | its text branch standalone: `FT.SEARCH … WITHSCORES LIMIT {W-10} 10` — native score sort, top-W heap, 10-row reply (an `ADDSCORES+SORTBY` aggregate is ~30-45% slower for the same job and would overstate branch cost) |
| `vsim_branch` | its vector branch standalone: `*=>[KNN {K} …]` aggregate, K-sized sort, 10-row reply |

(The PERF-473 rerank aggregate was dropped from the matrix — its story is settled in earlier
runs: flat scaling, ≤42% overlap with true fusion, different semantics.)

**Matrix**: size (10K / 100K / 500K) × WORKERS (0 / 6) × text selectivity (selective /
medium / broad — *measured* via `LIMIT 0 0`, never assumed; broad is the PERF-473-style
`~@text:(17 tokens)` that matches 100% of the corpus).

**Fields axis** — every contender runs in two modes, never mixed:
- `none`: top-10 keys+scores only (`__key` is returned by FT.HYBRID natively). Work is
  identical across contenders — the exact-decomposition mode where ε and the accounting
  identity are meaningful.
- `title+text`: same commands + document fields. Strict work-equality is impossible here
  (the engine loads WINDOW+K rows per hybrid branch *pre-fusion*, while a pull-based
  standalone query only loads what it returns), so this mode compares **same goal, own
  cost** — and the per-contender delta between modes measures hybrid's load amplification.

**Retrieval depth scales with selectivity** — K/WINDOW = 10/20 (selective), 100/200
(medium), 1000/2000 (broad) — so the vector subquery and the merger are exercised
proportionally instead of staying at constant trivial cost while the text branch scales
(K = WINDOW/2, respecting the engine's KNN K ≤ WINDOW constraint; depth is constant
across dataset sizes so degradation ratios stay interpretable).

**Gates**: before timing, FT.HYBRID must return exactly what an untimed two-query oracle
fusion returns (tie-aware score comparison). A failed gate voids that configuration.""")

code("""import json
import pandas as pd
import bench_lib as B

titles, texts, emb, corpus_max = B.load_data()
print('module:', B.MODULE)
print(f'{emb.shape[0]} rows, dim={emb.shape[1]}, corpus={corpus_max} + {emb.shape[0]-corpus_max} held-out query rows')""")

md("## Run the full matrix\n\nPer dataset size: load + index, generate the query set, "
   "run the gates, then time every workers × selectivity × contender cell "
   "(3000 reps over 256 distinct queries, 300 warm-up).")

code("""results, gates, profiles, meta = B.run_matrix(titles, texts, emb, corpus_max)

with open('results.json', 'w') as f:
    json.dump(dict(meta=meta, results=results, gates=gates, profiles=profiles), f,
              indent=2, default=str)
pd.DataFrame(results).to_csv('results.csv', index=False)
print('saved results.json / results.csv')""")

md("## Gates\n\n`gate_*` = FT.HYBRID ≡ oracle fusion (out of 16 queries), tie-aware.")

code("pd.DataFrame(gates)")

md("## Results — p50 latency (ms)\n\nRows = configuration (including fields mode), columns = contender. "
   "The ticket's answer is visible here: `hybrid_*` tracks `search_branch` (its own text "
   "top-K work) in every cell, and both track |matches|, not corpus size.")

code("""df = pd.DataFrame(results)
pivot = df.pivot_table(index=['size', 'workers', 'selectivity', 'fields'], columns='contender',
                       values='p50_ms', sort=False).round(2)
pivot[['hybrid_linear', 'hybrid_rrf', 'search_branch', 'vsim_branch']]""")

md("""### ε — machinery overhead (fields=none only)

FT.HYBRID mean latency minus its **slowest branch** (the honest reference: branches can
deplete concurrently). Three components explain ε:

1. **Workers=0 depletes branches sequentially**, so the *other* branch's time is included.
2. **`YIELD_SCORE_AS` on the SEARCH subquery costs O(scanned docs)** — measured A/B at
   broad/100K with shallow windows (`debug_yield.py`): hybrid without any YIELD_SCORE_AS
   ≈ its branch (ε ≈ 2%); adding YIELD on SEARCH alone → +2ms. The score alias is written
   per scanned doc instead of only for the top-WINDOW survivors — **actionable
   optimization** (move the write post-sorter). YIELD on VSIM/COMBINE is free.
3. **Deep-window fusion in the merger** — with K/WINDOW = 1000/2000 the merger fuses
   ~3K unique docs per broad query (FT.PROFILE: `Hybrid Merger … Results processed: 2632`
   at 10K/broad): dict upkeep, per-doc merge, and a ~3K-row tail sort. This is the cost
   deliberately accepted to exercise the vector branch; it scales with WINDOW, not with
   corpus size.""")

code("""m = df[df.fields == 'none'].pivot_table(index=['size', 'workers', 'selectivity'],
                   columns='contender', values='mean_ms', sort=False)
eps = (m['hybrid_linear'] - m[['search_branch', 'vsim_branch']].max(axis=1))
pd.DataFrame({'hybrid_mean_ms': m['hybrid_linear'].round(2),
              'slowest_branch_ms': m[['search_branch', 'vsim_branch']].max(axis=1).round(2),
              'eps_ms': eps.round(2),
              'eps_pct': (100 * eps / m['hybrid_linear']).round(1)})""")

md("""## Notes / deviations from the plan

- N_TIMED=3000 per cell after 300 warm-up (plan said 30K; scaled for local run time —
  p99.9 still indicative).
- Embeddings: the dataset's precomputed 512-d vectors serve as both doc and query vectors
  (no re-embedding; engine behavior doesn't depend on which model produced them).
- Client timing includes redis-py round-trip (~50-100µs), identical for all contenders.
- Side finding 1: FT.AGGREGATE `ADDSCORES + SORTBY @__score MAX 20` measures ~30-45% slower
  than `FT.SEARCH … LIMIT 0 20` for the identical top-K-by-BM25 job (profiled: costlier
  Scorer + Sorter). Potential optimization target.
- Side finding 2: `YIELD_SCORE_AS` on the SEARCH subquery adds an O(scanned-docs) per-doc
  write (~2ms/query at broad/100K, ~25% of hybrid latency; `debug_yield.py`). PERF-473's
  command used it. Potential optimization: write the alias only for top-WINDOW survivors.
- `FT.PROFILE` captures for hybrid (workers=0, selective/broad per size) are saved in
  `results.json` under `profiles`.""")

nb["cells"] = cells
nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, "mod12930_benchmark.ipynb")
print("wrote mod12930_benchmark.ipynb")
