# MOD-12930 — FT.HYBRID Benchmark Plan

Ticket: [MOD-12930](https://redislabs.atlassian.net/browse/MOD-12930) · Origin: [PERF-473](https://redislabs.atlassian.net/browse/PERF-473) / [redis-performance/hybrid-perf-tests](https://github.com/redis-performance/hybrid-perf-tests)

Implementation, results and conclusions live in [`mod-12930-bench/`](mod-12930-bench/)
(see its `README.md` and `SUMMARY.md`). This document is the design.

## Context

The PERF-473 benchmark compared `FT.HYBRID ... COMBINE LINEAR` against an
`FT.AGGREGATE "(~text)=>[KNN 10]"` "equivalent" and observed FT.HYBRID degrading much more
when moving from 10K to 100K docs. These two commands are not equivalent — they return
**different results**, not just different timings:

- In the **aggregate** form the text acts as a *filter*: it is scored only for the K
  vector winners and is never ranked. A strong text match outside the vector top-K can
  never be returned. Cost ≈ one HNSW query, nearly flat in corpus size.
- **FT.HYBRID** ranks the full text matching set (top-WINDOW) and fuses it with the vector
  top-K. Ranking the text side costs O(|matches|) by definition — work the aggregate never
  does. The PERF query (`~@text:(17 OR'd tokens)`, optional operator) matches 100% of the
  corpus, the worst case.

So a KNN rerank was compared against true fusion. The goal here is a benchmark that
attributes FT.HYBRID's cost correctly, against baselines with equal semantics.

## Approach

Time FT.HYBRID alongside its two subqueries run standalone, and read the **raw latencies
side by side** — derived metrics (ratios, deltas) hide which subquery dominates in a cell
and can turn measurement error into plausible-looking numbers.

### Contenders

All BM25STD, DIALECT 2, output fixed at top-10 (exact commands in the HTML report):

| contender | command |
|---|---|
| `hybrid_linear` / `hybrid_rrf` | `FT.HYBRID` under test (LINEAR and RRF fusion) |
| `search_branch` | text-subquery equivalent: `FT.SEARCH … WITHSCORES LIMIT (W−10) 10` — the sort heap is still WINDOW-sized (offset+count) while the reply carries the same 10 rows hybrid returns |
| `vsim_branch` | vector-subquery equivalent: `FT.AGGREGATE "*=>[KNN K …]"`, K-deep sort, 10-row reply |

A client-side fusion baseline was considered and rejected: too implementation-dependent,
while its interesting parts (serialization, network) are covered by simpler means.
OpenSearch's `bool` baseline is likewise not comparable — a single WAND-pruned traversal,
whereas nothing prunes here.

### Balanced calibration

Per (size, depth) cell, the text query is tuned (bisection on the target match count)
until the SEARCH subquery's latency is similar to the VSIM subquery's (±8%). With heavily
skewed subqueries, hybrid-vs-subqueries says little about concurrency or merge cost.
Consequences to keep in mind:

- The SEARCH numbers are **not** the native scaling of one fixed text query.
- Calibration probes must run on the exact query set that gets timed (a different sample
  drifted the achieved ratio by up to ~30%).
- The largest corpus aims slightly above the vector latency (bias ×1.05) so calibration
  slack cannot make it undercut smaller corpora.

### Matrix

- Dataset size: 10K / 100K / 500K (dbpedia, 512-dim embeddings, HASH, HNSW cosine — same
  data as PERF-473)
- Depth K/WINDOW: 10/20, 50/100, 250/500 (K = WINDOW/2; the engine enforces K ≤ WINDOW)
- Loader (keyspace access): off (keys+scores only — the exact-comparison mode) / on
  (title+text loaded)
- Workers: 2
- Plus a **merger sweep**: window × size × text selectivity (match-fraction 1/10/50%)
  varied independently — the calibrated matrix ties selectivity to the vector latency, so
  it cannot separate these axes.

### Protocol

- 256 distinct queries per cell (built from held-out dataset rows; query vector = the
  row's embedding), 5000 timed repetitions after 500 warm-up; p50/p90/p99/p99.9 + QPS.
- **Warm the index after every load**: the first timed command on a cold index measured
  3–4× slower (500K HNSW); a short per-command warm-up is not enough.
- Fresh redis-server per corpus, `TIMEOUT 0 ON_TIMEOUT FAIL` (the default 500ms timeout
  would silently truncate broad queries).

### Correctness gates

Before a configuration's numbers count, FT.HYBRID must return exactly what an offline
fusion of the two raw queries returns — LINEAR: `0.3·bm25 + 0.7·(2−d)/2` (the engine's
cosine normalization); RRF: `Σ 1/(60+rank)`; missing branch contributes 0. Comparison is
tie-aware (score vectors, not member sets — RRF/BM25 ties at the K boundary legitimately
reorder). 16 queries per configuration; a failed gate voids the configuration.

## Deliverables

1. `results_balanced_full.json` / `.csv`, `results_merger_sweep.json` — raw numbers.
2. `mod12930_balanced_report.html` — degradation with dataset size, p50 by contender,
   merge overhead (raw subtraction, in ms), calibration & gates, full table.
3. `SUMMARY.md` — conclusions for the ticket: the semantics gap, no scaling regression,
   testing recommendations, disclaimers.
