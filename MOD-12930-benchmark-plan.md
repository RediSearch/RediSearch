# MOD-12930 — Fair FT.HYBRID Benchmark Plan

Ticket: [MOD-12930](https://redislabs.atlassian.net/browse/MOD-12930) · Origin: [PERF-473](https://redislabs.atlassian.net/browse/PERF-473) / [redis-performance/hybrid-perf-tests](https://github.com/redis-performance/hybrid-perf-tests)

## Context

The PERF-473 benchmark compared `FT.HYBRID ... COMBINE LINEAR` against an
`FT.AGGREGATE "(~text)=>[KNN 10]"` "equivalent" and observed FT.HYBRID degrading much more
when moving from 10K to 100K docs. The comparison is not apples-to-apples:

- The **aggregate** form is a *KNN rerank*: the `HybridIterator` caps output at K=10
  (`src/iterators/hybrid_reader.c:661`), so the whole pipeline (ADDSCORES/APPLY/SORTBY)
  touches ~10 rows. Text score is evaluated *pointwise* on those 10 docs only (the iterator
  preserves each winner's term-match subtree, `hybrid_reader.c:93-124`). Strong text matches
  outside the vector top-10 can never be returned. Cost ≈ one HNSW query, nearly flat in N.
- The **FT.HYBRID** SEARCH branch is a *true text top-K*: it must BM25-score every matching
  document to fill its top-WINDOW heap (default WINDOW=20, `src/hybrid/hybrid_scoring.h:12`,
  sorter limit at `src/hybrid/parse_hybrid.c:935-941`), then the merger fuses
  union(text top-20, vector top-10). Cost is O(|text matching set|).
- The benchmark's text query `~@text:(17 OR'd tokens)` uses the optional operator, so it
  matches **100% of the corpus** — the worst case, and it conflates "corpus grew 10×" with
  "text matching set grew 10×".

Goal: a benchmark whose numbers attribute FT.HYBRID's cost correctly — to its branches, to
fusion overhead, and to text-query selectivity — against baselines with *equal semantics*.

## Hypotheses (what the results must confirm or refute)

- **H1**: FT.HYBRID cost tracks |text-branch matching set|, not corpus size; near-flat in N
  for selective queries.
- **H2**: Accounting identity — with WORKERS>0, `t(hybrid) ≈ max(t(search), t(vsim)) + ε`;
  with WORKERS=0, `≈ sum + ε`. ε is the depleter/merger/tail overhead we actually own.
- **H3**: ε is small and roughly constant across cells — the hybrid machinery itself
  (depleters, merger, tail) adds no meaningful cost beyond its branches.
- **H4**: The KNN-rerank aggregate is flat in N but has low overlap with true fusion results
  (its speed buys weaker semantics).

## Matrix

| Axis | Values |
|---|---|
| Dataset size | 10K / 100K (/ 1M if load time permits — 3 points give a slope) |
| WORKERS | 0 / 6 (`CONFIG SET search-workers 6`, or `WORKERS 6` module arg) |
| Text selectivity | selective / medium / broad (see below) |

Dataset: dbpedia (`filipecosta90/dbpedia-openai-1M-text-embedding-3-large-512d`), re-embedded
with `all-MiniLM-L6-v2` (384-dim), HASH, HNSW cosine float32 — same as `dbpedia.py`.
Record HNSW build params (M, EF_CONSTRUCTION) and module version in the results.

### Selectivity variants

Selectivity is *measured*, not assumed: for each variant × dataset size, record
`|matches| = FT.SEARCH idx <query> LIMIT 0 0` and report it next to every number.

- **broad** — current PERF-473 query: `~@text:(tok1 | ... | tok17)` (optional ⇒ ~100% of corpus)
- **medium** — non-optional OR of a few common tokens, target ~1–10% of corpus,
  e.g. `@text:(ancient | river | africa)`
- **selective** — intersection of rare terms, target ≲0.1%,
  e.g. `@text:(hieroglyphics pyramids)`

## Contenders (exact commands)

All use `K=10`, `WINDOW=20`, `SCORER BM25STD`, `DIALECT 2`, same LOAD/RETURN fields
(`title`, `text`), `$vector` = query embedding blob. `<Q>` = text query variant.

### 1. FT.HYBRID — LINEAR (primary) and RRF (once per cell, cost-parity check)

```
FT.HYBRID dbpedia
  SEARCH "<Q>" SCORER BM25STD YIELD_SCORE_AS text_score
  VSIM @text_vector $vector KNN 2 K 10 YIELD_SCORE_AS vector_score
  COMBINE LINEAR 8 ALPHA 0.3 BETA 0.7 WINDOW 20 YIELD_SCORE_AS combined_score
  LOAD 2 @title @text
  LIMIT 0 10
  PARAMS 2 vector <blob>
```

RRF variant: `COMBINE RRF 6 CONSTANT 60 WINDOW 20 YIELD_SCORE_AS combined_score`.

### 2. Branch decomposition (for the H2 accounting identity)

SEARCH-branch equivalent — same scorer, same top-WINDOW cap, LOAD after the sort
(mirrors the hybrid subquery's sorter-upstream-of-loader layout):

```
FT.AGGREGATE dbpedia "<Q>"
  SCORER BM25STD ADDSCORES
  SORTBY 2 @__score DESC MAX 20
  LOAD 2 @title @text
  DIALECT 2
```

VSIM-branch equivalent — pure KNN:

```
FT.AGGREGATE dbpedia "*=>[KNN 10 @text_vector $vector AS vector_distance]"
  SORTBY 2 @vector_distance ASC MAX 10
  LOAD 2 @title @text
  PARAMS 2 vector <blob> DIALECT 2
```

**Decision rule** (on the ε from this decomposition, WORKERS=0 cells are the clean read):
FT.HYBRID materially above max/sum of its branches in any cell ⇒ real implementation
deficiency in the hybrid machinery (depleters/merger/tail) — open a follow-up ticket with
the profile capture. ε small everywhere ⇒ the PERF-473 "degradation" is fully explained by
semantics: O(|text matches|) is the price of true fusion, paid by any implementation.
FT.HYBRID's user-facing gain over hand-rolled fusion (one round trip, one dispatch, no
client code) is stated analytically in the writeup, not benchmarked — its magnitude is a
property of deployment topology, not of the engine.

Note: a timed "client-side fusion" contender (two pipelined queries + client merge) is
deliberately **not** in the matrix — its cost is by construction ≈ the sum of the two branch
timings above plus client noise, so it measures nothing this decomposition doesn't. Client
fusion appears only as the untimed correctness oracle in the Gates section.

### 3. Legacy KNN-rerank aggregate (optional, clearly labeled *different semantics*)

The original PERF-473 aggregate query, kept only as context for H4:

```
FT.AGGREGATE dbpedia "(<Q>)=>[KNN 10 @text_vector $vector AS vector_distance]"
  SCORER BM25STD ADDSCORES LOAD 2 title text DIALECT 2
  APPLY "(2 - @vector_distance)/2" AS vector_similarity
  APPLY "@__score" AS text_score
  APPLY "0.3 * @text_score + 0.7 * @vector_similarity" AS hybrid_score
  SORTBY 2 @hybrid_score DESC MAX 10
  PARAMS 2 vector <blob>
```

## Gates (pass/fail sanity, run once per dataset × selectivity — not reported as metrics)

Numbers from a configuration are quoted only after its gates pass. A gate failure halts the
benchmark and becomes its own correctness investigation.

**Correctness oracle** (untimed script, the independent ground truth): run the two raw
queries —

```
FT.SEARCH dbpedia "<Q>" SCORER BM25STD WITHSCORES LIMIT 0 20 RETURN 2 title text DIALECT 2
FT.SEARCH dbpedia "*=>[KNN 10 @text_vector $vector AS vector_distance]"
  SORTBY vector_distance LIMIT 0 10 RETURN 3 title text vector_distance
  PARAMS 2 vector <blob> DIALECT 2
```

— and fuse offline, replicating FT.HYBRID exactly (missing branch contributes 0,
`src/hybrid/hybrid_scoring.c:74-84`):

- **LINEAR**: `0.3 * bm25 + 0.7 * (2 - cosine_distance) / 2` — `(2-d)/2` is FT.HYBRID's own
  cosine normalization (`src/vector_normalization.h:60-64`); text score used raw.
- **RRF**: `Σ 1/(60 + rank_i)` over branches where the doc appears (rank = 1-based).

1. **FT.HYBRID LINEAR ≡ oracle LINEAR**: same doc-id set; combined scores equal within
   float tolerance (1e-6). Compare as set + score map — ties may order differently.
2. **FT.HYBRID RRF ≡ oracle RRF**: same doc-id set and RRF scores.
3. **Branch equivalents ≡ oracle's raw queries**: SEARCH-equivalent aggregate top-20
   (ids + scores) matches `FT.SEARCH <Q>`; VSIM-equivalent matches the KNN query.
4. **KNN-rerank overlap** (context, not a gate): |rerank top-10 ∩ fusion top-10| / 10,
   recorded per cell — expected to shrink as N grows.

## Protocol

- **Query set**: ~256 distinct queries (sample texts/titles from the dataset), embeddings
  precomputed and cached; cycle through them. Never a single repeated query.
- **Warm-up**: first ~1000 executions per contender excluded; 10s pause between contenders
  (as in the existing harness).
- **Volume**: 30K queries per cell per contender; latency via tdigest
  (p50/p90/p99/p99.9) + QPS, matching the existing `benchmark()` harness in `dbpedia.py`.
- **Load model**: single-connection latency for the full matrix; plus one saturation cell
  (e.g. 100K/broad, WORKERS 0 vs 6, ~16 concurrent connections) — workers buy single-query
  latency and may cost saturated throughput; measure both.
- **Attribution**: `FT.PROFILE`-equivalent capture on representative cells
  (10K & 100K × selective & broad, WORKERS=0) to show *where* time goes
  (child iterator scan + scoring vs depleter/merger/tail).
- **Environment**: standalone single shard; pinned module build; record redis-server
  version, module version, WORKERS value, index params, and measured |matches| per cell.

## Deliverables

1. Results table per cell: QPS, p50/p90/p99/p99.9, |text matches|, gate status.
2. H1–H4 verdicts with the supporting cells called out.
3. Profile captures for the representative cells.
4. Conclusion comment on MOD-12930: expected outcome is reframing the "degradation" as
   O(|text matching set|) inherent to true fusion semantics, with FT.HYBRID's own overhead
   (ε from H2) quantified — plus, if ε or the client-fusion delta is large, a concrete
   follow-up on where the implementation loses time. Text-side top-K pruning
   (WAND/MaxScore-style) is noted as the long-term lever for broad queries; out of scope here.
