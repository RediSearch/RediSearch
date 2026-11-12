# NaviX with filter summaries POC

This layer depends on the always-on filter summaries and the VecSim NaviX batch iterator.
Enable the experimental query route with:

```text
CONFIG SET search-enable-unstable-features yes
FT.CREATE idx ON HASH SCHEMA category TAG price NUMERIC v VECTOR HNSW 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE
FT.SEARCH idx '(@category:{books} @price:[0 100])=>[KNN 10 @v $q HYBRID_POLICY NAVIX BATCH_SIZE 10 EF_RUNTIME 100 AS distance]' PARAMS 2 q <512-byte-float32-vector> SORTBY distance DIALECT 2
```

`HYBRID_POLICY NAVIX` selects the adaptive POC; `NAVIX_BLIND` fixes blind two-hop
expansion for comparison. Both are explicit modes; AUTO and ordinary BATCHES are unchanged.
Without BATCH_SIZE, NaviX requests k candidates per batch rather than applying the old
global-selectivity estimate. The route requires a filter child and unquantized,
single-value FLOAT32 RAM HNSW. Pure `*=>[KNN ...]` and other index types are rejected when an iterator is built.
An empty filter can short-circuit to zero results before the index-shape check; the
feature gate is still validated before that shortcut.

RediSearch lowers normalized exact TAG tokens, NUMERIC ranges, AND, and OR into the
existing quick-check predicates. TEXT, NOT, expansions, multiword tags, and unsupported
syntax remain UNKNOWN. UNKNOWN is conservative, including inside OR. Lowering is bounded
to 256 nodes/32 levels; overflow falls back to UNKNOWN.

VecSim receives only a random-access NO/MAYBE callback. It never checks the exact filter
iterator. RediSearch's existing batch intersection verifies every returned candidate and
requests another batch if necessary. The query AST owns callback contexts until its
iterators are destroyed. No filter membership set is materialized.

`FT.PROFILE` reports `NAVIX_QC` and filter checks/rejects, distance computations, expanded
nodes, one-hop/blind/directed counts, second-hop visits, and frontend candidates. Both
NaviX policies use that profile name; the original command distinguishes the policy.
Distances exclude upper-layer descent and the seed distance. Exact-filter batch counts
remain available in the usual vector profile.

This is a performance experiment, not a recall guarantee. A filtered two-hop frontier
can exhaust before k despite more matching documents elsewhere. There is no automatic
fallback. Compare latency only together with recall and underfilled-result counts.
Summaries are runtime-only: reindex the benchmark data after reload to measure them;
missing summaries safely return MAYBE but defeat the intended pruning.

Focused validation: VecSim's four deterministic `test_navix` cases plus
`test_navix:test_navix_exact_filter_and_batch_refill` cover the route and exact-filter
refill, including UNKNOWN predicates. The initial run is standalone HASH; cluster,
concurrent mutation, persistence/reload behavior, JSON, quantization, and other numeric
vector types are outside this POC's validated scope.

## Traversal experiments

The following additional `HYBRID_POLICY` values require the same unstable-feature gate
and index restrictions as `NAVIX`. They include the per-query filter-decision cache.

| Policy | Behavior |
| --- | --- |
| `NAVIX_DEDUP` | Adaptive NaviX, skipping completely processed second-hop neighborhoods across batches. |
| `NAVIX_BUDGET` | Deduplication plus at most `4 * M0 = 8 * M` second-hop neighbor inspections per expansion (512 at M=64). |
| `NAVIX_BRIDGE` | No second-hop expansion. Traverse all direct neighbors, including summary rejects, and admit only MAYBE nodes to result heaps. |

`NAVIX` and `NAVIX_BLIND` remain controls with their existing traversal. Exact filter
verification and batch refill remain in RediSearch for every policy. The budget is a
fixed POC preset here; VecSim callers can set `secondHopBudget` directly (zero = unlimited).
Budgets can lower recall and cause underfilled results; partial lists remain eligible but
restart from the beginning. Bridge mode can cross rejected chains, but is not an exact
search or guaranteed sparse-query fallback and can visit the entire reachable graph.

`FT.PROFILE` adds `NaviX second hop skipped`, `NaviX second hop budget exhausted`, and
`NaviX bridge expansions`. The existing second-hop visit counter should be zero for
bridge mode. Deduplication adds one byte per encountered internal-ID slot, plus capacity
slack, and reset clears it. Completed neighborhoods are not revisited for concurrent graph
changes; use a fully built stable graph for comparisons. These are experiments without a
performance or recall claim. This branch pins the matching VecSim traversal implementation;
callers must rebuild because its query-parameter and counter structs extend the C ABI.
