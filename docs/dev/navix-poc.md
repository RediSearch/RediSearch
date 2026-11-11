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
