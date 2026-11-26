# Stock-mode selector for numeric range KNN

This experiment routes selective two-range AND queries to the existing
`ADHOC_BF` mode. It uses the estimates already available from the two
numeric iterators. The first selector divides their product by the index's
document count. The early-exact selector keeps that rule and uses the larger
child estimate to decide when to choose exact search for more conjunctions.
The product is a planning feature. Correlations between fields can make it
a poor estimate of the actual number of matches.

The rule is scoped to the measured case of one million documents, K=25,
100-dimensional single-vector float32 in-memory cosine HNSW, and
`EF_RUNTIME=2048`. The setting defaults to off. Other cases use legacy AUTO.
Queries with an explicit `HYBRID_POLICY` or `BATCH_SIZE` keep those settings.
Both routes use the standard numeric iterators and VecSim implementation.

## Train and screen the rule

Use a disposable Redis server with six search workers and a Unix socket.
Install the Python Redis client. Download `random_float_1m.tgz` from the
[pinned dataset registry](https://github.com/redis/vector-db-benchmark/blob/d3f4cc350b24b4cdc71432784e524b4ff76dec4d/datasets/datasets.json)
and extract `vectors.npy`, `payloads.jsonl` and `tests.jsonl`. The archive
SHA-256 is `8ae0aae24fd8eda4d265a3968b1d7b0c5a1b3dc504ec4e6d970a100655ea4baa`.
Start Redis with `--enable-debug-command local` so the loader can verify the
vector indexing queue has drained. If using a module with the experimental
selector, set `search-_numeric-knn-decision-tree` to `no` for calibration and
stock-mode replay.

```sh
python3 scripts/numeric_knn_selector/replay.py \
  --socket /tmp/redis-bench.sock --data /path/to/data
python3 scripts/numeric_knn_selector/fit_selector.py capture \
  /tmp/redis-bench.sock samples.jsonl --data /path/to/data
python3 scripts/numeric_knn_selector/fit_selector.py fit samples.jsonl > rules.json
```

The seed fixes 200 training and 100 held-out queries each for single ranges,
two-range ANDs, and two-range ORs. The capture measures legacy AUTO, forced
ADHOC_BF, and forced BATCHES three times per query, with the order varied.
The two graph modes use EF 2048. Exact search has no graph effort parameter.
The script extracts estimates from an untimed profile and saves the IDs
that matched ground truth for every mode. A reply with an unexpected result
count stops the run and reports the query ID and mode.

The fitter looks for one threshold per family. It uses training latency
medians, requires at least 20 queries and a 5% saving in the selected
branch, and rejects loss of any ground-truth hit found by AUTO. Evaluate
each selected rule on the held-out sample. The AND rule passed this check.
The selected forced-BATCHES OR rule lost two hits on the held-out sample
and was discarded. The script reports the AND-only candidate and its
holdout check alongside the original training fit. Sum-of-median latency
is a screening metric.

## Screen a deeper tree

Capture a new split with a different seed. Use the previous capture to
exclude its query IDs, including the earlier holdout. Each timed query
runs the same actions in a randomized order that rotates between repeats.
The fixed batch sizes are stock `BATCH_SIZE` query options. All batch
variants use the same graph and EF 2048.

```sh
python3 scripts/numeric_knn_selector/fit_selector.py capture \
  /tmp/redis-bench.sock fresh-samples.jsonl --data /path/to/data \
  --clients 6 --per-family-holdout 120 --repeats 3 --seed 205002 \
  --exclude-samples samples.jsonl \
  --actions auto adhoc batches batch-size-64 batch-size-128 batch-size-256
python3 scripts/numeric_knn_selector/fit_tree.py fresh-samples.jsonl > tree.json
```

Each filter family's tree can have two levels of splits. Single ranges and ORs use the
filter iterator estimate. ANDs can also use the smaller child estimate,
the larger child estimate, or their normalized product. These features
are available before vector search. Outcome fields such as the mode AUTO
finished in and the batch count stay out of the tree.

Each leaf can keep the existing selector or choose a measured stock action.
The fitter favors fewer splits and requires evidence of improvement for each
changed leaf. An action must preserve every ground-truth hit found by either
legacy AUTO or the existing selector on each training query it would route.
The held-out report checks the same hit-retention rule and reports latency
by filter family. Its hindsight score chooses the fastest eligible action
per query after seeing the results and describes the available headroom on
those measured actions.

The fixed batch-size candidate lost ground-truth hits on the full AND
replay. To fit an exact-only alternative, replay every AND query under
its current choice and stock exact. The current selector already uses exact
for some queries and the replay skips duplicate measurements for those.
The replay accepts a family-to-tree JSON map or the report from either fitter.

```sh
cat > exact-policy.json <<'JSON'
{"single":{"action":"current"},"and":{"action":"adhoc"},"or":{"action":"current"}}
JSON
python3 scripts/numeric_knn_selector/replay_policy.py \
  /tmp/redis-bench.sock exact-policy.json exact-replay.jsonl --data /path/to/data
python3 scripts/numeric_knn_selector/fit_early_exact.py exact-replay.jsonl \
  --calibration-samples samples.jsonl fresh-samples.jsonl > exact-tree.json
```

The early-exact fitter learns two splits from AND queries in the earlier
calibrations. It verifies hit retention and evaluates the frozen rule on the
other queries. Use the 100-client benchmark below for throughput and tail
latency. Capture ranked replies separately and compare per-query
ground-truth hits against both Redis controls.

## Run the 100-client comparison

```redis
CONFIG SET search-workers 6
CONFIG SET search-enable-unstable-features yes
CONFIG SET search-_numeric-knn-decision-tree early-exact
```

Set the last flag to `no` for legacy AUTO or `yes` for the first selector
on the same module and graph.
Use the same 10,000 requests and
[pinned native driver](https://github.com/redis/vector-db-benchmark/tree/d22d44e8b46009a800a59e952710166ece2e38a1)
for all three Redis arms and OpenSearch. Use K=25, an index built with M=64 and
construction EF 512, Redis EF 2048 and OpenSearch 3.7.0 EF 1024. Rotate the
run order at 100 clients. Check QPS, mean latency, p95, p99, completed
queries, and per-query retention of every ground-truth hit returned by
legacy AUTO or the first selector. The fixed 10,000-query workload
includes the earlier calibration queries.

## Native benchmark results

The AWS c7i.4xlarge run used four counterbalanced trials per arm and
10,000 requests per trial. All three Redis policies ran on one module,
process, and HNSW graph with six Search workers. The table reports the
median per-trial QPS, p95, and recall, and the mean client latency across
the four equally sized trials.

| Policy | QPS | Mean latency (ms) | p95 (ms) | Recall@25 |
| --- | ---: | ---: | ---: | ---: |
| Redis legacy AUTO | 101.29 | 984.44 | 1,146.05 | 99.6824% |
| Redis first selector | 108.29 | 919.91 | 1,082.41 | 99.6976% |
| Redis early-exact | 108.98 | 912.83 | 1,073.41 | 99.7040% |
| OpenSearch | 249.67 | 402.92 | 592.23 | 99.6532% |

Early-exact improved throughput by 7.59% over legacy AUTO and 0.63%
over the first selector. Mean latency fell by 7.27% and 0.77%,
respectively. Early-exact had higher QPS and lower mean latency than
both Redis controls in all four trials. OpenSearch delivered 2.29×
the throughput of early-exact on this workload.

Separate ranked replays over all 10,000 queries found zero lost
ground-truth hits against either Redis control. Early-exact found
54 additional ground-truth hits compared with legacy AUTO and 16
compared with the first selector.

## Screen the unified benchmark

The unified replay covers 11,990 cases across 12 index sizes, 15 K
values, and numeric, TAG, TEXT, and compound filters. This includes
140 customer-derived FLOAT16 cases. The paired runner exercises the
stock AUTO, ADHOC_BF, and BATCHES paths on the same graph for each case.
It records ordinary query timings and the ground-truth hits retained
by each mode.

```sh
python3 scripts/numeric_knn_selector/fit_unified.py paired-measurements \
  --split-published-configuration --reference legacy-profile-results \
  > unified-tree.json
```

The original historical seeds are used to fit the historical subtree.
The expanded seeds and 20% of the published query IDs are held out
from fitting. Each leaf can retain the current route or start in stock
exact search. The fit screens for any lost ground-truth hit and for
latency regressions exceeding both 20% and 0.1 ms. Negations and
optional filter roots retain their current route.

The guarded fit changed the actual path in 26 of 3,167 held-out cases.
All 26 were from the 32-dimensional grid. On that grid the mean
per-case latency ratio was 1.55% lower than the current policy. The
other three configurations used their existing execution paths. Across
the full replay, the rule retained every ground-truth hit found by
legacy AUTO and early-exact. These are results from selecting among
paired measurements. The section above reports native throughput for
the earlier early-exact policy.

```redis
CONFIG SET search-enable-unstable-features yes
CONFIG SET search-_numeric-knn-decision-tree unified-exact
```

The unified setting inherits early-exact for the published
100-dimensional configuration. On the supported historical RAM
HNSW configurations, it adds direct exact when the filter estimate
exceeds 1,886 and `floor(1000 * estimate / K) <= 13460`. The initial
exact decision uses existing query paths and respects explicit query
options.
