# Training the RAM HNSW latency policy

The `latency` setting selects an experimental policy for filtered, automatic
RAM HNSW queries. Redis Search defaults to `legacy`. The first learned policy
is available as `trained` for comparison.

```sh
redis-cli CONFIG SET search-_hnsw-hybrid-policy latency
redis-cli CONFIG GET search-_hnsw-hybrid-policy
redis-cli CONFIG SET search-_hnsw-hybrid-policy legacy
```

## Policy choices

- **Direct brute force.** Train on legacy AUTO timings and forced brute-force
  timings from the same graph. The tree can start in brute force when the
  estimated eligible set is small relative to K and the index. This avoids
  work in batches when a short exact scan is cheaper.
- **Near-full estimates.** At estimated selectivity of at least 99%, keep
  the legacy batch sizes. Switch to exact search when the feedback estimate
  falls to one eighth of its starting value and at most one sixteenth of K
  has been found. Queries that continue in batches keep the legacy graph
  traversal. This targets sparse filters whose estimates cover almost the
  entire index.
- **Exact scans with an empty buffer.** In latency mode, a RAM HNSW scan
  resolves the exact distance lookup to the backend while holding the
  shared locks. This removes a flat-buffer lookup for every matching
  document when the buffer is empty.

The initial policy uses the legacy decision on index shapes outside the
training coverage. The learned choices cover single-vector, unquantized
cosine HNSW with a drained buffer, through 1M vectors and K up to 4,000.
Settings specified via `HYBRID_POLICY`, `EF_RUNTIME`, or `BATCH_SIZE` take
precedence over learned initial tuning.

## Fit and generate the header

The trainer uses Python 3.12, NumPy 2.5.5, and scikit-learn 1.9.1 for the
recorded fit. Supply JSONL measurements with one case per row. Each row
contains its workload, predicate family, index size, label count, dimensions,
graph M, K, and planner estimate. The `runs` array holds paired repetitions
of `legacy` and `trained` AUTO. The optional `controls` array holds forced
policies, including `ADHOC_BF`.

```sh
python3 scripts/hnsw_hybrid_policy/train_latency.py \
  measurements/fp32-d32-m8.jsonl measurements/fp32-d96-m32.jsonl \
  measurements/customer-fp16.jsonl \
  --report /tmp/hnsw-latency-fit.json \
  --cpp src/vecsim/algorithms/hnsw/hnsw_policy_latency.h
```

The tree uses estimated selectivity, estimated eligible count, and estimated
eligible count divided by K alongside the index and query parameters. Split
weights use differences in milliseconds. The fit caps the influence of
individual outliers and rewards qualifying recall gains. The measured policy
for a leaf must preserve the legacy recall passes, stay within the per-leaf
slowdown budget, and avoid a median slowdown above 5% in the common
benchmark cases. The fit report includes input hashes and its settings.
The replay uses legacy measurements for the adaptive branch. A live run is
required to measure the branch's feedback and exact-scan behavior.

## Validate on fresh workloads

Run all three config values on each frozen graph. Balance each policy's
position and predecessor in the trial order. Pair timings by case and add
a second legacy control under `legacy_repeat`.
Compute recall against exact filtered top K. Run at least eight repetitions
per case. Record profile modes separately from timed commands.

```sh
python3 scripts/hnsw_hybrid_policy/check_latency.py \
  validation/fp32-d32-m8.jsonl validation/fp32-d96-m32.jsonl \
  validation/customer-fp16.jsonl --report /tmp/hnsw-latency-score.json
```

The scorer checks common-case mean latency against both legacy controls, zero
confirmed common-case regressions, fewer than 10% confirmed regressions
overall, and preservation of every legacy recall pass without new query
failures. A flagged slowdown exceeds 5% in all but one paired repetition
against each legacy control. On balanced runs, repeat flagged cases in a
separate balanced block on the same graph to confirm the slowdown.
The report also gives raw median
deltas and first-trained recall outcomes. Common cases are the customer-shaped
replays and single numeric ranges, TAG, and TEXT predicates at K up to 100,
including broad negation controls. With extra saturation stress cases, the
original matrix and the expanded matrix must each pass the gates. The
validation corpus defines the groups. Production frequencies require
separate traffic evidence.
