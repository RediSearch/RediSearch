# Iterator match-estimate benchmark report

## Status and revisions

All measurements are complete: 254 initial baseline/candidate comparisons,
38 explicit accessor measurements, and 16 CPU-pinned comparisons in both ABBA
rounds.

The consistent added cost is numeric filter construction. Tree-based equality
construction increases by a two-round point estimate of **7.89%**, or
**3.41–4.36 ns** per construction across the two rounds. Tree-based partial-range
construction increases by **8.65%**, or **2.37–6.24 ns**. Direct partial-range
construction increases by **6.96%**, or **1.48–2.08 ns**. Each of these cases has
a relative 95% interval entirely above zero in both rounds.

No tested traversal case has a relative 95% interval entirely above zero in
both controlled rounds. Controls still show substantial run-to-run drift, so
these measurements do not establish an overall query-regression percentage.

Explicit advisory calls measure **0.984 ns** for an ID list, about **3–44 ns**
for ordinary AND/OR trees, and up to **2.916 µs** for a 1,024-leaf tree through
the C callback bridge. Those costs apply when the new method is requested;
existing query planning does not call it in this PR.

| Input | Revision or scope |
| --- | --- |
| Baseline | `04783c49fbdfed33ffd867aa16371df32b351e49` |
| Candidate, initial full run | `7720807303329e81271fc35f1a81926ee5537a1e` |
| Candidate, accessor and ABBA runs | `d0c078cf465d3b182927b7a1f5344c543be2d83c` |
| Existing iterator suite | 246 cases, unchanged benchmark source on both revisions |
| Numeric filter suite | 8 cases, identical additional harness on both revisions |
| Accessor suite | 38 cases on the candidate only |

The baseline has two uncommitted harness-only changes: the `numeric_filter`
benchmark declaration in `rqe_iterators_bencher/Cargo.toml` and a copy of
`rqe_iterators_bencher/benches/numeric_filter.rs` from the candidate. Its
production sources remain at the baseline revision. This allows the new
numeric construction work to be measured using the same operations on both
revisions.

## Environment

Measurements were collected on November 9, 2026, with these settings:

| Setting | Value |
| --- | --- |
| Operating system | Linux `7.0.0-42-generic`, x86_64 |
| Processor | Intel Core Ultra 7 155U, 14 logical CPUs |
| Rust toolchain | 1.94.0 |
| C/C++ build | Default release configuration; CMake `RelWithDebInfo`, assertions disabled |
| Rust benchmark build | Default `bench` / release profile, optimized with debug information; workspace release settings |
| Criterion samples | 20 per case |
| Existing suite duration | Existing group settings: their measurement and warm-up durations are unchanged |
| Numeric and accessor duration | 1 second measurement, 0.2 seconds warm-up per case |
| CPU controls, initial full run | No CPU pinning and no frequency lock |
| CPU controls, accessor and ABBA runs | Pinned to CPU 0; no frequency lock |
| Execution order, initial full run | Complete baseline run, then complete candidate run; no concurrent build or benchmark commands |

Both checkouts build their own C artifacts. Cargo build artifacts are shared
sequentially at the candidate checkout's `bin/redisearch_rs`; `BINDIR` selects
the C artifacts belonging to the revision being benchmarked. Criterion results
are shared separately through `CRITERION_HOME`.

## What is being measured

The new advisory method is not consumed by query planning in this change.
Existing execution decisions still use `NumEstimated` / `num_estimated`.
There is no new per-read match counter or sampling pass.

Numeric filter construction caches a matching fraction even when no caller
requests an advisory estimate. That is the additional estimator arithmetic on
ordinary execution paths. C wrapper construction also initializes the added
callback pointer. On this 64-bit platform, the C iterator header grows by eight
bytes, and each concrete numeric iterator gains an eight-byte selectivity
value. These are iterator-object costs, not per-indexed-document costs.

Three measurements answer different questions:

1. The existing iterator suite compares construction/traversal behavior already
   benchmarked by the repository. It includes intersections with different
   overlap and child counts, flat/heap unions, leaf readers, optional and NOT
   iterators, and dense/sparse seek cases. Most cases directly use concrete
   Rust iterators.
2. The `numeric_filter` suite measures the newly affected construction paths.
   Its single numeric range contains 10,000 documents with values alternating
   between 0 and 100. It exercises a partial filter `[0, 50]` and an equality
   filter `[100, 100]`, through both the direct-reader and range-tree APIs.
   Constructor-only measurements include dropping the constructed object;
   traversal measurements also construct and drain it. Constructed values pass
   through `black_box` so unused cached fields cannot be optimized away.
3. The `cardinality` suite measures an explicit advisory call against upper-bound
   access on the same candidate iterator. It covers ID lists, intersections,
   flat and heap unions, nested AND-of-OR and OR-of-AND trees, and an AND-of-OR
   tree using the C callback bridge at every level. Fanouts are 2, 8, and 32;
   each nested tree has fanout squared leaves. Construction is outside the
   timed accessor operation. These timings are potential caller costs, not
   regressions in queries that do not call the new method.

## Reproduction

Use separate checkouts at the revisions above, initialize their dependencies,
and build each with the repository's default release configuration. The C
bundle and Rust bindings must match the revision whose benchmarks are running.
Copy the numeric harness and its Cargo benchmark declaration into the baseline
without copying production changes.

The following commands reproduce the benchmark settings. Run the baseline
commands from its checkout and the candidate commands from its checkout. Set
`CARGO_TARGET_DIR` to one common absolute artifact directory if sharing Cargo
artifacts; this does not permit concurrent invocations. `BINDIR` must change
when switching checkouts.

Baseline:

```bash
set -o pipefail
export BINDIR="$PWD/bin/linux-x64-release/search-community"
export CRITERION_HOME=/tmp/iterator-match-estimates-criterion
bench_log=$(mktemp /tmp/iterator-baseline-benches.XXXXXX.log)
echo "Log: $bench_log"
{
  cargo bench --manifest-path src/redisearch_rs/Cargo.toml \
    -p rqe_iterators_bencher --bench iterators -- \
    --sample-size 20 --save-baseline before &&
  cargo bench --manifest-path src/redisearch_rs/Cargo.toml \
    -p rqe_iterators_bencher --bench numeric_filter -- \
    --sample-size 20 --measurement-time 1 --warm-up-time 0.2 \
    --save-baseline before
} 2>&1 | tee "$bench_log" | tail -60
```

Candidate, initial full-run revision:

```bash
set -o pipefail
export BINDIR="$PWD/bin/linux-x64-release/search-community"
export CRITERION_HOME=/tmp/iterator-match-estimates-criterion
bench_log=$(mktemp /tmp/iterator-candidate-benches.XXXXXX.log)
echo "Log: $bench_log"
{
  cargo bench --manifest-path src/redisearch_rs/Cargo.toml \
    -p rqe_iterators_bencher --bench iterators -- \
    --sample-size 20 --baseline before &&
  cargo bench --manifest-path src/redisearch_rs/Cargo.toml \
    -p rqe_iterators_bencher --bench numeric_filter -- \
    --sample-size 20 --measurement-time 1 --warm-up-time 0.2 \
    --baseline before
} 2>&1 | tee "$bench_log" | tail -60
```

Accessor measurements use the final candidate revision. Build the `cardinality`
target with `cargo bench --no-run` and copy its reported executable to the path
below before running it. Compilation is outside the measured run.

```bash
set -o pipefail
export CRITERION_HOME=/tmp/iterator-match-estimates-criterion
accessor_log=$(mktemp /tmp/iterator-accessor-benches.XXXXXX.log)
echo "Log: $accessor_log"
taskset --cpu-list 0 /tmp/iterator-candidate-cardinality \
  --bench --sample-size 20 --measurement-time 1 --warm-up-time 0.2 \
  2>&1 | tee "$accessor_log" | tail -60
```

The ordinary iterator groups specify their own durations, which override
Criterion's global duration defaults. Their settings were not shortened.
Each paired target ran once per applicable revision in the initial experiment.
The saved logs and JSON supply its results. A separate targeted ABBA experiment
addresses the scheduling/order sensitivity exposed by those results.

## Analysis convention

Absolute timings use the arithmetic mean from Criterion's `estimates.json`,
in nanoseconds per benchmark iteration. A traversal iteration drains a
workload; it does not represent the cost of one document. Constructor and
accessor iterations each measure one operation as described above.

Relative time change is `candidate_mean / baseline_mean - 1`. Positive values
mean slower execution. Confidence intervals come directly from the bootstrap
mean-change estimate in `change/estimates.json`, at the recorded confidence
level (95% for this run). Absolute mean confidence intervals are not divided
to synthesize a relative interval.

Family and whole-suite summaries use the unweighted geometric mean of those
per-case ratios. Each benchmark has equal weight regardless of its execution
time or frequency in a real workload. This statistic is not a prediction of
end-to-end search throughput. An aggregate confidence interval is not claimed.

The local analysis script is `/tmp/summarize_iterator_estimates.py`. It requires
a fresh comparison result before counting a case, because a baseline save also
writes `new/` files. It distinguishes intervals above, below, and overlapping
zero. Criterion's extra hypothesis-test p-value is not persisted in these
estimate JSON files, so these interval categories do not reconstruct the CLI's
regression verdict. No correction for multiple comparisons is applied.

```bash
python3 /tmp/summarize_iterator_estimates.py \
  --markdown /tmp/iterator-estimates-summary.md \
  --json /tmp/iterator-estimates-summary.json
```

## Initial full-run results

All **254 comparisons** completed, with no missing or invalid comparison artifacts.
The 246 existing iterator cases have an unweighted geometric mean time change
of **-5.20%**; including the eight numeric harness cases gives
**-5.75%**. These are descriptive measurements, not evidence that the
change speeds up queries. Several unchanged traversal paths move sharply in
opposite directions, while the new estimate is never requested in this suite.
The selected confirmation experiment below tests whether the largest positive
changes reproduce under CPU pinning and reversed execution order.

Across all 254 cases, 139 relative 95% intervals lie entirely above zero,
78 entirely below zero, and 37 overlap zero. Those within-run intervals do
not capture systematic differences in scheduling, power, or thermal state
between the baseline and candidate runs.

### Family aggregates

| Family | Cases | Geometric mean time change |
| --- | ---: | ---: |
| Empty | 2 | +6.87% |
| GeoShape | 3 | +12.51% |
| IdList | 3 | +14.85% |
| Intersection | 54 | +12.85% |
| InvertedIndex/Missing | 3 | -42.85% |
| InvertedIndex/Numeric | 12 | -47.01% |
| InvertedIndex/Tag | 3 | -59.72% |
| InvertedIndex/Term | 33 | -37.52% |
| InvertedIndex/Wildcard | 3 | -58.99% |
| Metric | 3 | +14.65% |
| Not | 5 | +11.01% |
| NotOptimized | 6 | -10.49% |
| NumericFilter | 8 | -21.19% |
| Optional | 20 | +27.31% |
| Optional comparison | 4 | -22.13% |
| OptionalOptimized | 20 | +2.58% |
| Union | 68 | +7.55% |
| Wildcard | 4 | +2.76% |

### Six largest positive existing-suite changes

Absolute means in this table are milliseconds per benchmark workload. The
confidence intervals are relative time-change intervals, not absolute timings.

| Benchmark | Baseline ms | Candidate ms | Time change | Relative 95% CI |
| --- | ---: | ---: | ---: | --- |
| `Iterator - OptionalOptimized - Read/Rust child_ratio=70` | 13.652 | 25.008 | +83.19% | [+74.23%, +90.09%] |
| `Iterator - OptionalOptimized - Read/Rust child_ratio=80` | 12.683 | 22.247 | +75.41% | [+66.82%, +82.22%] |
| `Iterator - Union - Read Disjoint Interleaved 50 Children/Flat Full/Rust` | 494.427 | 855.573 | +73.04% | [+10.12%, +173.77%] |
| `Iterator - Union - SkipTo Disjoint Sequential 5 Children/Flat Quick/Rust` | 0.574 | 0.913 | +59.17% | [+43.16%, +78.02%] |
| `Iterator - Union - SkipTo Disjoint Interleaved 5 Children/Flat Quick/Rust` | 0.280 | 0.442 | +57.58% | [+38.96%, +75.70%] |
| `Iterator - Union - SkipTo Disjoint Interleaved 5 Children/Heap Full/Rust` | 0.374 | 0.564 | +50.98% | [+26.09%, +76.76%] |

### Numeric filter construction and traversal

Absolute means below are nanoseconds. Each read workload includes construction
and draining the 10,000-document fixture. Its timing is not a per-document cost.

| Filter / operation | Baseline ns | Candidate ns | Time change | Relative 95% CI |
| --- | ---: | ---: | ---: | --- |
| `equality/construct_direct` | 39.934 | 26.852 | -32.76% | [-35.55%, -30.05%] |
| `equality/construct_tree` | 82.586 | 54.262 | -34.30% | [-36.42%, -32.13%] |
| `equality/read_direct` | 257,887.515 | 219,968.855 | -14.70% | [-30.52%, +2.51%] |
| `equality/read_tree` | 210,538.909 | 151,003.283 | -28.28% | [-35.82%, -16.20%] |
| `partial/construct_direct` | 38.602 | 46.675 | +20.92% | [+2.19%, +42.26%] |
| `partial/construct_tree` | 88.062 | 53.486 | -39.26% | [-41.93%, -37.02%] |
| `partial/read_direct` | 241,597.696 | 215,785.920 | -10.68% | [-27.83%, +8.54%] |
| `partial/read_tree` | 220,819.884 | 185,416.442 | -16.03% | [-30.01%, +1.92%] |

The partial direct constructor measures +8.073 ns in this run, while several
other constructor cases appear substantially faster. This inconsistent pattern
is why all eight numeric cases are included in the controlled confirmation.

### Explicit advisory access

All 38 accessor cases completed on final code revision
`d0c078cf465d3b182927b7a1f5344c543be2d83c`, pinned to CPU 0. Each row compares
upper-bound access and advisory access on the same constructed candidate tree.
Values are mean nanoseconds per call with their absolute 95% intervals.
The added-nanoseconds column is a difference of point estimates and has no
joint confidence interval.

| Tree | Fanout | Upper bound ns [95% CI] | Advisory estimate ns [95% CI] | Added ns |
| --- | ---: | ---: | ---: | ---: |
| AndOfOr | 2 | 0.394 [0.385, 0.410] | 8.801 [8.680, 8.966] | 8.407 |
| AndOfOr | 8 | 0.400 [0.394, 0.408] | 86.999 [85.645, 88.546] | 86.599 |
| AndOfOr | 32 | 0.386 [0.384, 0.389] | 1,526.971 [1,519.049, 1,539.626] | 1,526.585 |
| AndOfOr FFI | 2 | 1.093 [1.039, 1.166] | 18.080 [17.690, 18.663] | 16.986 |
| AndOfOr FFI | 8 | 1.012 [1.006, 1.020] | 181.299 [174.487, 190.725] | 180.287 |
| AndOfOr FFI | 32 | 1.003 [0.999, 1.008] | 2,916.357 [2,908.209, 2,925.380] | 2,915.353 |
| IdList | — | 0.646 [0.505, 0.805] | 0.984 [0.978, 0.991] | 0.337 |
| Intersection | 2 | 0.387 [0.383, 0.392] | 3.051 [3.035, 3.068] | 2.665 |
| Intersection | 8 | 0.394 [0.391, 0.398] | 8.288 [8.227, 8.354] | 7.894 |
| Intersection | 32 | 0.395 [0.391, 0.402] | 31.191 [31.003, 31.400] | 30.795 |
| OrOfAnd | 2 | 0.407 [0.400, 0.416] | 8.602 [8.455, 8.779] | 8.195 |
| OrOfAnd | 8 | 0.404 [0.401, 0.409] | 75.857 [75.196, 76.671] | 75.453 |
| OrOfAnd | 32 | 0.416 [0.411, 0.422] | 1,065.440 [1,056.125, 1,077.366] | 1,065.024 |
| UnionFlat | 2 | 0.388 [0.386, 0.390] | 3.590 [3.431, 3.805] | 3.202 |
| UnionFlat | 8 | 0.396 [0.389, 0.407] | 10.963 [10.468, 11.610] | 10.567 |
| UnionFlat | 32 | 0.419 [0.403, 0.439] | 44.406 [44.118, 44.748] | 43.987 |
| UnionHeap | 2 | 0.406 [0.401, 0.411] | 3.559 [3.444, 3.742] | 3.154 |
| UnionHeap | 8 | 0.399 [0.396, 0.401] | 10.430 [10.316, 10.581] | 10.032 |
| UnionHeap | 32 | 0.399 [0.396, 0.403] | 44.316 [44.187, 44.464] | 43.917 |

The ID-list estimate measures 0.984 ns. Ordinary intersections and unions span
about 3–44 ns across fanouts 2–32. At fanout 32, nested trees contain 1,024
leaves: advisory access measures 1.065 µs for OR-of-AND, 1.527 µs for
AND-of-OR, and 2.916 µs for AND-of-OR through the C callback bridge.

These are explicit prediction-call costs, not per-document overhead or changes
in existing query execution. Large ratios against a cached, inlined upper-bound
load are not useful estimates of query regression. Sub-nanosecond results reflect
microbenchmark throughput under optimization, rather than isolated function-call
latency.

## CPU-pinned ABBA confirmation

### Status and inputs

The controlled experiment completed all 16 comparisons in both rounds, with
every benchmark process exiting successfully. Its results are reported
separately from the original measurements to expose order sensitivity rather
than replacing the initial experiment.

The confirmation baseline uses the same production revision
`04783c49fbdfed33ffd867aa16371df32b351e49`. Its numeric executable is frozen from
the original run. Its ordinary iterator executable is rebuilt offline at that
revision with the same C bundle and release profile, then frozen.

The confirmation candidate is `d0c078cf465d3b182927b7a1f5344c543be2d83c`.
It includes the shared estimator-helper cleanup, C test annotations,
lazy-iterator tests, and accessor-harness correction made after
`7720807303329e81271fc35f1a81926ee5537a1e`. The estimation formulas and traversal
logic are unchanged. The four frozen executable SHA-256 hashes recorded before
the controlled run are:

| Executable | SHA-256 |
| --- | --- |
| Baseline iterators | `c59c34298e41439b42e2b771539149b0fca8d99ff419b60ccd7bebe7b94b1082` |
| Baseline numeric | `92a5a2e484be03a650618502f5d63c179e684d0d507a38f6efa0b24c1563ac50` |
| Candidate iterators | `cb331452747ac0be50f982a92ea0ed59112a4e41f177f65901b09b090c0ae321` |
| Candidate numeric | `a2c0cf5c7448eac03e045f00f8c144e66e512b5d555383b245af7e4c63ae6d3e` |

### Selection and order

The six largest positive existing-suite point changes are selected, together
with two fixed controls. All eight numeric cases from the preceding table are
also included, for **16 cases in each comparison round**.

| Exact existing-suite label | Selection reason |
| --- | --- |
| `Iterator - OptionalOptimized - Read/Rust child_ratio=70` | Largest positive changes |
| `Iterator - OptionalOptimized - Read/Rust child_ratio=80` | Largest positive changes |
| `Iterator - Union - Read Disjoint Interleaved 50 Children/Flat Full/Rust` | Largest positive changes |
| `Iterator - Union - SkipTo Disjoint Sequential 5 Children/Flat Quick/Rust` | Largest positive changes |
| `Iterator - Union - SkipTo Disjoint Interleaved 5 Children/Flat Quick/Rust` | Largest positive changes |
| `Iterator - Union - SkipTo Disjoint Interleaved 5 Children/Heap Full/Rust` | Largest positive changes |
| `Iterator - IdList - Read Dense/Rust` | Fixed control |
| `Iterator - Not - Read Dense Child/Rust` | Fixed control |

The numeric labels are `Iterator - NumericFilter - ` followed by each
filter/operation in the eight-row numeric table. Both binaries run to completion
in each phase before the next phase begins:

| Phase | Binary revision | Criterion result directory | Baseline operation |
| --- | --- | --- | --- |
| A1 | Baseline | Round 1 | Save as `before` |
| B1 | Candidate | Round 1 | Compare against `before` |
| B2 | Candidate | Round 2 | Save as `after` |
| A2 | Baseline | Round 2 | Compare against `after` |

Each standalone executable is pinned to CPU 0. Sample size and durations match
the initial experiment: 20 samples, unchanged ordinary group durations, and
1-second measurement / 0.2-second warm-up for numeric cases. Frequency and power
management remain uncontrolled. No build or other benchmark runs concurrently.

### Reproducing the controlled run

Build the two benchmark targets with `cargo bench --no-run` in their respective
checkouts and copy the executable paths Cargo reports to the four paths below.
Each executable must retain its matching C bundle at link time. Do not rebuild
between phases. Record their hashes with `sha256sum` before running them.

This Bash example uses only standard tools and Criterion's CLI; it does not
depend on the local Python driver. The anchored filters select exactly the
cases above, and `--bench` enables measurement in the standalone executable.

```bash
set -o pipefail
baseline_iterators=/tmp/iterator-baseline-iterators
candidate_iterators=/tmp/iterator-candidate-iterators
baseline_numeric=/tmp/iterator-baseline-numeric
candidate_numeric=/tmp/iterator-candidate-numeric
sha256sum "$baseline_iterators" "$candidate_iterators" \
  "$baseline_numeric" "$candidate_numeric"

iterator_filter='^Iterator - ('
iterator_filter+='OptionalOptimized - Read/Rust child_ratio=(70|80)|'
iterator_filter+='Union - Read Disjoint Interleaved 50 Children/Flat Full/Rust|'
iterator_filter+='Union - SkipTo Disjoint Sequential 5 Children/Flat Quick/Rust|'
iterator_filter+='Union - SkipTo Disjoint Interleaved 5 Children/(Flat Quick|Heap Full)/Rust|'
iterator_filter+='IdList - Read Dense/Rust|Not - Read Dense Child/Rust)$'
numeric_filter='^Iterator - NumericFilter - (partial|equality)/'
numeric_filter+='(construct_direct|construct_tree|read_direct|read_tree)$'
run_root=$(mktemp -d /tmp/iterator-pinned-repro.XXXXXX)

run_phase() {
  local result_dir=$1 iterator_bin=$2 numeric_bin=$3
  local baseline_mode=$4 baseline_name=$5 phase_log=$6
  {
    CRITERION_HOME="$result_dir" taskset --cpu-list 0 \
      "$iterator_bin" "$iterator_filter" --bench --sample-size 20 \
      "$baseline_mode" "$baseline_name" &&
    CRITERION_HOME="$result_dir" taskset --cpu-list 0 \
      "$numeric_bin" "$numeric_filter" --bench --sample-size 20 \
      --measurement-time 1 --warm-up-time 0.2 \
      "$baseline_mode" "$baseline_name"
  } 2>&1 | tee "$phase_log" | tail -20
}

run_phase "$run_root/round1" "$baseline_iterators" "$baseline_numeric" \
  --save-baseline before "$run_root/A1.log" &&
run_phase "$run_root/round1" "$candidate_iterators" "$candidate_numeric" \
  --baseline before "$run_root/B1.log" &&
run_phase "$run_root/round2" "$candidate_iterators" "$candidate_numeric" \
  --save-baseline after "$run_root/B2.log" &&
run_phase "$run_root/round2" "$baseline_iterators" "$baseline_numeric" \
  --baseline after "$run_root/A2.log"
```

Normalize both rounds to candidate/base. Round 1 already reports that direction.
Round 2's recorded point change `d` becomes `1 / (1 + d) - 1`. Its relative
interval `[lower, upper]` becomes
`[1 / (1 + upper) - 1, 1 / (1 + lower) - 1]`, reversing the endpoints because
reciprocation is decreasing. A per-case two-round point summary is the geometric
mean of the two normalized ratios, minus one. No combined confidence interval
is synthesized.

### Controlled results

All percentages are candidate/base time changes. Round 2 has been normalized
by reciprocating its ratio and reversing the relative interval endpoints.
The final column is a geometric mean point estimate, with no synthetic
confidence interval. Both round intervals are 95%.

| Case | Round 1 change [95% CI] | Round 2 change [95% CI] | Two-round geometric mean point |
| --- | --- | --- | ---: |
| `Iterator - OptionalOptimized - Read/Rust child_ratio=70` | -0.38% [-0.85%, +0.06%] | -4.36% [-4.86%, -3.97%] | -2.39% |
| `Iterator - OptionalOptimized - Read/Rust child_ratio=80` | -1.70% [-2.23%, -1.14%] | -5.23% [-6.10%, -4.38%] | -3.48% |
| `Iterator - Union - Read Disjoint Interleaved 50 Children/Flat Full/Rust` | +2.67% [+0.09%, +5.32%] | -0.28% [-3.06%, +3.08%] | +1.18% |
| `Iterator - Union - SkipTo Disjoint Sequential 5 Children/Flat Quick/Rust` | -0.23% [-7.49%, +6.94%] | -6.43% [-14.59%, +2.28%] | -3.38% |
| `Iterator - Union - SkipTo Disjoint Interleaved 5 Children/Flat Quick/Rust` | +2.79% [+0.04%, +5.82%] | +1.27% [-1.71%, +4.66%] | +2.03% |
| `Iterator - Union - SkipTo Disjoint Interleaved 5 Children/Heap Full/Rust` | -0.62% [-3.18%, +1.68%] | -4.10% [-7.97%, +0.54%] | -2.38% |
| `Iterator - IdList - Read Dense/Rust (control)` | -59.24% [-62.20%, -55.21%] | -1.39% [-8.19%, +5.44%] | -36.60% |
| `Iterator - Not - Read Dense Child/Rust (control)` | -7.95% [-16.25%, +1.12%] | +18.08% [+6.96%, +29.48%] | +4.25% |
| `Iterator - NumericFilter - equality/construct_direct` | +5.83% [+4.96%, +6.68%] | -3.80% [-10.43%, +1.87%] | +0.90% |
| `Iterator - NumericFilter - equality/construct_tree` | +9.02% [+8.34%, +9.63%] | +6.76% [+3.77%, +9.36%] | +7.89% |
| `Iterator - NumericFilter - equality/read_direct` | -2.10% [-2.82%, -1.34%] | -0.28% [-1.27%, +0.75%] | -1.19% |
| `Iterator - NumericFilter - equality/read_tree` | -3.09% [-5.19%, -1.37%] | +0.83% [-0.20%, +2.02%] | -1.15% |
| `Iterator - NumericFilter - partial/construct_direct` | +8.10% [+2.62%, +12.36%] | +5.82% [+4.31%, +7.75%] | +6.96% |
| `Iterator - NumericFilter - partial/construct_tree` | +12.81% [+9.48%, +17.92%] | +4.64% [+3.76%, +5.39%] | +8.65% |
| `Iterator - NumericFilter - partial/read_direct` | +1.55% [-3.49%, +8.29%] | -1.66% [-2.08%, -1.31%] | -0.07% |
| `Iterator - NumericFilter - partial/read_tree` | +0.40% [-0.82%, +1.37%] | -4.51% [-8.03%, -1.26%] | -2.08% |

### Controlled numeric absolute means

All values are nanoseconds per benchmark iteration. Read cases construct and
drain the 10,000-document fixture; constructor cases construct and drop it.

| Filter / operation | Round 1 baseline ns | Round 1 candidate ns | Round 2 baseline ns | Round 2 candidate ns |
| --- | ---: | ---: | ---: | ---: |
| `equality/construct_direct` | 24.696 | 26.137 | 26.709 | 25.694 |
| `equality/construct_tree` | 48.306 | 52.664 | 50.495 | 53.909 |
| `equality/read_direct` | 140,051.018 | 137,116.406 | 137,950.527 | 137,562.541 |
| `equality/read_tree` | 140,915.738 | 136,563.370 | 138,677.534 | 139,833.205 |
| `partial/construct_direct` | 25.709 | 27.792 | 25.389 | 26.867 |
| `partial/construct_tree` | 48.714 | 54.955 | 51.102 | 53.471 |
| `partial/read_direct` | 137,465.408 | 139,600.223 | 133,170.760 | 130,962.610 |
| `partial/read_tree` | 133,967.160 | 134,505.480 | 139,129.877 | 132,859.301 |

### Interpretation of the controlled measurements

- Tree-based equality and partial-range construction, and direct partial-range
  construction, increase in both rounds with both relative intervals above zero.
  These are the consistent constructor overheads summarized at the start of this
  report. Direct equality construction changes direction between rounds:
  its +0.90% combined point is not evidence of a consistent regression.
- No tested traversal case has both relative intervals entirely above zero.
  The flat quick union with disjoint interleaved children has positive point
  changes in both rounds (+2.03% combined), but its second-round interval crosses
  zero. The experiment therefore does not establish a reproducible traversal
  regression for that case.
- The fixed controls still move substantially: ID-list dense reads change by
  −59.24% and −1.39%, while NOT dense-child reads change by −7.95% and +18.08%.
  These controls demonstrate remaining execution-state sensitivity despite
  pinning. The aggregate of this selected subset is not used as a query-overhead
  estimate.
- The subset was chosen from the six largest positive original point changes
  plus controls and all numeric cases. It is intentionally biased toward prior
  apparent regressions; its results are not a representative production workload.

## Validation scope

The initial implementation revision `7720807303329e81271fc35f1a81926ee5537a1e`
passed CI build and lint checks, the full Rust and C/C++ test suites, and QUICK
behavioral testing in standalone and coordinator configurations. CI filters
known-flaky tests, and QUICK omits extended behavioral-test variants.
The final code revision `d0c078cf465d3b182927b7a1f5344c543be2d83c` passed 1,351
local tests, formatting checks, and compilation of all benchmark targets used
here. Independent review reported no findings. The latest CI rerun has not
completed; the earlier CI result applies to the initial revision.

## Limits and local evidence

- The initial experiment is a single baseline-then-candidate run on an unpinned
  laptop-class CPU. Scheduling, power management, thermal drift, and unrelated
  system work can affect results. Opposite changes across unrelated cases are
  not proof of a code-induced speedup or regression. The separate confirmation
  pins a core and reverses order, but does not control all of those factors.
- Short constructor/accessor operations are sensitive to measurement overhead.
  Confidence intervals and absolute nanoseconds are more informative than large
  percentages alone.
- These are microbenchmarks. They do not measure end-to-end Redis throughput,
  coordinator fan-out, concurrent queries, or application latency.
- The synthetic datasets measure cost, not prediction accuracy on real correlated
  predicates or skewed/multi-valued numeric distributions. The estimator's
  assumptions remain those in [the design](design.md).
- Allocation layout changes may have effects beyond the directly measured Rust
  iterator paths. The callback benchmark measures estimation dispatch, not the
  complete Redis command pipeline.

Local evidence files for this experiment:

- Baseline log: `/tmp/iterator-baseline-benches.zvxsAF.log`.
- Candidate log: `/tmp/iterator-candidate-benches.z8VURw.log`.
- Criterion JSON and reports: `/tmp/iterator-match-estimates-criterion/`.
- Analysis script: `/tmp/summarize_iterator_estimates.py`.
- Initial result summaries: `/tmp/iterator-estimates-summary.md` and
  `/tmp/iterator-estimates-summary.json`.
- Controlled selection and argument plan: `/tmp/iterator-pinned-plan.json`.
- Controlled driver and analyzer: `/tmp/run_iterator_pinned.py`.
- Controlled run metadata and hashes:
  `/tmp/iterator-match-estimates-pinned-metadata.json`.
- Controlled results: `/tmp/iterator-match-estimates-pinned-1/` and
  `/tmp/iterator-match-estimates-pinned-2/`.
- Controlled summaries: `/tmp/iterator-match-estimates-pinned-summary.md` and
  `/tmp/iterator-match-estimates-pinned-summary.json`.
- Accessor and ABBA execution log: `/tmp/iterator-controlled-benches.fC2r7Q.log`.

These paths identify local run artifacts; the recorded tables and reproduction
commands are the durable PR evidence.
