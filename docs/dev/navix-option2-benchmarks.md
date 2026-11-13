# NaviX POC: Option 1 and the latency follow-up

[Option 1 RediSearch draft](https://github.com/RediSearch/RediSearch/pull/12323) and [VecSim draft](https://github.com/RedisAI/VectorSimilarity/pull/1119) preserve the baseline implementation. [Option 2 RediSearch draft](https://github.com/RediSearch/RediSearch/pull/12327) is stacked on Option 1; it uses the same VecSim revision.

## Result

On the million-vector numeric workload, Option 2 versus tuned AUTO gives mean 53.9% lower, median 13.1% lower, p95 75.1% lower, p99 74.6% lower latency. On the broader categorical workload, the comparison is mean 25.3% lower, median 4.1% lower, p95 11.1% lower, p99 6.8% lower. The tables include observed maxima, throughput and recall rather than treating a faster mean as sufficient.

These are workload-level measurements; they do not show that every query or dataset is faster. High-dimensional numeric and already-sparse TAG cases are reported separately below. The working quality floor is 95% **mean recall@25**, not 95% recall for every individual query.

## What changed

`NAVIX_AUTO2` chooses among exact filtering plus vector scoring, ordinary AUTO, and NaviX traversal:

- Keep exact scans when the whole-index cost heuristic already favors them, or the existing filter estimate is at most 2% of the vector population.
- For conjunctions, sample at most 512 deterministic document summaries. At most eight MAYBEs selects an exact scan; the ninth stops sampling. Probe only when the queried vector field covers every indexed document. Unknown summaries count as MAYBE.
- Restore ordinary AUTO for TAG non-conjunctions, and for conjunctions containing a TAG union of at least 32 terms when the estimate covers at least half the vectors. This path preserves the caller's EF and adaptive batch-to-scan switching. Sparse scan selection takes precedence.
- Otherwise retain Option 1's scaled search effort and bridge traversal. RediSearch still checks the actual predicate and refills results.

The probe selects a route; it never rejects a result or substitutes for exact filtering. These thresholds are experimental heuristics informed by this tuning corpus. `AUTO` and `NAVIX_AUTO` remain unchanged. The new policy is default-off behind `ENABLE_UNSTABLE_FEATURES`, for filtered, unquantized, single-value FLOAT32 RAM HNSW.

## Numeric: 1M vectors × 100 dimensions

Three rotated passes over all 10,000 query IDs, K=25, one client. Percentiles pool 30,000 observations per policy/EF setting; QPS is total requests divided by total measured duration.

| Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 576 | 37.379 | 16.884 | 134.023 | 189.204 | 311.592 | 26.745 | 95.5728% |
| Option 1 | 640 | 19.760 | 15.052 | 42.698 | 102.781 | 389.672 | 50.582 | 95.5940% |
| Option 2 | 640 | 17.239 | 14.678 | 33.392 | 48.078 | 91.602 | 57.974 | 95.6136% |
| Option 2 | 608 | 16.849 | 14.301 | 32.770 | 47.925 | 91.077 | 59.316 | 95.2264% |

Median reduction: 13.07%, paired-query bootstrap 95% interval 11.87%–14.19%, conditional on this graph. Mean recall difference: +0.0408 percentage points.

Averaging the three timings for each query, 72.06% of query IDs are faster, 6.25% are over 10% slower, and 0.60% are over twice as slow. The largest ratio is query 3193: 23.431 ms AUTO versus 68.111 ms Option 2 (2.91×). Per-query recall p10 is 88.0% versus 88.0%; aggregate recall hides shape/selectivity differences.

| Numeric predicate | AUTO median ms | Option 2 median ms | AUTO mean recall@25 | Option 2 mean recall@25 |
|---|---:|---:|---:|---:|
| and_2 | 46.211 | 22.318 | 97.644% | 97.707% |
| or_2 | 15.306 | 14.381 | 94.359% | 95.501% |
| single | 13.301 | 11.511 | 94.715% | 93.634% |

Selectivity-cohort metrics and raw per-query timings are retained in the JSON evidence.

AUTO uses EF576; Option 2 uses EF640. Thus the comparison includes search-effort selection. At the same EF as Option 1, the incremental reductions are 12.76% mean, 2.49% median, 21.80% p95 and 53.22% p99. This isolates the routing improvement more directly. The repeated lower-effort alternative EF608 reaches 14.301 ms median, 47.925 ms p99 and 95.2264% mean recall; it is a separate latency/recall tradeoff. The full-query expansion is retained separately from the confirmation passes.

## Numeric concurrency: 100 clients

Three rotated passes of 10,000 requests per policy; these percentiles are separate from single-client results. Closed-loop throughput is not an open-loop capacity limit.

| Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 576 | 606.035 | 603.351 | 726.354 | 799.380 | 904.459 | 164.085 | 95.5728% |
| Option 2 | 640 | 276.156 | 276.029 | 299.930 | 313.956 | 359.874 | 360.174 | 95.6136% |
| Option 2 | 608 | 269.396 | 269.237 | 293.089 | 307.599 | 352.594 | 369.204 | 95.2264% |

Median response time is 54.3% lower, and throughput is 119.5% higher. Driver saturation, errors and independent validation are checked separately from latency.

## Broad categorical TAG filters

1M × 100D with deterministic 64-category fields, 1,200 queries: 400 single-field predicates, 400 two-field ANDs, 400 two-field ORs; 1/4/16/48 categories per leaf. Actual selectivity spans approximately 0.005%–94.9%. Exact float64 cosine ground truth is computed independently from source vectors; runtime routing does not use those filter counts. Three rotated passes on a fresh graph confirm the EF512 setting selected from the earlier 100-query screen and full 1,200-query expansion. EF512 was frozen before building this confirmation graph. A separate strict-recall-matching comparison is retained in `03-tag64-v5-confirm-metrics.json`.

| Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 512 | 20.456 | 8.622 | 72.697 | 78.638 | 129.231 | 48.857 | 95.5567% |
| Option 1 | 512 | 20.965 | 9.503 | 114.617 | 139.723 | 427.522 | 47.671 | 95.1833% |
| Option 2 | 512 | 15.271 | 8.266 | 64.625 | 73.295 | 129.187 | 65.433 | 95.5167% |

Median reduction: 4.13%, paired-query 95% interval 2.63%–6.12%. Mean recall difference: -0.0400 percentage points. 53.08% of query IDs are faster and 1.75% are over 10% slower.

The stricter recall-matching comparison on the preceding graph selected EF576: mean recall 95.573% AUTO versus 96.180% Option 2, but median 8.634 versus 8.819 ms. It improved tails while spending more effort on common queries. The EF512 confirmation above follows the user's preference for high recall and lower median, rather than requiring higher recall than AUTO.

The previous runtime `aa902daf` improved TAG mean but regressed median (8.742 → 10.725 ms) and p99 (77.856 → 121.300 ms). Those measurements motivated the TAG fallback and wider sparse probe; they are preserved in `option2-v4-benchmarks.md` and are not mixed with current timings. The initial TAG fixture attempt failed before search because the driver required root AND/OR groups; one-element AND wrappers repaired single predicates without changing vectors, predicate meaning or ground truth. Failed setup is excluded from measured requests.

## High-dimensional numeric: 100K × 2048D

Three rotated passes of 1,000 query IDs, both policies EF768.

| Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 768 | 36.904 | 32.716 | 86.002 | 96.702 | 104.872 | 27.088 | 99.9920% |
| Option 2 | 768 | 36.983 | 32.614 | 86.037 | 96.600 | 104.505 | 27.030 | 99.9920% |

Option 2 versus AUTO: mean +0.22%, median -0.31% (positive is slower). The paired 95% interval for median reduction is -0.55%–0.79%. This predominantly exact-scan case checks that the large Option 1 regression is avoided; it does not establish a NaviX traversal benefit.

## Original sparse TAG: 1M × 100D

Three rotated passes of 1,000 query IDs, both policies EF768.

Of these 1,000 queries, 333 have fewer than 25 matching documents, including 106 with no matches. Recall uses the available ground-truth count as its denominator and assigns 1.0 to an empty ground-truth set. This workload therefore includes cheaper empty-result queries; its recall and latency should not be compared directly with the broader TAG case.

| Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 768 | 1.225 | 1.075 | 2.150 | 2.269 | 2.429 | 810.579 | 100.0000% |
| Option 2 | 768 | 1.223 | 1.094 | 2.143 | 2.257 | 2.549 | 812.460 | 100.0000% |

Option 2 versus AUTO: mean -0.19%, median +1.73% (positive is slower). The paired 95% interval for median reduction is -3.59%–-0.96%. Already-selective filters favor exact scanning; this case must not be counted as evidence for graph-traversal gains.

## Method and limits

AWS c7i.4xlarge, six Search workers, server CPUs0–6/8–14 and client CPUs7/15, loopback. FLOAT32 cosine HNSW, M64, EF_CONSTRUCTION512, K25, persistence disabled. Fresh fully ingested static graphs, one graph shared by every compared policy in each case. No build or other benchmark runs alongside timed queries. Each cell has its own warm-up; profiling and independent ID validation run outside timing.

Numeric effort selection prefers the lowest-median candidate matching AUTO mean recall, or a candidate above the 95% floor if none matches. TAG EF512 was selected for better median/tails at high mean recall, accepting a small recall difference from AUTO; its stricter recall-matching comparison remains separately recorded. All query IDs were available during development/tuning: this is fresh-graph replication, not an unseen-query holdout. Bootstrap resamples query IDs with repeated observations together and does not capture host or graph-build variation. Observed maxima are descriptive, not stable tail estimates. Concurrent driver output lacks query-ID order and is not used for paired-query analysis.

AUTO uses the same POC binary and summary-enabled index. This is not a stock-release, Flex, ROR or OpenSearch comparison. No claim is made about production persistence, concurrent updates or cluster performance.

## Conclusion

The POC demonstrates a substantial numeric latency improvement with high mean recall, including a repeatable median gain and higher throughput under load. It supports keeping Option 2 as an opt-in candidate for further development. It does **not** support replacing AUTO everywhere: the high-dimensional mean, sparse-TAG median, and some individual numeric queries still regress. The next performance work should improve those routing decisions, then confirm them on workloads that were not used for tuning. Production rollout also needs the deferred persistence, update and coordinator work.

## Reproducibility and checks

- RediSearch runtime: `5f1e23be8057a2a52260dc8dad5365a41cb18e8a`.
- VecSim: `8f785a8d190032ac71b72a199f355c5fcba31019`.
- Redis: `01ed15184bf4369c922c33c20705dbb5982d13da`.
- Benchmark driver: `d3f4cc350b24b4cdc71432784e524b4ff76dec4d`.
- Release module SHA256: `fec297d2dc6583447a8e081baa41163386b2ab5b0ef03f4d4ae2a9e5ebea1b16`.

| Case | Graph run ID |
|---|---|
| Numeric | `c900abecd7bd19bbb8c3f9eb44a5630a10b2cf61` |
| Broad TAG | `45ca57b7b89e231292f3c995f234e1cbd7d44730` |
| High-dimensional numeric | `44df9bb60755b5dea5be2fb824bf52ffaee572d4` |
| Sparse TAG | `ccd29e7ad32300ef5550cf04efc8c15c34f7178b` |

Independent result validation found zero ineligible IDs, duplicates, underfilled or overfilled results:

- range100: AUTO EF576 (10,000 queries), NAVIX_AUTO2 EF608 (10,000 queries), NAVIX_AUTO2 EF640 (10,000 queries)
- tag64: AUTO EF512 (1,200 queries), NAVIX_AUTO EF512 (1,200 queries), NAVIX_AUTO2 EF512 (1,200 queries)
- range2048: AUTO EF768 (1,028 queries), NAVIX_AUTO2 EF768 (1,028 queries)
- tag100: AUTO EF768 (4,000 queries), NAVIX_AUTO2 EF768 (4,000 queries)

The repeated comparisons above contain 232,800 successful requests, excluding warm-ups and effort-selection passes. They report zero failed/dropped requests and no client saturation. The public source fixtures are `random-range-100-angular-filters`, `random-range-2048-angular-filters` and `random-match-keyword-100-angular-filters`; the broad TAG case derives its vectors from the last fixture. Raw timing arrays, plans, corpus SHA256 hashes, validation records and scripts are retained with the local report artifacts. Assertions build and all six focused standalone flow tests passed. Runtime `5f1e23be80` draft CI completed with 25 passed checks and no failures, including build/lint, unit, standalone and all three coordinator suites. Independent review found no concrete correctness defect but left two P2 coverage findings open: EF/adaptive-switch preservation on TAG fallback, and dedicated coordinator execution tests. The PR remains draft; coverage/sanitizer/Miri/Rust-Valgrind lanes are draft-gated.
