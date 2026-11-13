# Option 2: avoid unnecessary NaviX traversal

Status: completed prior iteration `aa902daf`. This report is historical evidence; it does not describe the later TAG-routing revision. The broad TAG results at the end identify the remaining median/p99 regression.

Option 1 is preserved in [RediSearch draft 12323](https://github.com/RediSearch/RediSearch/pull/12323) and [VecSim draft 1119](https://github.com/RedisAI/VectorSimilarity/pull/1119). The follow-up is [RediSearch draft 12327](https://github.com/RediSearch/RediSearch/pull/12327), stacked on Option 1. VecSim is unchanged by Option 2.

## What changed

The opt-in `NAVIX_AUTO2` policy keeps the existing preference for exact scanning when the index is small relative to its vector dimensionality. For conjunctions with misleading cardinality estimates, a bounded sample of document summaries detects very sparse intersections and selects an exact scan. On other supported queries it uses the existing NaviX path.

The sample checks at most 512 deterministic document IDs and stops at the third possible match. Unknown or absent summaries count as possible matches. Sampling is restricted to indexes where every document has the queried vector field; it affects route selection only. RediSearch still applies the actual predicate before returning results. Existing `AUTO` and `NAVIX_AUTO` behavior is unchanged.

## Numeric confirmation: three rotated passes

1M vectors, 100 dimensions, 10,000 numeric query IDs, K=25, one client. Each policy executes all queries three times on the same graph and release binary. Percentiles pool the 30,000 observations per policy; QPS is total completed requests divided by total measured duration.

| Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 576 | 37.134 | 16.812 | 132.388 | 186.870 | 301.465 | 26.922 | 95.5900% |
| Option 1 | 640 | 20.035 | 15.175 | 43.532 | 104.460 | 373.337 | 49.887 | 95.6020% |
| Option 2 | 640 | 18.462 | 14.955 | 37.916 | 69.800 | 186.699 | 54.135 | 95.6052% |

The quality-matched Option 2 setting reduces mean latency by 50.3%, median by 11.1%, p95 by 71.4%, p99 by 62.6%, and observed maximum by 38.1% against tuned AUTO; throughput rises 101.1%. The paired-query 95% bootstrap interval for the median reduction is 10.0%–12.3%, conditional on this graph. Recall is effectively matched: the 0.0152 percentage-point difference has an interval spanning zero.

At the same EF as Option 1, the incremental reductions are 7.9% mean, 1.5% median, 12.9% p95, and 33.2% p99. The larger AUTO comparison includes search-budget tuning; the probe alone does not account for the entire gain.

These aggregate improvements do not mean every query wins. Averaging each query's three observations, 67.86% of query IDs are faster, 9.59% are over 10% slower, and 1.33% are over twice as slow. The 1–2% selectivity cohort still has a slower median than AUTO. Overall recall also hides differences between predicate shapes: single-predicate recall is 93.63% for Option 2 versus 94.74% for AUTO. The accepted 95% working floor is an overall recall criterion; the per-query 10th percentile is 88% for both primary policies. The largest remaining individual latency increase is query 2146 (0.2052% selectivity): 8.72 ms with AUTO versus 186.36 ms with Option 2, averaged over the three passes. The bounded probe does not eliminate every sparse-query regression.

A secondary EF=608 setting, measured in one full pass only, yields 17.804 ms mean, 14.380 ms median, 36.882 ms p95, 68.368 ms p99, and 95.2136% recall. It trades some recall for lower latency and is not the repeated primary comparison.

The 12 largest earlier sparse-conjunction regressions now select exact scanning. For query 8224, an untimed `FT.PROFILE` diagnostic falls from about 364 ms with Option 1 to 7.5 ms with Option 2. Those profiler values are not substituted for client timings. All 12 queries return 25 independently validated, eligible, unique IDs under every compared policy.

## Numeric concurrency: 100 clients

Three rotated passes of 10,000 requests per policy on the same graph. Percentiles pool observations within this concurrency level only.

| Policy | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 597.413 | 593.551 | 720.304 | 790.232 | 906.088 | 166.495 | 95.5900% |
| Option 2 | 293.160 | 292.488 | 325.696 | 350.615 | 465.134 | 339.381 | 95.6052% |

Median response time falls 50.7% and throughput rises 103.8%. All latency aggregates improve in each repeated pass. The driver reports no client saturation, failed requests, or dropped requests. These are closed-loop load results, not an open-loop capacity limit.

Untimed independent validation checks all 10,000 query IDs for each primary policy: zero ineligible IDs, duplicates, underfilled results, or overfilled results. The repeated single-client and 100-client comparisons contain 150,000 successful requests excluding warm-ups; the initial check and secondary effort setting are separate.

## High-dimensional numeric case

100K × 2048D, EF=768 for every policy, three rotated passes of 1,000 query IDs on a fresh graph.

| Policy | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 38.755 | 34.433 | 89.679 | 101.282 | 108.615 | 25.794 | 99.992% |
| Option 1 | 50.847 | 42.858 | 93.687 | 141.062 | 264.274 | 19.660 | 95.192% |
| Option 2 | 38.932 | 34.739 | 90.312 | 101.150 | 107.944 | 25.676 | 99.992% |

Option 2 recovers the large Option 1 regression by retaining exact scans. It does not beat AUTO here: measured mean is 0.46% higher and median 0.89% higher, with identical recall. The paired-query interval for mean overhead is 0.33%–0.56%; the median interval spans zero. Do not count this as a NaviX speedup.

Independent validation covers 1,028 queries per policy, including short ground-truth rows, with zero eligibility, duplicate, underfill, or overfill errors. Graph run ID: `873749eee31dcda47436cb0267987b67761a6c53`.

## Original sparse TAG case

1M × 100D, EF=768 for every policy, three rotated passes of 1,000 query IDs on a fresh graph.

| Policy | Mean ms | Median ms | p95 ms | p99 ms | Max ms | QPS | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AUTO | 1.297 | 1.129 | 2.249 | 2.381 | 2.626 | 765.870 | 100.000% |
| Option 1 | 1.289 | 1.128 | 2.230 | 2.342 | 2.676 | 770.620 | 100.000% |
| Option 2 | 1.301 | 1.134 | 2.252 | 2.397 | 2.739 | 763.724 | 100.000% |

Performance is effectively unchanged: pooled Option 2 mean is 0.31% higher and median 0.44% higher than AUTO, with the direction varying between repeats. All five sampled profiles per policy use exact ad-hoc scanning, so this case does not demonstrate a NaviX traversal benefit.

Independent validation covers 4,000 queries per policy, including short and empty ground-truth rows, with zero eligibility, duplicate, underfill, or overfill errors. Graph run ID: `f5dff4aa6e0d0e91193cfb8592a96514934fe0f1`.

## Confirmation protocol and limits

The suite includes three 100-client passes, independent checks of all 10,000 numeric result sets, and repeated comparisons on 100K × 2048D numeric and original sparse TAG data. A fourth case reuses the 1M × 100D TAG vectors with deterministic 64-category labels and 1,200 new single/AND/OR predicates, covering approximately 0.005%–94.9% selectivity. It has a separate exact cosine oracle; a first-100-query screen selected settings for expansion to all 1,200 queries and three rotated repeats.

AWS c7i.4xlarge, six Search workers, server CPUs 0–6/8–14, client CPUs 7/15, loopback. FLOAT32 cosine HNSW, M=64, EF_CONSTRUCTION=512; ingestion is fully drained before timing. Each timed cell has a separate warm-up. Recall uses available exact neighbors up to K=25, accounting for short ground truth. Validation and profiling run outside timed cells. Persistence is disabled during these static-index measurements.

The numeric corpus was inspected while designing the probe. This is fresh-graph replication on a tuning corpus, not an unseen-query holdout. Bootstrap intervals resample query IDs with their repeated observations together and are conditional on the measured graph. Concurrent driver output does not preserve query-ID order and is analyzed separately. Maximum latency is descriptive, not a stable percentile estimate.

AUTO runs on the same POC build and summary-enabled index as the candidates. This is not a stock-release, Flex, ROR, or OpenSearch comparison. The measurements do not establish persistence, concurrent-update, or production behavior.

## Reproducibility and verification

- RediSearch runtime: `aa902daf5bfde9c375999891c09e9f0e36af1181`.
- VecSim: `8f785a8d190032ac71b72a199f355c5fcba31019`.
- Redis: `01ed15184bf4369c922c33c20705dbb5982d13da`.
- Rust benchmark driver: `d3f4cc350b24b4cdc71432784e524b4ff76dec4d`.
- Release module SHA-256: `8af9fd2a4fac985de4b39343a6a20343febebdaed24398b34a4ea61f1c874bff`.
- Numeric graph run ID: `3480db0c3538350b2c235333bed501c3a13bb590`.

Plans, invocation logs, raw timing arrays, corpus hashes, independent validation, and analysis scripts are retained with the local benchmark artifacts. The five focused assertions-enabled flow tests pass. Draft CI for runtime `aa902daf` completed with 23 passed checks and no failures, including build, lint, unit, standalone, and coordinator suites. Draft-gated coverage, sanitizer, Miri, and Rust Valgrind jobs did not run. Independent review found no concrete correctness defect; dedicated coordinator coverage of the new policy remains open before merge.

## Completed broad TAG case (prior iteration aa902daf)

Three rotated passes of 1,200 queries on graph `9ef35c90c535fd412ddb31f16ea41acc9305b6c7`. All three policies passed independent eligibility, duplicate and result-count checks on all 1,200 query IDs.

| Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | Mean recall@25 |
|---|---:|---:|---:|---:|---:|---:|
| AUTO | 512 | 20.409 | 8.742 | 72.153 | 77.856 | 95.583% |
| Option 1 | 640 | 22.381 | 11.270 | 116.197 | 140.785 | 96.453% |
| Option 2, prior routing | 640 | 15.248 | 10.725 | 30.934 | 121.300 | 96.453% |

This iteration regresses median and p99 against AUTO despite improving mean and p95; it motivated the next routing experiment. The initial fixture setup aborted before search due to unsupported bare root leaves; one-element AND wrappers repaired the driver encoding without changing predicates, query vectors or exact truth. All 1,200 queries were available during tuning.
