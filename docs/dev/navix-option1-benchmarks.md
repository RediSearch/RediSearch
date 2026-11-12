# Option 1: broader benchmark results

Option 1 improves the numeric 1M-vector workload after reducing search effort, ties the sparse TAG case, and regresses on the smaller high-dimensional workload. It is not a universal replacement for AUTO.

## Method

AWS c7i.4xlarge; six Search workers; server CPUs 0–6/8–14 and client CPUs 7/15; loopback. Fresh, fully ingested static RAM graphs, FLOAT32 cosine, M=64, EF_CONSTRUCTION=512, K=25. Each case compares policies on the same graph. The default AUTO policy is unchanged.

Screen: the same first 100 queries at EF 128/256/512/640/768/1024/1536/2048/4096 for both policies. Expand qualifying mean/median leaders and neighboring EF settings to the first 1,000 queries, then run three paired repeats with rotated policy order. Each run has a separate 100-query warm-up. The 1,000-query sample is used for both tuning and these repeats; full-query follow-up is required for a held-out claim.

Select AUTO by lowest mean among expanded settings with at least 95% recall. Prefer a candidate with at least that recall; if none qualifies, retain the expanded candidate with highest recall as a conservative diagnostic. The working quality floor is 95%: the faster 95%-floor high-dimensional candidate is reported separately below.

Recall uses available ground-truth neighbors up to 25. Short or empty ground-truth rows are checked against exact filter counts; they are not counted as underfilled results merely because fewer than 25 documents exist. All eligibility checks are untimed.

## Three paired passes per case

| Case | Policy | EF | Mean ms | Median ms | p95 ms | p99 ms | QPS | Recall |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1M × 100D numeric | AUTO | 640 | 37.789 | 17.315 | 132.908 | 187.467 | 26.456 | 96.220% |
| 1M × 100D numeric | NAVIX_AUTO | 768 | 21.063 | 16.651 | 41.184 | 104.688 | 47.453 | 96.812% |
| 100K × 2048D numeric | AUTO | 768 | 37.023 | 32.849 | 86.138 | 96.975 | 27.000 | 99.992% |
| 100K × 2048D numeric | NAVIX_AUTO | 1024 | 56.882 | 48.985 | 98.006 | 139.821 | 17.575 | 97.024% |
| 1M × 100D TAG | AUTO | 1024 | 1.257 | 1.108 | 2.176 | 2.310 | 790.251 | 100.000% |
| 1M × 100D TAG | NAVIX_AUTO | 2048 | 1.262 | 1.110 | 2.172 | 2.282 | 787.039 | 100.000% |

## Interpretation

- Numeric 1M/100D: mean falls 44.3%, median 3.8%, p95 69.0%, and p99 44.2%. Recall rises 0.592 percentage points. 52.6% of query IDs are faster, while 22.6% are over 10% slower. The aggregate win does not remove individual regressions.
- Numeric 100K/2048D: Option 1 forces traversal for filters above its fixed 2% cutoff even when AUTO prefers exact scanning for the index shape. This regresses latency and loses recall. The next policy should preserve that existing scan preference.
- Sparse TAG: effectively tied. All sampled profiles take exact ad-hoc scans in both policies, so EF does not control this path. The small timing differences do not demonstrate a NaviX traversal benefit; broader TAG predicates remain unmeasured.

The fastest high-dimensional expanded candidate above the 95% recall floor is NAVIX_AUTO EF=768: mean 50.598 ms, median 42.128 ms, p99 139.679 ms, recall 95.268%. This is one 1,000-query pass, not the repeated comparison above. It also regresses against AUTO.

## Correctness and reproducibility

- 1M × 100D numeric: 1,002 independently checked queries per policy, with zero ineligible IDs, duplicates, underfilled or overfilled results. Same server run ID across all paired timing passes: `8c5b51781479146f47aae59f92d7a67928d0fd82`.
- 100K × 2048D numeric: 1,028 independently checked queries per policy, with zero ineligible IDs, duplicates, underfilled or overfilled results. Same server run ID across all paired timing passes: `3d8f31ad8a7a70e5d8a5183c637bcba4f70ebdff`.
- 1M × 100D TAG: 4,000 independently checked queries per policy, with zero ineligible IDs, duplicates, underfilled or overfilled results. Same server run ID across all paired timing passes: `f265e758cad0085a45e8eeb0ebc998b253993cd4`.

All 60,300 driver requests, including warm-ups, succeeded; zero failed or dropped queries.

Measured source pins:

- RediSearch: `9a4acad28c196f69814930ef58d366f30994196d`.
- VecSim: `8f785a8d190032ac71b72a199f355c5fcba31019`.
- Redis: `01ed15184bf4369c922c33c20705dbb5982d13da`.
- Rust benchmark driver: `d3f4cc350b24b4cdc71432784e524b4ff76dec4d`.
- Module SHA-256: `c403cdfdb940529215ba2e34367d9978eee59f0e894b715cf24c8a0d96761477`.

The public dataset fixtures are `random-range-100-angular-filters`, `random-match-keyword-100-angular-filters`, and `random-range-2048-angular-filters`. Corpus, payload and query SHA-256 values are recorded with the local raw evidence and scripts in `outputs/navix-options-goal-20261112/`. Timing arrays preserve query order only for single-client runs. Profiles are diagnostic and are not substituted for the timed driver measurements.

RediSearch CI at the measured source passed build, lint, unit, standalone and coordinator flow tests. Draft-gated sanitizer, coverage, Miri and Rust Valgrind jobs did not run. These results do not establish persistence, concurrent-update, production, Flex or OpenSearch behavior. Prefetch is included in the stack but its contribution is not isolated.

The earlier 10,000-query EF=1024 result used a different graph and is not pooled with these samples. Next: preserve Option 1 as a draft, protect index shapes where exact scanning is already preferred, and refine the median on full queries at high recall.
