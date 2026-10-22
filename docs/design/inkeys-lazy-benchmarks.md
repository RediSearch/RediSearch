# Lazy key-map backfill measurements

This experiment compares the draft's historical-ID scan with the resumable
bucket-chain cursor and background capacity reservation. It measures first use,
backfill duration, time spent inside the batch while holding the spec write lock,
and query/write latency during construction and after readiness.

## Procedure

The harness is `tests/benchmarks/inkeys_lazy_map.py`. Timing uses the accompanying
`inkeys_lazy_map_timing.patch`, applied only to measurement builds; production
code contains no timing instrumentation. The patch measures batch execution after
acquiring the write lock and before releasing it. Lock acquisition wait is excluded.

Run on an Intel Core Ultra 7 255U, Linux x86-64, with CPU energy-performance
preference set to `performance`. Redis is development commit `559d143b`, built with
jemalloc. Both modules use release optimization (`RelWithDebInfo`, Rust release)
with `ENABLE_ASSERT=1`. GC is disabled to isolate backfill; this is
not an end-to-end capacity or sustained-load certification.

Each case runs in a fresh Redis process and data directory. One client repeatedly
queries a single indexed key with INKEYS. The write variant adds one client updating
an indexed numeric field, beginning 300 ms before activation and continuing until
300 ms after readiness. Query results are checked throughout. WORKERS is either
0 or 2.

| Case | Live documents | IDs assigned during setup | Document-table bucket limit |
| --- | ---: | ---: | ---: |
| Dense | 1,000,000 | 1,000,000 | 1,000,000 |
| Churn | 100,000 | 2,000,000 | 1,000,000 |
| Long chains | 200,000 | 200,000 | 1,000 |

Churn repeatedly updates the same documents; it does not leave two million live
keys. The chain case deliberately exercises a small supported bucket limit.
The writer creates additional IDs during measurement.

Reproduction, from the selected implementation revision:

```bash
git apply tests/benchmarks/inkeys_lazy_map_timing.patch
./build.sh ENABLE_ASSERT=1
.venv/bin/python tests/benchmarks/inkeys_lazy_map.py \
  --module "$PWD/bin/linux-x64-release/search-community/redisearch.so" \
  --output /tmp/inkeys-lazy-measurements --repeats 2
git apply -R tests/benchmarks/inkeys_lazy_map_timing.patch
```

Use a fresh output directory for each invocation. Do not run builds or other
benchmarks concurrently. Rebuild after removing instrumentation before running
normal verification. The historical-ID baseline is the algorithm from `87383f62c8`, with the
profiling and shutdown-test fixes already present (neither affects these benchmark
commands); copy the
harness and timing patch into that checkout for comparison.

Short construction windows can contain few client samples. The recorded p99 values
are descriptive sample percentiles, not estimates of a production tail-latency SLO.
Raw results include sample counts, first-query latency, p50/p99/max client latency,
batch count, and aggregate/max batch duration.

## Results

The original algorithm ran once per matrix cell; the revised implementation ran
twice. Values below are **old → revised**, in milliseconds; revised values use
the worse of the two repeats for each metric. This deliberately avoids choosing
the fastest revised run. The final implementation revision is `a27bbf3d7f`.
[Raw results and sample counts](inkeys-lazy-benchmarks.jsonl) are retained alongside
this report.

| Case | WORKERS | Writes | Build | Max batch under lock | First query | Query p99 during build | Write p99 during build |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| dense | 0 | no | 48.550 → 57.522 | 13.495 → 1.275 | 1.619 → 1.135 | 7.354 → 0.560 | — |
| dense | 0 | yes | 50.502 → 38.441 | 13.406 → 1.334 | 0.935 → 0.909 | 35.567 → 22.288 | 35.539 → 23.279 |
| dense | 2 | no | 69.063 → 54.190 | 18.965 → 1.524 | 1.507 → 2.854 | 7.589 → 1.027 | — |
| dense | 2 | yes | 53.100 → 38.780 | 13.113 → 1.517 | 50.590 → 27.021 | 50.590 → 27.021 | 26.762 → 17.784 |
| churn | 0 | no | 10.331 → 5.091 | 1.097 → 0.389 | 1.211 → 1.168 | 1.211 → 1.168 | — |
| churn | 0 | yes | 9.587 → 4.972 | 0.817 → 0.439 | 1.168 → 1.351 | 4.184 → 2.385 | 4.111 → 2.367 |
| churn | 2 | no | 10.048 → 6.191 | 0.832 → 0.360 | 1.473 → 1.441 | 1.473 → 1.441 | — |
| churn | 2 | yes | 12.025 → 5.594 | 1.566 → 0.515 | 1.495 → 2.231 | 8.893 → 2.857 | 8.929 → 2.777 |
| chains | 0 | no | 1394.272 → 21.174 | 23.631 → 0.958 | 1.259 → 2.156 | 23.031 → 0.375 | — |
| chains | 0 | yes | 1702.488 → 19.052 | 26.414 → 0.970 | 0.933 → 2.147 | 832.801 → 17.776 | 853.017 → 18.778 |
| chains | 2 | no | 1493.254 → 23.141 | 26.166 → 1.185 | 50.873 → 2.781 | 50.826 → 2.781 | — |
| chains | 2 | yes | 1249.671 → 19.902 | 25.660 → 0.974 | 1250.799 → 20.880 | 1250.799 → 20.880 | 218.518 → 13.394 |

The chain walk removes the pathological repeated-lookup cost: the worst chain
case fell from 1,702 ms to 23.2 ms. Across the entire matrix, the maximum measured
batch fell from 26.4 ms to 1.6 ms. The churn case no longer scans all historical
IDs; its work is bounded by snapshot bucket capacity and visited DMDs.

These changes do not make first use free. The slowest revised first query was
27.1 ms, and client latency can exceed one batch because several lock acquisitions
can intervene before a waiting client runs. The million-document build still took
up to 58 ms. This is an explicit first-use tradeoff for avoiding the map entirely
on indexes that never use INKEYS; it is not a guarantee of sub-millisecond stalls.

After readiness, query and write p99s in the revised matrix remained below 0.3 ms
for this single-query/single-writer workload. Short build windows sometimes contain
only two query samples, so the during-build p99 must be read together with the
sample count. Future workloads with many concurrent clients, allocator pressure,
or a busy reindex pool can have different tails.

The measured improvement is sufficient to replace the draft's ID scan. No
additional user configuration is introduced. A hard wall-clock latency bound,
parallel backfill, or eviction after use would require a separate design.
