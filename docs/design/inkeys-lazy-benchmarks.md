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
jemalloc. Modules are release builds. GC is disabled to isolate backfill; this is
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
./build.sh
.venv/bin/python tests/benchmarks/inkeys_lazy_map.py \
  --module "$PWD/bin/linux-x64-release/search-community/redisearch.so" \
  --output /tmp/inkeys-lazy-measurements --repeats 2
git apply -R tests/benchmarks/inkeys_lazy_map_timing.patch
```

Use a fresh output directory for each invocation. Do not run builds or other
benchmarks concurrently. Rebuild after removing instrumentation before running
normal verification. The historical-ID baseline is commit `87383f62c8`; copy the
harness and timing patch into that checkout for comparison.

Short construction windows can contain few client samples. The recorded p99 values
are descriptive sample percentiles, not estimates of a production tail-latency SLO.
Raw results include sample counts, first-query latency, p50/p99/max client latency,
batch count, and aggregate/max batch duration.
