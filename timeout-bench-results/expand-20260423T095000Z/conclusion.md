# MOD-14527 — Effective timeout: v8.6.0 vs master (expanded: SEARCH + AGGREGATE + HYBRID)

## TL;DR

The new timeout mechanism on `master` (`ON_TIMEOUT FAIL`, multi-thread search
with blocked-client signalling) **still reduces mean timeout overshoot by
~29 % overall and by up to ~61 % on the workloads that matter most — heavy
`FT.AGGREGATE` with `LOAD *` on a 1 M-doc index**.

Across the **17** cells where both versions observed 100 % timeouts
(apples-to-apples):

| | v8.6.0 | master | delta | ratio |
|---|---:|---:|---:|---:|
| Mean overshoot (ms) | 365.1 | 257.8 | **−107.3 ms** | **0.71×** |

The single largest reduction is still standalone `FT.AGGREGATE / 1 M /
TIMEOUT 100 ms`: **mean overshoot dropped from 2 239 ms to 863 ms (−1 377 ms,
0.39×)**.

Adding `FT.HYBRID` (TEXT AND vector KNN, `FLAT` index, brute-force scan)
surfaces a **new, opposite-direction finding that is not present on SEARCH or
AGGREGATE**: on `standalone / 1 M / TIMEOUT 50 ms` the v8.6 HYBRID path stops
almost exactly at the configured 50 ms (mean overshoot 0.4 ms), while master
lets the query run to its natural ~100 ms before returning with
`timed_out=1` (mean overshoot **+50.4 ms**). On `cluster3 / 1 M / HYBRID /
TIMEOUT 50 ms` the same pattern shows up as 40 % → 94 % timeout rate and
+41 ms overshoot on master. See “HYBRID-specific behaviour” below.

## Setup

Same host, module config and tooling as the pilot
(`timeout-bench-results/pilot-20260421T141317Z/`). Changes for the expansion:

- **New workload: `FT.HYBRID`.**
  The loader now also writes a 32-dim FLOAT32 vector field
  (`{vec: VECTOR FLAT 6 TYPE FLOAT32 DIM 32 DISTANCE_METRIC L2}`). Queries
  are issued as
  `FT.HYBRID idx SEARCH "hit" VSIM @vec $q K 10 COMBINE RRF 2 K 60 LIMIT 0 1000 TIMEOUT <ms>`,
  keeping the pilot's TEXT keyword prevalence so the text branch matches ~5 %
  of docs. FLAT is chosen so the vector branch is a full brute-force scan and
  reliably reaches the target timeouts on 1 M docs (natural p50 ≈ 100 ms on
  standalone, ≈ 50 ms per shard on cluster3).
- **Binary-safe client:** the driver uses `decode_responses=False` and
  sends the query vector as a raw 128-byte FLOAT32 blob.
- **Cluster stability:** during initial HYBRID runs the `new / cluster3 / 1M`
  cell reported `CLUSTERDOWN` after accumulated short-timeout events. Those
  cells were re-run in isolation with a fresh 3-shard cluster brought up per
  timeout value; the 250 resulting clean samples replace the contaminated
  ones in `raw.csv`.
- **Matrix.** `{v8.6.0, master} × {sa, cluster3} × {SEARCH, AGGREGATE,
  HYBRID} × {100 K, 1 M} × {50, 100, 500, 1000, 2000} ms × N = 50` =
  **6 000 samples, 120 cells**.

`overshoot_ms = max(0, observed_ms − timeout_ms)` is computed per row; the
summary then aggregates mean/p50/p95/p99/max per cell.

## Key results

### 1. SEARCH / AGGREGATE story is unchanged from the pilot

The two regimes already identified in the pilot still hold:

1. **Short `TIMEOUT` with long underlying work** (timeouts actually fire):
   master aborts faster on AGGREGATE.
2. **Long `TIMEOUT` never reached** (0 % timeout rate on both): wall time is
   just natural latency; within a few ms of each other.

Biggest overshoot wins (all AGGREGATE, unchanged from pilot):

| topology | query | size | timeout | v8.6 over_mean | master over_mean | delta |
|---|---|---:|---:|---:|---:|---:|
| sa | AGGREGATE | 1 000 000 | 100 ms | 2239.5 | **862.8** | **−1376.7** |
| sa | AGGREGATE | 1 000 000 | 500 ms | 1839.5 | **1710.6** | −128.9 |
| sa | AGGREGATE | 1 000 000 | 2000 ms | 340.7 | **218.4** | −122.3 |
| sa | AGGREGATE | 1 000 000 | 1000 ms | 1338.0 | **1227.6** | −110.4 |
| cluster3 | AGGREGATE | 1 000 000 | 1000 ms | 106.0 | **31.8** | −74.1 |
| sa | AGGREGATE | 100 000 | 100 ms | 85.1 | **59.0** | −26.1 |
| sa | AGGREGATE | 100 000 | 50 ms | 119.8 | **106.2** | −13.7 |
| cluster3 | AGGREGATE | 1 000 000 | 500 ms | 52.6 | **39.4** | −13.2 |

One additional silent-correctness item worth flagging on `cluster3 /
AGGREGATE / 1 M / TIMEOUT 2000 ms`: v8.6 reports `timed_out=0` on **every**
query despite observed wall time of ~6.5 s, while master correctly sets
`timed_out=1` on 18 % and brings the mean down to 5.8 s. See
[`tables.md`](./tables.md).

### 2. HYBRID-specific behaviour (new this run)

HYBRID is cheap on 100 K docs (natural ~4–6 ms), so every 100 K HYBRID cell
lands in regime 2 and the two versions are identical within noise. All the
signal is on 1 M docs.

`sa / HYBRID / 1 M` (natural latency ≈ 101–108 ms):

| timeout | v8.6 mean / tmo% / over_mean | master mean / tmo% / over_mean |
|---:|---:|---:|
| 50 ms  | 50.4 / 100 % / **0.4**  | 100.8 / 100 % / **50.8** |
| 100 ms | 99.9 /  38 % / 0.3      | 106.8 /  16 % / 6.8      |
| 500 ms+ | ~101 / 0 % / 0 | ~107 / 0 % / 0 |

`cluster3 / HYBRID / 1 M` (natural latency ≈ 49–52 ms per shard):

| timeout | v8.6 mean / tmo% / over_mean | master mean / tmo% / over_mean |
|---:|---:|---:|
| 50 ms | 47.9 / 40 % / 0.4 | 91.4 / 94 % / **41.5** |
| 100 ms+ | ~50 / 0 % / 0 | ~52 / 0 % / 0 |

Interpretation:

- When the configured timeout sits **below** natural latency, v8.6's HYBRID
  path terminates cleanly at the deadline (`observed_ms ≈ timeout_ms`).
  Master’s new mechanism keeps running until the vector brute-force chunk
  finishes (~one natural-latency period) and only then returns with
  `timed_out=1`. On `sa / 1 M / 50 ms` this is a +50 ms overshoot.
- When the configured timeout sits **near** natural latency, master detects
  timeouts more aggressively (94 % vs 40 % on `cluster3 / 1 M / 50 ms`, and
  at `sa / 1 M / 100 ms` the directions swap: 38 % v8.6 vs 16 % master
  because master’s natural latency is ~6 ms higher). Whenever master does
  detect a timeout, it still overshoots by a full vector-chunk interval.
- This is consistent with master’s blocked-client / worker-thread timeout
  signalling being coarser than the per-record polling v8.6 performs inside
  the vector iterator. It matches the pilot’s SEARCH/AGGREGATE finding for
  cases where the work is per-record HGETALL (master wins), but inverts on
  HYBRID where the work is a contiguous vector-distance chunk that master
  doesn’t preempt.

### 3. Tight-timeout noise at 50 ms is unchanged

On `SEARCH / 50 ms` cells master is within ±2 ms of v8.6 (inside N=50
noise). The picture matches the pilot.

## Artefacts in this directory

- `raw.csv` — one row per query (**6 000 rows**, 120 cells × 50 iters).
- `summary.csv` — one row per cell (120 rows) with mean/p50/p95/p99/max for
  `observed_ms` and `overshoot_ms`, plus `timeout_frac`.
- `tables.md` — per-`(topology, query_type, index_size)` side-by-side tables.
- `headlines.txt` — the apples-to-apples 100 %-timeout table above.
- `meta.txt` — build/host metadata and run-specific notes.

The pilot directory (`timeout-bench-results/pilot-20260421T141317Z/`) is
retained as-is; its rows are included verbatim in this run's `raw.csv`.
