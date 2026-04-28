**MOD-14527 — Effective timeout: v8.6.0 vs `master` (`ON_TIMEOUT FAIL`)**

**TL;DR — yes, `master` is stricter.** Across all cells where both versions actually timed out 100% of the time (45 cells including 10K and 100K-on-cluster5/7), `master` reduces mean overshoot from **207.9 ms → 152.1 ms (−27%)**, better in 32 cells, worse in 13. Two known regressions, both at sub-preemption-granularity timeouts (`FT.HYBRID` @ 50 ms and `FT.AGGREGATE` on tiny indexes @ 10 ms — see §3).

**1. Setup**

`{v8.6.0, master} × {sa, cluster3, cluster5, cluster7} × {SEARCH, AGGREGATE, HYBRID} × {10K, 100K, 500K, 1M} × {1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000} ms` (axes pruned per topology/size — cluster5/7 is 100K + 1M only, 10K is `sa+cluster3 × AGG × {1..50} ms` only). N=50, `WORKERS 4`, `ON_TIMEOUT FAIL`, filter `*`. **16 200 samples / 324 cells** across the three run dirs.

**2. Headline overshoot reductions** (apples-to-apples, both versions @ 100% timeout)

| topology | qtype | size | TIMEOUT | v8.6 over_mean | master over_mean | Δ |
|---|---|---:|---:|---:|---:|---:|
| sa | AGGREGATE | 1M | 100 ms | +2239.5 | +862.8 | **−1376.7** |
| sa | AGGREGATE | 500K | 50 ms | +1003.4 | +671.6 | −331.8 |
| **cluster5** | **AGGREGATE** | **100K** | **100 ms** | **+422.5** | **+197.0** | **−225.5** *(new)* |
| **cluster7** | **AGGREGATE** | **100K** | **100 ms** | **+432.8** | **+254.2** | **−178.6** *(new)* |
| sa | AGGREGATE | 1M | 500 ms | +1839.5 | +1710.6 | −128.9 |
| sa | AGGREGATE | 1M | 2000 ms | +340.7 | +218.4 | −122.3 |
| sa | AGGREGATE | 1M | 1000 ms | +1338.0 | +1227.6 | −110.4 |
| cluster3 | AGGREGATE | 1M | 1000 ms | +106.0 | +31.8 | −74.1 |
| **cluster7** | **SEARCH** | **100K** | **50 ms** | **+71.0** | **+17.9** | **−53.1** *(new)* |
| cluster7 | SEARCH | 1M | 100 ms | +49.9 | +4.6 | −45.3 |
| **cluster5** | **SEARCH** | **100K** | **50 ms** | **+52.3** | **+8.6** | **−43.8** *(new)* |
| cluster3 | AGGREGATE | 500K | 500 ms | +53.6 | +18.8 | −34.8 |
| sa | AGGREGATE | 500K | 100 ms | +957.0 | +923.8 | −33.2 |
| **cluster7** | **SEARCH** | **100K** | **100 ms** | **+26.8** | **+5.1** | **−21.8** *(new)* |
| cluster5 | AGGREGATE | 1M | 500 ms | +73.9 | +53.0 | −20.9 |

**Behavioural wins** (v8.6 silently runs to natural latency; `master` honours the budget):

| topology | qtype | size | TIMEOUT | v8.6 obs / tmo% | master obs / tmo% |
|---|---|---:|---:|---|---|
| cluster5 | AGGREGATE | 1M | 1000 ms | 5872 ms / **6%** | 1036 ms / **100%** |
| cluster7 | AGGREGATE | 1M | 1000 ms | 5850 ms / 6% | 3494 ms / 54% |
| sa | AGGREGATE | 500K | 1000 ms | 3589 ms / 0% | 2148 ms / 58% |

**3. Two opposite-direction results**, both at sub-preemption-granularity timeouts:

| cell | v8.6 over_mean | master over_mean | Δ |
|---|---:|---:|---:|
| sa / **HYBRID** / 1M / 50 ms | +0.4 | +50.8 | **+50.4** |
| cluster3 / **HYBRID** / 1M / 50 ms | +0.4 (40% tmo) | +41.5 (94% tmo) | +41.1 |
| **sa / AGGREGATE / 10K / 10 ms** | +19.2 (62% tmo) | +35.5 (24% tmo) | **+16.3** *(new)* |
| **cluster3 / AGGREGATE / 10K / 10 ms** | +12.3 (76% tmo) | +30.3 (36% tmo) | **+18.1** *(new)* |

In both regimes v8.6 polled the timeout often enough to hit the deadline; `master`'s preemption is at a coarser granularity (vector-chunk for HYBRID, scan-chunk for tiny-index AGG) so when the configured timeout sits *inside* one chunk, master runs to the end of the chunk before honouring it. The effect is bounded by the natural per-chunk time (~50 ms HYBRID, ~50 ms 10K-AGG), not by index size, so it does not show up at higher TIMEOUT values.

**Where the regression goes away:**
- `AGG / 10K / TIMEOUT ≥ 20 ms` — 0% timeout fire on either version, both ~57 ms natural. Master and v8.6 converge.
- `AGG / 10K / TIMEOUT ≤ 5 ms` — within ±1 ms (both fire 100%, master a touch better at 1 ms).
- `HYBRID / TIMEOUT ≥ 100 ms` — converges within a few ms on both topologies.

**4. `server_mean_ms` (cmdstat-derived, operator note)**

On `master`, timed-out queries report **near-zero** `cmdstat_FT.*.usec` even when the client waits hundreds of ms. v8.6 tracks observed wall time closely. Not a correctness issue for MOD-14527, but operators relying on `INFO commandstats` for capacity planning will under-count work attributable to timed-out queries on the new mechanism.

| cell | observed old / new | server_mean old / new |
|---|---:|---:|
| cluster3 / SEARCH / 500K / 50 ms | 86.4 / 63.3 | 86.1 / **0.12** |
| cluster3 / AGGREGATE / 500K / 500 ms | 553.6 / 518.8 | 553.3 / **0.21** |
| cluster5 / AGGREGATE / 1M / 100 ms | 116.4 / 105.1 | 116.1 / **0.14** |
| sa / AGGREGATE / 500K / 100 ms | 1057.0 / 1023.8 | 1056.7 / **0.19** |

**5. Verdict**

| workload | verdict |
|---|---|
| `FT.AGGREGATE` (medium/large indexes) | ✅ Clear win, holds across `sa / cluster3 / cluster5 / cluster7`. Single-cell wins up to −1377 ms; new 100K cells on cluster5/7 land at −178 to −226 ms. |
| `FT.SEARCH` | ➖ Mostly neutral, with consistent small wins (−22 to −53 ms) on short-timeout cluster cells at 100K and 1M. |
| `FT.HYBRID`, TIMEOUT ≥ chunk period | ➖ Neutral. |
| `FT.HYBRID`, TIMEOUT < chunk period | ⚠️ +40–50 ms vs v8.6. |
| `FT.AGGREGATE` on tiny indexes (10K) at TIMEOUT < ~20 ms | ⚠️ +16–18 ms vs v8.6. Same root cause as HYBRID. |
| `cmdstat_FT.*.usec` on timed-out queries | ⚠️ Under-reports on `master`. |

**Artefacts**

- `timeout-bench-results/expand-20260423T095000Z/` — sa+cluster3 × {100K, 1M} × all qtypes.
- `timeout-bench-results/expand2-20260427T092816Z/` — sa+cluster3 @ 500K, cluster5/7 @ 1M, plus `server_mean_ms`.
- `timeout-bench-results/expand3-20260428T075540Z/` — cluster5/7 @ 100K (all qtypes) + sa/cluster3 @ 10K AGGREGATE tight timeouts.
