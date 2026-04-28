# MOD-14527 - Effective timeout: v8.6.0 vs master

## TL;DR

Benchmark completed locally for the `ON_TIMEOUT=FAIL` strictness comparison.

The new mechanism on `master` is generally stricter than the old v8.6.0
in-pipeline timeout mechanism. Across the full set of completed runs, in the
cells where both versions actually timed out 100% of the time, `master`
reduced mean timeout overshoot from **269.3 ms** to **198.9 ms**:
**-70.4 ms**, or about **26% less overshoot**. In those 34 apples-to-apples
cells, `master` was better in 26 cells and worse in 8.

Using the looser threshold of cells where both versions timed out at least
50% of the time gives the same picture: **262.3 ms** mean overshoot on v8.6.0
vs **193.4 ms** on `master`, again about **26% less overshoot**.

The strongest signal is still `FT.AGGREGATE`: `master` is much closer to the
configured timeout on the heavy cells, including the wider `cluster5` and
`cluster7` runs. `FT.SEARCH` is mostly close, with small wins for `master` on
the short-timeout cluster cases. `FT.HYBRID` has one caveat: on the earlier
`sa / cluster3 / 1M / TIMEOUT 50ms` cases, `master` overshoots more because it
appears to wait for the vector brute-force chunk to finish.

## Setup

- Compared `v8.6.0` old timeout mechanism vs current `master` new
  blocked-client timeout mechanism.
- Same host, same client-side timing method, same module config:
  `WORKERS 4`, `ON_TIMEOUT FAIL`, filter `*`, N=50 per cell.
- Covered query types: `FT.SEARCH`, `FT.AGGREGATE`, `FT.HYBRID`.
- Covered topologies and sizes:
  - `sa` and `cluster3`: 100K, 500K, and 1M docs.
  - `cluster5` and `cluster7`: 1M docs.
- Covered timeouts: 50, 100, 500, 1000, and 2000 ms.
- Total: **12,000 raw samples / 240 cells** across the combined result set.
- The latest run also added `server_mean_ms`, based on
  `cmdstat_FT.<qtype>.usec` deltas. For cluster topologies this is
  coordinator-only, so it matches the client/coordinator perspective.

## Headline Results

Largest apples-to-apples overshoot reductions where both versions timed out
100% of the time:

| topology | qtype | size | timeout | v8.6 over_mean | master over_mean | reduction |
|---|---|---:|---:|---:|---:|---:|
| sa | AGGREGATE | 1,000,000 | 100 ms | +2239.5 ms | +862.8 ms | -1376.7 ms |
| sa | AGGREGATE | 500,000 | 50 ms | +1003.4 ms | +671.6 ms | -331.8 ms |
| sa | AGGREGATE | 1,000,000 | 500 ms | +1839.5 ms | +1710.6 ms | -128.9 ms |
| sa | AGGREGATE | 1,000,000 | 2000 ms | +340.7 ms | +218.4 ms | -122.3 ms |
| sa | AGGREGATE | 1,000,000 | 1000 ms | +1338.0 ms | +1227.6 ms | -110.4 ms |
| cluster3 | AGGREGATE | 1,000,000 | 1000 ms | +106.0 ms | +31.8 ms | -74.1 ms |
| cluster7 | SEARCH | 1,000,000 | 100 ms | +49.9 ms | +4.6 ms | -45.3 ms |
| cluster3 | AGGREGATE | 500,000 | 500 ms | +53.6 ms | +18.8 ms | -34.8 ms |
| sa | AGGREGATE | 500,000 | 100 ms | +957.0 ms | +923.8 ms | -33.2 ms |
| cluster3 | SEARCH | 500,000 | 50 ms | +36.4 ms | +13.3 ms | -23.1 ms |
| cluster5 | AGGREGATE | 1,000,000 | 500 ms | +73.9 ms | +53.0 ms | -20.9 ms |
| cluster5 | SEARCH | 1,000,000 | 50 ms | +35.4 ms | +20.5 ms | -14.9 ms |
| cluster7 | AGGREGATE | 1,000,000 | 500 ms | +79.8 ms | +66.5 ms | -13.2 ms |

There is also an important asymmetric result that is not apples-to-apples but
is behaviorally important:

| topology | qtype | size | timeout | v8.6 observed / timeout rate | master observed / timeout rate |
|---|---|---:|---:|---:|---:|
| cluster5 | AGGREGATE | 1,000,000 | 1000 ms | 5872.2 ms / 6% | 1036.1 ms / 100% |
| cluster7 | AGGREGATE | 1,000,000 | 1000 ms | 5849.9 ms / 6% | 3493.7 ms / 54% |
| sa | AGGREGATE | 500,000 | 1000 ms | 3589.1 ms / 0% | 2148.0 ms / 58% |

These rows show cases where v8.6.0 mostly lets the query run to natural
latency instead of reporting a timeout, while `master` starts aborting closer
to the configured budget.

## Workload Notes

- `FT.AGGREGATE`: strongest improvement. `master` is consistently closer to
  the requested timeout in the cells where timeout enforcement actually
  triggers, especially at 500K/1M docs and on wider clusters.
- `FT.SEARCH`: mostly similar, with small wins for `master` on short-timeout
  cluster cases such as `cluster7 / 1M / TIMEOUT 100ms`.
- `FT.HYBRID`: the one caveat. In the earlier 1M runs:
  - `sa / HYBRID / TIMEOUT 50ms`: v8.6.0 mean overshoot was +0.4 ms, while
    `master` was +50.8 ms.
  - `cluster3 / HYBRID / TIMEOUT 50ms`: v8.6.0 mean overshoot was +0.37 ms,
    while `master` was +41.5 ms.
  - The later `cluster5` and `cluster7` HYBRID 1M rows mostly did not hit the
    timeout budget because natural latency was under or near 50 ms.

## server_mean_ms

The new `server_mean_ms` column is useful, but it should not replace
client-observed wall time for this comparison.

On v8.6.0, `server_mean_ms` usually tracks `observed_ms` closely. On
`master`, timeout-heavy worker-thread paths often report near-zero
`server_mean_ms` even when the client waits tens or hundreds of milliseconds.
That means `INFO commandstats` under-reports work attributable to timed-out
queries on the new mechanism.

Examples from the latest run:

| cell | observed old/new | server_mean old/new |
|---|---:|---:|
| cluster3 / SEARCH / 500K / 50 ms | 86.4 / 63.3 ms | 86.1 / 0.12 ms |
| cluster3 / AGGREGATE / 500K / 500 ms | 553.6 / 518.8 ms | 553.3 / 0.21 ms |
| cluster5 / AGGREGATE / 1M / 100 ms | 116.4 / 105.1 ms | 116.1 / 0.14 ms |
| cluster7 / AGGREGATE / 1M / 50 ms | 60.2 / 54.8 ms | 59.9 / 0.13 ms |
| sa / AGGREGATE / 500K / 100 ms | 1057.0 / 1023.8 ms | 1056.7 / 0.19 ms |

So the main metric for MOD-14527 should remain client-observed wall time and
overshoot. `server_mean_ms` is a useful operator-facing caveat: timed-out
queries on `master` may not be represented accurately in commandstats.

## Caveats

- Long-timeout AGGREGATE cells sometimes run to natural latency without
  firing the timeout. Those rows are useful behaviorally, but they should not
  be treated as apples-to-apples timeout overshoot unless `timeout_pct` is
  high on both versions.
- HYBRID exact-deadline strictness at very tight timeouts should be tracked
  separately if it matters. It is the only clear opposite-direction result.
- This is still a local single-host benchmark with N=50. It answers the
  yes/no and rough-magnitude question, not an absolute SLA question.

## Artefacts

- `timeout-bench-results/expand-20260423T095000Z/raw.csv` - earlier expanded
  run, including the pilot rows.
- `timeout-bench-results/expand2-20260427T092816Z/raw.csv` - 500K,
  cluster5/cluster7, and `server_mean_ms` run.
- `summary.csv` in each directory - per-cell aggregates.
- `meta.txt` in each directory - run metadata and notes.
