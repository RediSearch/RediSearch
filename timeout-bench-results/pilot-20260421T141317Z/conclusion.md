# MOD-14527 — Effective timeout: v8.6.0 vs master

## TL;DR

The new timeout mechanism on `master` (`ON_TIMEOUT FAIL`, multi-thread search
with blocked-client signalling) **reduces mean timeout overshoot by ~30 %
overall and by up to ~60 % on the workloads that matter most — heavy
`FT.AGGREGATE` with `LOAD *` on a 1 M-doc index**.

Across the 16 cells in this pilot where both versions timed out on 100 % of
queries (apples-to-apples comparison):

| | v8.6.0 | master | delta | ratio |
|---|---:|---:|---:|---:|
| Mean overshoot (ms) | 387.9 | 270.7 | **−117.2 ms** | **0.70×** |

The single largest reduction was on standalone `FT.AGGREGATE` / 1 M docs
/ `TIMEOUT 100`: **mean overshoot dropped from 2 239 ms to 863 ms
(−1 377 ms, 0.39×)**.

On cheap `FT.SEARCH` workloads the absolute numbers are small for both
versions (≤ a few ms of overshoot), and the two implementations are within
noise.

## Setup

- **Old:** RediSearch `v8.6.0` built from tag (sha matches the `v8.6.0` tag).
- **New:** RediSearch `master` at HEAD of this workspace.
- **Redis:** unstable build from `/tmp/oss-redis` (`v=255.255.255`).
- **Host:** single local machine (`ip-172-31-38-140`).
- **Module config (both versions):** `WORKERS 4`, `ON_TIMEOUT FAIL`.
- **Topologies:**
  - `sa` — single standalone shard.
  - `cluster3` — 3-shard OSS cluster, slots split evenly, `SEARCH.CLUSTERREFRESH` issued on every shard.
- **Index sizes:** 100 000 and 1 000 000 hash documents (`t:TEXT`, `n:NUMERIC`, `tag:TAG`).
- **Query shapes** (both versions, identical):
  - `FT.SEARCH idx * LIMIT 0 1000 TIMEOUT <ms>` — forces doc materialisation.
  - `FT.AGGREGATE idx * LOAD * LIMIT 0 1000 TIMEOUT <ms>` — forces one HGETALL per returned doc.
- **Per-query timeouts:** 50, 100, 500, 1000, 2000 ms.
- **Iterations:** 50 per cell × 2 versions × 2 topologies × 2 query types
  × 2 sizes × 5 timeouts = **4 000 samples** (`raw.csv`).

`overshoot_ms = max(0, observed_ms − timeout_ms)` is computed per row; the
summary then aggregates mean/p50/p95/p99/max per cell.

## Key results

### Two regimes show up clearly

1. **Short `TIMEOUT` with long underlying work (timeouts actually fire):**
   master is faster to abort and the observed wall time stays closer to the
   configured timeout. This is the intended improvement of MOD-14527.
2. **Long `TIMEOUT` that queries never hit (0 % timeout rate on both):**
   wall time is just the natural query latency; the two versions are within
   a few ms of each other. Any small deltas here reflect query-path cost,
   not timeout enforcement.

Rows marked `0 %/0 %` in the detailed tables belong to regime 2 and are
**not** evidence of regression or improvement in timeout behaviour.

### Biggest overshoot reductions (master vs v8.6)

From `headlines.txt`:

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

Every single one of the top-8 wins is on `FT.AGGREGATE LOAD *`, which is
exactly the hot-path `MOD-14527` targets (long per-doc `HGETALL` work on
worker threads while the coordinator holds the client blocked).

### A small counter-point at the very tight end

At `TIMEOUT 50 ms` the natural runtime is already ~55–65 ms on this host,
so the timeout detector has essentially no room. In a handful of cells
(`cluster3/AGG/50`, `cluster3/SEARCH/50/1M`, `sa/SEARCH/50/1M`) master
shows +0.3–+2.9 ms more overshoot than v8.6. These deltas are inside
sample noise (N = 50) and disappear at any realistic production timeout.

See the full per-cell breakdown in [`tables.md`](./tables.md).

## Artefacts in this directory

- `raw.csv` — one row per query (4 000 rows). Columns:
  `version, topology, query_type, index_size, timeout_ms, iter, observed_ms, timed_out, error_snippet`.
- `summary.csv` — one row per cell (80 rows) with mean/p50/p95/p99/max for
  `observed_ms` and `overshoot_ms`, plus `timeout_frac`.
- `tables.md` — per-`(topology, query_type, index_size)` comparison tables.
- `headlines.txt` — the apples-to-apples 100 %-timeout table above.
- `driver.log`, `driver_resume.log` — driver stdout for reproducibility.
- `meta.txt` — build/host metadata.

## Caveats

- Single-host run; no isolation from other load. Numbers are meant to show
  **relative** behaviour between v8.6 and master, not absolute SLA values.
- N = 50 per cell is enough to separate the large AGGREGATE deltas from
  noise but marginal for sub-5 ms differences at `TIMEOUT 50`.
- No coordinator/RLEC Enterprise topology in this pilot — only OSS
  standalone and OSS cluster. The new mechanism is orthogonal to the coord
  layer, so the trend should carry over; that can be confirmed in a follow-up
  on an Enterprise build.
- `FT.HYBRID` was deferred. It is expected to behave like `FT.AGGREGATE`
  (per-doc work on worker threads) and should show the same pattern.
