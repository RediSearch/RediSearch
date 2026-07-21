# Disabled benchmarks

Benchmark definitions in this folder are **temporarily excluded from CI**.

The benchmark runner discovers tests with a flat, non-recursive filename glob
(e.g. `search*.yml`) in `tests/benchmarks/`, so files under this subdirectory
are not picked up by any benchmark job (`search*.yml`, `vecsim*.yml`,
`hybrid*.yml`, `search-msmarco*.yml`).

Each entry below must link a tracking ticket. Re-enable by `git mv`-ing the file
back up to `tests/benchmarks/` once its ticket is resolved — the file's internal
`name:` (and therefore its time-series metric identity) is unchanged by the move.

| Benchmark | Reason | Ticket |
|-----------|--------|--------|
| `search-groupby-collect-100K-entity-events-{hash,json}-cached-sortby-fields-{explicit,star-offset500}-k50` | Heavy `FT.AGGREGATE GROUPBY + REDUCE COLLECT` query stalls under 64 concurrent workers — zero completed queries, client fails with `i/o timeout` — despite the 500 ms `ON_TIMEOUT RETURN` server config. Pre-existing and intermittent; not a topology-PR regression. | [MOD-16899](https://redislabs.atlassian.net/browse/MOD-16899) |
