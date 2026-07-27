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
| `search-groupby-collect-100K-entity-events-json-cached-sortby-fields-{explicit,star-offset500}-k50` | Heavy `FT.AGGREGATE GROUPBY + REDUCE COLLECT` query stalls under load — zero completed queries, client fails with `i/o timeout`. Root cause: the JSON `LOAD` re-compiles the JSONPath per field, and that load runs under the GIL, so concurrent queries serialize and never return within the client deadline. The `hash` variants do **not** hit this (no JSONPath) and run enabled at 100K; the `json` variants are stood in for by `search-groupby-collect-10K-*-json-*` (MOD-17201) until the compile-cache fix lands. Re-raise these to 100K when MOD-16899 is fixed. | [MOD-16899](https://redislabs.atlassian.net/browse/MOD-16899) |
