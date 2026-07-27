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
| _(none currently)_ | | |

## Notes

The `search-groupby-collect-100K-entity-events-json-cached-sortby-fields-{explicit,star-offset500}-k50` benchmarks are **enabled but run a reduced 10K dataset** at 16 client workers (see their yml headers), because at 100K the JSON `LOAD` re-compiles the JSONPath per field under the GIL, so concurrent queries serialize and never return within the client deadline. Restore them to the 100K dataset once [MOD-16899](https://redislabs.atlassian.net/browse/MOD-16899) lands. The `hash` variants are unaffected (no JSONPath) and run at the full 100K. Tracked by [MOD-17201](https://redislabs.atlassian.net/browse/MOD-17201).
