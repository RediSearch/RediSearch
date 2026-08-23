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
