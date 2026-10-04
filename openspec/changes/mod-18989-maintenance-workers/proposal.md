# Keep vector index maintenance off the main thread at WORKERS 0

## Why

[MOD-18989](https://redislabs.atlassian.net/browse/MOD-18989): with `WORKERS 0`, the
workers pool is empty, so VecSim writes in place. Every vector delete or overwrite then
repairs the HNSW graph on the main thread. A transaction of a few hundred deletes on
large-dimension indexes blocks the event loop for seconds, which is long enough for a
shard to be treated as unresponsive and restarted. `WORKERS 0` is the default on Redis
Enterprise (non-Flex), so this is the default behavior there.

## What Changes

A new config, `search-min-maintenance-workers` (default 1), keeps a minimum number of
pool threads at all times, next to the event-only `search-min-operation-workers`.
Vector index jobs (graph repair, HNSW/SVS ingestion) run on those threads, as they
already do with `WORKERS > 0`. Queries still route by `WORKERS` alone, so at `WORKERS 0`
they keep running on the main thread.

User-visible effects at `WORKERS 0`:

- Main-thread stalls under vector mutation drop by 3-35x.
- One background thread runs while vector writes are repaired.
- Vector reads become eventually consistent with writes, as with `WORKERS > 0`.
- The repair backlog and the memory of deleted-but-unrepaired vectors are not bounded,
  as with `WORKERS > 0`.

`search-min-maintenance-workers 0` restores the previous behavior exactly. The default
needs owner sign-off; see the open questions in `design.md`. Implementation and review
are in [PR #11643](https://github.com/RediSearch/RediSearch/pull/11643).
