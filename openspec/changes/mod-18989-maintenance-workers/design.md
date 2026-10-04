# Maintenance worker floor at WORKERS 0

## Mechanism

VecSim picks in-place or async writes from the pool size: an empty pool means in-place.
The pool is now sized `MAX(WORKERS, MIN_MAINTENANCE_WORKERS, in_event ? MIN_OPERATION_WORKERS : 0)`,
at creation and on every resize, so at `WORKERS 0` it keeps one thread and VecSim stays
async. `RunInThread` still checks `WORKERS`, not the pool size, so no query is queued
to the floor thread at `WORKERS 0`. With `MIN_MAINTENANCE_WORKERS` above a non-zero
`WORKERS`, the pool is larger and queries may run on all of its threads.

The thread is started on the first queued job, so a shard without vector writes does
not run it.

Shrinking to the floor needs care. The old shrink was to an empty pool, where leaving
threads exit only once the queue is empty. The thread pool supports that mode only for
a shrink to zero; a partial shrink removes threads as soon as they finish their current
job, which would leave an event's backlog to one thread. So:

- At the end of a load, the main thread waits for the queue on the full event pool,
  shrinks, then waits again for the thread-removal jobs.
- At the end of a trim or ASM event, and on `WORKERS N -> 0`, the shrink is deferred.
  The jobs queued or running at the request are counted; a 100 ms main-thread timer
  applies the shrink once that many jobs are done, or once the queue is empty. Jobs
  queued later do not postpone it, and the main thread does not block.
- A paused pool (`FT.DEBUG WORKERS PAUSE`, now allowed at `WORKERS 0`) defers any
  shrink until it is resumed.

## Measured behavior

Release build, `WORKERS 0`, DIM 1024, 3000 docs, 200-command MULTI/EXEC batches,
server pinned to 2 cores, medians of 3 interleaved runs.

| Load | Before (p50 / max) | After (p50 / max) | Peak queued jobs | Peak memory per index |
|---|---|---|---|---|
| 4 indexes, 1 s between batches | 842 / 1013 ms | 23 / 45 ms | 8.0k | 21.8 MiB |
| 4 indexes, back-to-back | 841 / 954 ms | 259 / 415 ms | 21.6k | 26.9 MiB |
| 8 indexes, back-to-back | 1675 / 1884 ms | 505 / 850 ms | 42.3k, growing | 22.7 MiB |

The baseline holds 13.0 MiB per index. Deleted vectors are kept until repaired and
removed; one fork GC pass removes up to 1024 per index.

While repair runs concurrently with queries, about 0.5% of exact-match KNN lookups
missed, the same rate as `WORKERS 2` today. After deletes or overwrites, recall at a
low `EF_RUNTIME` can stay reduced until fork GC has removed the deleted vectors.

Not yet measured: query latency at `WORKERS 0`, where main-thread KNN queries now scan
up to `TIERED_HNSW_BUFFER_LIMIT` (1024) buffered vectors per field and can wait on
index locks held by the worker; and SVS indexes.

## Alternatives

- **Writer-side backpressure** (a writer waits while the backlog is over a limit) was
  prototyped and dropped. Waiting while holding the GIL and the spec write lock could
  deadlock against a queued query, and the bounded versions could make a single command
  slower than in-place.
- **In-place fallback above a backlog limit**, inside VecSim, would bound the backlog
  without waiting. A possible follow-up if a hard bound is required.
- **Partial terminate-when-empty in the thread pool**, instead of the timer. A thread
  draining the queue could consume a later resize's admin job and leave the pool larger
  than its target.
- **Recommending `WORKERS > 0`** works today but costs a query core and leaves the default
  as is.
- **Cheaper in-place deletion** in VecSim is complementary and out of scope.

## Open questions

1. Default: 1 as proposed, or 0 with opt-in per database.
2. CPU: the floor thread uses a second core while repairing, in deployments that assume
   one core per database.
3. Bound: is an unbounded backlog acceptable, as with `WORKERS > 0`, or is a hard bound
   required first.
4. Config surface: module config only, or also the `FT.CONFIG` name `MIN_MAINTENANCE_WORKERS`
   like the sibling worker settings. The deprecated `MT_MODE` handling currently relies on
   the latter.
