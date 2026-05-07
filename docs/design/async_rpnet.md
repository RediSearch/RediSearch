# RPNetAsync: Async Depleters with Blocking Coordinator

## Overview

RPNetAsync splits the distributed hybrid merge pipeline at the RPNet boundary into
two phases, allowing I/O-bound shard communication to run cooperatively on the
coordinator thread pool while keeping the coordinator's reply path unchanged.

In the original flow, the coordinator thread blocks in `rpnetNext` waiting for
shard `CURSOR READ` replies via `MRChannel_Pop`. Since the Sorter and
`AggregateResults` already buffer all results before producing output, the thread
is wasted during every network wait.

RPNetAsync eliminates this by running the I/O draining on cooperative pool threads
that yield when the channel is empty, freeing threads for other queries.

## Architecture

```
  Coordinator Thread          Pool Thread              IO Thread            MRChannel
        |                         |                       |                    |
        | ProcessHybridCursorMappings (blocking, unchanged)                    |
        |------------------------>|                       |                    |
        | Submit RPNetAsync(search) + RPNetAsync(vsim)   |                    |
        |                         |                       |                    |
        | Block on condvar        |                       |                    |
        |    ...                  |                       |                    |
        |                         |--- Phase A: Cooperative I/O drain -------->|
        |                         |                       |                    |
        |                         | TryPop -------------->|                    |
        |                         |<-- reply -------------|                    |
        |                         | deserialize + buffer  |                    |
        |                         | ManuallyTriggerNext   |                    |
        |                         |                       |                    |
        |                         | TryPop -------------->|                    |
        |                         |<-- NULL (empty) ------|                    |
        |                         | waiting = true        |                    |
        |                         | return (yield to pool)|                    |
        |                         | (thread is free for   |                    |
        |                         |  other work items)    |                    |
        |                         |                       |                    |
        |                         |                       | Push(reply) ------>|
        |                         |                       | if (waiting):      |
        |                         |<--- re-dispatch ------|  notify            |
        |                         |                       |                    |
        |                         | TryPop -------------->|                    |
        |                         |<-- reply -------------|                    |
        |                         | deserialize + buffer  |                    |
        |                         |        ...            |                    |
        |                         |                       |                    |
        |                         | all shards EOF        |                    |
        |<-- signal done_cond ----|                       |                    |
        |                         |                       |                    |
        | Wake up                 |                       |                    |
        | Patch pipeline: RPBufferedSource replaces RPNet |                    |
        | (PoC shortcut — ideally built with right source)|                    |
        | sendChunk_hybrid (Phase B, unchanged)           |                    |
        |                         |                       |                    |
```

## Two-Phase Execution

### Phase A: Async I/O Draining

New `RPNetAsync` tasks run on the coordinator thread pool. Each task:

1. Calls `MRChannel_TryPop` (non-blocking) on the RPNet's channel.
2. If data is available: deserializes the reply into `SearchResult` objects, appends
   them to an internal buffer, triggers `MR_ManuallyTriggerNextIfNeeded` for the
   next cursor batch.
3. If the channel is empty and shards are still pending: sets an atomic `waiting`
   flag and returns, yielding the thread back to the pool.
4. If all shards have sent EOF (no pending commands, empty channel): marks completion
   and signals the coordinator's condvar.

When the IO thread pushes a reply to `MRChannel`, `netCursorCallback` checks the
barrier's `asyncWakeCallback`. If the RPNetAsync consumer was yielded, it is
re-dispatched to the thread pool.

### Phase B: Synchronous CPU Pipeline

After Phase A completes for both subqueries (search + vsim), the coordinator:

1. Creates `RPBufferedSource` processors from each feeder's buffer.
2. Patches the pipeline: replaces RPNet (the root processor) with RPBufferedSource.
3. Runs `sendChunk_hybrid` exactly as before. The rest of the RP chain (Sorter,
   HybridMerger, tail pipeline) is unchanged.

No I/O blocking is possible in Phase B — all data is already in memory.

## Key Components

### `MRChannel_TryPop` (`src/coord/rmr/chan.c`)

Non-blocking pop. Acquires the channel lock, returns NULL immediately if empty,
otherwise pops and returns the head item.

### `RPNetAsync` (`src/coord/rpnet_async.h`, `src/coord/rpnet_async.c`)

The async re-dispatchable I/O drainer. Contains:

- A reference to the original `RPNet` (for access to MRIterator, command, AREQ)
- A buffer of heap-allocated `SearchResult *` objects
- Profile data array (`shardsProfile`)
- Accumulated `totalResults` count
- Completion signaling (mutex + condvar + `complete` flag)
- Lock-free yield/resume via `_Atomic(bool) waiting`

The core loop (`RPNetAsync_Run`) is submitted as a thread pool work item and handles:
- Timeout checking (query timeout + blocked-client timeout)
- Non-blocking channel reads with yield on empty
- Reply deserialization (same logic as `rpnetNext`)
- Cursor batch triggering

### `RPBufferedSource` (`src/coord/rpnet_async.h`, `src/coord/rpnet_async.c`)

A trivial synchronous `ResultProcessor` for Phase B. Returns pre-buffered
`SearchResult` objects sequentially. When the buffer is exhausted, returns the
terminal status from Phase A (EOF, TIMEDOUT, or ERROR).

### `RPNet_InitIterator` (`src/coord/rpnet.c`)

Extracted iterator initialization from `rpnetNext_StartWithMappings`. Creates the
`MRIterator`, sets up the cursor read command, and registers the abort-wake channel
— without invoking `rpnetNext`. Accepts an optional `ShardResponseBarrier` for async
wake notification delivery.

### Re-dispatch Notification (`src/coord/dist_utils.c`)

After `MRIteratorCallback_AddReply` pushes a reply to the channel in
`netCursorCallback`, the barrier's `asyncWakeCallback` is invoked. This performs an
`atomic_exchange` on the RPNetAsync's `waiting` flag — if it was set, the task is
re-submitted to the coordinator pool.

## Timeout Handling

- **Query timeout**: Checked at the top of each `RPNetAsync_Run` iteration via
  `TimedOut()` and `AREQ_TimedOut()`. If expired, marks `lastRc = RS_RESULT_TIMEDOUT`
  and signals completion.

- **Timeout while yielded**: The next re-dispatch (triggered by the IO thread pushing
  data) immediately detects the timeout and exits.

- **Phase B propagation**: `RPBufferedSource_Next` returns `RS_RESULT_TIMEDOUT` once
  the buffer is exhausted, propagating through the RP chain normally.

## Error Handling

If a shard returns an error that should be fatal (based on timeout/OOM policy), the
feeder stores the error in its `QueryError` field, sets `lastRc = RS_RESULT_ERROR`,
and signals completion. The orchestration code in `HybridRequest_executePlan` checks
both feeders' status before proceeding to Phase B.

## What Changes vs. What Stays

### New (no changes to existing logic):

| Component | Location |
|-----------|----------|
| `MRChannel_TryPop` | `src/coord/rmr/chan.h`, `chan.c` |
| `RPNetAsync` | `src/coord/rpnet_async.h`, `rpnet_async.c` |
| `RPBufferedSource` | `src/coord/rpnet_async.h`, `rpnet_async.c` |
| `RPNet_InitIterator` | `src/coord/rpnet.h`, `rpnet.c` |

### Modified:

| Component | Change |
|-----------|--------|
| `ShardResponseBarrier` | Added `asyncWakeCallback` and `asyncWakeContext` fields |
| `netCursorCallback` | Calls `asyncWakeCallback` after pushing to channel |
| `HybridRequest_executePlan` | Orchestrates Phase A then Phase B |

### Unchanged:

- Coordinator thread blocking model (blocks on condvar, not on channel)
- `sendChunk_hybrid`, `startPipelineHybrid`, `AggregateResults`
- All RP stages: Sorter, HybridMerger, Limiter, tail pipeline
- SA path (RPSafeDepleter) — no network I/O
- Reply path, timeout callbacks (FAIL policy)
- IO event loop, fan-out mechanics
- `ProcessHybridCursorMappings` (still blocking)

## Thread Pool Usage

RPNetAsync tasks run on the coordinator pool (default 20 threads). For 2 tasks per
query (search + vsim), contention is minimal. Since tasks yield when blocked on I/O,
they do not monopolize threads — other queries' tasks can execute in the gaps.

## Memory Characteristics

The buffer holds all `SearchResult` objects in memory until Phase B consumes them.
This is identical to the existing behavior where the Sorter buffers all results
before producing output. No additional memory overhead is introduced.
