# Design: Expand Timeout Metrics (per-stage)

- **Ticket:** [MOD-13192 — Expand timeout metrics](https://redislabs.atlassian.net/browse/MOD-13192)
- **Epic:** [MOD-8477 — Revisit and improve the query engine timeout mechanism](https://redislabs.atlassian.net/browse/MOD-8477)
- **Status:** Draft for review
- **Author:** Ben Goldberger

## 1. Overview

Today RediSearch exposes a single counter for query timeouts (split only by
`error`/`warning` and `shard`/`coord`). When operators see the timeout counter
rising they cannot tell *where* in the query lifecycle the time was lost.

This change introduces a **stage dimension** for the timeout metric, attributing
each timeout to the pipeline stage in which the deadline was exceeded:

1. **Wait in queue** — the request timed out before the result-processor
   pipeline started running (request prep + time spent queued in the worker
   thread pool / blocked-client wait).
2. **RP pipeline** — the deadline was exceeded while the result-processor
   pipeline was executing (reading iterators, aggregating, sorting, loading).
3. **Reply phase** — the deadline was exceeded while serializing/sending the
   reply to the client (after the pipeline produced its results).

A second requirement from the ticket is to **increment the metric on *all*
possible timeouts** — including reply-phase timeouts, which are currently not
detected or counted at all — and to **report them to Redis** via the module
`INFO` output.

## 2. Goals / Non-goals

### Goals
- Add a per-stage breakdown to the query-timeout counters in the global stats.
- Detect and count timeouts that occur in the **reply phase**, which are
  invisible today.
- Centralize the stage classification so every timeout-recording site reports a
  stage (no silently-unclassified timeout paths).
- Expose the breakdown through the module `INFO` sections. The change is
  **purely additive**: the existing aggregate counters and `INFO` fields are
  kept untouched, and the per-stage counters are added alongside them.
- Cover shard, coordinator/standalone, and hybrid query paths.

### Non-goals
- Changing timeout *behavior* (policies `RETURN` / `FAIL` / `RETURN_STRICT`
  keep their current semantics; this is observability only).
- Per-index or per-command timeout breakdowns (this is global, like the
  existing counters).
- Measuring *how long* a stage took (this counts *occurrences*, not durations).
- Wall-clock attribution of the coordinator fan-out wait beyond the 3 named
  stages (see [§8 Open questions](#8-open-questions)).

## 3. Background — current state (with code references)

### 3.1 The data model
The global stats live in [`src/info/global_stats.h`](../../src/info/global_stats.h).
The timeout counter is a single `size_t timeout` inside two structs:

```c
// src/info/global_stats.h:56-69
typedef struct {
  size_t syntax;
  size_t arguments;
  size_t timeout;          // <-- single, stage-less counter
  size_t oom;
  size_t unavailableSlots;
} QueryErrorsGlobalStats;

typedef struct {
  size_t timeout;          // <-- single, stage-less counter
  size_t oom;
  size_t maxPrefixExpansion;
  size_t asm_inaccuracy;
} QueryWarningGlobalStats;
```

These are held four times in `QueriesGlobalStats`
([`global_stats.h:71-81`](../../src/info/global_stats.h)) — `shard_errors`,
`coord_errors`, `shard_warnings`, `coord_warnings` — giving the existing
`{shard,coord} × {error,warning}` matrix.

### 3.2 Recording
Two functions mutate the counters
([`src/info/global_stats.c:144-191`](../../src/info/global_stats.c)):

```c
void QueryErrorsGlobalStats_UpdateError(QueryErrorCode code, int toAdd, bool coord);   // :144
void QueryWarningsGlobalStats_UpdateWarning(QueryWarningCode code, int toAdd, bool coord); // :172
```

The timeout codes are defined in the Rust FFI header
[`src/redisearch_rs/headers/query_error.h`](../../src/redisearch_rs/headers/query_error.h)
(`QUERY_ERROR_CODE_TIMED_OUT`, `QUERY_WARNING_CODE_TIMED_OUT`).

The error-vs-warning decision is policy-driven
([`src/aggregate/aggregate_exec_common.c:26-38`](../../src/aggregate/aggregate_exec_common.c)):
`TimeoutPolicy_Fail` → recorded as **error**; `TimeoutPolicy_Return` /
`TimeoutPolicy_ReturnStrict` → recorded as **warning**. The policy enum
`RSTimeoutPolicy` lives in [`src/config.h`](../../src/config.h).

### 3.3 Reading & reporting to Redis
`TotalGlobalStats_GetQueryStats()`
([`global_stats.c:96-124`](../../src/info/global_stats.c)) snapshots the counters
atomically. They are rendered in `AddToInfo_ErrorsAndWarnings()`
([`src/info/info_redis/info_redis.c:312-342`](../../src/info/info_redis/info_redis.c)):

```c
// shard section ("warnings_and_errors")
RedisModule_InfoAddFieldULongLong(ctx, "shard_total_query_errors_timeout",   stats.shard_errors.timeout);   // :323
RedisModule_InfoAddFieldULongLong(ctx, "shard_total_query_warnings_timeout", stats.shard_warnings.timeout); // :324
// coord section ("coordinator_warnings_and_errors")
RedisModule_InfoAddFieldULongLong(ctx, "coord_total_query_errors_timeout",   stats.coord_errors.timeout);   // :335
RedisModule_InfoAddFieldULongLong(ctx, "coord_total_query_warnings_timeout", stats.coord_warnings.timeout); // :336
```

### 3.4 Where timeouts are detected / recorded today
The recording happens at a handful of **centralized** sites, plus the
coordinator/hybrid variants:

| Stage (proposed) | Detection / recording site | Reference |
|---|---|---|
| Wait in queue | Request prep timed out before execution | `replyForPreExecutionTimeout()` [`aggregate_exec.c:551-567`](../../src/aggregate/aggregate_exec.c), reached from [`:1954`](../../src/aggregate/aggregate_exec.c) and [`:2507`](../../src/aggregate/aggregate_exec.c) |
| Wait in queue | RETURN_STRICT timeout callback won the claim before the pipeline produced results | `startPipeline()` TryClaim-lost / `AREQ_TimedOut` pre-pipeline [`aggregate_exec.c:413-427`](../../src/aggregate/aggregate_exec.c) |
| Wait in queue | FAIL blocked-client callback fires | `QueryTimeoutFailCallback()` [`aggregate_exec.c:~1548`](../../src/aggregate/aggregate_exec.c); `DistAggregateTimeoutFailCallback()` [`coord/dist_aggregate.c:~718`](../../src/coord/dist_aggregate.c); `DistSearchTimeoutFailCallback()` [`module.c:~4429`](../../src/module.c) |
| RP pipeline | Pipeline returns `RS_RESULT_TIMEDOUT` | `startPipelineCommon()` / `AggregateResults` set `rc`; recorded in `handleSendChunkError()` [`aggregate_exec.c:537-549`](../../src/aggregate/aggregate_exec.c) and `trackWarnings_Resp2()` [`aggregate_exec.c:604-632`](../../src/aggregate/aggregate_exec.c) (+ a RESP3 variant near [`:794`](../../src/aggregate/aggregate_exec.c)) |
| Reply phase | **Not detected today** — there is no deadline check during serialization | — |
| (coord fan-out) | Shard reply carried a TIMEDOUT warning / barrier timeout | `processWarningsAndCleanup()` [`coord/rpnet.c:163`](../../src/coord/rpnet.c), `shardResponseBarrier_HandleTimeout()` [`coord/rpnet.c:110`](../../src/coord/rpnet.c); surfaced via `QEXEC_S_SHARD_TIMED_OUT_WARNING` in `trackWarnings_Resp2()` [`aggregate_exec.c:609-610`](../../src/aggregate/aggregate_exec.c) |

The full inventory of `QueryErrorsGlobalStats_UpdateError` /
`QueryWarningsGlobalStats_UpdateWarning` timeout call sites is in
[Appendix A](#appendix-a--timeout-recording-call-sites).

**Key observation:** the recording is already funneled through a small number of
functions. The challenge is not *where* to record, but *how to know the stage*
at the recording point — because the main-thread timeout callbacks run
concurrently with the background worker and don't currently know how far the
worker progressed.

## 4. Proposed design

### 4.1 Stage enum
Add to [`src/info/global_stats.h`](../../src/info/global_stats.h) (or a small
shared header included by both the stats and execution layers):

```c
typedef enum {
  QUERY_TIMEOUT_STAGE_QUEUE    = 0, // before the RP pipeline started running
  QUERY_TIMEOUT_STAGE_PIPELINE = 1, // during result-processor execution
  QUERY_TIMEOUT_STAGE_REPLY    = 2, // during reply serialization
  QUERY_TIMEOUT_STAGE_COUNT
} QueryTimeoutStage;
```

### 4.2 Data model change (additive)
**Keep** the existing scalar `timeout` field exactly as it is, and **add** a
per-stage struct alongside it in **both** the error and warning structs
([`global_stats.h:56-69`](../../src/info/global_stats.h)):

```c
typedef struct {
  size_t queue;
  size_t pipeline;
  size_t reply;
} QueryTimeoutStageStats;

typedef struct {
  size_t syntax;
  size_t arguments;
  size_t timeout;                   // UNCHANGED — existing aggregate counter
  QueryTimeoutStageStats timeout_by_stage; // NEW — per-stage breakdown
  size_t oom;
  size_t unavailableSlots;
} QueryErrorsGlobalStats;

typedef struct {
  size_t timeout;                   // UNCHANGED — existing aggregate counter
  QueryTimeoutStageStats timeout_by_stage; // NEW — per-stage breakdown
  size_t oom;
  size_t maxPrefixExpansion;
  size_t asm_inaccuracy;
} QueryWarningGlobalStats;
```

The existing `timeout` counter keeps its current meaning and is incremented on
every timeout exactly as today. The per-stage struct is incremented in the same
call (§4.3), giving a **maintained invariant** that doubles as a test assertion:

```
timeout == timeout_by_stage.queue + timeout_by_stage.pipeline + timeout_by_stage.reply
```

`TotalGlobalStats_GetQueryStats()`
([`global_stats.c:96-124`](../../src/info/global_stats.c)) is updated to also
snapshot the three new sub-fields per struct (8 new `READ()` lines); the
existing `timeout` reads are left as-is.

### 4.3 Recording API
Add a `QueryTimeoutStage stage` parameter to the two recording functions
([`global_stats.c:144-191`](../../src/info/global_stats.c)). The stage is only
consulted inside the `*_TIMED_OUT` case; other codes ignore it:

```c
void QueryErrorsGlobalStats_UpdateError(QueryErrorCode code, QueryTimeoutStage stage,
                                        int toAdd, bool coord);
void QueryWarningsGlobalStats_UpdateWarning(QueryWarningCode code, QueryTimeoutStage stage,
                                            int toAdd, bool coord);
```

```c
case QUERY_ERROR_CODE_TIMED_OUT:
  INCR_BY(queries_errors->timeout, toAdd);   // existing aggregate — unchanged
  switch (stage) {                           // new per-stage breakdown
    case QUERY_TIMEOUT_STAGE_QUEUE:    INCR_BY(queries_errors->timeout_by_stage.queue,    toAdd); break;
    case QUERY_TIMEOUT_STAGE_PIPELINE: INCR_BY(queries_errors->timeout_by_stage.pipeline, toAdd); break;
    case QUERY_TIMEOUT_STAGE_REPLY:    INCR_BY(queries_errors->timeout_by_stage.reply,    toAdd); break;
    default: break;
  }
  break;
```

Incrementing both in the same branch keeps the aggregate and the breakdown
consistent by construction.

Adding the parameter (rather than a separate function) makes the compiler flag
**every** call site, so no timeout path can be silently left unclassified —
directly satisfying the "increment on all possible timeouts" requirement. The
~40 call sites in [Appendix A](#appendix-a--timeout-recording-call-sites) get a
mechanical update: timeout-specific sites pass the request's recorded stage
(§4.4); non-timeout sites pass `QUERY_TIMEOUT_STAGE_QUEUE` (ignored).

> Alternative considered: keep the existing signatures and add dedicated
> `QueryTimeoutGlobalStats_Record{Error,Warning}(stage, …)` helpers. Rejected
> because several generic call sites pass `QueryError_GetCode(err)` (which *may*
> be the timeout code), so they would still need the stage — and the compiler
> would not force us to update them.

### 4.4 Stage propagation — the execution-phase marker
The main-thread timeout callbacks (`QueryTimeoutFailCallback`,
`QueryTimeoutReturnStrictCallback`, `CursorReadTimeout*Callback`, and the
coordinator equivalents) fire *asynchronously* relative to the background
worker. To classify the stage they must know how far the worker has progressed.

Add an **atomic execution-phase marker** to `RequestSyncCtx`
([`src/aggregate/aggregate.h:180-209`](../../src/aggregate/aggregate.h)), which
already hosts the `timedOut` latch and the GIL handshake state and is shared by
`AREQ` and `HybridRequest`:

```c
typedef enum {
  EXEC_PHASE_QUEUED    = 0, // job created / queued, pipeline not started
  EXEC_PHASE_PIPELINE  = 1, // inside startPipelineCommon / AggregateResults
  EXEC_PHASE_REPLY     = 2, // pipeline produced results; serializing the reply
} QueryExecPhase;

typedef struct RequestSyncCtx {
  RS_Atomic(bool) timedOut;
  RS_Atomic(QueryExecPhase) execPhase;   // NEW
  ...
} RequestSyncCtx;
```

The background worker advances the marker as it progresses:
- `EXEC_PHASE_QUEUED` at init (`RequestSyncCtx_Init`,
  [`aggregate.h:212`](../../src/aggregate/aggregate.h)).
- `EXEC_PHASE_PIPELINE` at the top of `startPipelineCommon()` (just after the
  TryClaim handshake in `startPipeline()`,
  [`aggregate_exec.c:433`](../../src/aggregate/aggregate_exec.c)).
- `EXEC_PHASE_REPLY` once the pipeline returns and we enter serialization
  (`sendChunk` / `serializeAndReplyResults` / `AREQ_StoreResults`
  [`aggregate_exec.c:449`](../../src/aggregate/aggregate_exec.c)).

Then define a single classifier used everywhere a timeout is recorded:

```c
// Returns the stage for a timeout, given the request's last-known exec phase
// and whether the deadline was hit synchronously by the pipeline.
QueryTimeoutStage AREQ_TimeoutStage(const AREQ *req);
```

- For **synchronous** detections (the pipeline itself returned
  `RS_RESULT_TIMEDOUT`), the marker is still `PIPELINE`, so the stage is
  `PIPELINE`. Once serialization begins the marker is advanced to `REPLY`
  (§4.5).
- For **asynchronous** detections (a blocked-client timeout callback fired), map
  `execPhase → stage`: `QUEUED → QUEUE`, `PIPELINE → PIPELINE`, `REPLY → REPLY`.
- For **request-prep** timeouts (`replyForPreExecutionTimeout`, before an `AREQ`
  even exists) the stage is `QUEUE` by construction.

Defaulting the marker to `QUEUED` and advancing it monotonically means an
otherwise-unclassified timeout still lands in a real bucket reflecting how far
execution got.

### 4.5 Reply-phase timeout detection
Reply-phase timeouts are not detected today (§3.4). As implemented, when the
pipeline finishes **without** timing out and we enter serialization, the
execution-phase marker is advanced to `REPLY` (`enterReplyPhase()` in the
`sendChunk_Resp2/3` paths,
[`aggregate_exec.c`](../../src/aggregate/aggregate_exec.c)):

```c
// Entering the reply/serialization phase. Move the marker only -- never force a
// timeout -- so streamed (RETURN-policy) results are not suppressed.
static inline void enterReplyPhase(AREQ *req, int rc) {
  if (rc != RS_RESULT_TIMEDOUT) {
    AREQ_SetTimeoutStage(req, QUERY_TIMEOUT_STAGE_REPLY);
  }
}
```

A timeout that fires from this point on is then attributed to `REPLY`. The
dominant real case is the **asynchronous stored-reply path**: the background
worker has finished the pipeline and stored results, and the blocked-client
deadline fires while the reply is still pending on the main thread — the
timeout callback records the count via `AREQ_TimeoutStage(req)` and reads
`REPLY`.

> **Design refinement vs. the original §4.5 sketch.** An earlier draft set
> `rc = RS_RESULT_TIMEDOUT` at the reply boundary. That was dropped: under the
> `RETURN` policy `serializeAndReplyResults` *streams* rows (it does not replay a
> fully-materialized buffer), so forcing `rc` to timed-out before serialization
> could suppress rows that would otherwise be returned. Moving the marker only
> is behavior-preserving — it never changes whether a timeout is reported, only
> the stage attributed to one that is. The trade-off: a purely *synchronous*
> reply-phase timeout on the inline (non-threaded) path is attributed to
> `PIPELINE` via `startPipelineCommon`'s existing post-aggregation check rather
> than to `REPLY`. This is acceptable for an observability metric and avoids any
> risk to the reply contents.

### 4.6 INFO reporting
Leave the four existing aggregate fields **exactly as they are** and add the
per-stage breakdown alongside them in `AddToInfo_ErrorsAndWarnings()`
([`info_redis.c:312-342`](../../src/info/info_redis/info_redis.c)):

```c
// shard "warnings_and_errors" section — existing aggregate, UNCHANGED:
RedisModule_InfoAddFieldULongLong(ctx, "shard_total_query_errors_timeout", stats.shard_errors.timeout);
// new per-stage fields:
RedisModule_InfoAddFieldULongLong(ctx, "shard_total_query_errors_timeout_queue",    stats.shard_errors.timeout_by_stage.queue);
RedisModule_InfoAddFieldULongLong(ctx, "shard_total_query_errors_timeout_pipeline", stats.shard_errors.timeout_by_stage.pipeline);
RedisModule_InfoAddFieldULongLong(ctx, "shard_total_query_errors_timeout_reply",    stats.shard_errors.timeout_by_stage.reply);
// …and the same triple for shard_total_query_warnings_timeout_*
```

Mirror the same six new fields in the `coordinator_warnings_and_errors` section
with the `coord_` prefix. Net: **4 aggregate fields kept + 12 new fields**
(2 stage-able counters × 3 stages × {shard, coord}).

New field names:

| Side | Kind | Field |
|---|---|---|
| shard | errors | `shard_total_query_errors_timeout_{queue,pipeline,reply}` |
| shard | warnings | `shard_total_query_warnings_timeout_{queue,pipeline,reply}` |
| coord | errors | `coord_total_query_errors_timeout_{queue,pipeline,reply}` |
| coord | warnings | `coord_total_query_warnings_timeout_{queue,pipeline,reply}` |

## 5. Implementation plan (one item ≈ one reviewable commit)

1. **Data model + API (additive, no behavior change).**
   Add `QueryTimeoutStage` and `QueryTimeoutStageStats`; add the
   `timeout_by_stage` field to both structs **without touching** the existing
   `timeout` field; thread the `stage` parameter through
   `QueryErrorsGlobalStats_UpdateError` /
   `QueryWarningsGlobalStats_UpdateWarning` (incrementing both the old and new
   counters in the timeout case) and extend `TotalGlobalStats_GetQueryStats()`.
   Mechanically update all call sites in
   [Appendix A](#appendix-a--timeout-recording-call-sites) to pass
   `QUERY_TIMEOUT_STAGE_PIPELINE` for now (so the breakdown initially attributes
   everything to `pipeline` while the aggregate stays correct). Files:
   [`global_stats.h`](../../src/info/global_stats.h),
   [`global_stats.c`](../../src/info/global_stats.c), all call sites.

2. **INFO reporting.** Add the 12 new fields and leave the 4 existing aggregate
   fields untouched in [`info_redis.c`](../../src/info/info_redis/info_redis.c).
   At this point everything still funnels into `pipeline`, so the new fields are
   wired and testable end-to-end before real classification lands.

3. **Execution-phase marker.** Add the atomic `execPhase` to `RequestSyncCtx`
   ([`aggregate.h`](../../src/aggregate/aggregate.h)) with setter/getter; advance
   it in `startPipeline`/`startPipelineCommon` and the serialization entry; add
   `AREQ_TimeoutStage()` and the hybrid equivalent.

4. **Classify QUEUE timeouts.** Use `execPhase`/`AREQ_TimeoutStage()` at:
   `replyForPreExecutionTimeout` ([`aggregate_exec.c:551`](../../src/aggregate/aggregate_exec.c)),
   `startPipeline` TryClaim-lost / pre-pipeline `AREQ_TimedOut`
   ([`:413-427`](../../src/aggregate/aggregate_exec.c)),
   `QueryTimeoutFailCallback` / `CursorReadTimeoutFailCallback`,
   and the coordinator FAIL callbacks
   ([`dist_aggregate.c`](../../src/coord/dist_aggregate.c),
   [`module.c`](../../src/module.c)).

5. **Classify PIPELINE timeouts.** Confirm the synchronous
   `rc == RS_RESULT_TIMEDOUT` path records `PIPELINE` via `AREQ_TimeoutStage()`
   in `handleSendChunkError` / `trackWarnings_Resp2` and the RESP3 variant.

6. **Add + classify REPLY timeouts (§4.5).** New reply-boundary deadline check;
   record `REPLY`. Include the coordinator drain / RETURN_STRICT reply callbacks
   when `execPhase == EXEC_PHASE_REPLY`.

7. **Coordinator & hybrid parity.** Mirror the marker + classification in
   [`coord/dist_aggregate.c`](../../src/coord/dist_aggregate.c),
   [`coord/rpnet.c`](../../src/coord/rpnet.c),
   [`coord/hybrid/`](../../src/coord/hybrid/),
   [`hybrid/hybrid_exec.c`](../../src/hybrid/hybrid_exec.c), and the
   `module.c` distributed search callbacks. Decide coordinator fan-out mapping
   per [§8](#8-open-questions).

8. **Tests** (see §6) and **docs** — update the `INFO` field reference and add
   release notes.

## 6. Testing plan

The change is **not done** until new/changed behavior is covered and build,
lint, and test suites are green.

- **C unit (`tests/cpptests`)** — exercise `QueryErrorsGlobalStats_UpdateError` /
  `QueryWarningsGlobalStats_UpdateWarning` with each `QueryTimeoutStage` and
  assert the right sub-counter increments **and** that the additive invariant
  holds: `timeout == timeout_by_stage.queue + .pipeline + .reply`. There is an
  existing global-stats test target to extend.
- **Python e2e (`tests/pytests`)** — drive real timeouts per stage and assert the
  new `FT.INFO`/module `INFO` fields. The opened file
  [`tests/pytests/test_blocked_client_timeout.py`](../../tests/pytests/test_blocked_client_timeout.py)
  and the timeout-policy tests are the natural home. Use the existing debug
  hooks/sync points (`SYNC_POINT_BEFORE_AGGREGATE_RESULTS_CLAIM`,
  `SYNC_POINT_BEFORE_FIRST_READ`, `StoreResultsDebugCtx`,
  [`aggregate_exec.c:354-398`](../../src/aggregate/aggregate_exec.c)) and a tiny
  `TIMEOUT 1` to force:
  - a **queue** timeout (deadline already past before the worker starts, e.g. via
    the pre-execution path),
  - a **pipeline** timeout (slow query with many docs),
  - a **reply** timeout (large result set; deadline tripped at the reply boundary
    via a debug pause).
  Assert the per-stage field increments and that the aggregate equals the sum.
  Cover RESP2 + RESP3 and `RETURN` / `FAIL` / `RETURN_STRICT`, on both
  standalone and cluster (coord) setups.
- **Coverage** — run [/check-flow-coverage](../../.skills/check-flow-coverage/SKILL.md)
  on the touched C files to confirm the new branches are exercised.

## 7. Backward compatibility & risk

- The four existing aggregate `INFO` fields and their backing `timeout` counters
  are **left completely untouched** (purely additive change), so
  dashboards/alerts keep working with identical values.
- Counters only ever increase and are read with relaxed atomics; the added
  `execPhase` is a relaxed atomic store on a path that already does atomic work,
  so no measurable overhead.
- Stage misclassification (e.g. a race between the worker advancing `execPhase`
  and a callback reading it) only moves a count between buckets; the aggregate
  total stays correct. This is acceptable for an observability metric.
- Risk concentrates in step 6 (reply-phase detection) — the only step that adds a
  new detection point. It is isolated behind the reply-boundary check and does
  not change reply *behavior*.

## 8. Decisions & open questions

### Decided
1. **Extend, don't replace.** The existing `timeout` counters and `INFO` fields
   are kept untouched; the per-stage counters are purely additive (§4.2).
2. **Coordinator fan-out → `PIPELINE`.** The coordinator's wait for shard replies
   (`MRIterator_NextWithTimeout`, `shardResponseBarrier_HandleTimeout`) is
   recorded as `PIPELINE` on the `coord_*` counters (RPNet is the coord-side
   result processor). No separate "fan-out" bucket for now.
3. **Reply phase = marker advance (behavior-preserving).** On entering
   serialization the execution-phase marker is advanced to `REPLY` without
   forcing a timeout (§4.5), so a deadline that fires during the reply — chiefly
   the async stored-reply path — is attributed to `REPLY`. The earlier
   "boundary check that sets `rc=TIMEDOUT`" idea was dropped because it could
   suppress streamed `RETURN`-policy results.
4. **Request-prep folds into `QUEUE`.** Timeouts during `prepareRequest`
   (`replyForPreExecutionTimeout`) are counted as `QUEUE`; parsing/prep is not a
   separate stage. It happens before pipeline execution, so it belongs with the
   pre-execution / wait-in-queue bucket.

### Still open
_None — all design decisions resolved; ready for implementation._

## Appendix A — timeout recording call sites

Sites that call `QueryErrorsGlobalStats_UpdateError(... TIMED_OUT ...)` or
`QueryWarningsGlobalStats_UpdateWarning(... TIMED_OUT ...)` (or pass a generic
`QueryError_GetCode(...)` that may be the timeout code). Each must pass a stage
after step 1. "Proposed stage" is the intended classification.

| File | Line(s) | Kind | Proposed stage |
|---|---|---|---|
| `src/aggregate/aggregate_exec.c` | 540, 544 (`handleSendChunkError`) | error | PIPELINE / REPLY (via `AREQ_TimeoutStage`) |
| `src/aggregate/aggregate_exec.c` | 560 (`replyForPreExecutionTimeout`) | error | QUEUE |
| `src/aggregate/aggregate_exec.c` | 612, 794 (`trackWarnings_*`) | warning | PIPELINE / REPLY |
| `src/aggregate/aggregate_exec.c` | 1067, 1128 (profile empty-results) | warning | PIPELINE |
| `src/aggregate/aggregate_exec.c` | 1229, 1724, 1800, 1830 (stored-reply error) | error | per `execPhase` |
| `src/aggregate/aggregate_exec.c` | 1556, 1566 (`QueryTimeoutFailCallback`) | error | QUEUE / per `execPhase` |
| `src/aggregate/aggregate_exec.c` | 1606, 1620 (`QueryTimeoutReturnStrictCallback`) | error/empty | QUEUE / per `execPhase` |
| `src/aggregate/aggregate_exec.c` | 1749, 1758 (`CursorReadTimeoutFailCallback`) | error | per `execPhase` |
| `src/aggregate/aggregate_exec.c` | 1968 (`error:` final status) | error | per `execPhase` |
| `src/aggregate/reply_empty.c` | 134 (hybrid empty reply) | warning | PIPELINE |
| `src/hybrid/hybrid_exec.c` | 63, 86, 314, 591, 673, 876, 900 | error/warning | per `execPhase` |
| `src/module.c` | 3152 (coord FT.SEARCH warning) | warning | PIPELINE (coord fan-out) |
| `src/module.c` | 4429, 4474 (`DistSearchTimeout*Callback`) | error | QUEUE / per `execPhase` |
| `src/coord/dist_aggregate.c` | 739, 790, 831, 844, 918 | error/empty | QUEUE / per `execPhase` |
| `src/coord/hybrid/dist_hybrid.c` | (coord hybrid timeout) | error | per `execPhase` |

> Line numbers reflect the tree at design time and will drift; treat the
> enclosing function names as the stable anchors.
