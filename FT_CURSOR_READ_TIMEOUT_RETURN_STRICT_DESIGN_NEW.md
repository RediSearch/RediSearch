# FT.CURSOR READ + RETURN_STRICT — Coordinator Design (NEW)

Supersedes `FT_CURSOR_READ_TIMEOUT_RETURN_STRICT_DESIGN.md`. Captures the design
agreed after re-deriving the threading model and the `drainOnly` safety contract.

> **Scope**: Coordinator path only. Shard-level RETURN_STRICT for cursor reads
> is out of scope (reuses the shard FAIL/RETURN policies already in place).

---

## 1. Threading model (corrected)

For coord + RETURN_STRICT cursor reads:

| Step | Thread | Action |
|------|--------|--------|
| 1 | **Main** | `CursorCommand` (module.c) parses argv and peeks the cursor's cached info via the extended `Cursors_PeekTimeoutInfo` (now reports `found`). If `!info.found`, the command replies inline `"Cursor not found, id: <cid>"` and **returns** — no BC is armed and no DIST_THPOOL dispatch happens. |
| 2 | **Main** | For RETURN_STRICT (new branch alongside the existing FAIL branch), allocates a `CoordRequestCtx`, sets `useReplyCallback = true`, installs a **new** `reply_callback` and `timeout_callback`, then dispatches via `ConcurrentSearch_HandleRedisCommandEx(DIST_THREADPOOL, …)`. The `BlockedClient` is armed here, on the main thread. |
| 3 | **DIST_THPOOL** | `RSCursorReadCommand` runs. With `upstreamBC != NULL` and `reqCtx != NULL`, it takes the **inline** branch (sub-case 1, lines 2080-2099 in `aggregate_exec.c`). |
| 4 | **DIST_THPOOL** | (new) Early TimedOut check — see §3 scenario 1. |
| 5 | **DIST_THPOOL** | `Cursors_TakeForExecution` → `CoordRequestCtx_SetRequest` → `cursorRead` → `runCursor` → `sendChunk` → `startPipelineCommon` → `AREQ_TryClaimAggregateResults`. |
| 6 | **Main** (timer) | If the BC deadline expires, the new timeout callback fires on the main thread and coordinates with DIST_THPOOL via `CoordRequestCtx`'s lock + `RequestSyncCtx` (TimedOut atomic, abort channel, `aggregateResultsDone` condvar). |

**Key facts:**
- The cursor-read RETURN_STRICT path does **not** go through `cursorRead_ctx` /
  `workersThreadPool_AddWork`. That branch (line 2045) requires `!upstreamBC`
  and is only taken by shard / single-shard / `!RunInThread` flows.
- The existing FAIL `DistAggregateTimeoutFailClient` and `CursorReadTimeoutFailCallback`
  are **not modified**. A new RETURN_STRICT-cursor-read variant is added.
- **Cid validity is established on the main thread (step 1) for both FAIL and
  RETURN_STRICT.** Consequence: by the time the BC is armed, the cid was a
  real cursor; the timer callback can blindly trust the cid in argv. If the
  cursor is deleted/expires between peek and timer fire, that is benign —
  the user's retry will hit `RSCursorReadCommand` → `TakeForExecution` →
  NULL → inline `"Cursor not found"`. Self-correcting after one round-trip.

---

## 2. Three scenarios

**The fundamental split is whether BG has called `Cursors_TakeForExecution`
for this request.** Per §3.1, BG performs `TakeForExecution` and
`CoordRequestCtx_SetRequest` atomically under the same `setRequestLock`,
so `reqCtx->request != NULL` (observed by the timer) is a reliable proxy
for "BG took the cursor". The `req` pointer itself is only used by the
timer as a wait-target/identifier; the *semantic* discriminator is the
take.

The timer itself does not branch on the pipeline-vs-error sub-cases — it
always sets TimedOut, wakes the abort channel, and waits for BG's signal.
The scenarios below describe **which BG path produces the signal** and
**what `hasStoredResults` looks like at wake-up**.

### Scenario 1 — Timer fires before BG calls `TakeForExecution`

BG has not yet taken the cursor (and therefore has not called
`SetRequest` either; both happen under the same lock — see §3.1). Timer
observes `req == NULL`, which is the proxy for "not taken". Per §3.1,
BG's pre-take `TimedOut` check then short-circuits and BG returns without
ever taking the cursor or producing a signal — so the timer must reply
inline with cursor-shaped empty + cid. The cursor remains paused at its
last position and is reusable on the next `FT.CURSOR READ`.

### Scenario 2 — Timer fires after `TakeForExecution`, before BG enters pipeline

BG has taken the cursor and called `SetRequest` (atomically, §3.1) but
hasn't yet reached `startPipelineCommon`. Timer observes `req != NULL`.
Two sub-paths:
- **Pipeline-bound**: BG eventually reaches the existing
  `(!TryClaim || TimedOut)` check at `aggregate_exec.c:368-369` → bails
  with `RS_RESULT_TIMEDOUT` → existing `sendChunk` store+signal site fires.
  Wake-up state: `hasStoredResults == true` (empty results).
- **Error-bound**: BG bails before reaching `startPipeline` (e.g. spec
  dropped in `cursorRead`). Funnels through `AREQ_ReplyOrStoreError`,
  which signals (§3.4). Wake-up state: `hasStoredResults == false`,
  `storedReplyState.err` populated.

### Scenario 3 — Timer fires while BG is inside the pipeline

BG took the cursor, called `SetRequest`, and already entered the pipeline
(past the `TryClaim` check). Timer observes `req != NULL` (same as
scenario 2 — the timer cannot distinguish 2 from 3, and does not need
to). Pipeline runs to completion or to `RS_RESULT_TIMEDOUT` inside
`endProc->Next` (observing the TimedOut atomic via
`MRIterator_NextWithTimeout` / `rpnetNext`). Either way, control reaches
the existing `sendChunk` store+signal site. Wake-up state:
`hasStoredResults == true` (possibly with partial rows if RPs yield
them).

> **Why `req != NULL` is sufficient as the take-proxy.** §3.1 wraps
> `Cursors_TakeForExecution` and `CoordRequestCtx_SetRequest` in a single
> critical section under `setRequestLock`. The timer's TimedOut write
> takes the same lock. So at the moment the timer reads `req`, exactly
> one of two states holds: (a) BG has not yet entered the take+set
> critical section, in which case `req == NULL` *and* the cursor has not
> been taken (scenario 1); or (b) BG has completed both, in which case
> `req != NULL` *and* the cursor was taken (scenarios 2/3). There is no
> "took but didn't set" or "set but didn't take" intermediate window.

---

## 3. Per-scenario handling

### 3.1 Scenario 1 — pre-take early skip on BG, direct empty reply on timer

**Race that motivates the change:**
With the current sub-case (1) layout (`TakeForExecution` then check TimedOut):

```
T0  DIST_THPOOL: TakeForExecution → cur->pos = -1
T1  Main/timer:  lock → SetTimedOut → unlock → req == NULL → reply (empty + cid)
T2  Client:      receives reply, fires next FT.CURSOR READ
T3  Main→DIST:   new RSCursorReadCommand → TakeForExecution → pos == -1
                 → "Cursor not found, id: <cid>"
T4  DIST_THPOOL (orig): lock → sees TimedOut → unlock → Cursor_Pause   ← too late
```

A retry between T2 and T4 spuriously gets "Cursor not found".

**Fix: option (a) — early TimedOut check before `TakeForExecution`.**

Gated by a new flag on `CoordRequestCtx` (e.g. `isCursorReadReturnStrict`) so
the FAIL path is untouched.

```c
// In RSCursorReadCommand, BEFORE Cursors_TakeForExecution:
if (reqCtx && CoordRequestCtx_IsCursorReadReturnStrict(reqCtx)) {
  CoordRequestCtx_LockSetRequest(reqCtx);
  if (CoordRequestCtx_TimedOut(reqCtx)) {
    CoordRequestCtx_UnlockSetRequest(reqCtx);
    return REDISMODULE_OK; // cursor never taken; pos > 0; user retry safe
  }
  // Take cursor + SetRequest under the SAME lock so the timer cannot observe
  // the in-between (post-take, pre-SetRequest) window.
  Cursor *cursor = Cursors_TakeForExecution(GetGlobalCursor(cid), cid);
  if (!cursor) {
    CoordRequestCtx_UnlockSetRequest(reqCtx);
    // No AREQ exists yet (we never took the cursor), so there is no
    // storedReplyState to populate. Reply directly via the BC's reply
    // context — the BC was armed with a reply_callback that honors a
    // pre-populated reply, but on this path we short-circuit and emit
    // the error inline before returning to the dispatcher.
    RedisModule_ReplyWithError(ctx, "Cursor not found");
    return REDISMODULE_OK;
  }
  // Reset per-read sync state before publishing `req` to the timer.
  // This must happen under setRequestLock; see §5.5.2.
  AREQ_ResetAggregateResultsClaim(cursor->execState); // extended per §5.5.1
  CoordRequestCtx_SetRequest(reqCtx, cursor->execState);
  // Park the cursor handle on the per-read reqCtx (see §3.5 for ownership
  // rules and consumer table). Done inside the same critical section so
  // no consumer can observe "took but not parked" — every code path that
  // observes `req != NULL` (timer, reply, free_privdata) also observes a
  // parked cursor (or finds it cleared by an earlier consumer).
  CoordRequestCtx_SetParkedCursor(reqCtx, cursor);
  CoordRequestCtx_UnlockSetRequest(reqCtx);
  // Continue with cursorRead(ctx, cursor, count, false);
}
```

Lock-ordering rule introduced: **`reqCtx` (per-request) → cursor table (global)**.
No other code path takes the cursor table while holding `reqCtx`, so no
inversion. Worth a comment at both lock acquisitions.

**Why take + SetRequest under the same lock matters for scenario
discrimination.** Co-locating `TakeForExecution` and `SetRequest` in one
critical section is what allows the timer to use `req == NULL` as a
*reliable* proxy for "BG has not yet taken the cursor". If the two were
separated, the timer could observe `req == NULL` *after* BG had already
taken the cursor (and set `pos = -1`), reproducing exactly the T0–T4 race
above. With the co-located take+set, the timer's read of `req` (under
the same lock as BG's write) collapses into one of two states:

| BG state under `setRequestLock` | Timer observation | Scenario |
|---|---|---|
| pre-take (no `TakeForExecution`, no `SetRequest`) | `req == NULL` | 1 |
| post-take + post-`SetRequest` | `req != NULL` | 2 / 3 |

The scenario split is **logically about whether BG took the cursor**;
`req != NULL` is just the syntactic check the timer performs, made
trustworthy by the lock co-location.

**Timer side for scenario 1:** `req == NULL` ⇒ cursor was not taken. BG
either hasn't run yet or, if it ran, hit the early TimedOut check above
and returned without taking. No signal will arrive. Reply with cursor-
	shaped empty + cid directly — the cid is trusted (validated in
	`CursorCommand` per §1, step 1; no per-callback re-verification needed).
	The helper also emits the timeout warning and updates coordinator warning
	stats, matching the normal stored-results timeout reply. See §5 for the
	helper.

### 3.2 Scenario 2 — BG hasn't entered the pipeline; timer waits

We **drop the timer-side `TryClaim`** entirely. Both scenarios 2 and 3 are
handled by the same timer code: set TimedOut, wake abort, wait for BG's
signal, drain residual replies, reply.

**BG behavior in scenario 2:** BG is between `SetRequest` and
`startPipeline.TryClaim`. Two sub-paths:

- **Happy sub-path**: BG reaches `startPipeline.TryClaim`, the existing
  `(!TryClaim || TimedOut)` check at `aggregate_exec.c:368` bails with
  `RS_RESULT_TIMEDOUT`. Control returns to `sendChunk_Resp{2,3}` which
  unconditionally calls `AREQ_StoreResults` + `AREQ_SignalAggregateResultsComplete`
  (lines 702-715 / 909-922).
- **Error sub-path**: BG bails *before* reaching `startPipeline` via an
  early-exit in `cursorRead` (e.g. `!StrongRef_Get(execution_ref)` at
  `aggregate_exec.c:1896-1905` — index dropped while idle). These paths
  funnel through `AREQ_ReplyOrStoreError`. We extend that helper to also
  signal completion when `RequiresThreadsSyncResults` is true — see §3.4.

In both sub-paths, the timer's wait completes; the cursor is freed by BG's
own bail-out (or paused/freed by `AREQ_ReplyWithStoredResults` in the happy
sub-path). The timer's reply distinguishes the two sub-paths by
`req->storedReplyState.hasStoredResults`:

- **`hasStoredResults == true` (happy sub-path)**: BG reached the pipeline
  bail-out, called `AREQ_StoreResults` (which sets `hasStoredResults`
  and clones any partial pipeline output), and signaled. The timer
  drains residual shard replies (§5.4) and calls
  `AREQ_ReplyWithStoredResults`, which serializes the (possibly empty)
  partial result set in the cursor-shaped envelope `[results, cid]` and
  pauses or frees the cursor based on `QEXEC_S_ITERDONE`.
- **`hasStoredResults == false` (error sub-path)**: BG bailed in
  `cursorRead` *before* the pipeline ever ran, so no result set exists
  to serialize. `AREQ_ReplyOrStoreError` populated
  `req->storedReplyState.err` with the `QueryError` (e.g. `"The index
  was dropped while the cursor was idle"`) and freed the cursor on its
  way out. The timer:
  1. asserts `QueryError_HasError(&req->storedReplyState.err)` (the
     contract: `!hasStoredResults` after a signal ⇔ stored error);
  2. calls `QueryErrorsGlobalStats_UpdateError(code, 1, !IsInternal(req))`
     for stats parity with the inline reply path;
  3. calls `QueryError_ReplyAndClear(ctx, &req->storedReplyState.err)` to
     flush the error to the client as a plain `-ERR` reply (**not** a
     cursor-shaped envelope — there is no cid to return; the cursor is
     gone).

  This mirrors the pre-existing pattern in `QueryReplyCallback`
  (lines 1559-1563 of `aggregate_exec.c`), so the user observes the
  same error string and stats-counter behavior they would have gotten
  from a non-RETURN_STRICT cursor read that hit the same spec drop.
  No skipping, no "empty + cid" envelope — the cursor cannot be retried
  because it no longer exists.

**Note on "drain" semantics:** drain is **not** a no-op in scenario 2.
The MRIterator/MRChannel persists across coord cursor reads, and the IO
thread asynchronously pushes shard replies (from earlier in-flight CURSOR
READ dispatches) into the channel independently of coord-side pipeline
execution. So at scenario-2 timer fire, the channel may already hold
buffered replies that drain will harvest as partial results. The drain
stops on the first empty-channel observation (non-blocking pop under
`timedOut=true`); replies that arrive later stay queued for the next
cursor read.

**BG bail-out (existing TIMEDOUT site, no new branch needed):**
The existing `sendChunk_Resp{2,3}` path at lines 702-715 / 909-922 already
calls `AREQ_StoreResults` + `AREQ_SignalAggregateResultsComplete` when
`useReplyCallback` is set. No new BG branch is added; we just rely on the
existing `(!TryClaim || TimedOut)` check at line 368-369 to route BG into
the `rc = RS_RESULT_TIMEDOUT` early-return, which then funnels through the
same store+signal site.

The drain we previously proposed in BG (drainPartialResultsAfterTimeout
inside startPipelineCommon's bail-out) is moved entirely to the timer
side — see §4. This keeps BG's bail-out symmetric with FT.AGGREGATE.

### 3.3 Scenario 3 — Timer waits; BG runs pipeline to TIMEDOUT

BG drives the pipeline. `rpnetNext` returns `RS_RESULT_TIMEDOUT` once it
observes the TimedOut atomic. Pipeline shapes accepted by
`pipelineCanYieldPartialResults` (post-PR #9366: bare `RPNet`,
`RPPager_Limiter -> RPNet`, and the SORTBY peel chain `[RPPager_Limiter ->]
RPSorter -> ... -> RPNet`) yield their buffered prefix on the main thread
via the timer's drain (§4); rejected shapes (e.g. GROUPBY, FILTER above
the sorter) discard their buffer on TIMEDOUT and the timer's drain pops
nothing. Either way, BG calls `AREQ_StoreResults` +
`AREQ_SignalAggregateResultsComplete` from the existing TIMEDOUT site
(same as today's FT.AGGREGATE RETURN_STRICT path).

For SORTBY-shaped pipelines, PR #9366 also latches `RPSorter::base.Next =
rpsortNext_Yield` on the TIMEDOUT path. This latch is **load-bearing** for
the cursor-read snapshot-pop semantics (the heap is frozen as a sorted
snapshot, drained one chunk at a time across subsequent cursor reads) and
must NOT be reset between reads — see §5.5.5. The only per-execution
state that does need resetting before the next read is enumerated in
§5.5.1.

Timer logic is **identical to scenario 2** (see §4). The timer never calls
`TryClaim` — both scenarios converge on `wait → drain → reply` and branch
on `hasStoredResults` only at the reply step.

### 3.4 BG early-exit invariant — `AREQ_ReplyOrStoreError` must signal

After `CoordRequestCtx_SetRequest` (timer can now observe `req != NULL`),
every BG exit path **must** signal `aggregateResultsDone` so the timer's
`AREQ_WaitForAggregateResultsComplete` returns. We enforce this by routing
all BG error paths through `AREQ_ReplyOrStoreError` and extending it to
signal when `RequiresThreadsSyncResults` is true:

```c
void AREQ_ReplyOrStoreError(AREQ *req, RedisModuleCtx *ctx, QueryError *status) {
  if (req->useReplyCallback) {
    QueryError_ClearError(&req->storedReplyState.err);
    QueryError_CloneFrom(status, &req->storedReplyState.err);
    QueryError_ClearError(status);
    // NEW: wake any RETURN_STRICT timer waiting on aggregateResultsDone.
    // hasStoredResults stays false; the timer distinguishes error-bail
    // from results via storedReplyState.err (mirrors QueryReplyCallback's
    // existing pattern at aggregate_exec.c:1559-1563, 1623-1626).
    if (AREQ_RequiresThreadsSyncResults(req)) {
      AREQ_SignalAggregateResultsComplete(req);
    }
  } else {
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, !IsInternal(req));
    QueryError_ReplyAndClear(ctx, status);
  }
}
```

**Audit of `AREQ_ReplyOrStoreError` call sites** (none changed semantically
for FAIL — the new signal call is a no-op when `RequiresThreadsSyncResults`
is false):

| Call site | Path | RequiresThreadsSyncResults | Effect of new signal |
|---|---|---|---|
| `aggregate_exec.c:1187` (`AREQ_Execute_Callback`, early err) | shard FAIL worker | false | no-op |
| `aggregate_exec.c:1248` (`AREQ_Execute_Callback`, `error:`) | shard FAIL worker | false | no-op |
| `aggregate_exec.c:1902` (`cursorRead` spec drop) | DIST_THPOOL coord | **true** for new path | wakes the timer ✓ |
| `dist_aggregate.c:512` (`DistAggregateCleanups`) | DIST_THPOOL coord FT.AGGREGATE (not the new cursor-read path) | true if RETURN_STRICT | This row applies to the **existing FT.AGGREGATE coord+RETURN_STRICT** path, which still uses the timer-side `TryClaim` short-circuit (Open Decision 6 caveat — the new cursor-read timer drops `TryClaim` but the FT.AGGREGATE timer is untouched per §6 "Not modified"). Under that timer's `TryClaim`, the wait is short-circuited before reaching the abort channel, so the extra signal here is harmless because no one is waiting on it. No behavior change for FT.AGGREGATE; documented for completeness. |

**Audit of BG exit paths in `cursorRead` after `SetRequest`:**

| Path | File:line | Goes through `AREQ_ReplyOrStoreError`? |
|---|---|---|
| Spec dropped (`!StrongRef_Get`) | `aggregate_exec.c:1902` | yes ✓ |
| Hybrid fallback (`req == NULL`) | `aggregate_exec.c:1943-1945` | no — but unreachable on coord path (`cursor->execState` is non-NULL); the assertion at line 1889 enforces this |
| Normal `runCursor`/`sendChunk` happy path | `aggregate_exec.c:1941` | n/a — reaches `sendChunk_Resp{2,3}` store+signal site directly |
| Normal `runCursor`/`sendChunk` TIMEDOUT path | same | n/a — reaches the same store+signal site (rc = TIMEDOUT) |

**Invariant going forward**: any new BG early-exit path between `SetRequest`
and the `sendChunk` store+signal site must either go through
`AREQ_ReplyOrStoreError` or call `AREQ_SignalAggregateResultsComplete`
explicitly. Document this at the relevant call sites.

**Cursor-clear obligation on the BG error sub-path.** The spec-drop bail at
`aggregate_exec.c:1896-1905` calls `AREQ_ReplyOrStoreError` and then
`Cursor_Free(cursor)`. With Option C (§3.5), the cursor was already parked on
`reqCtx->parkedCursor` inside the take-lock window, and `AREQ_ReplyOrStoreError`
now signals `aggregateResultsDone` (above). To prevent the timer from waking
on the signal and observing a stale `parkedCursor`, **the bail must order its
steps as follows**:

1. **Take + clear** `parkedCursor` under `setRequestLock` (use the returned
   handle for the free in step 3).
2. **Free the cursor** synchronously (`Cursor_Free(handle)`).
3. **Then** call `AREQ_ReplyOrStoreError` — which both stores the error on
   `storedReplyState.err` and signals `aggregateResultsDone`.

This ordering guarantees the §4 timer error-branch's invariant: by the time
the timer wakes on the signal, `parkedCursor` is already `NULL` *and* the
cursor is already freed, so the timer's `hasStoredResults == false` branch
can flush the error without touching the cursor at all. Other consumers
(`free_privdata`, late timer wake) likewise observe `parkedCursor == NULL`
and no-op.

---

## 3.5 Cursor-park ownership — per-read parking on `CoordRequestCtx`

**Problem.** The pre-existing FAIL path (and an earlier sketch of this design)
parks the cursor on `AREQ::storedReplyState.cursor` inside `runCursor`. Two
distinct issues for RETURN_STRICT:

- **Issue A — signal-before-park.** `runCursor` stashes the cursor *after*
  `sendChunk` already ran `AREQ_StoreResults` +
  `AREQ_SignalAggregateResultsComplete`. A timer waking on the signal could
  attempt to read the cursor handle before BG had stored it.
- **Issue B — shared field across reads.** `req->storedReplyState.cursor`
  lives on the AREQ, which spans the whole cursor session. A second read
  overwriting the field while the first read's reply path still references
  it would race.

**Fix.** Move the parked cursor off the AREQ onto the per-read
`CoordRequestCtx`:

```c
struct CoordRequestCtx {
  ...
  Cursor *parkedCursor;   // NEW; written/read under setRequestLock
};
```

Set inside `RSCursorReadCommand`'s take-lock critical section (§3.1),
*atomically* with `Cursors_TakeForExecution` and
`CoordRequestCtx_SetRequest`. Three consumers later retrieve-and-clear
`parkedCursor` under the same lock:

| Consumer | When it runs | Action on `parkedCursor` |
|---|---|---|
| Reply callback (success) | After BG signals at the normal completion site | Take + null under lock; `Cursor_Pause` if `!QEXEC_S_ITERDONE` else `Cursor_Free` |
| Timer callback (`hasStoredResults == true`) | After BG signals via TIMEDOUT bail | Same as reply: take + null under lock; pause/free per `QEXEC_S_ITERDONE` |
| Timer callback (`hasStoredResults == false`) | After BG signals via `AREQ_ReplyOrStoreError` | `parkedCursor == NULL` (BG already cleared + freed in its bail-out, see §3.4) — no-op |
| BG error sub-path (§3.4) | Spec drop, etc., before reaching pipeline | Take + null + `Cursor_Free` *before* signaling |
| `free_privdata` (disconnect) | After BG calls `RM_UnblockClient` and reply was skipped | Take + null under lock; `Cursor_Free` (see §3.6) |

Whichever consumer wins races on the lock; the others find
`parkedCursor == NULL` and no-op. This eliminates Issue A (parking happens
*before* BG can signal — BG's signal site is downstream of the take-lock)
and Issue B (the field is per-read; a second read constructs its own
`CoordRequestCtx`).

**`runCursor` interaction.** `runCursor` no longer needs to assign
`req->storedReplyState.cursor` for the RETURN_STRICT cursor-read path —
the cursor was parked on reqCtx before `runCursor` ran. The existing
assignment at `aggregate_exec.c:1839` is gated to skip the new path
(e.g. `useReplyCallback && !isCursorReadReturnStrict`), or made a no-op
when `reqCtx->parkedCursor` is already set. Either way, the FAIL path is
unaffected — it never touches `parkedCursor` (the field stays NULL on
non-RETURN_STRICT-cursor-read flows, and FAIL keeps using
`req->storedReplyState.cursor`).

---

## 3.6 Disconnect handling — free the cursor in `free_privdata`

**What runs on disconnect.** Verified against Redis source
(`src/blocked.c`, `src/module.c`):

| Event | Path |
|---|---|
| Client disconnects while BC is blocked and BG hasn't called `RM_UnblockClient` yet | `freeClient → unblockClient → unblockClientFromModule`. For `RM_BlockClient` (not on keys), this does **not** invoke `disconnect_callback` (we never register one) and does **not** queue the BC for `moduleHandleBlockedClients`. The BC stays alive with `bc->client = NULL`. |
| BG eventually completes and calls `RM_UnblockClient` | BC is queued; `moduleHandleBlockedClients` detects `c == NULL` → **skips reply_callback**, **runs `free_privdata`**, frees the BC. |
| Disconnect after BG already queued the BC | Same `moduleHandleBlockedClients` cycle: `c == NULL` ⇒ no reply, `free_privdata` runs. |
| Timeout fires before BG completes | `moduleBlockedClientTimedOut` invokes the timer callback (§4); when BG eventually calls `RM_UnblockClient`, BC is queued and `free_privdata` runs (reply already sent by the timer). |

**Implication.** `free_privdata` is the *only* callback guaranteed to run
on disconnect; reply_callback is skipped. We do **not** need
`RedisModule_SetDisconnectCallback` for cleanup — `free_privdata` plus
the take-under-lock contract from §3.5 is sufficient.

**Cursor disposition on disconnect — `Cursor_Free` (not `Pause`).** When
the client disconnects after BG has already consumed a batch from the
pipeline into `req->storedReplyState`:

1. BG pulled N rows from `MRChannel`/`RPNet` into the AREQ's stored state.
2. The reply was never delivered — `free_privdata` will free the AREQ and
   discard those N rows.
3. The MRIterator/pipeline state on the cursor has *advanced* past those
   N rows.
4. If we paused the cursor and the user retried with the same cid, the
   next `FT.CURSOR READ` would pull from row N+1, **silently skipping**
   the lost batch. The user has no signal that data was dropped.

Pausing optimizes for the (rare) "disconnect-then-reconnect-with-same-cid"
case at the cost of silent data loss in the common case where the user
assumes their cid still represents a contiguous result stream. **Free is
the correct call**: the user gets an explicit "Cursor not found" on retry
(matching the truth — the cursor session was abandoned), and there is no
risk of the user merging stale results into a complete-looking output.

**Concrete cleanup in `free_privdata`:**

```c
static void coordRequestCtxFreePrivData(RedisModuleCtx *ctx, void *privdata) {
  CoordRequestCtx *reqCtx = privdata;

  // Disconnect-cleanup: if a cursor is still parked, the reply/timer
  // path never ran for this request (or the disconnect outraced it).
  // Free the cursor — see §3.6 for the data-integrity rationale.
  CoordRequestCtx_LockSetRequest(reqCtx);
  Cursor *parked = CoordRequestCtx_TakeParkedCursor(reqCtx); // returns + NULLs
  CoordRequestCtx_UnlockSetRequest(reqCtx);
  if (parked) Cursor_Free(parked);

  // existing reqCtx teardown ...
}
```

**Timing.** Because BG always calls `RM_UnblockClient` after returning
from `runCursor` (via `concurrent_ctx.c:88` or the equivalent dispatch
wrapper), `free_privdata` runs strictly *after* BG is done with the
cursor — there is no "BG mid-execution" race. The take-under-lock
guarantees no consumer (timer / reply / BG error bail) double-frees.

---

## 4. Unified timer callback skeleton

```c
int DistCursorReadTimeoutReturnStrictClient(RedisModuleCtx *ctx,
                                            RedisModuleString **argv, int argc) {
  CoordRequestCtx *reqCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!reqCtx) return RedisModule_ReplyWithError(ctx, "Internal error: no ctx");
  RS_ASSERT(reqCtx->type == COMMAND_AGGREGATE);

  // Set TimedOut and read `req` under the same lock that gates BG's atomic
  // TakeForExecution+SetRequest critical section (§3.1). This is what makes
  // `req == NULL` a reliable proxy for "BG has not yet taken the cursor"
  // — i.e. what splits scenario 1 from 2/3.
  CoordRequestCtx_LockSetRequest(reqCtx);
  CoordRequestCtx_SetTimedOut(reqCtx);
  AREQ *req = (AREQ *)CoordRequestCtx_GetRequest(reqCtx);
  CoordRequestCtx_UnlockSetRequest(reqCtx);

  if (!req) {
    // Scenario 1: BG has NOT taken the cursor (take + SetRequest are atomic
    // under setRequestLock per §3.1, so req == NULL ⇔ no take). BG either
    // hasn't run yet, or ran and hit the early TimedOut check before
    // TakeForExecution and returned. No condvar signal will arrive — emit
    // cursor-shaped empty + cid directly. Cursor stays paused at its prior
    // position and is reusable on the user's next FT.CURSOR READ.
    // Cid is trusted: CursorCommand (main thread) already verified `info.found`
    // before arming the BC, so argv[3] is a syntactically and (at-arm-time)
    // semantically valid cursor id. RS_ASSERT the StringToLongLong succeeds.
    long long cid;
    RS_ASSERT(RedisModule_StringToLongLong(argv[3], &cid) == REDISMODULE_OK);
    return coord_cursor_read_reply_timeout_empty(ctx, cid);
  }

  // req != NULL ⇒ BG has taken the cursor and called SetRequest (atomically
  // under the same lock above). Scenarios 2 & 3 from here on; the timer does
  // not (and need not) distinguish them.

  // Scenarios 2 & 3 unified. Timer never calls TryClaim — BG's existing
  // (!TryClaim || TimedOut) check at startPipelineCommon (aggregate_exec.c:368)
  // bails on TimedOut alone. Wake the abort channel unconditionally: in
  // scenario 3 it unblocks BG from MRIterator_NextWithTimeout; in scenario 2
  // it is inert (no popper blocked on the channel).
  RequestSyncCtx_WakeAbortChannel(&req->syncCtx);

  AREQ_WaitForAggregateResultsComplete(req);

  if (req->storedReplyState.hasStoredResults) {
    // BG reached the pipeline and stored results (possibly empty). Harvest
    // any residual shard replies that landed after BG's signal, then reply
    // and dispose of the parked cursor (§3.5).
    drainPartialResultsAfterTimeout(req);
    AREQ_ReplyWithStoredResults(ctx, req);    // serializes results only
    coordReleaseParkedCursorAfterReply(reqCtx, req); // §3.5 — see below
  } else {
    // BG bailed pre-pipeline through AREQ_ReplyOrStoreError (e.g. spec
    // dropped at cursorRead:1902). The error is in storedReplyState.err
    // and the cursor was already taken+freed by the bail path under the
    // setRequestLock (§3.4 cursor-clear obligation), so parkedCursor is
    // already NULL here. Flush the error to the client (mirrors
    // QueryReplyCallback's pattern at lines 1559-1563).
    QueryError *err = &req->storedReplyState.err;
    RS_ASSERT(QueryError_HasError(err));
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(err), 1, !IsInternal(req));
    QueryError_ReplyAndClear(ctx, err);
  }
  return REDISMODULE_OK;
}
```

**Cursor lifecycle (under Option C — §3.5)**:
- Happy branch (`hasStoredResults == true`): the timer takes
  `parkedCursor` off `reqCtx` under `setRequestLock` and disposes of it
  per `QEXEC_S_ITERDONE` (`Cursor_Free` if iter-done, else
  `Cursor_Pause`) — the same logic that `AREQ_ReplyWithStoredResults`
  uses today, but reading from `reqCtx->parkedCursor` (set in the
  take-lock window) instead of `req->storedReplyState.cursor`. Encapsulate
  in a small helper, e.g.:

  ```c
  static void coordReleaseParkedCursorAfterReply(CoordRequestCtx *reqCtx,
                                                 AREQ *req) {
    CoordRequestCtx_LockSetRequest(reqCtx);
    Cursor *c = CoordRequestCtx_TakeParkedCursor(reqCtx);
    CoordRequestCtx_UnlockSetRequest(reqCtx);
    if (!c) return;                       // already disposed (e.g. by BG bail)
    if (req->stateflags & QEXEC_S_ITERDONE) Cursor_Free(c);
    else                                   Cursor_Pause(c);
  }
  ```

- Error branch (`hasStoredResults == false`): BG took + cleared
  `parkedCursor` under the lock and freed it before signaling
  (`cursorRead:1903`, §3.4). Timer just flushes the error string.
- Disconnect (no callback ran): `coordRequestCtxFreePrivData` takes
  `parkedCursor` under the lock and `Cursor_Free`s it (§3.6).

**`AREQ_ReplyWithStoredResults` extension.** Today it both serializes
results *and* pauses/frees the cursor from `req->storedReplyState.cursor`
(lines 1530-1537). For the new RETURN_STRICT path we want the
serialization but not the in-place pause/free — the cursor lives on
`reqCtx` instead. Two options: (a) gate the in-place pause/free on
`useReplyCallback && !isCursorReadReturnStrict` (cursor stays NULL on
the new path so the gate is paranoia); or (b) skip
`req->storedReplyState.cursor` entirely on the new path — `runCursor`
never sets it because §3.5 makes that assignment unnecessary. Either
way no FAIL behavior changes.

---

## 5. Concerns & open questions

### 5.1 `coord_aggregate_query_reply_empty` is not cursor-aware

The existing helper (`reply_empty.c`) calls `shallow_parse_query_args`, which
expects FT.AGGREGATE-shaped argv (`<index> <query> [args…]`). FT.CURSOR READ
argv is `FT.CURSOR READ <index> <cid> [COUNT n]` — no query string. Calling
the existing helper would error out.

**Required:** new helper `coord_cursor_read_reply_timeout_empty(ctx, cid)`
(or similar) that emits the cursor-envelope shape. **To decide:**

- **Outer wrapping**: `[<empty_results_payload>, <cid>]`? Or `[<empty>, 0]`
  (cid=0 = exhausted)? Per the pinned rule "RETURN_STRICT must preserve
  cursor IDs for partial results", emit the **original cid** so the client
  can retry. The cid=0 fast-bail is reserved for the explicit exhaustion case
  (not a timeout).
- **RESP2 vs RESP3 shape**: must match what `AREQ_ReplyWithStoredResults`
  would have emitted in the non-timeout case for an empty chunk. Cross-check
  against `serializeAndReplyResults_Resp{2,3}` and the cursor-read shape used
  by shard FT.CURSOR READ.
- **`FORMAT` parsing**: the current `coord_aggregate_query_reply_empty` parses
  `FORMAT` from argv. FT.CURSOR READ doesn't take `FORMAT` — formatting is
  inherited from the originating FT.AGGREGATE (cached on the AREQ). For
  scenario 1, we have no AREQ accessible from the timer (`req == NULL`). The
  empty reply must use the **default format** (RESP-version default), which
  is acceptable because there are no rows to format.
- **Timeout warning + stats**: the helper must still represent this as a
  timeout reply, not a silent empty chunk. For RESP3, include the normal
  `warnings` entry containing `QueryWarning_Strwarning(TIMED_OUT)`. For
  RESP2, preserve the existing cursor envelope shape (no inline warnings
  array), but still call
  `QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1,
  COORD_ERR_WARN)` so stats match `AREQ_ReplyWithStoredResults`.

### 5.2 Race window in scenario 1 is closed; but what about scenarios 2/3?

The "transient cursor-not-found" race (timer flushes reply → user retries →
BG hasn't paused yet) does **not** apply to scenarios 2 & 3, because under
Option C (§3.5) the cursor is parked on `reqCtx->parkedCursor` *before* BG
ever reaches `sendChunk`'s signal site:

- BG's signal site is downstream of the take-lock; the cursor was parked
  at take time (§3.1).
- The timer wakes on the signal, takes `parkedCursor` under
  `setRequestLock`, and disposes of it (pause / free per
  `QEXEC_S_ITERDONE`) **before** `RM_ReplyWith*` returns the cid to the
  client (the dispose is a synchronous step inside the timer callback,
  ordered after the reply is built but before the BC unblock that
  flushes the response — see §4 helper).
- A retry from the client therefore observes the cursor in its disposed
  state (paused if survivable, gone if iter-done), never in the
  intermediate "took but not paused" window that motivated §3.1.

Issues A and B from §3.5 are eliminated by construction: the parked field
is per-read (no cross-read sharing), and parking happens before any signal.

✅ No race in 2/3. Scenario 1 race is closed by §3.1 option (a).

### 5.3 `useReplyCallback` propagation for RETURN_STRICT

`runCursor` decides whether to stash the cursor and skip pause/free based on
`req->useReplyCallback` (line 1839). For the new RETURN_STRICT cursor-read
path to work, `useReplyCallback` **must** be true on the AREQ at the time
`runCursor` runs.

`CursorCommand` (module.c:3893-3914) currently only sets it on the FAIL
branch via `CoordRequestCtx_SetUseReplyCallback`. The new RETURN_STRICT
branch must do the same. Then `CoordRequestCtx_SetRequest` propagates the
flag to the AREQ.

### 5.4 `drainOnly` safety contract

`RPNet::drainOnly` is a **plain `bool`** (rpnet.h:76). Its safety relies on
the contract: *only set after BG has exited the pipeline; written and read
on the same thread for the drain itself; cleared before any later BG read*.
We respect this in both scenarios 2 and 3:

- Timer (main) sets `drainOnly` only after
  `AREQ_WaitForAggregateResultsComplete` returns, i.e. after BG has signaled
  and is no longer in the pipeline.
- Timer then reads it indirectly through `drainPartialResultsAfterTimeout` /
  `rpnetNext` on the same main-thread call stack.
- The next cursor read clears `drainOnly` inside the `setRequestLock`
  critical section before BG can re-enter the pipeline (§5.5.2).

If we ever want the timer to set `drainOnly` while BG is running, we must
upgrade the field to `atomic_bool` and add a release/acquire pair with
explicit happens-before. **Not needed for this design.**

### 5.5 Per-execution state that must be reset between cursor reads

Each new FT.CURSOR READ enters `cursorRead` and currently calls
`AREQ_ResetAggregateResultsClaim` (`aggregate_exec.c:1939`). The
`RS_ASSERT(req->reqConfig.timeoutPolicy != TimeoutPolicy_ReturnStrict)`
guard at line 1936 must be removed.

`AREQ_ResetAggregateResultsClaim` today only clears the claim handshake
(`syncCtx.aggregatingResults`, `syncCtx.aggregateResultsDone`). For the new
RETURN_STRICT cursor-read path, two more pieces of per-execution state
get latched on a timed-out read and would otherwise poison the next read
on the same cursor.

#### 5.5.1 Fields requiring reset

| Field | Owner | Why it latches | Failure mode if not reset |
|---|---|---|---|
| `syncCtx.aggregatingResults` | `RequestSyncCtx` | BG sets it via `TryClaim` at the start of each pipeline run; the timer's drop-from-timer design (§3.2) removes the timer-side `ResetAggregateResultsClaim` call, so BG is the only writer. | Next read's `TryClaim` returns `false` → BG bails immediately with `RS_RESULT_TIMEDOUT`. **Already reset today.** |
| `syncCtx.aggregateResultsDone` | `RequestSyncCtx` | Set by BG's `SignalAggregateResultsComplete` at the end of each pipeline run. | Next read's timer `Wait` returns immediately on the previous read's signal, then reads stale `storedReplyState`. **Already reset today.** |
| `syncCtx.timedOut` (atomic) | `RequestSyncCtx` | Set by the timer in §4. Read by BG via `MRIterator_NextWithTimeout` (`rpnet.c:295,347`) and `AREQ_TimedOut` (`rpnet.c:359`). | Next read's BG sees `timedOut == true` on entry → `getNextReply` returns `RS_RESULT_TIMEDOUT` before doing any work. **NEW**. |
| `RPNet::drainOnly` (plain bool) | root RP, located via `qctx->rootProc` | Set by the timer's `drainPartialResultsAfterTimeout` (§4) after BG has exited. Read by BG of the next read in `getNextReply` (`rpnet.c:356`): `if (nc->drainOnly) return RS_RESULT_EOF;`. | For non-sortby shapes (bare `RPNet`, pager+`RPNet`): next read's first empty-channel observation returns `EOF` → BG stores zero rows → reply emits `[empty, cid=0]` → **false cursor exhaustion**. Shard cursors stay alive; data silently abandoned. For shape (3) (sortby), the sorter is in Yield mode and never calls upstream, so `drainOnly` is never observed — reset is harmless but not required. **NEW** (gated to non-sortby shapes; safe to reset unconditionally). |

> **`RPSorter::base.Next` must NOT be reset.** Although PR #9366's
> `rpsortNext_innerLoop` (`result_processor.c:660-672`) unconditionally
> latches `rp->Next = rpsortNext_Yield` on `RS_RESULT_TIMEDOUT` under
> both `TimeoutPolicy_Return` and `TimeoutPolicy_ReturnStrict`, this
> latch is **load-bearing** for the `cursor + SORTBY + RETURN_STRICT`
> semantics, not poisonous state. See §5.5.5 for why.

> **`RPSorter::timedOut` and `RPMaxScoreNormalizer::timedOut`** are not in the
> reset surface: both are gated to `TimeoutPolicy_Return` (`result_processor.c:660,1490`)
> and never set under `ReturnStrict`. PR #9366 only widened the `Next`-latch
> branch, not the `timedOut` write. `RPDepleter::last_rc` is per-`Deplete`
> call and similarly unaffected.

> **`storedReplyState.{results, hasStoredResults, cursor, err}`** are
> self-managed: writers (`AREQ_StoreResults`, `AREQ_ReplyOrStoreError`) clear
> before write, and `AREQ_ReplyWithStoredResults` clears `results` /
> `hasStoredResults` / `cursor` after consuming them. No new reset needed.

#### 5.5.2 Placement — must be inside the `setReqLock` critical section

The current `AREQ_ResetAggregateResultsClaim` call at `cursorRead:1939` runs
on DIST_THPOOL **after** `RSCursorReadCommand` released `setRequestLock` and
**after** the new BC for this read has been armed on the main thread. That
position is race-prone for RETURN_STRICT:

```
T0  Main:  CursorCommand arms new BC → dispatches to DIST_THPOOL
T1  DIST:  RSCursorReadCommand → setReqLock → early TimedOut check (false:
           timer hasn't fired yet) → TakeForExecution → SetRequest → unlock
T2  Main:  BC timer fires → setReqLock → SetTimedOut(true) → unlock
T3  DIST:  cursorRead → AREQ_ResetAggregateResultsClaim clears
           syncCtx.timedOut (!!) → BG runs the pipeline as if no timeout
T4  DIST:  BG drains a full chunk normally, advances the cursor, calls
           AREQ_StoreResults + Signal
T5  Main:  timer's wait completes, sees hasStoredResults=true → emits a
           "partial timeout" reply with rows that should NOT have been
           consumed → next read won't see them → silent data loss
```

The reset must therefore move **into** the §3.1 `setReqLock` critical
section. It happens immediately after a successful `Cursors_TakeForExecution`
(the AREQ lives on `cursor->execState`, so we cannot reset before the take)
and before `CoordRequestCtx_SetRequest` publishes `req` to the timer:

```c
// In RSCursorReadCommand, RETURN_STRICT branch (mirrors the §3.1 snippet —
// shown here with the reset call called out for §5.5.x discussion):
CoordRequestCtx_LockSetRequest(reqCtx);
if (CoordRequestCtx_TimedOut(reqCtx)) {
  CoordRequestCtx_UnlockSetRequest(reqCtx);
  return REDISMODULE_OK;
}
// NEW placement: reset BEFORE the timer can possibly observe req != NULL.
// The timer takes the same lock to write syncCtx.timedOut, so this reset
// happens-before the timer's earliest possible write (mutex acquire/release
// pair carries the synchronization).
Cursor *cursor = Cursors_TakeForExecution(...);
if (!cursor) {
  CoordRequestCtx_UnlockSetRequest(reqCtx);
  // store/reply "Cursor not found"
  return REDISMODULE_OK;
}
AREQ_ResetAggregateResultsClaim(cursor->execState); // extended per 5.5.1
CoordRequestCtx_SetRequest(reqCtx, cursor->execState);
CoordRequestCtx_SetParkedCursor(reqCtx, cursor);    // §3.5
CoordRequestCtx_UnlockSetRequest(reqCtx);
```

The existing call at `cursorRead:1939` is then guarded so the FAIL /
non-coord paths keep the in-place reset (their cursor reads have no
concurrent timer that could clobber it):

```c
// cursorRead:1939
if (!req->useReplyCallback) {
  AREQ_ResetAggregateResultsClaim(req);
}
```

#### 5.5.3 Why each reset is race-free at the new placement

- **`syncCtx.aggregatingResults` / `aggregateResultsDone`**: BG owns the
  claim/done lifecycle within a single read; nobody else writes them.
- **`syncCtx.timedOut`**: only the timer ever writes `true`. The reset to
  `false` is a release-store; it happens under `setRequestLock` before
  `CoordRequestCtx_SetRequest` publishes `req`, so the reset is
  happens-before any timer write for *this* read.
- **`RPNet::drainOnly`**: only ever written by the timer's drain code,
  which runs *after* `WaitForAggregateResultsComplete` returns. BG of the
  next read cannot observe a concurrent write — the previous read's timer
  is fully retired by the time `RSCursorReadCommand` for the next read
  acquires `setRequestLock`.

#### 5.5.4 Where to put the new resets

Two viable factorings:

- **Inline in `AREQ_ResetAggregateResultsClaim`** — extend the existing
  helper to also walk to `qctx->rootProc` and clear `RPNet::drainOnly`.
- **Split** — keep `AREQ_ResetAggregateResultsClaim` for the `syncCtx`
  fields and add `pipelineResetForNextCursorRead(qctx)` that resets
  `RPNet::drainOnly` (and is the natural extension point if a future
  yielding-shape adds another resettable RP-chain field — though see
  §5.5.5 for why "yielding shapes that re-accumulate across reads" is
  load-bearing-incompatible with sort/group ordering and probably means
  no such field will ever exist for the sortby class).

**Recommendation**: inline for now. The original split rationale assumed
`RPSorter::base.Next` would also need resetting; once §5.5.5 establishes
that it must NOT be reset, only `drainOnly` remains on the RP-chain side
— a single-field walk that doesn't justify a separate helper.

#### 5.5.5 Why `RPSorter::base.Next` must NOT be reset (snapshot-pop semantics)

PR #9366's `rpsortNext_innerLoop` latches `rp->Next = rpsortNext_Yield`
on `RS_RESULT_TIMEDOUT`. Reverting it to `rpsortNext_Accum` for the next
cursor read would be a **correctness bug**, not a fix:

- Re-accumulating from upstream means pulling more shard rows that are
  about to land in the channel (e.g. a previously-paused shard's reply).
- Those rows can be globally smaller than rows already returned to the
  user in the previous chunk's drain — the sorted prefix the user
  observed defined a high-watermark that re-accumulated rows would
  silently violate.
- SORTBY's contract is global ordering across the whole cursor stream;
  there is no safe way to resume mid-stream without restarting the sort,
  and the underlying shard cursors have already advanced past the rows
  that fed the snapshot.

The correct semantics for `cursor + SORTBY + RETURN_STRICT` after a
timeout is **snapshot-pop**:

1. **Read 1, mid-accumulation timeout**: BG's `rpsortNext_innerLoop`
   sees `RS_RESULT_TIMEDOUT` from upstream, latches `Next = Yield`, and
   propagates `TIMEDOUT`. The heap is *frozen as a sorted snapshot* of
   whatever the sorter had merged so far.
2. **Main-thread drain**: pops one chunk's worth from the snapshot via
   `endProc->Next` (pager → sorter[Yield] → heap). Stops at the pager's
   per-chunk limit, **not** at heap-empty — leftovers stay in the heap.
3. **Read 2..N-1**: Sorter is *still* in Yield mode. The pager pulls
   another chunk from the same snapshot. Sorted order is preserved
   because the snapshot itself is sorted; each read just consumes the
   next contiguous prefix.
4. **Read N (snapshot exhausted)**: Yield returns `EOF`. `sendChunk`
   reports cursor-done → reply carries `cid=0`.

Late-arriving shard replies that land in the channel after step 1 are
silently discarded: merging them would violate the high-watermark from
step 2's drain. This is the price RETURN_STRICT pays in exchange for
"all the rows you got are correctly sorted relative to each other".
The `drainOnly` flag on `RPNet` is what enforces this — the IO thread
keeps pushing replies but the network layer gates them out, and on the
final cursor exhaustion the runtime sends `_FT.CURSOR DEL` to wind down
the shard cursors.

> **Why my earlier "false cursor exhaustion" framing for `RPSorter::Next`
> was wrong on two counts**: (a) the heap is usually *non-empty* after
> one chunk's drain (drain stops at the pager's per-chunk limit, not at
> heap-empty), so subsequent reads pop more rows from the snapshot, not
> EOF; (b) when the heap *is* empty, EOF is the *correct* answer (the
> snapshot is exhausted), not a false exhaustion.

> **Optional follow-up (out of scope)**: surface this snapshot-exhaustion
> case to the user explicitly by returning `cid=0` *in the very first
> timeout reply* on a SORTBY shape, instead of preserving the cursor
> across N drain reads. This would change the user-observable shape (one
> reply with all snapshot rows + `cid=0` + TIMEOUT warning, vs. the
> current N replies that gradually drain it) but is semantically
> equivalent and avoids the "why is my cursor still alive after a
> timeout" surprise. Decision deferred — file a follow-up issue if we
> want this.

### 5.5b Drain may dispatch fresh shard CURSOR READ commands

`MR_ManuallyTriggerNextIfNeeded` (rmr.c:911-921) is invoked unconditionally
inside `getNextReply` (rpnet.c:338) for cursor commands. During a
`drainOnly` cycle, if `it->ctx.pending != 0` and `channelSize <= threshold`,
this **schedules a new batch of shard CURSOR READs** via
`IORuntimeCtx_Schedule` whose replies arrive *after* we've already replied
to the user.

Effects:
- **Benign / pre-warming**: the new batch's replies sit in the channel
  ready for the next coord cursor read. Saves a round-trip.
- **Wasteful in the worst case**: if the user never reads again
  (deletes the cursor or disconnects), those replies are dispatched to
  shards and discarded when the cursor is freed.

**Open**: gate the manual-trigger on `!nc->drainOnly` to suppress fresh
dispatches during drain. One-line change at rpnet.c:338, but extends the
`drainOnly` contract beyond what FT.AGGREGATE needs and changes existing
behavior on the FT.AGGREGATE timer path. **Recommendation**: leave
unchanged in this PR; document the behavior. Revisit if benchmarks show
the wasted dispatches matter.

### 5.6 Lock-ordering invariant introduced by §3.1

New rule: hold `reqCtx`'s `setRequestLock` *before* any cursor-table
operation. Verify by inspection that no existing code path acquires the
cursor-table lock while holding any `CoordRequestCtx` lock.

### 5.7 Scenario 2 — wait worst-case after `SetRequest`

Timeline for scenario 2 (pipeline-bound sub-path):
- Timer: lock → SetTimedOut → unlock → wake abort →
  `WaitForAggregateResultsComplete`.
- BG (somewhere in `cursorRead`/`runCursor`/`sendChunk` setup): not yet at
  `startPipelineCommon`. It will reach the existing
  `(!TryClaim || TimedOut)` check at `aggregate_exec.c:368-369` eventually;
  the `TimedOut` branch fires and BG returns `RS_RESULT_TIMEDOUT` to
  `sendChunk_Resp{2,3}`, which calls `AREQ_StoreResults` +
  `AREQ_SignalAggregateResultsComplete`.

For the error-bound sub-path (spec drop, etc.), BG signals via
`AREQ_ReplyOrStoreError` (§3.4) before ever reaching the pipeline.

**Worst case**: BG is suspended (CPU contention) for a long time. The timer
holds the BC reply pending until BG signals. The user is blocked on the
network until then. This is the same constraint that already applies to
scenario 3 today and is bounded by `MRChannel`/spec-locking deadlock-freedom.
No new deadlock vector.

### 5.8 What if shards never reply?

`drainPartialResultsAfterTimeout` calls `endProc->Next` which calls
`rpnetNext`. With `drainOnly = true`, the channel pop is non-blocking (existing
code path at rpnet.c:340) — so the drain returns `EOF` promptly even if shards
are stuck. ✅

### 5.9 Profile / FT.CURSOR PROFILE

Out of scope for this design — `RSCursorProfileCommand` (line 2107) is a
distinct command and currently `RS_ASSERT`s on profile cursors not being
supported with `WITHCURSOR` user-facing. Confirm the assertion still holds
under RETURN_STRICT.

### 5.10 `_FT.HYBRID WITHCURSOR`

Read via `_FT.CURSOR READ`, **bypasses `CursorCommand`** (per cursor.h:200-201
comment). Does not go through this design. To decide separately whether
RETURN_STRICT applies to hybrid cursors at all.

---

## 6. Required new / modified symbols

| Symbol | Location | Action |
|--------|----------|--------|
| `CursorTimeoutInfo::found` | `cursor.h` | **new field** (`bool`); set by `Cursors_PeekTimeoutInfo` from the `kh_get` result. Existing callers ignore it; `CursorCommand` consumes it to validate the cid up-front. Update the doc comment on `queryTimeoutMS` (the "0 = not found OR TIMEOUT 0" overload is replaced by "use `info.found`"). |
| `CoordRequestCtx::isCursorReadReturnStrict` | `coord/dist_aggregate.h` | new field + setter/getter |
| `CoordRequestCtx::parkedCursor` | `coord/coord_request_ctx.{h,c}` | **new field** (`Cursor *`); written/read under `setRequestLock`. Per-read ownership of the cursor handle for the RETURN_STRICT cursor-read path (§3.5). NULL on FAIL and non-cursor-read flows. |
| `CoordRequestCtx_SetParkedCursor` / `CoordRequestCtx_TakeParkedCursor` | `coord/coord_request_ctx.{h,c}` | **new helpers**. `Set` writes the field (caller must hold `setRequestLock`). `Take` returns the current value and NULLs the field atomically with respect to the lock; idempotent — returns NULL after first call. Consumers: timer reply path (§4), reply callback (success), BG error sub-path (§3.4), `coordRequestCtxFreePrivData` (§3.6). |
| `coordRequestCtxFreePrivData` (cursor-cleanup extension) | `coord/coord_request_ctx.c` | extend the existing `free_privdata` callback to take + free `parkedCursor` under `setRequestLock` *before* the rest of the reqCtx teardown (§3.6). Handles the disconnect case where reply / timer never ran. **Free, not Pause** — see §3.6 rationale. |
| `coordReleaseParkedCursorAfterReply` | `coord/dist_aggregate.c` | **new helper** invoked from `DistCursorReadTimeoutReturnStrictClient` and the reply callback after `AREQ_ReplyWithStoredResults`. Takes `parkedCursor` under the lock and `Cursor_Free`s it on `QEXEC_S_ITERDONE` else `Cursor_Pause`s (§4 cursor-lifecycle block). |
| `DistCursorReadTimeoutReturnStrictClient` | `coord/dist_aggregate.c` | new function (sibling of `DistAggregateTimeoutReturnStrictClient`) |
| `DistCursorReadReplyCallback` | `coord/dist_aggregate.c` | likely a thin wrapper — or reuse `DistAggregateReplyCallback` if it Just Works (TBD: verify). Whichever is chosen, it **must** invoke `coordReleaseParkedCursorAfterReply` after `AREQ_ReplyWithStoredResults` for the new RETURN_STRICT cursor-read path (§3.5 / §4). |
| `coord_cursor_read_reply_timeout_empty(ctx, cid)` | `aggregate/reply_empty.{c,h}` | new helper |
| `CursorCommand` (cid validation + RETURN_STRICT branch) | `module.c:3893-3914` | (a) early bail using `info.found` *before* the policy branch (applies to both FAIL and RETURN_STRICT); (b) new RETURN_STRICT branch alongside the existing `TimeoutPolicy_Fail` branch |
| `RSCursorReadCommand` (early TimedOut check + take + reset + park) | `aggregate/aggregate_exec.c:2080-2099` | gated insertion before `TakeForExecution` (§3.1). Inside the same `setRequestLock` critical section, in order: (i) early TimedOut check; (ii) `Cursors_TakeForExecution`; (iii) `AREQ_ResetAggregateResultsClaim` (§5.5.2); (iv) `CoordRequestCtx_SetRequest`; (v) **`CoordRequestCtx_SetParkedCursor` (§3.5)**. This is the only race-free placement for both `parkedCursor` set and `syncCtx.timedOut` reset on the RETURN_STRICT path. (Functionally, all five steps run under the same lock, so any total order in which the reset and the two `Set*` calls precede the unlock is race-free; the order above is canonical for the doc and matches the §3.1 / §5.5.2 snippets.) |
| `runCursor` (cursor-stash gating) | `aggregate/aggregate_exec.c:1839` | for the RETURN_STRICT cursor-read path the cursor is already parked on `reqCtx` (§3.5), so the existing `req->storedReplyState.cursor = cursor` assignment is unnecessary. Either gate it (`useReplyCallback && !isCursorReadReturnStrict`) or leave it and have the reply path ignore the AREQ field on the new path. FAIL behavior is unchanged. |
| `cursorRead` BG error sub-path (`aggregate_exec.c:1896-1905`) | `aggregate/aggregate_exec.c` | before calling `Cursor_Free(cursor)`, take + clear `parkedCursor` under `setRequestLock` so later consumers (timer / `free_privdata`) observe NULL and do not double-free (§3.4 cursor-clear obligation). |
| `AREQ_ResetAggregateResultsClaim` | `aggregate/aggregate_request.c:1137` | (a) reset `syncCtx.timedOut` to `false` (release-store); (b) reset `RPNet::drainOnly` to `false` on the root RP (`qctx->rootProc`); (c) remove the `RS_ASSERT(timeoutPolicy != ReturnStrict)` guard at `aggregate_exec.c:1936`; (d) at `cursorRead:1939`, gate the existing call with `if (!req->useReplyCallback)` so only the FAIL / non-coord cursor paths keep the original placement — the RETURN_STRICT path now resets earlier, inside `RSCursorReadCommand`'s `setRequestLock` critical section (§5.5.2). **Does NOT touch `RPSorter::base.Next`**: the Yield latch is load-bearing for snapshot-pop semantics, see §5.5.5. |
| `AREQ_ReplyOrStoreError` | `aggregate/aggregate_exec.c:1150` | extend the `useReplyCallback` branch to call `AREQ_SignalAggregateResultsComplete` when `RequiresThreadsSyncResults` — closes the BG-bail hang for the new RETURN_STRICT cursor-read timer (§3.4). Pre-existing FAIL callers are unaffected (no-op when `RequiresThreadsSyncResults` is false). |

**Not modified** — relying on existing behavior:
- `startPipelineCommon`: the existing `(!TryClaim || TimedOut)` check at
  `aggregate_exec.c:368-369` already routes BG into `rc = RS_RESULT_TIMEDOUT`
  when the timer has set TimedOut. No new bail-out branch is needed; the
  existing `sendChunk_Resp{2,3}` store+signal site at lines 702-715 / 909-922
  handles it.

**Untouched (per "don't touch unless necessary")**:
- `DistAggregateTimeoutFailClient`, `DistAggregateTimeoutReturnStrictClient`
- `CursorReadTimeoutFailCallback`, `CursorReadReplyCallback`
- The `if (reqCtx)` sub-case (1) FAIL handling at `RSCursorReadCommand:2080-2090`
  (the `Cursor_Free` stays for FAIL).

---

## 7. Test plan additions

In addition to existing FT.CURSOR READ + FAIL coverage:

1. **Happy-path RETURN_STRICT cursor read** (no timeout): N reads with full
   chunks, final read returns `cid=0` (exhausted).
2. **Scenario 1 race reproduction**: inject a delay between `TakeForExecution`
   and SetRequest with the early-check **disabled**, verify "Cursor not
   found" on a tight retry; then enable the early-check, verify retry
   succeeds.
3. **Scenario 2 pipeline-bound — timer fires before BG's `TryClaim`**:
   inject a delay before BG's `TryClaim` so the timer fires first. Verify:
   - reply is partial-results-shaped (not error)
   - cid in the reply equals the original cid (cursor preserved)
   - next FT.CURSOR READ on the same cid succeeds and returns more rows
4. **Scenario 3 — timer fires while BG is in the pipeline**: BG already
   in pipeline (past `TryClaim`) when timer fires. Verify same observable
   as scenario 2 (partial + preserved cid).
5. **Scenario 2 error-bound — BG bails via `AREQ_ReplyOrStoreError`**:
   force a spec drop between `SetRequest` and `startPipeline` (e.g. drop
   the index from another connection while BG is queued in DIST_THPOOL),
   fire the timer in parallel. Verify:
   - timer's wait completes (no hang)
   - timer flushes the "Index was dropped" error to the client (not an
     empty cursor envelope) — exercises the `!hasStoredResults` branch
     of the timer (§4)
   - cursor was freed by BG (subsequent `FT.CURSOR DEL <cid>` returns
     "Cursor not found")
6. **Stale `drainOnly` poison (non-sortby shape)**: trigger a timed-out
   cursor read on a bare `FT.AGGREGATE * WITHCURSOR` (or `LIMIT`-only)
   shape, then issue a follow-up read. Verify the follow-up does **not**
   return `EOF` immediately (i.e. `RPNet::drainOnly` was reset per §5.5.1).
6b. **SORTBY snapshot-pop across cursor reads (post-PR #9366)**: arrange
   for `pipelineCanYieldPartialResults` to accept the shape (e.g.
   `FT.AGGREGATE idx * SORTBY 1 @name LIMIT 0 N WITHCURSOR` with
   per-chunk `COUNT` smaller than the snapshot size) and trigger a
   timeout mid-accumulation on read 1 such that the heap snapshot has
   strictly more rows than one chunk's drain. Verify (per §5.5.5):
   - read 1 returns the first chunk in sorted order with the cursor
     preserved (cid != 0) and a TIMEOUT warning;
   - reads 2..N-1 return successive chunks, each in sorted order, with
     each chunk's first row ≥ the previous chunk's last row (snapshot
     monotonicity);
   - read N returns `cid=0` once the snapshot is exhausted, with no
     TIMEOUT warning re-raised on the exhaustion read;
   - **negative**: late-arriving rows from the previously-paused shard
     never appear in any reply (silent discard is the documented
     trade-off — confirm by injecting a delayed shard reply that would
     globally sort *before* the last row of read 1's chunk and
     asserting it does not appear in any subsequent read).
7. **Cursor-shaped empty reply**: scenario 1 with no buffered rows. Verify
   the reply matches RESP2 / RESP3 expected shapes.
8. **MAXIDLE preservation across timeout**: cursor's MAXIDLE is honored after
   a timed-out read pauses the cursor.
9. **Disconnect during BG run — `free_privdata` reaps the cursor (§3.6)**:
   issue an `FT.CURSOR READ` (RETURN_STRICT) with a long shard delay so BG
   is in the pipeline; close the client connection before BG signals.
   Verify:
   - server does not crash / leak (run under valgrind/ASan);
   - `FT.CURSOR DEL <cid>` from a fresh connection returns
     `"Cursor not found"` (cursor was freed by `coordRequestCtxFreePrivData`,
     not paused);
   - a tight retry of `FT.CURSOR READ <idx> <cid>` from a fresh connection
     also returns `"Cursor not found"` (no silent skip past lost rows).
10. **Disconnect after BG signals but before reply flush**: arrange for BG
    to complete and signal `aggregateResultsDone`, then close the client
    before the timer / reply callback flushes. Verify the cursor is
    freed (consumer race: whichever of reply path or `free_privdata`
    wins the lock disposes; the other no-ops on `parkedCursor == NULL`).

---

## 8. Open decisions to confirm before implementation

1. **Cid-in-empty-reply** — **Decided: original cid (per §5.1).**
   Rationale: RETURN_STRICT must preserve cursor IDs for partial results so
   the client can retry. The cid=0 fast-bail is reserved for the explicit
   exhaustion case (`QEXEC_S_ITERDONE`), not a timeout. See §5.1 third
   bullet for the full RESP2/RESP3 shape.
2. **Reuse vs fork the reply callback**: `DistAggregateReplyCallback` may
   already handle the cursor case correctly via `AREQ_ReplyWithStoredResults`
   (cursor disposition built in). Verify or fork. Either way it must be
   updated to invoke `coordReleaseParkedCursorAfterReply` (§3.5 / §6) so the
   parked cursor is disposed on the success path; the existing in-place
   pause/free inside `AREQ_ReplyWithStoredResults` runs against
   `req->storedReplyState.cursor`, which is NULL on the new path.
3. **`AREQ_ResetAggregateResultsClaim` factoring** — **Decided: inline.**
   Reset surface is `syncCtx.aggregatingResults`, `aggregateResultsDone`,
   `syncCtx.timedOut`, and `RPNet::drainOnly` (single field on the root
   RP, located via `qctx->rootProc`). `RPSorter::base.Next` is **not**
   in the reset surface — see §5.5.5 for why the Yield latch is
   load-bearing rather than poisonous. With only one RP-chain field
   left, a separate `pipelineResetForNextCursorRead` helper isn't
   justified; extend `AREQ_ResetAggregateResultsClaim` in place. Called
   from the `setRequestLock` critical section in `RSCursorReadCommand`
   (§5.5.2).
4. **Apply scenario 1's BG-side early-check to FAIL too?** **Decided: no.**
   Rationale: FAIL semantics make the cursor unusable post-timeout. The existing
   "take then `Cursor_Free`" path correctly tears the cursor down. The residual
   T2–T4 race only changes the error string a tight retry sees ("Cursor not
   found" instead of "Timeout") — and that string is *semantically correct*,
   because the cursor is in fact gone (or about to be, at T4). Combined with
   the new main-thread cid validation (§1 step 1), all subsequent retries on
   that cid receive the same "Cursor not found" message inline. No FAIL-side
   code (`DistAggregateTimeoutFailClient`, `CursorReadTimeoutFailCallback`,
   the `Cursor_Free` at `aggregate_exec.c:2087`) is touched.
5. ~~`pipelineResetForNextCursorRead` placement~~ — withdrawn; helper
   not needed under the inline factoring (open decision 3).
6. **TryClaim's role on the timer side** — **Decided: drop from timer.**
   Rationale: the timer no longer needs to short-circuit the wait based on
   BG's pipeline progress. BG's existing `(!TryClaim || TimedOut)` check at
   `startPipelineCommon` (line 368-369) already routes pipeline-side bails
   to the store+signal site, and BG's pre-pipeline early exits (e.g. spec
   drop in `cursorRead`) are now signaled via the extended
   `AREQ_ReplyOrStoreError` (§3.4). The timer waits unconditionally and
   branches at the reply step on `hasStoredResults`. Asymmetry with
   FT.AGGREGATE coord+RETURN_STRICT is acceptable — that path predates this
   design and uses TryClaim as its sole bail-coordination mechanism; it
   could be migrated to the same pattern in a follow-up but is out of scope
   here.

   **Invariant introduced**: any future BG early-exit between
   `CoordRequestCtx_SetRequest` and `sendChunk`'s store+signal site must
   either (a) go through `AREQ_ReplyOrStoreError`, or (b) call
   `AREQ_SignalAggregateResultsComplete` directly. See §3.4 audit table.
7. **Cursor-park ownership** — **Decided: per-read field on `CoordRequestCtx`
   (Option C, §3.5).** The parked cursor moves off `AREQ::storedReplyState.cursor`
   onto a new `CoordRequestCtx::parkedCursor`, written under the take-lock
   alongside `Cursors_TakeForExecution` and `CoordRequestCtx_SetRequest` (§3.1).
   This eliminates both the signal-before-park race (Issue A) and the
   shared-field-across-reads race (Issue B). All four consumers (timer, reply,
   BG error sub-path, `free_privdata`) take + clear under the same lock and
   no-op on NULL. The `AREQ` parked field stays in place for FAIL.
8. **Disconnect handling** — **Decided: `free_privdata` reaps the cursor with
   `Cursor_Free` (§3.6).** No `RedisModule_SetDisconnectCallback` is registered
   — verified against Redis source that `free_privdata` always runs on
   disconnect after BG calls `RM_UnblockClient`, while `reply_callback` is
   skipped when `bc->client == NULL`. Pause is rejected because BG-side rows
   already drained from the pipeline are lost when the AREQ is freed; pausing
   would let a retry silently skip those rows. Free is loud and consistent.
