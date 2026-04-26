# Plan — shard-side `FT.CURSOR READ` FAIL timeout via blocked-client API

Follow-up to `FT_CURSOR_READ_BLOCKED_CLIENT_TIMEOUT_PLAN.md`. That plan wired
the blocked-client timer on the **coordinator** path (`NumShards > 1`,
`module.c::CursorCommand`). This plan does the symmetric change on the
**shard** path, for both:

- Standalone (`NumShards == 1`) `FT.CURSOR READ`: `CursorCommand` shortcut
  goes straight into `RSCursorReadCommand`.
- Internal `_FT.CURSOR READ` arriving on a shard from the coordinator
  (cursor depletion): also enters `RSCursorReadCommand` with no upstream
  blocked client.

Both paths are wired here only for the worker-dispatch branch
(`RunInThread() && !upstreamBC`, `aggregate_exec.c:1953`). The synchronous
`!RunInThread()` branch is untouched: it has no blocked client to attach a
timer to, and the existing in-pipeline clock (`sctx->time.timeout`, not
skipped when `RunInThread()` was false at cursor-creation time \u2014 see
`aggregate_request.c:1149`) already covers FAIL on that branch.

FAIL policy only. RETURN / RETURN-STRICT, hybrid cursors (`cursor->hybrid_ref`),
and the coord-level `FT.CURSOR READ` path are unchanged.

## Goal

Replace the in-pipeline `SearchCtx_UpdateTime` clock (the only timeout
enforcement on the shard cursor-read worker path today) with Redis' own
blocked-client timeout timer, reusing the exact mechanism already wired for
shard-side `FT.SEARCH` / `FT.AGGREGATE` under FAIL (`buildPipelineAndExecute`
→ `BlockQueryClientWithTimeout` with `QueryTimeoutFailCallback` and
`QueryReplyCallback`). The reply-side machinery (`storedReplyState`,
`AREQ_ReplyWithStoredResults`, `ChunkReplyState_Destroy`) is already in place
from the coord plan and is policy-agnostic.

## Already verified (relied on, no changes needed)

- `Cursor::queryTimeoutMS` and `Cursor::queryTimeoutPolicy` are already
  populated in `AREQ_StartCursor` (`aggregate_exec.c:1695-1696`) as a
  sticky snapshot of the originating AREQ's `reqConfig`. The same snapshot
  used by the coord plan is directly reusable on the shard — no new cache
  needed.
- `QueryTimeoutFailCallback` (`aggregate_exec.c:1420`) and
  `QueryReplyCallback` (`aggregate_exec.c:1501`) are not AREQ-dispatch
  specific: they read `node->privdata` as `AREQ *` and use
  `req->useReplyCallback` / `req->storedReplyState` / `AREQ_SetTimedOut`.
  They are directly reusable for the cursor path.
- `runCursor` already branches on `req->useReplyCallback`
  (`aggregate_exec.c:1709, 1734-1739`): with the flag set it skips the
  in-pipeline clock re-arm (`SearchCtx_UpdateTime`) and parks the cursor
  handle in `storedReplyState.cursor` instead of calling `Cursor_Pause` /
  `Cursor_Free` inline. Introduced by the coord plan and applies on the
  shard the moment we set the flag.
- `AREQ_ReplyWithStoredResults` (`aggregate_exec.c:1444-1494`) runs
  `finishSendChunk` on the main thread, flips `QEXEC_S_ITERDONE`, and
  then does the final `Cursor_Pause` / `Cursor_Free`
  (`aggregate_exec.c:1486-1493`). No coord-specific state — it only needs
  `req->storedReplyState`.
- `ChunkReplyState_Destroy` (`aggregate_request.c:1545-1568`) already
  handles the timeout edge case "cursor parked in `storedReplyState.cursor`
  but `QueryReplyCallback` never ran": it clears `execState` to break the
  refcount cycle and frees the cursor. This runs from `AREQ_Free` once the
  final `AREQ_DecrRef` fires.
- The "index dropped while idle" error site inside `cursorRead`
  (`aggregate_exec.c:1786-1802`) already routes through
  `AREQ_ReplyOrStoreError(req_local, ctx, &err)` — the only post-
  `BlockCursorClient` error path on the shard. With §3 setting
  `useReplyCallback = true`, that helper stores into
  `req->storedReplyState.err` for `QueryReplyCallback` to emit. No change
  needed at the call site.
- `skipTimeoutChecks` (`req->sctx->time.skipTimeoutChecks`) is already set
  to `true` at cursor-creation time for FAIL + workers (via
  `shouldCheckInPipelineTimeout == false` → `AREQ_SetSkipTimeoutChecks(r, true)`
  in `parseAggPlan`), so `startPipelineCommon`'s clock check is already a
  no-op on this path. The AREQ-level `AREQ_TimedOut` flag set by
  `QueryTimeoutFailCallback` is the stop signal the pipeline already
  honors in the same way it does for shard `FT.SEARCH` / `FT.AGGREGATE`
  FAIL today.

## Non-goals (same as coord plan)

- Standalone or shard `FT.CURSOR READ` under RETURN / RETURN-STRICT.
- Hybrid cursors (`cursor->hybrid_ref != NULL`, `cursor->execState == NULL`).
- Coordinator `FT.CURSOR READ` entry point (covered by the coord plan).
- `FT.CURSOR DEL` / `FT.CURSOR GC` / `FT.CURSOR PROFILE`.
- Any change to `QueryTimeoutFailCallback` / `QueryReplyCallback` themselves —
  they are reused verbatim.

## Plan

### 1. Give `BlockedCursorNode` a privdata slot

`QueryTimeoutFailCallback` and `QueryReplyCallback` both do
`node = RedisModule_GetBlockedClientPrivateData(ctx);` and then
`req = node->privdata;`. `BlockedQueryNode` has `privdata` + `freePrivData`
(`blocked_queries.h:37-38`); `BlockedCursorNode` does not. Without those
two fields, the callbacks cannot reach the cursor's `AREQ`. Symmetry with
`BlockedQueryNode` is the right shape: cursor reads behave the same as
queries from the blocked-client point of view, they just refer to a
live cursor.

- [ ] `info/info_redis/types/blocked_queries.h`: add two fields to
      `BlockedCursorNode`:
      `void *privdata;` and
      `BlockedQueryNode_FreePrivData freePrivData;`
      (reuse the existing `BlockedQueryNode_FreePrivData` typedef).
- [ ] `info/info_redis/types/blocked_queries.c::BlockedQueries_AddCursor`:
      extend the signature with `void *privdata,
      BlockedQueryNode_FreePrivData freePrivData` and write both into the
      new node after `rm_calloc`. All existing callers (block_client.c
      only, via `BlockCursorClient`) must pass these through.
- [ ] `info/info_redis/types/blocked_queries.h`: update the
      `BlockedQueries_AddCursor` prototype.

### 2. Change `BlockCursorClient` to take a full `BlockClientCtx`

Today `BlockCursorClient(ctx, cursor, count, int timeoutMS)` hardcodes
`RedisModule_BlockClient(ctx, NULL, NULL, FreeCursorNode, 0)` —
**the `timeoutMS` parameter is dead** (see `block_client.c:67`, the `0`
literal). Replace it with the same `BlockClientCtx *` shape
`BlockQueryClientWithTimeout` already uses. Having a single struct type
on both cursor and non-cursor block sites makes this (and the existing
coord plan) uniform.

- [ ] `info/info_redis/block_client.h`: change the prototype to
      `RedisModuleBlockedClient* BlockCursorClient(RedisModuleCtx *ctx,
      Cursor *cursor, size_t count, BlockClientCtx *blockClientCtx);`.
      `blockClientCtx` may be `NULL` (the pre-FAIL and RETURN-policy
      callers), matching the defensive-null-check pattern in the older
      variant of this function.
- [ ] `info/info_redis/block_client.c::BlockCursorClient`:
  - Assert the same invariant as `BlockQueryClientWithTimeout`:
    `blockClientCtx == NULL || blockClientCtx->timeoutMS == 0 ||
    (blockClientCtx->timeoutCallback != NULL &&
     blockClientCtx->replyCallback != NULL)`.
  - Pass the four callback/timeout fields to `BlockedQueries_AddCursor`
    (privdata + freePrivData) and to `RedisModule_BlockClient`
    (replyCallback + timeoutCallback + FreeCursorNode + timeoutMS).
  - When `blockClientCtx` is `NULL`, continue the current "no-timeout,
    no reply callback" behavior (passes NULL/0 to both).
- [ ] `info/info_redis/block_client.c::FreeCursorNode`: call
      `cursorNode->freePrivData(cursorNode->privdata)` if both are set,
      **before** `BlockedQueries_RemoveCursor(cursorNode)`. This mirrors
      `FreeQueryNode` exactly and releases the AREQ extra ref taken in
      §3.

### 3. Wire the timer in `RSCursorReadCommand` (shard worker-pool branch)

In `aggregate_exec.c::RSCursorReadCommand`, the block entered when
`RunInThread(ctx) && !upstreamBC` (the worker-pool dispatch,
`aggregate_exec.c:1934-1946`). Note the existing code already does
`cursor->execState->useReplyCallback = false` defensively at line 1940 to
clear stale state — the FAIL branch below replaces that assignment with
`= true`.

- [ ] After `Cursors_TakeForExecution` has succeeded and `cursor->execState`
      is non-NULL (non-hybrid), read the sticky snapshot directly off the
      cursor — no `Cursors_PeekTimeoutInfo` needed since we already own the
      cursor exclusively:
      ```c
      RSTimeoutPolicy policy        = cursor->queryTimeoutPolicy;
      rs_wall_clock_ms_t queryToMS  = cursor->queryTimeoutMS;
      ```
      Hybrid cursors (`cursor->execState == NULL`) skip the FAIL branch and
      fall through to the existing no-timeout wiring (`blockClientCtx = NULL`).
- [ ] Prepare the `BlockClientCtx` only for FAIL:
      ```c
      BlockClientCtx blockClientCtx = {0};
      BlockClientCtx *bcCtxPtr = NULL;
      if (cursor->execState && policy == TimeoutPolicy_Fail) {
          // Extra ref owned by the BlockedCursorNode (released in
          // FreeCursorNode → blockClientCtx.freePrivData; see §4 wrapper).
          AREQ_IncrRef(cursor->execState);
          blockClientCtx.privdata         = cursor->execState;
          blockClientCtx.freePrivData     = ShardCursorBlockClient_FreeAREQ;
          blockClientCtx.replyCallback    = QueryReplyCallback;
          blockClientCtx.timeoutCallback  = QueryTimeoutFailCallback;
          blockClientCtx.timeoutMS        = queryToMS;
          blockClientCtx.ast              = &cursor->execState->ast;
          cursor->execState->useReplyCallback = true;
          bcCtxPtr = &blockClientCtx;
      } else if (cursor->execState) {
          // RETURN / hybrid / policy-not-FAIL: preserve current behavior
          // (replaces the existing line 1940 `useReplyCallback = false`).
          cursor->execState->useReplyCallback = false;
      }
      cr_ctx->bc = BlockCursorClient(ctx, cursor, count, bcCtxPtr);
      ```
      `BlockClientCtx::timeoutMS` is `rs_wall_clock_ms_t`
      (`block_client.h:33`), matching `Cursor::queryTimeoutMS`'s storage
      after the coord plan. The `ShardCursorBlockClient_FreeAREQ` helper is
      defined in §4; we don't reuse the existing
      `AREQ_DecrRefWrapper` because the cursor path needs to drain a
      possible parked `storedReplyState.cursor` before decrementing, which
      the AREQ-only wrapper does not do.
- [ ] No `cr_ctx` schema change. The existing `CursorReadCtx` struct
      (`aggregate_exec.c:1840-1844`: `bc`, `cursor`, `count`) is already
      sufficient — the `useReplyCallback` flag is carried on the AREQ
      itself, and `runCursor` reads it back off `req` (`:1709, :1725,
      :1734`).

### 4. Plug the "timeout fired, cursor parked in `storedReplyState`" leak

When the blocked-client timeout fires on the shard, `QueryTimeoutFailCallback`
replies and sets `AREQ_TimedOut`. The worker still runs `runCursor` to
completion under `useReplyCallback = true`, parks the cursor in
`req->storedReplyState.cursor`, and returns. `cursorRead_ctx` calls
`UnblockClient`, but because the client was already unblocked by the
timeout, the reply_callback does not fire — so nobody calls
`Cursor_Pause` / `Cursor_Free` on the parked cursor.

On the coord path this is fixed in `CoordRequestCtx_Free`
(`coord_request_ctx.c:41-45`): before `AREQ_DecrRef`, it explicitly frees
a leftover `storedReplyState.cursor`. The shard path needs the same
cleanup, but its "freePrivData" is owned by the `BlockedCursorNode` and
sees the raw AREQ pointer, not a CoordRequestCtx.

Per the §"Decisions" Q2 resolution, extract the cursor-drain step into a
shared `AREQ_DrainStoredCursor` helper so the coord and shard sites share
one implementation. The helper is AREQ-level, so it lives next to the
other AREQ ref helpers in `aggregate_request.c` — neither shard-only nor
coord-only. The shard's `freePrivData` is then a thin wrapper that calls
the helper and decrements the ref.

- [ ] `aggregate_request.c` (next to `AREQ_DecrRef`): add
      ```c
      // If the worker parked a cursor in req->storedReplyState (the
      // useReplyCallback path) and nothing drained it (e.g. because the
      // blocked-client timeout fired first), free it. Safe to call
      // unconditionally; a no-op when storedReplyState.cursor is NULL.
      void AREQ_DrainStoredCursor(AREQ *req) {
          if (req->storedReplyState.cursor) {
              Cursor *cursor = req->storedReplyState.cursor;
              req->storedReplyState.cursor = NULL;
              Cursor_Free(cursor);
          }
      }
      ```
      Add the prototype to `aggregate.h` near the other `AREQ_*` helpers.
- [ ] `coord/coord_request_ctx.c::CoordRequestCtx_Free`: replace the
      inlined `if (ctx->areq->storedReplyState.cursor) { ... }` block at
      lines 41-45 with a single `AREQ_DrainStoredCursor(ctx->areq);` call
      before `AREQ_DecrRef(ctx->areq)`. Behavior identical; the comment
      block above stays.
- [ ] `aggregate_exec.c` (next to `AREQ_DecrRefWrapper`): add the shard
      `freePrivData` wrapper:
      ```c
      // freePrivData for BlockCursorClient on the shard FAIL path.
      // Drains a leftover storedReplyState.cursor (set by runCursor when
      // the timeout fires before QueryReplyCallback can run) before
      // releasing our AREQ ref. The drain helper is shared with the coord
      // path (see AREQ_DrainStoredCursor in aggregate_request.c).
      static void ShardCursorBlockClient_FreeAREQ(void *privdata) {
          AREQ *req = (AREQ *)privdata;
          AREQ_DrainStoredCursor(req);
          AREQ_DecrRef(req);
      }
      ```
      Use it in §3 as `blockClientCtx.freePrivData` instead of
      `AREQ_DecrRefWrapper`. Plain `AREQ_DecrRefWrapper` stays in use on
      the FT.SEARCH / FT.AGGREGATE non-cursor path.
- [ ] Note: this wrapper runs **after** `QueryReplyCallback` on the happy
      path, which already NULLs `stored->cursor` after handling it
      (`aggregate_exec.c:1492`). So `AREQ_DrainStoredCursor` is a no-op on
      the happy path and only acts when `QueryReplyCallback` was skipped
      because the timeout unblocked the client first.

### 5. Don't re-arm the in-pipeline clock for the FAIL path

Nothing to do here — `runCursor`'s `!req->useReplyCallback` guard around
`SearchCtx_UpdateTime` (`aggregate_exec.c:1709-1711`), introduced by the
coord plan, already suppresses the clock re-arm the moment §3 sets the
flag. Leaving this explicit as a reminder to keep the guard intact if the
coord plan is ever reverted.

### 6. Error-reply routing on the cursor path

**The existing early-return paths in `RSCursorReadCommand` all run before
`BlockCursorClient` is called** (`aggregate_exec.c:1873-1932`): `argc < 4`,
bad CID, unknown arg, bad COUNT, `Cursors_TakeForExecution` returning
NULL. They reply via `RedisModule_ReplyWith*` (no upstreamBC) or
`CoordRequestCtx_ReplyOrStoreError` (when an upstream coord+FAIL bc is
present). Neither path interacts with the shard FAIL bc this plan adds,
so no change is needed for them.

The only error that can fire **after** `BlockCursorClient` on this path
is the "index was dropped while the cursor was idle" branch inside
`cursorRead` (`aggregate_exec.c:1786-1802`), and it already routes
through `AREQ_ReplyOrStoreError(req_local, ctx, &err)` before
`Cursor_Free(cursor)`. `AREQ_ReplyOrStoreError` reads
`req->useReplyCallback` (`aggregate_exec.c:1108`) and, with the flag set
by §3, stores into `req->storedReplyState.err` for `QueryReplyCallback`
to emit. Existing code, no change needed.

In that error branch `runCursor` is **not** called, so
`storedReplyState.cursor` stays NULL. After `Cursor_Free(cursor)` the
worker returns and `cursorRead_ctx` calls `UnblockClient`. Two outcomes:
(a) **timeout did not fire**: `QueryReplyCallback` runs, sees
`hasStoredResults == false`, takes the error branch
(`aggregate_exec.c:1516-1525`), and emits the stored error;
(b) **timeout fired first**: client already replied -TIMEOUT,
`QueryReplyCallback` is skipped, and `ShardCursorBlockClient_FreeAREQ`
runs as a no-op for the cursor (already NULL) before the final
`AREQ_DecrRef`.

### 7. Tests

Target file: `tests/pytests/test_blocked_client_timeout.py`, under a
shard-scoped test class (mirror `TestCoordinatorTimeout`, e.g. extending
the existing `TestShardTimeout`). Tests use the same deterministic
toolkit as the coord-plan tests (no wall-clock-based timing).

Toolkit (nothing new needed):

- `CLIENT UNBLOCK <id> TIMEOUT` — fires the blocked-client timeout
  callback deterministically.
- `setPauseBeforeStoreResults` / `setPauseAfterStoreResults`
  (`ENABLE_ASSERT` builds only) — freezes the worker around
  `AREQ_StoreResults` to exercise the two post-pipeline race windows.
- `SYNC_POINT_BEFORE_CURSOR_READ_SEND_CHUNK` — debug sync point already
  in `runCursor` on the `useReplyCallback` path
  (`aggregate_exec.c:1726`), lets a test park the worker exactly before
  the pipeline drives.
- Metrics: `TIMEOUT_ERROR_SHARD_METRIC` (shard equivalent of the coord
  metric) must go up by exactly 1; other timeout counters unchanged
  (reuse `_verify_metrics_not_changed`).

New test methods (all FAIL policy, all post-initial `FT.CURSOR READ`):

- [ ] `test_fail_timeout_shard_cursor_read` — standalone
      (`NumShards == 1`). Issue `FT.AGGREGATE ... WITHCURSOR` under FAIL,
      then `FT.CURSOR READ` in a thread, `CLIENT UNBLOCK <id> TIMEOUT`,
      assert `-TIMEOUT` and shard metric bumped by 1. Subsequent
      `FT.CURSOR READ` on the same CID returns `Cursor not found`.
- [ ] `test_fail_timeout_shard_cursor_read_before_store` — pause before
      `AREQ_StoreResults` via `setPauseBeforeStoreResults`, fire
      `CLIENT UNBLOCK ... TIMEOUT`, resume. Covers "timeout between
      pipeline end and StoreResults" (the `storedReplyState.results`
      was not populated yet, but `storedReplyState.cursor` will be set by
      `runCursor`). Exercises the §4 wrapper's cursor cleanup branch.
- [ ] `test_fail_timeout_shard_cursor_read_after_store` — pause after
      `AREQ_StoreResults` via `setPauseAfterStoreResults`, fire
      `CLIENT UNBLOCK ... TIMEOUT`, resume. Covers "timeout between
      StoreResults and UnblockClient". Exercises the
      `storedReplyState.results` + `storedReplyState.cursor` cleanup
      path in `ChunkReplyState_Destroy` together with §4's wrapper.
- [ ] `test_fail_timeout_internal_cursor_read` — cluster mode
      (`NumShards > 1`), force the coord to dispatch `_FT.CURSOR READ`
      to a target shard (e.g. via the existing cursor-depletion pattern),
      and trigger the blocked-client timeout on the **shard** side (not
      the coord) by pausing the shard worker and firing
      `CLIENT UNBLOCK ... TIMEOUT` on the connection the coord opened
      against the shard. Asserts the shard replies `-TIMEOUT`, the coord
      forwards the error as `-TIMEOUT` to the user, the shard-side
      cursor is freed, and the shard timeout metric is bumped by 1 on
      that shard only.

Sticky-policy regression (the `Cursor::queryTimeoutPolicy` snapshot
frozen at `AREQ_StartCursor` is the same field the coord plan relies on,
so these are shared regression tests — but verify shard-side too):

- [ ] `test_sticky_policy_fail_aggregate_config_return_shard_cursor_read`
      — standalone. Open cursor under `ON_TIMEOUT FAIL`, then
      `CONFIG SET search-on-timeout return`, then `FT.CURSOR READ`.
      Assert that the shard worker attaches the blocked-client timer
      (e.g. visible as a `type=module-cursor` entry with non-zero
      timeout in `CLIENT LIST`, or via the `active cursors` field of
      `FT.INFO`).
- [ ] `test_sticky_policy_return_aggregate_config_fail_shard_cursor_read`
      — reverse: cursor created under RETURN must continue to reply
      inline via `cursorRead_ctx` → `UnblockClient` with no timeout
      callback, even after `CONFIG SET search-on-timeout fail`.

Regression (existing tests, must continue to pass unchanged):

- [ ] `TestShardTimeout::test_cursor_read_after_initial_timeout` —
      covers the initial `FT.AGGREGATE WITHCURSOR` timeout + subsequent
      `FT.CURSOR READ` returning `Cursor not found`. Unchanged semantics.
- [ ] `test_cursors.py::testCursorDepletionStrictTimeoutPolicy` —
      standalone wall-clock `TIMEOUT 1` on `FT.AGGREGATE WITHCURSOR`.
      Our changes do not affect the initial-query path (already uses
      blocked-client timer via `buildPipelineAndExecute`). No change.
- [ ] `test_cursors.py` suite end-to-end — cursor pause/resume and
      `FT.CURSOR DEL` / `GC` paths must remain untouched.

### 8. Documentation

- [ ] Update `FT_CURSOR_READ_TIMEOUT_FAIL_FLOW.md` standalone/shard
      section to replace the in-pipeline `sctx->time.timeout` narrative
      with the new blocked-client flow, mirroring the coord-section
      update that landed with the coord plan.
- [ ] Delete or re-scope the "FAIL timeout is *not* enforced through
      the blocked-client timeout mechanism for FT.CURSOR READ" sentence
      at the top of the same doc — it is the exact statement this plan
      retires.

## Summary of file changes (estimated)

| File | Change |
|---|---|
| `src/info/info_redis/types/blocked_queries.h` | add `privdata`+`freePrivData` to `BlockedCursorNode`; update `BlockedQueries_AddCursor` prototype |
| `src/info/info_redis/types/blocked_queries.c` | thread the two new fields through `BlockedQueries_AddCursor` |
| `src/info/info_redis/block_client.h` | `BlockCursorClient` takes `BlockClientCtx *` (nullable) |
| `src/info/info_redis/block_client.c` | consume `BlockClientCtx *`; call `freePrivData` in `FreeCursorNode` |
| `src/aggregate/aggregate.h` | declare `AREQ_DrainStoredCursor` |
| `src/aggregate/aggregate_request.c` | define `AREQ_DrainStoredCursor` |
| `src/aggregate/aggregate_exec.c` | `ShardCursorBlockClient_FreeAREQ` wrapper (calls `AREQ_DrainStoredCursor`); wire FAIL branch in `RSCursorReadCommand`'s worker-pool dispatch |
| `src/coord/coord_request_ctx.c` | replace inlined cursor-drain block in `CoordRequestCtx_Free` with `AREQ_DrainStoredCursor` |
| `tests/pytests/test_blocked_client_timeout.py` | new shard-scoped FAIL tests + sticky-policy tests |
| `FT_CURSOR_READ_TIMEOUT_FAIL_FLOW.md` | update shard/standalone section |

## Decisions (peer-review feedback, locked in)

1. **`BlockedCursorNode` shape — follow the `BlockedQueryNode` pattern.**
   Add two fields (`void *privdata`, `BlockedQueryNode_FreePrivData freePrivData`)
   to `BlockedCursorNode` and thread them through `BlockedQueries_AddCursor`,
   reusing the existing `BlockedQueryNode_FreePrivData` typedef. No
   shared-header refactor — keep the diff scoped to what's needed and
   mirror the established blocked-client wiring (§1 above already
   proposes exactly this).
2. **Extract `AREQ_DrainStoredCursor` (Q2 yes).** OK because the helper
   is AREQ-level, not coord/shard-level: it lives in
   `aggregate_request.c` next to `AREQ_DecrRef`, declared in
   `aggregate.h`. The coord-side `CoordRequestCtx_Free` (in
   `src/coord/`) and the shard-side `ShardCursorBlockClient_FreeAREQ`
   (in `src/aggregate/`) both call it. No layering violation: shard code
   stays in shard/aggregate files, coord code stays in coord files;
   only the shared AREQ-cleanup primitive moves to AREQ infrastructure.
   §4 above already reflects this.
3. **`_FT.CURSOR READ` behaves as `FT.CURSOR READ` (Q3).** Verified
   parity with `_FT.AGGREGATE` vs `FT.AGGREGATE`:
   - `RSAggregateCommand` (`module.c:477-479`) is the single handler
     registered both at `module.c:1781` for the internal `RS_AGGREGATE_CMD`
     (= `_FT.AGGREGATE`, prefix from `commands.h:14,66`) and called from
     the coord public `FT.AGGREGATE` path at `module.c:3708` when
     `NumShards == 1`. The internal/external distinction is made *inside*
     `prepareRequest` by sniffing `argv[0][0] == '_'`
     (`aggregate_exec.c:1391-1393`) and setting `QEXEC_F_INTERNAL`.
     The blocked-client / worker-pool dispatch logic does **not** branch
     on `IsInternal`.
   - `RSCursorReadCommand` mirrors this exactly: registered at
     `module.c:3927` under `RS_CURSOR_CMD` (= `_FT.CURSOR`) and reached
     from the coord public `FT.CURSOR READ` via `CursorCommand`
     (`module.c:3849-3906`) — either as a direct local call when
     `NumShards == 1` (`module.c:3867`) or as
     `CursorReadCommandInternal` → `RSCursorReadCommand` on the
     dist-threadpool worker.
   - Conclusion: do **not** gate the §3 FAIL wiring on `!IsInternal(req)`.
     The shard arms its own timer in both standalone (`FT.CURSOR READ`
     local call) and cluster (`_FT.CURSOR READ` from coord) cases, the
     same way the shard arms its own timer for `_FT.AGGREGATE`. The
     coord timer is unaffected — it lives on the upstream blocked
     client, the shard timer lives on the per-shard `RedisModule_Call`
     blocked client.
