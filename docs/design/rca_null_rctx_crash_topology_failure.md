# Root Cause Analysis: SIGSEGV in sendSearchResults During Topology Failure

## Summary

**Date:** 2026-02-17
**Severity:** Critical (Redis crash)
**Signal:** SIGSEGV (11)
**Accessing Address:** 0x18 (NULL + offset 24)

Redis crashed with a segmentation fault during blocked client handling when cluster topology validation failed.

## Crash Context

The crash occurred after the following log messages:
```
<search> Scanning index idx21 in background: done (scanned=2762295)
<search> IORuntime ID 1: Topology validation failed: not all nodes connected
```

The topology validation failure indicates cluster connectivity issues, which led to a code path where the reducer context (`searchReducerCtx`) was never initialized.

## Stack Trace Analysis

```
4173 redis-server *
/tmp/redisearch.so(+0x3ce7b4)[0x7dff9dbce7b4]     <-- Crash location
/tmp/redisearch.so(+0x3cf038)[0x7dff9dbcf038]     <-- Caller
redis-server(moduleHandleBlockedClients+0xba)
redis-server(blockedBeforeSleep+0xa5)
redis-server(beforeSleep+0xd2)
redis-server(aeMain+0x3f)
redis-server(main+0x502)
```

The crash occurs in RediSearch's blocked client reply callback, invoked by Redis's `moduleHandleBlockedClients`.

## Root Cause

### Crash Location

The crash occurs in `sendSearchResults()` at `src/module.c:3006`:

```c
static void sendSearchResults(RedisModule_Reply *reply, searchReducerCtx *rCtx) {
  searchRequestCtx *req = rCtx->searchCtx;   // <-- CRASH: rCtx is NULL
  size_t num = req->requestedResultsCount;
  ...
}
```

### Why Address 0x18?

The `searchReducerCtx` struct layout (from `src/module.c:2022-2041`):

```c
typedef struct searchReducerCtx {
  MRReply *fieldNames;           // offset 0   (8 bytes)
  MRReply *lastError;            // offset 8   (8 bytes)
  searchResult *cachedResult;    // offset 16  (8 bytes)
  searchRequestCtx *searchCtx;   // offset 24 = 0x18  <-- THIS FIELD
  ...
} searchReducerCtx;
```

When `rCtx` is NULL, accessing `rCtx->searchCtx` computes `NULL + 24 = 0x18`, causing the SIGSEGV.

### Call Path

The problematic call path is in `DistSearchUnblockClient()` at `src/module.c:3992-4017`:

```c
static int DistSearchUnblockClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  ...
  struct MRCtx *mrctx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (mrctx) {
    // Check for errors... (lines 3999-4003)

    searchRequestCtx *req = MRCtx_GetPrivData(mrctx);
    searchReducerCtx *rCtx = req->rctx;     // rCtx can be NULL!

    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

    if (req->profileArgs > 0) {
      profileSearchReply(reply, rCtx, ...);
    } else {
      sendSearchResults(reply, rCtx);        // <-- Called with NULL rCtx
    }
    ...
  }
  return REDISMODULE_OK;
}
```

### Why is rCtx NULL?

The `rctx` field in `searchRequestCtx` is set during the reducer phase in `searchResultReducer()` at line 3269:

```c
// src/module.c:3266-3269
searchReducerCtx *rCtx = rm_calloc(1, sizeof(searchReducerCtx));
// Save the rctx in the request so it can be used in reply_callback of unblock client
req->rctx = rCtx;
```

**This reducer function is only called when replies are received.** The initialization path is:

1. `MR_Fanout()` schedules `uvFanoutRequest()` on the IO thread
2. `uvFanoutRequest()` calls `MRCluster_FanoutCommand()` which sends commands to shards
3. Each shard reply triggers `fanoutCallback()`
4. When all replies received: `fanoutCallback()` calls `ctx->fn()` (the reducer)
5. **`searchResultReducer_background()`** queues `searchResultReducer()` on thread pool
6. `searchResultReducer()` allocates `rCtx` and sets `req->rctx = rCtx`

### Code Paths Where rCtx is NOT Initialized

**Path 1: Zero shards respond (numExpected == 0)**
```c
// src/coord/rmr/rmr.c:285-297 - uvFanoutRequest()
static void uvFanoutRequest(void *p) {
  MRCtx *mrctx = p;
  mrctx->numExpected = MRCluster_FanoutCommand(...);  // Returns 0 if no shards reachable

  if (mrctx->numExpected == 0) {
    // Unblock client immediately - reducer NEVER runs
    RedisModule_UnblockClient(bc, mrctx);  // → DistSearchUnblockClient with rCtx=NULL
  }
}
```

**Path 2: All fanout commands fail (all shards error/timeout)**
```c
// src/coord/rmr/rmr.c:243-276 - fanoutCallback()
static void fanoutCallback(...) {
  if (!r) {
    ctx->numErrored++;  // Reply is NULL - shard failed
  }

  if (ctx->numReplied + ctx->numErrored == ctx->numExpected) {
    if (!timedOut && ctx->fn) {
      ctx->fn(ctx, ctx->numReplied, ctx->replies);  // Only if numReplied > 0!
    } else {
      RedisModule_UnblockClient(bc, ctx);  // → DistSearchUnblockClient with rCtx=NULL
    }
  }
}
```
When all shards error, `numReplied == 0` but `numErrored == numExpected`, the `else` branch unblocks without running reducer.

**Path 3: Timeout before any replies (timedOut flag set)**
```c
// Same fanoutCallback - if timedOut is true:
if (timedOut) {
  if (r) MRReply_Free(r);
  ctx->numErrored++;
}
// ...
if (!timedOut && ctx->fn) {  // timedOut=true, so this is false
  ctx->fn(...);
} else {
  RedisModule_UnblockClient(bc, ctx);  // → DistSearchUnblockClient with rCtx=NULL
}
```

**Path 4: MRCluster_FanoutCommand returns 0 due to topology issues**
```c
// src/coord/rmr/cluster.c:50-75 - MRCluster_FanoutCommand()
int MRCluster_FanoutCommand(IORuntimeCtx *ioRuntime, MRCommand *cmd, ...) {
  struct MRClusterTopology *topo = ioRuntime->topo;
  int ret = 0;
  for (size_t i = 0; i < topo->numShards; i++) {
    MRConn *conn = MRConn_Get(...);
    if (conn) {  // NULL if topology validation failed for this shard
      if (MRConn_SendCommand(conn, cmd, fn, privdata) != REDIS_ERR) {
        ret++;
      }
    }
  }
  return ret;  // Returns 0 if all MRConn_Get or MRConn_SendCommand fail
}
```

When topology validation fails ("not all nodes connected"), `MRConn_Get()` returns NULL for affected shards, causing `MRCluster_FanoutCommand()` to return 0 or send to fewer shards than expected.

## Trigger Scenario

1. Coordinator receives FT.SEARCH command
2. Command is blocked and fanout begins to shards
3. IORuntime detects topology validation failure: "not all nodes connected"
4. Request fails/times out before reducer runs
5. Blocked client is unblocked, triggering `DistSearchUnblockClient`
6. `req->rctx` is NULL because reducer never ran
7. `sendSearchResults(reply, NULL)` is called
8. **CRASH** at `NULL->searchCtx`

## Fix (Implemented)

Added NULL checks for both `req` and `rCtx` before calling `sendSearchResults` in both affected functions.

### DistSearchUnblockClient (src/module.c:3992)

```c
searchRequestCtx *req = MRCtx_GetPrivData(mrctx);

// Handle case where request context was never set (e.g., early bailout)
if (!req) {
  return RedisModule_ReplyWithError(ctx, "ERR Search request context not available");
}

searchReducerCtx *rCtx = req->rctx;

// Handle case where reducer never ran (e.g., topology failure, all shards failed)
if (!rCtx) {
  return RedisModule_ReplyWithError(ctx, "ERR Search failed: no results available (cluster error)");
}
```

### DistSearchTimeoutPartialClient (src/module.c:4114)

```c
searchRequestCtx *req = MRCtx_GetPrivData(mrctx);

// Handle case where request context was never set (e.g., early bailout)
if (!req) {
  return RedisModule_ReplyWithError(ctx, "ERR Search request context not available");
}

// ... reducer logic ...

searchReducerCtx *rCtx = req->rctx;

// Handle case where reducer never ran or failed to initialize
if (!rCtx) {
  return RedisModule_ReplyWithError(ctx, "ERR Search failed: no results available (cluster error)");
}
```

## Additional Considerations

1. **Proper error propagation**: When topology fails, ensure `MRCtx_GetStatus()` contains an appropriate error so the early return path is taken.

2. **Defensive programming**: `sendSearchResults()` could also validate its input:
   ```c
   static void sendSearchResults(RedisModule_Reply *reply, searchReducerCtx *rCtx) {
     RS_ASSERT(rCtx && "rCtx must not be NULL");
     RS_ASSERT(rCtx->searchCtx && "searchCtx must not be NULL");
     ...
   }
   ```

3. **Test coverage**: Add tests for topology failure scenarios during active queries.

## Files Affected

- `src/module.c`: `DistSearchUnblockClient`, `DistSearchTimeoutPartialClient`, `sendSearchResults`
- `src/coord/rmr/rmr.c`: Error handling in topology validation paths
