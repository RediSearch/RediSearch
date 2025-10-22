# MOD-11658 Proposed Fix

## Overview

This document outlines the specific code changes needed to fix the race condition that causes Redis shards to become unresponsive when the WORKERS configuration is changed from a high value to 0 while queries are running.

---

## Fix Strategy

The fix involves three main components:

1. **Graceful Connection Draining**: Wait for in-flight operations before stopping connections
2. **Synchronization**: Ensure worker thread changes and connection pool updates are atomic
3. **Validation**: Add warnings for potentially dangerous configuration changes

---

## Code Changes

### 1. Add Connection Draining Function

**File**: `src/coord/rmr/conn.c`

**Location**: Before `MRConnManager_Shrink` function (around line 328)

```c
// Drain connections before shrinking the pool
// Wait for in-flight operations to complete with timeout
static void MRConnManager_DrainConnections(MRConnManager *m, size_t target_num, 
                                           RedisModuleCtx *ctx, long long timeout_ms) {
  if (!m || !m->map) return;
  
  long long start_time = RedisModule_Milliseconds();
  long long elapsed = 0;
  
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  
  // For each connection pool
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);
    
    // For connections that will be stopped
    for (size_t i = target_num; i < pool->num; i++) {
      MRConn *conn = pool->conns[i];
      
      // Wait for this connection to finish pending operations
      while (MRConn_HasPendingOperations(conn)) {
        elapsed = RedisModule_Milliseconds() - start_time;
        
        // Timeout check
        if (elapsed >= timeout_ms) {
          RedisModule_Log(RSDummyContext, "warning",
            "Timeout waiting for connection to drain (node: %s, elapsed: %lld ms)",
            pool->conns[0]->ep.host, elapsed);
          break;
        }
        
        // Yield to allow Redis to process PING and other commands
        if (ctx && RedisModule_Yield) {
          RedisModule_Yield(ctx, REDISMODULE_YIELD_FLAG_CLIENTS, 
                           "Draining connections before pool shrink");
        }
        
        // Small sleep to avoid busy waiting
        usleep(1000); // 1ms
      }
    }
  }
  
  dictReleaseIterator(it);
  
  elapsed = RedisModule_Milliseconds() - start_time;
  RedisModule_Log(RSDummyContext, "notice",
    "Connection pool drain completed in %lld ms", elapsed);
}
```

**Note**: This requires adding a `MRConn_HasPendingOperations()` function to check if a connection has pending operations. This can be implemented by checking the connection's internal state/queue.

---

### 2. Modify Connection Pool Shrinking

**File**: `src/coord/rmr/conn.c`

**Function**: `MRConnManager_Shrink` (line 331)

**Before**:
```c
void MRConnManager_Shrink(MRConnManager *m, size_t num) {
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);

    for (size_t i = num; i < pool->num; i++) {
      MRConn_Stop(pool->conns[i]);  // <-- IMMEDIATE STOP
    }

    pool->num = num;
    pool->rr %= num;
    pool->conns = rm_realloc(pool->conns, num * sizeof(MRConn *));
  }
  m->nodeConns = num;
  dictReleaseIterator(it);
}
```

**After**:
```c
void MRConnManager_Shrink(MRConnManager *m, size_t num) {
  RS_ASSERT(num > 0);
  RS_ASSERT(num < m->nodeConns);
  
  // Log the shrinking operation
  RedisModule_Log(RSDummyContext, "notice",
    "Shrinking connection pool from %d to %zu connections per node",
    m->nodeConns, num);
  
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);

    // Stop connections that will be removed
    for (size_t i = num; i < pool->num; i++) {
      MRConn_Stop(pool->conns[i]);
    }

    pool->num = num;
    pool->rr %= num;
    pool->conns = rm_realloc(pool->conns, num * sizeof(MRConn *));
  }
  m->nodeConns = num;
  dictReleaseIterator(it);
  
  RedisModule_Log(RSDummyContext, "notice",
    "Connection pool shrink completed");
}
```

---

### 3. Update Connection Pool Size Update Function

**File**: `src/coord/rmr/rmr.c`

**Function**: `uvUpdateConnPoolSize` (line 288)

**Before**:
```c
static void uvUpdateConnPoolSize(void *p) {
  struct UpdateConnPoolSizeCtx *ctx = p;
  IORuntimeCtx *ioRuntime = ctx->ioRuntime;
  IORuntimeCtx_UpdateConnPoolSize(ioRuntime, ctx->conn_pool_size);
  size_t max_pending = ioRuntime->conn_mgr.nodeConns * PENDING_FACTOR;
  RQ_UpdateMaxPending(ioRuntime->queue, max_pending);
  IORuntimeCtx_RequestCompleted(ioRuntime);
  rm_free(ctx);
}
```

**After**:
```c
static void uvUpdateConnPoolSize(void *p) {
  struct UpdateConnPoolSizeCtx *ctx = p;
  IORuntimeCtx *ioRuntime = ctx->ioRuntime;
  
  size_t old_size = ioRuntime->conn_mgr.nodeConns;
  size_t new_size = ctx->conn_pool_size;
  
  // If shrinking, drain connections first
  if (new_size < old_size) {
    RedisModule_Log(RSDummyContext, "notice",
      "Draining connections before shrinking pool from %zu to %zu",
      old_size, new_size);
    
    // Drain with 5 second timeout
    MRConnManager_DrainConnections(&ioRuntime->conn_mgr, new_size, 
                                   ctx->redisCtx, 5000);
  }
  
  IORuntimeCtx_UpdateConnPoolSize(ioRuntime, ctx->conn_pool_size);
  size_t max_pending = ioRuntime->conn_mgr.nodeConns * PENDING_FACTOR;
  RQ_UpdateMaxPending(ioRuntime->queue, max_pending);
  IORuntimeCtx_RequestCompleted(ioRuntime);
  rm_free(ctx);
}
```

**Note**: Need to add `redisCtx` to `UpdateConnPoolSizeCtx` struct.

---

### 4. Add Warning for Dangerous Configuration Changes

**File**: `src/config.c`

**Function**: `set_workers` (line 397)

**Before**:
```c
int set_workers(const char *name, long long val, void *privdata,
RedisModuleString **err) {
  uint32_t externalTriggerId = 0;
  RSConfig *config = (RSConfig *)privdata;
  config->numWorkerThreads = val;
  workersThreadPool_SetNumWorkers();
  // Trigger the connection per shard to be updated (only if we are in coordinator mode)
  COORDINATOR_TRIGGER();
  return REDISMODULE_OK;
}
```

**After**:
```c
int set_workers(const char *name, long long val, void *privdata,
RedisModuleString **err) {
  uint32_t externalTriggerId = 0;
  RSConfig *config = (RSConfig *)privdata;
  
  size_t old_workers = config->numWorkerThreads;
  size_t new_workers = (size_t)val;
  
  // Warn about potentially dangerous changes
  if (old_workers > 0 && new_workers == 0) {
    RedisModule_Log(RSDummyContext, "warning",
      "Reducing WORKERS from %zu to 0. This may cause temporary slowdown "
      "as all queries will run on the main thread. "
      "Connection pool will be reduced from %zu to 1.",
      old_workers, old_workers + 1);
  } else if (old_workers > new_workers && (old_workers - new_workers) > 4) {
    RedisModule_Log(RSDummyContext, "warning",
      "Large reduction in WORKERS from %zu to %zu. "
      "Connection pool will be reduced from %zu to %zu. "
      "Consider gradual reduction if under heavy load.",
      old_workers, new_workers, old_workers + 1, new_workers + 1);
  }
  
  config->numWorkerThreads = val;
  workersThreadPool_SetNumWorkers();
  // Trigger the connection per shard to be updated (only if we are in coordinator mode)
  COORDINATOR_TRIGGER();
  return REDISMODULE_OK;
}
```

---

### 5. Add Helper Function to Check Pending Operations

**File**: `src/coord/rmr/conn.c`

**Location**: Add new function before `MRConnManager_Shrink`

```c
// Check if a connection has pending operations
// This is a simplified version - actual implementation depends on MRConn internals
static bool MRConn_HasPendingOperations(MRConn *conn) {
  if (!conn) return false;
  
  // Check connection state
  if (conn->state != MRConn_Connected) {
    return false; // Not connected, no pending ops
  }
  
  // Check if there are pending requests in the connection's queue
  // This depends on the internal structure of MRConn
  // May need to add a field to track pending operations
  
  // For now, return false if connection is idle
  // TODO: Implement proper pending operation tracking
  return false;
}
```

**Note**: This is a placeholder. The actual implementation needs to track pending operations in the `MRConn` structure.

---

## Additional Changes Needed

### 1. Update `UpdateConnPoolSizeCtx` Structure

**File**: `src/coord/rmr/rmr.c`

**Location**: Around line 280

**Before**:
```c
struct UpdateConnPoolSizeCtx {
  IORuntimeCtx *ioRuntime;
  size_t conn_pool_size;
};
```

**After**:
```c
struct UpdateConnPoolSizeCtx {
  IORuntimeCtx *ioRuntime;
  size_t conn_pool_size;
  RedisModuleCtx *redisCtx;  // NEW: For yielding during drain
};
```

### 2. Update Callers of `MR_UpdateConnPoolSize`

**File**: `src/coord/rmr/rmr.c`

**Function**: `MR_UpdateConnPoolSize` (line 299)

Need to pass `RedisModuleCtx` through the call chain so it can be used for yielding during drain.

---

## Testing the Fix

### Unit Tests

1. **Test graceful shrinking**: Verify connections are drained before stopping
2. **Test timeout**: Verify timeout works if operations don't complete
3. **Test expansion**: Verify expanding pool still works correctly
4. **Test warnings**: Verify warnings are logged for dangerous changes

### Integration Tests

Use the test file created: `tests/pytests/test_mod_11658_workers_reduction.py`

```bash
# Run the reproduction test
make pytest TEST=test_mod_11658_workers_reduction.py:test_MOD_11658_workers_reduction_under_load

# Should pass after fix is applied
```

### Manual Testing

1. Set up Redis Enterprise cluster
2. Create database with QPF=8
3. Run load test with concurrent queries
4. Change QPF to 0 via UI
5. Verify shard remains responsive
6. Check logs for warnings

---

## Rollout Plan

### Phase 1: Development
- [ ] Implement connection draining function
- [ ] Add pending operation tracking to MRConn
- [ ] Update connection pool shrinking logic
- [ ] Add warnings for dangerous changes

### Phase 2: Testing
- [ ] Run unit tests
- [ ] Run integration tests
- [ ] Manual testing in dev environment
- [ ] Load testing with configuration changes

### Phase 3: Deployment
- [ ] Code review
- [ ] Merge to master
- [ ] Backport to affected versions (7.28.x, 7.26.x, etc.)
- [ ] Release notes
- [ ] Documentation updates

### Phase 4: Monitoring
- [ ] Monitor customer environments
- [ ] Track metrics for connection pool changes
- [ ] Collect feedback

---

## Risks and Mitigation

### Risk 1: Drain Timeout Too Short
**Mitigation**: Make timeout configurable, default to 5 seconds

### Risk 2: Yield Not Available (Redis < 7)
**Mitigation**: Check for `RedisModule_Yield` availability, skip if not present

### Risk 3: Performance Impact
**Mitigation**: Only drain when shrinking, not when expanding

### Risk 4: Incomplete Pending Operation Tracking
**Mitigation**: Start with conservative approach, improve over time

---

## Alternative Approaches Considered

### 1. Prevent WORKERS=0 Entirely
**Pros**: Eliminates the issue completely  
**Cons**: Breaks existing functionality, customer expectations

### 2. Require Maintenance Mode for Changes
**Pros**: Safest approach  
**Cons**: Poor user experience, not always practical

### 3. Automatic Rollback on Failure
**Pros**: Self-healing  
**Cons**: Complex to implement, may not detect all failures

### 4. Gradual Reduction Only
**Pros**: Reduces race condition window  
**Cons**: Slower, still has race condition

**Selected Approach**: Graceful draining (most balanced)

---

## Success Criteria

- [ ] Test `test_MOD_11658_workers_reduction_under_load` passes
- [ ] No shard unresponsiveness when changing WORKERS under load
- [ ] Warnings logged for dangerous configuration changes
- [ ] Connection pool transitions complete within timeout
- [ ] No performance regression for normal operations
- [ ] Documentation updated with best practices

---

## References

- **Jira**: [MOD-11658](https://redislabs.atlassian.net/browse/MOD-11658)
- **Root Cause Analysis**: `MOD-11658_root_cause_analysis.md`
- **Test File**: `tests/pytests/test_mod_11658_workers_reduction.py`
- **Flow Diagram**: `MOD-11658_flow_diagram.md`

