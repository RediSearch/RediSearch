# MOD-11658 Root Cause Analysis
## Redis Shard Unresponsive After Query Performance Factor Change

### Executive Summary

**Issue**: Redis shard (redis:2) became completely unresponsive after changing the Query Performance Factor (QPF) from 8 to 0, requiring a kill -9 and restore from backup.

**Root Cause**: A race condition/deadlock in the connection pool shrinking mechanism when the number of worker threads is drastically reduced (from 8 to 0) while concurrent queries are actively using connections.

**Impact**: Complete shard unavailability for ~8 hours (09:29 UTC to 17:24 UTC on Sep 30, 2025), affecting customer database operations.

---

### Timeline of Events

| Time (UTC) | Event | Source |
|------------|-------|--------|
| 09:29:04 | Customer changed QPF from 8 to 0 via UI | Activity Log |
| 09:29:21 | RediSearch module logged: "Successfully changed configuration for `WORKERS`" | redis:2 log |
| 09:29:22 | node_wd started getting "Connection refused" errors to redis:2 | node_wd.log |
| 09:29:42 | node_wd getting "Timeout reading from localhost:28188" | node_wd.log |
| 09:31:02 | node_wd attempted auto_recovery but skipped (not safe) | node_wd.log |
| 09:33:03 | Last log entry from redis:2: "Fork CoW for Module fork" | redis:2 log |
| 17:24:01 | Issue resolved by killing redis:2 process and restoring from backup | Manual intervention |

---

### Technical Analysis

#### 1. Query Performance Factor (QPF) Configuration

The Query Performance Factor in Redis Enterprise controls the `WORKERS` configuration parameter for RediSearch:

- **QPF = 0**: `WORKERS = 0` (no worker threads, all queries run on main thread)
- **QPF = 2**: `WORKERS = 3` (minimum 3 CPUs for RediSearch)
- **QPF = 4**: `WORKERS = 6`
- **QPF = 8**: `WORKERS = 12`

In this case, the customer changed from QPF=8 (12 workers) to QPF=0 (0 workers).

#### 2. Connection Pool Calculation

The connection pool size is calculated based on worker threads:

```c
// src/coord/config.c:76-88
int triggerConnPerShard(RSConfig *config) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  size_t connPerShard;
  if (realConfig->connPerShard != 0) {
    connPerShard = realConfig->connPerShard;
  } else {
    connPerShard = config->numWorkerThreads + 1;  // <-- KEY FORMULA
  }
  size_t conn_pool_size = CEIL_DIV(connPerShard, realConfig->coordinatorIOThreads);
  
  MR_UpdateConnPoolSize(conn_pool_size);
  return REDISMODULE_OK;
}
```

**Connection Pool Size Calculation:**
- **Before**: `numWorkerThreads = 12` → `connPerShard = 13` → `conn_pool_size = CEIL_DIV(13, 1) = 13`
- **After**: `numWorkerThreads = 0` → `connPerShard = 1` → `conn_pool_size = CEIL_DIV(1, 1) = 1`

This means the connection pool was **shrunk from 13 connections to 1 connection per shard**.

#### 3. The Shrinking Process

When `WORKERS` is changed, the following sequence occurs:

```c
// src/config.c:397-405
int set_workers(const char *name, long long val, void *privdata, RedisModuleString **err) {
  RSConfig *config = (RSConfig *)privdata;
  config->numWorkerThreads = val;
  workersThreadPool_SetNumWorkers();        // Step 1: Remove worker threads
  COORDINATOR_TRIGGER();                     // Step 2: Trigger connection pool update
  return REDISMODULE_OK;
}
```

The connection pool shrinking happens in `MRConnManager_Shrink`:

```c
// src/coord/rmr/conn.c:331-347
void MRConnManager_Shrink(MRConnManager *m, size_t num) {
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);
    
    for (size_t i = num; i < pool->num; i++) {
      MRConn_Stop(pool->conns[i]);  // <-- STOPS CONNECTIONS IMMEDIATELY
    }
    
    pool->num = num;
    pool->rr %= num;
    pool->conns = rm_realloc(pool->conns, num * sizeof(MRConn *));
  }
  m->nodeConns = num;
  dictReleaseIterator(it);
}
```

#### 4. The Race Condition

**The Problem:**

1. **Active Queries**: At 09:29:21, there were active queries running (evidenced by high CPU usage in Grafana)
2. **Concurrent Connections**: Multiple connections (12+) were actively being used by worker threads
3. **Immediate Shutdown**: When QPF changed to 0:
   - Worker threads were removed (from 12 to 0)
   - Connection pool was shrunk (from 13 to 1)
   - **12 connections were stopped immediately** via `MRConn_Stop()`
4. **No Graceful Drain**: There was no mechanism to:
   - Wait for in-flight queries to complete
   - Drain pending requests before closing connections
   - Ensure the remaining 1 connection could handle all pending work

**Evidence from Logs:**

```
12121:M 30 Sep 2025 09:29:21.310 * <search> Successfully changed configuration for `WORKERS`
2035795:C 30 Sep 2025 09:33:03.425 * Fork CoW for Module fork: current 1109 MB, peak 1109 MB, average 1109 MB
```

The shard stopped responding immediately after the WORKERS change, with only a fork operation logged 3 minutes later (likely a scheduled background task).

#### 5. Why the Shard Became Unresponsive

**Hypothesis (based on Jira comments and code analysis):**

1. **Connection Starvation**: With only 1 connection remaining and all queries forced to the main thread:
   - Multiple concurrent queries tried to use the same connection
   - The single connection became a bottleneck
   - Queries started queuing up

2. **Main Thread Blocking**: With `numWorkerThreads = 0`:
   - All query processing moved to the main thread
   - The main thread became overwhelmed with query processing
   - Redis couldn't respond to PING commands (hence the timeout errors)

3. **Potential Deadlock**: 
   - Active queries may have been holding locks/resources
   - Connection pool shrinking tried to stop connections in use
   - This could have created a deadlock situation

4. **No Event Loop Processing**: 
   - The main thread was blocked processing queries
   - It couldn't process the event loop to handle new connections
   - This explains the "Connection refused" errors from node_wd

**From Jira Comment (dor-forer):**
> "We stay with one connection per shard, and because we are dropping from 8 workers to 1 in one shot, we get some kind of race condition or I/O starvation because multiple concurrent operations tried to use the same connection at the same time?"

#### 6. Supporting Evidence

**Grafana Metrics:**
- High CPU usage on shards at the time of incident
- One active query remained stuck on redis:2 during the entire unavailability period
- DMC CPU usage dropped to 0% at 09:30 (no traffic reaching DMC)

**Slowlog Activity:**
- Showed queries were running at the time of the configuration change

**Fork GC Failures (from second incident on Oct 10):**
```
19129:M 10 Oct 2025 08:41:00.378 # Can't fork for module: File exists
19129:M 10 Oct 2025 08:41:00.378 # <search> fork failed - got errno 17, aborting fork GC
```
This suggests the main thread was in a bad state, unable to perform basic operations.

---

### Root Cause Summary

**The root cause is a race condition in the connection pool shrinking mechanism when worker threads are drastically reduced while queries are active:**

1. **Immediate Connection Closure**: Connections are stopped immediately without waiting for in-flight operations
2. **No Graceful Degradation**: No mechanism to drain pending work before shrinking the pool
3. **Main Thread Overload**: Moving all work to the main thread (workers=0) while simultaneously reducing connections creates a perfect storm
4. **Lack of Synchronization**: The worker thread removal and connection pool shrinking are not properly synchronized with active query processing

**The issue is exacerbated when:**
- Changing from a high worker count to 0 (large delta)
- Active queries are running during the change
- High query load on the database

---

### Why This is a Bug

1. **Configuration changes should be safe**: Changing a configuration parameter should not cause complete shard unavailability
2. **No warning or validation**: The system allowed a dangerous configuration change without warning
3. **No recovery mechanism**: Once stuck, the shard couldn't recover without manual intervention
4. **Violates principle of least surprise**: Users expect configuration changes to be applied gracefully

---

### Recommendations for Fix

1. **Graceful Connection Pool Shrinking**:
   - Wait for in-flight queries to complete before stopping connections
   - Implement a drain mechanism similar to `workersThreadPool_Drain`

2. **Validation**:
   - Warn or prevent drastic worker thread reductions (e.g., >50% change)
   - Require explicit confirmation for workers=0 changes

3. **Synchronization**:
   - Ensure worker thread removal and connection pool updates are atomic
   - Block new queries during the transition period

4. **Monitoring**:
   - Add metrics for connection pool utilization
   - Alert when connection pool is at capacity

5. **Documentation**:
   - Document the risks of changing QPF under load
   - Recommend maintenance windows for such changes

---

### Related Issues

- **RED-139857**: Similar issue mentioned in the ticket (not exact match)
- This issue occurred twice on the same cluster (Sep 30 and Oct 10, 2025)

---

### Next Steps

1. Write a reproduction test (see separate test file)
2. Implement graceful connection pool shrinking
3. Add validation for dangerous configuration changes
4. Update documentation with warnings

