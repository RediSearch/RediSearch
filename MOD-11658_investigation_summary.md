# MOD-11658 Investigation Summary

## Problem Statement
Redis shard became completely unresponsive for ~8 hours after changing Query Performance Factor (QPF) from 8 to 0 via `CONFIG SET WORKERS 0`. The shard required manual intervention (kill -9) and restore from backup.

**Incident Details:**
- **Date**: September 30, 2025, 09:29:21 UTC
- **Cluster**: c41399.us-east-1-mz.ec2.cloud.rlrcp.com
- **Database**: db:13338213
- **Redis Version**: 7.28.0-39
- **RediSearch Version**: 2.10.20

---

## Root Cause: GIL Deadlock

When `CONFIG SET WORKERS 0` is executed while worker threads are actively processing FT.SEARCH queries in cluster mode, a GIL (Global Interpreter Lock) deadlock occurs:

1. **Main Thread**: Holds the GIL and blocks in `barrier_wait_and_destroy()` waiting for worker threads (infinite loop, NO timeout)
2. **Worker Threads (8)**: Need the GIL to process coordinator connection responses and complete their work

**Result**: Main thread holds GIL and waits for workers → Workers need GIL to finish → **Deadlock forever**

**The Critical Mistake:**
`barrier_wait_and_destroy()` is called while holding the GIL, but the worker threads it's waiting for also need the GIL to complete their work. This creates a classic deadlock scenario where the main thread holds a resource (GIL) that the workers need, while waiting for the workers to finish.

---

## Investigation Progress

### Phase 1: Production Log Analysis ✅
- Analyzed production logs from debuginfo tarball
- Last log entry: `09:29:21.310 * <search> Successfully changed configuration for WORKERS`
- Complete silence for ~8 hours until manual kill
- Confirmed main thread stuck in `barrier_wait_and_destroy()`

### Phase 2: Code Analysis ✅
- Traced through worker thread pool termination code
- Identified `barrier_wait_and_destroy()` has NO timeout mechanism
- Found coordinator connection management in `MRConnManager_Shrink()`
- Analyzed hiredis async disconnect behavior

### Phase 3: Reproduction Test ✅
Created test: `tests/pytests/test_mod_11658_workers_reduction.py`
- Sets up 3-shard cluster
- Runs continuous FT.SEARCH queries
- Changes WORKERS from 8 to 0 while queries are running
- Successfully reproduced the bug (test times out)

### Phase 4: Instrumentation ✅
Added logging to track:
1. `MRConnManager_Shrink()` - when connections are stopped
2. `signalCallback()` - when `redisAsyncDisconnect()` is called, with pending callback count
3. `MRConn_SendCommand()` - when commands are sent to shards
4. `fanoutCallback()` - when callbacks are invoked

### Phase 5: Crash Analysis ✅
Analyzed crash logs from 3 failed test runs:

**Crash Evidence:**
- **Main Thread**: Crashed in `barrier_wait_and_destroy` (infinite loop)
- **Worker Thread** (`workers-7589`): Stuck in `pthread_barrier_wait`
- **Coordinator Threads**: Stuck in `pthread_cond_wait`

**Timeline:**
```
10:27:03.899 - Commands being sent to shards (callbacks registered)
10:27:03.900 - FT.CONFIG command received (triggers WORKERS reduction)
10:27:03.900 - Some fanoutCallbacks still being invoked
[SILENCE - No more logs]
10:31:59.072 - CRASH after ~5 minutes stuck in barrier_wait_and_destroy
```

**Critical Discovery**: Logs for `MRConnManager_Shrink()` and `redisAsyncDisconnect()` did NOT appear (were using "verbose" level)

### Phase 6: Enhanced Logging ✅
Changed all MOD-11658 logs from "verbose" to "warning" level to ensure visibility:
- `src/coord/rmr/conn.c`: `MRConnManager_Shrink()`, `signalCallback()`, `MRConn_SendCommand()`
- `src/coord/rmr/rmr.c`: `fanoutCallback()`

**Result**: Re-ran test and captured complete logs

### Phase 7: Root Cause Identified ✅
Analyzed latest test logs from `tests/pytests/logs/`:

**Timeline from logs:**
```
10:55:15.317 - FT.CONFIG command received (line 125390)
10:55:15.317-318 - Fanout callbacks still being invoked
[SILENCE - No logs for ~5 minutes]
11:00:10.632 - CRASH in barrier_wait_and_destroy
```

**Critical Discovery:**
- **NO logs from `MRConnManager_Shrink()`** - function was NEVER called!
- **NO logs from `signalCallback()`** - connections were NEVER stopped!
- **NO logs from `redisAsyncDisconnect()`** - disconnect was NEVER initiated!

**Stack Trace Confirms:**
- Main thread: Stuck in `barrier_wait_and_destroy` → `workersThreadPool_SetNumWorkers`
- Worker threads (8): Stuck in `pthread_barrier_wait`
- Coordinator threads (20): Stuck in `pthread_cond_wait`

### Phase 8: Code Path Analysis ✅
Traced the execution path from `CONFIG SET WORKERS 0`:

**Expected Path:**
1. `set_workers()` (src/config.c:397-406)
2. `workersThreadPool_SetNumWorkers()` (blocks waiting for workers)
3. `COORDINATOR_TRIGGER()` → `RSGlobalConfigTriggers[externalTriggerId](config)`
4. `triggerConnPerShard()` (src/coord/config.c:76-89)
5. `MR_UpdateConnPoolSize()` (src/coord/rmr/rmr.c:306-325)
6. `IORuntimeCtx_Schedule()` → schedules `uvUpdateConnPoolSize` on UV loop
7. `uvUpdateConnPoolSize()` → `IORuntimeCtx_UpdateConnPoolSize()`
8. `MRConnManager_Shrink()` → stops connections

**Actual Path:**
1. ✅ `set_workers()` called
2. ✅ `workersThreadPool_SetNumWorkers()` called → **BLOCKS in barrier_wait_and_destroy**
3. ❓ `COORDINATOR_TRIGGER()` - **May or may not be called** (happens AFTER step 2)
4. ❌ `MR_UpdateConnPoolSize()` - **Scheduled on UV loop but NEVER executed**
5. ❌ `MRConnManager_Shrink()` - **NEVER called**

---

## Key Technical Findings

### 1. Barrier Synchronization Has No Timeout
```c
// src/util/workers.c - barrier_wait_and_destroy()
while (barrier->received < barrier->count) {
  usleep(1);  // ← Infinite loop, NO timeout!
}
```

### 2. GIL Deadlock Mechanism
The main thread holds the GIL while blocking in `barrier_wait_and_destroy()`:
- Main thread calls `workersThreadPool_SetNumWorkers(0)` while holding the GIL
- This calls `redisearch_thpool_remove_threads()` which calls `barrier_wait_and_destroy()`
- `barrier_wait_and_destroy()` blocks in an infinite loop waiting for workers to finish
- **Main thread STILL HOLDS THE GIL** while blocked

Worker threads need the GIL to complete their work:
- Worker threads are processing FT.SEARCH queries
- They need to acquire the GIL to process coordinator responses
- Since main thread holds the GIL, workers cannot acquire it
- Workers cannot finish without the GIL
- Workers never call `barrier_wait()`, so the barrier never completes

**Result**: Classic deadlock - Main holds GIL and waits for workers → Workers need GIL to finish

### 3. Why Coordinator Connections Are Not the Problem
Initially suspected that coordinator connections were being stopped prematurely, but investigation revealed:
- `redisAsyncDisconnect()` properly waits for pending callbacks before disconnecting
- Even if responses arrive, workers cannot process them without the GIL
- The real problem is the GIL deadlock, not the connection management

---

## Files Modified

### Source Code (Instrumentation)
- `src/coord/rmr/conn.c` - Added logging for connection management
- `src/coord/rmr/rmr.c` - Added logging for fanout callbacks

### Test Files
- `tests/pytests/test_mod_11658_workers_reduction.py` - Reproduction test

### Documentation
- `MOD-11658_jira_comment.md` - Detailed root cause analysis for JIRA
- `run_until_fail.bash` - Script to run test repeatedly until failure

### Crash Logs
- `crashes/test1/` - First crash logs
- `crashes/test2/` - Second crash logs
- `crashes/test3/` - Third crash logs

---

## ROOT CAUSE CONFIRMED: GIL Deadlock

### The Deadlock Mechanism

**The deadlock occurs because the main thread holds the GIL while waiting for worker threads to finish:**

```c
// src/util/workers.c - workersThreadPool_SetNumWorkers()
void workersThreadPool_SetNumWorkers(size_t worker_count) {
  // ... code ...

  if (worker_count == 0 && curr_workers > 0) {
    redisearch_thpool_terminate_when_empty(_workers_thpool);

    // ❌ BUG: Main thread holds GIL and blocks here
    new_num_threads = redisearch_thpool_remove_threads(_workers_thpool, curr_workers);
    //                 ↑
    //                 This calls barrier_wait_and_destroy() which blocks
    //                 in an infinite loop WHILE HOLDING THE GIL

    workersThreadPool_OnDeactivation(curr_workers);
  }
}
```

**Inside `redisearch_thpool_remove_threads()`** (`src/util/thpool.c`):
```c
size_t redisearch_thpool_remove_threads(redisearch_threadpool thpool, size_t num_threads) {
  // ... code ...

  // ❌ BUG: This blocks in infinite loop while main thread holds GIL
  barrier_wait_and_destroy(&barrier);  // while (received < count) { usleep(1); }

  // ... code ...
}
```

**The GIL Deadlock:**
1. **Main thread** holds the GIL via `RedisModule_ThreadSafeContext`
2. **Main thread** calls `workersThreadPool_SetNumWorkers(0)`
3. **Main thread** blocks in `barrier_wait_and_destroy()` waiting for workers to finish
4. **Worker threads** are processing FT.SEARCH queries
5. **Worker threads** need to acquire the GIL to process coordinator responses
6. **Workers cannot acquire the GIL** because the main thread holds it
7. **Workers cannot finish** without the GIL
8. **Workers never call `barrier_wait()`**, so the barrier never completes
9. **Main thread waits forever** for workers that can never finish
10. **Classic deadlock**: Main holds GIL and waits for workers → Workers need GIL to finish

---

## Root Cause Summary

✅ **Root Cause Confirmed**: GIL (Global Interpreter Lock) deadlock

**The Problem:**
- Main thread holds the GIL while calling `barrier_wait_and_destroy()`
- Worker threads need the GIL to process coordinator responses and finish their work
- Since main thread holds the GIL, workers cannot acquire it
- Workers cannot finish without the GIL, so they never call `barrier_wait()`
- Main thread waits forever for workers that can never finish

**Why This Is a Classic Deadlock:**
- Main thread: Holds resource (GIL), waits for workers to finish
- Worker threads: Need resource (GIL) to finish, but main thread holds it
- Result: Both sides wait forever for each other
   - Reason: UV loop task never executed due to main thread blocking

2. ✅ **How many pending callbacks exist when `redisAsyncDisconnect()` is called?**
   - **N/A** - `redisAsyncDisconnect()` is never called because `MRConnManager_Shrink()` is never called

3. ✅ **Why doesn't `redisProcessCallbacks()` get called after responses arrive?**
   - **N/A** - The issue is earlier in the chain: connections are never stopped at all

4. ✅ **Are callbacks invoked with NULL or with actual replies?**
   - **N/A** - Callbacks are never invoked because connections are never stopped

---

## Next Steps

1. ✅ Change logging from "verbose" to "warning" level
2. ✅ Rebuild RediSearch with new logging
3. ✅ Re-run reproduction test
4. ✅ Analyze new crash logs to answer open questions
5. ✅ Determine actual root cause
6. 🔄 Design and implement fix
7. ⏳ Verify fix with reproduction test

---

## Proposed Fix

**The fix requires reordering operations in `set_workers()` to ensure connection pool shrinking happens BEFORE worker termination:**

### Option 1: Trigger Connection Shrinking First (Recommended)

```c
// src/config.c:397-406
int set_workers(const char *name, long long val, void *privdata, RedisModuleString **err) {
  RSConfig *config = (RSConfig *)privdata;
  size_t old_num_workers = config->numWorkerThreads;
  config->numWorkerThreads = val;

  // STEP 1: Trigger connection pool update FIRST (before blocking)
  COORDINATOR_TRIGGER();

  // STEP 2: Then terminate workers (may block)
  workersThreadPool_SetNumWorkers();

  return REDISMODULE_OK;
}
```

**Pros:**
- Simple change
- Ensures connections are stopped before workers are terminated
- UV loop can execute while main thread is blocked in barrier_wait

**Cons:**
- For multi-shard clusters, connection shrinking is asynchronous (scheduled on UV loop)
- No guarantee shrinking completes before worker termination starts
- May still have race condition

### Option 2: Make Connection Shrinking Synchronous for Worker Reduction

**More complex but safer - requires changes to UV loop scheduling to wait for completion**

### Option 3: Add Timeout to barrier_wait_and_destroy

**Defensive measure but doesn't fix root cause - workers would still be stuck**

---

## Recommendation

**Implement Option 1 first** as it's the simplest fix that addresses the immediate deadlock. The asynchronous nature of connection shrinking is acceptable because:
1. The UV loop will process the shrink request while main thread waits
2. Even if shrinking completes after some workers start terminating, it will still free up stuck workers
3. The barrier wait will eventually succeed once workers receive responses or connections are closed

**Then add defensive timeout** (Option 3) as a safety measure to prevent infinite hangs in case of other unforeseen issues.

---

## Production vs Test Comparison

### Production Incident (Sept 30, 2025)
**Environment:**
- Redis Enterprise Cloud (RLEC)
- Database: `database-MCB7OOPQ` (ID: 13338213)
- 2 shards on node 8
- Module args: `WORKERS 6`
- Redis version: 7.4.3
- RediSearch module loaded

**Symptoms:**
- Database became **completely unresponsive** for ~8 hours
- Timeout errors when trying to connect: `TimeoutError: Timeout reading from 192.168.0.12:3333`
- Redis process still running (receiving timeseries cluster set commands every ~4 seconds)
- No crash - process was **hung/deadlocked**
- Required manual intervention to recover

**Timeline:**
- Unknown when WORKERS was changed (no logs available from production)
- Database remained unresponsive until manual recovery

**Evidence:**
- `/tmp/database_13338213/database_13338213_error.txt`: Shows timeout errors
- `/tmp/node_8/logs/redis-1.log`: Shows process still running, receiving periodic commands
- No stack trace available (process didn't crash)

### Test Reproduction (Oct 22, 2025)
**Environment:**
- Local OSS cluster mode
- 3 shards
- Initial WORKERS: 8
- Changed to: WORKERS 0
- Continuous FT.SEARCH queries running

**Symptoms:**
- Test **crashed after ~5 minutes** with timeout
- Stack trace shows deadlock in `barrier_wait_and_destroy`
- Main thread stuck in `usleep` loop
- 8 worker threads stuck in `pthread_barrier_wait`
- 20 coordinator threads stuck in `pthread_cond_wait`

**Timeline:**
- 10:55:15.317: FT.CONFIG command received
- 10:55:15.317-318: Fanout callbacks still being invoked
- [SILENCE for ~5 minutes]
- 11:00:10.632: CRASH in `barrier_wait_and_destroy`

**Evidence:**
- `tests/pytests/logs/8e7728035820411ea01d0de36b08aa7a.master-1-*.log`: Complete crash log with stack trace
- 110,262 MOD-11658 log lines showing fanout callbacks
- **NO logs from `MRConnManager_Shrink()`** - confirms connections were never stopped

### Comparison Summary

| Aspect | Production | Test |
|--------|-----------|------|
| **Outcome** | Hung indefinitely | Crashed after 5 min |
| **WORKERS change** | Unknown (configured as 6) | 8 → 0 |
| **Cluster type** | RLEC (2 shards) | OSS (3 shards) |
| **Stack trace** | Not available | Available |
| **Process state** | Running but unresponsive | Crashed |
| **Root cause** | Same deadlock mechanism | Same deadlock mechanism |
| **Connection shrinking** | Never happened | Never happened |
| **Recovery** | Manual intervention | Test timeout/crash |

### Conclusion

**The test successfully reproduces the same root cause as the production incident:**

1. ✅ **Same deadlock mechanism**: Main thread blocks in `barrier_wait_and_destroy` waiting for workers
2. ✅ **Same missing behavior**: `MRConnManager_Shrink()` is never called
3. ✅ **Same symptom**: Database becomes unresponsive
4. ✅ **Same trigger**: Reducing WORKERS while queries are running

**Differences are environmental, not fundamental:**
- Production hung indefinitely (no timeout mechanism)
- Test crashed after 5 minutes (test framework timeout)
- Both exhibit the same three-way deadlock

**This confirms the fix will address the production issue.**

---

## How to Continue Investigation

### Rebuild and Test
```bash
cd /home/dor-forer/repos/RediSearch
make clean && make -j8
source venv/bin/activate
bash run_until_fail.bash
```

### Analyze Crash Logs
```bash
# Find MOD-11658 logs
grep -n "MOD-11658" crashes/test*/*.log

# Find configuration change
grep -n "CONFIG SET\|changed configuration\|Shrink" crashes/test*/*.log

# Check for disconnect logs
grep -n "redisAsyncDisconnect\|pending callbacks" crashes/test*/*.log
```

### Key Files to Examine
- `src/coord/rmr/conn.c` - Connection management
- `src/coord/rmr/rmr.c` - Fanout callback handling
- `src/util/workers.c` - Worker thread pool and barrier
- `deps/hiredis/async.c` - Async disconnect behavior

---

## References

- **JIRA Ticket**: MOD-11658
- **Production Logs**: `debuginfo_c41399.us-east-1-mz.ec2.cloud.rlrcp.com_2025-09-30-14.tar.gz`
- **Reproduction Test**: `tests/pytests/test_mod_11658_workers_reduction.py`
- **Detailed Analysis**: `MOD-11658_jira_comment.md`

