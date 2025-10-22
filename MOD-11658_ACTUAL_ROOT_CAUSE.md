# MOD-11658: ACTUAL Root Cause (After Reproduction with Logging)

## Executive Summary

After adding detailed logging and successfully reproducing the bug, we discovered that **our initial hypothesis was PARTIALLY WRONG**.

**Initial Hypothesis**: `MRConnManager_Shrink()` stops coordinator connections immediately, causing worker threads to wait forever for responses.

**ACTUAL Root Cause**: `MRConnManager_Shrink()` is **NEVER CALLED** because the main thread blocks in `barrier_wait_and_destroy()` BEFORE reaching the `COORDINATOR_TRIGGER()` call that would invoke it.

---

## The Actual Deadlock Sequence

### Code Path in `src/config.c:376-389`

```c
CONFIG_SETTER(setWorkThreads) {
  size_t newNumThreads;
  int acrc = AC_GetSize(ac, &newNumThreads, AC_F_GE0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (newNumThreads > MAX_WORKER_THREADS) {
    return errorTooManyThreads(status);
  }
  config->numWorkerThreads = newNumThreads;

  workersThreadPool_SetNumWorkers();  // Line 385 - BLOCKS FOREVER HERE!
  // Trigger the connection per shard to be updated (only if we are in coordinator mode)
  COORDINATOR_TRIGGER();              // Line 387 - NEVER REACHED!
  return REDISMODULE_OK;
}
```

### Timeline from Reproduction Logs

**Test Run**: October 22, 2025, 10:10:04

1. **10:10:04.222**: `CONFIG SET WORKERS 0` command received by main thread
   - Log: `FT.CONFIG is deprecated, please use CONFIG SET search-workers instead`

2. **10:10:04.222**: Main thread calls `setWorkThreads()` → `workersThreadPool_SetNumWorkers()` (line 385)

3. **Inside `workersThreadPool_SetNumWorkers()` (src/util/workers.c:77-101)**:
   ```c
   if (worker_count == 0 && curr_workers > 0) {
     redisearch_thpool_terminate_when_empty(_workers_thpool);  // Line 88
     new_num_threads = redisearch_thpool_remove_threads(_workers_thpool, curr_workers);  // Line 89
     workersThreadPool_OnDeactivation(curr_workers);  // Line 90
   }
   ```

4. **Inside `redisearch_thpool_terminate_when_empty()` (deps/thpool/thpool.c:527)**:
   ```c
   void redisearch_thpool_terminate_when_empty(redisearch_thpool_t *thpool_p) {
     thpool_p->terminate_when_empty = 1;
     redisearch_thpool_broadcast_new_state(thpool_p);  // Calls barrier_wait_and_destroy()
   }
   ```

5. **Inside `barrier_wait_and_destroy()` (deps/thpool/barrier.c:28)**:
   ```c
   void barrier_wait_and_destroy(barrier_t *barrier) {
     while (barrier->received < barrier->count) {
       usleep(1);  // INFINITE LOOP - NO TIMEOUT!
     }
     // ... destroy barrier ...
   }
   ```
   - `barrier->count = 8` (number of worker threads)
   - `barrier->received = 0` (no workers have called `barrier_wait()` yet)
   - **BLOCKS HERE FOREVER**

6. **Worker threads** (at the same time):
   - Processing FT.SEARCH queries
   - Sent requests to other shards via coordinator connections
   - Waiting for responses to arrive via UV loop events
   - **Responses are processed by the MAIN THREAD's UV loop**

7. **Main thread is blocked**:
   - Cannot process UV loop events
   - Cannot deliver responses to worker threads
   - Cannot process ANY Redis commands (PING, GET, SET, etc.)
   - Cannot write to logs

8. **Worker threads never get responses**:
   - Never finish their jobs
   - Never call `barrier_wait()`
   - Never increment `barrier->received`

9. **Main thread never gets past line 385**:
   - Never reaches `COORDINATOR_TRIGGER()` at line 387
   - Never calls `MRConnManager_Shrink()`
   - **Stuck forever in `barrier_wait_and_destroy()`**

10. **10:15:00.581**: Test timeout kills the process (5 minutes later)
    - Stack trace shows crash in `barrier_wait_and_destroy`

---

## Evidence from Reproduction Logs

### ✅ What We Found:

1. **Last MOD-11658 log**: `10:10:04.222`
   - Commands being sent: `MOD-11658: Sending command, callback=0x..., privdata=0x...`
   - Callbacks being invoked: `MOD-11658: fanoutCallback called: r=0x..., numReplied=..., numExpected=3`
   - Everything working normally

2. **FT.CONFIG deprecation warning**: `10:10:04.222`
   - `FT.CONFIG is deprecated, please use CONFIG SET search-workers instead`
   - Confirms the config command was received

3. **Crash in `barrier_wait_and_destroy`**: `10:15:00.581`
   ```
   /home/dor-forer/repos/RediSearch/bin/.../redisearch.so(barrier_wait_and_destroy+0x1a)
   /home/dor-forer/repos/RediSearch/bin/.../redisearch.so(redisearch_thpool_terminate_when_empty+0x110)
   /home/dor-forer/repos/RediSearch/bin/.../redisearch.so(workersThreadPool_SetNumWorkers+0x79)
   /home/dor-forer/repos/RediSearch/bin/.../redisearch.so(setWorkThreads+0x53)
   ```

### ❌ What We Did NOT Find:

1. **NO logs for `MRConnManager_Shrink`**
   - Expected: `MOD-11658: MRConnManager_Shrink called: shrinking from 8 to 1 connections per node`
   - Actual: Nothing - function was never called

2. **NO logs for `redisAsyncDisconnect`**
   - Expected: `MOD-11658: Calling redisAsyncDisconnect with X pending callbacks`
   - Actual: Nothing - connections were never stopped

3. **NO "Successfully changed configuration for WORKERS"**
   - This log message appears AFTER `workersThreadPool_SetNumWorkers()` returns
   - Never logged because the function never returned

---

## The Real Problem: Architectural Issue

### The Deadlock Pattern

```
Main Thread:  Waiting for workers to finish (barrier_wait_and_destroy)
              ↓
Worker Threads: Waiting for coordinator responses (I/O wait)
              ↓
UV Loop Events: Waiting for main thread to process them
              ↓
Main Thread:  (blocked in barrier_wait_and_destroy, cannot process events)
              ↓
              DEADLOCK!
```

### Why This Happens

**The main Redis thread has two conflicting responsibilities:**

1. **Worker thread synchronization**: 
   - Blocks in `barrier_wait_and_destroy()` waiting for workers to finish
   - Uses infinite loop with `usleep(1)` - NO timeout mechanism

2. **UV loop event processing**: 
   - Processes coordinator connection responses
   - Delivers responses to worker threads
   - **Cannot run while blocked in `barrier_wait_and_destroy()`**

**When `CONFIG SET WORKERS 0` is executed:**
- Main thread enters `barrier_wait_and_destroy()` and blocks
- Worker threads are waiting for coordinator responses
- Coordinator responses arrive but cannot be processed (main thread is blocked)
- Worker threads never get responses, never finish, never call `barrier_wait()`
- Main thread waits forever for workers to call `barrier_wait()`
- **Classic circular dependency deadlock**

---

## Why Our Initial Hypothesis Was Wrong

We thought the problem was:
- `MRConnManager_Shrink()` stops connections immediately
- Worker threads waiting for responses that will never arrive

But actually:
- `MRConnManager_Shrink()` is never called
- Worker threads are waiting for responses that COULD arrive
- But the main thread is blocked and cannot deliver them

The connections are NOT stopped - they're still active and responses are arriving, but the main thread cannot process the UV loop events to deliver them to the worker threads.

---

## Comparison: Production vs. Test

### Production (September 30, 2025, 09:29:21.310):
- Last log: `Successfully changed configuration for WORKERS`
- This means `workersThreadPool_SetNumWorkers()` **DID return** in production
- So `COORDINATOR_TRIGGER()` **WAS called** in production
- So `MRConnManager_Shrink()` **WAS called** in production
- **Different deadlock mechanism than in our test!**

### Test (October 22, 2025, 10:10:04.222):
- Last log: `FT.CONFIG is deprecated...`
- NO "Successfully changed configuration for WORKERS"
- `workersThreadPool_SetNumWorkers()` **DID NOT return**
- `COORDINATOR_TRIGGER()` **WAS NOT called**
- `MRConnManager_Shrink()` **WAS NOT called**

### Conclusion:

**There are TWO different deadlock scenarios:**

1. **Scenario A (Our test)**: Main thread blocks in `barrier_wait_and_destroy()` before calling `COORDINATOR_TRIGGER()`
   - Workers waiting for responses
   - Main thread cannot process UV events
   - Deadlock

2. **Scenario B (Production)**: Main thread successfully calls `COORDINATOR_TRIGGER()` and `MRConnManager_Shrink()`
   - Connections are stopped
   - Workers waiting for responses that will never arrive
   - Main thread blocks in `barrier_wait_and_destroy()` waiting for workers
   - Deadlock

**Both scenarios lead to the same outcome (deadlock), but through different paths!**

---

## Next Steps

1. **Investigate why production behaved differently**:
   - Why did `workersThreadPool_SetNumWorkers()` return in production but not in test?
   - Was the timing different?
   - Were there fewer active queries in production at that moment?

2. **Fix both deadlock scenarios**:
   - Scenario A: Don't block main thread in `barrier_wait_and_destroy()`
   - Scenario B: Don't stop connections while workers are using them

3. **Add timeout to `barrier_wait_and_destroy()`**:
   - Current implementation has infinite loop with no timeout
   - Should have configurable timeout (e.g., 30 seconds)
   - Should log warning and abort if timeout is reached

4. **Separate UV loop processing from main thread**:
   - Move UV loop to separate thread
   - Or use async/non-blocking barrier mechanism

