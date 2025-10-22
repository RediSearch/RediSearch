## Root Cause Analysis - MOD-11658

### Incident Summary
**Date**: September 30, 2025, 09:29:21 UTC  
**Impact**: Redis shard completely unresponsive for ~8 hours, required kill -9 and restore from backup  
**Trigger**: `CONFIG SET WORKERS 0` (reducing from 8 to 0) while FT.SEARCH queries were actively running

---

### Root Cause: GIL Deadlock

**The Problem:**

When `CONFIG SET WORKERS 0` is executed, the main Redis thread must terminate all worker threads. To do this safely, it calls `barrier_wait_and_destroy()` which blocks in an infinite loop waiting for each worker thread to signal completion by calling `barrier_wait()`.

However, there's a critical flaw: **the main thread holds the GIL (Global Interpreter Lock) while waiting in this infinite loop**.

At the same time, worker threads that are actively processing FT.SEARCH queries need to:
1. Send requests to other shards via coordinator connections
2. Wait for responses from those shards
3. **Acquire the GIL** to process the coordinator responses (via `RedisModule_ThreadSafeContext`)
4. Aggregate results and complete their work
5. Call `barrier_wait()` to signal completion

Since the main thread holds the GIL and won't release it until workers finish, and workers can't finish without acquiring the GIL, we have a **classic deadlock**.

**Classic Deadlock:**
- **Main thread**: Holds GIL → Waits for workers to finish → Never releases GIL
- **Worker threads**: Need GIL to finish → Cannot acquire it (main holds it) → Never finish
- **Result**: Both wait forever, Redis completely frozen

---

### The Deadlock Mechanism

```
Main Thread                        Worker Threads (8)
     |                                    |
     |--CONFIG SET WORKERS 0------------->|
     |  (Holds GIL)                       |
     |                                    |
     |--barrier_wait_and_destroy()        |
     |  while (received < 8)              |
     |    usleep(1) ←←←←←←←←←←←←←←←←←←←←←←|
     |  (INFINITE LOOP, HOLDS GIL)        |--Need GIL to process responses
     |                                    |--BLOCKED waiting for GIL
     |                                    |--Cannot call barrier_wait()
     |                                    |
     ↓                                    ↓
   STUCK                                STUCK
```

**Key Code** (`src/util/workers.c`):
```c
void workersThreadPool_SetNumWorkers(size_t worker_count) {
    if (worker_count == 0 && curr_workers > 0) {
        redisearch_thpool_terminate_when_empty(_workers_thpool);
        
        // ❌ BUG: Blocks while holding GIL
        new_num_threads = redisearch_thpool_remove_threads(_workers_thpool, curr_workers);
        //                 ↑ Calls barrier_wait_and_destroy() - infinite loop
    }
}
```

---

### Evidence from Production

**Timeline:**
- **09:29:21.310**: Last log: "Successfully changed configuration for WORKERS"
- **09:29:21.310**: Main thread blocks in `barrier_wait_and_destroy()`
- **09:29:22+**: Watchdog: "Redis ping check failed"
- **09:29:21 → 17:24:01**: Complete silence (~8 hours)
- **17:24:01**: Manual kill -9 and restore

**Why Redis appeared frozen:**
- ❌ Cannot process commands (PING, GET, SET, FT.SEARCH)
- ❌ Cannot write to logs
- ❌ Cannot respond to health checks
- ❌ No timeout mechanism in `barrier_wait_and_destroy()`

---

### Reproduction

Created test `test_mod_11658_workers_reduction.py`:
- ✅ Successfully reproduced the bug
- ✅ Same trigger: WORKERS 8→0 under load
- ✅ Same symptom: Complete unresponsiveness, timeout after 300s
- ✅ Same stack trace: Main thread stuck in `barrier_wait_and_destroy()`

**Note**: Race condition - doesn't fail 100% of the time, but reproducible under load.

---

### Impact

**Severity**: CRITICAL
- Complete shard unavailability
- 8 hours of downtime
- Required manual intervention

**Affected**:
- Redis Enterprise clusters using RediSearch
- When changing WORKERS under active query load
- Likely all versions with cluster mode

**Frequency**: RARE
- Requires drastic WORKERS reduction (e.g., 8→0)
- Must occur during active FT.SEARCH queries
- Timing-dependent race condition

---

### Technical Details

**The GIL Problem:**
1. Main thread holds GIL via `RedisModule_ThreadSafeContext`
2. Main thread calls `workersThreadPool_SetNumWorkers(0)`
3. This calls `barrier_wait_and_destroy()` which blocks in infinite loop
4. Worker threads need GIL to process coordinator responses
5. Workers cannot acquire GIL (main holds it)
6. Workers never finish, never call `barrier_wait()`
7. Main thread waits forever

**Why no timeout helps:**
`barrier_wait_and_destroy()` has NO timeout - just infinite loop: `while (received < count) { usleep(1); }`

---

### Incident Details
- **Cluster**: c41399.us-east-1-mz.ec2.cloud.rlrcp.com
- **Database**: db:13338213
- **Redis Version**: 7.28.0-39
- **RediSearch Version**: 2.10.20
- **Duration**: ~8 hours (28,680 seconds)
- **Resolution**: Manual kill -9 and restore from backup

