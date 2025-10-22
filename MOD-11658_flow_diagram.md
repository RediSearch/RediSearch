# MOD-11658 Flow Diagram

## Normal Operation (WORKERS=8)

```
┌─────────────────────────────────────────────────────────────┐
│                    Redis Shard (redis:2)                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Main Thread                    Worker Thread Pool            │
│  ┌──────────┐                  ┌──────────────────┐          │
│  │ Event    │                  │ Worker 1         │          │
│  │ Loop     │                  │ Worker 2         │          │
│  │          │                  │ Worker 3         │          │
│  │ PING     │                  │ ...              │          │
│  │ Commands │                  │ Worker 12        │          │
│  └──────────┘                  └──────────────────┘          │
│                                                               │
│  Connection Pool (13 connections)                            │
│  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
│  │ C1 │ C2 │ C3 │ C4 │ C5 │ C6 │ C7 │ C8 │ C9 │C10 │C11 │C12 │C13 │
│  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘
│                                                               │
└─────────────────────────────────────────────────────────────┘
         ▲                                    ▲
         │                                    │
    ┌────┴────┐                          ┌───┴────┐
    │ Query 1 │                          │ Query N│
    └─────────┘                          └────────┘
    
Status: ✅ Healthy
- Queries distributed across 12 workers
- 13 connections available
- Main thread handles PING/commands
- Event loop responsive
```

---

## The Configuration Change

```
Customer Action:
┌──────────────────────────────────────┐
│ Redis Enterprise UI                  │
│                                      │
│ Query Performance Factor: 8 → 0      │
└──────────────────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────┐
│ RediSearch Module                    │
│                                      │
│ CONFIG SET WORKERS 0                 │
└──────────────────────────────────────┘
                 │
                 ▼
        ┌────────┴────────┐
        │                 │
        ▼                 ▼
┌───────────────┐  ┌──────────────────┐
│ Remove Worker │  │ Shrink Connection│
│ Threads       │  │ Pool             │
│ 12 → 0        │  │ 13 → 1           │
└───────────────┘  └──────────────────┘
```

---

## The Race Condition (What Goes Wrong)

```
Time: 09:29:21.310 - Config change initiated
┌─────────────────────────────────────────────────────────────┐
│                    Redis Shard (redis:2)                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Main Thread                    Worker Thread Pool            │
│  ┌──────────┐                  ┌──────────────────┐          │
│  │ Event    │                  │ Worker 1  ⚠️     │          │
│  │ Loop     │                  │ Worker 2  ⚠️     │          │
│  │          │                  │ Worker 3  ⚠️     │          │
│  │ Blocked! │◄─────────────────│ ...       ⚠️     │          │
│  │ ❌       │  All queries     │ Worker 12 ⚠️     │          │
│  └──────────┘  moved here!     └──────────────────┘          │
│                                 Being terminated...           │
│                                                               │
│  Connection Pool (1 connection remaining)                    │
│  ┌────┐ ❌  ❌  ❌  ❌  ❌  ❌  ❌  ❌  ❌  ❌  ❌  ❌           │
│  │ C1 │ C2  C3  C4  C5  C6  C7  C8  C9  C10 C11 C12 C13      │
│  └────┘ Stopped immediately!                                 │
│    ▲                                                          │
│    │                                                          │
│    └──── 12+ queries competing for this single connection!   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
         ▲                                    ▲
         │                                    │
    ┌────┴────┐                          ┌───┴────┐
    │ Query 1 │ ⏳ Waiting...             │ Query N│ ⏳ Waiting...
    └─────────┘                          └────────┘
    
Time: 09:29:22 - Connection refused errors start
         ▲
         │
    ┌────┴────┐
    │ New     │ ❌ Connection refused!
    │ Query   │
    └─────────┘

Status: ❌ DEADLOCKED
- Main thread blocked processing queries
- Event loop can't run (no PING response)
- Single connection overwhelmed
- New connections refused
- Shard appears dead
```

---

## The Deadlock Sequence

```
Step 1: Worker threads removed (12 → 0)
┌─────────────────────────────────────┐
│ Active Queries: 12                  │
│ Worker Threads: 12 → 0              │
│ Destination: Main Thread            │
└─────────────────────────────────────┘
                 │
                 ▼
Step 2: Connection pool shrunk (13 → 1)
┌─────────────────────────────────────┐
│ Active Connections: 13              │
│ New Pool Size: 1                    │
│ Action: MRConn_Stop() on 12 conns   │
│ Wait for in-flight? NO ❌           │
└─────────────────────────────────────┘
                 │
                 ▼
Step 3: Race condition
┌─────────────────────────────────────┐
│ 12 queries need connections         │
│ Only 1 connection available          │
│ All queries on main thread           │
│ Main thread can't process event loop│
└─────────────────────────────────────┘
                 │
                 ▼
Step 4: Deadlock
┌─────────────────────────────────────┐
│ Main thread: Blocked on queries     │
│ Event loop: Not running              │
│ PING: No response                    │
│ New connections: Refused             │
│ Recovery: Impossible                 │
└─────────────────────────────────────┘
```

---

## The Fix (Proposed)

```
Step 1: Pause new queries
┌─────────────────────────────────────┐
│ workersThreadPool_Pause()           │
│ - Block new query submissions       │
│ - Let current queries continue      │
└─────────────────────────────────────┘
                 │
                 ▼
Step 2: Drain in-flight operations
┌─────────────────────────────────────┐
│ MRConnManager_DrainConnections()    │
│ - Wait for active queries           │
│ - Timeout after reasonable period   │
│ - Use RedisModule_Yield for PING    │
└─────────────────────────────────────┘
                 │
                 ▼
Step 3: Shrink connection pool
┌─────────────────────────────────────┐
│ MRConnManager_Shrink()              │
│ - Now safe to stop connections      │
│ - No active operations               │
└─────────────────────────────────────┘
                 │
                 ▼
Step 4: Update worker threads
┌─────────────────────────────────────┐
│ workersThreadPool_SetNumWorkers()   │
│ - Remove worker threads              │
│ - Move to main thread mode           │
└─────────────────────────────────────┘
                 │
                 ▼
Step 5: Resume operations
┌─────────────────────────────────────┐
│ workersThreadPool_Resume()          │
│ - Allow new queries                  │
│ - System stable with new config      │
└─────────────────────────────────────┘
                 │
                 ▼
Result: ✅ Graceful transition
┌─────────────────────────────────────┐
│ WORKERS: 8 → 0                      │
│ Connections: 13 → 1                 │
│ Downtime: None                       │
│ Data loss: None                      │
│ Responsiveness: Maintained           │
└─────────────────────────────────────┘
```

---

## Key Insight

```
┌──────────────────────────────────────────────────────────┐
│                    THE PROBLEM                           │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Connection pool shrinking happens IMMEDIATELY           │
│  without waiting for in-flight operations                │
│                                                          │
│  MRConn_Stop(pool->conns[i]);  ← No drain, no wait!    │
│                                                          │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│                    THE SOLUTION                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Add graceful draining before stopping connections       │
│                                                          │
│  MRConnManager_DrainConnections(m, num);  ← NEW!        │
│  MRConn_Stop(pool->conns[i]);                           │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## Comparison: Before vs After Fix

```
BEFORE (Current Behavior)
─────────────────────────────────────────────────────────
Time    Action                          Result
─────────────────────────────────────────────────────────
T+0s    CONFIG SET WORKERS 0            Accepted
T+0s    Remove worker threads           12 threads gone
T+0s    Stop 12 connections             Immediate stop
T+0s    12 queries need connections     Compete for 1 conn
T+1s    Main thread blocked             No event loop
T+2s    PING timeout                    Unresponsive
T+3s    Connection refused              Dead shard
T+8h    Manual recovery                 kill -9 + restore
─────────────────────────────────────────────────────────

AFTER (With Fix)
─────────────────────────────────────────────────────────
Time    Action                          Result
─────────────────────────────────────────────────────────
T+0s    CONFIG SET WORKERS 0            Accepted
T+0s    Pause new queries               Queued safely
T+0s    Drain in-flight operations      Wait for completion
T+1s    All operations complete         Safe to proceed
T+1s    Stop 12 connections             Clean shutdown
T+1s    Remove worker threads           12 threads gone
T+1s    Resume with new config          WORKERS=0 active
T+2s    PING                            PONG ✅
T+2s    New queries                     Working ✅
─────────────────────────────────────────────────────────
```

