# Design: Add "Queue Time" to FT.PROFILE

## Problem

FT.PROFILE's "Parsing time" incorrectly includes time spent waiting in thread pool queues.

## Thread Pools Overview

| Queue | Debug Command | Where It Appears | Affects "Parsing time"? |
|-------|---------------|------------------|------------------------|
| **Workers Queue** | `_FT.DEBUG WORKERS PAUSE/RESUME` | Shard profile | **YES (bug)** |
| **Coordinator Queue** | `_FT.DEBUG COORD_THREADS PAUSE/RESUME` | Coordinator profile (cluster only) | No |

---

## Part 1: Workers Queue Time

**Root Cause:** In `src/aggregate/aggregate_exec.c`, `initClock` is set before enqueueing (line 1087), but `profileParseTime` is calculated after worker starts (line 987), incorrectly including queue wait.

**Changes:**

| File | Location | Change |
|------|----------|--------|
| `src/aggregate/aggregate.h` | ~line 273 | Add `rs_wall_clock_ns_t profileQueueTime` to AREQ struct |
| `src/aggregate/aggregate_exec.c` | `AREQ_Execute_Callback()` ~line 893 | Capture queue time at function start, reset `initClock` |
| `src/profile.c` | ~line 170 | Print "Workers queue time" after "Parsing time" |

---

## Part 2: Coordinator Queue Time (Cluster Only)

**Root Cause:** In `src/concurrent_ctx.c`, `coordStartTime` is set before enqueueing (line 110), but no queue time is captured when worker starts.

**Changes:**

| File | Location | Change |
|------|----------|--------|
| `src/concurrent_ctx.c` | `threadHandleCommand()` ~line 74 | Capture queue time using `coordStartTime` |
| `src/module.c` | `profileSearchReplyCoordinator()` ~line 3014 | Print "Coordinator queue time" |

---

## Testing

**Approach:** Use `_FT.DEBUG WORKERS/COORD_THREADS PAUSE/RESUME` to artificially create queue time.

### Test 1: Verify Bug Exists (Before Implementation)

**⚠️ CHECKPOINT: Run these tests first and report results before proceeding.**

| Test | Debug Command | Expected |
|------|---------------|----------|
| `testParsingTimeIncludesWorkersQueueTime_BUG` | `WORKERS PAUSE` | Parsing time >= 20ms (bug confirmed) |
| `testParsingTimeDoesNotIncludeCoordQueueTime` | `COORD_THREADS PAUSE` | Parsing time < 20ms (coord queue separate) |

**Test pattern** (same for all tests):
```python
pause_duration_ms = 20
env.cmd(debug_cmd(), '<POOL>', 'PAUSE')
# Run FT.PROFILE in background thread
time.sleep(pause_duration_ms / 1000.0)
env.cmd(debug_cmd(), '<POOL>', 'RESUME')
# Assert timing field >= or < pause_duration_ms
```

**Action Required:** Report results, wait for approval before implementing.

### Validation Results (2026-01-28)

| Test | Result | Details |
|------|--------|---------|
| `testParsingTimeIncludesWorkersQueueTime_BUG` | ✅ PASSED | Bug confirmed: Parsing time includes queue wait time |
| `testParsingTimeDoesNotIncludeCoordQueueTime` | ✅ PASSED | Coordinator queue time is correctly separate from shard's Parsing time |

**Conclusion:** The bug exists as described. Ready to proceed with implementation.

### Tests 2-3: After Implementation

| Test | Asserts |
|------|---------|
| `testWorkersQueueTimeInProfile` | `shard['Workers queue time'] >= 20ms` AND `shard['Parsing time'] < 20ms` |
| `testCoordinatorQueueTimeInProfile` (cluster) | `coord['Coordinator queue time'] >= 20ms` |

**Note:** Test 2 validates both that queue time is captured AND that parsing time no longer includes it.

---

## Implementation Order

1. ~~Run validation tests (Test 1) → report results → wait for approval~~ ✅ DONE
2. Implement Part 1 (Workers Queue Time) ← **NEXT**
3. Run Test 2 → verify pass
4. Implement Part 2 (Coordinator Queue Time)
5. Run Test 3 → verify pass

## Expected Output

**Shard Profile:** `"Parsing time", 0.5, "Workers queue time", 8.0, ...`

**Coordinator Profile:** `"Coordinator queue time", 5.0, "Total Coordinator time", 15.0, ...`

