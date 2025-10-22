# MOD-11658 Investigation Summary

## Quick Reference

**Jira**: [MOD-11658](https://redislabs.atlassian.net/browse/MOD-11658)  
**Issue**: Redis shard unresponsive after Query Performance Factor changed from 8 to 0  
**Status**: Root cause identified, test created  
**Files Created**:
- `MOD-11658_root_cause_analysis.md` - Detailed technical analysis
- `tests/pytests/test_mod_11658_workers_reduction.py` - Reproduction test

---

## The Bug in One Sentence

**Changing the WORKERS configuration from a high value (8) to 0 while queries are running causes a race condition in the connection pool shrinking mechanism, leading to complete shard unresponsiveness.**

---

## What Happened

1. **Customer Action**: Changed Query Performance Factor (QPF) from 8 to 0 via Redis Enterprise UI
2. **Behind the Scenes**: This changed RediSearch `WORKERS` config from 12 to 0
3. **Consequence**: Connection pool shrunk from 13 connections to 1 connection
4. **Result**: Redis shard (redis:2) became completely unresponsive for ~8 hours
5. **Resolution**: Required `kill -9` and restore from backup

---

## Root Cause

### The Formula

```
connPerShard = numWorkerThreads + 1
conn_pool_size = CEIL_DIV(connPerShard, coordinatorIOThreads)
```

**Before**: `12 + 1 = 13` connections  
**After**: `0 + 1 = 1` connection

### The Race Condition

When WORKERS changes from 8 to 0:

1. **Worker threads removed**: 12 threads → 0 threads (all queries move to main thread)
2. **Connections stopped**: 13 connections → 1 connection (12 connections closed immediately)
3. **No graceful drain**: Active queries using those connections are not waited for
4. **Main thread overload**: All queries now on main thread + only 1 connection = bottleneck
5. **Deadlock/starvation**: Multiple queries compete for single connection, main thread can't process event loop
6. **Unresponsive**: Redis can't respond to PING, new connections refused

### Code Location

The problematic code is in `src/coord/rmr/conn.c:331-347` (`MRConnManager_Shrink`):

```c
void MRConnManager_Shrink(MRConnManager *m, size_t num) {
  // ...
  for (size_t i = num; i < pool->num; i++) {
    MRConn_Stop(pool->conns[i]);  // <-- STOPS IMMEDIATELY, NO DRAIN
  }
  // ...
}
```

---

## Evidence

### Timeline
- **09:29:04 UTC**: QPF changed from 8 to 0
- **09:29:21 UTC**: "Successfully changed configuration for `WORKERS`"
- **09:29:22 UTC**: Connection refused errors start
- **09:29:42 UTC**: Timeout errors
- **17:24:01 UTC**: Manual recovery (kill -9 + restore)

### Logs
```
12121:M 30 Sep 2025 09:29:21.310 * <search> Successfully changed configuration for `WORKERS`
2035795:C 30 Sep 2025 09:33:03.425 * Fork CoW for Module fork: current 1109 MB, peak 1109 MB, average 1109 MB
```
(No more logs after this - shard was stuck)

### Metrics
- High CPU usage at time of incident
- One active query stuck on redis:2 during entire outage
- DMC CPU dropped to 0% (no traffic reaching DMC)

---

## The Test

Created `tests/pytests/test_mod_11658_workers_reduction.py` with three test cases:

### 1. `test_MOD_11658_workers_reduction_under_load()`
**Purpose**: Reproduce the exact scenario from the incident

**Steps**:
1. Start with WORKERS=8
2. Create index and load 1000 documents
3. Run 10 concurrent query threads
4. Change WORKERS to 0 while queries are running
5. Verify Redis remains responsive

**Expected**: Should pass (after fix)  
**Current**: May reproduce the bug (shard becomes unresponsive)

### 2. `test_MOD_11658_workers_reduction_sequence()`
**Purpose**: Test if gradual reduction avoids the issue

**Steps**: Reduce workers gradually: 8 → 4 → 2 → 1 → 0

**Hypothesis**: Smaller deltas might not trigger the race condition

### 3. `test_MOD_11658_workers_zero_to_nonzero()`
**Purpose**: Test the reverse direction (connection pool expansion)

**Steps**: Start with WORKERS=0, increase to 8

**Expected**: Should work (expansion is safer than shrinking)

---

## How to Run the Test

```bash
# From RediSearch root directory
cd tests/pytests

# Run all MOD-11658 tests
make pytest TEST=test_mod_11658_workers_reduction.py REDIS_STANDALONE=0 COORD=oss

# Run specific test
make pytest TEST=test_mod_11658_workers_reduction.py:test_MOD_11658_workers_reduction_under_load REDIS_STANDALONE=0 COORD=oss

# Run with verbose output
make pytest TEST=test_mod_11658_workers_reduction.py REDIS_STANDALONE=0 COORD=oss VERBOSE=1
```

**Note**: These tests use `skipTest(cluster=True)` so they only run in standalone mode with coordinator (OSS cluster simulation).

---

## Recommended Fixes

### 1. Graceful Connection Pool Shrinking (Critical)

Add a drain mechanism before stopping connections:

```c
void MRConnManager_Shrink(MRConnManager *m, size_t num) {
  // NEW: Wait for in-flight operations to complete
  MRConnManager_DrainConnections(m, num);
  
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);
    
    for (size_t i = num; i < pool->num; i++) {
      MRConn_Stop(pool->conns[i]);
    }
    // ...
  }
}
```

### 2. Validation (Important)

Prevent dangerous configuration changes:

```c
int set_workers(const char *name, long long val, void *privdata, RedisModuleString **err) {
  RSConfig *config = (RSConfig *)privdata;
  
  // NEW: Warn on large reductions
  if (config->numWorkerThreads > 0 && val == 0) {
    RedisModule_Log(RSDummyContext, "warning", 
      "Reducing WORKERS to 0 may cause temporary unresponsiveness under load");
  }
  
  config->numWorkerThreads = val;
  workersThreadPool_SetNumWorkers();
  COORDINATOR_TRIGGER();
  return REDISMODULE_OK;
}
```

### 3. Synchronization (Important)

Ensure atomic transition:

```c
int set_workers(...) {
  // NEW: Block new queries during transition
  workersThreadPool_Pause();
  
  config->numWorkerThreads = val;
  workersThreadPool_SetNumWorkers();
  COORDINATOR_TRIGGER();
  
  // NEW: Resume after connection pool is updated
  workersThreadPool_Resume();
  
  return REDISMODULE_OK;
}
```

### 4. Documentation (Required)

Update Redis Enterprise documentation:
- Warn about changing QPF under load
- Recommend maintenance windows for QPF changes
- Document the relationship between QPF and connection pools

---

## Related Issues

- **RED-139857**: Similar but not exact issue mentioned in ticket
- This issue occurred **twice** on the same cluster (Sep 30 and Oct 10, 2025)

---

## Impact Assessment

**Severity**: Critical  
**Frequency**: Rare (requires specific conditions)  
**Conditions Required**:
1. Changing WORKERS from high value to 0 (or large reduction)
2. Active queries running during the change
3. High query load

**Affected Versions**: 
- Redis Enterprise 7.28.0-39
- RediSearch 2.10.20
- Likely affects all versions with coordinator mode

---

## Next Steps

1. ✅ **Root cause analysis** - Complete
2. ✅ **Reproduction test** - Complete
3. ⏳ **Run test to confirm reproduction** - Pending
4. ⏳ **Implement fix** - Pending
5. ⏳ **Verify fix with test** - Pending
6. ⏳ **Update documentation** - Pending
7. ⏳ **Backport to affected versions** - Pending

---

## Questions for Discussion

1. Should we prevent WORKERS=0 entirely in production?
2. Should we require a confirmation for large WORKERS reductions?
3. Should we implement automatic rollback if Redis becomes unresponsive?
4. Should we add metrics for connection pool utilization?
5. Is there a way to detect this condition and auto-recover?

---

## Contact

For questions about this analysis:
- **Assignee**: dor-forer
- **Jira**: [MOD-11658](https://redislabs.atlassian.net/browse/MOD-11658)
- **Related RCA**: [RCA-387](https://redislabs.atlassian.net/browse/RCA-387)

