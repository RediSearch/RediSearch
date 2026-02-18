# [Optional] FT.SEARCH Coordinator Refactor Assessment

## Overview

This document assesses the feasibility and risks of migrating FT.SEARCH coordinator
to use `CoordRequestCtx` for timeout handling, aligning it with FT.AGGREGATE/HYBRID.

**Status**: Optional refactoring opportunity  
**Risk Level**: MEDIUM-HIGH  
**Complexity**: HIGH  
**Priority**: Low (existing implementation works)

## Current Architecture Comparison

| Aspect | FT.SEARCH | FT.AGGREGATE/HYBRID |
|--------|-----------|---------------------|
| Request struct | `searchRequestCtx` (lightweight) | `AREQ`/`HybridRequest` (heavy) |
| Fanout mechanism | `MRCtx` + `MR_Fanout` | Direct shard calls via RPNet |
| Sync context | `MRCtx` holds sync primitives | AREQ/HREQ holds sync fields |
| Dispatcher | Custom `DistSearchCommandImp` | `ConcurrentSearch_HandleRedisCommandEx` |
| Execution flow | Fanout → collect → reduce | Pipeline execution |
| Timeout mechanism | MRCtx atomic flags + mutex/condvar | CoordRequestCtx with AREQ/HREQ refcount |

## CoordRequestCtx Design (Current)

```c
typedef struct CoordRequestCtx {
  CommandType type;
  union {
    AREQ *areq;           // Pointer, not embedded
    HybridRequest *hreq;  // Pointer, not embedded
  };
} CoordRequestCtx;
```

Key features:
- **Pointer-based**: Holds pointer to request, not embedded struct
- **Reference counting**: Uses `IncrRef`/`DecrRef` for shared ownership
- **Late binding**: Request pointer set by background thread after parsing
- **Null-safe**: Helper functions handle NULL request pointers

## searchRequestCtx Structure

```c
typedef struct {
  char *queryString;
  long long offset, limit, requestedResultsCount;
  int withScores, withExplainScores, withPayload, withSortby, sortAscending;
  int withSortingKeys, noContent;
  uint32_t format;
  specialCaseCtx** specialCases;
  const char** requiredFields;
  int profileArgs, profileLimited;
  rs_wall_clock profileClock, initClock;
  rs_wall_clock_ns_t coordQueueTime;
  void *reducer;
  bool queryOOM, timedOut;
  struct searchReducerCtx *rctx;
} searchRequestCtx;
```

**Key observations**:
- ~200 bytes, simple parsed query parameters
- No pipeline, no `QueryProcessingCtx`, no `AGGPlan`
- No `RedisSearchCtx`, no index iterators
- Has simple `timedOut` bool (not atomic)
- No `replyState` or `refcount` fields

## What Migration Would Require

### 1. Add Sync Fields to searchRequestCtx

```c
typedef struct searchRequestCtx {
  // ... existing fields ...
  
  // New fields for CoordRequestCtx compatibility
  RS_Atomic(bool) timedOut;        // Replace existing bool
  RS_Atomic(uint8_t) replyState;   // NOT_REPLIED -> REPLYING -> REPLIED
  uint8_t refcount;                // For shared ownership
} searchRequestCtx;
```

### 2. Add Helper Functions

```c
searchRequestCtx *searchRequestCtx_IncrRef(searchRequestCtx *req);
void searchRequestCtx_DecrRef(searchRequestCtx *req);
bool searchRequestCtx_TryClaimReply(searchRequestCtx *req);
void searchRequestCtx_MarkReplied(searchRequestCtx *req);
uint8_t searchRequestCtx_GetReplyState(searchRequestCtx *req);
bool searchRequestCtx_TimedOut(searchRequestCtx *req);
void searchRequestCtx_SetTimedOut(searchRequestCtx *req);
```

### 3. Extend CoordRequestCtx Union

```c
typedef struct CoordRequestCtx {
  CommandType type;
  union {
    AREQ *areq;
    HybridRequest *hreq;
    searchRequestCtx *sreq;  // New member
  };
} CoordRequestCtx;
```

### 4. Modify MRCtx Integration

Current flow uses MRCtx sync primitives. Options:
- **Option A**: Keep MRCtx sync, use CoordRequestCtx only for type abstraction
- **Option B**: Move sync to searchRequestCtx, deprecate MRCtx sync fields
- **Option C**: Dual sync (not recommended - two sources of truth)

## Risks

### Risk 1: Redundant Synchronization (MEDIUM)
MRCtx already has working sync primitives (`timedOut`, `reducing`, `reducingLock`).
Adding sync to searchRequestCtx creates potential for inconsistency.

**Mitigation**: Option A - keep MRCtx sync, use CoordRequestCtx only for abstraction.

### Risk 2: Regression in Critical Path (HIGH)
FT.SEARCH is heavily used. Any changes to timeout handling risk regressions.

**Mitigation**: Extensive testing, feature flag for gradual rollout.

### Risk 3: MR_Fanout Callback Changes (MEDIUM)
Reducer callbacks expect MRCtx. Changing to CoordRequestCtx affects callback signatures.

**Mitigation**: Keep MRCtx as inner context, wrap with CoordRequestCtx at entry point.

### Risk 4: Memory Management Complexity (MEDIUM)
Adding refcount to searchRequestCtx changes ownership model.
Current model: MRCtx owns searchRequestCtx as privdata.

**Mitigation**: Careful refcount management, clear ownership documentation.

## Benefits of Migration

1. **Unified abstraction**: Single `CoordRequestCtx` type for all coordinator commands
2. **Code reuse**: Shared timeout callback infrastructure
3. **Consistency**: Same pattern across FT.SEARCH/AGGREGATE/HYBRID
4. **Future-proofing**: Easier to add new coordinator commands

## Recommendation

**This refactoring is OPTIONAL and LOW PRIORITY**.

The existing FT.SEARCH implementation (PR #8191) works correctly. Migration would
provide architectural consistency but adds risk without immediate functional benefit.

**If pursued**:
1. Start with Option A (keep MRCtx sync, add CoordRequestCtx as wrapper)
2. Extensive testing before merging
3. Consider as part of larger coordinator refactoring effort

## Related Resources

- PR #8191: FT.SEARCH FAIL timeout implementation
- PR #8335: AREQ timeout fields for shard-level
- Epic MOD-8477: Coordinator timeout handling

