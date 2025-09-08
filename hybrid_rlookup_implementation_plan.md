# Hybrid Search RLookup Implementation Plan

## Overview
This document outlines the specific functions that need to be implemented to support merging multiple upstream lookups in hybrid search, enabling proper field mapping between search and vector results.

## Core Functions to Implement

### 1. Schema Construction Functions

#### `RLookup_AddKeysFrom(RLookup *dest, const RLookup *src)`
**Location**: `src/rlookup.c`
**Purpose**: Add keys from source lookup into destination lookup
**Behavior**:
- Iterate through all keys in `src`
- For each key, check if it already exists in `dest` by name
- If not exists, create new key in `dest` with unique `dstidx`
- Skip keys that already exist (first source wins for conflicts)

```c
void RLookup_AddKeysFrom(RLookup *dest, const RLookup *src);
```

**Implementation Details**:
- Use `RLookup_FindKey()` to check for existing keys
- Use `RLookup_GetKey_WriteEx()` to create new keys
- Maintain deterministic ordering (preserve insertion order)
- Simple conflict resolution: first key with name wins

### 2. Row Data Transfer Functions

#### `RLookupRow_TransferFields(RLookupRow *srcRow, const RLookup *srcLookup, RLookupRow *destRow, const RLookup *destLookup)`
**Location**: `src/rlookup.c`
**Purpose**: Transfer field data from source row to destination row with different schemas
**Behavior**:
- Iterate through source lookup keys
- For each key with data in srcRow, find corresponding key in destLookup by name
- Transfer ownership using `RLookup_WriteOwnKey()`
- Nullify source row slots after transfer

```c
void RLookupRow_TransferFields(RLookupRow *srcRow, const RLookup *srcLookup,
                              RLookupRow *destRow, const RLookup *destLookup);
```

**Implementation Details**:
- Use `RLookup_GetItem()` to extract values from source
- Use `RLookup_FindKey()` to locate destination keys by name
- Use `RLookup_WriteOwnKey()` for ownership transfer
- Handle missing destination keys gracefully (log warning)

### 3. Hybrid Search Integration Functions

#### `mergeSearchResults_Enhanced(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx, const RLookup *tailLookup)`
**Location**: `src/hybrid/hybrid_search_result.c`
**Purpose**: Enhanced version of mergeSearchResults with unified schema support
**Behavior**:
- Create new unified SearchResult
- Transfer data from all source SearchResults to unified schema using `RLookupRow_TransferFields()`
- Apply hybrid scoring on unified result
- Return ownership of unified result

```c
SearchResult* mergeSearchResults_Enhanced(HybridSearchResult *hybridResult,
                                         HybridScoringContext *scoringCtx,
                                         const RLookup *tailLookup);
```

## Modified Functions

### 1. `HybridRequest_BuildMergePipeline()`
**Location**: `src/hybrid/hybrid_request.c:58-76`
**Changes**:
- Replace single `RLookup_CloneInto()` call
- Add loop to collect keys from all source lookups
- Use `RLookup_AddKeysFrom()` for each source lookup

**Before**:
```c
RLookup_CloneInto(lookup, AGPLN_GetLookup(AREQ_AGGPlan(req->requests[SEARCH_INDEX]), NULL, AGPLN_GETLOOKUP_FIRST));
```

**After**:
```c
// Add keys from all source lookups
for (size_t i = 0; i < req->nrequests; i++) {
    RLookup *srcLookup = AGPLN_GetLookup(AREQ_AGGPlan(req->requests[i]), NULL, AGPLN_GETLOOKUP_FIRST);
    if (srcLookup) {
        RLookup_AddKeysFrom(lookup, srcLookup);
    }
}
```

### 2. `RPHybridMerger` Integration
**Location**: `src/result_processor.c` (hybrid merger)
**Changes**:
- Store reference to tailLookup in RPHybridMerger struct
- Pass tailLookup to enhanced mergeSearchResults function

**New RPHybridMerger Fields**:
```c
typedef struct {
  // ... existing fields ...
  const RLookup *tailLookup;  // Reference to unified schema
} RPHybridMerger;
```

## Implementation Order

### Phase 1: Core Functions
1. `RLookup_AddKeysFrom()`
2. `RLookupRow_TransferFields()`

### Phase 2: Integration
3. `mergeSearchResults_Enhanced()`
4. Modify `HybridRequest_BuildMergePipeline()`
5. Update `RPHybridMerger` to use new functions

### Phase 3: Testing & Optimization
6. Add unit tests for each new function
7. Add integration tests for hybrid search scenarios
8. Performance testing and optimization

## Function Signatures Summary

```c
// Schema construction
void RLookup_AddKeysFrom(RLookup *dest, const RLookup *src);

// Row data transfer
void RLookupRow_TransferFields(RLookupRow *srcRow, const RLookup *srcLookup,
                              RLookupRow *destRow, const RLookup *destLookup);

// Hybrid search integration
SearchResult* mergeSearchResults_Enhanced(HybridSearchResult *hybridResult,
                                         HybridScoringContext *scoringCtx,
                                         const RLookup *tailLookup);
```

## Header File Changes

### `src/rlookup.h`
Add declarations for new public functions:
- `RLookup_AddKeysFrom()`
- `RLookupRow_TransferFields()`

### `src/hybrid/hybrid_search_result.h`
Add declaration for:
- `mergeSearchResults_Enhanced()`

## Test Files to Create/Update

### New Test Files
- `tests/cpptests/test_cpp_rlookup_keys.cpp` - Schema construction tests
- `tests/cpptests/test_cpp_rlookup_transfer.cpp` - Row data transfer tests

### Updated Test Files
- `tests/cpptests/test_cpp_rlookup.cpp` - Add tests for new functions
- `tests/cpptests/test_cpp_parsed_hybrid_pipeline.cpp` - Integration tests

## Configuration Options (Future)

Consider adding configuration for field name conflict resolution:
```c
typedef enum {
  HYBRID_FIELD_CONFLICT_PREFIX,     // Add prefixes (search_field, vector_field)
  HYBRID_FIELD_CONFLICT_PRIORITY,   // Use first source only
  HYBRID_FIELD_CONFLICT_ERROR       // Fail on conflicts
} HybridFieldConflictStrategy;
```

## Memory Management Notes

- All new functions follow existing RediSearch patterns
- Use `rm_calloc()`, `rm_free()` for allocations
- Leverage existing `RLookup_WriteOwnKey()` for ownership transfer
- Ensure proper cleanup in error paths
- Document ownership transfer in function comments
