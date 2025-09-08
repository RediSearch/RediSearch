# Hybrid Search RLookup Implementation - Phased Plan

## Overview
This document divides the hybrid search RLookup implementation into two distinct phases to ensure robust development and testing.

## Phase 1: RLookup Core Functions (Pure RLookup Logic)

### Functions to Implement

#### `RLookup_AddKeysFrom(RLookup *dest, const RLookup *src, uint32_t flags)`
**Purpose**: Add keys from source lookup into destination lookup
**Behavior**:
- Iterate through all keys in `src`
- For each key, check if it already exists in `dest` by name
- If not exists, create new key in `dest` with unique `dstidx`
- Handle existing keys based on flags (skip by default, override with `RLOOKUP_F_OVERRIDE`)
- Preserve key properties (flags, svidx) from source
- Apply string allocation behavior based on `RLOOKUP_F_NAMEALLOC` flag

#### `RLookupRow_TransferFields(RLookupRow *srcRow, const RLookup *srcLookup, RLookupRow *destRow, const RLookup *destLookup)`
**Purpose**: Transfer field data from source row to destination row with different schemas
**Behavior**:
- Iterate through source lookup keys
- For each key with data in srcRow, find corresponding key in destLookup by name
- Transfer ownership using `RLookup_WriteOwnKey()`
- Nullify source row slots after transfer
- Handle missing destination keys gracefully

### Test Scenarios for RLookup Functions

#### Test Group 1: `RLookup_AddKeysFrom` Basic Functionality

**Test 1.1: Basic Key Addition**
- Create source lookup with 2-3 keys
- Create empty destination lookup
- Call `RLookup_AddKeysFrom(dest, src, RLOOKUP_F_NOFLAGS)`
- Verify all keys from source exist in destination
- Verify destination `rowlen` is updated correctly
- Verify key names and properties are preserved

**Test 1.2: Empty Source Lookup**
- Create empty source lookup
- Create destination with existing keys
- Call `RLookup_AddKeysFrom(dest, src, RLOOKUP_F_NOFLAGS)`
- Verify destination remains unchanged
- Verify no crashes or errors

**Test 1.3: Key Name Conflicts - Default Behavior (First Wins)**
- Create source with keys: "field1", "field2", "field3"
- Create destination with keys: "field2", "field4"
- Call `RLookup_AddKeysFrom(dest, src, RLOOKUP_F_NOFLAGS)`
- Verify destination has: "field2" (original), "field4" (original), "field1" (from source), "field3" (from source)
- Verify "field2" retains destination's properties and flags, not overwritten by source

**Test 1.4: Key Name Conflicts - Override Behavior**
- Create source with keys: "field1", "field2", "field3"
- Create destination with keys: "field2", "field4"
- Call `RLookup_AddKeysFrom(dest, src, RLOOKUP_F_OVERRIDE)`
- Verify destination has: "field2" (from source - overridden), "field4" (original), "field1" (from source), "field3" (from source)
- Verify "field2" now has source's properties and flags
- Test string allocation behavior with `RLOOKUP_F_NAMEALLOC` flag

#### Test Group 2: `RLookup_AddKeysFrom` Edge Cases

**Test 2.1: Multiple Additions**
- Create 3 different lookups with overlapping keys
- Add lookup1 to destination with `RLOOKUP_F_NOFLAGS`
- Add lookup2 to destination with `RLOOKUP_F_NOFLAGS`
- Add lookup3 to destination with `RLOOKUP_F_NOFLAGS`
- Verify correct conflict resolution (first wins)
- Verify all unique keys are present

*Note: Self-addition (dest = src) handled by RS_ASSERT(dest != src) in implementation*

#### Test Group 3: `RLookupRow_TransferFields` Functionality
*Assumption: All source keys have been added to destination using `RLookup_AddKeysFrom()` - enforce with ASSERT*

**Test 3.1: Basic Field Transfer**
- Create source lookup with keys: "field1", "field2"
- Create destination lookup, add all source keys using `RLookup_AddKeysFrom(dest, src, RLOOKUP_F_NOFLAGS)`
- ASSERT all source keys exist in destination
- Create source row with data for both fields (e.g., field1=100, field2=200)
- Create empty destination row
- Call `RLookupRow_TransferFields()`
- Verify data is readable from destination row using field names:
  - `RLookup_GetKey_Read(dest, "field1")` then `RLookup_GetItem(key, destRow)` should return 100
  - `RLookup_GetKey_Read(dest, "field2")` then `RLookup_GetItem(key, destRow)` should return 200
- Verify source row slots are nullified
- Verify RSValue refcounts are correct (same pointers transferred, not copied)

**Test 3.2: Empty Source Row**
- Setup lookups as above
- Source lookup has keys but source row has no data
- Call transfer function
- Verify destination remains empty
- Verify no errors

**Test 3.3: Different Schema Mapping**
- Create source lookup with keys: "field1", "field2", "field3"
- Create destination lookup, add source keys using `RLookup_AddKeysFrom(dest, src, RLOOKUP_F_NOFLAGS)` (destination may have different indices)
- ASSERT all source keys exist in destination
- Fill source row with distinct values (e.g., field1=111, field2=222, field3=333)
- Transfer fields
- Verify data is readable from destination row using field names:
  - `RLookup_GetKey_Read(dest, "field1")` then `RLookup_GetItem(key, destRow)` should return 111
  - `RLookup_GetKey_Read(dest, "field2")` then `RLookup_GetItem(key, destRow)` should return 222
  - `RLookup_GetKey_Read(dest, "field3")` then `RLookup_GetItem(key, destRow)` should return 333
- Verify RSValue ownership transfer (same RSValue pointers, not copies)
- Verify source and destination keys may have different `dstidx` but data is accessible by name

#### Test Group 4: Multiple Upstream Integration Tests

**Test 4.1: No Overlap - Distinct Field Sets**
- Create 2 source lookups: src1["field1", "field2"], src2["field3", "field4"]
- Create destination lookup
- Add keys from both sources: `RLookup_AddKeysFrom(dest, src1, RLOOKUP_F_NOFLAGS)` then `RLookup_AddKeysFrom(dest, src2, RLOOKUP_F_NOFLAGS)`
- Create rows for both sources with data (e.g., src1: field1=10, field2=20; src2: field3=30, field4=40)
- Transfer data from both sources to single destination row
- Verify all 4 fields are readable from destination using field names:
  - `RLookup_GetKey_Read(dest, "field1")` → `RLookup_GetItem()` should return 10
  - `RLookup_GetKey_Read(dest, "field2")` → `RLookup_GetItem()` should return 20
  - `RLookup_GetKey_Read(dest, "field3")` → `RLookup_GetItem()` should return 30
  - `RLookup_GetKey_Read(dest, "field4")` → `RLookup_GetItem()` should return 40

**Test 4.2: Partial Overlap - Some Shared Fields**
- Create 2 source lookups: src1["field1", "field2", "field3"], src2["field2", "field4", "field5"]
- Create destination lookup
- Add keys: `RLookup_AddKeysFrom(dest, src1, RLOOKUP_F_NOFLAGS)` then `RLookup_AddKeysFrom(dest, src2, RLOOKUP_F_NOFLAGS)`
- Destination gets: "field1", "field2" (from src1), "field3" (from src1), "field4", "field5" (from src2)
- Create rows with data, including conflicting data for "field2" (e.g., src1: field2=100, src2: field2=999)
- Transfer src1 first, then src2
- Verify data accessibility using field names:
  - `RLookup_GetKey_Read(dest, "field2")` → should return 100 (src1 wins, src2's 999 ignored)
  - Verify all unique fields transferred correctly from both sources

**Test 4.3: Full Overlap - Identical Field Sets**
- Create 2 source lookups with identical field names: both have ["field1", "field2", "field3"]
- Create destination lookup
- Add keys: `RLookup_AddKeysFrom(dest, src1, RLOOKUP_F_NOFLAGS)` then `RLookup_AddKeysFrom(dest, src2, RLOOKUP_F_NOFLAGS)`
- Create rows with different data for same field names (e.g., src1: field1=100, src2: field1=200)
- Transfer src1 first, then src2
- Verify data accessibility using field names:
  - All fields should contain src1 data (first source wins for all fields)
  - `RLookup_GetKey_Read(dest, "field1")` → should return 100 (not 200)

**Test 4.4: One Empty Source**
- Create 2 source lookups: src1["field1", "field2"], src2["field3", "field4"]
- Create destination lookup
- Add keys from both sources: `RLookup_AddKeysFrom(dest, src1, RLOOKUP_F_NOFLAGS)` then `RLookup_AddKeysFrom(dest, src2, RLOOKUP_F_NOFLAGS)`
- Create row with data for src1 (e.g., field1=50, field2=60), empty row for src2
- Transfer from both sources
- Verify data accessibility using field names:
  - `RLookup_GetKey_Read(dest, "field1")` → should return 50
  - `RLookup_GetKey_Read(dest, "field2")` → should return 60
  - `RLookup_GetKey_Read(dest, "field3")` → should return NULL (empty)
  - `RLookup_GetKey_Read(dest, "field4")` → should return NULL (empty)


### Test File Structure

```
tests/cpptests/test_cpp_rlookup.cpp (extend existing file)
├── RLookupTest (existing test fixture)
├── [Existing tests]
├── Test Group 1: RLookup_AddKeysFrom Basic (4 tests)
├── Test Group 2: RLookup_AddKeysFrom Edge Cases (1 test)
├── Test Group 3: RLookupRow_TransferFields (3 tests)
├── Test Group 4: Multiple Upstream Integration (4 tests)
└── Helper utilities
```

### Success Criteria for Phase 1

- [ ] All 12 test scenarios pass
- [ ] No memory leaks detected
- [ ] Functions handle RLookup flags correctly
- [ ] Memory ownership transfer works correctly
- [ ] Code coverage > 95% for new functions
- [ ] Documentation is complete

---

## Phase 2: Hybrid Search Integration

### Functions to Implement (Phase 2)

#### `mergeSearchResults_Enhanced(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx, const RLookup *tailLookup)`
**Purpose**: Enhanced version of mergeSearchResults with unified schema support

### Modified Functions (Phase 2)

#### `HybridRequest_BuildMergePipeline()`
**Changes**: Replace `RLookup_CloneInto()` with loop calling `RLookup_AddKeysFrom()`

#### `RPHybridMerger` struct
**Changes**: Add reference to unified tail lookup

### Integration Test Scenarios (Phase 2)

#### Integration Test Group 1: End-to-End Hybrid Search
- Create hybrid request with search and vector subqueries
- Verify unified schema contains all fields from both sources
- Execute hybrid search with mixed results
- Verify merged results contain data from both sources

#### Integration Test Group 2: Field Conflict Resolution
- Create subqueries with conflicting field names
- Verify conflict resolution strategy works
- Test field accessibility after merging

#### Integration Test Group 3: Performance Integration
- Compare performance with original implementation
- Verify no significant regression
- Test with various data sizes

### Success Criteria for Phase 2

- [ ] All hybrid search tests pass
- [ ] Integration with existing codebase complete
- [ ] Performance regression < 5%
- [ ] All existing hybrid search functionality preserved
- [ ] New field mapping functionality works correctly

## Implementation Order

### Phase 1 Implementation Steps
1. Implement `RLookup_AddKeysFrom()`
2. Write and pass Test Groups 1-2 (5 tests)
3. Implement `RLookupRow_TransferFields()`
4. Write and pass Test Groups 3-4 (7 tests)
5. Performance testing and optimization
6. Documentation and code review

### Phase 2 Implementation Steps
1. Implement `mergeSearchResults_Enhanced()`
2. Modify `HybridRequest_BuildMergePipeline()`
3. Update `RPHybridMerger` struct
4. Integration testing
5. Performance regression testing
6. Final code review and documentation

## Benefits of Phased Approach

1. **Isolated Testing**: RLookup functions can be thoroughly tested independently
2. **Reduced Risk**: Issues can be caught early in core functions
3. **Incremental Development**: Each phase builds on the previous
4. **Clear Success Criteria**: Easy to measure progress and quality
5. **Parallel Development**: Core and integration work can be done by different developers

## Dependencies

- Phase 2 depends on successful completion of Phase 1
- All tests in Phase 1 must pass before starting Phase 2
- Performance benchmarks from Phase 1 inform Phase 2 optimization
