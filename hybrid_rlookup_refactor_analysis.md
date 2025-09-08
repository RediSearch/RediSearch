# Hybrid Search RLookup Refactor: Analysis & Implementation Plan

## Executive Summary

This document analyzes the current hybrid search RLookup system in RediSearch and proposes a refactor to improve field mapping and merging between multiple search result sources (search index vs vector index).

## Current Architecture Analysis

### Current RLookup System Overview

The RLookup system serves as a field registry and data access mechanism:

- **RLookup**: Container holding a linked list of RLookupKey objects, representing the schema
- **RLookupKey**: Maps field names to array indices (`dstidx` for dynamic array, `svidx` for sorting vector)
- **RLookupRow**: Contains actual data in two arrays:
  - `sv` (RSSortingVector): Static document metadata
  - `dyn` (RSValue**): Dynamic values created during query processing

### Current Hybrid Search Implementation

**Location**: `src/hybrid/hybrid_request.c:62-64`
```c
RLookup *lookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
RLookup_Init(lookup, IndexSpec_GetSpecCache(req->sctx->spec));
RLookup_CloneInto(lookup, AGPLN_GetLookup(AREQ_AGGPlan(req->requests[SEARCH_INDEX]), NULL, AGPLN_GETLOOKUP_FIRST));
```

**Current Process:**
1. Get tail pipeline RLookup
2. Initialize it with spec cache
3. Clone the SEARCH_INDEX request's lookup into tail lookup
4. Hybrid merger merges SearchResult objects containing RLookupRows structured according to their source schemas

**Problem**:
- Tail lookup only contains schema from search index (index 0)
- Vector search results (index 1) may have different field mappings
- During merging, field data from vector results may not map correctly to tail schema

## Proposed Architecture

### New RLookup Initialization Process

Instead of cloning only the search lookup:

1. **Collect All Upstream Lookups**: Iterate through all request lookups (`req->requests[i]`)
2. **Merge Schema Keys**: For each lookup, iterate through all keys and add them to tail lookup (just key names, not data)
3. **Create Unified Schema**: Tail lookup becomes a superset of all upstream schemas

### New Merging Process

**Location**: `src/hybrid/hybrid_search_result.c:120` (`mergeSearchResults`)

Current process:
1. Find primary result (first non-null SearchResult)
2. Apply hybrid scoring
3. Merge flags from all sources
4. Return primary result

**New Process:**
1. Create new RLookupRow with tail schema
2. For each source SearchResult:
   - Extract its RLookupRow data
   - Map each field value to corresponding position in tail schema
   - Write data to new unified RLookupRow
3. Create new SearchResult with unified RLookupRow
4. Apply hybrid scoring and merge flags

## Technical Deep Dive

### Key Components to Modify

#### 1. HybridRequest_BuildMergePipeline()
**File**: `src/hybrid/hybrid_request.c:58-76`

**Current**:
```c
RLookup_CloneInto(lookup, AGPLN_GetLookup(AREQ_AGGPlan(req->requests[SEARCH_INDEX]), NULL, AGPLN_GETLOOKUP_FIRST));
```

**Proposed**:
```c
// Iterate through all upstream lookups and collect keys
for (size_t i = 0; i < req->nrequests; i++) {
    RLookup *upstream_lookup = AGPLN_GetLookup(AREQ_AGGPlan(req->requests[i]), NULL, AGPLN_GETLOOKUP_FIRST);
    if (upstream_lookup) {
        RLookup_MergeKeysInto(lookup, upstream_lookup); // New function to implement
    }
}
```

#### 2. mergeSearchResults() Enhancement
**File**: `src/hybrid/hybrid_search_result.c:120`

**Key Changes**:
- Create new RLookupRow with tail schema
- Map data from each source RLookupRow to unified row
- Handle field name conflicts and type coercion

### New Functions to Implement

#### `RLookup_MergeKeysInto(RLookup *dest, const RLookup *src)`
- Iterate through src keys
- For each key, check if it exists in dest
- If not, create new key in dest with unique dstidx
- Handle name conflicts (perhaps with prefixing)

#### `RLookupRow_MapToSchema(const RLookupRow *src, const RLookup *src_lookup, RLookupRow *dest, const RLookup *dest_lookup)`
- Map data from source row to destination row
- Use key names to find corresponding positions
- Handle missing fields gracefully

## Implementation Challenges & Questions

### 1. Field Name Conflicts
**Challenge**: Multiple sources may have fields with same names but different semantics.

**Questions**:
- Should we prefix field names with source identifier (e.g., `search_score`, `vector_score`)?
- How should we handle system fields like `_score` that exist in all sources?
- Should we prioritize one source over others for conflicting field names?

### 2. Index Assignment Strategy
**Challenge**: Dynamic index assignment in unified schema.

**Questions**:
- Should we maintain a deterministic order (search fields first, then vector fields)?
- How do we ensure backward compatibility with existing field access patterns?
- Should we implement a field name resolution strategy?

### 3. Data Type Consistency
**Challenge**: Same field name from different sources might have different types.

**Questions**:
- Should we perform automatic type coercion?
- How do we handle type mismatches (e.g., string vs numeric)?
- Should we emit warnings for type conflicts?

### 4. Performance Implications
**Challenge**: Additional complexity in field mapping and data copying.

**Questions**:
- What's the performance impact of the additional mapping step?
- Should we optimize for common cases (e.g., no field conflicts)?
- Can we pre-compute mapping tables during initialization?

### 5. Memory Management
**Challenge**: New RLookupRow creation and cleanup.

**Questions**:
- Who owns the unified RLookupRow memory?
- How do we ensure proper cleanup of temporary mapping structures?
- Should we use reference counting for shared field values?

### 6. Sorting Vector Handling
**Challenge**: RLookupRow contains both dynamic (`dyn`) and sorting vector (`sv`) data.

**Questions**:
- How do we handle merging of sorting vector data across sources?
- Should unified rows only use dynamic arrays for simplicity?
- How do we maintain sorting vector optimization benefits?

## Proposed Implementation Plan

### Phase 1: Foundation (Low Risk)
1. **Implement `RLookup_MergeKeysInto()`**
   - Create function to merge key schemas
   - Add comprehensive unit tests
   - Handle basic field name conflicts with prefixing

2. **Modify `HybridRequest_BuildMergePipeline()`**
   - Replace single clone with multi-source key collection
   - Maintain backward compatibility
   - Add logging for debugging field mapping

### Phase 2: Core Functionality (Medium Risk)
3. **Implement `RLookupRow_MapToSchema()`**
   - Map data from source rows to unified schema
   - Handle type coercion and missing fields
   - Optimize for common no-conflict scenarios

4. **Update `mergeSearchResults()`**
   - Create unified RLookupRow instead of using primary
   - Integrate new mapping function
   - Maintain existing score calculation and flag merging

### Phase 3: Optimization & Polish (High Risk)
5. **Performance Optimization**
   - Pre-compute mapping tables during initialization
   - Optimize memory allocation patterns
   - Profile and benchmark against current implementation

6. **Advanced Conflict Resolution**
   - Implement sophisticated field name resolution strategies
   - Add configuration options for conflict handling
   - Support custom field mapping rules

### Phase 4: Testing & Integration
7. **Comprehensive Testing**
   - Unit tests for all new functions
   - Integration tests for various hybrid search scenarios
   - Performance regression tests

8. **Documentation & Migration**
   - Update API documentation
   - Create migration guide for existing code
   - Add troubleshooting guide for field conflicts

## Risk Assessment

### High Risk Areas
- **Breaking Changes**: Field access patterns may change
- **Performance**: Additional mapping overhead
- **Complexity**: Increased code complexity in critical path

### Mitigation Strategies
- **Backward Compatibility**: Maintain existing behavior as fallback
- **Incremental Rollout**: Feature flags for gradual deployment
- **Comprehensive Testing**: Extensive unit and integration tests

## Success Metrics

1. **Functional**: All upstream fields accessible in tail pipeline
2. **Performance**: <5% performance degradation on hybrid search queries
3. **Reliability**: Zero crashes or data corruption in field mapping
4. **Maintainability**: Clear separation of concerns and well-documented APIs

## Next Steps

1. **Stakeholder Review**: Get approval for proposed approach
2. **Proof of Concept**: Implement basic field merging without optimization
3. **Architecture Review**: Validate design with core team
4. **Implementation**: Follow phased implementation plan
5. **Testing**: Comprehensive validation across use cases

## Appendix: Code References

### Key Files to Modify
- `src/hybrid/hybrid_request.c:58-76` - Pipeline initialization
- `src/hybrid/hybrid_search_result.c:120-156` - Result merging
- `src/rlookup.h` - Add new function declarations
- `src/rlookup.c` - Implement new functions

### Key Functions to Study
- `RLookup_CloneInto()` - Current cloning mechanism
- `AGPLN_GetLookup()` - Lookup retrieval from aggregate plans
- `HybridSearchResult_StoreResult()` - Current result storage
- `RLookup_WriteKey()` / `RLookup_GetItem()` - Data access patterns

### Test Files to Update
- `tests/cpptests/test_cpp_rlookup.cpp` - RLookup functionality
- `tests/cpptests/test_cpp_parsed_hybrid_pipeline.cpp` - Hybrid search integration
