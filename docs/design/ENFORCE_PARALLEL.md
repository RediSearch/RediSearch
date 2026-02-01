# ENFORCE_PARALLEL Query Option

## Overview

When `WORKERS` configuration is set to > 0, RediSearch executes queries in background worker threads. However, accessing Redis keyspace from these threads requires acquiring the Global Interpreter Lock (GIL) via `RedisModule_ThreadSafeContextLock`, which serializes access and limits parallelism.

This document proposes an `ENFORCE_PARALLEL` keyword for `FT.SEARCH`, `FT.AGGREGATE`, and `FT.HYBRID` commands that validates at query time whether all requested fields can be served without GIL acquisition, enabling true parallel query execution.

## Background

### Current Architecture

1. **Worker Threads**: When `WORKERS > 0`, queries are dispatched to a thread pool for background execution.

2. **Field Value Sources**: Field values can come from two sources:
   - **Sorting Vector**: Pre-computed values stored in `RSSortingVector` during indexing
   - **Redis Keyspace**: Original document data (HASH/JSON keys)

3. **GIL Acquisition**: The `RPSafeLoader` result processor acquires the GIL when loading field values from Redis keyspace:
   ```c
   RedisModule_ThreadSafeContextLock(sctx->redisCtx);
   rpSafeLoader_Load(self);
   RedisModule_ThreadSafeContextUnlock(sctx->redisCtx);
   ```

4. **SORTABLE + UNF Fields**: Fields declared as `SORTABLE UNF` (Un-Normalized Format) store the original value in the sorting vector, making it available without keyspace access. The `RLOOKUP_F_VAL_AVAILABLE` flag indicates this:
   ```c
   if (FieldSpec_IsSortable(fs)) {
     key->_flags |= RLOOKUP_F_SVSRC;
     if (FieldSpec_IsUnf(fs)) {
       key->_flags |= RLOOKUP_F_VAL_AVAILABLE;
     }
   }
   ```

### The Problem

When queries request fields that are not `SORTABLE UNF`, the loader must access Redis keyspace, requiring GIL acquisition. This serializes what could otherwise be parallel query execution, negating the benefits of worker threads.

Users currently have no way to:
1. Ensure their queries run in full parallel mode
2. Get early feedback if their schema doesn't support parallel execution for specific queries

## Proposed Solution

### New Keyword: `ENFORCE_PARALLEL`

Add an optional `ENFORCE_PARALLEL` keyword to query commands that:
1. Validates all fields that need to be loaded/returned are available without GIL acquisition
2. Returns an error if any field would require keyspace access
3. Guarantees the query will execute in full parallel mode when successful

### Syntax

```
FT.SEARCH <index> <query> [ENFORCE_PARALLEL] [RETURN <num> <field> ...] ...
FT.AGGREGATE <index> <query> [ENFORCE_PARALLEL] [LOAD <num> <field> ...] ...
FT.HYBRID <index> SEARCH <query> VSIM ... [ENFORCE_PARALLEL] ...
```

### Validation Rules

A field is considered "parallel-safe" if ANY of the following conditions are met:

1. **SORTABLE + UNF**: The field is declared with both `SORTABLE` and `UNF` options
2. **NUMERIC SORTABLE**: Numeric fields with `SORTABLE` (numeric values are stored as-is)
3. **No Loading Required**: The field is not requested in RETURN/LOAD clauses

For TAG fields specifically:
- Must be declared as `SORTABLE UNF` to be parallel-safe
- Without `UNF`, TAG values are normalized (case-folded) in the sorting vector, which differs from the original value

### Error Messages

When validation fails, return a descriptive error:

```
ENFORCE_PARALLEL: Field '<field_name>' requires keyspace access. 
Declare it as SORTABLE UNF in the index schema for parallel execution.
```

For TAG fields without UNF:
```
ENFORCE_PARALLEL: TAG field '<field_name>' is SORTABLE but not UNF. 
TAG fields must be SORTABLE UNF for parallel execution.
```

## Implementation Details

### Phase 1: Validation Logic

Add validation in the query parsing/planning phase:

```c
typedef enum {
  PARALLEL_SAFE,
  PARALLEL_UNSAFE_NOT_SORTABLE,
  PARALLEL_UNSAFE_NOT_UNF,
  PARALLEL_UNSAFE_DYNAMIC_FIELD,
} ParallelSafetyStatus;

ParallelSafetyStatus checkFieldParallelSafety(const FieldSpec *fs) {
  if (!FieldSpec_IsSortable(fs)) {
    return PARALLEL_UNSAFE_NOT_SORTABLE;
  }
  if (fs->options & FieldSpec_Dynamic) {
    return PARALLEL_UNSAFE_DYNAMIC_FIELD;
  }
  // Numeric fields don't need UNF - values are stored as-is
  if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
    return PARALLEL_SAFE;
  }
  // Text and TAG fields need UNF to preserve original value
  if (!FieldSpec_IsUnf(fs)) {
    return PARALLEL_UNSAFE_NOT_UNF;
  }
  return PARALLEL_SAFE;
}
```

### Phase 2: Integration Points

1. **FT.SEARCH**: Validate fields in `RETURN` clause
2. **FT.AGGREGATE**: Validate fields in `LOAD` clause  
3. **FT.HYBRID**: Validate fields in aggregation parameters

### Phase 3: Request Flag

Add a new request flag:
```c
#define QEXEC_F_ENFORCE_PARALLEL 0x80000000
```

### Fields to Validate

The validation should check:
1. Explicit `RETURN` fields (FT.SEARCH)
2. Explicit `LOAD` fields (FT.AGGREGATE)
3. `SORTBY` fields (if returning sort keys)
4. Fields used in `HIGHLIGHT`/`SUMMARIZE` (always require keyspace - should fail)
5. Implicit field loading (when no explicit RETURN and not NOCONTENT)

### Special Cases

1. **NOCONTENT / RETURN 0**: No field loading required - always parallel-safe
2. **LOAD ***: Loads all fields - must validate entire schema
3. **HIGHLIGHT/SUMMARIZE**: Always requires keyspace access - incompatible with ENFORCE_PARALLEL
4. **JSON indexes**: JSON fields with SORTABLE are automatically UNF
5. **Dynamic fields**: Cannot be SORTABLE, always require keyspace access

## Example Usage

### Schema Creation
```redis
FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA
  title TEXT SORTABLE UNF
  category TAG SORTABLE UNF
  price NUMERIC SORTABLE
  description TEXT
```

### Parallel-Safe Queries
```redis
# All returned fields are SORTABLE UNF or NUMERIC SORTABLE
FT.SEARCH idx "@category:{electronics}" ENFORCE_PARALLEL RETURN 3 title category price

# No content - no field loading needed
FT.SEARCH idx "@category:{electronics}" ENFORCE_PARALLEL NOCONTENT

# Aggregate with parallel-safe LOAD
FT.AGGREGATE idx "*" ENFORCE_PARALLEL LOAD 2 @title @price GROUPBY 1 @category
```

### Queries That Will Fail Validation
```redis
# 'description' is not SORTABLE
FT.SEARCH idx "*" ENFORCE_PARALLEL RETURN 1 description
# Error: ENFORCE_PARALLEL: Field 'description' requires keyspace access...

# TAG without UNF (if schema had: category TAG SORTABLE)
FT.SEARCH idx "*" ENFORCE_PARALLEL RETURN 1 category  
# Error: ENFORCE_PARALLEL: TAG field 'category' is SORTABLE but not UNF...

# HIGHLIGHT always requires keyspace
FT.SEARCH idx "hello" ENFORCE_PARALLEL HIGHLIGHT
# Error: ENFORCE_PARALLEL: HIGHLIGHT requires keyspace access...
```

## Behavioral Considerations

### When WORKERS = 0

If `ENFORCE_PARALLEL` is specified but `WORKERS` is 0 (no worker threads):
- **Option A**: Ignore the flag silently (query runs on main thread anyway)
- **Option B**: Return a warning but execute the query
- **Option C**: Return an error indicating workers are not enabled

**Recommendation**: Option A - the flag becomes a no-op when workers are disabled, maintaining query compatibility across configurations.

### Interaction with Cursors

For cursor-based queries (`WITHCURSOR`):
- Validation happens once at query start
- Subsequent `FT.CURSOR READ` calls inherit the parallel guarantee
- If index schema changes between cursor reads, behavior is undefined (existing limitation)

### Coordinator (Cluster) Mode

In cluster mode:
- Each shard validates independently
- If any shard fails validation, the entire query fails
- Error message should indicate which shard(s) failed

## Performance Implications

### Benefits
- Guaranteed parallel execution when validation passes
- Early failure for queries that would serialize
- Enables users to optimize schemas for parallel workloads

### Overhead
- Additional validation step during query parsing
- Negligible for typical queries (O(n) where n = number of returned fields)

## Migration Path

### For Existing Users

1. **Audit Schema**: Identify fields used in RETURN/LOAD clauses
2. **Update Schema**: Add `SORTABLE UNF` to frequently returned fields
3. **Reindex**: Schema changes require reindexing
4. **Enable**: Add `ENFORCE_PARALLEL` to queries

### Schema Design Guidelines

For maximum parallelism:
```redis
# Good: All returned fields are parallel-safe
FT.CREATE idx ON HASH SCHEMA
  title TEXT SORTABLE UNF        # Text with original value preserved
  tags TAG SORTABLE UNF          # Tags with original value preserved
  price NUMERIC SORTABLE         # Numeric (no UNF needed)
  vector VECTOR FLAT ...         # Vectors don't need SORTABLE for queries

# Searchable but not returned fields don't need SORTABLE
  body TEXT                      # Full-text search only
```

## Alternatives Considered

### 1. Automatic Parallel Mode Detection

Instead of explicit `ENFORCE_PARALLEL`, automatically detect and optimize:
- **Pros**: No user action required
- **Cons**: Silent performance degradation when fields aren't parallel-safe; harder to debug

### 2. Index-Level Parallel Mode

Declare entire index as parallel-only:
```redis
FT.CREATE idx ON HASH PARALLEL_ONLY SCHEMA ...
```
- **Pros**: Single configuration point
- **Cons**: Too restrictive; prevents mixed workloads

### 3. Warning Instead of Error

Return results with a warning header when parallel execution isn't possible:
- **Pros**: Queries still succeed
- **Cons**: Easy to miss; doesn't enforce behavior

## Future Enhancements

### 1. EXPLAIN PARALLEL

Add diagnostic command to check query parallel-safety:
```redis
FT.EXPLAIN idx "@category:{electronics}" RETURN 2 title description PARALLEL
# Returns:
# - title: PARALLEL_SAFE (SORTABLE UNF)
# - description: REQUIRES_KEYSPACE (not SORTABLE)
```

### 2. Index Info Enhancement

Extend `FT.INFO` to show parallel-safety status per field:
```redis
FT.INFO idx
# ...
# fields:
#   - name: title
#     type: TEXT
#     SORTABLE: true
#     UNF: true
#     parallel_safe: true
```

### 3. Metrics

Add metrics for parallel vs serialized query execution:
- `parallel_queries_total`
- `serialized_queries_total`
- `enforce_parallel_failures_total`

## Testing Strategy

### Unit Tests
- Validation logic for each field type
- Error message formatting
- Flag parsing

### Integration Tests
- End-to-end queries with ENFORCE_PARALLEL
- Cluster mode validation
- Cursor interaction
- Schema edge cases (dynamic fields, JSON indexes)

### Performance Tests
- Verify no GIL acquisition when ENFORCE_PARALLEL succeeds
- Measure validation overhead
- Compare throughput with/without parallel execution

## Appendix: Field Type Parallel-Safety Matrix

| Field Type | SORTABLE | UNF | Parallel-Safe | Notes |
|------------|----------|-----|---------------|-------|
| TEXT | No | - | ❌ | Requires keyspace |
| TEXT | Yes | No | ❌ | Sorting vector has normalized value |
| TEXT | Yes | Yes | ✅ | Original value in sorting vector |
| TAG | No | - | ❌ | Requires keyspace |
| TAG | Yes | No | ❌ | Sorting vector has normalized value |
| TAG | Yes | Yes | ✅ | Original value in sorting vector |
| NUMERIC | No | - | ❌ | Requires keyspace |
| NUMERIC | Yes | - | ✅ | Numeric values stored as-is |
| GEO | No | - | ❌ | Requires keyspace |
| GEO | Yes | - | ✅ | Coordinates stored as-is |
| VECTOR | - | - | N/A | Not typically returned |
| GEOMETRY | - | - | ❌ | Cannot be SORTABLE |
| JSON (any) | Yes | Auto | ✅ | JSON SORTABLE implies UNF |

