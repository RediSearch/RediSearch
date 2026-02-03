# FT.HYBRID - Hybrid Search Feature

## Status
Current status: **Completed**  
Last updated: 2026-02-01

## Overview
FT.HYBRID is a Redis command that enables hybrid search by combining multiple search modalities (text search and vector similarity search) into a single unified query. The feature allows users to leverage both traditional full-text search and modern vector-based semantic search, merging results using sophisticated scoring algorithms.

The command executes two parallel subqueries:
1. **SEARCH**: Traditional full-text search with filters
2. **VSIM**: Vector similarity search (KNN or range-based)

Results are merged using configurable fusion algorithms (RRF or LINEAR) to produce a unified ranked result set.

### Important Distinction: FT.HYBRID vs Hybrid Iterator

**This spec covers the FT.HYBRID command**, not the Hybrid Iterator:

- **FT.HYBRID Command** (`src/hybrid/`): High-level command that runs two separate queries (text + vector) and merges their results
  - Example: `FT.HYBRID idx SEARCH "shoes" VSIM @vec $BLOB COMBINE RRF`
  - Merges results from independent SEARCH and VSIM queries
  - Uses RRF or LINEAR scoring to combine results

- **Hybrid Iterator** (`src/iterators/hybrid_reader.c`): Low-level iterator for filtered vector search within FT.SEARCH
  - Example: `FT.SEARCH idx "@vec:[VECTOR ... KNN 10] @category:{shoes}"`
  - Combines vector KNN with filters in a single query
  - Used internally by FT.SEARCH, not exposed as a separate command
  - Tested in `tests/cpptests/benchmark_vecsim_hybrid_queries.cpp`

## Requirements

### Functional Requirements
- Support two subqueries: SEARCH (text) and VSIM (vector)
- Provide multiple result fusion methods:
  - **RRF (Reciprocal Rank Fusion)**: Rank-based fusion with configurable constant
  - **LINEAR**: Weighted score combination with configurable alpha/beta weights
- Support all standard aggregation pipeline operations (LOAD, APPLY, FILTER, SORTBY, LIMIT, etc.)
- Enable score visibility through YIELD_SCORE_AS for individual and fused scores
- Support query parameters via PARAMS for dynamic queries
- Provide cursor support for paginated results
- Work in both standalone and cluster (coordinator/shard) modes

### Non-Functional Requirements
- **Performance**: Execute subqueries in parallel where possible
- **Compatibility**: Maintain backward compatibility with existing search features
- **Scalability**: Support distributed execution in cluster environments
- **Debuggability**: Provide debug mechanisms for testing timeout scenarios

### Constraints
- Currently limited to exactly 2 subqueries (SEARCH + VSIM)
- Vector search requires pre-indexed vector fields (FLAT or HNSW)
- Scoring algorithms must handle cases where documents appear in only one subquery

## Design

### Architecture
```
FT.HYBRID Command
    ↓
Parse & Validate
    ↓
Create HybridRequest
    ├─→ SEARCH Subquery (AREQ)
    └─→ VSIM Subquery (AREQ)
    ↓
Execute Subqueries (parallel)
    ↓
Tail Pipeline (Merger)
    ├─→ Hybrid Scorer (RRF/LINEAR)
    ├─→ Aggregation Processors
    └─→ Result Formatter
    ↓
Return Results
```

### Key Components

#### 1. Command Parsing (`src/hybrid/parse_hybrid.c`)
- Parses FT.HYBRID command syntax
- Creates separate AREQ structures for SEARCH and VSIM
- Validates subquery compatibility
- Builds HybridPipelineParams with scoring context

#### 2. Hybrid Request (`src/hybrid/hybrid_request.c/h`)
- Manages lifecycle of hybrid search execution
- Coordinates multiple subqueries
- Handles error aggregation from subqueries
- Manages timing and profiling

#### 3. Scoring Algorithms (`src/hybrid/hybrid_scoring.c/h`)
- **RRF (Reciprocal Rank Fusion)**: `score = Σ(1 / (constant + rank_i))`
  - Default constant: 60
  - Configurable window size (default: 20)
- **LINEAR**: `score = Σ(weight_i * score_i)`
  - Default weights: ALPHA=0.5, BETA=0.5
  - Configurable per-source weights

#### 4. Pipeline Construction (`src/pipeline/pipeline_construction.c`)
- Builds depletion pipelines for each subquery
- Constructs tail pipeline for result merging
- Integrates hybrid scorer as result processor
- Adds aggregation processors (LOAD, APPLY, FILTER, etc.)

#### 5. Execution (`src/hybrid/hybrid_exec.c`)
- Handles command entry point
- Manages background execution via blocked clients
- Coordinates cursor creation for pagination
- Handles both internal (shard) and external (coordinator) modes

### Command Syntax

```
FT.HYBRID <index>
  SEARCH <query> [SCORER <scorer>] [YIELD_SCORE_AS <field>]
  VSIM <@field> <vector> [KNN <k_params>] [FILTER <filter>] [YIELD_SCORE_AS <field>]
  [COMBINE <method> [params] [YIELD_SCORE_AS <field>]]
  [LOAD <fields>]
  [APPLY <expression> AS <field>]
  [FILTER <filter_expression>]
  [SORTBY <field> [ASC|DESC]]
  [LIMIT <offset> <count>]
  [PARAMS <nargs> <name> <value> ...]
  [WITHCURSOR]
  [TIMEOUT <milliseconds>]
```

#### COMBINE Methods

**RRF (Reciprocal Rank Fusion)**:
```
COMBINE RRF [<param_count>] [CONSTANT <k>] [WINDOW <size>]
```

**LINEAR (Weighted Score)**:
```
COMBINE LINEAR [<param_count>] [ALPHA <weight>] [BETA <weight>] [WINDOW <size>]
```

## Implementation Plan

- [x] Core hybrid request structure
- [x] Command parsing for SEARCH and VSIM subqueries
- [x] RRF scoring algorithm implementation
- [x] LINEAR scoring algorithm implementation
- [x] Pipeline construction for hybrid search
- [x] Result merging and deduplication
- [x] YIELD_SCORE_AS support for individual and fused scores
- [x] PARAMS support for dynamic queries
- [x] Cursor support for pagination
- [x] Cluster mode support (coordinator/shard)
- [x] Debug mechanisms for timeout testing
- [x] Comprehensive test coverage
- [x] Documentation and examples

## Dependencies

### External Dependencies
- Redis Module API
- VecSim library (for vector operations)

### Internal Dependencies
- Aggregation pipeline framework (`src/aggregate/`)
- Query parsing infrastructure (`src/query_parser/`)
- Iterator framework (`src/iterators/`)
- Result processor system (`src/result_processor/`)
- Cursor management (`src/cursor.c`)

### Module Structure
```
src/hybrid/
├── hybrid_exec.c/h           # Command handler and execution
├── hybrid_request.c/h        # Request lifecycle management
├── hybrid_scoring.c/h        # Scoring algorithms (RRF, LINEAR)
├── hybrid_search_result.c/h  # Result representation
├── hybrid_lookup_context.c/h # Field lookup coordination
├── hybrid_debug.c/h          # Debug mechanisms
├── parse_hybrid.c/h          # Command parsing
└── parse/
    ├── hybrid_callbacks.c/h      # Parsing callbacks
    ├── hybrid_combine.c          # COMBINE clause parsing
    └── hybrid_optional_args.c/h  # Optional argument parsing
```

## Testing Strategy

### Unit Tests (C/C++)
- Located in `tests/cpptests/`
- Test scoring algorithm correctness (RRF, LINEAR)
- Test pipeline construction and execution
- Test error handling and edge cases

**Note**: `benchmark_vecsim_hybrid_queries.cpp` tests the **Hybrid Iterator** (vector search with filters within FT.SEARCH), not the FT.HYBRID command. The Hybrid Iterator is a lower-level component used for filtered vector search.

### Integration Tests (Python)
- Located in `tests/pytests/`
- **test_hybrid.py**: Core hybrid search functionality
  - KNN + text search combinations
  - PARAMS support
  - Error handling
- **test_hybrid_linear.py**: LINEAR scoring tests
  - Default weights (0.5/0.5)
  - Custom ALPHA/BETA weights
  - Score calculation verification
- **test_hybrid_rrf.py**: RRF scoring tests (if exists)
- **test_hybrid_yield.py**: YIELD_SCORE_AS functionality
  - Individual score visibility
  - Fused score visibility
- **test_hybrid_load.py**: LOAD functionality
  - Field loading behavior
  - LOAD * support
- **test_hybrid_apply_filter.py**: APPLY and FILTER operations
  - Expression evaluation on hybrid results
  - Filtering based on fused scores

### Performance Tests
- Benchmark hybrid query execution times
- Compare with separate SEARCH + VSIM queries
- Test scalability with large result sets

## Known Issues

### Issue #1: Depleters Use Separate Thread Pool Instead of Main Worker Pool

**Status**: Open
**Priority**: Medium
**Affected Component**: `src/result_processor.c` (RPSafeDepleter)

#### Description
When `WORKERS > 0`, the FT.HYBRID depletion mechanism uses a dedicated `depleterPool` thread pool (fixed size of 4 threads) instead of the main `_workers_thpool` (configurable via `WORKERS` config). This creates several issues:

1. **Resource Inefficiency**: Maintains a separate pool of 4 threads that cannot be shared with other operations
2. **Configuration Inconsistency**: Users cannot control depleter thread count via `WORKERS` configuration
3. **Dual Thread Pool Architecture**: Adds complexity by maintaining two separate thread pools

**Note**: When `WORKERS=0`, async depleters are NOT used at all - queries run synchronously without background depletion.

#### Current Implementation
- **Location**: `src/result_processor.c:1541-1545` (`RPSafeDepleter_StartDepletionThread`)
- **Thread Pool**: `depleterPool` (global, defined in `src/module.c:111`)
- **Pool Size**: Fixed at 4 threads (`DEPLETER_POOL_SIZE` in `src/module-init/module-init.c:38`)
- **Initialization**: `src/module-init/module-init.c:173`

```c
// Current implementation uses dedicated pool
static inline void RPSafeDepleter_StartDepletionThread(RPSafeDepleter *self) {
  int rc = redisearch_thpool_add_work(depleterPool, RPSafeDepleter_Deplete,
                                      self, THPOOL_PRIORITY_HIGH);
  RS_ASSERT_ALWAYS(rc == 0);
}
```

#### How It Works Currently

**When `WORKERS > 0`** (multi-threaded execution):
- `RunInThread()` returns true (line 188 in `src/aggregate/aggregate.h`)
- `HybridRequest_BuildPipelineAndExecute()` takes the background execution path (line 538 in `src/hybrid/hybrid_exec.c`)
- Calls `buildPipelineAndExecute(..., depleteInBackground=true)` (line 700)
- `HybridRequest_BuildDepletionPipeline()` creates `RPSafeDepleter` instances (line 53-59 in `src/hybrid/hybrid_request.c`)
- Depleters use the dedicated `depleterPool` (4 threads)

**When `WORKERS=0`** (single-threaded execution):
- `RunInThread()` returns false
- `HybridRequest_BuildPipelineAndExecute()` takes the synchronous path (line 562)
- Calls `buildPipelineAndExecute(..., depleteInBackground=false)`
- `HybridRequest_BuildDepletionPipeline()` does NOT create `RPSafeDepleter` instances
- No async depleters are used; queries run synchronously

#### Why Separate Pool Was Created

The original technical issue: **Reentrancy Problem**

When `WORKERS > 0`, the hybrid query itself runs in a worker thread from `_workers_thpool`. During execution, it needs to submit depleter tasks to run in the background. However, submitting a new task to the same thread pool from within a running task can cause technical issues:

1. **Deadlock Risk**: If the pool is at capacity, a task trying to submit another task to the same pool could deadlock
2. **Thread Pool Reentrancy**: Thread pool implementations may not support tasks submitting new tasks to the same pool
3. **Resource Contention**: Tasks competing for slots in the same pool they're already occupying

**Solution**: Create a separate `depleterPool` (4 threads) dedicated to depletion tasks, avoiding the reentrancy problem.

From code comments in `src/result_processor.c:1602-1604`:
> A dedicated thread-pool `depleterPool` is used, such that there are no
> contentions with the `_workers_thpool` thread-pool, such as adding a new job
> to its queue after `WORKERS` has been set to `0`.

(Note: The comment mentions `WORKERS=0`, but the actual issue is the reentrancy problem when running inside `_workers_thpool`.)

#### Desired Behavior
Ideally, use a unified thread pool architecture to:
- Allow unified thread pool management
- Respect `WORKERS` configuration
- Reduce overall thread count and resource usage (eliminate fixed 4-thread pool)
- Simplify architecture

#### Challenges
1. **Reentrancy Problem**: The main challenge - hybrid queries run in `_workers_thpool`, and need to submit depleter tasks without causing deadlock or reentrancy issues
2. **Thread Pool Capacity**: Ensure enough threads available for both query execution and depletion tasks
3. **Thread Pool Implementation**: Verify that `_workers_thpool` implementation supports tasks submitting new tasks to the same pool
4. **Backward Compatibility**: Ensure existing behavior is preserved when `WORKERS > 0`
5. **Performance Testing**: Verify that any solution doesn't degrade performance

#### Potential Solutions
1. **Verify Thread Pool Supports Reentrancy**: Check if `redisearch_thpool` implementation already handles tasks submitting new tasks
   - If supported, simply switch `RPSafeDepleter_StartDepletionThread()` to use `_workers_thpool`
   - May need to ensure `WORKERS` is set high enough to avoid deadlock
2. **Separate Execution Phases**: Restructure so query execution completes before submitting depleter tasks
   - Avoids reentrancy by not submitting from within a running task
   - May require significant refactoring
3. **Keep Separate Pool**: Accept the current architecture as a valid solution to the reentrancy problem
   - Document that `depleterPool` size should scale with workload
   - Consider making `DEPLETER_POOL_SIZE` configurable
4. **Hybrid Approach**: Use main pool when query runs in main thread, use depleter pool when query runs in worker thread
   - Adds complexity but maximizes resource utilization

#### Related Code
- Main worker pool: `src/util/workers.c` (`_workers_thpool`)
- Depleter pool creation: `src/module-init/module-init.c:173`
- Depleter usage: `src/result_processor.c:1541`, `src/result_processor.c:1709`
- Hybrid depletion pipeline: `src/hybrid/hybrid_request.c:24-67`

## Backlog Tasks

### Performance Optimization

| Task | Priority | Notes |
|------|----------|-------|
| Ensure limits are correctly propagated to subqueries | Medium | Performance optimization for result limiting |
| Add normalization step to FT.SEARCH result reducer for BM25STD.NORM scorer | Medium | Cluster optimization |
| Timeouts in cluster due to late cursor ID retrieval | Medium | Potential performance optimization |
| Implement Merge Sort In Coordinator | Medium | Performance optimization for result merging |

### Feature Enhancements

| Task | Priority | Notes |
|------|----------|-------|
| Add unit tests for Param_DictXXX functions | Low | Test coverage improvement |
| Improve Hybrid function source documentation | Low | Documentation enhancement |
| Define and use AREQ_IsCursor() | Low | Code quality improvement |
| Providing YIELD_DISTANCE_AS to Range in vsim search config fails in hybrid command | Medium | Bug fix |
| Support EXPLAINCLI in Hybrid Query | Medium | Add explain support |
| Support Debug Hybrid command to run in workers thread pool when it exists | Medium | Related to Issue #1 (depleter pool) |
| FT.HYBRID: Support SCORER with args values | Medium | Enhance scorer flexibility |
| RRF/Hybrid INFO & metrics | Medium | Add metrics and monitoring |

### Priority Breakdown
- **Medium Priority**: 11 tasks (performance, features, bug fixes)
- **Low Priority**: 3 tasks (tests, documentation, code quality)

## Open Questions

None currently - feature is implemented and stable.

## Decisions Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2024-Q3 | Limit to 2 subqueries (SEARCH + VSIM) | Simplifies initial implementation; can be extended later if needed |
| 2024-Q3 | Use RRF and LINEAR as fusion methods | Industry-standard approaches with proven effectiveness |
| 2024-Q3 | Default RRF constant = 60 | Common value in literature that balances rank contributions |
| 2024-Q3 | Default window = 20 | Reasonable default for most use cases |
| 2024-Q3 | Support YIELD_SCORE_AS at multiple levels | Enables debugging and custom scoring logic |
| 2024-Q4 | Add debug timeout mechanisms | Essential for testing timeout handling in CI/CD |

## References

### Code Files
- Command definition: `src/commands.h` (RS_HYBRID_CMD)
- Main implementation: `src/hybrid/` directory
- Tests: `tests/pytests/test_hybrid*.py`
- Distributed execution: `src/coord/hybrid/dist_hybrid.c`

### Related Features
- FT.SEARCH: Traditional full-text search
- Vector similarity search (KNN)
- Aggregation pipeline
- Cursor support

### External Resources
- RRF Paper: "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods"
- Vector search best practices
- Redis Module API documentation

