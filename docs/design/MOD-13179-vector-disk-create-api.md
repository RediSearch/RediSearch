# MOD-13179: Implement API on Search side - field creation

**Jira**: MOD-13179
**Design Doc**: Vector Search on Disk - MVP1 Design (Confluence DX space)

## Related Branches

| Repository | Branch |
|------------|--------|
| RediSearchDisk | `vector-disk-create-api` |
| RediSearch (deps/RediSearch) | `vector-disk-create-api` |
| VectorSimilarity (deps/RediSearch/deps/VectorSimilarity) | `vector-disk-create-api` |

## Overview

Add disk-mode validation to `FT.CREATE` VECTOR field parsing. Reject unsupported features (FLAT, non-FLOAT32, multi-value) and enforce mandatory parameters (M, EF_CONSTRUCTION, EF_RUNTIME, RERANK) in disk mode.

**Background**: See `vecsim_disk/STRUCTURE.md` for architecture. Disk mode is detected via `SearchDisk_IsEnabledForValidation()` in `deps/RediSearch/src/spec.c`.

---

## Requirements

### 1. Supported Parameters (Must Work)

| Parameter | Requirement | Notes |
|-----------|-------------|-------|
| `DIM <n>` | Mandatory | Dimension of vectors |
| `TYPE FLOAT32` | Mandatory | Only supported type in disk mode |
| `DISTANCE_METRIC <COSINE\|L2\|IP>` | Mandatory | Distance metric |
| `M <m>` | Mandatory (no default in disk mode) | HNSW graph connectivity |
| `EF_CONSTRUCTION <num>` | Mandatory (no default in disk mode) | Construction-time search depth |
| `EF_RUNTIME <num>` | Mandatory (no default in disk mode) | Query-time search depth |
| `RERANK` | Mandatory in MVP (boolean flag) | Must be present; sets `rerank=true`. Absence fails with error. |

**Note on RERANK**: In MVP, rerank-skipping is not implemented, so RERANK is mandatory. Internally, if present → `rerank=true`, if absent → `rerank=false`, but FT.CREATE will fail if RERANK is not specified. This allows future relaxation when rerank-skipping is implemented.

**Test Case:** Create index with all mandatory parameters including RERANK → Success
```
FT.CREATE idx ON HASH SCHEMA vec VECTOR HNSW 14
  TYPE FLOAT32 DIM 128 DISTANCE_METRIC L2
  M 16 EF_CONSTRUCTION 200 EF_RUNTIME 10 RERANK
```

**Test Case:** Create index without RERANK → Error
```
FT.CREATE idx ON HASH SCHEMA vec VECTOR HNSW 12
  TYPE FLOAT32 DIM 128 DISTANCE_METRIC L2
  M 16 EF_CONSTRUCTION 200 EF_RUNTIME 10
# Expected error: "Disk HNSW index requires RERANK parameter"
```

### 2. Reject Unsupported Features (Must Fail)

| Feature | Expected Error |
|---------|----------------|
| Algorithm other than HNSW (e.g., FLAT) | "Disk index does not support FLAT algorithm" |
| Type other than FLOAT32 | "Disk index does not support FLOAT64 vector type" (or specific type) |
| Multi-value vectors | "Disk index does not support multi-value vectors" |

**Note**: Error messages follow existing disk index pattern: `"Disk index does not support <FEATURE>"`

**Test Cases:**
```
# FLAT algorithm → Error
FT.CREATE idx ON HASH SCHEMA vec VECTOR FLAT 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2

# FLOAT64 type → Error
FT.CREATE idx ON HASH SCHEMA vec VECTOR HNSW 6 TYPE FLOAT64 DIM 4 DISTANCE_METRIC L2
```

### 3. Enforce Mandatory Parameters (Must Fail if Missing)

In disk mode, HNSW parameters must be explicitly provided (no defaults):

| Missing Parameter | Expected Error |
|-------------------|----------------|
| M | "Disk HNSW index requires M parameter" |
| EF_CONSTRUCTION | "Disk HNSW index requires EF_CONSTRUCTION parameter" |
| EF_RUNTIME | "Disk HNSW index requires EF_RUNTIME parameter" |
| RERANK | "Disk HNSW index requires RERANK parameter" |

**Note**: Error messages follow pattern: `"Disk HNSW index requires <PARAM> parameter"`

**Test Cases:**
```
# Missing M → Error
FT.CREATE idx ON HASH SCHEMA vec VECTOR HNSW 10
  TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2 EF_CONSTRUCTION 200 EF_RUNTIME 10 RERANK

# Missing EF_RUNTIME → Error
FT.CREATE idx ON HASH SCHEMA vec VECTOR HNSW 10
  TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2 M 16 EF_CONSTRUCTION 200 RERANK

# Missing RERANK → Error
FT.CREATE idx ON HASH SCHEMA vec VECTOR HNSW 12
  TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2 M 16 EF_CONSTRUCTION 200 EF_RUNTIME 10
```

### 4. Verify Index Creation

After successful creation:
- `FT.INFO` shows the index with correct parameters
- `_FT.DEBUG VECSIM_INFO idx vec` shows `IS_DISK=1`

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `flow-tests/test_vecsim_disk.py` | Update existing tests, add new rejection tests |
| `deps/RediSearch/deps/VectorSimilarity/src/VecSim/vec_sim_common.h` | Add `rerank` field to `VecSimHNSWDiskParams` |
| `deps/RediSearch/src/spec.c` | Add RERANK parsing, disk-mode validation |

### Ordered Steps

#### Step 1: Update Existing Flow Tests
Update existing tests to include M, EF_CONSTRUCTION, EF_RUNTIME, RERANK before adding validation (prevents breakage).

Tests to update:
- `test_drop_index` - add M, EF_CONSTRUCTION, EF_RUNTIME, RERANK
- `test_create_hnsw_disk_index` - add EF_RUNTIME, RERANK
- `test_add_vectors_and_knn_query` - add EF_RUNTIME, RERANK

#### Step 2: Add New Test Cases (Initially Failing)
Add tests for rejection scenarios. These will fail until Step 4 is complete.

New tests:
- `test_create_disk_index_rejects_flat_algorithm`
- `test_create_disk_index_rejects_float64`
- `test_create_disk_index_rejects_multi_value`
- `test_create_disk_index_requires_m_parameter`
- `test_create_disk_index_requires_ef_construction`
- `test_create_disk_index_requires_ef_runtime`
- `test_create_disk_index_requires_rerank`

#### Step 3: Add RERANK Field and Parsing
1. Add `bool rerank` to `VecSimHNSWDiskParams` struct in `vec_sim_common.h`
2. In `parseVectorField_hnsw()`: add tracking variables (`mandM`, `mandEfConstruction`, `mandEfRuntime`, `rerank`)
3. In parsing loop: add `AC_AdvanceIfMatch(ac, "RERANK")` handling (boolean flag, no value)
4. Populate `rerank` in diskParams

#### Step 4: Add Disk-Mode Validation
In `parseVectorField()`:
- Reject FLAT algorithm when `isSpecOnDiskForValidation(sp)` is true

In `parseVectorField_hnsw()` after parsing:
- Reject non-FLOAT32 types in disk mode
- Reject multi-value vectors in disk mode
- Enforce M, EF_CONSTRUCTION, EF_RUNTIME, RERANK as mandatory in disk mode

#### Step 5: Verify All Tests Pass
Run `./build.sh test-flow --redis-lib-path ~/workspace/redis-private/src --test test_vecsim_disk.py`

---

## Code Reference

### RERANK parsing (boolean flag pattern):
```c
} else if (AC_AdvanceIfMatch(ac, "RERANK")) {
    rerank = true;
}
```

### Disk-mode validation pattern:
```c
if (isSpecOnDiskForValidation(sp)) {
    if (!mandM) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk HNSW index requires M parameter");
        return 0;
    }
    // ... similar for other mandatory params
}
```

### FLAT rejection pattern:
```c
if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_BF)) {
    if (isSpecOnDiskForValidation(sp)) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk index does not support FLAT algorithm");
        rm_free(logCtx);
        return 0;
    }
    // ... existing FLAT handling ...
}
```

---

## How to Run Tests

```bash
# Flow tests (primary)
./build.sh test-flow --redis-lib-path ~/workspace/redis-private/src --test test_vecsim_disk.py

# C++ unit tests (won't be affected - bypass RediSearch parsing)
./build.sh test-vecsim
```

---

## Key Design Decisions

1. **RERANK is mandatory in MVP**: Boolean flag, but FT.CREATE fails if absent. Future: remove mandatory check when rerank-skipping is implemented.
2. **Disk mode detection**: Use `SearchDisk_IsEnabledForValidation()`
3. **Error message patterns**:
   - Unsupported features: `"Disk index does not support <FEATURE>"`
   - Mandatory params: `"Disk HNSW index requires <PARAM> parameter"`
4. **RAM mode unchanged**: Defaults still apply in RAM mode.

---

## Appendix: Current State

### What's Already Working
- Basic HNSW disk index creation (`IS_DISK=1` verified)
- Parsing of TYPE, DIM, DISTANCE_METRIC, M, EF_CONSTRUCTION, EF_RUNTIME (with defaults)
- Disk mode detection via `SearchDisk_IsEnabledForValidation()`

### What's Missing
- FLAT algorithm rejection in disk mode
- Non-FLOAT32 type rejection in disk mode
- Multi-value vector rejection in disk mode
- Mandatory parameter enforcement (M, EF_CONSTRUCTION, EF_RUNTIME)
- RERANK parameter (doesn't exist yet)

---

## Implementation Progress (In-Progress)

### Completed Changes (to be stashed and reapplied)

#### 1. `flow-tests/test_vecsim_disk.py`
**Status: COMPLETE**

Updated existing tests to include all mandatory parameters:
- `test_create_hnsw_disk_index` - Updated parameter count from 10 to 14, added EF_RUNTIME and RERANK
- `test_add_vectors_and_knn_query` - Same updates
- `test_delete_vector_and_requery` - Same updates (skipped test)
- `test_drop_index` - Updated from 6 to 14 parameters, added M, EF_CONSTRUCTION, EF_RUNTIME, RERANK

Added new rejection test cases:
- `test_create_disk_index_rejects_flat_algorithm`
- `test_create_disk_index_rejects_float64`
- `test_create_disk_index_requires_m_parameter`
- `test_create_disk_index_requires_ef_construction`
- `test_create_disk_index_requires_ef_runtime`
- `test_create_disk_index_requires_rerank`

**Note**: Multi-value rejection test was NOT added because multi-value vectors require JSON indexes, and JSON is already rejected for Flex/disk mode (only HASH is supported).

#### 2. `deps/RediSearch/deps/VectorSimilarity/src/VecSim/vec_sim_common.h`
**Status: COMPLETE**

Added `rerank` field to `VecSimDiskContext` struct (line ~249):
```c
typedef struct {
    void *storage; // Opaque pointer to disk storage
    const char *indexName;
    size_t indexNameLen;
    bool rerank; // Whether to enable reranking for disk-based HNSW
} VecSimDiskContext;
```

#### 3. `deps/RediSearch/src/spec.c`
**Status: COMPLETE**

Changes made:

1. **Forward declaration** (line ~99):
```c
// Forward declaration for disk validation
inline static bool isSpecOnDiskForValidation(const IndexSpec *sp);
```

2. **Updated `parseVectorField_hnsw` signature** (line ~749):
```c
static int parseVectorField_hnsw(IndexSpec *sp, FieldSpec *fs, VecSimParams *params, ArgsCursor *ac, QueryError *status, bool *rerank)
```

3. **Added tracking variables for disk-mode mandatory params**:
```c
bool mandM = false;
bool mandEfConstruction = false;
bool mandEfRuntime = false;
*rerank = false;
```

4. **Modified parameter count handling** to support boolean flags (RERANK counts as 1 arg, not 2):
```c
size_t expPairs = expNumParam / 2;
size_t remainder = expNumParam % 2;
while ((expPairs + remainder) > (numParam + boolFlags) && !AC_IsAtEnd(ac)) {
```

5. **Added RERANK parsing** in the parsing loop:
```c
} else if (AC_AdvanceIfMatch(ac, "RERANK")) {
    *rerank = true;
    boolFlags++;
    continue;  // Don't increment numParam for boolean flags
}
```

6. **Added disk-mode validation** after mandatory param checks (line ~850):
```c
if (isSpecOnDiskForValidation(sp)) {
    if (params->algoParams.hnswParams.type != VecSimType_FLOAT32) {
        const char *typeName = VecSimType_ToString(params->algoParams.hnswParams.type);
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk index does not support %s vector type", typeName);
        return 0;
    }
    if (params->algoParams.hnswParams.multi) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk index does not support multi-value vectors");
        return 0;
    }
    if (!mandM) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk HNSW index requires M parameter");
        return 0;
    }
    if (!mandEfConstruction) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk HNSW index requires EF_CONSTRUCTION parameter");
        return 0;
    }
    if (!mandEfRuntime) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk HNSW index requires EF_RUNTIME parameter");
        return 0;
    }
    if (!*rerank) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk HNSW index requires RERANK parameter");
        return 0;
    }
}
```

7. **Added FLAT algorithm rejection** (line ~1217):
```c
if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_BF)) {
    // Disk mode does not support FLAT algorithm
    if (isSpecOnDiskForValidation(sp)) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
            "Disk index does not support FLAT algorithm");
        rm_free(logCtx);
        return 0;
    }
    // ... existing FLAT handling ...
}
```

8. **Updated call site** to pass `sp` and handle `rerank` (line ~1237):
```c
bool rerank = false;
result = parseVectorField_hnsw(sp, fs, params, ac, status, &rerank);
// Build disk params if disk mode is enabled
if (result && sp->diskSpec) {
    size_t nameLen;
    const char *namePtr = HiddenString_GetUnsafe(fs->fieldName, &nameLen);
    fs->vectorOpts.diskCtx = (VecSimDiskContext){
        .storage = sp->diskSpec,
        .indexName = rm_strndup(namePtr, nameLen),
        .indexNameLen = nameLen,
        .rerank = rerank,
    };
}
```

### Remaining Work

1. **Build and verify compilation** - Build was failing due to unrelated SpeedB/boost issues
2. **Run flow tests** - `./build.sh test-flow --redis-lib-path ~/workspace/redis-private/src --test test_vecsim_disk.py`
3. **Fix any test failures** - Adjust error messages or logic as needed

