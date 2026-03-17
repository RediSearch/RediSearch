# Reducers & Distribution Strategies

This document covers the reducer system in detail: the framework, all built-in reducers,
how each one accumulates and finalizes, and — critically — how each is decomposed for
distributed (cluster) execution.

## Reducer Framework

### Architecture

```mermaid
graph TB
    subgraph "Reducer Framework"
        FACTORY["ReducerFactory<br/>RDCR_GetFactory(name) → factory_fn"]
        REDUCER["Reducer struct<br/>srckey, dstkey, alloc<br/>NewInstance, Add, Finalize, Free"]
        INSTANCE["Per-Group Instance<br/>void* accumdata<br/>Created by NewInstance()"]
    end

    subgraph "Lifecycle"
        REG["1. Registry Lookup<br/>RDCR_GetFactory('COUNT')"]
        CREATE["2. Factory Call<br/>factory(&options) → Reducer*"]
        BIND["3. Bind to Grouper<br/>Grouper_AddReducer(grp, reducer, dstkey)"]
        PERGRP["4. Per-Group:<br/>NewInstance() → accumdata"]
        ADDROW["5. Per-Row:<br/>Add(reducer, accumdata, srcrow)"]
        FINAL["6. Finalize:<br/>Finalize(reducer, accumdata) → RSValue*"]
    end

    FACTORY --> REG
    REG --> CREATE --> BIND --> PERGRP --> ADDROW
    ADDROW -->|"more rows"| ADDROW
    ADDROW -->|"yield phase"| FINAL
```

### Factory Registry

**File:** `src/aggregate/reducer.c`

The global reducer registry maps reducer names (case-insensitive) to factory functions:

```mermaid
graph LR
    REG["Reducer Registry"]
    REG --> C["COUNT → RDCRCount_New"]
    REG --> S["SUM → RDCRSum_New"]
    REG --> A["AVG → RDCRAvg_New"]
    REG --> MN["MIN → RDCRMin_New"]
    REG --> MX["MAX → RDCRMax_New"]
    REG --> TL["TOLIST → RDCRToList_New"]
    REG --> CD["COUNT_DISTINCT → RDCRCountDistinct_New"]
    REG --> CDI["COUNT_DISTINCTISH → RDCRCountDistinctish_New"]
    REG --> Q["QUANTILE → RDCRQuantile_New"]
    REG --> SD["STDDEV → RDCRStdDev_New"]
    REG --> FV["FIRST_VALUE → RDCRFirstValue_New"]
    REG --> RS["RANDOM_SAMPLE → RDCRRandomSample_New"]
    REG --> H["HLL → RDCRHLL_New"]
    REG --> HS["HLL_SUM → RDCRHLLSum_New"]
```

### ReducerOptions

When a factory is called, it receives a `ReducerOptions` struct that provides context:

```c
typedef struct {
    const char    *name;        // Reducer name as called
    ArgsCursor    *args;        // Arguments (e.g., "@price" for SUM)
    RLookup       *srclookup;   // Upstream lookup (for resolving source keys)
    const RLookupKey ***loadKeys; // Out: keys that need loading
    QueryError    *status;      // Out: error info
    bool           strictPrefix; // Require @ prefix
} ReducerOptions;
```

---

## Built-in Reducers

### COUNT

**File:** `src/aggregate/reducers/count.c`  
**Syntax:** `REDUCE COUNT 0`

Simply counts the number of rows in each group. Takes no arguments.

```mermaid
graph LR
    subgraph "Per-Group State"
        S["counterData { count: size_t }"]
    end

    subgraph "Operations"
        NEW["NewInstance: count = 0"]
        ADD["Add: count++"]
        FIN["Finalize: RSValue_NewNumber(count)"]
    end

    NEW --> ADD -->|"per row"| ADD
    ADD --> FIN
```

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Allocate `counterData`, set `count = 0` |
| `Add` | `count++` (ignores all row data) |
| `Finalize` | Return `RSValue_NewNumber(count)` |

---

### SUM

**File:** `src/aggregate/reducers/sum.c`  
**Syntax:** `REDUCE SUM 1 @field`

Sums numeric values of the specified field across all rows in the group.

```mermaid
graph LR
    subgraph "Per-Group State"
        S["sumCtx { count: size_t, total: double }"]
    end

    subgraph "Operations"
        NEW["NewInstance: count=0, total=0"]
        ADD["Add: v = RLookupRow_Get(srckey, row)<br/>if numeric: total += v, count++"]
        FIN["Finalize: return total<br/>(NAN if count == 0)"]
    end

    NEW --> ADD -->|"per row"| ADD
    ADD --> FIN
```

Non-numeric values are silently skipped. If no valid numeric values are seen, the result
is `NaN`.

---

### AVG

**File:** `src/aggregate/reducers/sum.c`  
**Syntax:** `REDUCE AVG 1 @field`

Computes the average of numeric values. Uses the same `sumCtx` as SUM, but divides in
the finalize step.

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Same as SUM: `count=0, total=0` |
| `Add` | Same as SUM: `total += v, count++` |
| `Finalize` | Return `total / count` (NAN if count == 0) |

---

### MIN / MAX

**File:** `src/aggregate/reducers/minmax.c`  
**Syntax:** `REDUCE MIN 1 @field` / `REDUCE MAX 1 @field`

Tracks the minimum or maximum value seen for the specified field.

```mermaid
graph LR
    subgraph "Per-Group State"
        S["minmaxCtx { val: RSValue* }"]
    end

    subgraph "MIN Operations"
        NEW["NewInstance: val = NULL"]
        ADD["Add: v = get value<br/>if val==NULL or v < val: val = v"]
        FIN["Finalize: return val (or NULL)"]
    end

    NEW --> ADD -->|"per row"| ADD
    ADD --> FIN
```

Values are compared using `RSValue_Cmp()`, which handles mixed types (strings, numbers).

---

### TOLIST

**Syntax:** `REDUCE TOLIST 1 @field`

Collects all values of the specified field into an array.

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Create empty array |
| `Add` | Append value to array |
| `Finalize` | Return array as `RSValue` |

---

### COUNT_DISTINCT

**Syntax:** `REDUCE COUNT_DISTINCT 1 @field`

Counts the number of distinct values using an exact hash set.

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Create hash set |
| `Add` | Hash the value, insert into set |
| `Finalize` | Return set size as number |

> **Note:** This reducer has no distribution function and cannot be split across shards.
> In cluster mode, all data must be sent to the coordinator for grouping.

---

### COUNT_DISTINCTISH

**Syntax:** `REDUCE COUNT_DISTINCTISH 1 @field`

Approximation of COUNT_DISTINCT using HyperLogLog. More memory-efficient and distributable.

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Create HLL structure |
| `Add` | Hash value, add to HLL |
| `Finalize` | Return HLL cardinality estimate |

---

### QUANTILE

**Syntax:** `REDUCE QUANTILE 2 @field quantile`  
(where `quantile` is 0.0–1.0)

Estimates a percentile value using a sorted sample.

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Create sorted sample buffer |
| `Add` | Insert value into sample |
| `Finalize` | Sort sample, return value at quantile position |

---

### STDDEV

**Syntax:** `REDUCE STDDEV 1 @field`

Computes the sample standard deviation of numeric values.

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Create running stats (mean, M2, count) |
| `Add` | Welford's online algorithm update |
| `Finalize` | Return `sqrt(M2 / (count - 1))` |

---

### FIRST_VALUE

**Syntax:** `REDUCE FIRST_VALUE 1 @field` or `REDUCE FIRST_VALUE 4 @field BY @sortfield ASC|DESC`

Returns the first value of a field, optionally sorted by another field.

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | `{ value: NULL, sortval: NULL }` |
| `Add` | Compare sortval, keep if better (or first seen) |
| `Finalize` | Return stored value |

> **Note:** No distribution function. Cluster mode falls back to coordinator-only grouping.

---

### RANDOM_SAMPLE

**Syntax:** `REDUCE RANDOM_SAMPLE 2 @field sample_size`

Reservoir sampling of `sample_size` values (max 1000).

| Phase | Implementation |
|-------|---------------|
| `NewInstance` | Create reservoir of capacity `sample_size` |
| `Add` | Reservoir sampling algorithm (replace with decreasing probability) |
| `Finalize` | Return array of sampled values |

This reducer is used internally by the distribution layer as a building block for
`QUANTILE` and `STDDEV` in cluster mode.

---

### HLL / HLL_SUM (Internal)

These are **internal reducers** used by the distribution layer. They are not meant to be
called directly by users.

**HLL:** Serializes HyperLogLog state for a field value.  
**HLL_SUM:** Merges multiple serialized HLL states and returns the cardinality estimate.

---

## Distribution Strategies

**File:** `src/coord/dist_plan.cpp`

In cluster mode, each reducer must be decomposed into:
- A **remote reducer** that runs on each shard
- A **local reducer** (or expression) that merges partial results on the coordinator

### Distribution Registry

```mermaid
graph TB
    subgraph "Distribution Functions"
        DC["distributeCount"]
        DSA["distributeSingleArgSelf<br/>(SUM, MIN, MAX, TOLIST)"]
        DA["distributeAvg"]
        DQ["distributeQuantile"]
        DSD["distributeStdDev"]
        DCDI["distributeCountDistinctish"]
    end

    subgraph "No Distribution (fallback)"
        ND1["COUNT_DISTINCT"]
        ND2["FIRST_VALUE"]
        ND3["RANDOM_SAMPLE"]
    end
```

### Strategy Details

#### COUNT → SUM

```mermaid
graph LR
    subgraph "Original"
        O["REDUCE COUNT 0 AS cnt"]
    end

    subgraph "Remote (shards)"
        R["REDUCE COUNT 0 AS __count"]
    end

    subgraph "Local (coordinator)"
        L["REDUCE SUM 1 @__count AS cnt"]
    end

    O -->|"split"| R & L
```

**Why SUM?** Each shard produces a count of documents in its group. The coordinator needs
the total count across all shards, which is the sum of per-shard counts.

---

#### SUM → SUM

```mermaid
graph LR
    subgraph "Original"
        O["REDUCE SUM 1 @price AS total"]
    end

    subgraph "Remote (shards)"
        R["REDUCE SUM 1 @price AS __sum_price"]
    end

    subgraph "Local (coordinator)"
        L["REDUCE SUM 1 @__sum_price AS total"]
    end

    O -->|"split"| R & L
```

SUM is self-distributable: the sum of partial sums equals the total sum.

---

#### MIN → MIN, MAX → MAX

```mermaid
graph LR
    subgraph "Original"
        O["REDUCE MIN 1 @price AS min_price"]
    end

    subgraph "Remote"
        R["REDUCE MIN 1 @price AS __min_price"]
    end

    subgraph "Local"
        L["REDUCE MIN 1 @__min_price AS min_price"]
    end

    O -->|"split"| R & L
```

MIN and MAX are also self-distributable: the min of mins is the global min.

---

#### TOLIST → TOLIST

Self-distributable but with a caveat: the coordinator TOLIST concatenates all per-shard
lists, which is semantically correct.

---

#### AVG → COUNT + SUM + APPLY

AVG is the most complex distribution because it requires **both** count and sum to
compute the average. It cannot be merged by simply averaging the averages.

```mermaid
graph TB
    subgraph "Original"
        O["REDUCE AVG 1 @price AS avg_price"]
    end

    subgraph "Remote (shards)"
        RC["REDUCE COUNT 0 AS __count"]
        RS["REDUCE SUM 1 @price AS __sum_price"]
    end

    subgraph "Local (coordinator)"
        LC["REDUCE SUM 1 @__count AS __local_count<br/>(hidden)"]
        LS["REDUCE SUM 1 @__sum_price AS __local_sum<br/>(hidden)"]
        AP["APPLY (@__local_sum / @__local_count)<br/>AS avg_price"]
    end

    O -->|"split"| RC & RS
    RC -->|"merge"| LC
    RS -->|"merge"| LS
    LC & LS -->|"combine"| AP
```

Step by step:
1. **Remote:** Two reducers — `COUNT` (how many) and `SUM` (sum of values)
2. **Local:** Two hidden `SUM` reducers to merge the partial counts and sums
3. **Local APPLY step:** Computes `@__local_sum / @__local_count` and stores as the
   final alias

The `isHidden` flag on the intermediate local reducers ensures they don't appear in the
final output.

> **Note on deduplication:** If the query also has a `COUNT` reducer, the remote `COUNT`
> added by AVG distribution reuses the same remote reducer (detected by
> `PLNGroupStep_FindReducer()`).

---

#### QUANTILE → RANDOM_SAMPLE + QUANTILE

```mermaid
graph LR
    subgraph "Original"
        O["REDUCE QUANTILE 2 @price 0.95"]
    end

    subgraph "Remote"
        R["REDUCE RANDOM_SAMPLE 2 @price 500"]
    end

    subgraph "Local"
        L["REDUCE QUANTILE 2 @__sample_price 0.95"]
    end

    O -->|"split"| R & L
```

Shards collect a random sample of 500 values per group. The coordinator then computes
the quantile from the merged sample. This is an **approximation** — the accuracy depends
on sample size relative to the total number of values.

---

#### STDDEV → RANDOM_SAMPLE + STDDEV

```mermaid
graph LR
    subgraph "Original"
        O["REDUCE STDDEV 1 @price"]
    end

    subgraph "Remote"
        R["REDUCE RANDOM_SAMPLE 2 @price 500"]
    end

    subgraph "Local"
        L["REDUCE STDDEV 1 @__sample_price"]
    end

    O -->|"split"| R & L
```

Same strategy as QUANTILE: collect a random sample per shard, compute STDDEV on the
merged sample at the coordinator. Also an approximation.

---

#### COUNT_DISTINCTISH → HLL + HLL_SUM

```mermaid
graph LR
    subgraph "Original"
        O["REDUCE COUNT_DISTINCTISH 1 @city"]
    end

    subgraph "Remote"
        R["REDUCE HLL 1 @city AS __hll_city"]
    end

    subgraph "Local"
        L["REDUCE HLL_SUM 1 @__hll_city AS city"]
    end

    O -->|"split"| R & L
```

HyperLogLog is naturally distributable: each shard computes an HLL sketch, and the
coordinator merges the sketches. The final cardinality estimate comes from the merged HLL.

---

### Non-Distributable Reducers

These reducers have **no distribution function** in the registry:

| Reducer | Reason |
|---------|--------|
| `COUNT_DISTINCT` | Requires exact set merge — too expensive to serialize |
| `FIRST_VALUE` | Sort-order-dependent; partial results can't be trivially merged |
| `RANDOM_SAMPLE` | Used as a building block, not directly user-facing in distribution |
| `HLL` | Internal only, used by COUNT_DISTINCTISH distribution |
| `HLL_SUM` | Internal only, used by COUNT_DISTINCTISH distribution |

When a plan contains non-distributable reducers, `AGGPLN_Distribute()` will fail to
distribute the `GROUP` step. In this scenario:
- The GROUP step remains in the local plan only
- All raw rows are sent from shards to the coordinator (with appropriate LOAD)
- Grouping happens entirely on the coordinator, at higher network cost

---

## Complete Distribution Example

```
FT.AGGREGATE idx '*'
  GROUPBY 2 @brand @category
  REDUCE COUNT 0 AS cnt
  REDUCE AVG 1 @price AS avg_price
  REDUCE MAX 1 @price AS max_price
  SORTBY 2 @cnt DESC
  LIMIT 0 5
```

### Remote Plan (per shard)

```
GROUPBY 2 @brand @category
  REDUCE COUNT 0 AS __count
  REDUCE SUM 1 @price AS __sum_price
  REDUCE MAX 1 @price AS __max_price
SORTBY 2 @__count DESC
LIMIT 0 5
```

### Local Plan (coordinator)

```
PLN_DistributeStep (RPNet as root)
GROUPBY 2 @brand @category
  REDUCE SUM 1 @__count AS cnt
  REDUCE SUM 1 @__sum_price AS __local_sum_price (hidden)
  REDUCE SUM 1 @__count AS __local_count_price (hidden)
  REDUCE MAX 1 @__max_price AS max_price
APPLY (@__local_sum_price / @__local_count_price) AS avg_price
SORTBY 2 @cnt DESC
LIMIT 0 5
```

### Visualization

```mermaid
flowchart LR
    subgraph "Each Shard"
        S_SCAN["Scan Index"]
        S_GROUP["GROUPBY @brand @category"]
        S_RED["COUNT → __count<br/>SUM @price → __sum_price<br/>MAX @price → __max_price"]
        S_SORT["SORTBY __count DESC<br/>LIMIT 0 5"]

        S_SCAN --> S_GROUP --> S_RED --> S_SORT
    end

    subgraph "Coordinator"
        C_NET["RPNet"]
        C_GROUP["GROUPBY @brand @category"]
        C_RED["SUM @__count → cnt<br/>SUM @__sum_price → __local_sum (hidden)<br/>SUM @__count → __local_cnt (hidden)<br/>MAX @__max_price → max_price"]
        C_APPLY["APPLY<br/>@__local_sum / @__local_cnt<br/>AS avg_price"]
        C_SORT["SORTBY cnt DESC<br/>LIMIT 0 5"]

        C_NET --> C_GROUP --> C_RED --> C_APPLY --> C_SORT
    end

    S_SORT -->|"partial groups<br/>{brand, category,<br/>__count, __sum_price,<br/>__max_price}"| C_NET
```

---

## Memory and Performance Considerations

### Block Allocation

Both `Group` structs and reducer instance data (per-group accumulators) use `BlkAlloc`
(block allocator) to reduce heap fragmentation. Groups are allocated in blocks of 1024.

### Hash Table Sizing

The `khash` hash table (`kh_init(khid)`) starts small and grows dynamically. For very
high-cardinality group keys, this can consume significant memory. The hash is computed
using `RSValue_Hash()` which produces a `uint64_t` from the group key values.

### Accumulation Buffering

During accumulation, the Grouper sets `resultLimit = UINT32_MAX` to prevent upstream
processors from stopping early. This means the Grouper will consume **all** matching
documents before emitting any output. For queries matching millions of documents, this
can be memory-intensive (one `Group` per unique key combination, plus per-group
accumulator data for each reducer).

### Cluster Overhead

In cluster mode, the distribution strategy adds overhead:
- **Network:** Each shard sends pre-grouped rows; for N shards with K unique keys each,
  worst case is N×K rows sent to the coordinator
- **Coordinator memory:** The local Grouper must re-accumulate all shard rows
- **Approximation:** QUANTILE, STDDEV, and COUNT_DISTINCTISH use sampling, trading
  accuracy for reducibility
