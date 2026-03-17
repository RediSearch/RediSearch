# Data Types & Entities

This document describes the core data structures involved in the GROUPBY mechanism,
how they relate to each other, and how data transforms as it flows through the pipeline.

## Entity Relationship Overview

```mermaid
erDiagram
    SearchResult ||--|| RLookupRow : "contains (_row_data)"
    SearchResult ||--o| RSDocumentMetadata : "references"
    SearchResult ||--o| RSIndexResult : "references"
    RLookupRow ||--o| RSSortingVector : "references (sv)"
    RLookupRow ||--o{ RSValue : "owns (dyn[])"
    RLookup ||--|{ RLookupKey : "linked list of keys"
    RLookupKey }|--|| RLookupRow : "indexes into"
    Grouper ||--|{ Group : "hash map of"
    Grouper ||--|{ Reducer : "array of"
    Grouper }|--|| ResultProcessor : "extends (base)"
    Group ||--|| RLookupRow : "contains (rowdata)"
    Group ||--o{ "void*" : "accumdata[] per reducer"
    Reducer ||--o| RLookupKey : "srckey (input)"
    Reducer ||--|| RLookupKey : "dstkey (output)"
    AGGPlan ||--|{ PLN_BaseStep : "linked list of steps"
    PLN_GroupStep }|--|| PLN_BaseStep : "extends (base)"
    PLN_GroupStep ||--|{ PLN_Reducer : "array of reducers"
    PLN_GroupStep ||--|| RLookup : "output lookup"
```

---

## SearchResult

**File:** `src/redisearch_rs/headers/search_result_rs.h`, `src/search_result.h`

The `SearchResult` is the **universal row object** that flows through the entire
`ResultProcessor` chain. Every processor receives a pointer to a `SearchResult`, reads
or modifies it, and passes it downstream.

```c
typedef struct SearchResult {
    t_docId              _doc_id;              // Internal document ID
    double               _score;               // Relevance score
    RSScoreExplain      *_score_explain;        // Score breakdown (optional)
    const RSDocumentMetadata *_document_metadata; // Document metadata pointer
    const RSIndexResult  *_index_result;         // Index result (term positions, etc.)
    RLookupRow           _row_data;             // Field values — the main data carrier
    SearchResultFlags    _flags;                // Flags (e.g., expired doc)
} SearchResult;
```

### Dual Nature: Document vs. Group

The same struct serves two fundamentally different purposes depending on where it is
in the pipeline:

```mermaid
graph TB
    subgraph "Pre-GROUPBY: Document Result"
        D["SearchResult"]
        D1["_doc_id = 42"]
        D2["_score = 3.14"]
        D3["_document_metadata → DMD"]
        D4["_index_result → positions, flags"]
        D5["_row_data → {brand='Nike', price=99.99}"]
        D --> D1 & D2 & D3 & D4 & D5
    end

    subgraph "Post-GROUPBY: Group Result"
        G["SearchResult"]
        G1["_doc_id = 0 (unused)"]
        G2["_score = 0 (unused)"]
        G3["_document_metadata = NULL"]
        G4["_index_result = NULL"]
        G5["_row_data → {brand='Nike', count=127, avg_price=85.5}"]
        G --> G1 & G2 & G3 & G4 & G5
    end

    D -->|"Grouper consumes<br/>and re-emits"| G

    style D1 fill:#9f9,stroke:#333
    style D2 fill:#9f9,stroke:#333
    style D3 fill:#9f9,stroke:#333
    style D4 fill:#9f9,stroke:#333
    style G1 fill:#f99,stroke:#333
    style G2 fill:#f99,stroke:#333
    style G3 fill:#f99,stroke:#333
    style G4 fill:#f99,stroke:#333
```

After GROUPBY, only `_row_data` carries meaningful information. The group key values and
reducer outputs are all stored there.

### Key Operations

| Function | Purpose |
|----------|---------|
| `SearchResult_New()` | Zero-initialize a result |
| `SearchResult_Clear(res)` | Reset `_row_data` without freeing the struct |
| `SearchResult_Destroy(res)` | Full cleanup of owned resources |
| `SearchResult_GetRowDataMut(res)` | Get mutable pointer to `_row_data` |

---

## RLookupRow

**File:** `src/rlookup.h`

The `RLookupRow` is the **field-value storage** inside each `SearchResult`. It provides
indexed access to values via `RLookupKey` objects, avoiding string-based lookups at
query time.

```c
typedef struct {
    const RSSortingVector *sv;   // Sortable values from the index (read-only)
    RSValue             **dyn;   // Dynamic values (loaded/computed)
    size_t                ndyn;  // Count of values in dyn
} RLookupRow;
```

### Two Value Sources

```mermaid
graph TB
    subgraph "RLookupRow"
        SV["sv (RSSortingVector*)<br/>Pre-indexed sortable fields<br/>Read-only, from document metadata"]
        DYN["dyn (RSValue**)<br/>Dynamic values array<br/>Written by loaders, projectors, groupers"]
    end

    subgraph "Value Retrieval: RLookupRow_Get(key, row)"
        CHECK["Check dyn[key._dstIdx]"]
        FOUND1["Return value"]
        CHECK2["key has F_SVSRC?<br/>Check sv[key._svIdx]"]
        FOUND2["Return value"]
        NULL["Return NULL"]

        CHECK -->|"found"| FOUND1
        CHECK -->|"not found"| CHECK2
        CHECK2 -->|"found"| FOUND2
        CHECK2 -->|"not found"| NULL
    end
```

The lookup order is:
1. Check `dyn[key._dstIdx]` — the dynamic array (values written by processors)
2. If not found and key has `RLOOKUP_F_SVSRC`, check `sv[key._svIdx]` — the sorting vector

### In the GROUPBY Context

- **Before GROUPBY:** The row may contain values from the sorting vector (indexed sortable
  fields) and/or dynamically loaded values from the Redis keyspace.
- **Inside GROUPBY:** The Grouper reads values from the upstream row via `srckeys`, then
  writes group key values to the Group's own `RLookupRow` via `dstkeys`.
- **After GROUPBY:** The yielded `SearchResult._row_data` contains only the group key
  values and reducer outputs — no sorting vector, no document source data.

---

## RLookup and RLookupKey

**File:** `src/rlookup.h`, `src/rlookup.c`

An `RLookup` is a **schema registry** — a linked list of `RLookupKey` objects that map
field names to array indices. Each step in the aggregation plan that produces new fields
(like `GROUPBY`) has its own `RLookup`.

```c
typedef struct RLookup {
    RLookupKey *_head;         // First key in linked list
    RLookupKey *_tail;         // Last key
    uint32_t    _rowlen;       // Number of slots in the row
    uint32_t    _options;      // Options flags
    IndexSpecCache *_spcache;  // Schema cache for field resolution
} RLookup;
```

An `RLookupKey` is a **named slot reference**:

```c
typedef struct RLookupKey {
    uint16_t _dstIdx;          // Index in RLookupRow.dyn[]
    uint16_t _svIdx;           // Index in RSSortingVector (if sortable)
    char    *_name;            // Field name (e.g., "brand")
    char    *_path;            // Field path (e.g., "$.brand")
    size_t   _nameLen;
    uint32_t _flags;           // RLOOKUP_F_* flags
    struct RLookupKey *_next;  // Next key in linked list
} RLookupKey;
```

### How Lookups Chain Across Steps

Each reducing step (like `GROUPBY`) introduces a new `RLookup`, breaking the lookup chain.
Non-reducing steps (like `FILTER`, `APPLY`) share the lookup from their upstream reducing step.

```mermaid
graph LR
    subgraph "Lookup Chain"
        L1["RLookup #1 (Root)<br/>fields: brand, price, name<br/>from index schema"]
        L2["RLookup #2 (GROUPBY)<br/>fields: brand, count, avg_price<br/>output keys"]

        L1 -->|"GROUPBY boundary"| L2
    end

    subgraph "Key Resolution in GROUPBY"
        SRC["srckeys[] → read from L1"]
        DST["dstkeys[] → write to L2"]
        SRC -->|"@brand value"| DST
    end
```

### Key Flag Reference

| Flag | Value | Meaning |
|------|-------|---------|
| `RLOOKUP_F_DOCSRC` | 0x01 | Value comes from the document |
| `RLOOKUP_F_SCHEMASRC` | 0x02 | Field is part of the index schema |
| `RLOOKUP_F_SVSRC` | 0x04 | Value resides in the sorting vector |
| `RLOOKUP_F_QUERYSRC` | 0x08 | Created by the query (APPLY, GROUPBY output) |
| `RLOOKUP_F_HIDDEN` | 0x100 | Transient field, not emitted in output |
| `RLOOKUP_F_UNRESOLVED` | 0x80 | Source unknown, needs resolution later |

---

## Group

**File:** `src/aggregate/group_by.c`

A `Group` is the per-unique-key accumulator. One `Group` is created for each distinct
combination of group key values encountered during accumulation.

```c
typedef struct {
    RLookupRow  rowdata;       // Group key values (for output)
    void       *accumdata[0];  // Flexible array: one slot per reducer
} Group;
```

The `accumdata` array uses a C flexible array member (`[0]`). Its actual length equals
the number of reducers in the `Grouper`. Each slot holds the reducer-specific accumulator
created by `Reducer::NewInstance()`.

```mermaid
graph TB
    subgraph "Group (brand='Nike')"
        RD["rowdata (RLookupRow)<br/>dyn[0] = 'Nike'"]
        A0["accumdata[0]<br/>COUNT → {count: 127}"]
        A1["accumdata[1]<br/>SUM → {count: 127, total: 10858.5}"]
        A2["accumdata[2]<br/>AVG → {count: 127, total: 10858.5}"]
    end
```

### Memory Layout

Groups are allocated via `BlkAlloc` (block allocator) in 1024-group blocks to minimize
fragmentation. The allocation size per group is:

```
sizeof(Group) + sizeof(void*) * num_reducers
```

### Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Created: extractGroups() finds new hash
    Created --> Accumulating: invokeReducers() calls Reducer::Add()
    Accumulating --> Accumulating: more rows with same key
    Accumulating --> Finalizing: Grouper_rpYield() calls Reducer::Finalize()
    Finalizing --> Emitted: values written to SearchResult._row_data
    Emitted --> [*]: Grouper_rpFree() cleans up
```

---

## Grouper

**File:** `src/aggregate/group_by.c`

The `Grouper` is the **core grouping engine**, implemented as a `ResultProcessor`. It
owns the hash map of groups, the source/destination key mappings, and the reducers.

```c
typedef struct Grouper {
    ResultProcessor     base;          // RP interface (Next, Free, type=RP_GROUP)
    khash_t(khid)      *groups;        // Hash map: uint64_t → Group*
    BlkAlloc            groupsAlloc;   // Block allocator for Group structs
    const RLookupKey  **srckeys;       // Keys to read from upstream rows
    const RLookupKey  **dstkeys;       // Keys to write in output rows
    size_t              nkeys;         // Number of group-by keys
    Reducer           **reducers;      // Array of reducer instances
    khiter_t            iter;          // Iterator for yielding groups
} Grouper;
```

### Two-Phase Operation

The Grouper has a unique two-phase execution model, driven by swapping its `Next` function
pointer:

```mermaid
sequenceDiagram
    participant Downstream as Downstream RP (Sorter)
    participant Grouper
    participant Upstream as Upstream RP (Loader)

    Note over Grouper: Phase 1: base.Next = Grouper_rpAccum

    Downstream->>Grouper: Next(result)
    loop Until upstream returns EOF
        Grouper->>Upstream: Next(result)
        Upstream-->>Grouper: RS_RESULT_OK + row data
        Note over Grouper: invokeGroupReducers()<br/>→ extractGroups()<br/>→ hash keys, find/create Group<br/>→ invokeReducers() → Reducer::Add()
        Note over Grouper: SearchResult_Clear(result)
    end
    Upstream-->>Grouper: RS_RESULT_EOF

    Note over Grouper: Switch: base.Next = Grouper_rpYield<br/>Set totalResults = num_groups

    Note over Grouper: Phase 2: Yielding

    loop For each Group in hash map
        Grouper-->>Downstream: RS_RESULT_OK
        Note over Downstream: result._row_data has<br/>group keys + Reducer::Finalize() values
    end
    Grouper-->>Downstream: RS_RESULT_EOF
```

### Key Mapping (srckeys → dstkeys)

The `srckeys` and `dstkeys` arrays are fundamental to understanding how data flows
across the GROUPBY boundary:

- **`srckeys[i]`** — An `RLookupKey` that indexes into the **upstream** `RLookup`. Used
  to read field values from incoming `SearchResult` rows.
- **`dstkeys[i]`** — An `RLookupKey` that indexes into the **GROUPBY step's own** `RLookup`.
  Used to write group key values into the `Group.rowdata` and later into the output
  `SearchResult`.

```mermaid
graph LR
    subgraph "Upstream RLookup (Root)"
        UK1["RLookupKey 'brand'<br/>dstIdx=0"]
        UK2["RLookupKey 'price'<br/>dstIdx=1"]
        UK3["RLookupKey 'name'<br/>dstIdx=2"]
    end

    subgraph "Input SearchResult._row_data"
        IV["dyn[0]='Nike', dyn[1]=99.99, dyn[2]='Air Max'"]
    end

    subgraph "GROUPBY RLookup"
        DK1["RLookupKey 'brand'<br/>dstIdx=0"]
        DK2["RLookupKey 'count'<br/>dstIdx=1"]
        DK3["RLookupKey 'avg_price'<br/>dstIdx=2"]
    end

    subgraph "Output SearchResult._row_data"
        OV["dyn[0]='Nike', dyn[1]=127, dyn[2]=85.5"]
    end

    UK1 -->|"srckeys[0]<br/>read 'Nike'"| DK1
    DK1 -->|"dstkeys[0]<br/>write 'Nike'"| OV
```

### Hashing and Group Lookup

Groups are stored in a `khash` hash map keyed by a `uint64_t` hash computed from the
group key values. The `extractGroups()` function computes this hash recursively:

```mermaid
flowchart TD
    Start["invokeGroupReducers(g, srcrow)"]
    ReadKeys["Read all srckey values<br/>groupvals[] = { val1, val2, ... }"]
    Extract["extractGroups(g, groupvals, pos=0, len=N, hash=0, row)"]

    CheckPos{"pos == len?"}
    GetVal["v = groupvals[pos]"]
    IsArray{"RSValue_IsArray(v)?"}

    HashScalar["hash = RSValue_Hash(v, hash)<br/>extractGroups(pos+1)"]
    EmptyArray{"ArrayLen == 0?"}
    HashNull["hash = RSValue_Hash(NULL, hash)<br/>extractGroups(pos+1)"]

    LoopArray["For each element in array:<br/>hash element<br/>extractGroups(pos+1)"]

    LookupGroup["kh_get(groups, hash)"]
    Found{"Found?"}
    CreateGroup["createGroup(g, groupvals, len)<br/>kh_set(groups, hash, group)"]
    InvokeReducers["invokeReducers(g, group, srcrow)"]

    Start --> ReadKeys --> Extract --> CheckPos
    CheckPos -->|"yes"| LookupGroup
    CheckPos -->|"no"| GetVal --> IsArray
    IsArray -->|"no"| HashScalar
    IsArray -->|"yes"| EmptyArray
    EmptyArray -->|"yes"| HashNull
    EmptyArray -->|"no"| LoopArray

    LookupGroup --> Found
    Found -->|"yes"| InvokeReducers
    Found -->|"no"| CreateGroup --> InvokeReducers
```

Note the **array expansion** behavior: if a group key value is an array (e.g., a multi-value
TAG field), each array element creates a separate group entry. This is the **cartesian
product** behavior — a document with `tags=['A','B']` grouped by `@tags` will contribute
to both group `A` and group `B`.

---

## Reducer

**File:** `src/aggregate/reducer.h`

A `Reducer` defines a statistically aggregating function. It is a **factory** that creates
per-group accumulator instances and provides callbacks for adding values and finalizing
results.

```c
typedef struct Reducer {
    const RLookupKey *srckey;   // Source field to read from (optional)
    RLookupKey       *dstkey;   // Destination field for output
    BlkAlloc          alloc;    // Block allocator for per-group instances
    uint32_t          reducerId;

    // Per-group lifecycle callbacks:
    void    *(*NewInstance)(struct Reducer *r);
    int      (*Add)(struct Reducer *parent, void *instance, const RLookupRow *srcrow);
    RSValue *(*Finalize)(struct Reducer *parent, void *instance);
    void     (*FreeInstance)(struct Reducer *parent, void *instance);
    void     (*Free)(struct Reducer *r);
} Reducer;
```

### Reducer Lifecycle Per Group

```mermaid
sequenceDiagram
    participant G as Group
    participant R as Reducer

    Note over R: createGroup() is called
    R->>G: NewInstance() → accumdata[i]

    loop For each row in this group
        R->>G: Add(instance, srcrow)<br/>e.g., counter++ or total += value
    end

    Note over R: Grouper_rpYield() is called
    R->>G: Finalize(instance) → RSValue*
    Note over G: Value written to output row<br/>via RLookup_WriteOwnKey(dstkey, ...)
```

### Reducer Types

| Type | Accumulator State | Finalize Logic |
|------|-------------------|----------------|
| `COUNT` | `{count}` | Return count |
| `SUM` | `{count, total}` | Return total |
| `AVG` | `{count, total}` | Return total/count |
| `MIN` | `{value}` | Return minimum seen |
| `MAX` | `{value}` | Return maximum seen |
| `COUNT_DISTINCT` | Hash set | Return set size |
| `COUNT_DISTINCTISH` | HyperLogLog | Return HLL estimate |
| `TOLIST` | Array of values | Return array |
| `FIRST_VALUE` | `{value, sortval}` | Return first by sort order |
| `RANDOM_SAMPLE` | Reservoir of N items | Return sample array |
| `QUANTILE` | Sorted sample | Return quantile value |
| `STDDEV` | Running stats | Return standard deviation |
| `HLL` | HyperLogLog bytes | Return serialized HLL |
| `HLL_SUM` | Merged HLL | Return estimated count |

See [Reducers & Distribution](reducers.md) for detailed descriptions.

---

## ResultProcessor

**File:** `src/result_processor.h`

The `ResultProcessor` is the **pipeline building block**. Each processor is a node in a
singly-linked list, pulling from its `upstream` and producing results for its caller.

```c
typedef struct ResultProcessor {
    QueryProcessingCtx     *parent;    // Shared query context
    struct ResultProcessor *upstream;   // Previous processor in chain
    ResultProcessorType     type;       // RP_INDEX, RP_GROUP, RP_SORTER, etc.
    int (*Next)(struct ResultProcessor *self, SearchResult *res);
    void (*Free)(struct ResultProcessor *self);
} ResultProcessor;
```

### Processor Types Relevant to GROUPBY

```mermaid
graph TD
    subgraph "Full Pipeline Example"
        RP1["RP_INDEX<br/>RPQueryIterator<br/>Pulls doc IDs from index"]
        RP2["RP_SCORER<br/>RPScorer<br/>Computes relevance scores"]
        RP3["RP_LOADER<br/>RPLoader<br/>Loads field values from Redis"]
        RP4["RP_GROUP<br/>Grouper<br/>Groups rows, runs reducers"]
        RP5["RP_SORTER<br/>RPSorter<br/>Sorts output rows"]
        RP6["RP_PAGER_LIMITER<br/>RPPager<br/>Applies LIMIT offset count"]

        RP1 --> RP2 --> RP3 --> RP4 --> RP5 --> RP6
    end

    style RP4 fill:#ff9,stroke:#333,stroke-width:2px
```

### QueryProcessingCtx

The shared context that all processors in a chain reference:

```c
typedef struct QueryProcessingCtx {
    ResultProcessor *rootProc;      // First processor (usually RPQueryIterator)
    ResultProcessor *endProc;       // Last processor (tail of chain)
    uint32_t         totalResults;  // Total results (set by Grouper = num_groups)
    uint32_t         resultLimit;   // Current chunk limit
    QueryError      *err;           // Error state
    // ... timing, flags, etc.
} QueryProcessingCtx;
```

During GROUPBY accumulation, the Grouper temporarily sets `resultLimit = UINT32_MAX`
to consume all upstream rows, then restores it afterward.

---

## AGGPlan and PLN_GroupStep

**File:** `src/aggregate/aggregate_plan.h`

The `AGGPlan` is the **logical query plan** — a linked list of `PLN_BaseStep` nodes that
describe what operations to perform. It is constructed during parsing and later converted
into the `ResultProcessor` chain during pipeline construction.

```c
struct AGGPlan {
    DLLIST          steps;          // Doubly-linked list of PLN_BaseStep
    PLN_ArrangeStep *arrangement;   // Current SORTBY/LIMIT step
    PLN_FirstStep    firstStep_s;   // Root step with initial RLookup
    uint64_t         steptypes;     // Bitmask of step types present
};
```

### PLN_GroupStep

```c
typedef struct {
    PLN_BaseStep  base;              // Type = PLN_T_GROUP
    RLookup       lookup;            // Output lookup (group keys + reducer outputs)
    StrongRef     properties_ref;    // Reference to property names array (e.g., ["@brand"])
    PLN_Reducer  *reducers;          // Array of reducer definitions
    int           idx;
    bool          strictPrefix;
} PLN_GroupStep;
```

### PLN_Reducer

```c
struct PLN_Reducer {
    const char *name;      // Reducer function name (e.g., "COUNT", "SUM")
    char       *alias;     // Output alias (e.g., "count", "total_price")
    bool        isHidden;  // Hidden from output (used in distribution)
    ArgsCursor  args;      // Arguments to the reducer
};
```

### Plan-to-Pipeline Mapping

```mermaid
graph LR
    subgraph "Logical Plan (AGGPlan)"
        PS1["PLN_FirstStep<br/>(root)"]
        PS2["PLN_LoadStep<br/>LOAD 1 @brand"]
        PS3["PLN_GroupStep<br/>GROUPBY 1 @brand<br/>REDUCE COUNT 0 AS cnt"]
        PS4["PLN_ArrangeStep<br/>SORTBY 2 @cnt DESC"]

        PS1 --> PS2 --> PS3 --> PS4
    end

    subgraph "Physical Pipeline (ResultProcessor chain)"
        RP1["RPQueryIterator"]
        RP2["RPLoader"]
        RP3["Grouper"]
        RP4["RPSorter"]

        RP1 --> RP2 --> RP3 --> RP4
    end

    PS1 -.->|"builds"| RP1
    PS2 -.->|"builds"| RP2
    PS3 -.->|"builds"| RP3
    PS4 -.->|"builds"| RP4
```

---

## PLN_DistributeStep (Cluster Only)

**File:** `src/coord/dist_plan.cpp`

In cluster mode, the plan is split into a **remote plan** (sent to shards) and a **local
plan** (executed on the coordinator). The `PLN_DistributeStep` is inserted at the head
of the local plan and holds the remote plan plus its serialization.

```c
typedef struct {
    PLN_BaseStep     base;        // Type = PLN_T_DISTRIBUTE
    AGGPlan         *plan;        // The remote plan (to be serialized and sent to shards)
    char           **serialized;  // Serialized remote plan as string array
    PLN_GroupStep  **oldSteps;    // Original steps replaced by distribution
    BlkAlloc         alloc;       // Allocator for distribution metadata
    RLookup          lk;          // Lookup for fields coming from shards
} PLN_DistributeStep;
```

See [Cluster Flow](cluster-flow.md) for how this is used.

---

## RPNet (Cluster Only)

**File:** `src/coord/rpnet.h`

The `RPNet` is a `ResultProcessor` that acts as the **network source** in the coordinator's
pipeline. It replaces `RPQueryIterator` as the root processor, pulling rows from shard
replies instead of a local index.

```c
typedef struct {
    ResultProcessor base;             // RP interface
    struct {
        MRReply *root;                // Current shard reply (for freeing)
        MRReply *rows;                // Array of result rows
        MRReply *meta;                // Reply metadata (RESP3)
    } current;
    RLookup     *lookup;              // Lookup for writing row fields
    size_t       curIdx;              // Current index in rows array
    MRIterator  *it;                  // Multi-shard iterator
    MRCommand    cmd;                 // Command sent to shards
    AREQ        *areq;               // Aggregate request
    // ... cursor mappings, profile data, barrier ...
} RPNet;
```

### How RPNet Populates SearchResult

When `rpnetNext()` is called, it:

1. Gets the next row from the current shard reply (or fetches a new reply via `getNextReply()`)
2. Iterates over field-value pairs in the row
3. Converts each `MRReply` value to an `RSValue` via `MRReply_ToValue()`
4. Writes each value to the `SearchResult._row_data` via `RLookupRow_WriteByNameOwned()`

```mermaid
sequenceDiagram
    participant Local as Local Grouper
    participant RPNet
    participant Shard as Shard Reply

    Local->>RPNet: Next(result)
    RPNet->>Shard: MRReply_ArrayElement(rows, curIdx++)
    Note over RPNet: For each field/value pair:
    RPNet->>RPNet: MRReply_ToValue(val) → RSValue
    RPNet->>RPNet: RLookupRow_WriteByNameOwned(lookup, field, row, value)
    RPNet-->>Local: RS_RESULT_OK (result._row_data populated)
```
