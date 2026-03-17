# Cluster GROUPBY Flow

In a Redis Cluster deployment, `FT.AGGREGATE` with `GROUPBY` involves coordination between
a **coordinator node** and multiple **shard nodes**. The coordinator splits the aggregation
plan, distributes partial work to shards, collects partial results, and merges them locally.

## High-Level Architecture

```mermaid
flowchart TB
    CLIENT["Client"]
    COORD["Coordinator Node"]

    subgraph "Shard 1"
        S1_IDX["Index Scan"]
        S1_GRP["GROUPBY (partial)<br/>COUNT → local count"]
        S1_REPLY["Reply: grouped rows"]
    end

    subgraph "Shard 2"
        S2_IDX["Index Scan"]
        S2_GRP["GROUPBY (partial)<br/>COUNT → local count"]
        S2_REPLY["Reply: grouped rows"]
    end

    subgraph "Shard 3"
        S3_IDX["Index Scan"]
        S3_GRP["GROUPBY (partial)<br/>COUNT → local count"]
        S3_REPLY["Reply: grouped rows"]
    end

    subgraph "Coordinator Pipeline"
        RPNET["RPNet<br/>(network source)"]
        LOCAL_GRP["GROUPBY (merge)<br/>SUM of counts"]
        SORT["RPSorter"]
        PAGE["RPPager"]
    end

    CLIENT -->|"FT.AGGREGATE idx '*'<br/>GROUPBY 1 @brand<br/>REDUCE COUNT 0 AS cnt"| COORD
    COORD -->|"_FT.AGGREGATE (remote plan)"| S1_IDX & S2_IDX & S3_IDX
    S1_IDX --> S1_GRP --> S1_REPLY
    S2_IDX --> S2_GRP --> S2_REPLY
    S3_IDX --> S3_GRP --> S3_REPLY
    S1_REPLY & S2_REPLY & S3_REPLY -->|"partial group results"| RPNET
    RPNET --> LOCAL_GRP --> SORT --> PAGE
    PAGE -->|"final results"| CLIENT
```

---

## End-to-End Flow

```mermaid
sequenceDiagram
    participant Client
    participant Coord as Coordinator
    participant DistPlan as dist_plan.cpp
    participant Shards as Shard Nodes
    participant RPNet as RPNet (Coord)
    participant LocalPipe as Local Pipeline

    Client->>Coord: FT.AGGREGATE idx '*' GROUPBY 1 @brand REDUCE COUNT 0 AS cnt

    Note over Coord: 1. Parse into AGGPlan

    Coord->>DistPlan: AGGPLN_Distribute(plan)
    Note over DistPlan: Split plan into remote + local
    DistPlan->>DistPlan: distributeGroupStep()<br/>Remote: GROUPBY @brand REDUCE COUNT 0<br/>Local: GROUPBY @brand REDUCE SUM 1 @__count AS cnt

    Note over Coord: 2. Serialize remote plan

    Coord->>DistPlan: AGPLN_Serialize(remote_plan)
    DistPlan-->>Coord: ["GROUPBY", "1", "@brand", "REDUCE", "COUNT", "0", "AS", "__count"]

    Note over Coord: 3. Build MR command

    Coord->>Coord: buildMRCommand()<br/>_FT.AGGREGATE idx '*' ... GROUPBY 1 @brand REDUCE COUNT 0 AS __count

    Note over Coord: 4. Build local pipeline

    Coord->>Coord: AREQ_BuildDistributedPipeline()<br/>RPNet → Local Grouper (SUM) → Sorter → Pager

    Note over Coord: 5. Dispatch to shards

    Coord->>Shards: _FT.AGGREGATE (via MRIterator)

    par Shard execution (parallel)
        Shards->>Shards: Parse + execute remote plan
        Note over Shards: Each shard: scan index → group → count
        Shards-->>RPNet: Partial results (grouped rows)
    end

    Note over Coord: 6. Process shard replies

    loop For each shard reply row
        RPNet->>RPNet: MRReply_ToValue() → RSValue<br/>RLookupRow_WriteByNameOwned()
        RPNet-->>LocalPipe: SearchResult with {brand: X, __count: N}
    end

    Note over LocalPipe: Local Grouper merges<br/>same keys from different shards

    LocalPipe-->>Client: Final merged results
```

---

## Phase 1: Plan Distribution

**Entry point:** `AGGPLN_Distribute()` in `src/coord/dist_plan.cpp`  
**Called from:** `RSExecDistAggregate()` → `prepareForExecution()`

### How the Plan is Split

`AGGPLN_Distribute()` walks the original plan step by step. Steps that can run on shards
are moved or copied to a **remote plan**. The original plan becomes the **local plan**
(coordinator-side).

```mermaid
flowchart TD
    START["AGGPLN_Distribute(src_plan)"]
    INIT["Create remote AGGPlan<br/>Create PLN_DistributeStep"]

    WALK["Walk plan steps from root"]

    CHECK{"Step type?"}

    ROOT["PLN_T_ROOT → skip"]
    LOAD["PLN_T_LOAD → move to remote"]
    APPLY["PLN_T_APPLY → move to remote"]
    FILTER["PLN_T_FILTER → move to remote<br/>(if before ARRANGE)"]
    VNORM["PLN_T_VECTOR_NORMALIZER → move to remote"]

    ARRANGE["PLN_T_ARRANGE →<br/>Copy to remote (limit shard output)<br/>Keep in local (final sort)"]

    GROUP["PLN_T_GROUP →<br/>distributeGroupStep()"]
    STOP["Everything after GROUP<br/>stays local"]

    FINAL["finalize_distribution():<br/>Prepend PLN_DistributeStep to local<br/>Serialize remote plan"]

    START --> INIT --> WALK --> CHECK
    CHECK -->|ROOT| ROOT --> WALK
    CHECK -->|LOAD| LOAD --> WALK
    CHECK -->|APPLY| APPLY --> WALK
    CHECK -->|FILTER| FILTER --> WALK
    CHECK -->|VNORM| VNORM --> WALK
    CHECK -->|ARRANGE| ARRANGE --> WALK
    CHECK -->|GROUP| GROUP --> STOP --> FINAL
```

### distributeGroupStep() in Detail

This is the core function that splits a single `GROUPBY` step into remote and local parts.
For each reducer, it calls a **distribution function** that knows how to decompose the
reducer into a shard-side computation and a coordinator-side merge.

```mermaid
flowchart TD
    ENTRY["distributeGroupStep(origPlan, remote, step, dstp)"]
    CLONE["Clone properties_ref for both:<br/>grLocal = PLNGroupStep_New(properties)<br/>grRemote = PLNGroupStep_New(properties)"]

    LOOP["For each reducer in original step"]
    GETFN["fn = getDistributionFunc(reducer.name)"]
    CALL["fn(&rdctx, status)"]

    NOTE1["Distribution function adds:<br/>- Reducer(s) to grRemote<br/>- Reducer(s) to grLocal<br/>- Possibly APPLY steps to local"]

    DONE["Save original step in oldSteps<br/>Add grLocal after original position<br/>Add grRemote to remote plan"]

    ENTRY --> CLONE --> LOOP --> GETFN --> CALL --> NOTE1
    NOTE1 --> LOOP
    LOOP -->|"all done"| DONE
```

### Plan Before and After Distribution

**Original plan:**
```
ROOT → GROUPBY 1 @brand REDUCE COUNT 0 AS cnt → SORTBY 2 @cnt DESC → LIMIT 0 10
```

**After distribution:**

```mermaid
graph TB
    subgraph "Remote Plan (sent to shards)"
        R_GRP["GROUPBY 1 @brand<br/>REDUCE COUNT 0 AS __count"]
        R_ARR["SORTBY 2 @cnt DESC<br/>LIMIT 0 10"]
        R_GRP --> R_ARR
    end

    subgraph "Local Plan (coordinator)"
        L_DIST["PLN_DistributeStep<br/>(holds serialized remote plan)"]
        L_GRP["GROUPBY 1 @brand<br/>REDUCE SUM 1 @__count AS cnt"]
        L_ARR["SORTBY 2 @cnt DESC<br/>LIMIT 0 10"]
        L_DIST --> L_GRP --> L_ARR
    end
```

Notice:
- The remote `COUNT` uses a temporary alias `__count` (double-underscore prefix)
- The local step uses `SUM` on `@__count` to merge the partial counts
- The `SORTBY`/`LIMIT` is duplicated to both plans — shards pre-sort and limit their
  output, and the coordinator re-sorts and re-limits the merged result

---

## Phase 2: Command Building and Dispatch

**Entry point:** `buildMRCommand()` in `src/coord/dist_aggregate.c`

### What is Sent to Shards

The coordinator serializes the remote plan and builds an `_FT.AGGREGATE` command (internal
command, prefixed with underscore):

```mermaid
graph LR
    subgraph "MRCommand to each shard"
        CMD["_FT.AGGREGATE"]
        IDX["idx"]
        QUERY["'*'"]
        OPTS["WITHCURSOR<br/>_NUM_SSTRING 14<br/>DIALECT 2<br/>..."]
        PLAN["GROUPBY 1 @brand<br/>REDUCE COUNT 0<br/>AS __count<br/>SORTBY 2 @__count DESC<br/>LIMIT 0 10"]

        CMD --> IDX --> QUERY --> OPTS --> PLAN
    end
```

The command includes:
- Index name and query string
- Execution options (`WITHCURSOR`, `DIALECT`, `FORMAT`, `SCORER`, `TIMEOUT`, etc.)
- The serialized remote plan (GROUPBY + reducers + optional SORTBY/LIMIT)
- Any LOAD directives for fields needed by filter expressions
- Query PARAMS if present

### Dispatch via MRIterator

The command is dispatched to all shards via `MR_Iterate()`, which creates an `MRIterator`
that manages concurrent communication with all shards. Replies are collected asynchronously
and made available through `MRIterator_Next()`.

---

## Phase 3: Shard Execution

Each shard receives `_FT.AGGREGATE` and processes it exactly like a [single-shard flow](single-shard-flow.md), except:

1. The plan contains only the **remote** portion (GROUPBY with partial reducers)
2. The shard groups its local documents and applies the partial reducers
3. The result is serialized and sent back to the coordinator

### Shard Reply Format

**RESP2:**
```
*2                       # Array: [results_array, cursor_id]
  *N                     # Results array: [total, row1, row2, ...]
    :42                  # total_results from this shard
    *4                   # Row 1: [field, value, field, value, ...]
      $5 brand
      $4 Nike
      $7 __count
      $2 15
    *4                   # Row 2
      $5 brand
      $6 Adidas
      $7 __count
      $1 8
  :0                     # cursor_id (0 = no more data)
```

**RESP3:**
```
[
  {
    "total_results": 42,
    "results": [
      {"brand": "Nike", "__count": 15},
      {"brand": "Adidas", "__count": 8}
    ]
  },
  0  // cursor_id
]
```

---

## Phase 4: Coordinator Pipeline Execution

### Pipeline Structure

The coordinator's physical pipeline looks like:

```mermaid
graph LR
    NET["RPNet<br/>type: RP_NETWORK<br/>Pulls rows from shard replies"]
    GRP["Grouper<br/>type: RP_GROUP<br/>REDUCE SUM 1 @__count AS cnt<br/>Merges partial groups"]
    SRT["RPSorter<br/>type: RP_SORTER<br/>SORTBY @cnt DESC"]
    PGR["RPPager<br/>type: RP_PAGER_LIMITER<br/>LIMIT 0 10"]

    NET --> GRP --> SRT --> PGR

    style NET fill:#9cf,stroke:#333,stroke-width:2px
    style GRP fill:#ff9,stroke:#333,stroke-width:2px
```

### RPNet: Network to SearchResult

The `RPNet` processor is the bridge between network replies and the local pipeline. Its
`rpnetNext()` function converts shard reply data into `SearchResult` objects.

```mermaid
flowchart TD
    CALL["rpnetNext(self, result)"]
    HASROWS{"Have unconsumed<br/>rows in current batch?"}

    GETREPLY["getNextReply(nc)<br/>Fetch next shard reply batch"]
    PARSE["Parse reply structure:<br/>RESP2: rows = reply[0], total = rows[0]<br/>RESP3: rows = meta['results']"]

    EXTRACT["fields = rows[curIdx++]"]
    FIELDLOOP["For each field/value pair:"]
    CONVERT["value = MRReply_ToValue(val)"]
    WRITE["RLookupRow_WriteByNameOwned(<br/>  lookup, field_name, &result._row_data, value)"]

    RETURN["return RS_RESULT_OK"]
    EOF["return RS_RESULT_EOF"]

    CALL --> HASROWS
    HASROWS -->|"yes"| EXTRACT
    HASROWS -->|"no"| GETREPLY
    GETREPLY -->|"got reply"| PARSE --> EXTRACT
    GETREPLY -->|"no more"| EOF
    EXTRACT --> FIELDLOOP --> CONVERT --> WRITE
    WRITE --> RETURN
```

### Merge via Local Grouper

The local Grouper processes rows from RPNet identically to the single-shard Grouper, but
the data it sees is already **pre-grouped** by the shards. Same brand keys from different
shards are merged into the same group, and the local reducers (e.g., `SUM`) aggregate the
partial results.

```mermaid
sequenceDiagram
    participant S1 as Shard 1 Reply
    participant S2 as Shard 2 Reply
    participant S3 as Shard 3 Reply
    participant NET as RPNet
    participant GRP as Local Grouper

    Note over GRP: Accumulation Phase

    NET->>S1: Read row
    S1-->>NET: {brand:"Nike", __count:15}
    NET-->>GRP: SearchResult {brand:"Nike", __count:15}
    Note over GRP: hash("Nike") → Create Group<br/>SUM.Add(__count=15) → total=15

    NET->>S1: Read row
    S1-->>NET: {brand:"Adidas", __count:8}
    NET-->>GRP: SearchResult {brand:"Adidas", __count:8}
    Note over GRP: hash("Adidas") → Create Group<br/>SUM.Add(__count=8) → total=8

    NET->>S2: Read row
    S2-->>NET: {brand:"Nike", __count:22}
    NET-->>GRP: SearchResult {brand:"Nike", __count:22}
    Note over GRP: hash("Nike") → Existing Group<br/>SUM.Add(__count=22) → total=37

    NET->>S2: Read row
    S2-->>NET: {brand:"Adidas", __count:5}
    NET-->>GRP: SearchResult {brand:"Adidas", __count:5}
    Note over GRP: hash("Adidas") → Existing Group<br/>SUM.Add(__count=5) → total=13

    NET->>S3: Read row
    S3-->>NET: {brand:"Nike", __count:10}
    NET-->>GRP: SearchResult {brand:"Nike", __count:10}
    Note over GRP: hash("Nike") → Existing Group<br/>SUM.Add(__count=10) → total=47

    NET-->>GRP: RS_RESULT_EOF

    Note over GRP: Yield Phase

    GRP-->>GRP: Finalize SUM for Nike → cnt=47
    GRP-->>GRP: Finalize SUM for Adidas → cnt=13
```

### WITHCOUNT and ShardResponseBarrier

When `WITHCOUNT` is specified, the coordinator needs an **accurate total result count**
from the start of the response. The `ShardResponseBarrier` mechanism ensures this:

```mermaid
sequenceDiagram
    participant Coord as Coordinator (rpnetNext)
    participant Barrier as ShardResponseBarrier
    participant IO as I/O Threads
    participant S1 as Shard 1
    participant S2 as Shard 2
    participant S3 as Shard 3

    Note over Barrier: numShards=0 (not initialized yet)

    Coord->>IO: MR_Iterate() — dispatch command
    IO->>S1: _FT.AGGREGATE
    IO->>S2: _FT.AGGREGATE
    IO->>S3: _FT.AGGREGATE

    IO->>Barrier: shardResponseBarrier_Init(numShards=3)

    S1-->>IO: Reply (total=42)
    IO->>Barrier: Notify(shard=0, total=42)
    Note over Barrier: numResponded=1, accumulatedTotal=42

    S3-->>IO: Reply (total=18)
    IO->>Barrier: Notify(shard=2, total=18)
    Note over Barrier: numResponded=2, accumulatedTotal=60

    Note over Coord: getNextReply() waits...<br/>numResponded(2) < numShards(3)

    S2-->>IO: Reply (total=35)
    IO->>Barrier: Notify(shard=1, total=35)
    Note over Barrier: numResponded=3, accumulatedTotal=95

    Note over Coord: All shards responded!<br/>totalResults = 95

    Coord->>Coord: Process pending replies
```

---

## Data Flow Summary: What Moves Where

```mermaid
flowchart LR
    subgraph "Client"
        CMD["FT.AGGREGATE idx '*'<br/>GROUPBY 1 @brand<br/>REDUCE COUNT 0 AS cnt"]
    end

    subgraph "Coordinator"
        SPLIT["Split Plan"]
        SERIALIZE["Serialize remote plan"]
        RPNET["RPNet receives<br/>partial groups"]
        MERGE["Local Grouper<br/>merges with SUM"]
        REPLY["Final reply<br/>{brand, cnt}"]
    end

    subgraph "Shard 1"
        S1["Scan + Group<br/>{brand:Nike, __count:15}<br/>{brand:Adidas, __count:8}"]
    end

    subgraph "Shard 2"
        S2["Scan + Group<br/>{brand:Nike, __count:22}<br/>{brand:Adidas, __count:5}"]
    end

    subgraph "Shard 3"
        S3["Scan + Group<br/>{brand:Nike, __count:10}"]
    end

    CMD --> SPLIT --> SERIALIZE
    SERIALIZE -->|"_FT.AGGREGATE<br/>GROUPBY @brand<br/>REDUCE COUNT 0 AS __count"| S1 & S2 & S3

    S1 -->|"{brand, __count} rows"| RPNET
    S2 -->|"{brand, __count} rows"| RPNET
    S3 -->|"{brand, __count} rows"| RPNET

    RPNET --> MERGE --> REPLY
    REPLY -->|"[{brand:Nike, cnt:47},<br/>{brand:Adidas, cnt:13}]"| CMD
```

### Network Data Size Considerations

The distribution strategy is designed to minimize network traffic:

1. **Shards group locally** — instead of sending every raw document, shards send one row
   per unique group key. For high-cardinality fields this still produces many rows, but
   for typical group-by fields (category, status, region) this is a massive reduction.

2. **Pre-sorting and limiting on shards** — if the original query has `SORTBY ... LIMIT`,
   the shards also sort and limit, sending at most `offset + limit` rows per shard instead
   of all groups.

3. **Temporary aliases** — distributed reducers use `__`-prefixed aliases (e.g., `__count`)
   as internal column names. These are hidden from the final output.

---

## Deduplication of Remote Reducers

When a query uses multiple reducers that decompose to the same shard-side reducer, the
distribution logic avoids duplication. For example:

```
GROUPBY 1 @brand REDUCE COUNT 0 AS cnt REDUCE AVG 1 @price AS avg_price
```

Both `COUNT` and `AVG` need a `COUNT` on the remote side. The `addRemote()` method in
`ReducerDistCtx` checks for existing reducers with the same name and arguments before
adding a new one:

```mermaid
flowchart TD
    AVG["Distribute AVG"]
    NEED_COUNT["Need remote COUNT 0"]
    CHECK["PLNGroupStep_FindReducer(remote, 'COUNT', args)"]
    FOUND{"Already exists?"}
    REUSE["Reuse existing __count alias"]
    ADD["Add new COUNT reducer"]

    AVG --> NEED_COUNT --> CHECK --> FOUND
    FOUND -->|"yes"| REUSE
    FOUND -->|"no"| ADD
```

This prevents sending redundant `COUNT` reducers when `COUNT` and `AVG` are used together.

---

## Error and Timeout Handling

### Shard Errors

If any shard returns an error during the collection phase:

1. `ShardResponseBarrier` sets `hasShardError = true`
2. The error reply is extracted from pending replies
3. The error is propagated to `QueryProcessingCtx.err`
4. The coordinator returns `RS_RESULT_ERROR`

### Timeouts

Two timeout scenarios:

1. **Waiting for first responses:** If not all shards respond within the timeout while
   using WITHCOUNT, the barrier check triggers `RS_RESULT_TIMEDOUT`
2. **During row processing:** Each call to `rpnetNext()` checks the query timeout. If
   expired, the `MRIteratorCtx` is flagged and the iterator sends `CURSOR DEL` to shards
   instead of `CURSOR READ`.

### Reducers That Can't Be Distributed

Some reducers (like `COUNT_DISTINCT`, `FIRST_VALUE`) don't have registered distribution
functions. If the coordinator encounters one, `getDistributionFunc()` returns `NULL` and
the step cannot be distributed — the GROUP step stays in the local plan only. This means
all raw rows must be sent from shards (with just LOAD), and grouping happens entirely on
the coordinator. This falls back gracefully but with higher network cost.
