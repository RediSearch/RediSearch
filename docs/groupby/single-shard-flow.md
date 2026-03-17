# Single-Shard GROUPBY Flow

This document provides a complete end-to-end walkthrough of how a `GROUPBY` clause in
`FT.AGGREGATE` is processed on a single shard, from command reception to reply serialization.

## End-to-End Overview

```mermaid
flowchart TB
    CMD["Client sends:<br/>FT.AGGREGATE idx '*'<br/>GROUPBY 1 @brand<br/>REDUCE COUNT 0 AS cnt<br/>SORTBY 2 @cnt DESC<br/>LIMIT 0 10"]

    subgraph "Phase 1: Parse & Plan"
        PARSE["parseAggPlan()<br/>Tokenize arguments"]
        PLAN["AGGPlan built:<br/>PLN_GroupStep → PLN_ArrangeStep"]
    end

    subgraph "Phase 2: Compile & Validate"
        COMPILE["AREQ_Compile()<br/>Parse query, apply context"]
        VALIDATE["Schema validation<br/>Reducer factory lookup"]
    end

    subgraph "Phase 3: Build Pipeline"
        BUILD["AREQ_BuildPipeline()<br/>→ Pipeline_BuildQueryPart()<br/>→ Pipeline_BuildAggregationPart()"]
        CHAIN["ResultProcessor chain:<br/>RPQueryIterator → RPLoader → Grouper → RPSorter → RPPager"]
    end

    subgraph "Phase 4: Execute"
        EXEC["sendChunk()<br/>Loop: endProc→Next(result)"]
        ACCUM["Grouper accumulates all rows"]
        YIELD["Grouper yields groups"]
        SERIAL["serializeResult() → Redis reply"]
    end

    CMD --> PARSE --> PLAN --> COMPILE --> VALIDATE --> BUILD --> CHAIN --> EXEC
    EXEC --> ACCUM --> YIELD --> SERIAL
```

---

## Phase 1: Parsing

**Entry point:** `RSAggregateCommand()` in `src/module.c`  
**Core parser:** `parseAggPlan()` in `src/aggregate/aggregate_request.c`

### Command Parsing Flow

When Redis receives `FT.AGGREGATE`, the module command handler creates an `AREQ`
(Aggregate Request) struct and passes the arguments to `parseAggPlan()`.

```mermaid
sequenceDiagram
    participant Redis
    participant Module as module.c
    participant Parser as aggregate_request.c
    participant Plan as AGGPlan

    Redis->>Module: RSAggregateCommand(ctx, argv, argc)
    Module->>Module: AREQ_New() — allocate request
    Module->>Parser: parseAggPlan(plan, ac, status)

    loop For each token in args
        Parser->>Parser: AC_GetStringNC() — read token
        alt Token == "GROUPBY"
            Parser->>Parser: parseGroupby(plan, ac, status)
            Note over Parser: 1. Read nproperties<br/>2. Read property names (@brand)<br/>3. Create PLN_GroupStep<br/>4. Parse REDUCE clauses
        else Token == "SORTBY"
            Parser->>Parser: parseSortby(plan, ac, status)
        else Token == "LIMIT"
            Parser->>Parser: parseLimit(plan, ac)
        else Token == "APPLY"
            Parser->>Parser: parseApply(plan, ac, status)
        else Token == "FILTER"
            Parser->>Parser: parseFilter(plan, ac, status)
        else Token == "LOAD"
            Parser->>Parser: parseLoad(plan, ac, status)
        end
    end

    Parser->>Plan: Steps linked list populated
    Parser-->>Module: REDISMODULE_OK
```

### parseGroupby() Detail

The `parseGroupby()` function (lines 882–930 of `aggregate_request.c`) performs:

1. **Read property count:** `nproperties = AC_GetLongLong()`
2. **Read property names:** For each property, reads a string starting with `@` (e.g., `@brand`)
3. **Create step:** `PLNGroupStep_New(properties_ref, strictPrefix)` — allocates the
   `PLN_GroupStep` and stores the property array
4. **Parse reducers:** Loops while the next token is `REDUCE`:
   - Reads the reducer name (e.g., `COUNT`, `SUM`)
   - Reads `nargs` (number of arguments)
   - Reads the arguments
   - Optionally reads `AS alias`
   - Calls `PLNGroupStep_AddReducer(gstp, name, args, status)`
5. **Add to plan:** Sets the `QEXEC_F_HAS_GROUPBY` flag and appends the step

### Resulting Plan Structure

For the command:
```
FT.AGGREGATE idx '*' GROUPBY 1 @brand REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC LIMIT 0 10
```

```mermaid
graph LR
    R["PLN_FirstStep<br/>(root)<br/>RLookup: index schema"]
    G["PLN_GroupStep<br/>properties: ['@brand']<br/>reducers: [{name:'COUNT', alias:'cnt'}]<br/>RLookup: {brand, cnt}"]
    A["PLN_ArrangeStep<br/>sortKeys: ['@cnt']<br/>sortAscMap: DESC<br/>offset: 0, limit: 10"]

    R --> G --> A
```

---

## Phase 2: Compilation and Validation

**Entry point:** `AREQ_Compile()` in `src/aggregate/aggregate.c`

After parsing, the request is compiled. This involves:

1. **Query parsing:** The query string (`*`) is parsed into a `QueryAST`
2. **Context application:** `AREQ_ApplyContext()` validates the plan against the index schema
3. **Reducer validation:** Each reducer name is checked against the factory registry
   (`RDCR_GetFactory()`) to ensure it exists
4. **Key resolution:** Source keys for the GROUPBY properties are resolved against the
   upstream `RLookup` to verify the fields exist (or can be loaded)

---

## Phase 3: Pipeline Construction

**Entry point:** `AREQ_BuildPipeline()` → `Pipeline_BuildAggregationPart()`  
**File:** `src/pipeline/pipeline_construction.c`

This is where the logical `AGGPlan` is converted into the physical `ResultProcessor` chain.

### Building the GROUPBY Processor

```mermaid
sequenceDiagram
    participant Build as Pipeline_BuildAggregationPart
    participant GetGRP as getGroupRP()
    participant BuildGRP as buildGroupRP()
    participant Factory as RDCR_GetFactory()
    participant Push as pushRP()

    Build->>Build: Iterate over plan steps
    Note over Build: Step type = PLN_T_GROUP

    Build->>GetGRP: getGroupRP(pipeline, params, gstp, rpUpstream, status)
    GetGRP->>GetGRP: lookup = AGPLN_GetLookup(PREV)<br/>firstLk = AGPLN_GetLookup(FIRST)
    GetGRP->>BuildGRP: buildGroupRP(gstp, lookup, &loadKeys, err)

    loop For each property in GROUPBY
        BuildGRP->>BuildGRP: srckeys[i] = RLookup_GetKey_Read(lookup, name)
        BuildGRP->>BuildGRP: dstkeys[i] = RLookup_GetKey_Write(&gstp->lookup, name)
    end

    BuildGRP->>BuildGRP: grp = Grouper_New(srckeys, dstkeys, nproperties)

    loop For each PLN_Reducer
        BuildGRP->>Factory: ff = RDCR_GetFactory(reducer_name)
        Factory-->>BuildGRP: ReducerFactory function pointer
        BuildGRP->>BuildGRP: rr = ff(&options) — create Reducer
        BuildGRP->>BuildGRP: dstkey = RLookup_GetKey_Write(&gstp->lookup, alias)
        BuildGRP->>BuildGRP: Grouper_AddReducer(grp, rr, dstkey)
    end

    BuildGRP-->>GetGRP: Grouper_GetRP(grp) — returns &grp->base

    opt If loadKeys needed (fields not in sorting vector)
        GetGRP->>Push: pushRP(RPLoader) before Grouper
    end

    GetGRP->>Push: pushRP(Grouper) into chain
```

### Key Resolution: srckeys and dstkeys

The `buildGroupRP()` function resolves keys across two different `RLookup` tables:

| Key Array | Lookup Table | Purpose |
|-----------|-------------|---------|
| `srckeys[]` | Previous step's `RLookup` (upstream) | Read field values from incoming rows |
| `dstkeys[]` | `PLN_GroupStep.lookup` (this step) | Write group key values to output rows |

If a srckey cannot be found for reading (the field isn't in the sorting vector or already
loaded), and the field is in the index schema, an implicit `RPLoader` is inserted before
the Grouper to load that field from Redis.

### Resulting Pipeline

```mermaid
graph LR
    subgraph "ResultProcessor Chain"
        QI["RPQueryIterator<br/>type: RP_INDEX<br/>Pulls doc IDs"]
        LD["RPLoader<br/>type: RP_LOADER<br/>Loads @brand from Redis"]
        GR["Grouper<br/>type: RP_GROUP<br/>srckeys: [brand_src]<br/>dstkeys: [brand_dst]<br/>reducers: [COUNT]"]
        SO["RPSorter<br/>type: RP_SORTER<br/>Sorts by @cnt DESC"]
        PG["RPPager<br/>type: RP_PAGER_LIMITER<br/>offset=0, limit=10"]

        QI --> LD --> GR --> SO --> PG
    end

    QI -.-> |"upstream"| LD
    LD -.-> |"upstream"| GR
    GR -.-> |"upstream"| SO
    SO -.-> |"upstream"| PG

    style GR fill:#ff9,stroke:#333,stroke-width:2px
```

> Note: The arrows represent the `upstream` pointers — each processor points to the one
> *before* it. Execution flows **right to left**: the tail (RPPager) calls
> `upstream->Next()`, which calls the Sorter, which calls the Grouper, and so on.

---

## Phase 4: Execution

**Entry point:** `sendChunk()` in `src/aggregate/aggregate_exec.c`

### Execution Loop

```mermaid
flowchart TD
    START["sendChunk(req, reply)"]
    INIT["Get endProc (tail of chain)<br/>Initialize reply array"]

    CALL["rc = endProc->Next(endProc, &result)"]

    CHECK{rc?}
    OK["RS_RESULT_OK<br/>serializeResult(req, reply, &result, cv)<br/>SearchResult_Clear(&result)<br/>nrows++"]
    EOF["RS_RESULT_EOF<br/>Done — finalize reply"]
    TIMEOUT["RS_RESULT_TIMEDOUT<br/>Handle timeout policy"]
    ERROR["RS_RESULT_ERROR<br/>Return error"]

    NEXT{nrows < limit?}

    START --> INIT --> CALL --> CHECK
    CHECK -->|OK| OK --> NEXT
    CHECK -->|EOF| EOF
    CHECK -->|TIMEDOUT| TIMEOUT
    CHECK -->|ERROR| ERROR
    NEXT -->|yes| CALL
    NEXT -->|no| EOF
```

### Inside the Grouper: Accumulation Phase

When `RPSorter` (or whatever is downstream) first calls `Grouper.Next()`, the Grouper
enters its accumulation phase (`Grouper_rpAccum`):

```mermaid
flowchart TD
    ENTRY["Grouper_rpAccum(base, res)"]
    SAVE["Save chunkLimit = parent->resultLimit<br/>Set parent->resultLimit = UINT32_MAX"]

    LOOP["rc = upstream->Next(upstream, res)"]
    CHECK{rc == OK?}

    PROCESS["invokeGroupReducers(g, res->_row_data)"]
    CLEAR["SearchResult_Clear(res)"]

    RESTORE["Restore parent->resultLimit = chunkLimit"]
    EOFCHECK{rc == EOF?}
    SWITCH["Switch: base->Next = Grouper_rpYield<br/>totalResults = kh_size(groups)<br/>iter = kh_begin"]
    YIELD["return Grouper_rpYield(base, res)"]
    RETURN["return rc (error/timeout)"]

    ENTRY --> SAVE --> LOOP --> CHECK
    CHECK -->|"OK"| PROCESS --> CLEAR --> LOOP
    CHECK -->|"not OK"| RESTORE --> EOFCHECK
    EOFCHECK -->|"EOF"| SWITCH --> YIELD
    EOFCHECK -->|"error"| RETURN
```

### Inside the Grouper: Per-Row Processing

For each incoming row, `invokeGroupReducers()` performs these steps:

```mermaid
flowchart TD
    IGR["invokeGroupReducers(g, srcrow)"]
    READ["For each srckey:<br/>groupvals[i] = RLookupRow_Get(srckey, srcrow)<br/>If NULL → RSValue_NullStatic()"]
    EXTRACT["extractGroups(g, groupvals, 0, nkeys, hash=0, srcrow)"]

    RECURSE{"pos < nkeys?"}
    GETVAL["v = RSValue_Dereference(groupvals[pos])"]
    ISARR{"IsArray(v)?"}

    SCALAR["hash = RSValue_Hash(v, hash)<br/>Recurse with pos+1"]

    EMPTYARR{"ArrayLen == 0?"}
    NULLHASH["Replace with NULL<br/>hash = RSValue_Hash(NULL, hash)<br/>Recurse with pos+1"]

    ARRAYLOOP["For each element in array:<br/>Replace groupvals[pos] with element<br/>hash element, recurse with pos+1<br/>Restore groupvals[pos]"]

    LOOKUP["pos == nkeys:<br/>k = kh_get(groups, hash)"]
    EXISTS{"Group exists?"}
    CREATE["group = createGroup(g, groupvals, nkeys)<br/>For each reducer: group->accumdata[i] = NewInstance()<br/>Write group key values to group->rowdata"]
    USE["group = kh_value(groups, k)"]
    INVOKE["invokeReducers(g, group, srcrow)<br/>For each reducer: reducer->Add(reducer, accumdata[i], srcrow)"]

    IGR --> READ --> EXTRACT --> RECURSE
    RECURSE -->|"yes"| GETVAL --> ISARR
    ISARR -->|"no (scalar)"| SCALAR
    ISARR -->|"yes"| EMPTYARR
    EMPTYARR -->|"yes"| NULLHASH
    EMPTYARR -->|"no"| ARRAYLOOP
    RECURSE -->|"no (all keys processed)"| LOOKUP --> EXISTS
    EXISTS -->|"yes"| USE --> INVOKE
    EXISTS -->|"no"| CREATE --> INVOKE
```

### Inside the Grouper: Yield Phase

After EOF from upstream, the Grouper switches to `Grouper_rpYield` and iterates over
its hash map, emitting one `SearchResult` per group:

```mermaid
flowchart TD
    YIELD["Grouper_rpYield(base, res)"]
    ITER["Advance iterator to next valid slot"]
    CHECK{"iter != end?"}

    GETGROUP["gr = kh_value(groups, iter)"]
    WRITEKEYS["writeGroupValues(g, gr, res)<br/>For each dstkey:<br/>  groupval = RLookupRow_Get(dstkey, &gr->rowdata)<br/>  RLookup_WriteKey(dstkey, &res->_row_data, groupval)"]
    REDUCERS["For each reducer:<br/>  v = reducer->Finalize(reducer, gr->accumdata[i])<br/>  RLookup_WriteOwnKey(reducer->dstkey, &res->_row_data, v)"]
    ADVANCE["iter++"]
    RETURN["return RS_RESULT_OK"]

    EOF["return RS_RESULT_EOF"]

    YIELD --> ITER --> CHECK
    CHECK -->|"yes"| GETGROUP --> WRITEKEYS --> REDUCERS --> ADVANCE --> RETURN
    CHECK -->|"no"| EOF
```

### Concrete Example: Data Flow

Consider this data and query:

```
Documents:
  doc:1 → {brand: "Nike",   price: 100}
  doc:2 → {brand: "Nike",   price: 80}
  doc:3 → {brand: "Adidas", price: 120}
  doc:4 → {brand: "Nike",   price: 90}
  doc:5 → {brand: "Adidas", price: 110}

Query:
  FT.AGGREGATE idx '*' GROUPBY 1 @brand REDUCE COUNT 0 AS cnt REDUCE AVG 1 @price AS avg_price
```

```mermaid
sequenceDiagram
    participant QI as RPQueryIterator
    participant LD as RPLoader
    participant GR as Grouper
    participant SO as (downstream)

    Note over GR: Phase 1: Accumulation

    SO->>GR: Next(result)
    GR->>LD: Next(result)
    LD->>QI: Next(result)
    QI-->>LD: doc:1 {brand:Nike, price:100}
    LD-->>GR: row: {brand:Nike, price:100}
    Note over GR: hash("Nike")=0xA1<br/>Create Group(Nike)<br/>COUNT.Add → count=1<br/>AVG.Add → total=100, count=1

    GR->>LD: Next(result)
    LD-->>GR: doc:2 {brand:Nike, price:80}
    Note over GR: hash("Nike")=0xA1 → existing<br/>COUNT.Add → count=2<br/>AVG.Add → total=180, count=2

    GR->>LD: Next(result)
    LD-->>GR: doc:3 {brand:Adidas, price:120}
    Note over GR: hash("Adidas")=0xB2<br/>Create Group(Adidas)<br/>COUNT.Add → count=1<br/>AVG.Add → total=120, count=1

    GR->>LD: Next(result)
    LD-->>GR: doc:4 {brand:Nike, price:90}
    Note over GR: hash("Nike")=0xA1 → existing<br/>COUNT.Add → count=3<br/>AVG.Add → total=270, count=3

    GR->>LD: Next(result)
    LD-->>GR: doc:5 {brand:Adidas, price:110}
    Note over GR: hash("Adidas")=0xB2 → existing<br/>COUNT.Add → count=2<br/>AVG.Add → total=230, count=2

    GR->>LD: Next(result)
    LD-->>GR: RS_RESULT_EOF

    Note over GR: Switch to yield phase<br/>totalResults = 2

    Note over GR: Phase 2: Yield

    GR-->>SO: {brand:"Nike", cnt:3, avg_price:90.0}
    SO->>GR: Next(result)
    GR-->>SO: {brand:"Adidas", cnt:2, avg_price:115.0}
    SO->>GR: Next(result)
    GR-->>SO: RS_RESULT_EOF
```

### Result Serialization

After the pipeline emits a group row, `serializeResult()` iterates over the output
`RLookup` keys and writes field-value pairs to the Redis reply:

**RESP2 format:**
```
*3                          # 3 elements: total + 2 rows
:2                          # total_results = 2
*4                          # row 1: 4 elements (2 field-value pairs)
$5 brand                    # field name
$4 Nike                     # field value
$3 cnt                      # field name
$1 3                        # field value  
*4                          # row 2
$5 brand
$6 Adidas
$3 cnt
$1 2
```

**RESP3 format:**
```
%2                          # Map with 2 keys
$7 results                  # "results" key
*2                          # Array of 2 result maps
%2                          # Result 1
$5 brand $4 Nike
$3 cnt :3
%2                          # Result 2  
$5 brand $6 Adidas
$3 cnt :2
...
```

---

## Special Cases

### Array Values in Group Keys

When a group-by field contains an array value (e.g., a multi-value TAG field), the
`extractGroups()` function performs a **cartesian product expansion**. Each element of the
array is treated as a separate group key value.

**Example:**
```
doc:1 → {tags: ["sports", "running"], brand: "Nike"}

GROUPBY 2 @tags @brand
```

This produces two group entries from a single document:
- Group key: `("sports", "Nike")`
- Group key: `("running", "Nike")`

Both groups receive the reducer `Add()` call for `doc:1`.

### Empty Results

If the upstream produces no rows (empty query result), the Grouper immediately receives
EOF, switches to yield mode with `totalResults = 0`, and returns EOF to the downstream
processor without emitting any groups.

### resultLimit Manipulation

The Grouper temporarily overrides `parent->resultLimit` to `UINT32_MAX` during
accumulation to ensure it consumes **all** upstream rows. This is critical because
`resultLimit` is used by upstream processors (like the safe loader) to decide how many
results to buffer. After accumulation, the original limit is restored.
