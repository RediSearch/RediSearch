# RediSearch Architecture Analysis: RLookup, ResultProcessor, and SearchResult

## Executive Summary

These three components form the **core query result processing pipeline** in RediSearch:

1. **SearchResult** - The data container (payload)
2. **ResultProcessor** - The processing pipeline (transformations)
3. **RLookup** - The field management system (schema & data access)

They work together in a lazy-evaluation pipeline where SearchResults flow through a chain of ResultProcessors, with RLookup managing field access and storage.

---

## 1. SearchResult: The Data Container

**Location**: `src/search_result.h`, `src/search_result.c`

### Core Type
```c
typedef struct {
  t_docId docId;                    // Document identifier
  double score;                     // Relevance score
  RSScoreExplain *scoreExplain;    // Score explanation tree
  const RSDocumentMetadata *dmd;   // Document metadata
  const RSIndexResult* indexResult; // Raw index scan data
  RLookupRow rowdata;               // Field values container
  uint8_t flags;                    // Status flags
} SearchResult;
```

### Responsibility
- **Primary container** that flows through the entire result processing pipeline
- Aggregates data from multiple sources: index scan, document metadata, field lookups
- Carries both immutable metadata (dmd) and mutable field data (rowdata)
- Contains scoring information and explanations for debugging

### Key Supporting Types
- **RSDocumentMetadata**: Document properties (key, user score, term frequencies, document type)
- **RSScoreExplain**: Tree structure for score explanations
- **HybridSearchResult**: Extension for multi-source hybrid search

---

## 2. RLookup: The Field Management System

**Location**: `src/rlookup.h`, `src/rlookup.c`

### Core Types

#### **RLookup** (Container)
```c
typedef struct {
  RLookupKey *head, *tail;    // Linked list of keys
  uint32_t rowlen;            // Length of data rows
  uint32_t options;           // Lookup behavior options
  IndexSpecCache *spcache;    // Schema cache reference
} RLookup;
```

#### **RLookupKey** (Field Descriptor)
```c
typedef struct RLookupKey {
  uint16_t dstidx;           // Index into dynamic values array
  uint16_t svidx;            // Index into sorting vector
  uint32_t flags;            // Behavioral flags
  const char *path;          // Field path in document
  const char *name;          // Field name as referenced
  struct RLookupKey *next;   // Linked list pointer
} RLookupKey;
```

#### **RLookupRow** (Data Container)
```c
typedef struct {
  const RSSortingVector *sv;  // Cached sortable field values
  RSValue **dyn;              // Dynamic computed values
  size_t ndyn;                // Count of dynamic values
} RLookupRow;
```

### Responsibility
- **Field registry**: Maps string field names to integer indices (O(1) lookups vs string comparisons)
- **Dual-source data access**: Abstracts accessing data from sorting vectors (cached) vs dynamic values (computed)
- **Schema integration**: Resolves field names against index schema
- **Memory efficiency**: Tracks which fields need loading and where they're stored

### Key Operations
- **Key Management**: Create, find, and merge field descriptors across queries
- **Data Loading**: Load field values from Redis (hash/JSON) into RLookupRow
- **Row Operations**: Read/write values using keys as indirection
- **Field Mapping**: Coordinate field names across different query stages

---

## 3. ResultProcessor: The Processing Pipeline

**Location**: `src/result_processor.h`, `src/result_processor.c`

### Core Types

#### **ResultProcessor** (Base)
```c
typedef struct ResultProcessor {
  QueryProcessingCtx *parent;           // Parent context
  struct ResultProcessor *upstream;     // Previous processor
  ResultProcessorType type;             // Processor type
  int (*Next)(struct ResultProcessor*, SearchResult*);  // Iterator
  void (*Free)(struct ResultProcessor*);                // Cleanup
} ResultProcessor;
```

#### **QueryProcessingCtx** (Pipeline Manager)
```c
typedef struct {
  ResultProcessor *rootProc;      // First processor
  ResultProcessor *endProc;       // Last processor
  uint32_t totalResults;          // Total results found
  uint32_t resultLimit;           // Results per chunk
  double minScore;                // Score threshold
  QueryError *err;                // Error handling
} QueryProcessingCtx;
```

### Processor Types (18+ specialized processors)
- **RP_INDEX**: Root processor reading from index
- **RP_LOADER/RP_SAFE_LOADER**: Load fields from Redis
- **RP_SCORER**: Apply scoring functions
- **RP_SORTER**: Sort results by multiple fields
- **RP_PAGER_LIMITER**: Pagination (offset/limit)
- **RP_HYBRID_MERGER**: Merge multiple search sources
- **RP_PROJECTOR**: Select specific fields
- **RP_FILTER**: Post-query filtering
- **RP_GROUP**: Aggregation grouping
- **RP_HIGHLIGHTER**: Text highlighting

### Responsibility
- **Lazy evaluation pipeline**: Pull results on-demand through processor chain
- **Transformation stages**: Each processor adds specific processing (scoring, sorting, loading)
- **Resource coordination**: Manages Redis locking, threading, memory
- **Performance tracking**: Profiles timing and counts per processor

---

## How They Relate: The Complete Flow

```
┌────────────────────────────────────────────────────────────────┐
│                    Query Execution Pipeline                     │
└────────────────────────────────────────────────────────────────┘

1. Query Planning Phase:
   ┌─────────────┐
   │   RLookup   │ ← Created for query, registers needed fields
   │  (Schema)   │   Maps field names → indices
   └─────────────┘

2. Pipeline Construction:
   ┌─────────────────────────────────────────────────────────────┐
   │ ResultProcessor Chain                                        │
   │                                                              │
   │  RP_INDEX → RP_SCORER → RP_LOADER → RP_SORTER → RP_PAGER   │
   │                                                              │
   │  Each processor holds reference to RLookup                  │
   └─────────────────────────────────────────────────────────────┘

3. Result Flow (Per Document):

   RP_INDEX:
   ┌──────────────┐
   │ SearchResult │ ← Created, populated with docId, score
   │  .docId      │
   │  .score      │
   │  .dmd        │ ← RSDocumentMetadata attached
   │  .rowdata    │ ← RLookupRow initialized (empty)
   └──────────────┘
         ↓
   RP_SCORER:
   ┌──────────────┐
   │ SearchResult │ ← Score recalculated
   │  .score ✓    │   Uses .dmd for term frequencies
   └──────────────┘
         ↓
   RP_LOADER:
   ┌──────────────┐
   │ SearchResult │ ← Fields loaded via RLookup
   │  .rowdata    │
   │   .dyn[]     │ ← RLookup_LoadDocument() populates
   │   .sv        │ ← Or pulls from sorting vector
   └──────────────┘
         ↓
   RP_SORTER:
   - Reads fields using: RLookup_GetItem(key, &result.rowdata)
   - RLookupKey.svidx or .dstidx tells where to find value
   - Accumulates results, sorts, yields in order
         ↓
   RP_PAGER:
   ┌──────────────┐
   │ SearchResult │ ← Paginated result returned to client
   └──────────────┘
```

---

## Key Integration Points

### 1. **SearchResult.rowdata ↔ RLookupRow**
- SearchResult embeds a RLookupRow to hold field data
- RLookupRow provides dual-source access: sorting vectors + dynamic values
- ResultProcessors read/write fields through this container

### 2. **RLookup ↔ ResultProcessor**
- Each ResultProcessor references the query's RLookup
- Loaders use `RLookup_LoadDocument()` to populate SearchResult.rowdata
- Sorters/Projectors use `RLookup_GetItem()` to read field values
- All field access goes through RLookup indirection for performance

### 3. **ResultProcessor Chain ↔ SearchResult Flow**
- ResultProcessor.Next() receives a SearchResult to populate
- Each processor adds/transforms data in the SearchResult
- SearchResult is reused (cleared/recycled) for efficiency
- Pipeline pulls results lazily via Next() iterator pattern

### 4. **HybridSearchResult: Multi-Source Integration**
- RP_HYBRID_MERGER creates HybridSearchResult containers
- Each source (text, vector) produces its own SearchResult
- Hybrid scorer combines scores and merges field data
- Final merged SearchResult continues through pipeline

---

## Design Patterns

1. **Pipeline Pattern**: ResultProcessor chain for composable transformations
2. **Lazy Evaluation**: Results pulled on-demand, not all at once
3. **Indirection**: RLookup provides fast field access without string operations
4. **Virtual Tables**: Function pointers (Next/Free) enable polymorphism in C
5. **Resource Pooling**: SearchResults cleared and reused, not reallocated
6. **Separation of Concerns**:
   - SearchResult = Data container (what)
   - RLookup = Field schema (where)
   - ResultProcessor = Transformations (how)

---

## Summary

These three components implement a sophisticated, high-performance query result pipeline:

- **SearchResult** is the **vehicle** carrying result data
- **RLookup** is the **map** showing where fields are stored
- **ResultProcessor** is the **assembly line** transforming raw results into final output

Together, they enable RediSearch to efficiently process millions of results through complex transformations (scoring, sorting, filtering, field loading) while maintaining low memory overhead and high throughput.
