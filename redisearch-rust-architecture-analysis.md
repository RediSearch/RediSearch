# RediSearch Rust Architecture: RLookup, ResultProcessor, and SearchResult

## Overview

The Rust layer (`src/redisearch_rs/`) provides safe wrappers around C types using pinning, lifetimes, and RAII to prevent memory safety issues while maintaining C interoperability.

---

## 1. RLookup (`src/redisearch_rs/rlookup/`)

### Key Types

**RLookup<'a>**: Field name registry mapping strings to integer indices
**RLookupKey<'a>**: Self-referential struct with owned strings and internal pointers (requires pinning)
**RLookupRow<'a, T>**: Data container with dual-source access (sorting vectors + dynamic values)

### Responsibility

- **Field registry**: O(1) lookups by mapping field names → indices
- **Schema integration**: Resolves fields against index schema
- **Data abstraction**: Unified access to cached (sorting vector) vs computed (dynamic) values
- **Memory safety**: Pinning prevents pointer invalidation; lifetimes prevent use-after-free

### Key Patterns

```rust
#[pin_project(!Unpin)]  // Self-referential: header pointers point to owned strings
struct RLookupKey<'a> {
    header: RLookupKeyHeader<'a>,  // Contains raw pointers
    name: Cow<'a, CStr>,            // Owned data that header.name points to
    path: Cow<'a, CStr>,            // Owned data that header.path points to
}
```

**Tombstone overrides**: Instead of modifying keys (would invalidate C pointers), mark old as Hidden and create new

---

## 2. ResultProcessor (`src/redisearch_rs/result_processor/`)

### Key Types

**ResultProcessor** (trait): Core interface with `next(&mut self, cx: Context, res: &mut SearchResult) -> Result<Option<()>, Error>`
**ResultProcessorWrapper<P>**: Generic wrapper combining C-compatible Header with Rust processor
**Context<'a>** / **Upstream<'a>**: Lifetime-bound access to upstream processor and parent context
**Header**: FFI-compatible struct with VTable function pointers matching C layout

### Responsibility

- **FFI bridge**: Translates between C VTable calls and Rust trait dispatch
- **Type safety**: Generic wrapper allows any `P: ResultProcessor` to integrate with C chain
- **Memory safety**: Pinning prevents moves of intrusive linked list nodes
- **Pipeline execution**: Lazy evaluation pulling results through processor chain

### Key Patterns

```rust
#[repr(C)]
struct ResultProcessorWrapper<P> {
    header: Header,        // Must be first - allows casting to *mut Header for C
    result_processor: P,   // Actual Rust implementation
}

// VTable function bridges C → Rust
unsafe extern "C" fn result_processor_next<P>(...) {
    let wrapper = &mut *(ptr as *mut ResultProcessorWrapper<P>);
    let result = wrapper.result_processor.next(...);
    // Translate Result<Option<()>, Error> → RPStatus enum
}
```

**Integration**: C calls VTable → Rust casts pointer back → Trait method executes → Result translates to C enum

---

## 3. SearchResult (`src/redisearch_rs/search_result/`)

### Key Types

**SearchResult<'index>**: Safe wrapper with RAII cleanup of nested allocations
**DocumentMetadata**: Reference-counted wrapper (manual refcount via AtomicU16)

### Responsibility

- **Data container**: Flows through processor chain carrying document data
- **RAII cleanup**: Drop trait automatically frees score_explain, document metadata, row data
- **Reference counting**: Multiple SearchResults can share DocumentMetadata safely
- **FFI by-value**: "Mimic pattern" allows stack-based passing to C without heap allocation

### Key Patterns

```rust
impl Drop for SearchResult<'_> {
    fn drop(&mut self) {
        if let Some(explain) = self.score_explain.take() {
            unsafe { ffi::SEDestroy(explain.as_ptr()) };
        }
        self.document_metadata = None;  // Drops wrapper, decrements refcount
        unsafe { ffi::RLookupRow_Reset(&mut self.row_data); }
    }
}

impl Clone for DocumentMetadata {
    fn clone(&self) -> Self {
        refcount.fetch_add(1, Ordering::Relaxed);  // Manual refcount management
        Self(self.ptr)
    }
}
```

**Mimic pattern**: `SearchResult::into_mimic() → Size72Align8` allows C to receive by-value on stack

---

## How They Work Together

```
Query Execution Flow:
┌─────────────────────────────────────────────────────────┐
│ 1. RLookup created, registers needed fields            │
│    - Maps "title" → dstidx: 0                          │
│    - Maps "score" → svidx: 2 (in sorting vector)       │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ 2. Processor chain constructed (C + Rust mixed)         │
│    RP_INDEX (C) → Counter (Rust) → RP_SORTER (C)       │
│    Each processor references the RLookup               │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ 3. SearchResult flows through chain:                    │
│                                                         │
│  RP_INDEX: Creates SearchResult, sets docId/score      │
│            Initializes SearchResult.rowdata (empty)    │
│            ↓                                            │
│  Counter:  VTable → Rust trait → cx.upstream().next()  │
│            Increments count, passes through            │
│            ↓                                            │
│  RP_SORTER: Reads fields via RLookup:                  │
│            key = RLookup.get_key("score")              │
│            val = SearchResult.rowdata.get(key)         │
│            → Returns value from sorting_vector[2]      │
└─────────────────────────────────────────────────────────┘
```

### Data Access Pattern

```rust
// Processor needs field "title"
let key = rlookup.get_key_read("title");      // Get RLookupKey
let value = search_result.rowdata.get(key);   // Access via key

// RLookupRow.get() checks:
if key.svidx != INVALID {
    // Read from sorting_vector[key.svidx] (cached)
} else {
    // Read from dyn_values[key.dstidx] (computed)
}
```

### Memory Safety Guarantees

| Component | Mechanism | Prevents |
|-----------|-----------|----------|
| **RLookupKey** | Pinning (`#[pin_project(!Unpin)]`) | Pointer invalidation from moves |
| **RLookup** | Lifetimes + tombstones | Use-after-free, C pointer invalidation |
| **ResultProcessor** | Pinning + lifetime-bound Context | Moving linked list nodes, double borrows |
| **SearchResult** | RAII + refcounting | Memory leaks, double-free |

---

## Key Design Principles

1. **Zero-cost FFI**: Rust wrappers compile to same layout as C structs (`#[repr(C)]`, offset assertions)
2. **Pinning for stability**: Self-referential structs and intrusive lists require stable addresses
3. **Gradual migration**: Rust handles safe wrappers; C still implements complex logic
4. **Type safety with erasure**: Generic `ResultProcessorWrapper<P>` erases to `*mut Header` at FFI boundary
5. **Manual resource management**: Reference counting for C-allocated data (IndexSpecCache, DocumentMetadata)

The Rust layer provides memory safety, type safety, and maintainability while maintaining full C interoperability through careful use of pinning, lifetimes, and FFI patterns.
