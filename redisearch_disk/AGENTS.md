# redisearch_disk - Rust Disk Storage Layer

Rust library providing disk-based storage for RediSearch indexes using SpeedB.

## Code Style

- Edition 2024
- Document all `unsafe` blocks with `// SAFETY:` comments
- Use `#[expect(...)]` over `#[allow(...)]` for lint suppressions
- Use `tracing` macros for logging (debug!, info!, warn!, error!)

## Project Structure

```
src/
├── lib.rs               # FFI exports (SearchDisk_GetAPI)
├── database.rs          # SpeedB database wrapper
├── disk_context.rs      # Global disk context
├── index_spec/          # Per-index structures
│   ├── doc_table/       # Document table (metadata + async reads)
│   ├── inverted_index/  # Term and tag inverted indexes
│   └── deleted_ids.rs   # Deleted document ID tracking
├── metrics/             # Disk metrics collection
└── vecsim_disk.rs       # C++ vector index FFI bindings
tests/                   # Integration tests
benches/                 # Micro benchmarks
```

## Key Concepts

### DiskContext
Global context managing SpeedB database and shared resources (cache, write buffer manager).

### IndexSpec
Per-index handle with column families for doc_table, inverted_index, etc.

### DocTable
Stores document metadata (key, score, flags, expiration). Supports async reads via io_uring.

### InvertedIndex
Term-to-document mappings for full-text search. Uses merge operators for efficient updates.

## FFI Bridge

RediSearch (C) calls into redisearch_disk (Rust) via `SearchDisk_GetAPI()` which returns function pointers:

- `BasicDiskAPI`: open/close, RDB save/load
- `IndexDiskAPI`: document indexing, iterators, GC
- `DocTableDiskAPI`: document metadata, async reads
- `VectorDiskAPI`: disk-based vector index creation
- `MetricsDiskAPI`: metrics collection

## Build & Test

```bash
# From repo root
./build.sh build          # Full build
./build.sh test           # Run unit tests
./build.sh test-miri      # Run tests with Miri
./build.sh bench          # Run micro benchmarks

# Fast iteration (after initial full build)
cargo build
cargo test
```

## Linting & Formatting

```bash
./build.sh lint           # clippy + fmt check
./build.sh format         # Fix formatting
```

## See Also

- [Main AGENTS.md](../AGENTS.md) - Project overview
- [Build Skill](../.skills/build/SKILL.md) - Detailed build instructions
- [Lint Skill](../.skills/lint_and_format/SKILL.md) - Linting instructions

