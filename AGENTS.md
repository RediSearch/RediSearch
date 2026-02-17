# RediSearchEnterprise Development Guide

RediSearchEnterprise extends RediSearch with disk-based index storage using SpeedB (a RocksDB fork).
It provides enterprise-only features including disk-based full-text search indexes and vector similarity indexes.

## Project Architecture

```
redisearch.so (final Redis module)
├── redisearch       (static lib from deps/RediSearch - C codebase, search engine core)
├── redisearch-disk  (static lib from redisearch_disk/ - Rust, disk storage layer)
├── vecsim_disk      (static lib from vecsim_disk/ - C++, disk-based HNSW vector indexes)
└── libspeedb.so     (shared lib from deps/speedb-ent - key-value storage engine)
```

## Build & Testing

See [build skill](.skills/build/SKILL.md) for detailed build and test instructions.

## Linting & Formatting

See [lint skill](.skills/lint_and_format/SKILL.md) for linting and formatting instructions.

## Code Style

### Rust
- Edition 2024
- Document all `unsafe` blocks with `// SAFETY:` comments
- Use `#[expect(...)]` over `#[allow(...)]` for lint suppressions
- Use `tracing` macros for logging (debug!, info!, warn!, error!)

### C++
- C++20 standard
- Use clang-format for formatting
- Follow Google C++ style guidelines

## Subdirectory Guides

For detailed component-specific documentation:

- **[redisearch_disk/AGENTS.md](redisearch_disk/AGENTS.md)** - Rust disk storage layer (code style, structure, key concepts)
- **[vecsim_disk/AGENTS.md](vecsim_disk/AGENTS.md)** - C++ vector indexes (code style, integration flow, CI)

## Project Structure

```
redisearch_disk/              # Rust disk storage layer (see redisearch_disk/AGENTS.md)
vecsim_disk/                  # C++ disk-based vector indexes (see vecsim_disk/AGENTS.md)
deps/
├── RediSearch/              # Core search engine (C + Rust)
│   └── deps/VectorSimilarity/  # Vector similarity library (used by vecsim_disk)
├── speedb-ent/              # SpeedB storage engine
└── rust-speedb/             # Rust bindings for SpeedB
flow-tests/                   # Python integration tests (RLTest)
├── test_*.py                # Test files
├── features/                # BDD scenarios
└── conftest.py              # pytest fixtures
```

## Key Concepts

### Disk Storage Flow

1. **DiskContext**: Global context managing SpeedB database and shared resources
2. **IndexSpec**: Per-index handle with column families for doc_table, inverted_index, etc.
3. **DocTable**: Stores document metadata (key, score, flags, expiration)
4. **InvertedIndex**: Term-to-document mappings for full-text search

### FFI Bridge

RediSearch (C) calls into redisearch_disk (Rust) via `SearchDisk_GetAPI()` which returns function pointers:
- `BasicDiskAPI`: open/close, RDB save/load
- `IndexDiskAPI`: document indexing, iterators, GC
- `DocTableDiskAPI`: document metadata, async reads
- `VectorDiskAPI`: disk-based vector index creation
- `MetricsDiskAPI`: metrics collection

### Vector Index Integration

```
RediSearch (C) → SearchDisk_CreateVectorIndex()
                        ↓
redisearch_disk (Rust) → VectorDiskAPI.createVectorIndex
                        ↓
vecsim_disk (C++) → VecSimDisk_CreateIndex()
                        ↓
                  HNSWDiskIndex : VecSimIndexAbstract
```
