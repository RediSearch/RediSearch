# vecsim_disk - C++ Disk-Based Vector Indexes

Disk-based HNSW vector similarity indexes for RediSearchEnterprise.

## Code Style

- C++20 standard
- Use clang-format for formatting
- Follow Google C++ style guidelines

## Project Structure

```
include/
├── vecsim_disk_api.h    # C API (FFI boundary)
├── speedb_c_wrappers.h  # SpeedB C wrappers
└── hnsw_disk.h          # HNSWDiskIndex class
src/
├── algorithms/          # HNSW disk implementation
├── storage/             # SpeedB storage layer
└── vecsim_disk_api.cpp  # C API implementation
tests/unit/              # C++ unit tests
benchmarks/              # Performance benchmarks
```

## Integration Flow

```
RediSearch (C) → SearchDisk_CreateVectorIndex()
                        ↓
redisearch_disk (Rust) → VectorDiskAPI.createVectorIndex
                        ↓
vecsim_disk (C++) → VecSimDisk_CreateIndex()
                        ↓
                  HNSWDiskIndex : VecSimIndexAbstract
                        ↓
                  Returns VecSimIndex*
                        ↓
RediSearch (C) → VecSimIndex_AddVector(), VecSimIndex_TopKQuery(), etc.
```

After creation, all operations use standard VectorSimilarity API via polymorphism.

## Where Code Lives

| Component | Location | Language | Purpose |
|-----------|----------|----------|---------|
| HNSW algorithm | `include/hnsw_disk.h` | C++ | Disk-based HNSW implementation |
| Rust bindings | `../redisearch_disk/src/vecsim_disk.rs` | Rust | Declares C API for Rust |
| C API | `include/vecsim_disk_api.h` | C | FFI boundary |
| Unit tests | `tests/unit/` | C++ | Unit tests for C++ code |
| Flow tests | `../flow-tests/test_vecsim_disk.py` | Python | Integration tests via Redis |
| Benchmarks | `benchmarks/` | C++ | Performance benchmarks |

## Dependencies

Uses `deps/RediSearch/deps/VectorSimilarity/` for base vector index classes and algorithms.

## Build & Test

```bash
# From repo root
./build.sh build          # Full build
./build.sh test-vecsim    # Run C++ unit tests
./build.sh bench-vecsim   # Run benchmarks
```

## Linting & Formatting

```bash
./build.sh lint-vecsim    # clang-format check
./build.sh format-vecsim  # Fix formatting
```

## CI Workflows

| Workflow | File | What it runs |
|----------|------|--------------|
| Tests | `.github/workflows/task-test.yml` | `./build.sh test-vecsim` |
| Linting | `.github/workflows/task-lint.yml` | `./build.sh lint-vecsim` |

## See Also

- [Main AGENTS.md](../AGENTS.md) - Project overview
- [Build Skill](../.skills/build/SKILL.md) - Detailed build instructions
- [Lint Skill](../.skills/lint_and_format/SKILL.md) - Linting instructions

