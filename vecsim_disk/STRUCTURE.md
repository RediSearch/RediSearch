# VecSim Disk Structure

Disk-based HNSW vector similarity indexes for RediSearchDisk.

## Where Code Lives

| Component | Location | Language | Purpose |
|-----------|----------|----------|---------|
| HNSW algorithm | `vecsim_disk/include/hnsw_disk.h` | C++ | Disk-based HNSW implementation |
| Rust bindings | `redisearch_disk/src/vecsim_disk.rs` | Rust | Declares C API for Rust (storage lives in Rust) |
| C API | `vecsim_disk/include/vecsim_disk_api.h` | C | FFI boundary (Rust can't call C++ directly) |
| Unit tests | `vecsim_disk/tests/unit/` | C++ | Unit tests for C++ code |
| Flow tests | `flow-tests/test_vecsim_disk.py` | Python | Integration tests via Redis |
| Benchmarks | `vecsim_disk/benchmarks/` | C++ | Performance benchmarks |

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

## Key Files

```
vecsim_disk/
├── include/
│   ├── vecsim_disk_api.h    # C API
│   └── hnsw_disk.h          # HNSWDiskIndex class
├── src/
│   └── api.cpp              # C API implementation
├── tests/
│   └── unit/                # Unit tests (C++)
└── benchmarks/              # Benchmarks go here

flow-tests/
└── test_vecsim_disk.py      # Flow tests (require Redis with bigredis)
```

## Build

`vecsim_disk` compiles to a static library (`libvecsim_disk.a`) that gets linked into the final Redis module:

```
libvecsim_disk.a  ────┐
libredisearch.a   ────┼──→  redisearch.so  ──→  redis-server --loadmodule redisearch.so
libredisearch_disk.a ─┘
```

### Using build.sh (from repo root)

| Command | What it does |
|---------|--------------|
| `./build.sh build` | Build everything |
| `./build.sh clean` | Remove build artifacts |
| `./build.sh lint` | Lint Rust code (redisearch_disk) |
| `./build.sh lint-vecsim` | Lint C++ code (vecsim_disk) |
| `./build.sh format-vecsim` | Fix C++ formatting (vecsim_disk) |
| `./build.sh test` | Run Rust unit tests (redisearch_disk) |
| `./build.sh test-vecsim` | Run C++ unit tests (vecsim_disk) |
| `./build.sh test-miri` | Run Rust tests with Miri |
| `./build.sh test-flow` | Run integration tests with RLTest |
| `./build.sh bench` | Run Rust micro benchmarks |
| `./build.sh profile` | Profile the module with VTune |

## CI Workflows

vecsim_disk tests and linting run as part of the main CI workflows:

| Workflow | File | What it runs |
|----------|------|--------------|
| Tests | `.github/workflows/task-test.yml` | `./build.sh test-vecsim` |
| Linting | `.github/workflows/task-lint.yml` | `./build.sh lint-vecsim` |

### Flow Tests

Flow tests require Redis with bigredis support:

```bash
cd flow-tests
python runtests.py -t test_vecsim_disk.py
```

### Prerequisites for Flow Tests

The flow tests require Redis with **bigredis** support and the **SpeedB driver** (`bs_speedb.so`).
These are built from the private Redis fork.

### 1. Clone and Build Redis with SpeedB

```bash
# Clone the private Redis fork
git clone git@github.com:redislabsdev/Redis.git ~/redis-private
cd ~/redis-private
git checkout rl_big2_8.4

# Initialize submodules (includes SpeedB)
git submodule update --init --recursive

# Build Redis with SpeedB support
make -j$(nproc) BUILD_TLS=yes

# Install Redis and the SpeedB driver
sudo make install
```
