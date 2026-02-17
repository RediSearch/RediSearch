---
name: build
description: Compile the project to verify changes build successfully.
see_also: lint_and_format
---

# Build Skill

Compile the project to verify changes build successfully.

## Project Architecture

The project consists of several subprojects that are linked together into a final Redis module (`redisearch.so`):

```
redisearch.so (final Redis module)
├── redisearch (static lib from deps/RediSearch - the C codebase)
├── redisearch-disk (static lib from redisearch_disk/ - Rust codebase)
├── vecsim_disk (static lib from vecsim_disk/ - C++ codebase)
└── libspeedb.so (shared lib from deps/speedb-ent - key-value storage)
```

## Quick Reference: Build Commands

| Command | What it builds | When to use |
|---------|----------------|-------------|
| `./build.sh build` | Everything (full module) | After fresh clone, changing Rust code, or for final verification |
| `cd redisearch_disk && cargo build` | Rust code only | Fast iteration when only changing Rust code |
| `./build.sh test` | Rust unit tests | Testing Rust code changes |
| `./build.sh test-vecsim` | C++ unit tests | Testing vecsim_disk changes |
| `./build.sh test-flow` | Integration tests | End-to-end testing with Redis |

## Full Build (C & C++ & Rust)

```bash
# Debug build (default - faster compile, slower runtime)
./build.sh build

# Release build (slower compile, optimized runtime)
PROFILE=Release ./build.sh build
```

## Rust-Only Build (Fast Iteration)

When you're only changing Rust code in `redisearch_disk/`:

```bash
# Must have done a full build at least once before this
cd redisearch_disk && cargo build
```

**Important**: Run a full build at least once to:
- Create the SpeedB library (needed for Rust linking)
- Create the RediSearch static library (provides C FFI bindings)

## Integration Tests (test-flow)

Integration tests require `--redis-lib-path` pointing to the directory containing `bs_speedb.so` (typically `/usr/local/lib` after Redis is installed).

```bash
# Run all integration tests
./build.sh test-flow --redis-lib-path /usr/local/lib

# Run a specific test file
./build.sh test-flow --redis-lib-path /usr/local/lib --test test_basic.py

# Run a specific test function
./build.sh test-flow --redis-lib-path /usr/local/lib --test test_basic.py::test_module_loads_successfully

# Run a specific BDD scenario
./build.sh test-flow --redis-lib-path /usr/local/lib --test "features/basic.feature::Create a basic search index"

# Run with verbose output
./build.sh test-flow --redis-lib-path /usr/local/lib --verbose

# Run tests in parallel
./build.sh test-flow --redis-lib-path /usr/local/lib --parallel

# Pass additional pytest options
./build.sh test-flow --redis-lib-path /usr/local/lib -- -k "search" -v
```

The flow tests generate log files under `flow-tests/logs/`. Each test has its own directory with Redis logs and other artifacts. The log files are named according to the test name and timestamp. The log files are not automatically deleted, so you can inspect them for debugging purposes.

## Troubleshooting

### SpeedB linking errors
If cargo fails with SpeedB/RocksDB linking errors:
```bash
# Rebuild SpeedB
./build.sh clean
./build.sh build
```

### CMake cache issues
```bash
./build.sh clean  # Removes build/ directory
./build.sh build
```

### Checking built artifacts
After a successful build:
- Final module: `build/redisearch.so`
- SpeedB library: `build/speedb-build/libspeedb.so`
- Rust library: `redisearch_disk/target/*/libredisearch_disk.a`
- vecsim library: `build/vecsim_disk/libvecsim_disk.a`

## See Also

- [Lint and Format Skill](../lint_and_format/SKILL.md) - Check code quality and formatting before committing
