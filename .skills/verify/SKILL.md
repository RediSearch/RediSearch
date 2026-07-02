---
name: verify
description: Run full verification before committing or creating a PR. Use this when you want to create a PR.
---

# Verify Skill

Run full verification before committing or creating a PR.

## Usage
Use this skill to run comprehensive checks before finalizing changes.

## Instructions

Determine which code was modified (C, Rust, or both) and run the appropriate checks.

### If C code was modified

Run the following checks in order:

#### 1. Build
```bash
./build.sh
```
Ensure the full project compiles without warnings promoted to errors.

#### 2. C/C++ Unit Tests
```bash
./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1
```
All unit tests must pass. Use [/run-c-unit-tests](../run-c-unit-tests/SKILL.md) for details
on running specific tests.

#### 3. Behavioral Tests
```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1
```
Required for changes to command handlers, query execution, indexing pipeline, or RDB serialization.

#### 4. AddressSanitizer (recommended for memory-related changes)
```bash
./build.sh RUN_UNIT_TESTS SAN=address
```

#### 5. Coordinator Tests (if `coord/` code was modified)

Changes to the coordinator (`src/coord/`), distributed hybrid (`src/coord/hybrid/`), or
the Map-Reduce layer (`src/coord/rmr/`) must be tested in a clustered environment:

```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1 REDIS_STANDALONE=0 SHARDS=3
```

This spins up a 3-shard cluster and runs the full test suite against it.

### If Rust code was modified

#### 1. Format Check
```bash
make fmt CHECK=1
```
If it fails, run `make fmt` to fix formatting.

#### 2. Lint Check
```bash
make lint
```
Fix any clippy warnings or errors.

#### 3. Build
```bash
./build.sh
```
Ensure the full project compiles.

#### 4. Rust Tests
```bash
cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml
```
All Rust tests must pass.

### If both C and Rust were modified

Run all checks from both sections above.

### Behavioral Tests (for significant changes in either language)
```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1
```

## Quick Verification

For minor Rust-only changes (subshell keeps the chain readable — one cwd hop, three commands):
```bash
(cd src/redisearch_rs && cargo fmt --check && cargo clippy --all-targets && cargo nextest run)
```

For minor C-only changes:
```bash
./build.sh && ./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1
```
