---
name: verify
description: Run full verification before committing or creating a PR. Use this when you want to create a PR.
---

# Verify Skill

Run full verification before committing or creating a PR.

## Usage
Use this skill to run comprehensive checks before finalizing changes.

## Prefer the swamp workflows

`verify` runs everything below except the two conditional branches, in one command,
building once and reusing that build for every suite — plus miri, which CI gates on:

```bash
swamp workflow run verify
swamp report get @gdesmott/failure-digest --workflow verify --markdown   # what to fix
```

The cluster branch is its own workflow, because it is a second full run of the suite and
swamp cannot skip a step on a condition:

```bash
swamp workflow run verify-cluster   # changes under src/coord/
```

The AddressSanitizer branch is also a workflow, but it is not part of the gate. CI covers
asan on every pull request, so run it only to reproduce a failure that job reported —
narrowed to the case it named, since the sanitizer needs its own full build:

```bash
swamp workflow run verify-asan --input '{"cTestFilter":"<binary or gtest>"}'
```

For the quick paths at the bottom of this file, use `swamp workflow run rust-quick`
(tests then clippy, debug profile only).

Each workflow asserts that its suites actually ran, so a filter that matched nothing or a
module that failed to load reads as a failure rather than a pass. Follow the steps below
by hand only when swamp is unavailable.

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
