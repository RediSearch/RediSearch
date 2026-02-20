---
name: verify
description: Run full verification before committing or creating a PR. Use this when you want to create a PR.
---

# Verify Skill

Run full verification before committing or creating a PR.

## Usage
Use this skill to run comprehensive checks before finalizing changes.

## Instructions

Run the following checks in order:

### 1. Format Check
```bash
make fmt CHECK=1
```
If it fails, run `make fmt` to fix formatting.

### 2. Lint Check
```bash
make lint
```
Fix any clippy warnings or errors.

### 3. Build
```bash
./build.sh
```
Ensure the full project compiles.

### 4. Rust Tests
```bash
cd src/redisearch_rs && cargo nextest run
```
All Rust tests must pass.

### 5. Unit Tests (if C code was modified)
```bash
./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1
```

### 6. Behavioral Tests (optional, for significant changes)
```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1
```

## Quick Verification
For minor Rust changes, this minimal check is often sufficient:
```bash
cd src/redisearch_rs && cargo fmt --check && cargo clippy --all-targets && cargo nextest run
```
