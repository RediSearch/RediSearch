---
name: build
description: Compile the project to verify changes build successfully. Use this to verify your changes build properly together with the complete project and dependencies, and make sure to use it before running end to end tests.
---

# Build Skill

Compile the project to verify changes build successfully.

## Usage
Run this skill after making code changes to verify they compile.

## Instructions

### Full Build (C + Rust)
```bash
./build.sh
```
Use this when you modified C code, or when building for the first time.

### Debug Build (recommended for development)
```bash
./build.sh DEBUG=1
```
Enables debug symbols and additional assertions. Use this when developing or debugging.

### Rust-Only Build (faster iteration)
```bash
cargo build --manifest-path src/redisearch_rs/Cargo.toml
```
Only use after the C code has been built at least **once** with `./build.sh`.
If you update C code, run `./build.sh` again before the Rust-only build.

### Build with Tests
```bash
./build.sh TESTS
```
Compiles test binaries (C/C++ unit tests) alongside the module. Required before
running individual test binaries directly (e.g., `rstest --gtest_filter=...`).

### If Build Fails

- Read the compiler errors carefully.
- C errors: check for missing includes, incompatible pointer types, implicit function
  declarations (all promoted to errors).
- Rust errors: check for FFI signature mismatches if C headers changed.
- Fix the issues and re-run the build.

## Clean Build

If you encounter strange build errors (stale artifacts, CMake cache issues):
```bash
./build.sh FORCE
```

For Rust only:
```bash
(cd src/redisearch_rs && cargo clean && cargo build)   # subshell keeps clean+build co-located
```
