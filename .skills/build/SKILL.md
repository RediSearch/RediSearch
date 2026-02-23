---
name: build
description: Compile the project to verify changes build successfully. Use this to verify your changes build properly together with the complete project and dependencies, and make sure to use it before running end to end tests.
---

# Build Skill

Compile the project to verify changes build successfully.

## Usage
Run this skill after making code changes to verify they compile.

## Instructions

1. For a full build (C + Rust):
   ```bash
   ./build.sh
   ```

2. For Rust-only build (faster iteration):
   ```bash
   cd src/redisearch_rs && cargo build
   ```
   Always build the C code at least **once** before running the Rust-only build.

3. If build fails:
   - Read the compiler errors carefully
   - Fix the issues
   - Re-run the build

4. If you update C code, re-build the C code before running the Rust-only build:
   ```bash
   ./build.sh
   cd src/redisearch_rs && cargo build
   ```

## Clean Build

If you encounter strange build errors:
```bash
./build.sh FORCE
```

For Rust only:
```bash
cd src/redisearch_rs && cargo clean && cargo build
```
